"""Executor-side encoding of query results into C1 lines, bounded chunk files and result payloads."""
from __future__ import annotations

import hashlib
import heapq
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterable, Iterator

from ..common import canonical as c


class EncodeError(Exception):
    def __init__(self, message: str, error_class: str = "CONTRACT_VIOLATION"):
        super().__init__(message)
        self.error_class = error_class


def _dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class ChunkWriter:
    """Writes JSON lines into chunk files of at most `max_bytes` each; records sha256/rows/bytes."""

    def __init__(self, directory: Path, max_bytes: int, max_total_bytes: int | None):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max(1024, int(max_bytes))
        self.max_total = max_total_bytes
        self.chunks: list[dict] = []
        self.total_bytes = 0
        self.rows = 0
        self._f = None
        self._h = None
        self._cur_bytes = 0
        self._cur_rows = 0

    def _open(self) -> None:
        n = len(self.chunks) + 1
        self._path = self.dir / f"chunk_{n:05d}.jsonl"
        self._f = open(self._path, "wb")
        self._h = hashlib.sha256()
        self._cur_bytes = 0
        self._cur_rows = 0

    def _close_chunk(self) -> None:
        if self._f is None:
            return
        self._f.close()
        self.chunks.append({"chunk_no": len(self.chunks) + 1, "rows": self._cur_rows, "bytes": self._cur_bytes,
                            "sha256": self._h.hexdigest(), "file": self._path.name})
        self._f = None

    def write(self, line: dict) -> None:
        data = (_dumps(line) + "\n").encode("utf-8")
        if self.max_total is not None and self.total_bytes + len(data) > self.max_total:
            raise EncodeError(f"result exceeds max_result_bytes={self.max_total}", "BUDGET_EXCEEDED")
        if self._f is not None and self._cur_bytes + len(data) > self.max_bytes and self._cur_rows:
            self._close_chunk()
        if self._f is None:
            self._open()
        self._f.write(data)
        self._h.update(data)
        self._cur_bytes += len(data)
        self._cur_rows += 1
        self.total_bytes += len(data)
        self.rows += 1

    def finish(self) -> list[dict]:
        self._close_chunk()
        return [{k: v for k, v in ch.items() if k != "file"} for ch in self.chunks]


def read_chunk_lines(paths: Iterable[Path]) -> Iterator[dict]:
    for p in paths:
        with open(p, "rb") as f:
            for raw in f:
                if raw.strip():
                    yield json.loads(raw)


class ExternalSorter:
    """Sort (sort_key:str, line:dict) pairs with bounded memory by spilling sorted runs to disk."""

    def __init__(self, tmp_dir: Path, run_rows: int = 200_000):
        self.tmp_dir = Path(tmp_dir)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.run_rows = run_rows
        self.buf: list[tuple[str, str]] = []
        self.runs: list[Path] = []

    def add(self, key: str, line: dict) -> None:
        self.buf.append((key, _dumps(line)))
        if len(self.buf) >= self.run_rows:
            self._spill()

    def _spill(self) -> None:
        self.buf.sort(key=lambda kv: kv[0])
        fd, name = tempfile.mkstemp(dir=self.tmp_dir, suffix=".run")
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            for k, v in self.buf:
                f.write(k + "\t" + v + "\n")
        self.runs.append(Path(name))
        self.buf = []

    def __iter__(self) -> Iterator[tuple[str, dict]]:
        if not self.runs:
            self.buf.sort(key=lambda kv: kv[0])
            for k, v in self.buf:
                yield k, json.loads(v)
            return
        if self.buf:
            self._spill()

        def run_iter(p: Path) -> Iterator[tuple[str, str]]:
            with open(p, encoding="utf-8") as f:
                for raw in f:
                    k, _, v = raw.rstrip("\n").partition("\t")
                    yield k, v

        try:
            for k, v in heapq.merge(*(run_iter(p) for p in self.runs), key=lambda kv: kv[0]):
                yield k, json.loads(v)
        finally:
            for p in self.runs:
                p.unlink(missing_ok=True)


# ---- COUNT ---------------------------------------------------------------------

def encode_count(rows: list[tuple]) -> dict:
    if len(rows) != 1 or len(rows[0]) != 1:
        raise EncodeError("COUNT must return exactly one row with one column")
    v = rows[0][0]
    if v is None or isinstance(v, (float, bool)):
        raise EncodeError("COUNT returned NULL or a non-integer value")
    n = int(c.canonical_int_text(v))
    if n < 0 or n >= 2**63:
        raise EncodeError("COUNT out of BIGINT range")
    return {"row_count": str(n)}


# ---- METRIC --------------------------------------------------------------------

def metrics_schema_digest(rule: dict, columns: dict) -> str:
    group = [[g, columns[g]["type"], columns[g].get("scale")] for g in rule.get("group_by") or []]
    metrics = [[m["id"], m["function"], m.get("column"), m.get("value_type"),
                None if m.get("scale") is None else int(m["scale"])] for m in rule["metrics"]]
    return c.json_digest({"group_by": group, "metrics": metrics}).hex()


def _metric_value(m: dict, raw: Any) -> dict:
    vt = m["value_type"]
    if vt in ("FLOAT32", "FLOAT64"):
        st = c.float_state(raw)
        return {"type": vt, "state": st, "text": c.float_text(raw) if st == "FINITE" else None, "scale": None}
    if raw is None:
        return {"type": vt, "state": "NULL", "text": None, "scale": m.get("scale") if vt == "DECIMAL" else None}
    if isinstance(raw, float):
        raise EncodeError(f"metric {m['id']} returned a float for exact type {vt}")
    try:
        if vt == "INTEGER":
            return {"type": vt, "state": "FINITE", "text": c.canonical_int_text(raw), "scale": None}
        return {"type": vt, "state": "FINITE", "text": c.decimal_text(raw, int(m["scale"])), "scale": int(m["scale"])}
    except c.CanonicalError as e:
        raise EncodeError(f"metric {m['id']}: {e}") from e


def encode_metric(batches: Iterable[list[tuple]], rule: dict, columns: dict, layout: list[dict],
                  writer: ChunkWriter, limits: dict) -> dict:
    group = rule.get("group_by") or []
    max_groups = int(limits.get("max_groups", 10000))
    max_key = int(limits.get("max_group_key_bytes", 512))
    metrics = {m["id"]: m for m in rule["metrics"]}
    groups: dict[bytes, dict] = {}
    for batch in batches:
        for row in batch:
            if len(row) != len(group) + len(layout):
                raise EncodeError("METRIC row has an unexpected column count")
            gvals, rest = row[:len(group)], row[len(group):]
            try:
                key = c.key_bytes([c.encode_val(columns[g]["type"], v, scale=columns[g].get("scale"),
                                                fsp=columns[g].get("fsp", 6)) for g, v in zip(group, gvals)])
                disp = [c.display_text(columns[g]["type"], v, scale=columns[g].get("scale"),
                                       fsp=columns[g].get("fsp", 6)) for g, v in zip(group, gvals)]
            except c.CanonicalError as e:
                raise EncodeError(f"group value cannot be encoded: {e}") from e
            if len(key) > max_key:
                raise EncodeError(f"group key exceeds {max_key} bytes", "BUDGET_EXCEEDED")
            if key in groups:
                raise EncodeError("duplicate canonical group key (grouping semantics differ from C1)")
            rows_n = None
            nn: dict[str, str] = {}
            vals: dict[str, dict] = {}
            for item, raw in zip(layout, rest):
                if item["role"] == "rows":
                    rows_n = int(c.canonical_int_text(raw))
                elif item["role"] == "nn":
                    nn[item["metric"]] = c.canonical_int_text(raw)
                else:
                    vals[item["metric"]] = _metric_value(metrics[item["metric"]], raw)
            if rows_n is None or rows_n < 0:
                raise EncodeError("group_rows missing or negative")
            for mid, m in metrics.items():
                if m["function"] == "COUNT_ROWS":
                    vals[mid] = {"type": "INTEGER", "state": "FINITE", "text": str(rows_n), "scale": None}
                elif m["function"] == "COUNT_NONNULL":
                    vals[mid] = {"type": "INTEGER", "state": "FINITE", "text": nn[mid], "scale": None}
            for mid, v in nn.items():
                if not 0 <= int(v) <= rows_n:
                    raise EncodeError(f"non-null guard {mid} outside 0..group_rows")
            groups[key] = {"k": key.hex(), "v": disp, "rows": str(rows_n),
                           "nn": {k: v for k, v in nn.items() if metrics[k]["function"] == "SUM"}, "m": vals}
            if len(groups) > max_groups:
                raise EncodeError(f"more than {max_groups} groups", "BUDGET_EXCEEDED")
    for key in sorted(groups):
        writer.write(groups[key])
    chunks = writer.finish()
    return {"group_count": len(groups), "metrics_schema_digest": metrics_schema_digest(rule, columns),
            "chunks_manifest": chunks, "total_bytes": writer.total_bytes}


# ---- SAMPLE / DEEP rows -----------------------------------------------------------

def projection_digest(projection: dict, columns: dict) -> str:
    desc = {
        "key_columns": projection.get("key_columns") or [],
        "exact": [[n, columns[n]["type"], columns[n].get("scale"), columns[n].get("fsp", 6)]
                  for n in projection["exact_columns"]],
        "tolerant": [[n, columns[n]["type"]] for n in projection.get("tolerant_columns") or []],
    }
    return c.json_digest(desc).hex()


class RowEncoder:
    """Encodes selected rows: KEY, ROW over exact columns, row_hash, tolerant float text."""

    def __init__(self, select_cols: list[str], projection: dict, columns: dict, limits: dict,
                 sample: dict | None = None):
        self.idx = {n: i for i, n in enumerate(select_cols)}
        self.keys = projection.get("key_columns") or []
        self.exact = projection["exact_columns"]
        self.tolerant = projection.get("tolerant_columns") or []
        self.columns = columns
        self.sample = sample
        self.max_key_bytes = int(limits.get("max_key_bytes", 256))
        self.max_value = int(limits.get("max_value_bytes", 16 * 1024 * 1024))
        self.max_row = int(limits.get("max_row_bytes", 32 * 1024 * 1024))

    def encode(self, row: tuple) -> tuple[bytes | None, bytes, dict]:
        try:
            key = None
            if self.keys:
                kc = self.keys[0]
                kv = row[self.idx[kc]]
                lt = self.columns[kc]["type"]
                key = c.single_key(lt, kv)
                if lt == "STRING" and len(c.payload_for(lt, kv)) > self.max_key_bytes:
                    raise EncodeError(f"key exceeds max_key_bytes={self.max_key_bytes}")
                if self.sample is not None and not c.s1_selected(key, self.sample["seed_hex"],
                                                                 self.sample["bucket_count"],
                                                                 self.sample["selected_bucket"]):
                    raise EncodeError("SR returned a row that S1 does not select (pushdown semantics differ)")
            vals = []
            for n in self.exact:
                col = self.columns[n]
                v = c.encode_val(col["type"], row[self.idx[n]], scale=col.get("scale"), fsp=col.get("fsp", 6))
                if len(v) > self.max_value:
                    raise EncodeError(f"value of {n} exceeds max_value_bytes", "BUDGET_EXCEEDED")
                vals.append(v)
            rb = c.row_bytes(vals)
            if len(rb) > self.max_row:
                raise EncodeError("row exceeds max_row_bytes", "BUDGET_EXCEEDED")
        except c.CanonicalError as e:
            raise EncodeError(str(e)) from e
        tol = {}
        for n in self.tolerant:
            raw = row[self.idx[n]]
            tol[n] = None if raw is None else c.float_text(raw)
        line = {"h": c.sha256(rb).hex(), "v": [v.hex() for v in vals]}
        if tol:
            line["t"] = tol
        if key is not None:
            line["k"] = key.hex()
        return key, rb, line


def encode_keyed(batches: Iterable[list[tuple]], enc: RowEncoder, members: list[dict], writers: list[ChunkWriter],
                 tmp_dir: Path, limits: dict, *, bucket_of=None) -> list[dict]:
    """Sort rows by C1 KEY bytes per member, reject duplicate keys, build D1 digests. Returns per-member stats."""
    max_rows = int(limits.get("max_rows", 2_000_000))
    sorters = [ExternalSorter(tmp_dir / f"m{i}") for i in range(len(members))]
    total = 0
    for batch in batches:
        for row in batch:
            key, _rb, line = enc.encode(row)
            i = bucket_of(key, line) if bucket_of else 0
            sorters[i].add(line["k"], line)
            total += 1
            if total > max_rows:
                raise EncodeError(f"more than {max_rows} rows", "BUDGET_EXCEEDED")
    stats = []
    for i, member in enumerate(members):
        ctx = bytes.fromhex(member["context_digest_hex"])
        rows_d1 = c.D1(ctx)
        keys_d1 = c.D1(c.key_projection_context(ctx))
        prev = None
        n = 0
        for k, line in sorters[i]:
            if k == prev:
                raise EncodeError("duplicate key in result (key contract violated)")
            prev = k
            rows_d1.add(bytes.fromhex(line["h"]))
            keys_d1.add(c.sha256(bytes.fromhex(k)))
            writers[i].write(line)
            n += 1
        stats.append({"rows": n, "row_digest": rows_d1.finish().hex(), "key_digest": keys_d1.finish().hex(),
                      "chunks": writers[i].finish(), "total_bytes": writers[i].total_bytes})
    return stats


def encode_keyless(batches: Iterable[list[tuple]], enc: RowEncoder, members: list[dict], writers: list[ChunkWriter],
                   tmp_dir: Path, limits: dict, *, bucket_of=None) -> list[dict]:
    """Multiset of full canonical rows: sorted by row_hash with occurrence counts."""
    max_rows = int(limits.get("max_rows", 2_000_000))
    sorters = [ExternalSorter(tmp_dir / f"m{i}") for i in range(len(members))]
    total = 0
    for batch in batches:
        for row in batch:
            _k, _rb, line = enc.encode(row)
            i = bucket_of(None, line) if bucket_of else 0
            sorters[i].add(line["h"], line)
            total += 1
            if total > max_rows:
                raise EncodeError(f"more than {max_rows} rows", "BUDGET_EXCEEDED")
    stats = []
    for i, member in enumerate(members):
        d = c.D1(bytes.fromhex(member["context_digest_hex"]))
        cur = None
        n_rows = 0

        def flush():
            if cur is not None:
                writers[i].write(cur)

        for h, line in sorters[i]:
            d.add(bytes.fromhex(h))
            n_rows += 1
            if cur is not None and cur["h"] == h:
                cur["n"] += 1
                continue
            flush()
            cur = {"h": h, "n": 1, "v": line["v"]}
        flush()
        stats.append({"rows": n_rows, "row_digest": d.finish().hex(), "key_digest": None,
                      "chunks": writers[i].finish(), "total_bytes": writers[i].total_bytes})
    return stats


def hash_bucket_of(buckets: int):
    def f(key: bytes | None, line: dict) -> int:
        basis = c.sha256(key) if key is not None else bytes.fromhex(line["h"])
        return int.from_bytes(basis[:4], "big") % buckets
    return f
