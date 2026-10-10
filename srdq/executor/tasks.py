"""Execute one claimed work item against an adapter and produce per-member results (no controller I/O)."""
from __future__ import annotations

import datetime as _dt
import shutil
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from . import compiler, encoder
from .adapter.base import AdapterError


@dataclass
class MemberResult:
    node_id: str
    context_digest_hex: str
    payload: dict
    chunk_dir: Path | None
    read_started: str
    read_finished: str
    evidence: str


@dataclass
class TaskOutcome:
    members: list[MemberResult] = field(default_factory=list)
    metadata: dict | None = None
    bytes_read: int = 0


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


class Cancelled(Exception):
    pass


def _batches(session, sql, params, batch_rows, cancel: threading.Event, usage: Callable[[int], None]):
    for b in session.stream(sql, params, batch_rows):
        if cancel.is_set():
            raise Cancelled()
        usage(sum(_approx_size(r) for r in b))
        yield b


def _approx_size(row: tuple) -> int:
    n = 0
    for v in row:
        if v is None:
            n += 1
        elif isinstance(v, (bytes, str)):
            n += len(v)
        else:
            n += 8
    return n


def run_metadata(spec: dict, adapter) -> dict:
    t = spec["table"]
    meta = adapter.table_meta(t["db"], t["table"])
    return meta.to_json()


def run_check(spec: dict, adapter, workdir: Path, *, cancel: threading.Event,
              on_query_ref: Callable[[dict], None], usage: Callable[[int], None]) -> TaskOutcome:
    kind = spec["check_kind"]
    table = spec["table"]
    cols = spec["columns"]
    limits = spec.get("limits", {})
    batch_rows = int(limits.get("batch_rows", 5000))
    out = TaskOutcome()
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    members = spec["members"]
    session = adapter.connect()
    try:
        session.set_limits(int(limits.get("query_timeout_seconds", 300)), limits.get("query_mem_limit_bytes"))
        on_query_ref(session.query_ref())
        tracker = [0]

        def track(n: int) -> None:
            tracker[0] += n
            usage(tracker[0])

        if kind == "COUNT":
            for m in members:
                started = now_iso()
                sql, params = compiler.compile_count(table, m["predicate"])
                rows = [r for b in _batches(session, sql, params, batch_rows, cancel, track) for r in b]
                payload = encoder.encode_count(rows)
                out.members.append(MemberResult(m["node_id"], m["context_digest_hex"], payload, None,
                                                started, now_iso(), "COUNT"))
        elif kind == "METRIC":
            rule = spec["rule"]
            layout = compiler.metric_layout(rule)
            for m in members:
                started = now_iso()
                sql, params = compiler.compile_metric(table, m["predicate"], rule, cols)
                d = workdir / m["node_id"]
                w = encoder.ChunkWriter(d, limits.get("batch_bytes", 8 << 20), limits.get("max_result_bytes"))
                payload = encoder.encode_metric(_batches(session, sql, params, batch_rows, cancel, track),
                                                rule, cols, layout, w, limits)
                out.members.append(MemberResult(m["node_id"], m["context_digest_hex"], payload, d,
                                                started, now_iso(), "METRIC"))
        elif kind in ("SAMPLE", "DEEP"):
            _run_rows(spec, session, members, workdir, cancel, track, out)
        else:
            raise AdapterError(f"unknown check kind {kind}", error_class="CONTRACT_VIOLATION")
        out.bytes_read = tracker[0]
        return out
    finally:
        session.close()


def _run_rows(spec, session, members, workdir, cancel, track, out: TaskOutcome) -> None:
    kind = spec["check_kind"]
    table, cols, limits = spec["table"], spec["columns"], spec.get("limits", {})
    batch_rows = int(limits.get("batch_rows", 5000))
    proj = spec["projection"]
    keys = proj.get("key_columns") or []
    select_cols = list(dict.fromkeys(keys + proj["exact_columns"] + (proj.get("tolerant_columns") or [])))
    pdig = encoder.projection_digest(proj, cols)
    started = now_iso()
    tmp = workdir / "_sort"
    if kind == "SAMPLE":
        sample = spec["sample"]
        m = members[0]
        sql, params = compiler.compile_sample(table, m["predicate"], sample, select_cols)
        enc = encoder.RowEncoder(select_cols, proj, cols, limits, sample=sample)
        d = workdir / m["node_id"]
        w = encoder.ChunkWriter(d, limits.get("batch_bytes", 8 << 20), limits.get("max_result_bytes"))
        st = encoder.encode_keyed(_batches(session, sql, params, batch_rows, cancel, track), enc, [m], [w], tmp,
                                  limits)[0]
        payload = {"sample_rows": st["rows"], "sample_protocol": "SRDQ-SAMPLE-1", "rule_digest": spec["rule_digest_hex"],
                   "sample_key_digest": st["key_digest"], "sample_row_digest": st["row_digest"],
                   "projection_digest": pdig, "chunks_manifest": st["chunks"], "total_bytes": st["total_bytes"]}
        out.members.append(MemberResult(m["node_id"], m["context_digest_hex"], payload, d, started, now_iso(),
                                        "SAMPLE"))
    else:
        deep = spec["deep"]
        evidence = spec.get("evidence", "CONTENT_HASH")
        enc = encoder.RowEncoder(select_cols, proj, cols, limits)
        dirs = [workdir / m["node_id"] for m in members]
        writers = [encoder.ChunkWriter(d, limits.get("batch_bytes", 8 << 20), limits.get("max_result_bytes"))
                   for d in dirs]
        if deep["mode"] == "HASH_BUCKET":
            predicate = members[0]["predicate"]
            bucket_of = encoder.hash_bucket_of(int(deep["buckets"]))
            index = {int(m["bucket"]): i for i, m in enumerate(members)}
            if len(index) != int(deep["buckets"]):
                raise AdapterError("HASH_BUCKET unit must cover all buckets", error_class="CONTRACT_VIOLATION")
            route = lambda k, line: index[bucket_of(k, line)]  # noqa: E731
        else:
            if len(members) != 1:
                raise AdapterError("KEY_RANGE unit must have exactly one member", error_class="CONTRACT_VIOLATION")
            predicate = members[0]["predicate"]
            route = None
        sql, params = compiler.compile_rows(table, predicate, select_cols)
        batches = _batches(session, sql, params, batch_rows, cancel, track)
        if keys:
            stats = encoder.encode_keyed(batches, enc, members, writers, tmp, limits, bucket_of=route)
        else:
            stats = encoder.encode_keyless(batches, enc, members, writers, tmp, limits, bucket_of=route)
        finished = now_iso()
        for m, d, st in zip(members, dirs, stats):
            payload = {"row_count": st["rows"], "digest": st["row_digest"], "projection_digest": pdig,
                       "sort": "C1_KEY_BYTES" if keys else "ROW_HASH", "keyless": not keys,
                       "chunks_manifest": st["chunks"], "total_bytes": st["total_bytes"]}
            out.members.append(MemberResult(m["node_id"], m["context_digest_hex"], payload, d, started, finished,
                                            evidence))
    shutil.rmtree(tmp, ignore_errors=True)
