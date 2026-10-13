"""Compare both sides' results, derive SAMPLE drill-down tasks, and write the report files."""
from __future__ import annotations

import csv
import os
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Iterator

from .execute import latest_records
from .plan import SIDES, plan_drill
from .store import dumps, iter_gz_rows, write_csv, write_jsonl

STATUS_PRIORITY = ("DIFF", "ERROR", "BLOCKED", "PASS", "SKIPPED")
_NUMERIC_CATS = {"int", "decimal", "float", "bool"}
MAX_VALUE_CHARS = 512

DIFF_CSV_HEADER = ["run_id", "table_id", "rule_key", "rule", "diff_type", "partition", "key", "group", "metric",
                   "column", "src", "tgt", "delta", "task_id", "detail"]
TABLE_CSV_HEADER = ["run_id", "table_id", "source_table", "target_table", "status", "meta_status",
                    "partitions_src", "partitions_tgt", "partitions_common",
                    "row_count_status", "row_count_src", "row_count_tgt", "row_count_diff_partitions",
                    "metric_status", "metric_groups", "metric_diff_groups",
                    "sample_status", "sample_rows", "sample_diff_keys",
                    "failed_tasks", "elapsed_s", "note", "meta_issues"]


def worst(statuses: Iterable[str]) -> str:
    s = set(statuses)
    return next((x for x in STATUS_PRIORITY if x in s), "")


# ---------------------------------------------------------------- value comparison

def _num(v: Any) -> Decimal | None:
    if isinstance(v, bool):
        return Decimal(int(v))
    if isinstance(v, int):
        return Decimal(v)
    if isinstance(v, str):
        try:
            d = Decimal(v.strip())
        except InvalidOperation:
            return None
        return d if d.is_finite() else None
    return None


def values_equal(a: Any, b: Any, numeric: bool, tolerance: float = 0.0) -> bool:
    if a == b:
        return True
    if a is None or b is None or not numeric:
        return False
    da, db = _num(a), _num(b)
    if da is None or db is None:
        return False
    if da == db:
        return True
    if tolerance:
        return abs(da - db) <= Decimal(repr(tolerance)) * max(abs(da), abs(db))
    return False


def delta(a: Any, b: Any) -> str | None:
    da, db = _num(a), _num(b)
    return None if da is None or db is None else str(db - da)


def clip(v: Any) -> Any:
    if isinstance(v, str) and len(v) > MAX_VALUE_CHARS:
        return v[:MAX_VALUE_CHARS] + f"...(truncated, length {len(v)})"
    return v


# ---------------------------------------------------------------- sorted merge

class OrderError(Exception):
    pass


def _null_first(v: Any) -> tuple:
    return (0, "") if v is None else (1, str(v))


def grouped(rows: Iterable[list], keyfn) -> Iterator[tuple[Any, list[list]]]:
    """Group consecutive rows with equal keys, verifying the stream is ascending."""
    prev: Any = None
    bucket: list[list] = []
    for r in rows:
        k = keyfn(r)
        if bucket and k == prev:
            bucket.append(r)
            continue
        if bucket:
            if k < prev:
                raise OrderError("result rows are not ordered as expected; cannot merge")
            yield prev, bucket
        prev, bucket = k, [r]
    if bucket:
        yield prev, bucket


def merge(a_rows: Iterable[list], b_rows: Iterable[list], keyfn) -> Iterator[tuple[Any, list, list]]:
    ga, gb = grouped(a_rows, keyfn), grouped(b_rows, keyfn)
    a, b = next(ga, None), next(gb, None)
    while a is not None or b is not None:
        if b is None or (a is not None and a[0] < b[0]):
            yield a[0], a[1], []
            a = next(ga, None)
        elif a is None or b[0] < a[0]:
            yield b[0], [], b[1]
            b = next(gb, None)
        else:
            yield a[0], a[1], b[1]
            a, b = next(ga, None), next(gb, None)


# ---------------------------------------------------------------- results access

class Results:
    def __init__(self, out_dir: Path, records_path: Path):
        self.out_dir = out_dir
        self.recs = latest_records(records_path)

    def state(self, task: dict | None) -> tuple[str, str | None]:
        if task is None:
            return "PENDING", "not planned"
        recs = [self.recs.get((task["task_id"], s)) for s in SIDES]
        if all(r and r["status"] == "OK" and (self.out_dir / r["file"]).exists() for r in recs):
            return "OK", None
        errors = [f"{s}: {r['error']}" for s, r in zip(SIDES, recs) if r and r["status"] == "FAILED"]
        if errors:
            return "FAILED", "; ".join(errors)
        return "PENDING", "not executed yet"

    def rows(self, task: dict, side: str) -> Iterator[list]:
        return iter_gz_rows(self.out_dir / self.recs[(task["task_id"], side)]["file"])

    def elapsed(self, task: dict) -> float:
        return max((r["elapsed_s"] for s in SIDES if (r := self.recs.get((task["task_id"], s)))), default=0.0)

    def failures(self, task: dict) -> list[dict]:
        out = []
        for s in SIDES:
            r = self.recs.get((task["task_id"], s))
            if r is None or r["status"] != "OK":
                out.append({"side": s, "status": r["status"] if r else "PENDING",
                            "error": r["error"] if r else "not executed", "attempts": r["attempts"] if r else 0})
        return out


def sample_buckets(results: Results, task: dict) -> tuple[dict[int, list[int]], dict[int, set[int]]]:
    """Per partition index: sampled rows [src, tgt], and the set of buckets whose (count, fingerprint) differ."""
    sides = [{(int(r[0]), int(r[1])): (int(r[2]), str(r[3])) for r in results.rows(task, s)} for s in SIDES]
    counts: dict[int, list[int]] = defaultdict(lambda: [0, 0])
    mismatched: dict[int, set[int]] = defaultdict(set)
    for idx, side in enumerate(sides):
        for (p, _), (cnt, _) in side.items():
            counts[p][idx] += cnt
    for k in set(sides[0]) | set(sides[1]):
        if sides[0].get(k) != sides[1].get(k):
            mismatched[k[0]].add(k[1])
    return dict(counts), dict(mismatched)


def derive_drills(entries: list[dict], main_tasks: list[dict], results: Results) -> list[dict]:
    by_table = {e["table_id"]: e for e in entries}
    drills = []
    for t in main_tasks:
        if t["rule"] != "SAMPLE" or results.state(t)[0] != "OK":
            continue
        _, mism = sample_buckets(results, t)
        if mism:
            entry = by_table[t["table_id"]]
            rule = next(r for r in entry["rules"] if r["key"] == t["rule_key"])
            drills.append(plan_drill(entry, rule, t, mism))
    return drills


# ---------------------------------------------------------------- output sink

class Sink:
    def __init__(self, out_dir: Path, run_id: str, max_diffs: int):
        self.out_dir, self.run_id, self.max_diffs = out_dir, run_id, max_diffs
        self._files = {name: open(out_dir / f"{name}.tmp", "w", encoding="utf-8", newline="")
                       for name in ("05_compare.jsonl", "diffs.jsonl", "diffs.csv")}
        self._files["diffs.csv"].write("\ufeff")
        self._csv = csv.DictWriter(self._files["diffs.csv"], fieldnames=DIFF_CSV_HEADER)
        self._csv.writeheader()
        self.by_type: Counter = Counter()
        self.by_rule: Counter = Counter()
        self.written = 0
        self.truncated = 0

    def compare_row(self, entry: dict, rule: dict, partition: str, status: str, src: Any, tgt: Any,
                    diffs: int = 0, note: str | None = None, task_id: str | None = None) -> None:
        self._files["05_compare.jsonl"].write(dumps({
            "run_id": self.run_id, "table_id": entry["table_id"], "rule_key": rule["key"], "rule": rule["name"],
            "partition": partition, "status": status, "src": src, "tgt": tgt, "diffs": diffs, "note": note,
            "task_id": task_id}) + "\n")

    def diff(self, entry: dict, rule_key: str, rule: str, diff_type: str, partition: str | None,
             task_id: str | None, **fields: Any) -> None:
        self.by_type[diff_type] += 1
        self.by_rule[(entry["table_id"], rule_key)] += 1
        if self.by_rule[(entry["table_id"], rule_key)] > self.max_diffs:
            self.truncated += 1
            return
        for k in ("src", "tgt"):
            if k in fields:
                fields[k] = clip(fields[k])
        rec = {"run_id": self.run_id, "table_id": entry["table_id"], "rule_key": rule_key, "rule": rule,
               "diff_type": diff_type, "partition": partition, **fields, "task_id": task_id}
        self._files["diffs.jsonl"].write(dumps(rec) + "\n")
        row = {k: rec.get(k) for k in DIFF_CSV_HEADER if k != "detail"}
        row["detail"] = {k: v for k, v in rec.items() if k not in DIFF_CSV_HEADER} or None
        self._csv.writerow({k: (dumps(v) if isinstance(v, (dict, list)) else "" if v is None else v)
                            for k, v in row.items()})
        self.written += 1

    def close(self) -> None:
        for name, f in self._files.items():
            f.flush()
            os.fsync(f.fileno())
            f.close()
            os.replace(self.out_dir / f"{name}.tmp", self.out_dir / name)


# ---------------------------------------------------------------- per-rule comparison

def _compare_row_count(sink: Sink, entry: dict, rule: dict, task: dict, res: Results, st: dict) -> None:
    src, tgt = ({int(r[0]): int(r[1]) for r in res.rows(task, s)} for s in SIDES)
    for i, part in enumerate(task["parts"]):
        s, t = src.get(i), tgt.get(i)
        status = "PASS" if s == t and s is not None else "DIFF"
        st["row_count_src"] += s or 0
        st["row_count_tgt"] += t or 0
        if status == "DIFF":
            st["row_count_diff_partitions"] += 1
            sink.diff(entry, rule["key"], rule["name"], "COUNT_DIFF", part["key"], task["task_id"],
                      src=s, tgt=t, delta=delta(s, t))
        sink.compare_row(entry, rule, part["key"], status, s, t, int(status == "DIFF"), task_id=task["task_id"])
        st["statuses"].append(status)


def _compare_metric(sink: Sink, entry: dict, rule: dict, task: dict, res: Results, st: dict) -> None:
    r = rule["resolved"]
    ng = len(r["group_by"])
    gnames = [g["src"] for g in r["group_by"]]
    metrics = [("COUNT(*)", True, 0.0)] + [(m["label"], m["numeric"], m["tolerance"]) for m in r["metrics"]]
    per: dict[int, list[int]] = defaultdict(lambda: [0, 0, 0])  # src groups, tgt groups, diff groups

    def values(row: list) -> dict:
        return {label: clip(row[1 + ng + j]) for j, (label, _, _) in enumerate(metrics)}

    def keyfn(row: list) -> tuple:
        return int(row[0]), tuple(_null_first(v) for v in row[1:1 + ng])

    try:
        for key, a, b in merge(res.rows(task, "source"), res.rows(task, "target"), keyfn):
            if len(a) > 1 or len(b) > 1:
                raise OrderError("duplicate group rows in METRIC result")
            ra, rb = (a[0] if a else None), (b[0] if b else None)
            p = key[0]
            cnt = per[p]
            any_row = ra or rb
            group = {gnames[j]: any_row[1 + j] for j in range(ng)}
            part_key = task["parts"][p]["key"]
            cnt[0] += ra is not None
            cnt[1] += rb is not None
            if ra is None or rb is None:
                cnt[2] += 1
                kind = "METRIC_GROUP_TARGET_ONLY" if ra is None else "METRIC_GROUP_SOURCE_ONLY"
                sink.diff(entry, rule["key"], rule["name"], kind, part_key, task["task_id"], group=group,
                          values=values(any_row))
                continue
            differs = False
            for j, (label, numeric, tol) in enumerate(metrics):
                va, vb = ra[1 + ng + j], rb[1 + ng + j]
                if not values_equal(va, vb, numeric, tol):
                    differs = True
                    sink.diff(entry, rule["key"], rule["name"], "METRIC_VALUE_DIFF", part_key, task["task_id"],
                              group=group, metric=label, src=va, tgt=vb, delta=delta(va, vb),
                              tolerance=tol or None)
            cnt[2] += differs
    except OrderError as e:
        _error_parts(sink, entry, rule, task, task["parts"], str(e), st)
        return
    for i, part in enumerate(task["parts"]):
        s, t, d = per.get(i, (0, 0, 0))
        status = "DIFF" if d else "PASS"
        st["metric_groups"] += max(s, t)
        st["metric_diff_groups"] += d
        sink.compare_row(entry, rule, part["key"], status, s, t, d, task_id=task["task_id"])
        st["statuses"].append(status)


def _compare_sample(sink: Sink, entry: dict, rule: dict, task: dict, res: Results, drill: dict | None,
                    drill_res: Results, st: dict) -> None:
    counts, mism = sample_buckets(res, task)
    diff_keys: dict[int, int] = defaultdict(int)
    drill_error = None
    if mism:
        state, err = drill_res.state(drill)
        if state != "OK":
            drill_error = f"drill-down {state.lower()}: {err}"
        else:
            try:
                _sample_drill(sink, entry, rule, drill, drill_res, diff_keys)
            except OrderError as e:
                drill_error = str(e)
    for i, part in enumerate(task["parts"]):
        s, t = counts.get(i, (0, 0))
        st["sample_rows"] += max(s, t)
        if i in mism and drill_error:
            sink.compare_row(entry, rule, part["key"], "ERROR", s, t, note=drill_error, task_id=task["task_id"])
            st["statuses"].append("ERROR")
            continue
        d = diff_keys.get(part["key"], 0)
        note = "bucket fingerprint differed but no row difference found" if i in mism and not d else None
        status = "DIFF" if d else "PASS"
        st["sample_diff_keys"] += d
        sink.compare_row(entry, rule, part["key"], status, s, t, d, note=note, task_id=task["task_id"])
        st["statuses"].append(status)
    if drill_error:
        st["notes"].append(f"rule {rule['key']}: {drill_error}")


def _sample_drill(sink: Sink, entry: dict, rule: dict, drill: dict, res: Results, diff_keys: dict) -> None:
    r = rule["resolved"]
    nk = len(r["keys"])
    knames = [k["src"] for k in r["keys"]]
    cols = r["columns"]

    def keyfn(row: list) -> tuple:
        return int(row[0]), tuple(_null_first(v) for v in row[1:1 + nk])

    def row_values(row: list) -> dict:
        return {c["src"]: clip(row[1 + nk + j]) for j, c in enumerate(cols)}

    for key, a, b in merge(res.rows(drill, "source"), res.rows(drill, "target"), keyfn):
        part_key = drill["parts"][key[0]]["key"]
        any_row = (a or b)[0]
        kv = any_row[1:1 + nk]
        extra = {"key": kv[0] if nk == 1 else ", ".join(str(v) for v in kv)}
        if nk > 1:
            extra["key_columns"] = knames
        args = (entry, rule["key"], rule["name"])
        if len(a) > 1 or len(b) > 1:
            sink.diff(*args, "SAMPLE_DUPLICATE_KEY", part_key, drill["task_id"], **extra,
                      src=len(a), tgt=len(b))
        elif not b:
            sink.diff(*args, "SAMPLE_KEY_SOURCE_ONLY", part_key, drill["task_id"], **extra,
                      values=row_values(a[0]))
        elif not a:
            sink.diff(*args, "SAMPLE_KEY_TARGET_ONLY", part_key, drill["task_id"], **extra,
                      values=row_values(b[0]))
        else:
            ra, rb = a[0], b[0]
            differs = False
            for j, c in enumerate(cols):
                va, vb = ra[1 + nk + j], rb[1 + nk + j]
                if not values_equal(va, vb, c["cat"] in _NUMERIC_CATS):
                    differs = True
                    sink.diff(*args, "SAMPLE_COLUMN_DIFF", part_key, drill["task_id"], **extra,
                              column=c["src"], src=va, tgt=vb)
            if not differs:
                continue
        diff_keys[part_key] += 1


def _error_parts(sink: Sink, entry: dict, rule: dict, task: dict, parts: list[dict], error: str, st: dict) -> None:
    for part in parts:
        sink.compare_row(entry, rule, part["key"], "ERROR", None, None, note=error, task_id=task["task_id"])
        st["statuses"].append("ERROR")


# ---------------------------------------------------------------- table level

def _new_stats() -> dict:
    return {"statuses": [], "notes": [], "row_count_src": 0, "row_count_tgt": 0, "row_count_diff_partitions": 0,
            "metric_groups": 0, "metric_diff_groups": 0, "sample_rows": 0, "sample_diff_keys": 0}


def compare_table(sink: Sink, entry: dict, tasks: list[dict], drills: dict[str, dict], main_res: Results,
                  drill_res: Results) -> tuple[dict, list[dict]]:
    by_type: dict[str, list[str]] = defaultdict(list)
    ran: set[str] = set()
    stats = {"ROW_COUNT": _new_stats(), "METRIC": _new_stats(), "SAMPLE": _new_stats()}
    notes: list[str] = []
    failed: list[dict] = []
    elapsed = 0.0
    rules_out = []
    tasks_by_rule: dict[str, list[dict]] = defaultdict(list)
    for t in tasks:
        tasks_by_rule[t["rule_key"]].append(t)

    for p in entry["source_only_partitions"]:
        sink.diff(entry, "meta", "PARTITION", "PARTITION_SOURCE_ONLY", p["key"], None, partition_name=p["name"],
                  partition_rows=p["rows"])
    for p in entry["target_only_partitions"]:
        sink.diff(entry, "meta", "PARTITION", "PARTITION_TARGET_ONLY", p["key"], None, partition_name=p["name"],
                  partition_rows=p["rows"])

    for rule in entry["rules"]:
        st = stats[rule["name"]]
        if rule["status"] != "READY":
            status = rule["status"]
            notes.append(f"rule {rule['key']} {status}: {rule['reason']}")
        elif not tasks_by_rule[rule["key"]]:
            status = "SKIPPED"
            notes.append(f"rule {rule['key']} SKIPPED: no common partitions")
        else:
            ran.add(rule["name"])
            before = len(st["statuses"])
            for task in tasks_by_rule[rule["key"]]:
                elapsed += main_res.elapsed(task)
                state, err = main_res.state(task)
                if state != "OK":
                    failed.append(_failed(task, main_res, sink.run_id))
                    _error_parts(sink, entry, rule, task, task["parts"], f"task {state.lower()}: {err}", st)
                    continue
                if rule["name"] == "ROW_COUNT":
                    _compare_row_count(sink, entry, rule, task, main_res, st)
                elif rule["name"] == "METRIC":
                    _compare_metric(sink, entry, rule, task, main_res, st)
                else:
                    drill = drills.get(task["task_id"])
                    if drill is not None:
                        elapsed += drill_res.elapsed(drill)
                        if drill_res.state(drill)[0] != "OK":
                            failed.append(_failed(drill, drill_res, sink.run_id))
                    _compare_sample(sink, entry, rule, task, main_res, drill, drill_res, st)
            status = worst(st["statuses"][before:]) or "PASS"
        by_type[rule["name"]].append(status)
        rules_out.append({"key": rule["key"], "name": rule["name"], "status": status, "reason": rule.get("reason")})
    for s in stats.values():
        notes.extend(s["notes"])

    overall = [s for v in by_type.values() for s in v]
    if entry["meta_status"] == "NOT_FOUND":
        overall.append("BLOCKED")
    if entry["meta_status"] == "ERROR":
        overall.append("ERROR")
    if entry["source_only_partitions"] or entry["target_only_partitions"]:
        overall.append("DIFF")
        notes.insert(0, f"partitions only in source: {len(entry['source_only_partitions'])}, "
                        f"only in target: {len(entry['target_only_partitions'])}")
    rc, mt, sp = stats["ROW_COUNT"], stats["METRIC"], stats["SAMPLE"]
    row = {
        "run_id": sink.run_id, "table_id": entry["table_id"],
        "source_table": f"{entry['source_db']}.{entry['source_table']}",
        "target_table": f"{entry['target_db']}.{entry['target_table']}",
        "status": worst(overall) or "SKIPPED", "meta_status": entry["meta_status"],
        "partitions_src": entry["partitions_src"], "partitions_tgt": entry["partitions_tgt"],
        "partitions_common": len(entry["common_partitions"]) if entry["partitioned"] else 0,
        "row_count_status": worst(by_type["ROW_COUNT"]) or None,
        "row_count_src": rc["row_count_src"] if "ROW_COUNT" in ran else None,
        "row_count_tgt": rc["row_count_tgt"] if "ROW_COUNT" in ran else None,
        "row_count_diff_partitions": rc["row_count_diff_partitions"] if "ROW_COUNT" in ran else None,
        "metric_status": worst(by_type["METRIC"]) or None,
        "metric_groups": mt["metric_groups"] if "METRIC" in ran else None,
        "metric_diff_groups": mt["metric_diff_groups"] if "METRIC" in ran else None,
        "sample_status": worst(by_type["SAMPLE"]) or None,
        "sample_rows": sp["sample_rows"] if "SAMPLE" in ran else None,
        "sample_diff_keys": sp["sample_diff_keys"] if "SAMPLE" in ran else None,
        "failed_tasks": len(failed), "elapsed_s": round(elapsed, 3), "note": "; ".join(notes),
        "meta_issues": [f"{i['level']} {i['kind']}: {i['message']}" for i in entry["issues"]],
        "rules": rules_out,
    }
    return row, failed


def _failed(task: dict, res: Results, run_id: str) -> dict:
    return {"run_id": run_id, "task_id": task["task_id"], "phase": task["phase"], "table_id": task["table_id"],
            "rule_key": task["rule_key"], "rule": task["rule"], "sides": res.failures(task),
            "partitions": [p["key"] for p in task["parts"]],
            "source_sql": task["source_sql"], "target_sql": task["target_sql"]}


def build_outputs(out_dir: Path, run_id: str, entries: list[dict], main_tasks: list[dict], drill_tasks: list[dict],
                  main_res: Results, drill_res: Results, max_diffs: int) -> dict:
    sink = Sink(out_dir, run_id, max_diffs)
    tasks_by_table: dict[str, list[dict]] = defaultdict(list)
    for t in main_tasks:
        tasks_by_table[t["table_id"]].append(t)
    drills = {d["parent_task_id"]: d for d in drill_tasks}
    rows, failed = [], []
    try:
        for entry in entries:
            row, f = compare_table(sink, entry, tasks_by_table[entry["table_id"]], drills, main_res, drill_res)
            rows.append(row)
            failed.extend(f)
    finally:
        sink.close()
    write_jsonl(out_dir / "table_summary.jsonl", rows)
    write_csv(out_dir / "table_summary.csv", TABLE_CSV_HEADER,
              [{**r, "meta_issues": "; ".join(r["meta_issues"])} for r in rows])
    write_jsonl(out_dir / "failed_tasks.jsonl", failed)
    return {"tables": Counter(r["status"] for r in rows), "diffs_by_type": dict(sink.by_type),
            "diffs_written": sink.written, "diffs_truncated": sink.truncated, "failed_tasks": len(failed)}
