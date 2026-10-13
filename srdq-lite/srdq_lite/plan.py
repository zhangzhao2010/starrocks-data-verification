"""Stage 3: turn resolved rules into batched UNION ALL SQL tasks (and SAMPLE drill-down tasks)."""
from __future__ import annotations

import hashlib
import json

from .config import SAMPLE_MOD, RunSettings
from .sr import qi, qtable

SIDES = ("source", "target")
RULE_ORDER = {"ROW_COUNT": 0, "METRIC": 1, "SAMPLE": 2}
_SHORT = {"source": "src", "target": "tgt"}


def value_expr(col: str, cat: str) -> str:
    c = qi(col)
    if cat == "complex":
        return f"CAST(to_json(named_struct('v', {c})) AS STRING)"
    if cat == "binary":
        return f"hex({c})"
    return f"CAST({c} AS STRING)"


def _hash_arg(col: str, cat: str) -> str:
    return f"coalesce(concat('v', {value_expr(col, cat)}), 'n')"


def _cols(pairs: list[dict], side: str) -> list[tuple[str, str]]:
    return [(p[_SHORT[side]], p["cat"]) for p in pairs]


def _from(entry: dict, part: dict, side: str) -> str:
    tbl = qtable(entry[f"{side}_db"], entry[f"{side}_table"])
    name = part.get(f"{_SHORT[side]}_name")
    return f"{tbl} PARTITION ({qi(name)})" if name else tbl


def _where(flt: dict | None, side: str, *extra: str) -> str:
    conds = []
    if flt and flt.get(side):
        conds.append(f"({flt[side]})")
    conds.extend(e for e in extra if e)
    return " WHERE " + " AND ".join(conds) if conds else ""


def _union(branches: list[str], order_by: str = "") -> str:
    sql = "\nUNION ALL\n".join(branches)
    return sql + (f"\nORDER BY {order_by}" if order_by else "")


def row_count_sql(entry: dict, res: dict, parts: list[dict], side: str) -> str:
    return _union([f"SELECT {i} AS __p, COUNT(*) AS __cnt FROM {_from(entry, p, side)}{_where(res['filter'], side)}"
                   for i, p in enumerate(parts)])


def _metric_agg(m: dict, side: str) -> str:
    c = qi(m[_SHORT[side]])
    return {"SUM": f"SUM({c})", "MIN": f"MIN({c})", "MAX": f"MAX({c})", "COUNT": f"COUNT({c})",
            "COUNT_NULL": f"COUNT(*) - COUNT({c})", "COUNT_DISTINCT": f"COUNT(DISTINCT {c})"}[m["func"]]


def metric_sql(entry: dict, res: dict, parts: list[dict], side: str) -> str:
    groups = _cols(res["group_by"], side)
    sel = [f"{value_expr(c, cat)} AS __g{j}" for j, (c, cat) in enumerate(groups)]
    sel.append("COUNT(*) AS __cnt")
    sel += [f"{_metric_agg(m, side)} AS __m{j}" for j, m in enumerate(res["metrics"])]
    group_by = " GROUP BY " + ", ".join(f"__g{j}" for j in range(len(groups))) if groups else ""
    branches = [f"SELECT {i} AS __p, {', '.join(sel)} FROM {_from(entry, p, side)}"
                f"{_where(res['filter'], side)}{group_by}" for i, p in enumerate(parts)]
    return _union(branches, ", ".join(["__p"] + [f"__g{j}" for j in range(len(groups))]))


def _sample_exprs(res: dict, side: str) -> tuple[str, str, str | None]:
    key_args = ", ".join(_hash_arg(c, cat) for c, cat in _cols(res["keys"], side))
    row_args = ", ".join(_hash_arg(c, cat) for c, cat in _cols(res["keys"] + res["columns"], side))
    bucket = f"abs(xx_hash3_64({key_args}) % {res['buckets']})"
    pred = f"abs(murmur_hash3_32({key_args}) % {SAMPLE_MOD}) < {res['threshold']}" \
        if res["threshold"] < SAMPLE_MOD else None
    return bucket, f"CAST(xx_hash3_64({row_args}) AS LARGEINT)", pred


def sample_summary_sql(entry: dict, res: dict, parts: list[dict], side: str) -> str:
    bucket, row_hash, pred = _sample_exprs(res, side)
    return _union([f"SELECT {i} AS __p, {bucket} AS __b, COUNT(*) AS __cnt, SUM({row_hash}) AS __fp "
                   f"FROM {_from(entry, p, side)}{_where(res['filter'], side, pred)} GROUP BY __b"
                   for i, p in enumerate(parts)])


def sample_drill_sql(entry: dict, res: dict, parts: list[dict], side: str) -> str:
    bucket, _, pred = _sample_exprs(res, side)
    keys = _cols(res["keys"], side)
    sel = [f"{value_expr(c, cat)} AS __k{j}" for j, (c, cat) in enumerate(keys)]
    sel += [f"{value_expr(c, cat)} AS __c{j}" for j, (c, cat) in enumerate(_cols(res["columns"], side))]
    branches = []
    for i, p in enumerate(parts):
        in_buckets = f"{bucket} IN ({', '.join(str(b) for b in p['buckets'])})"
        branches.append(f"SELECT {i} AS __p, {', '.join(sel)} FROM {_from(entry, p, side)}"
                        f"{_where(res['filter'], side, pred, in_buckets)}")
    return _union(branches, ", ".join(["__p"] + [f"__k{j}" for j in range(len(keys))]))


_BUILDERS = {"ROW_COUNT": row_count_sql, "METRIC": metric_sql, "SAMPLE": sample_summary_sql}


def make_task(entry: dict, rule: dict, phase: str, parts: list[dict], builder) -> dict:
    res = rule["resolved"]
    task = {"table_id": entry["table_id"], "rule_key": rule["key"], "rule": rule["name"], "phase": phase,
            "parts": [{k: p[k] for k in ("key", "src_name", "tgt_name", "buckets") if k in p} for p in parts]}
    for side in SIDES:
        task[f"{side}_sql"] = builder(entry, res, parts, side)
    task["task_id"] = hashlib.sha1(json.dumps(task, sort_keys=True).encode()).hexdigest()[:20]
    task["max_rows"] = res["max_groups"] * len(parts) if rule["name"] == "METRIC" and phase == "main" else None
    return task


def batch_partitions(parts: list[dict], max_parts: int, max_rows: int) -> list[list[dict]]:
    batches: list[list[dict]] = []
    cur: list[dict] = []
    rows = 0
    for p in parts:
        if cur and (len(cur) >= max_parts or rows + p["rows"] > max_rows):
            batches.append(cur)
            cur, rows = [], 0
        cur.append(p)
        rows += p["rows"]
    if cur:
        batches.append(cur)
    return batches


def plan_main(entries: list[dict], run: RunSettings) -> list[dict]:
    tasks = []
    for order, entry in enumerate(entries):
        for rule in entry["rules"]:
            if rule["status"] != "READY" or not entry["common_partitions"]:
                continue
            for batch in batch_partitions(entry["common_partitions"], run.max_partitions_per_sql, run.max_rows_per_sql):
                t = make_task(entry, rule, "main", batch, _BUILDERS[rule["name"]])
                t["_order"] = (RULE_ORDER[rule["name"]], order)
                tasks.append(t)
    tasks.sort(key=lambda t: t.pop("_order"))
    return tasks


def plan_drill(entry: dict, rule: dict, main_task: dict, mismatched: dict[int, list[int]]) -> dict:
    parts = [{**main_task["parts"][i], "buckets": sorted(b)} for i, b in sorted(mismatched.items())]
    t = make_task(entry, rule, "drill", parts, sample_drill_sql)
    t["parent_task_id"] = main_task["task_id"]
    return t
