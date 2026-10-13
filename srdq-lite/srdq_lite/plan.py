"""Stage 3: turn resolved rules into batched UNION ALL SQL tasks, plus SAMPLE pick and lookup tasks."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Iterable

from .config import RunSettings
from .sr import qi, qtable

SIDES = ("source", "target")
RULE_ORDER = {"ROW_COUNT": 0, "METRIC": 1, "SAMPLE": 2}
LOOKUP_VALUES_PER_SQL = 10_000
LOOKUP_ROW_FACTOR = 20
_SHORT = {"source": "src", "target": "tgt"}
_NUMERIC_KEY_CATS = {"int", "decimal", "bool"}
_NUMBER_RE = re.compile(r"-?\d+(\.\d+)?")


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


def _sample_select(res: dict, side: str) -> tuple[list[str], str]:
    keys = _cols(res["keys"], side)
    sel = [f"{value_expr(c, cat)} AS __k{j}" for j, (c, cat) in enumerate(keys)]
    sel += [f"{value_expr(c, cat)} AS __c{j}" for j, (c, cat) in enumerate(_cols(res["columns"], side))]
    return sel, ", ".join(["__p"] + [f"__k{j}" for j in range(len(keys))])


def sample_pick_sql(entry: dict, res: dict, parts: list[dict]) -> str:
    sel, order = _sample_select(res, "source")
    return _union([f"SELECT * FROM (SELECT {i} AS __p, {', '.join(sel)} FROM {_from(entry, p, 'source')}"
                   f"{_where(res['filter'], 'source')} LIMIT {p['limit']}) __s{i}" for i, p in enumerate(parts)],
                  order)


def sql_literal(v: str, cat: str) -> str:
    if cat in _NUMERIC_KEY_CATS and _NUMBER_RE.fullmatch(v):
        return v
    return "'" + v.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _key_filter(col: str, cat: str, values: set) -> str:
    c = qi(col)
    conds = []
    lits = sorted(sql_literal(v, cat) for v in values if v is not None)
    if lits:
        conds.append(f"{c} IN ({', '.join(lits)})")
    if None in values:
        conds.append(f"{c} IS NULL")
    return conds[0] if len(conds) == 1 else "(" + " OR ".join(conds) + ")"


def sample_lookup_sql(entry: dict, res: dict, branches: list[dict]) -> str:
    """Each branch filters one partition by IN lists per key column; the superset is narrowed when comparing."""
    sel, order = _sample_select(res, "target")
    keys = _cols(res["keys"], "target")
    out = []
    for b in branches:
        conds = [_key_filter(c, cat, vals) for (c, cat), vals in zip(keys, b["values"])]
        out.append(f"SELECT {b['p']} AS __p, {', '.join(sel)} FROM {_from(entry, b, 'target')}"
                   f"{_where(res['filter'], 'target', *conds)}")
    return _union(out, order)


def task_sides(task: dict) -> list[str]:
    return [s for s in SIDES if task.get(f"{s}_sql")]


def make_task(entry: dict, rule: dict, phase: str, parts: list[dict], sqls: dict[str, str], **extra) -> dict:
    task = {"table_id": entry["table_id"], "rule_key": rule["key"], "rule": rule["name"], "phase": phase,
            "parts": parts, **{f"{s}_sql": sqls.get(s) for s in SIDES}, "max_rows": None, **extra}
    task["task_id"] = hashlib.sha1(json.dumps(task, sort_keys=True).encode()).hexdigest()[:20]
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


def _main_task(entry: dict, rule: dict, batch: list[dict], counts: dict[str, str]) -> dict:
    res = rule["resolved"]
    parts = [{k: p[k] for k in ("key", "src_name", "tgt_name")} for p in batch]
    if rule["name"] == "SAMPLE":
        sql = row_count_sql(entry, {"filter": res["filter"]}, batch, "source")
        if sql in counts:
            return make_task(entry, rule, "main", parts, {}, count_from=counts[sql])
        return make_task(entry, rule, "main", parts, {"source": sql})
    builder = row_count_sql if rule["name"] == "ROW_COUNT" else metric_sql
    sqls = {s: builder(entry, res, batch, s) for s in SIDES}
    if rule["name"] == "METRIC":
        return make_task(entry, rule, "main", parts, sqls, max_rows=res["max_groups"] * len(batch),
                         max_rows_note="max_groups per partition")
    task = make_task(entry, rule, "main", parts, sqls)
    counts.setdefault(sqls["source"], task["task_id"])
    return task


def plan_main(entries: list[dict], run: RunSettings) -> list[dict]:
    """SAMPLE main tasks only count source rows; they reuse a ROW_COUNT task with the same source SQL."""
    tasks = []
    for order, entry in enumerate(entries):
        if not entry["common_partitions"]:
            continue
        batches = batch_partitions(entry["common_partitions"], run.max_partitions_per_sql, run.max_rows_per_sql)
        counts: dict[str, str] = {}
        rules = sorted((r for r in entry["rules"] if r["status"] == "READY"), key=lambda r: RULE_ORDER[r["name"]])
        for rule in rules:
            for batch in batches:
                t = _main_task(entry, rule, batch, counts)
                t["_order"] = (RULE_ORDER[rule["name"]], order)
                tasks.append(t)
    tasks.sort(key=lambda t: t.pop("_order"))
    return tasks


def sample_size(rows: int, ratio: float, max_rows: int) -> int:
    return min(max_rows, max(1, int(rows * ratio + 1e-9)))


def plan_picks(entry: dict, rule: dict, main_task: dict, counts: dict[int, int], max_parts: int) -> list[dict]:
    res = rule["resolved"]
    parts = [{**p, "count": counts[i], "limit": sample_size(counts[i], res["ratio"], res["max_rows"])}
             for i, p in enumerate(main_task["parts"]) if counts.get(i, 0) > 0]
    return [make_task(entry, rule, "pick", chunk, {"source": sample_pick_sql(entry, res, chunk)},
                      parent_task_id=main_task["task_id"])
            for chunk in (parts[j:j + max_parts] for j in range(0, len(parts), max_parts))]


def plan_lookups(entry: dict, rule: dict, pick: dict, rows: Iterable[list], max_parts: int) -> list[dict]:
    """Split picked keys (ordered by partition, key) into target lookups bounded by distinct literal count."""
    res = rule["resolved"]
    nk = len(res["keys"])
    tasks: list[dict] = []
    branches: list[dict] = []
    cur: dict | None = None
    used = 0

    def flush() -> None:
        nonlocal branches, used
        if branches:
            parts = [{k: b[k] for k in ("key", "src_name", "tgt_name", "p", "first", "last", "rows")}
                     for b in branches]
            picked = sum(b["rows"] for b in branches)
            tasks.append(make_task(entry, rule, "lookup", parts,
                                   {"target": sample_lookup_sql(entry, res, branches)},
                                   parent_task_id=pick["task_id"], max_rows=LOOKUP_ROW_FACTOR * picked + 1000,
                                   max_rows_note="key IN-list superset too large"))
        branches, used = [], 0

    def open_branch(p: int, key: list) -> dict:
        if len(branches) >= max_parts:
            flush()
        b = {**pick["parts"][p], "p": p, "values": [set() for _ in range(nk)], "first": key, "last": key, "rows": 0}
        branches.append(b)
        return b

    for row in rows:
        p, key = int(row[0]), row[1:1 + nk]
        same = cur is not None and cur["p"] == p
        added = sum(v not in s for v, s in zip(key, cur["values"])) if same else nk
        if used and used + added > LOOKUP_VALUES_PER_SQL:
            flush()
            same, added = False, nk
        if not same:
            cur = open_branch(p, key)
        for v, s in zip(key, cur["values"]):
            s.add(v)
        cur["last"] = key
        cur["rows"] += 1
        used += added
    flush()
    return tasks
