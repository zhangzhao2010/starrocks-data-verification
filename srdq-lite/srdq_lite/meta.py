"""Stage 1 (connectivity) and stage 2 (metadata, partitions, rule resolution)."""
from __future__ import annotations

import json
import re
import time
from typing import Any

from .config import SAMPLE_MOD, Cluster, Rule, RunSettings, TableSpec
from .sr import Connection, SRError

_RANGE_KEYS_RE = re.compile(r"keys:\s*\[(.*?)\];")
_INT_TYPES = {"tinyint", "smallint", "int", "integer", "bigint", "largeint"}
_STRING_TYPES = {"char", "varchar", "string", "text"}
_UNSUPPORTED_TYPES = {"hll", "bitmap", "percentile", "object", "unknown"}
_NUMERIC = {"int", "decimal", "float", "bool"}
_MODEL_ALIASES = {"PRI_KEYS": "PRIMARY_KEYS", "UNQ_KEYS": "UNIQUE_KEYS", "DUP_KEYS": "DUPLICATE_KEYS",
                  "AGG_KEYS": "AGGREGATE_KEYS"}
WHOLE_TABLE = "__ALL__"


def check_connectivity(cluster: Cluster, run: RunSettings, session: dict) -> dict:
    t0 = time.time()
    try:
        conn = Connection(cluster, run, session)
        try:
            version = conn.query("SELECT current_version()")[0][0]
            user = conn.query("SELECT current_user()")[0][0]
        finally:
            conn.close()
        return {"side": cluster.side, **cluster.describe(), "ok": True, "version": version, "current_user": user,
                "elapsed_s": round(time.time() - t0, 3)}
    except SRError as e:
        return {"side": cluster.side, **cluster.describe(), "ok": False, "error": str(e),
                "elapsed_s": round(time.time() - t0, 3)}


def category(data_type: str, column_type: str) -> str:
    t = (data_type or "").lower()
    ct = (column_type or "").lower()
    if t in _INT_TYPES:
        return "int"
    if t.startswith("decimal"):
        return "decimal"
    if t in ("float", "double"):
        return "float"
    if t in _STRING_TYPES:
        return "string"
    if t in ("boolean", "bool"):
        return "bool"
    if t == "date":
        return "date"
    if t in ("datetime", "timestamp"):
        return "datetime"
    if t == "json":
        return "json"
    if t in ("binary", "varbinary"):
        return "binary"
    if t in ("array", "map", "struct") or ct.startswith(("array", "map", "struct")):
        return "complex"
    if t in _UNSUPPORTED_TYPES:
        return "unsupported"
    return t or "unknown"


def normalize_partition_value(raw: str) -> str:
    raw = (raw or "").strip()
    keys = _RANGE_KEYS_RE.findall(raw)
    if len(keys) == 2:
        return f"[{keys[0].strip()}, {keys[1].strip()})"
    try:
        vals = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return raw
    if isinstance(vals, list):
        items = sorted(json.dumps(v[0] if isinstance(v, list) and len(v) == 1 else v, ensure_ascii=False)
                       for v in vals)
        return "IN (" + ", ".join(items) + ")"
    return raw


def _split_keys(text: str) -> list[str]:
    return [k.strip().strip("`") for k in (text or "").split(",") if k.strip()]


def fetch_table_meta(conn: Connection, db: str, table: str) -> dict:
    cols = conn.query("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE FROM information_schema.columns "
                      "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s ORDER BY ORDINAL_POSITION", [db, table])
    if not cols:
        return {"exists": False}
    columns = [{"name": r[0], "data_type": str(r[1]).lower(), "column_type": str(r[2]).lower(),
                "nullable": str(r[3]).upper() == "YES", "category": category(str(r[1]), str(r[2]))} for r in cols]
    cfg = conn.query("SELECT TABLE_MODEL, PRIMARY_KEY, PARTITION_KEY, DISTRIBUTE_BUCKET FROM "
                     "information_schema.tables_config WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s", [db, table])
    model, keys, pkeys, buckets = None, [], [], None
    if cfg:
        raw_model = str(cfg[0][0] or "").upper()
        model = _MODEL_ALIASES.get(raw_model, raw_model) or None
        keys, pkeys, buckets = _split_keys(cfg[0][1]), _split_keys(cfg[0][2]), cfg[0][3]
    parts = conn.query("SELECT PARTITION_NAME, PARTITION_VALUE, ROW_COUNT FROM information_schema.partitions_meta "
                       "WHERE DB_NAME = %s AND TABLE_NAME = %s AND IS_TEMP = 0", [db, table]) if cfg else []
    rows_total = sum(int(r[2] or 0) for r in parts)
    partitions = []
    if pkeys:
        partitions = sorted(({"name": r[0], "key": normalize_partition_value(r[1]), "rows": int(r[2] or 0)}
                             for r in parts if not str(r[0]).startswith("$")), key=lambda p: p["name"])
    return {"exists": True, "model": model, "keys": keys, "partition_columns": pkeys, "buckets": buckets,
            "columns": columns, "partitions": partitions, "rows": rows_total}


class RuleProblem(Exception):
    def __init__(self, status: str, reason: str):
        super().__init__(reason)
        self.status = status
        self.reason = reason


def build_entry(spec: TableSpec, src: dict | None, tgt: dict | None, error: str | None = None) -> dict:
    entry: dict[str, Any] = {
        "table_id": spec.id, "source_db": spec.source_db, "source_table": spec.source_table,
        "target_db": spec.target_db, "target_table": spec.target_table, "issues": [],
        "partitions_src": 0, "partitions_tgt": 0, "partitioned": False, "common_partitions": [],
        "source_only_partitions": [], "target_only_partitions": [], "rules": [],
    }
    issues = entry["issues"]

    def blocked_rules(reason: str) -> list[dict]:
        return [{"index": r.index, "name": r.name, "key": r.key, "status": "BLOCKED", "reason": reason}
                for r in spec.rules]

    if error is not None:
        entry.update(meta_status="ERROR", rules=blocked_rules(error))
        issues.append({"level": "ERROR", "kind": "METADATA_QUERY_FAILED", "message": error})
        return entry
    assert src is not None and tgt is not None
    missing = [side for side, m in (("source", src), ("target", tgt)) if not m.get("exists")]
    if missing:
        for side in missing:
            name = f"{spec.source_db}.{spec.source_table}" if side == "source" else f"{spec.target_db}.{spec.target_table}"
            issues.append({"level": "BLOCKED", "kind": "TABLE_NOT_FOUND", "message": f"{side} table {name} not found"})
        entry.update(meta_status="NOT_FOUND", rules=blocked_rules("table not found"))
        return entry

    entry.update(src_model=src["model"], tgt_model=tgt["model"], src_keys=src["keys"], tgt_keys=tgt["keys"])
    src_cols = {c["name"].lower(): c for c in src["columns"]}
    tgt_cols = {c["name"].lower(): c for c in tgt["columns"]}
    mapping = spec.column_mapping
    mapped_targets = set()
    for c in src["columns"]:
        tname = mapping.get(c["name"].lower(), c["name"])
        tc = tgt_cols.get(tname.lower())
        if tc is None:
            issues.append({"level": "WARN", "kind": "COLUMN_MISSING_IN_TARGET",
                           "message": f"column {c['name']} (target {tname}) not found in target"})
            continue
        mapped_targets.add(tc["name"].lower())
        if c["category"] != tc["category"]:
            issues.append({"level": "WARN", "kind": "COLUMN_TYPE_CATEGORY_DIFF",
                           "message": f"{c['name']}: {c['column_type']} vs {tc['column_type']}"})
        elif c["column_type"] != tc["column_type"]:
            issues.append({"level": "WARN", "kind": "COLUMN_TYPE_DIFF",
                           "message": f"{c['name']}: {c['column_type']} vs {tc['column_type']}"})
        if c["nullable"] != tc["nullable"]:
            issues.append({"level": "INFO", "kind": "COLUMN_NULLABLE_DIFF",
                           "message": f"{c['name']}: nullable {c['nullable']} vs {tc['nullable']}"})
    for c in tgt["columns"]:
        if c["name"].lower() not in mapped_targets:
            issues.append({"level": "WARN", "kind": "COLUMN_ONLY_IN_TARGET", "message": f"column {c['name']}"})
    src_order = [mapping.get(c["name"].lower(), c["name"]).lower() for c in src["columns"]]
    tgt_order = [c["name"].lower() for c in tgt["columns"]]
    if sorted(src_order) == sorted(tgt_order) and src_order != tgt_order:
        issues.append({"level": "WARN", "kind": "COLUMN_ORDER_DIFF", "message": "column order differs"})
    if src["model"] != tgt["model"]:
        issues.append({"level": "WARN", "kind": "MODEL_DIFF", "message": f"{src['model']} vs {tgt['model']}"})
    src_keys_mapped = [mapping.get(k.lower(), k).lower() for k in src["keys"]]
    if src_keys_mapped != [k.lower() for k in tgt["keys"]]:
        issues.append({"level": "WARN", "kind": "KEY_DIFF", "message": f"{src['keys']} vs {tgt['keys']}"})
    if src["buckets"] != tgt["buckets"]:
        issues.append({"level": "INFO", "kind": "BUCKETS_DIFF", "message": f"{src['buckets']} vs {tgt['buckets']}"})

    _align_partitions(entry, spec, src, tgt)

    def col(name: str, purpose: str) -> dict:
        sc = src_cols.get(name.lower())
        if sc is None:
            raise RuleProblem("BLOCKED", f"{purpose} column {name} not found in source")
        tname = mapping.get(sc["name"].lower(), sc["name"])
        tc = tgt_cols.get(tname.lower())
        if tc is None:
            raise RuleProblem("BLOCKED", f"{purpose} column {name} (target {tname}) not found in target")
        if sc["category"] != tc["category"]:
            raise RuleProblem("BLOCKED", f"{purpose} column {name}: {sc['column_type']} vs {tc['column_type']}")
        return {"src": sc["name"], "tgt": tc["name"], "cat": sc["category"]}

    for rule in spec.rules:
        r = {"index": rule.index, "name": rule.name, "key": rule.key, "status": "READY", "reason": None}
        try:
            if rule.name == "ROW_COUNT":
                r["resolved"] = {"filter": rule.config["filter"]}
            elif rule.name == "METRIC":
                r["resolved"] = _resolve_metric(rule, col)
            else:
                r["resolved"] = _resolve_sample(rule, src, col, issues)
        except RuleProblem as p:
            r.update(status=p.status, reason=p.reason)
            if p.status == "BLOCKED":
                issues.append({"level": "BLOCKED", "kind": "RULE_BLOCKED", "message": f"rule {rule.key}: {p.reason}"})
        entry["rules"].append(r)

    levels = {i["level"] for i in issues}
    entry["meta_status"] = "BLOCKED" if "BLOCKED" in levels else "WARN" if "WARN" in levels else "OK"
    return entry


def _align_partitions(entry: dict, spec: TableSpec, src: dict, tgt: dict) -> None:
    entry["partitions_src"] = len(src["partitions"])
    entry["partitions_tgt"] = len(tgt["partitions"])
    mapping = spec.column_mapping
    src_pcols = [mapping.get(c.lower(), c).lower() for c in src["partition_columns"]]
    same_scheme = bool(src_pcols) and src_pcols == [c.lower() for c in tgt["partition_columns"]]
    if not same_scheme:
        if src["partition_columns"] or tgt["partition_columns"]:
            entry["issues"].append({"level": "WARN", "kind": "PARTITION_SCHEME_DIFF",
                                    "message": f"partition columns {src['partition_columns']} vs "
                                               f"{tgt['partition_columns']}; comparing whole table"})
        entry["common_partitions"] = [{"key": WHOLE_TABLE, "src_name": None, "tgt_name": None,
                                       "rows": max(src["rows"], tgt["rows"])}]
        return
    entry["partitioned"] = True
    tgt_by_key = {p["key"]: p for p in tgt["partitions"]}
    src_keys = set()
    for p in src["partitions"]:
        src_keys.add(p["key"])
        t = tgt_by_key.get(p["key"])
        if t is None:
            entry["source_only_partitions"].append({"key": p["key"], "name": p["name"], "rows": p["rows"]})
        else:
            entry["common_partitions"].append({"key": p["key"], "src_name": p["name"], "tgt_name": t["name"],
                                               "rows": max(p["rows"], t["rows"])})
    entry["target_only_partitions"] = [{"key": p["key"], "name": p["name"], "rows": p["rows"]}
                                       for p in tgt["partitions"] if p["key"] not in src_keys]


def _resolve_metric(rule: Rule, col) -> dict:
    cfg = rule.config
    metrics = []
    for m in cfg["metrics"]:
        c = col(m["column"], "metric")
        func = m["func"]
        if func == "SUM" and c["cat"] not in _NUMERIC:
            raise RuleProblem("BLOCKED", f"SUM({m['column']}) needs a numeric column, got {c['cat']}")
        if func not in ("COUNT", "COUNT_NULL") and c["cat"] in ("complex", "unsupported", "json"):
            raise RuleProblem("BLOCKED", f"{func}({m['column']}) is not supported for {c['cat']} columns")
        tol = m["tolerance"]
        if tol is None:
            tol = 1e-9 if func == "SUM" and c["cat"] == "float" else 0.0
        numeric = func in ("SUM", "COUNT", "COUNT_NULL", "COUNT_DISTINCT") or c["cat"] in _NUMERIC
        metrics.append({**c, "func": func, "label": f"{func}({m['column']})", "tolerance": tol, "numeric": numeric})
    group_by = []
    for g in cfg["group_by"]:
        c = col(g, "group_by")
        if c["cat"] in ("complex", "unsupported", "json"):
            raise RuleProblem("BLOCKED", f"group_by column {g} has unsupported type {c['cat']}")
        group_by.append(c)
    return {"metrics": metrics, "group_by": group_by, "filter": cfg["filter"], "max_groups": cfg["max_groups"]}


def _resolve_sample(rule: Rule, src: dict, col, issues: list) -> dict:
    cfg = rule.config
    if src["model"] not in ("PRIMARY_KEYS", "UNIQUE_KEYS"):
        raise RuleProblem("SKIPPED", f"SAMPLE needs a PRIMARY/UNIQUE KEY table, source is {src['model']}")
    table_keys = [k.lower() for k in src["keys"]]
    if cfg["key"] is not None and [k.lower() for k in cfg["key"]] != table_keys:
        raise RuleProblem("SKIPPED", f"SAMPLE key {cfg['key']} must equal the table key {src['keys']}")
    keys = [col(k, "key") for k in src["keys"]]
    key_names = {k["src"].lower() for k in keys}
    exclude = {c.lower() for c in cfg["exclude_columns"]}
    columns = []
    if cfg["columns"] == "*":
        for c in src["columns"]:
            lname = c["name"].lower()
            if lname in key_names or lname in exclude:
                continue
            if c["category"] == "unsupported":
                issues.append({"level": "INFO", "kind": "SAMPLE_COLUMN_EXCLUDED",
                               "message": f"rule {rule.key}: {c['name']} ({c['column_type']}) cannot be compared"})
                continue
            try:
                columns.append(col(c["name"], "sample"))
            except RuleProblem as p:
                issues.append({"level": "WARN", "kind": "SAMPLE_COLUMN_EXCLUDED",
                               "message": f"rule {rule.key}: {p.reason}"})
    else:
        for name in cfg["columns"]:
            if name.lower() in key_names or name.lower() in exclude:
                continue
            c = col(name, "sample")
            if c["cat"] == "unsupported":
                raise RuleProblem("BLOCKED", f"sample column {name} has an incomparable type")
            columns.append(c)
    return {"keys": keys, "columns": columns, "ratio": cfg["ratio"],
            "threshold": int(round(cfg["ratio"] * SAMPLE_MOD)), "buckets": cfg["buckets"], "filter": cfg["filter"]}
