"""Compile frozen work specs into StarRocks SQL. Identifiers come from approved metadata, values are bound."""
from __future__ import annotations

from typing import Any

from ..common import canonical as c
from ..common import dsl

KEY_HEX_PREFIX = (c.KEY_MAGIC + (1).to_bytes(4, "big")).hex()
_TAG_HEX = {"INTEGER": "0002", "STRING": "0004"}


def q(name: str) -> str:
    return "`" + dsl.check_ident(name) + "`"


def table_ref(table: dict) -> str:
    return f"{q(table['db'])}.{q(table['table'])}"


def _where(predicate: Any, extra: tuple[str, list] | None = None) -> tuple[str, list]:
    sql, params = dsl.compile_scope(predicate or [], q)
    if extra and extra[0]:
        sql = f"({sql}) AND ({extra[0]})"
        params = params + extra[1]
    return sql, params


def compile_count(table: dict, predicate: Any) -> tuple[str, list]:
    where, params = _where(predicate)
    return f"SELECT COUNT(*) AS row_count FROM {table_ref(table)} WHERE {where}", params


def metric_layout(rule: dict) -> list[dict]:
    """Ordered output columns after the group columns: rows, then per-metric guard/value columns."""
    layout = [{"role": "rows"}]
    for m in rule["metrics"]:
        fn = m["function"]
        if fn == "COUNT_ROWS":
            continue
        layout.append({"role": "nn", "metric": m["id"], "column": m["column"]})
        if fn == "SUM":
            layout.append({"role": "sum", "metric": m["id"], "column": m["column"]})
    return layout


def compile_metric(table: dict, predicate: Any, rule: dict, columns: dict) -> tuple[str, list]:
    group = rule.get("group_by") or []
    sel = [f"{q(g)} AS g{i}" for i, g in enumerate(group)]
    for i, item in enumerate(metric_layout(rule)):
        if item["role"] == "rows":
            sel.append(f"COUNT(*) AS r{i}")
        elif item["role"] == "nn":
            sel.append(f"COUNT({q(item['column'])}) AS r{i}")
        else:
            col = q(item["column"])
            expr = f"CAST({col} AS LARGEINT)" if columns[item["column"]]["type"] == "INTEGER" else col
            sel.append(f"SUM({expr}) AS r{i}")
    filt = rule.get("filter") or {"op": "TRUE"}
    where, params = _where(predicate, dsl.compile_filter(filt, columns, q))
    sql = f"SELECT {', '.join(sel)} FROM {table_ref(table)} WHERE {where}"
    if group:
        sql += " GROUP BY " + ", ".join(q(g) for g in group)
    return sql, params


def s1_key_hex_sql(col_sql: str, key_type: str) -> str:
    tag = _TAG_HEX[key_type]
    payload = f"CAST({col_sql} AS VARCHAR)" if key_type == "INTEGER" else col_sql
    return (f"concat('{KEY_HEX_PREFIX}', '{tag}', '01', lpad(lower(hex(length({payload}))), 16, '0'), "
            f"lower(hex({payload})))")


def s1_hash_sql(key_hex_sql: str, seed_hex: str) -> str:
    c.check_seed(seed_hex)
    return f"sha2(concat('SRDQ-SAMPLE-1|', '{seed_hex}', '|', {key_hex_sql}), 256)"


def s1_bucket_sql(h: str, bucket_count: int) -> str:
    """u mod n computed as (hi mod n * (2^32 mod n) + lo mod n) mod n, so no 64-bit unsigned type is needed."""
    n = int(bucket_count)
    mult = (1 << 32) % n
    hi = f"mod(CAST(conv(substr({h}, 1, 8), 16, 10) AS BIGINT), {n})"
    lo = f"mod(CAST(conv(substr({h}, 9, 8), 16, 10) AS BIGINT), {n})"
    return f"mod({hi} * {mult} + {lo}, {n})"


def compile_sample(table: dict, predicate: Any, sample: dict, select_cols: list[str]) -> tuple[str, list]:
    key = sample["key_column"]
    inner_cols = ", ".join(q(cname) for cname in select_cols)
    h = s1_hash_sql(s1_key_hex_sql(q(key), sample["key_type"]), sample["seed_hex"])
    where, params = _where(predicate)
    outer = ", ".join(q(cname) for cname in select_cols)
    sql = (f"SELECT {outer} FROM (SELECT {inner_cols}, {h} AS `__srdq_h` FROM {table_ref(table)} "
           f"WHERE {where}) `__srdq_s` "
           f"WHERE {s1_bucket_sql('`__srdq_h`', sample['bucket_count'])} = {int(sample['selected_bucket'])}")
    return sql, params


def compile_rows(table: dict, predicate: Any, select_cols: list[str]) -> tuple[str, list]:
    where, params = _where(predicate)
    return f"SELECT {', '.join(q(x) for x in select_cols)} FROM {table_ref(table)} WHERE {where}", params


def compile_s1_probe(key_type: str, value: Any, seed_hex: str, bucket_count: int = 1000) -> tuple[str, list]:
    """Capability probe on a literal (no business rows): returns key_hex, sha, bucket for one vector."""
    if key_type == "INTEGER":
        lit = f"CAST({int(c.canonical_int_text(value))} AS BIGINT)"
        params: list = []
    else:
        lit = "%s"
        params = [value]
    kh = s1_key_hex_sql("`__srdq_v`", key_type)
    h = s1_hash_sql("`__srdq_k`", seed_hex)
    sql = (f"SELECT `__srdq_k`, {h} AS `__srdq_h`, {s1_bucket_sql(h, bucket_count)} AS `__srdq_b` "
           f"FROM (SELECT {kh} AS `__srdq_k` FROM (SELECT {lit} AS `__srdq_v`) `__srdq_l`) `__srdq_p`")
    return sql, params
