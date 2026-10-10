"""Difference chains (doc 03 §7.10), derived on read from dq_comparison history."""
from __future__ import annotations

import datetime as _dt
from typing import Any

from .common import KIND_RANK, LAYER_RANK, ApiError, Ctx, as_uuid, iso

STATES = ("NEW", "PERSISTENT", "RESOLVED")

_CHAINS_SQL = """
WITH c AS (
    SELECT id, run_id, run_check_id, asset_id, scope_key, scope_key_digest, rule_key, rule_revision, check_kind,
           evidence, verdict, compared_at, difference_count, difference_count_mode, summary, reason,
           row_number() OVER (PARTITION BY asset_id, scope_key_digest, rule_key, rule_revision
                              ORDER BY compared_at DESC, id DESC) AS rn
    FROM dq_comparison
    WHERE verdict IN ('OBSERVED_EQUAL', 'OBSERVED_DIFFERENT') AND compared_at <= %(as_of)s
), g AS (
    SELECT asset_id, scope_key_digest, rule_key, rule_revision, count(*) AS n,
           min(rn) FILTER (WHERE verdict = 'OBSERVED_EQUAL') AS eq1,
           min(rn) FILTER (WHERE verdict = 'OBSERVED_EQUAL' AND rn > 1) AS eq2
    FROM c GROUP BY 1, 2, 3, 4
), s AS (
    SELECT g.*, l.verdict AS latest_verdict, l.compared_at AS latest_at, p.verdict AS prev_verdict,
           CASE WHEN l.verdict = 'OBSERVED_DIFFERENT' THEN COALESCE(g.eq1 - 1, g.n)
                ELSE COALESCE(g.eq2 - 2, g.n - 1) END AS streak,
           CASE WHEN l.verdict = 'OBSERVED_DIFFERENT' THEN
                     CASE WHEN COALESCE(g.eq1 - 1, g.n) = 1 THEN 'NEW' ELSE 'PERSISTENT' END
                WHEN p.verdict = 'OBSERVED_DIFFERENT' THEN 'RESOLVED' END AS state
    FROM g
    JOIN c l ON l.asset_id = g.asset_id AND l.scope_key_digest = g.scope_key_digest AND l.rule_key = g.rule_key
            AND l.rule_revision = g.rule_revision AND l.rn = 1
    LEFT JOIN c p ON p.asset_id = g.asset_id AND p.scope_key_digest = g.scope_key_digest AND p.rule_key = g.rule_key
            AND p.rule_revision = g.rule_revision AND p.rn = 2
)
SELECT s.state, s.streak, s.latest_at, s.latest_verdict,
       d.id AS comparison_id, d.run_id, d.run_check_id, d.asset_id, d.scope_key, d.rule_key, d.rule_revision,
       d.check_kind, d.evidence, d.compared_at AS last_different_at, d.difference_count, d.difference_count_mode,
       d.summary, d.reason, f.compared_at AS first_seen,
       a.layer, a.source_db, a.source_table, a.target_db, a.target_table,
       rc.execution_snapshot->>'scope_label' AS scope_label,
       count(*) OVER () AS total
FROM s
JOIN c d ON d.asset_id = s.asset_id AND d.scope_key_digest = s.scope_key_digest AND d.rule_key = s.rule_key
        AND d.rule_revision = s.rule_revision
        AND d.rn = CASE WHEN s.state = 'RESOLVED' THEN 2 ELSE 1 END
JOIN c f ON f.asset_id = s.asset_id AND f.scope_key_digest = s.scope_key_digest AND f.rule_key = s.rule_key
        AND f.rule_revision = s.rule_revision
        AND f.rn = CASE WHEN s.state = 'RESOLVED' THEN s.streak + 1 ELSE s.streak END
JOIN dq_asset a ON a.id = s.asset_id
LEFT JOIN dq_run_check rc ON rc.id = d.run_check_id
WHERE s.state IS NOT NULL {where}
ORDER BY {kind_order}, {layer_order}, a.source_db, a.source_table, d.scope_key, d.rule_key, d.rule_revision
LIMIT %(limit)s OFFSET %(offset)s
"""


def _case(col: str, ranks: dict) -> str:
    return "CASE " + col + " " + " ".join(f"WHEN '{k}' THEN {v}" for k, v in ranks.items()) + " ELSE 99 END"


def chains(cur, *, as_of: _dt.datetime, resolved_since: _dt.datetime | None = None, kind: str | None = None,
           layer: str | None = None, table: str | None = None, state: str | None = None, asset_id=None,
           limit: int = 100, offset: int = 0) -> tuple[int, list[dict]]:
    """Chains whose state is NEW / PERSISTENT / RESOLVED as of `as_of`.

    RESOLVED chains are listed only when the resolving comparison is after `resolved_since` (the report interval);
    without it they are listed only if resolved at all (dashboard view)."""
    where, params = [], {"as_of": as_of, "limit": int(limit), "offset": int(offset)}
    if kind is not None:
        if kind not in KIND_RANK:
            raise ApiError(422, "BAD_KIND", f"kind must be one of {list(KIND_RANK)}")
        where.append("d.check_kind = %(kind)s")
        params["kind"] = kind
    if layer is not None:
        if layer not in LAYER_RANK:
            raise ApiError(422, "BAD_LAYER", f"layer must be one of {list(LAYER_RANK)}")
        where.append("a.layer = %(layer)s")
        params["layer"] = layer
    if table:
        where.append("(a.source_db || '.' || a.source_table ILIKE %(table)s "
                     "OR a.target_db || '.' || a.target_table ILIKE %(table)s)")
        params["table"] = "%" + str(table).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
    if asset_id is not None:
        where.append("s.asset_id = %(asset_id)s")
        params["asset_id"] = as_uuid(asset_id, "asset_id")
    if state is not None:
        if state not in STATES:
            raise ApiError(422, "BAD_STATE", f"state must be one of {STATES}")
        where.append("s.state = %(state)s")
        params["state"] = state
    if resolved_since is not None:
        where.append("(s.state <> 'RESOLVED' OR s.latest_at > %(resolved_since)s)")
        params["resolved_since"] = resolved_since
    sql = _CHAINS_SQL.format(where="".join(" AND " + w for w in where), kind_order=_case("d.check_kind", KIND_RANK),
                             layer_order=_case("a.layer", LAYER_RANK))
    cur.execute(sql, params)
    rows = cur.fetchall()
    total = int(rows[0]["total"]) if rows else _count(cur, sql, params, offset)
    types = _type_counts(cur, [r["comparison_id"] for r in rows])
    return total, [_chain_out(r, types.get(r["comparison_id"], {})) for r in rows]


def _count(cur, sql: str, params: dict, offset: int) -> int:
    if offset <= 0:
        return 0
    cur.execute(sql, dict(params, limit=1, offset=0))
    r = cur.fetchone()
    return 0 if r is None else int(r["total"])


def _type_counts(cur, cids: list) -> dict:
    if not cids:
        return {}
    cur.execute("""SELECT comparison_id, type, count(*) AS n FROM dq_difference WHERE comparison_id = ANY(%s)
                   GROUP BY 1, 2""", (cids,))
    out: dict = {}
    for r in cur.fetchall():
        out.setdefault(r["comparison_id"], {})[r["type"]] = r["n"]
    return out


def diff_summary(r: dict, types: dict) -> str:
    s = r["summary"] or {}
    k = r["check_kind"]
    if k == "COUNT":
        try:
            delta = int(s.get("target_rows")) - int(s.get("source_rows"))
            return f"source {s.get('source_rows')} rows, target {s.get('target_rows')} rows (delta {delta:+d})"
        except (TypeError, ValueError):
            return f"{r['difference_count']} difference(s)"
    parts = [f"{t}={n}" for t, n in sorted(types.items())]
    mode = "" if r["difference_count_mode"] == "EXACT" else f" ({r['difference_count_mode']})"
    head = f"{r['difference_count']} difference(s){mode}"
    if k == "METRIC" and "source_groups" in s:
        head += f"; groups source {s.get('source_groups')} / target {s.get('target_groups')}"
    return head + (": " + ", ".join(parts) if parts else "")


def _chain_out(r: dict, types: dict) -> dict:
    return {"state": r["state"], "consecutive_different": int(r["streak"]), "first_seen": iso(r["first_seen"]),
            "latest_at": iso(r["latest_at"]), "latest_verdict": r["latest_verdict"],
            "last_different_at": iso(r["last_different_at"]), "comparison_id": str(r["comparison_id"]),
            "run_id": str(r["run_id"]), "run_check_id": str(r["run_check_id"]), "asset_id": str(r["asset_id"]),
            "layer": r["layer"], "source_table": f"{r['source_db']}.{r['source_table']}",
            "target_table": f"{r['target_db']}.{r['target_table']}", "scope_key": r["scope_key"],
            "scope_label": r["scope_label"] or r["scope_key"], "rule_key": r["rule_key"],
            "rule_revision": r["rule_revision"], "check_kind": r["check_kind"], "evidence": r["evidence"],
            "difference_count": int(r["difference_count"]), "difference_count_mode": r["difference_count_mode"],
            "difference_types": types, "summary": diff_summary(r, types)}


def list_differences(ctx: Ctx, params: dict[str, Any]) -> dict:
    """GET /v1/differences: filters kind / layer / table / state / asset_id; page + page_size."""
    max_size = int(ctx.cfg["web"]["max_page_size"])
    try:
        size = min(max(1, int(params.get("page_size") or 50)), max_size)
        page = max(1, int(params.get("page") or 1))
    except (TypeError, ValueError) as e:
        raise ApiError(422, "BAD_PAGE", "page and page_size must be integers") from e
    from .reports import last_daily_end

    with ctx.db.tx() as cur:
        since = None if params.get("state") else last_daily_end(cur)
        total, items = chains(cur, as_of=ctx.now(), resolved_since=since, kind=params.get("kind") or None,
                              layer=params.get("layer") or None, table=params.get("table") or None,
                              state=params.get("state") or None, asset_id=params.get("asset_id") or None,
                              limit=size, offset=(page - 1) * size)
    return {"total": total, "page": page, "page_size": size, "resolved_since": iso(since), "items": items}
