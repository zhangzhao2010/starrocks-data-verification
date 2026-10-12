"""Read-only queries behind the dashboard, list pages and GET endpoints. No writes, no locks."""
from __future__ import annotations

import datetime as _dt
import decimal
import uuid
from typing import Any

from ..config import POOL_KINDS
from . import window
from .common import KIND_RANK, LAYER_RANK, ApiError, Ctx, as_uuid, control, iso

TERMINAL_RUN = ("COMPLETED", "FAILED", "CANCELLED")
COVERAGE_STATES = ("PENDING", "COMPLETED_EQUAL", "COMPLETED_DIFFERENT", "INCONCLUSIVE", "ERROR", "UNSUPPORTED",
                   "BLOCKED", "EXCLUDED")
MASK = "••••"
VALUE_FIELDS = ("group_values", "key_values", "source_value", "target_value", "key_hex")


def clean(v: Any) -> Any:
    if isinstance(v, dict):
        return {k: clean(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [clean(x) for x in v]
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, (bytes, memoryview)):
        return bytes(v).hex()
    if isinstance(v, _dt.datetime):
        return v.isoformat()
    if isinstance(v, _dt.date):
        return v.isoformat()
    if isinstance(v, decimal.Decimal):
        return str(v)
    return v


def page_args(ctx: Ctx, params: dict, default_size: int = 50) -> tuple[int, int, int]:
    max_size = int(ctx.cfg["web"]["max_page_size"])
    try:
        size = min(max(1, int(params.get("page_size") or default_size)), max_size)
        page = max(1, int(params.get("page") or 1))
    except (TypeError, ValueError) as e:
        raise ApiError(422, "BAD_PAGE", "page and page_size must be integers") from e
    return page, size, (page - 1) * size


def _paged(rows: list[dict], page: int, size: int) -> dict:
    total = int(rows[0]["_total"]) if rows else 0
    items = [clean({k: v for k, v in r.items() if k != "_total"}) for r in rows]
    return {"total": total, "page": page, "page_size": size, "pages": max(1, -(-total // size)), "items": items}


def _like(s: str) -> str:
    return "%" + s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"


def _enum(v: Any, allowed, name: str) -> str | None:
    if v in (None, ""):
        return None
    if v not in allowed:
        raise ApiError(422, "BAD_FILTER", f"{name} must be one of {list(allowed)}")
    return v


def _one(ctx: Ctx, sql: str, params: Any, what: str) -> dict:
    row = ctx.db.one(sql, params)
    if row is None:
        raise ApiError(404, "NOT_FOUND", f"{what} not found")
    return row


def mask_difference(d: dict) -> dict:
    out = dict(d)
    for k in VALUE_FIELDS:
        if out.get(k) not in (None, "", [], {}):
            out[k] = MASK
    out["masked"] = True
    return out


# ---- dashboard -----------------------------------------------------------------------------------

def dashboard(ctx: Ctx) -> dict:
    ctl = control(ctx)
    ws = window.window_state(ctx.cfg, ctx.now(), ctl["window_override"])
    db = ctx.db
    incidents = db.query("""SELECT id, kind, cluster_id, severity, message, occurrences, first_seen, last_seen,
                                   run_id, asset_id, check_kind, scope_key, error_code
                            FROM dq_incident WHERE state='OPEN'
                            ORDER BY CASE severity WHEN 'CRITICAL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END, last_seen DESC
                            LIMIT 200""")
    runs = db.query("""SELECT r.id, r.purpose, r.execution_state, r.control_state, r.windows_used, r.created_at,
                              p.name AS plan_name, s.name AS schedule_name
                       FROM dq_run r LEFT JOIN dq_plan p ON p.id = r.plan_id
                       LEFT JOIN dq_schedule s ON s.id = r.schedule_id
                       WHERE r.purpose <> 'METADATA_PREVIEW' AND r.execution_state <> ALL(%s)
                       ORDER BY r.created_at""", (list(TERMINAL_RUN),))
    progress = _run_progress(ctx, [r["id"] for r in runs])
    for r in runs:
        r["progress"] = progress.get(r["id"], [])
    chains = db.query("""SELECT check_kind, state, count(*) AS n FROM (
                             SELECT DISTINCT ON (asset_id, scope_key_digest, rule_key, rule_revision)
                                    check_kind, verdict AS state
                             FROM dq_comparison WHERE verdict IN ('OBSERVED_EQUAL','OBSERVED_DIFFERENT')
                             ORDER BY asset_id, scope_key_digest, rule_key, rule_revision, compared_at DESC, id DESC
                         ) t GROUP BY 1, 2""")
    latest: dict = {}
    for r in chains:
        latest.setdefault(r["check_kind"], {})[r["state"]] = int(r["n"])
    oldest = db.query("""SELECT check_kind, min(t) AS oldest, count(*) AS scopes FROM (
                             SELECT check_kind, max(compared_at) AS t FROM dq_comparison
                             WHERE verdict IN ('OBSERVED_EQUAL','OBSERVED_DIFFERENT')
                             GROUP BY check_kind, asset_id, scope_key_digest, rule_key) x GROUP BY 1""")
    now = ctx.now()
    oldest_out = {r["check_kind"]: {"oldest_verified_at": iso(r["oldest"]), "scopes": int(r["scopes"]),
                                    "age_hours": round((now - r["oldest"]).total_seconds() / 3600, 1)}
                  for r in oldest}
    cancels = {r["cluster_id"]: int(r["n"]) for r in db.query(
        """SELECT cluster_id, count(*) AS n FROM dq_attempt
           WHERE status IN ('CANCEL_REQUESTED','CANCEL_PENDING') GROUP BY 1""")}
    deferred = db.one("SELECT count(*) AS n FROM dq_artifact WHERE purge_state='PURGE_DEFERRED'")["n"]
    gaps = db.one("""SELECT count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM dq_check_rule r WHERE r.asset_id = a.id
                                AND r.check_kind = 'METRIC' AND r.status = 'APPROVED' AND r.enabled)) AS metric,
                            count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM dq_check_rule r WHERE r.asset_id = a.id
                                AND r.check_kind = 'SAMPLE' AND r.status = 'APPROVED' AND r.enabled)) AS sample,
                            count(*) AS assets
                     FROM dq_asset a WHERE a.status = 'ACTIVE'""")
    pool_stats = _cluster_pool_stats(ctx, now)
    clusters = {}
    for cluster_id in ctx.cfg.cluster_ids:
        st = dict((ctl["cluster_state"] or {}).get(cluster_id) or {})
        pools = pool_stats[cluster_id]
        clusters[cluster_id] = {
            "name": ctx.cfg.cluster(cluster_id)["name"],
            "hard_stop": bool(st.get("hard_stop")),
            "triggers": st.get("triggers") or [],
            "observed_at": st.get("observed_at"),
            "running": sum(p["live"] for p in pools.values()),
            "cancel_unconfirmed": cancels.get(cluster_id, 0),
            "pools": pools,
        }
    inc = clean(incidents)
    return {
        "now": iso(now),
        "control": {"status": ctl["status"], "control_generation": str(ctl["control_generation"]),
                    "window_state": ctl["window_state"], "window_key": ctl["window_key"],
                    "window_opened_at": iso(ctl["window_opened_at"]), "window_closed_at": iso(ctl["window_closed_at"]),
                    "window_override": ctl["window_override"], "transfer_approval_ref": ctl["transfer_approval_ref"],
                    "last_report_window_key": ctl["last_report_window_key"],
                    "last_purge_date": clean(ctl["last_purge_date"]), "unknown_periods": ctl["unknown_periods"],
                    "revision": ctl["revision"], "run_enabled": bool(ctx.cfg["run_enabled"])},
        "window": clean(ws),
        "clusters": clusters,
        "alerts": [i for i in inc if i["kind"] != "STORAGE_SUSPECT"],
        "storage_suspect": [i for i in inc if i["kind"] == "STORAGE_SUSPECT"],
        "active_runs": clean(runs),
        "latest_verdicts": latest,
        "oldest_verified": oldest_out,
        "purge_deferred": int(deferred),
        "rule_gaps": {"assets": int(gaps["assets"]), "METRIC": int(gaps["metric"]), "SAMPLE": int(gaps["sample"])},
        "workers": list_workers(ctx),
    }


def _cluster_pool_stats(ctx: Ctx, now: _dt.datetime) -> dict:
    ready = ctx.db.query("""SELECT cluster_id,
                               CASE WHEN kind='METADATA' THEN 'METADATA' ELSE check_kind END AS claim_kind,
                               count(*) AS n, min(created_at) AS oldest
                            FROM dq_work_item WHERE status='READY' GROUP BY 1, 2""")
    live = ctx.db.query("""SELECT a.cluster_id,
                              CASE WHEN w.kind='METADATA' THEN 'METADATA' ELSE w.check_kind END AS claim_kind,
                              count(*) AS n
                           FROM dq_attempt a JOIN dq_work_item w ON w.id=a.work_item_id
                           WHERE a.status IN ('LEASED','RUNNING','CANCEL_REQUESTED','CANCEL_PENDING')
                           GROUP BY 1, 2""")
    recent = ctx.db.query("""SELECT a.cluster_id,
                                CASE WHEN w.kind='METADATA' THEN 'METADATA' ELSE w.check_kind END AS claim_kind,
                                count(*) AS completed,
                                count(*) FILTER (WHERE a.status='SUCCEEDED') AS succeeded,
                                count(*) FILTER (WHERE a.error_class='TIMEOUT') AS timed_out,
                                avg(a.duration_ms) FILTER (WHERE a.duration_ms IS NOT NULL) AS avg_ms,
                                percentile_cont(0.5) WITHIN GROUP (ORDER BY a.duration_ms)
                                    FILTER (WHERE a.duration_ms IS NOT NULL) AS p50_ms,
                                percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms)
                                    FILTER (WHERE a.duration_ms IS NOT NULL) AS p95_ms,
                                max(a.duration_ms) AS max_ms
                             FROM dq_attempt a JOIN dq_work_item w ON w.id=a.work_item_id
                             WHERE a.finished_at >= %s AND a.status IN ('SUCCEEDED','FAILED')
                             GROUP BY 1, 2""", (now - _dt.timedelta(hours=1),))
    ready_by = {(r["cluster_id"], r["claim_kind"]): r for r in ready}
    live_by = {(r["cluster_id"], r["claim_kind"]): r for r in live}
    recent_by = {(r["cluster_id"], r["claim_kind"]): r for r in recent}
    out = {}
    for cluster_id in ctx.cfg.cluster_ids:
        pools = {}
        for kind in POOL_KINDS:
            q = ready_by.get((cluster_id, kind))
            active = live_by.get((cluster_id, kind))
            done = recent_by.get((cluster_id, kind))
            queued = int(q["n"]) if q else 0
            running = int(active["n"]) if active else 0
            completed = int(done["completed"]) if done else 0
            success = int(done["succeeded"]) if done else 0
            timed_out = int(done["timed_out"]) if done else 0
            pools[kind] = {
                "limit": ctx.cfg.concurrency(cluster_id, kind),
                "ready": queued,
                "oldest_ready_at": iso(q["oldest"]) if q else None,
                "oldest_ready_age_seconds": max(0, int((now - q["oldest"]).total_seconds())) if q else None,
                "live": running,
                "completed_last_hour": completed,
                "rate_per_hour": completed,
                "avg_ms": round(float(done["avg_ms"]), 1) if done and done["avg_ms"] is not None else None,
                "p50_ms": round(float(done["p50_ms"]), 1) if done and done["p50_ms"] is not None else None,
                "p95_ms": round(float(done["p95_ms"]), 1) if done and done["p95_ms"] is not None else None,
                "max_ms": int(done["max_ms"]) if done and done["max_ms"] is not None else None,
                "success_rate": success / completed if completed else None,
                "timeout_rate": timed_out / completed if completed else None,
                "eta_hours": round((queued + running) / completed, 2) if completed else None,
            }
        out[cluster_id] = pools
    return out


def _run_progress(ctx: Ctx, run_ids: list) -> dict:
    if not run_ids:
        return {}
    rows = ctx.db.query("""SELECT run_id, check_kind, count(*) FILTER (WHERE coverage_state <> 'EXCLUDED') AS total,
                                  count(*) FILTER (WHERE coverage_state NOT IN ('PENDING','EXCLUDED')) AS done,
                                  count(*) FILTER (WHERE coverage_state IN ('COMPLETED_EQUAL','COMPLETED_DIFFERENT'))
                                      AS completed,
                                  count(*) FILTER (WHERE coverage_state = 'EXCLUDED') AS excluded,
                                  count(*) FILTER (WHERE coverage_state = 'COMPLETED_EQUAL') AS equal,
                                  count(*) FILTER (WHERE coverage_state = 'COMPLETED_DIFFERENT') AS different,
                                  count(*) FILTER (WHERE coverage_state IN ('ERROR','INCONCLUSIVE','UNSUPPORTED',
                                                                             'BLOCKED')) AS incomplete
                           FROM dq_run_check WHERE run_id = ANY(%s) GROUP BY 1, 2""", (run_ids,))
    out: dict = {}
    for r in sorted(rows, key=lambda r: KIND_RANK.get(r["check_kind"], 9)):
        out.setdefault(r["run_id"], []).append({k: int(r[k]) if k not in ("run_id", "check_kind") else r[k]
                                                for k in r if k != "run_id"})
    return out


# ---- assets and rules ----------------------------------------------------------------------------

def list_assets(ctx: Ctx, params: dict) -> dict:
    page, size, off = page_args(ctx, params)
    where, args = ["true"], {"limit": size, "offset": off}
    layer = _enum(params.get("layer"), LAYER_RANK, "layer")
    if layer:
        where.append("a.layer = %(layer)s")
        args["layer"] = layer
    status = _enum(params.get("status"), ("ACTIVE", "ARCHIVED"), "status")
    if status:
        where.append("a.status = %(status)s")
        args["status"] = status
    pair_id = _enum(params.get("pair_id"), {p["id"] for p in ctx.cfg["cluster_pairs"]}, "pair_id")
    if pair_id:
        where.append("a.pair_id = %(pair_id)s")
        args["pair_id"] = pair_id
    if params.get("q"):
        where.append("(a.source_db || '.' || a.source_table ILIKE %(q)s OR a.target_db || '.' || a.target_table "
                     "ILIKE %(q)s)")
        args["q"] = _like(str(params["q"]))
    missing = _enum(params.get("missing_kind"), ("METRIC", "SAMPLE"), "missing_kind")
    if missing:
        where.append("a.status = 'ACTIVE' AND NOT EXISTS (SELECT 1 FROM dq_check_rule r WHERE r.asset_id = a.id "
                     "AND r.check_kind = %(missing)s AND r.status = 'APPROVED' AND r.enabled)")
        args["missing"] = missing
    rows = ctx.db.query(f"""SELECT a.id, a.pair_id, a.layer, a.source_db, a.source_table, a.target_db,
                                   a.target_table, a.priority, a.enabled, a.status, a.revision, a.updated_at,
                                   (SELECT count(*) FROM dq_check_rule r WHERE r.asset_id = a.id
                                      AND r.status = 'APPROVED') AS approved_rules,
                                   (SELECT count(*) FROM dq_check_rule r WHERE r.asset_id = a.id
                                      AND r.status = 'DRAFT') AS draft_rules,
                                   count(*) OVER () AS _total
                            FROM dq_asset a WHERE {' AND '.join(where)}
                            ORDER BY CASE a.layer {' '.join(f"WHEN '{k}' THEN {v}" for k, v in LAYER_RANK.items())}
                                     END, a.source_db, a.source_table
                            LIMIT %(limit)s OFFSET %(offset)s""", args)
    return _paged(rows, page, size)


def get_asset(ctx: Ctx, asset_id) -> dict:
    aid = as_uuid(asset_id, "asset_id")
    a = _one(ctx, "SELECT * FROM dq_asset WHERE id=%s", (aid,), "asset")
    out = clean(a)
    out["pair"] = clean(ctx.cfg.pair(a["pair_id"]))
    out["rules"] = asset_rules(ctx, aid)
    out["slot"] = clean(ctx.db.one("SELECT run_id, state, acquired_at FROM dq_asset_slot WHERE asset_id=%s", (aid,)))
    out["scope_entries"] = clean(ctx.db.query(
        """SELECT e.scope_kind, e.scope_key, e.label, e.presence_source, e.presence_target, e.expected_reason,
                  e.status, e.status_reason, e.is_new
           FROM dq_scope_entry e JOIN dq_manifest m ON m.id = e.manifest_id AND m.status = 'CONFIRMED'
           WHERE e.asset_id = %s ORDER BY e.scope_key LIMIT 500""", (aid,)))
    out["policies"] = clean(ctx.db.query(
        """SELECT id, scope_key, node_kind, check_selector, control_state, priority_override, reason, created_at
           FROM dq_scope_policy WHERE asset_id=%s AND status='ACTIVE' ORDER BY created_at""", (aid,)))
    return out


def asset_rules(ctx: Ctx, asset_id) -> list[dict]:
    aid = as_uuid(asset_id, "asset_id")
    rows = ctx.db.query("""SELECT id, rule_key, revision, check_kind, definition, transfer_class, execution_policy,
                                  enabled, status, support_state, support_reason, approval_ref, approved_at,
                                  supersedes_id, created_at
                           FROM dq_check_rule WHERE asset_id=%s
                           ORDER BY CASE check_kind WHEN 'COUNT' THEN 0 WHEN 'METRIC' THEN 1 WHEN 'SAMPLE' THEN 2
                                    ELSE 3 END, rule_key, revision DESC""", (aid,))
    return clean(rows)


def list_rules(ctx: Ctx, params: dict) -> dict:
    page, size, off = page_args(ctx, params)
    where, args = ["true"], {"limit": size, "offset": off}
    kind = _enum(params.get("kind"), KIND_RANK, "kind")
    if kind:
        where.append("r.check_kind = %(kind)s")
        args["kind"] = kind
    status = _enum(params.get("status"), ("DRAFT", "APPROVED", "SUPERSEDED", "ARCHIVED"), "status")
    if status:
        where.append("r.status = %(status)s")
        args["status"] = status
    support = _enum(params.get("support_state"), ("SUPPORTED", "UNSUPPORTED"), "support_state")
    if support:
        where.append("r.support_state = %(support)s")
        args["support"] = support
    if params.get("q"):
        where.append("(a.source_db || '.' || a.source_table ILIKE %(q)s OR r.rule_key ILIKE %(q)s)")
        args["q"] = _like(str(params["q"]))
    rows = ctx.db.query(f"""SELECT r.id, r.asset_id, a.layer, a.source_db, a.source_table, r.rule_key, r.revision,
                                   r.check_kind, r.transfer_class, r.enabled, r.status, r.support_state,
                                   r.support_reason, r.approval_ref, r.created_at, count(*) OVER () AS _total
                            FROM dq_check_rule r JOIN dq_asset a ON a.id = r.asset_id
                            WHERE {' AND '.join(where)}
                            ORDER BY r.status = 'DRAFT' DESC, a.source_db, a.source_table, r.check_kind, r.rule_key,
                                     r.revision DESC
                            LIMIT %(limit)s OFFSET %(offset)s""", args)
    return _paged(rows, page, size)


def list_imports(ctx: Ctx, limit: int = 50) -> list[dict]:
    return clean(ctx.db.query("""SELECT id, kind, file_name, mode, status, summary, reason, created_at, confirmed_at
                                 FROM dq_import_batch ORDER BY created_at DESC LIMIT %s""", (int(limit),)))


def get_import(ctx: Ctx, batch_id) -> dict:
    r = _one(ctx, "SELECT * FROM dq_import_batch WHERE id=%s", (as_uuid(batch_id, "batch_id"),), "import batch")
    out = clean({k: v for k, v in r.items() if k != "preview"})
    out["rows"] = clean((r["preview"] or {}).get("rows", []))
    return out


# ---- manifests -----------------------------------------------------------------------------------

def list_manifests(ctx: Ctx, limit: int = 50) -> list[dict]:
    return clean(ctx.db.query("""SELECT id, version, status, inventory_complete, summary, reason, based_on_id,
                                        discovery_run_id, created_at, confirmed_at
                                 FROM dq_manifest ORDER BY version DESC LIMIT %s""", (int(limit),)))


def get_manifest(ctx: Ctx, manifest_id, params: dict) -> dict:
    mid = as_uuid(manifest_id, "manifest_id")
    m = _one(ctx, """SELECT id, version, status, inventory_complete, summary, reason, based_on_id, discovery_run_id,
                            digest, created_at, confirmed_at FROM dq_manifest WHERE id=%s""", (mid,), "manifest")
    out = clean(m)
    out["scopes"] = manifest_scopes(ctx, mid, params)
    return out


def manifest_scopes(ctx: Ctx, manifest_id, params: dict) -> dict:
    mid = as_uuid(manifest_id, "manifest_id")
    page, size, off = page_args(ctx, params)
    where, args = ["e.manifest_id = %(mid)s"], {"mid": mid, "limit": size, "offset": off}
    reason = _enum(params.get("expected_reason"), ("IN_SCOPE", "SOURCE_ONLY", "TARGET_ONLY", "UNKNOWN",
                                                    "ACCESS_DENIED"), "expected_reason")
    if reason:
        where.append("e.expected_reason = %(reason)s")
        args["reason"] = reason
    if params.get("new_only") in ("1", "true", True):
        where.append("e.is_new")
    if params.get("q"):
        where.append("(a.source_db || '.' || a.source_table ILIKE %(q)s OR e.label ILIKE %(q)s)")
        args["q"] = _like(str(params["q"]))
    rows = ctx.db.query(f"""SELECT e.id, e.asset_id, a.layer, a.source_db, a.source_table, e.scope_kind, e.scope_key,
                                   e.label, e.presence_source, e.presence_target, e.expected_reason, e.status,
                                   e.status_reason, e.is_new, count(*) OVER () AS _total
                            FROM dq_scope_entry e JOIN dq_asset a ON a.id = e.asset_id
                            WHERE {' AND '.join(where)}
                            ORDER BY a.source_db, a.source_table, e.scope_key
                            LIMIT %(limit)s OFFSET %(offset)s""", args)
    return _paged(rows, page, size)


# ---- plans ---------------------------------------------------------------------------------------

def list_plans(ctx: Ctx) -> list[dict]:
    return clean(ctx.db.query("""SELECT p.*, (SELECT count(*) FROM dq_schedule s WHERE s.plan_id = p.id) AS schedules,
                                        (SELECT max(created_at) FROM dq_run r WHERE r.plan_id = p.id) AS last_run_at
                                 FROM dq_plan p ORDER BY p.status, p.name"""))


def get_plan(ctx: Ctx, plan_id) -> dict:
    pid = as_uuid(plan_id, "plan_id")
    out = clean(_one(ctx, "SELECT * FROM dq_plan WHERE id=%s", (pid,), "plan"))
    out["runs"] = clean(ctx.db.query("""SELECT id, purpose, execution_state, control_state, windows_used, created_at,
                                               completed_at FROM dq_run WHERE plan_id=%s
                                        ORDER BY created_at DESC LIMIT 20""", (pid,)))
    out["schedules"] = clean(ctx.db.query("SELECT id, name, kind, status, enabled, next_due_at FROM dq_schedule "
                                          "WHERE plan_id=%s ORDER BY name", (pid,)))
    return out


# ---- runs ----------------------------------------------------------------------------------------

def list_runs(ctx: Ctx, params: dict) -> dict:
    page, size, off = page_args(ctx, params)
    where, args = ["true"], {"limit": size, "offset": off}
    purpose = _enum(params.get("purpose"), ("METADATA_PREVIEW", "COMPARE", "MANUAL"), "purpose")
    if purpose:
        where.append("r.purpose = %(purpose)s")
        args["purpose"] = purpose
    if params.get("active") in ("1", "true", True):
        where.append("r.execution_state NOT IN ('COMPLETED','FAILED','CANCELLED')")
    if params.get("plan_id"):
        where.append("r.plan_id = %(plan_id)s")
        args["plan_id"] = as_uuid(params["plan_id"], "plan_id")
    rows = ctx.db.query(f"""SELECT r.id, r.purpose, r.execution_state, r.control_state, r.revision, r.windows_used,
                                   r.created_at, r.completed_at, r.reason, p.name AS plan_name,
                                   s.name AS schedule_name, count(*) OVER () AS _total
                            FROM dq_run r LEFT JOIN dq_plan p ON p.id = r.plan_id
                            LEFT JOIN dq_schedule s ON s.id = r.schedule_id
                            WHERE {' AND '.join(where)} ORDER BY r.created_at DESC
                            LIMIT %(limit)s OFFSET %(offset)s""", args)
    out = _paged(rows, page, size)
    prog = _run_progress(ctx, [as_uuid(i["id"]) for i in out["items"]])
    for i in out["items"]:
        i["progress"] = prog.get(as_uuid(i["id"]), [])
    return out


def get_run(ctx: Ctx, run_id) -> dict:
    rid = as_uuid(run_id, "run_id")
    r = _one(ctx, """SELECT r.*, p.name AS plan_name, s.name AS schedule_name FROM dq_run r
                     LEFT JOIN dq_plan p ON p.id = r.plan_id LEFT JOIN dq_schedule s ON s.id = r.schedule_id
                     WHERE r.id=%s""", (rid,), "run")
    out = clean({k: v for k, v in r.items() if k != "config_snapshot"})
    out["progress"] = _run_progress(ctx, [rid]).get(rid, [])
    cov = ctx.db.query("SELECT check_kind, coverage_state, count(*) AS n FROM dq_run_check WHERE run_id=%s "
                       "GROUP BY 1, 2", (rid,))
    out["coverage"] = {}
    for c in cov:
        out["coverage"].setdefault(c["check_kind"], {})[c["coverage_state"]] = int(c["n"])
    out["budgets"] = clean(ctx.db.query("""SELECT resource, limit_amount, reserved, consumed, overrun_amount,
                                                  budget_state FROM dq_budget WHERE run_id=%s AND level='RUN'""",
                                        (rid,)))
    out["live_attempts"] = int(ctx.db.one(
        """SELECT count(*) AS n FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
           WHERE w.run_id=%s AND a.status IN ('LEASED','RUNNING','CANCEL_REQUESTED','CANCEL_PENDING')""",
        (rid,))["n"])
    return out


def run_nodes(ctx: Ctx, run_id, params: dict) -> dict:
    """TABLE nodes of a run (one per asset) with coverage counts; with asset_id, every node of that asset."""
    rid = as_uuid(run_id, "run_id")
    page, size, off = page_args(ctx, params)
    args: dict = {"rid": rid, "limit": size, "offset": off}
    if params.get("asset_id"):
        args["aid"] = as_uuid(params["asset_id"], "asset_id")
        rows = ctx.db.query("""SELECT n.id, n.asset_id, n.parent_id, n.kind, n.scope_key, n.label, n.control_state,
                                      n.priority_override, n.revision, a.layer, a.source_db, a.source_table,
                                      count(*) OVER () AS _total
                               FROM dq_task_node n JOIN dq_asset a ON a.id = n.asset_id
                               WHERE n.run_id=%(rid)s AND n.asset_id=%(aid)s
                               ORDER BY CASE n.kind WHEN 'TABLE' THEN 0 WHEN 'PARTITION' THEN 1 ELSE 2 END,
                                        n.scope_key LIMIT %(limit)s OFFSET %(offset)s""", args)
        out = _paged(rows, page, size)
        ids = [as_uuid(i["id"]) for i in out["items"]]
        cov = ctx.db.query("""SELECT node_id AS k, check_kind, coverage_state, count(*) AS n FROM dq_run_check
                              WHERE run_id=%s AND node_id = ANY(%s) GROUP BY 1, 2, 3""", (rid, ids))
        key = "id"
    else:
        layer = _enum(params.get("layer"), LAYER_RANK, "layer")
        cond = ""
        if layer:
            cond += " AND a.layer = %(layer)s"
            args["layer"] = layer
        if params.get("q"):
            cond += " AND a.source_db || '.' || a.source_table ILIKE %(q)s"
            args["q"] = _like(str(params["q"]))
        rows = ctx.db.query(f"""SELECT n.id, n.asset_id, n.parent_id, n.kind, n.scope_key, n.label, n.control_state,
                                       n.priority_override, n.revision, a.layer, a.source_db, a.source_table,
                                       count(*) OVER () AS _total
                                FROM dq_task_node n JOIN dq_asset a ON a.id = n.asset_id
                                WHERE n.run_id=%(rid)s AND n.kind='TABLE' {cond}
                                ORDER BY CASE a.layer {' '.join(f"WHEN '{k}' THEN {v}" for k, v in LAYER_RANK.items())}
                                         END, a.source_db, a.source_table
                                LIMIT %(limit)s OFFSET %(offset)s""", args)
        out = _paged(rows, page, size)
        aids = [as_uuid(i["asset_id"]) for i in out["items"]]
        cov = ctx.db.query("""SELECT asset_id AS k, check_kind, coverage_state, count(*) AS n FROM dq_run_check
                              WHERE run_id=%s AND asset_id = ANY(%s) GROUP BY 1, 2, 3""", (rid, aids))
        key = "asset_id"
    agg: dict = {}
    for c in cov:
        agg.setdefault(str(c["k"]), {}).setdefault(c["check_kind"], {})[c["coverage_state"]] = int(c["n"])
    for i in out["items"]:
        i["coverage"] = agg.get(i[key], {})
    return out


def run_checks(ctx: Ctx, run_id, params: dict) -> dict:
    rid = as_uuid(run_id, "run_id")
    page, size, off = page_args(ctx, params)
    where, args = ["rc.run_id = %(rid)s"], {"rid": rid, "limit": size, "offset": off}
    kind = _enum(params.get("check_kind"), KIND_RANK, "check_kind")
    if kind:
        where.append("rc.check_kind = %(kind)s")
        args["kind"] = kind
    cov = _enum(params.get("coverage_state"), COVERAGE_STATES, "coverage_state")
    if cov:
        where.append("rc.coverage_state = %(cov)s")
        args["cov"] = cov
    if params.get("asset_id"):
        where.append("rc.asset_id = %(aid)s")
        args["aid"] = as_uuid(params["asset_id"], "asset_id")
    if params.get("node_id"):
        where.append("rc.node_id = %(nid)s")
        args["nid"] = as_uuid(params["node_id"], "node_id")
    rows = ctx.db.query(f"""SELECT rc.id, rc.asset_id, rc.node_id, rc.rule_key, rc.rule_revision, rc.check_kind,
                                   rc.scope_key, rc.execution_snapshot->>'scope_label' AS scope_label,
                                   rc.expected_evidence, rc.expected_reason, rc.control_state, rc.execution_state,
                                   rc.coverage_state, rc.coverage_reason, rc.effective_priority, rc.attempts_failed,
                                   rc.current_comparison_id, rc.coverage_updated_at, rc.revision,
                                   a.layer, a.source_db, a.source_table, count(*) OVER () AS _total
                            FROM dq_run_check rc JOIN dq_asset a ON a.id = rc.asset_id
                            WHERE {' AND '.join(where)}
                            ORDER BY rc.kind_rank, rc.layer_rank, a.source_db, a.source_table, rc.scope_key,
                                     rc.rule_key
                            LIMIT %(limit)s OFFSET %(offset)s""", args)
    return _paged(rows, page, size)


# ---- comparisons ---------------------------------------------------------------------------------

def get_comparison(ctx: Ctx, comparison_id) -> dict:
    cid = as_uuid(comparison_id, "comparison_id")
    c = _one(ctx, """SELECT c.*, a.layer, a.source_db, a.source_table, a.target_db, a.target_table,
                            rc.execution_snapshot->>'scope_label' AS scope_label
                     FROM dq_comparison c JOIN dq_asset a ON a.id = c.asset_id
                     LEFT JOIN dq_run_check rc ON rc.id = c.run_check_id WHERE c.id=%s""", (cid,), "comparison")
    out = clean(c)
    out["statement"] = {"common_snapshot": "NONE", "read_mode": c["read_mode"],
                        "note": "两端为在线读取，没有共同快照；差异可能来自在线写入时差"}
    out["side_results"] = clean(ctx.db.query(
        """SELECT id, cluster_id, evidence, row_count, total_bytes, validity, read_started, read_finished,
                  query_ref, accepted_at, raw_purged_at
           FROM dq_side_result WHERE id = ANY(%s) ORDER BY cluster_id""",
        (list(c["source_result_ids"]) + list(c["target_result_ids"]),)))
    out["history"] = clean(ctx.db.query(
        """SELECT id, verdict, difference_count, compared_at, run_id FROM dq_comparison
           WHERE asset_id=%s AND scope_key_digest=%s AND rule_key=%s ORDER BY compared_at DESC LIMIT 20""",
        (c["asset_id"], c["scope_key_digest"], c["rule_key"])))
    return out


def comparison_differences(ctx: Ctx, comparison_id, params: dict) -> dict:
    cid = as_uuid(comparison_id, "comparison_id")
    _one(ctx, "SELECT id FROM dq_comparison WHERE id=%s", (cid,), "comparison")
    page, size, off = page_args(ctx, params)
    args: dict = {"cid": cid, "limit": size, "offset": off}
    cond = ""
    if params.get("type"):
        cond = " AND type = %(type)s"
        args["type"] = str(params["type"])
    rows = ctx.db.query(f"""SELECT id, ordinal, type, group_values, metric_id, key_hex, key_values, column_name,
                                   source_value, target_value, detail, count(*) OVER () AS _total
                            FROM dq_difference WHERE comparison_id=%(cid)s {cond}
                            ORDER BY ordinal LIMIT %(limit)s OFFSET %(offset)s""", args)
    out = _paged(rows, page, size)
    show = str(params.get("show_values") or "") in ("1", "true")
    masked = bool(ctx.cfg["web"]["default_masked_values"]) and not show
    if masked:
        out["items"] = [mask_difference(d) for d in out["items"]]
    out["masked"] = masked
    return out


# ---- operations log, incidents, audit, workers ---------------------------------------------------

def list_incidents(ctx: Ctx, params: dict) -> dict:
    page, size, off = page_args(ctx, params)
    where, args = ["true"], {"limit": size, "offset": off}
    state = _enum(params.get("state"), ("OPEN", "CLOSED"), "state")
    if state:
        where.append("state = %(state)s")
        args["state"] = state
    if params.get("kind"):
        where.append("kind = %(kind)s")
        args["kind"] = str(params["kind"])
    cluster_id = _enum(params.get("cluster_id"), ctx.cfg.cluster_ids, "cluster_id")
    if cluster_id:
        where.append("cluster_id = %(cluster_id)s")
        args["cluster_id"] = cluster_id
    rows = ctx.db.query(f"""SELECT id, kind, dedup_key, cluster_id, severity, state, message, occurrences,
                                   first_seen, last_seen, run_id, asset_id, rule_key, check_kind, scope_key,
                                   error_code, detail, count(*) OVER () AS _total
                            FROM dq_incident WHERE {' AND '.join(where)} ORDER BY last_seen DESC
                            LIMIT %(limit)s OFFSET %(offset)s""", args)
    return _paged(rows, page, size)


def list_audit(ctx: Ctx, params: dict) -> dict:
    page, size, off = page_args(ctx, params)
    where, args = ["true"], {"limit": size, "offset": off}
    if params.get("event_type"):
        where.append("event_type = %(et)s")
        args["et"] = str(params["event_type"])
    if params.get("object_type"):
        where.append("object_type = %(ot)s")
        args["ot"] = str(params["object_type"])
    if params.get("object_id"):
        where.append("object_id = %(oid)s")
        args["oid"] = str(params["object_id"])
    if params.get("actor"):
        where.append("actor = %(actor)s")
        args["actor"] = str(params["actor"])
    rows = ctx.db.query(f"""SELECT id, event_type, object_type, object_id, payload, reason, actor, created_at,
                                   count(*) OVER () AS _total
                            FROM dq_outbox WHERE {' AND '.join(where)} ORDER BY id DESC
                            LIMIT %(limit)s OFFSET %(offset)s""", args)
    return _paged(rows, page, size)


def list_workers(ctx: Ctx) -> list[dict]:
    rows = ctx.db.query("""SELECT w.id, w.name, w.cluster_id, w.execution_location, w.status, w.created_at,
                                  r.id AS registration_id, r.status AS registration_status, r.registered_at,
                                  r.last_heartbeat_at, r.capabilities->>'sr_version' AS sr_version,
                                  r.capabilities->'s1'->>'status' AS s1_status, r.stats
                           FROM dq_worker w LEFT JOIN LATERAL (
                               SELECT * FROM dq_worker_registration x WHERE x.worker_id = w.id
                               ORDER BY x.registered_at DESC LIMIT 1) r ON true
                           ORDER BY w.cluster_id, w.name""")
    return clean(rows)


def get_control(ctx: Ctx) -> dict:
    ctl = control(ctx)
    out = clean(ctl)
    out["window"] = clean(window.window_state(ctx.cfg, ctx.now(), ctl["window_override"]))
    out["next_open"] = iso(window.next_open(ctx.cfg, ctx.now(), ctl["window_override"]))
    return out
