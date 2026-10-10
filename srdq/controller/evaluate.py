"""Observation evaluation and coverage projection (doc 03 §14–15): retries, comparisons, slot and run completion."""
from __future__ import annotations

import datetime as _dt
from typing import Any

from ..db.db import jb, new_id
from . import compare, manifest, queue
from .common import COVERAGE_OF_VERDICT, Ctx, audit, lock_control

RETRYABLE = ("TIMEOUT", "NETWORK", "QUERY_ERROR", "STORAGE_SUSPECT", "INTERNAL", "LOCAL_RESOURCE", "CANCELLED")
TERMINAL_RC = ("COMPLETED", "FAILED", "CANCELLED", "BLOCKED")
TERMINAL_RUN = ("COMPLETED", "FAILED", "CANCELLED")
STOPPED = ("CANCELLED", "ARCHIVED")


def effective_control(cur, rc: dict) -> str:
    """Most restrictive control state over run, node, parent node and the run_check itself."""
    cur.execute("""SELECT r.control_state AS run_cs, n.control_state AS node_cs,
                          COALESCE(p.control_state, 'ENABLED') AS parent_cs
                   FROM dq_run r JOIN dq_task_node n ON n.id=%s LEFT JOIN dq_task_node p ON p.id = n.parent_id
                   WHERE r.id=%s""", (rc["node_id"], rc["run_id"]))
    r = cur.fetchone()
    states = (r["run_cs"], r["node_cs"], r["parent_cs"], rc["control_state"])
    for s in ("ARCHIVED", "CANCELLED", "PAUSED"):
        if s in states:
            return s
    return "ENABLED"


def _close_siblings(cur, oid, reason: str) -> None:
    cur.execute("""UPDATE dq_work_item SET status='ABANDONED', updated_at=now()
                   WHERE observation_id=%s AND status='READY'""", (oid,))
    cur.execute("""SELECT a.id FROM dq_work_item w JOIN dq_attempt a ON a.id = w.current_attempt_id
                   WHERE w.observation_id=%s AND a.status IN ('LEASED','RUNNING')""", (oid,))
    for r in cur.fetchall():
        queue.request_cancel(cur, r["id"], reason)


def _failure(works: list[dict]) -> dict:
    failed = [w for w in works if w["status"] == "FAILED"]
    infos = [((w["result"] or {}).get("failure") or {}) | {"side": w["side"]} for w in failed]
    for f in infos:
        if f.get("error_class") not in RETRYABLE:
            return f
    return infos[0]


def _set_rc(cur, rc_id, **fields: Any) -> None:
    sets = ", ".join(f"{k}=%s" for k in fields)
    cur.execute(f"UPDATE dq_run_check SET {sets}, revision=revision+1, updated_at=now() WHERE id=%s",
                list(fields.values()) + [rc_id])


def _on_failed(cur, ctx: Ctx, o: dict, rc: dict, works: list[dict]) -> None:
    f = _failure(works)
    cls = f.get("error_class") or "INTERNAL"
    code = f.get("error_code")
    cur.execute("UPDATE dq_observation SET status='FAILED', finished_at=now() WHERE id=%s", (o["id"],))
    _close_siblings(cur, o["id"], "OBSERVATION_FAILED")
    if rc["current_observation_id"] != o["id"] or rc["execution_state"] in TERMINAL_RC:
        return
    issue = dict(rc["issue"] or {})
    issue["last_error"] = {"error_class": cls, "error_code": code, "message": (f.get("message") or "")[:500],
                           "side": f.get("side"), "observation_id": str(o["id"]), "at": ctx.now().isoformat()}
    reason = f"{cls}:{code}" if code and code != cls else cls
    if effective_control(cur, rc) in STOPPED:
        _set_rc(cur, rc["id"], execution_state="CANCEL_PENDING" if rc_has_live(cur, rc["id"]) else "CANCELLED",
                issue=jb(issue))
        return
    if cls == "UNSUPPORTED":
        _set_rc(cur, rc["id"], execution_state="COMPLETED", coverage_state="UNSUPPORTED",
                coverage_reason=reason[:500], coverage_updated_at=ctx.now(), issue=jb(issue))
        return
    if cls in RETRYABLE:
        n = int(rc["attempts_failed"]) + 1
        ex = ctx.cfg["execution"]
        if n <= int(ex["max_retries"]):
            delay = float(ex["retry_backoff_seconds"]) * (2 ** (n - 1))
            _set_rc(cur, rc["id"], execution_state="READY", attempts_failed=n, issue=jb(issue),
                    next_eligible_at=ctx.now() + _dt.timedelta(seconds=delay))
            return
        _set_rc(cur, rc["id"], execution_state="FAILED", attempts_failed=n, coverage_state="ERROR",
                coverage_reason=f"RETRIES_EXHAUSTED:{reason}"[:500], coverage_updated_at=ctx.now(), issue=jb(issue))
        return
    _set_rc(cur, rc["id"], execution_state="FAILED", coverage_state="ERROR", coverage_reason=reason[:500],
            coverage_updated_at=ctx.now(), issue=jb(issue))


def _on_abandoned(cur, ctx: Ctx, o: dict, rc: dict, works: list[dict]) -> None:
    gone = [w for w in works if w["status"] in ("CANCELLED", "ABANDONED")]
    codes = [((w["result"] or {}).get("failure") or {}).get("error_code") for w in gone]
    reason = next((x for x in codes if x), "CANCELLED")
    cur.execute("UPDATE dq_observation SET status='ABANDONED', abandon_reason=%s, finished_at=now() WHERE id=%s",
                (reason[:200], o["id"]))
    _close_siblings(cur, o["id"], "OBSERVATION_ABANDONED")
    if rc["current_observation_id"] != o["id"] or rc["execution_state"] in TERMINAL_RC:
        return
    if effective_control(cur, rc) in STOPPED:
        state = "CANCEL_PENDING" if rc_has_live(cur, rc["id"]) else "CANCELLED"
    else:
        state = "READY"
    _set_rc(cur, rc["id"], execution_state=state)


def rc_has_live(cur, rc_id) -> bool:
    cur.execute("""SELECT 1 FROM dq_work_item w JOIN dq_attempt a ON a.id = w.current_attempt_id
                   WHERE w.run_check_id=%s AND a.status IN ('LEASED','RUNNING','CANCEL_REQUESTED','CANCEL_PENDING')
                   LIMIT 1""", (rc_id,))
    return cur.fetchone() is not None


def settle_cancel_pending(cur, run_id) -> int:
    """run_checks waiting for cancellation become CANCELLED once no query of theirs may still run."""
    cur.execute("""UPDATE dq_run_check rc SET execution_state='CANCELLED', revision=revision+1, updated_at=now()
                   WHERE rc.run_id=%s AND rc.execution_state='CANCEL_PENDING'
                     AND NOT EXISTS (SELECT 1 FROM dq_work_item w JOIN dq_attempt a ON a.id = w.current_attempt_id
                                     WHERE w.run_check_id = rc.id AND a.status IN
                                     ('LEASED','RUNNING','CANCEL_REQUESTED','CANCEL_PENDING'))
                     AND NOT EXISTS (SELECT 1 FROM dq_observation o WHERE o.run_check_id = rc.id
                                     AND o.status='OPEN')""", (run_id,))
    return cur.rowcount


def _lock_rc(cur, rc_id) -> dict:
    cur.execute("SELECT * FROM dq_run_check WHERE id=%s FOR UPDATE", (rc_id,))
    return cur.fetchone()


def evaluate_observation(ctx: Ctx, oid) -> str | None:
    """Advance one observation after any of its work items changed. Returns the new observation status."""
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("SELECT * FROM dq_observation WHERE id=%s FOR UPDATE", (oid,))
        o = cur.fetchone()
        if o is None:
            return None
        if o["status"] != "OPEN":
            finalize(cur, o["run_id"], o["asset_id"])
            return o["status"]
        cur.execute("SELECT * FROM dq_work_item WHERE observation_id=%s ORDER BY side, id FOR UPDATE", (oid,))
        works = cur.fetchall()
        statuses = {w["status"] for w in works}
        rc = _lock_rc(cur, o["run_check_id"])
        if "FAILED" in statuses:
            _on_failed(cur, ctx, o, rc, works)
            finalize(cur, o["run_id"], o["asset_id"])
            return "FAILED"
        if statuses & {"CANCELLED", "ABANDONED"}:
            _on_abandoned(cur, ctx, o, rc, works)
            finalize(cur, o["run_id"], o["asset_id"])
            return "ABANDONED"
        if statuses != {"DONE"}:
            return "OPEN"
        cur.execute("SELECT * FROM dq_side_result WHERE observation_id=%s AND validity='ACCEPTED' ORDER BY side, id",
                    (oid,))
        results = cur.fetchall()
    try:
        outcome = compare.compare(ctx, rc, results, lambda srid: compare.load_groups(ctx.db, srid))
    except Exception as e:  # noqa: BLE001 - any comparison failure becomes an ERROR verdict
        outcome = compare.error_outcome(ctx, results, f"{type(e).__name__}: {e}")
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("SELECT * FROM dq_observation WHERE id=%s FOR UPDATE", (oid,))
        o = cur.fetchone()
        if o["status"] != "OPEN":
            finalize(cur, o["run_id"], o["asset_id"])
            return o["status"]
        rc = _lock_rc(cur, o["run_check_id"])
        cid = _insert_comparison(cur, ctx, o, rc, outcome)
        cur.execute("UPDATE dq_observation SET status='COMPLETED', finished_at=now() WHERE id=%s", (oid,))
        if rc["current_observation_id"] == oid and rc["execution_state"] not in TERMINAL_RC:
            _set_rc(cur, rc["id"], execution_state="COMPLETED", coverage_state=COVERAGE_OF_VERDICT[outcome.verdict],
                    coverage_reason=outcome.reason, current_comparison_id=cid, coverage_updated_at=ctx.now())
        finalize(cur, o["run_id"], o["asset_id"])
    return "COMPLETED"


def _insert_comparison(cur, ctx: Ctx, o: dict, rc: dict, out: compare.Outcome):
    cur.execute("SELECT COALESCE(max(revision), 0) + 1 AS r FROM dq_comparison WHERE run_check_id=%s", (rc["id"],))
    rev = cur.fetchone()["r"]
    cid = new_id()
    d = out.diffs
    cur.execute("""INSERT INTO dq_comparison (id, run_id, asset_id, run_check_id, observation_id, rule_id, rule_digest,
                   rule_key, rule_revision, scope_key, scope_key_digest, check_kind, evidence, verdict, reason, revision,
                   source_result_ids, target_result_ids, source_read_started, source_read_finished,
                   target_read_started, target_read_finished, summary, difference_count, difference_count_mode,
                   differences_stored, compared_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'EXACT',%s,%s)""",
                (cid, rc["run_id"], rc["asset_id"], rc["id"], o["id"], rc["rule_id"], rc["rule_digest"], rc["rule_key"],
                 rc["rule_revision"], rc["scope_key"], rc["scope_key_digest"], rc["check_kind"],
                 rc["expected_evidence"], out.verdict, out.reason, rev, out.source_ids, out.target_ids,
                 out.source_read[0], out.source_read[1], out.target_read[0], out.target_read[1], jb(out.summary),
                 d.count, len(d.items), ctx.now()))
    rows = []
    for i, x in enumerate(d.items):
        rows.append((new_id(), cid, i, x["type"], x.get("group_key"),
                     None if x.get("group_values") is None else jb(x["group_values"]), x.get("metric_id"),
                     x.get("key_hex"), None if x.get("key_values") is None else jb(x["key_values"]),
                     x.get("column_name"), x.get("source_value"), x.get("target_value"), jb(x.get("detail") or {})))
    if rows:
        cur.executemany("""INSERT INTO dq_difference (id, comparison_id, ordinal, type, group_key, group_values,
                           metric_id, key_hex, key_values, column_name, source_value, target_value, detail)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", rows)
    audit(cur, "COMPARISON_RECORDED", "comparison", cid,
          {"run_check_id": str(rc["id"]), "verdict": out.verdict, "reason": out.reason, "differences": d.count})
    return cid


# ---- slots and run completion ------------------------------------------------------------------

def maybe_release_slot(cur, asset_id, run_id) -> bool:
    cur.execute("""SELECT 1 FROM dq_work_item w JOIN dq_attempt a ON a.id = w.current_attempt_id
                   WHERE w.run_id=%s AND w.asset_id=%s AND a.status IN ('LEASED','RUNNING','CANCEL_REQUESTED',
                   'CANCEL_PENDING') LIMIT 1""", (run_id, asset_id))
    if cur.fetchone():
        return False
    cur.execute("""SELECT 1 FROM dq_observation WHERE run_id=%s AND asset_id=%s AND status='OPEN'
                   AND started_at IS NOT NULL LIMIT 1""", (run_id, asset_id))
    if cur.fetchone():
        return False
    cur.execute("""UPDATE dq_asset_slot SET state='FREE', run_id=NULL, acquired_at=NULL, updated_at=now()
                   WHERE asset_id=%s AND run_id=%s RETURNING asset_id""", (asset_id, run_id))
    return cur.fetchone() is not None


def maybe_complete_run(cur, run_id) -> str | None:
    cur.execute("SELECT * FROM dq_run WHERE id=%s FOR UPDATE", (run_id,))
    run = cur.fetchone()
    if run is None or run["purpose"] == "METADATA_PREVIEW" or run["execution_state"] in TERMINAL_RUN:
        return None
    settle_cancel_pending(cur, run_id)
    cur.execute("SELECT 1 FROM dq_run_check WHERE run_id=%s AND execution_state <> ALL(%s) LIMIT 1",
                (run_id, list(TERMINAL_RC)))
    if cur.fetchone():
        return None
    cur.execute("""SELECT 1 FROM dq_work_item w JOIN dq_attempt a ON a.id = w.current_attempt_id
                   WHERE w.run_id=%s AND a.status IN ('LEASED','RUNNING','CANCEL_REQUESTED','CANCEL_PENDING')
                   LIMIT 1""", (run_id,))
    if cur.fetchone():
        return None
    cur.execute("UPDATE dq_observation SET status='ABANDONED', abandon_reason='RUN_FINISHED', finished_at=now() "
                "WHERE run_id=%s AND status='OPEN'", (run_id,))
    cur.execute("UPDATE dq_work_item SET status='ABANDONED', updated_at=now() WHERE run_id=%s AND status='READY'",
                (run_id,))
    state = "CANCELLED" if run["control_state"] in STOPPED else "COMPLETED"
    cur.execute("UPDATE dq_run SET execution_state=%s, completed_at=now(), revision=revision+1 WHERE id=%s",
                (state, run_id))
    cur.execute("UPDATE dq_asset_slot SET state='FREE', run_id=NULL, acquired_at=NULL, updated_at=now() "
                "WHERE run_id=%s", (run_id,))
    cur.execute("UPDATE dq_budget SET budget_state='CLOSED', updated_at=now() WHERE run_id=%s AND budget_state='OPEN'",
                (run_id,))
    cur.execute("""SELECT coverage_state, count(*) AS n FROM dq_run_check WHERE run_id=%s GROUP BY 1""", (run_id,))
    cov = {r["coverage_state"]: r["n"] for r in cur.fetchall()}
    audit(cur, "RUN_COMPLETED", "run", run_id, {"execution_state": state, "coverage": cov})
    return state


def finalize(cur, run_id, asset_id) -> None:
    settle_cancel_pending(cur, run_id)
    maybe_release_slot(cur, asset_id, run_id)
    maybe_complete_run(cur, run_id)


# ---- discovery -----------------------------------------------------------------------------------

def finish_discovery(ctx: Ctx, run_id) -> dict | None:
    """Turn a finished METADATA_PREVIEW run into a DRAFT manifest."""
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("SELECT * FROM dq_run WHERE id=%s FOR UPDATE", (run_id,))
        run = cur.fetchone()
        if run is None or run["purpose"] != "METADATA_PREVIEW" or run["execution_state"] in TERMINAL_RUN:
            return None
        cur.execute("""SELECT count(*) FILTER (WHERE status NOT IN ('DONE','FAILED')) AS open, count(*) AS n
                       FROM dq_work_item WHERE run_id=%s AND kind='METADATA'""", (run_id,))
        r = cur.fetchone()
        if r["open"]:
            return None
        if run["control_state"] in STOPPED:
            cur.execute("UPDATE dq_run SET execution_state='CANCELLED', completed_at=now() WHERE id=%s", (run_id,))
            return None
        out = manifest.build_draft(cur, ctx, run_id)
        cur.execute("UPDATE dq_run SET execution_state='COMPLETED', completed_at=now(), revision=revision+1 "
                    "WHERE id=%s", (run_id,))
        audit(cur, "DISCOVERY_COMPLETED", "run", run_id, {"manifest_id": out["id"], "work_items": r["n"]})
        return out
