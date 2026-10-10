"""Controller tick: window transitions, lease reaping, alerts, materialization, completion, schedules, reports."""
from __future__ import annotations

import logging
import threading

from ..db.db import jb
from . import evaluate, planning, queue, window
from .common import Ctx, audit, close_incident, control, incident, iso, lock_control

log = logging.getLogger("srdq.controller")


def tick(ctx: Ctx) -> dict:
    """One pass of the background loop. Every step is its own short transaction."""
    from . import loadguard, purge, reports, schedules

    if control(ctx)["status"] == "RECOVERY":
        return {"recovery": True}
    out: dict = {}
    out["window"] = step_window(ctx)
    out["reaped"] = step_reap(ctx)
    step_cancel_pending_alert(ctx)
    out["schedules"] = schedules.tick(ctx)
    out["materialized"] = step_materialize(ctx)
    step_discovery(ctx)
    out["completed_runs"] = step_complete_runs(ctx)
    loadguard.evaluate(ctx)
    out["report"] = reports.maybe_generate(ctx)
    out["purge"] = purge.maybe_purge(ctx)
    return out


# ---- execution window ----------------------------------------------------------------------------

def step_window(ctx: Ctx) -> str:
    to_eval: list = []
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        ws = window.window_state(ctx.cfg, ctx.now(), ctl["window_override"])
        if ws["state"] == ctl["window_state"] and ws["key"] == ctl["window_key"]:
            return ws["state"]
        new_window = ws["state"] != "CLOSED" and ws["key"] != ctl["window_key"]
        closing = ws["state"] == "CLOSED" and ctl["window_state"] != "CLOSED"
        now = ctx.now()
        cur.execute("""UPDATE dq_control_state SET window_state=%s, window_key=COALESCE(%s, window_key),
                       window_generation = window_generation + %s,
                       window_opened_at = CASE WHEN %s THEN %s ELSE window_opened_at END,
                       window_closed_at = CASE WHEN %s THEN %s ELSE window_closed_at END,
                       revision=revision+1, updated_at=now() WHERE id=1""",
                    (ws["state"], ws["key"], 1 if new_window else 0, new_window, now, closing, now))
        audit(cur, "WINDOW_STATE_CHANGED", "control", 1,
              {"from": ctl["window_state"], "to": ws["state"], "window_key": ws["key"],
               "close_at": iso(ws["close_at"])}, actor="controller")
        if closing:
            to_eval = _close_window(cur, ws["key"])
    for oid in to_eval:
        evaluate.evaluate_observation(ctx, oid)
    return ws["state"]


def _close_window(cur, key: str | None) -> list:
    """Window closed: abandon started observations and cancel every live query (CANCEL_AND_CONFIRM)."""
    failure = {"error_class": "CANCELLED", "error_code": "WINDOW_CLOSED", "message": "execution window closed"}
    cur.execute("""UPDATE dq_work_item w SET status='ABANDONED', updated_at=now(),
                   result = COALESCE(w.result, '{}'::jsonb) || jsonb_build_object('failure', %s::jsonb)
                   FROM dq_observation o WHERE o.id = w.observation_id AND o.status='OPEN'
                     AND o.started_at IS NOT NULL AND w.status='READY'
                   RETURNING w.observation_id""", (jb(failure),))
    oids = {r["observation_id"] for r in cur.fetchall()}
    cur.execute("""SELECT a.id, w.observation_id FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                   WHERE a.status IN ('LEASED','RUNNING')""")
    live = cur.fetchall()
    for r in live:
        queue.request_cancel(cur, r["id"], "WINDOW_CLOSED")
        if r["observation_id"] is not None:
            oids.add(r["observation_id"])
    if live or oids:
        incident(cur, "WINDOW_CLOSE_CANCEL", f"window:{key}",
                 f"window {key} closed with {len(live)} running queries; cancellation requested",
                 detail={"cancelled_attempts": len(live), "observations": len(oids)})
    return sorted(oids, key=str)


# ---- leases --------------------------------------------------------------------------------------

def step_reap(ctx: Ctx) -> int:
    lease = int(ctx.cfg["execution"]["lease_seconds"])
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("""SELECT * FROM dq_attempt WHERE status IN ('LEASED','RUNNING','CANCEL_REQUESTED')
                       AND lease_expires_at <= now() ORDER BY lease_expires_at FOR UPDATE""")
        expired = cur.fetchall()
        for a in expired:
            queue.expire_attempt(cur, a)
        cur.execute("""UPDATE dq_worker_registration SET status='EXPIRED' WHERE status='ACTIVE'
                       AND last_heartbeat_at < now() - make_interval(secs => %s)""", (3 * lease,))
    return len(expired)


def step_cancel_pending_alert(ctx: Ctx) -> None:
    secs = int(ctx.cfg["execution"]["cancel_pending_alert_seconds"])
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("""SELECT side, count(*) AS n, min(lease_expires_at) AS oldest FROM dq_attempt
                       WHERE status IN ('CANCEL_REQUESTED','CANCEL_PENDING')
                         AND lease_expires_at + make_interval(secs => %s) < now()
                       GROUP BY side""", (secs,))
        stuck = {r["side"]: r for r in cur.fetchall()}
        for side in ("huawei", "aliyun"):
            r = stuck.get(side)
            if r is None:
                close_incident(cur, "CANCEL_PENDING_TIMEOUT", side)
            else:
                incident(cur, "CANCEL_PENDING_TIMEOUT", side,
                         f"{r['n']} queries on {side} could not be confirmed as stopped; asset slots stay held",
                         side=side, severity="CRITICAL", detail={"count": r["n"], "oldest": iso(r["oldest"])})


# ---- queue and completion ------------------------------------------------------------------------

def step_materialize(ctx: Ctx) -> int:
    if not ctx.cfg["run_enabled"]:
        return 0
    with ctx.db.tx() as cur:
        lock_control(cur)
        return planning.materialize_observations(cur, ctx)


def step_discovery(ctx: Ctx) -> None:
    for r in ctx.db.query("""SELECT id FROM dq_run WHERE purpose='METADATA_PREVIEW'
                             AND execution_state NOT IN ('COMPLETED','FAILED','CANCELLED')"""):
        evaluate.finish_discovery(ctx, r["id"])


def step_complete_runs(ctx: Ctx) -> list[str]:
    done = []
    for r in ctx.db.query("""SELECT id FROM dq_run WHERE purpose <> 'METADATA_PREVIEW'
                             AND execution_state IN ('READY','RUNNING','PLANNED','CANCEL_PENDING')"""):
        with ctx.db.tx() as cur:
            lock_control(cur)
            if evaluate.maybe_complete_run(cur, r["id"]):
                done.append(str(r["id"]))
    return done


# ---- background loop -----------------------------------------------------------------------------

class TickLoop:
    def __init__(self, ctx: Ctx):
        self.ctx = ctx
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="srdq-tick", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        period = float(self.ctx.cfg["controller"]["tick_seconds"])
        while not self._stop.is_set():
            try:
                tick(self.ctx)
            except Exception:  # noqa: BLE001 - the loop must survive any single failing step
                log.exception("controller tick failed")
            self._stop.wait(period)

    def stop(self, timeout: float = 10) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
