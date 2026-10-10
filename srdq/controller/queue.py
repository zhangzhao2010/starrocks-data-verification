"""Worker identity, registration, claim scheduling, heartbeats and cancellation requests (doc 03 §12-13)."""
from __future__ import annotations

import datetime as _dt
import secrets
from typing import Any

from ..common import canonical as c
from ..db.db import jb, new_id
from . import budgets, planning, window
from .common import (LIVE_ATTEMPT, SIDES, ApiError, Ctx, as_uuid, audit, close_incident, digest_json, incident,
                     iso, lock_control, pg_now, protocol_ok, transfer_allowed)

MAX_CLAIM_ITEMS = 32
CANDIDATE_LIMIT = 200
BLOCKING_STATES = ("CANCELLED", "ARCHIVED")


# ---- worker identity ---------------------------------------------------------------------

def token_digest(token: str) -> bytes:
    return c.sha256(token.encode("utf-8"))


def create_worker(ctx: Ctx, name: str, side: str, execution_location: str = "ALIYUN_ECS",
                  rotate: bool = False) -> dict:
    """Create a side-bound worker identity (or rotate its token). The token is returned once, never stored."""
    if side not in SIDES:
        raise ApiError(422, "BAD_SIDE", f"side must be one of {SIDES}")
    if not name or len(name) > 64:
        raise ApiError(422, "BAD_NAME", "worker name must be 1..64 characters")
    token = secrets.token_urlsafe(32)
    with ctx.db.tx() as cur:
        cur.execute("SELECT * FROM dq_worker WHERE name=%s FOR UPDATE", (name,))
        w = cur.fetchone()
        if w is not None:
            if not rotate:
                raise ApiError(409, "WORKER_EXISTS", "worker exists; use rotate to issue a new token")
            if w["side"] != side:
                raise ApiError(409, "SIDE_MISMATCH", "an existing worker cannot change side")
            cur.execute("UPDATE dq_worker SET token_digest=%s, status='ACTIVE', execution_location=%s WHERE id=%s",
                        (token_digest(token), execution_location, w["id"]))
            cur.execute("UPDATE dq_worker_registration SET status='SUPERSEDED' WHERE worker_id=%s AND status='ACTIVE'",
                        (w["id"],))
            wid = w["id"]
            audit(cur, "WORKER_TOKEN_ROTATED", "worker", wid, {"name": name, "side": side})
        else:
            wid = new_id()
            cur.execute("""INSERT INTO dq_worker (id, name, side, execution_location, token_digest)
                           VALUES (%s,%s,%s,%s,%s)""", (wid, name, side, execution_location, token_digest(token)))
            audit(cur, "WORKER_CREATED", "worker", wid, {"name": name, "side": side,
                                                         "execution_location": execution_location})
    return {"worker_id": str(wid), "name": name, "side": side, "token": token}


def disable_worker(ctx: Ctx, name: str, reason: str | None = None) -> dict:
    with ctx.db.tx() as cur:
        cur.execute("UPDATE dq_worker SET status='DISABLED' WHERE name=%s RETURNING id", (name,))
        r = cur.fetchone()
        if r is None:
            raise ApiError(404, "NOT_FOUND", "worker not found")
        cur.execute("UPDATE dq_worker_registration SET status='SUPERSEDED' WHERE worker_id=%s AND status='ACTIVE'",
                    (r["id"],))
        audit(cur, "WORKER_DISABLED", "worker", r["id"], {"name": name}, reason)
    return {"worker_id": str(r["id"]), "status": "DISABLED"}


def authenticate(ctx: Ctx, token: str | None) -> dict:
    if not token:
        raise ApiError(401, "UNAUTHORIZED", "missing bearer token")
    w = ctx.db.one("SELECT * FROM dq_worker WHERE token_digest=%s", (token_digest(token),))
    if w is None or w["status"] != "ACTIVE":
        raise ApiError(401, "UNAUTHORIZED", "unknown or disabled worker token")
    return w


# ---- registration --------------------------------------------------------------------------

def register(ctx: Ctx, worker: dict, body: dict) -> dict:
    protocol_ok(body.get("protocol_version"))
    side = body.get("side")
    if side != worker["side"]:
        raise ApiError(403, "SIDE_MISMATCH", "token is bound to another side")
    boot_id = as_uuid(body.get("boot_id"), "boot_id")
    caps = body.get("capabilities") or {}
    if not isinstance(caps, dict):
        raise ApiError(422, "BAD_CAPABILITIES", "capabilities must be an object")
    cfg = ctx.cfg
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        if ctl["status"] == "RECOVERY":
            raise ApiError(503, "RECOVERY", "controller is in recovery; retry later")
        cur.execute("UPDATE dq_worker_registration SET status='SUPERSEDED' WHERE worker_id=%s AND status='ACTIVE'",
                    (worker["id"],))
        # A new boot means the previous process is gone; its queries may still run on SR until confirmed.
        cur.execute("""UPDATE dq_attempt SET status='CANCEL_PENDING',
                       cancel_reason=COALESCE(cancel_reason, 'EXECUTOR_RESTART')
                       WHERE worker_id=%s AND status IN ('LEASED','RUNNING') RETURNING id, work_item_id""",
                    (worker["id"],))
        for r in cur.fetchall():
            cur.execute("UPDATE dq_work_item SET status='CANCEL_PENDING', updated_at=now() "
                        "WHERE id=%s AND current_attempt_id=%s", (r["work_item_id"], r["id"]))
        rid = new_id()
        cur.execute("""INSERT INTO dq_worker_registration (id, worker_id, side, boot_id, control_generation,
                       capability_digest, capabilities) VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                    (rid, worker["id"], side, boot_id, ctl["control_generation"], digest_json(caps), jb(caps)))
        cur.execute("UPDATE dq_source SET capability=%s, updated_at=now() WHERE side=%s", (jb(caps), side))
        s1 = (caps.get("s1") or {}).get("status")
        if s1 != "PASS":
            incident(cur, "CAPABILITY_MISSING", f"{side}:s1", f"S1 pushdown probe on {side}: {s1 or 'MISSING'}",
                     side=side, detail={"s1": caps.get("s1")})
        else:
            close_incident(cur, "CAPABILITY_MISSING", f"{side}:s1")
        cur.execute("""SELECT id, query_ref, status FROM dq_attempt WHERE worker_id=%s
                       AND status IN ('CANCEL_REQUESTED','CANCEL_PENDING') ORDER BY claimed_at""", (worker["id"],))
        pending = [{"attempt_id": str(r["id"]), "query_ref": r["query_ref"], "status": r["status"]}
                   for r in cur.fetchall()]
        audit(cur, "WORKER_REGISTERED", "worker", worker["id"],
              {"registration_id": str(rid), "side": side, "boot_id": str(boot_id), "s1": s1,
               "sr_version": caps.get("sr_version"), "pending": len(pending)}, actor=f"worker:{worker['name']}")
    return {"registration_id": str(rid), "control_generation": str(ctl["control_generation"]),
            "lease_seconds": cfg["execution"]["lease_seconds"],
            "heartbeat_seconds": cfg["execution"]["heartbeat_seconds"], "pending": pending}


def _registration(cur, worker: dict, body: dict, ctl: dict) -> dict:
    rid = as_uuid(body.get("registration_id"), "registration_id")
    cur.execute("SELECT * FROM dq_worker_registration WHERE id=%s AND worker_id=%s", (rid, worker["id"]))
    reg = cur.fetchone()
    if reg is None:
        raise ApiError(409, "UNKNOWN_REGISTRATION", "registration not found for this worker")
    if reg["status"] != "ACTIVE":
        raise ApiError(409, "REGISTRATION_STALE", f"registration is {reg['status']}")
    if str(body.get("control_generation")) != str(ctl["control_generation"]) \
            or reg["control_generation"] != ctl["control_generation"]:
        raise ApiError(409, "GENERATION_MISMATCH", "control generation changed; register again")
    cur.execute("UPDATE dq_worker_registration SET last_heartbeat_at=now() WHERE id=%s", (rid,))
    return reg


# ---- claim -----------------------------------------------------------------------------------

def _source_ref(cur, side: str) -> str:
    cur.execute("SELECT connection_ref FROM dq_source WHERE side=%s", (side,))
    r = cur.fetchone()
    if r is None:
        raise ApiError(503, "NOT_INITIALIZED", "source rows are not initialized")
    return r["connection_ref"]


def _touch_run(cur, run_id, window_generation: int) -> None:
    cur.execute("""UPDATE dq_run SET execution_state='RUNNING',
                   windows_used = windows_used + CASE WHEN last_window_generation IS DISTINCT FROM %s THEN 1 ELSE 0 END,
                   last_window_generation=%s, first_window_at=COALESCE(first_window_at, now()), last_window_at=now()
                   WHERE id=%s AND execution_state IN ('READY','RUNNING')""",
                (window_generation, window_generation, run_id))


def _lease(cur, ctx: Ctx, worker: dict, reg: dict, ctl: dict, w: dict, source_ref: str,
           reservations: list[dict]) -> dict:
    aid = new_id()
    lg = int(w["lease_generation"]) + 1
    cur.execute("""INSERT INTO dq_attempt (id, work_item_id, registration_id, worker_id, side, control_generation,
                   lease_generation, lease_expires_at, source_ref, execution_location, window_generation, progress)
                   VALUES (%s,%s,%s,%s,%s,%s,%s, now() + make_interval(secs => %s), %s,%s,%s,%s)""",
                (aid, w["id"], reg["id"], worker["id"], w["side"], ctl["control_generation"], lg,
                 ctx.cfg["execution"]["lease_seconds"], source_ref, worker["execution_location"],
                 ctl["window_generation"],
                 jb(budgets.with_entries({}, reservations, read_bytes=0, usage_seq=0))))
    cur.execute("""UPDATE dq_work_item SET status='LEASED', current_attempt_id=%s, lease_generation=%s,
                   updated_at=now() WHERE id=%s""", (aid, lg, w["id"]))
    _touch_run(cur, w["run_id"], ctl["window_generation"])
    return {"work_item_id": str(w["id"]), "attempt_id": str(aid), "lease_generation": lg, "spec": w["spec"],
            "kind": w["kind"], "observation_id": None if w.get("observation_id") is None else str(w["observation_id"]),
            "run_check_id": None if w.get("run_check_id") is None else str(w["run_check_id"]),
            "scan_unit_id": str(w["scan_unit_id"]), "rule_id": None if w.get("rule_id") is None else str(w["rule_id"]),
            "check_kind": w.get("check_kind")}


def fail_work(cur, work_id, error_class: str, code: str | None, message: str) -> None:
    cur.execute("""UPDATE dq_work_item SET status='FAILED', updated_at=now(),
                   result = COALESCE(result, '{}'::jsonb) || jsonb_build_object('failure', %s::jsonb)
                   WHERE id=%s""",
                (jb({"error_class": error_class, "error_code": code, "message": message[:2000]}), work_id))


def _p90_ms(cur, side: str, asset_id, scope_key_digest, rule_key: str) -> float | None:
    cur.execute("""SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY a.duration_ms) AS p
                   FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                   JOIN dq_run_check rc ON rc.id = w.run_check_id
                   WHERE a.status='SUCCEEDED' AND a.side=%s AND w.asset_id=%s AND rc.scope_key_digest=%s
                     AND rc.rule_key=%s AND a.duration_ms IS NOT NULL AND a.finished_at > now() - interval '30 days'""",
                (side, asset_id, scope_key_digest, rule_key))
    r = cur.fetchone()
    return None if r is None or r["p"] is None else float(r["p"])


def _hold_slot(cur, asset_id, run_id) -> bool:
    cur.execute("INSERT INTO dq_asset_slot (asset_id) VALUES (%s) ON CONFLICT DO NOTHING", (asset_id,))
    cur.execute("SELECT * FROM dq_asset_slot WHERE asset_id=%s FOR UPDATE", (asset_id,))
    s = cur.fetchone()
    if s["state"] == "FREE":
        cur.execute("UPDATE dq_asset_slot SET state='HELD', run_id=%s, acquired_at=now(), updated_at=now() "
                    "WHERE asset_id=%s", (run_id, asset_id))
        return True
    return s["state"] == "HELD" and s["run_id"] == run_id


def claim(ctx: Ctx, worker: dict, body: dict) -> dict:
    from . import evaluate  # local import: evaluate depends on this module

    protocol_ok(body.get("protocol_version"))
    try:
        max_items = max(1, min(MAX_CLAIM_ITEMS, int(body.get("max_items") or 1)))
    except (TypeError, ValueError) as e:
        raise ApiError(422, "BAD_MAX_ITEMS", "max_items must be an integer") from e
    side = worker["side"]
    cfg = ctx.cfg
    failed_obs: list = []
    items: list[dict] = []
    reason = None
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        if ctl["status"] == "RECOVERY":
            raise ApiError(503, "RECOVERY", "controller is in recovery; retry later")
        reg = _registration(cur, worker, body, ctl)
        if ctl["queue_revision"] != ctl["queue_materialized_revision"]:
            planning.refresh_queue(cur)
        cs = (ctl["cluster_state"] or {}).get(side) or {}
        ws = window.window_state(cfg, ctx.now(), ctl["window_override"])
        if cs.get("hard_stop"):
            reason = "HARD_STOP"
        elif ws["state"] == "CLOSED":
            reason = "WINDOW_CLOSED"
        else:
            limit = int(cs.get("concurrency") or cfg["execution"]["concurrency_per_cluster"])
            cur.execute("SELECT count(*) AS n FROM dq_attempt WHERE side=%s AND status = ANY(%s)",
                        (side, list(LIVE_ATTEMPT)))
            free = min(max_items, limit - int(cur.fetchone()["n"]))
            if free <= 0:
                reason = "CONCURRENCY"
            else:
                source_ref = _source_ref(cur, side)
                items += _claim_metadata(cur, ctx, worker, reg, ctl, free, source_ref)
                if len(items) < free and cfg["run_enabled"]:
                    items += _claim_checks(cur, ctx, worker, reg, ctl, ws, free - len(items), source_ref,
                                           failed_obs)
                if not items:
                    reason = "NO_WORK" if cfg["run_enabled"] else "RUN_DISABLED"
    for oid in dict.fromkeys(failed_obs):
        evaluate.evaluate_observation(ctx, oid)
    return {"items": items, "reason": reason}


def _claim_metadata(cur, ctx, worker, reg, ctl, free: int, source_ref: str) -> list[dict]:
    cur.execute("""SELECT w.* FROM dq_work_item w JOIN dq_run r ON r.id = w.run_id
                   WHERE w.side=%s AND w.kind='METADATA' AND w.status='READY' AND r.control_state='ENABLED'
                     AND r.execution_state NOT IN ('COMPLETED','FAILED','CANCELLED')
                   ORDER BY w.created_at, w.id LIMIT %s FOR UPDATE OF w SKIP LOCKED""", (worker["side"], free))
    return [_lease(cur, ctx, worker, reg, ctl, w, source_ref, []) for w in cur.fetchall()]


def _claim_checks(cur, ctx: Ctx, worker, reg, ctl, ws: dict, free: int, source_ref: str,
                  failed_obs: list) -> list[dict]:
    cfg = ctx.cfg
    side = worker["side"]
    wcfg = cfg["execution_window"]
    cur.execute("""SELECT w.*, o.started_at AS obs_started_at, rc.node_id, rc.rule_id, rc.rule_key,
                          rc.scope_key_digest, rc.execution_snapshot, rc.control_state AS rc_cs,
                          r.control_state AS run_cs, n.control_state AS node_cs,
                          COALESCE(p.control_state, 'ENABLED') AS parent_cs
                   FROM dq_work_item w
                   JOIN dq_observation o ON o.id = w.observation_id
                   JOIN dq_run_check rc ON rc.id = w.run_check_id
                   JOIN dq_run r ON r.id = w.run_id
                   JOIN dq_task_node n ON n.id = rc.node_id
                   LEFT JOIN dq_task_node p ON p.id = n.parent_id
                   WHERE w.side=%s AND w.kind='CHECK' AND w.status='READY' AND o.status='OPEN'
                     AND r.execution_state NOT IN ('COMPLETED','FAILED','CANCELLED')
                   ORDER BY (o.started_at IS NOT NULL) DESC, rc.kind_rank, rc.effective_priority DESC,
                            rc.layer_rank, rc.ready_seq, w.id
                   LIMIT %s FOR UPDATE OF w SKIP LOCKED""", (side, CANDIDATE_LIMIT))
    candidates = cur.fetchall()
    s1 = ((reg["capabilities"] or {}).get("s1") or {}).get("status")
    now = ctx.now()
    p90_cache: dict = {}
    out: list[dict] = []
    for w in candidates:
        if len(out) >= free:
            break
        states = (w["run_cs"], w["rc_cs"], w["node_cs"], w["parent_cs"])
        if any(s in BLOCKING_STATES for s in states):
            continue
        started = w["obs_started_at"] is not None
        if not started and ("PAUSED" in states or ws["state"] == "DRAINING"):
            continue
        if not started and wcfg["predict_overrun"]:
            key = (w["asset_id"], bytes(w["scope_key_digest"]), w["rule_key"])
            if key not in p90_cache:
                p90_cache[key] = _p90_ms(cur, side, *key)
            p90 = p90_cache[key]
            if p90 is not None and now + _dt.timedelta(milliseconds=p90 * float(wcfg["predict_factor"])) \
                    > ws["close_at"]:
                continue
        xs = w["execution_snapshot"]
        if not (transfer_allowed(cfg, xs["transfer_class"], xs.get("detail_approval_ref"))
                and ctl["transfer_approval_ref"]):
            continue
        if w["check_kind"] == "SAMPLE" and s1 != "PASS":
            if s1 == "UNSUPPORTED_SAMPLE_PUSHDOWN":
                fail_work(cur, w["id"], "UNSUPPORTED", "SAMPLE_PUSHDOWN_UNSUPPORTED",
                          f"S1 pushdown probe failed on {side}; SAMPLE cannot run on this cluster")
                failed_obs.append(w["observation_id"])
            continue
        if not _hold_slot(cur, w["asset_id"], w["run_id"]):
            continue
        status, entries = budgets.reserve(cur, w["run_id"], w["node_id"], side, int(xs["result_cap"]))
        if status == "WAIT":
            continue
        if status == "EXCEEDED":
            fail_work(cur, w["id"], "BUDGET_EXCEEDED", "BUDGET_EXCEEDED", "run/node budget is exhausted")
            failed_obs.append(w["observation_id"])
            continue
        out.append(_lease(cur, ctx, worker, reg, ctl, w, source_ref, entries))
        cur.execute("UPDATE dq_observation SET started_at=COALESCE(started_at, now()) WHERE id=%s",
                    (w["observation_id"],))
        cur.execute("UPDATE dq_run_check SET execution_state='RUNNING', updated_at=now() "
                    "WHERE id=%s AND execution_state='READY'", (w["run_check_id"],))
    return out


# ---- heartbeat ---------------------------------------------------------------------------------

def request_cancel(cur, attempt_id, reason: str) -> bool:
    cur.execute("""UPDATE dq_attempt SET status='CANCEL_REQUESTED', cancel_reason=COALESCE(cancel_reason, %s)
                   WHERE id=%s AND status IN ('LEASED','RUNNING') RETURNING id""", (reason, attempt_id))
    return cur.fetchone() is not None


def expire_attempt(cur, attempt: dict, reason: str = "LEASE_EXPIRED") -> None:
    """Lease lost while the query may still run: wait for termination confirmation."""
    cur.execute("""UPDATE dq_attempt SET status='CANCEL_PENDING', cancel_reason=COALESCE(cancel_reason, %s)
                   WHERE id=%s AND status = ANY(%s)""", (reason, attempt["id"], list(LIVE_ATTEMPT)))
    cur.execute("UPDATE dq_work_item SET status='CANCEL_PENDING', updated_at=now() "
                "WHERE id=%s AND current_attempt_id=%s", (attempt["work_item_id"], attempt["id"]))
    incident(cur, "LEASE_EXPIRED", attempt["side"], f"lease expired for attempts on {attempt['side']}",
             side=attempt["side"], detail={"last_attempt_id": str(attempt["id"])})


def cancel_live(cur, reason: str, *, run_id=None, node_id=None, observation_id=None) -> int:
    conds, params = [], []
    if run_id is not None:
        conds.append("w.run_id=%s")
        params.append(run_id)
    if node_id is not None:
        conds.append("w.run_check_id IN (SELECT id FROM dq_run_check WHERE node_id=%s)")
        params.append(node_id)
    if observation_id is not None:
        conds.append("w.observation_id=%s")
        params.append(observation_id)
    where = " AND ".join(conds) or "true"
    cur.execute(f"""SELECT a.id FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                    WHERE {where} AND a.status IN ('LEASED','RUNNING')""", params)
    n = 0
    for r in cur.fetchall():
        n += int(request_cancel(cur, r["id"], reason))
    return n


def budget_exceeded(cur, b: dict) -> None:
    incident(cur, "BUDGET_EXCEEDED", f"{b['run_id']}:{b['level']}:{b['node_id']}:{b['resource']}",
             f"{b['level']} budget {b['resource']} exceeded ({int(b['consumed'])} > {int(b['limit_amount'])})",
             severity="CRITICAL", run_id=b["run_id"],
             detail={"budget_id": str(b["id"]), "node_id": None if b["node_id"] is None else str(b["node_id"])})
    if b["level"] == "RUN":
        cancel_live(cur, "BUDGET_EXCEEDED", run_id=b["run_id"])
    else:
        cancel_live(cur, "BUDGET_EXCEEDED", node_id=b["node_id"])


def _int_text(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    try:
        n = int(c.canonical_int_text(v))
    except c.CanonicalError as e:
        raise ApiError(422, "BAD_USAGE", f"usage value {v!r} is not an integer") from e
    if n < 0:
        raise ApiError(422, "BAD_USAGE", "usage must be non-negative")
    return n


def account_usage(cur, attempt: dict, read_bytes: int, seq: int | None) -> None:
    prog = attempt["progress"] or {}
    if seq is not None and seq <= int(prog.get("usage_seq", 0)):
        return
    prev = int(prog.get("read_bytes", 0))
    entries = budgets.progress_entries(prog)
    newly = []
    if read_bytes > prev and attempt["side"] == "huawei":
        newly = budgets.consume(cur, entries, "TRANSFER_BYTES", read_bytes - prev)
    new_prog = budgets.with_entries(prog, entries, read_bytes=max(prev, read_bytes),
                                    usage_seq=int(prog.get("usage_seq", 0)) if seq is None else seq)
    cur.execute("UPDATE dq_attempt SET progress=%s WHERE id=%s", (jb(new_prog), attempt["id"]))
    attempt["progress"] = new_prog
    for b in newly:
        budget_exceeded(cur, b)


def heartbeat(ctx: Ctx, worker: dict, body: dict) -> dict:
    protocol_ok(body.get("protocol_version"))
    attempts = body.get("attempts") or []
    if not isinstance(attempts, list) or len(attempts) > 256:
        raise ApiError(422, "BAD_ATTEMPTS", "attempts must be a list of at most 256 entries")
    directives: dict[str, str] = {}
    lease = ctx.cfg["execution"]["lease_seconds"]
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        if ctl["status"] == "RECOVERY":
            raise ApiError(503, "RECOVERY", "controller is in recovery; retry later")
        _registration(cur, worker, body, ctl)
        pnow = pg_now(cur)
        for a_in in attempts:
            if not isinstance(a_in, dict):
                raise ApiError(422, "BAD_ATTEMPTS", "attempt entries must be objects")
            aid = as_uuid(a_in.get("attempt_id"), "attempt_id")
            cur.execute("SELECT * FROM dq_attempt WHERE id=%s AND worker_id=%s FOR UPDATE", (aid, worker["id"]))
            a = cur.fetchone()
            try:
                lg = int(a_in.get("lease_generation"))
            except (TypeError, ValueError):
                lg = -1
            if a is None or a["lease_generation"] != lg or a["status"] not in LIVE_ATTEMPT:
                directives[str(aid)] = "LEASE_LOST"
                continue
            usage = a_in.get("usage") or {}
            seq = a_in.get("usage_seq")
            account_usage(cur, a, _int_text(usage.get("READ_BYTES")), None if seq is None else _int_text(seq))
            qref = a_in.get("query_ref")
            if qref is not None and not isinstance(qref, dict):
                raise ApiError(422, "BAD_QUERY_REF", "query_ref must be an object")
            if a["status"] in ("CANCEL_REQUESTED", "CANCEL_PENDING"):
                if qref is not None:
                    cur.execute("UPDATE dq_attempt SET query_ref=%s WHERE id=%s", (jb(qref), aid))
                directives[str(aid)] = "CANCEL"
                continue
            if a["lease_expires_at"] <= pnow:
                expire_attempt(cur, a)
                directives[str(aid)] = "LEASE_LOST"
                continue
            cur.execute("""UPDATE dq_attempt SET status='RUNNING', lease_expires_at = now() + make_interval(secs => %s),
                           query_ref = COALESCE(%s, query_ref),
                           read_started = COALESCE(read_started, CASE WHEN %s THEN now() END) WHERE id=%s""",
                        (lease, None if qref is None else jb(qref), qref is not None, aid))
            directives[str(aid)] = "CONTINUE"
        signals = body.get("signals")
        if isinstance(signals, dict):
            cs = dict(ctl["cluster_state"] or {})
            side_state = dict(cs.get(worker["side"]) or {})
            side_state["signals"] = {**{k: v for k, v in signals.items() if isinstance(k, str)},
                                     "at": iso(ctx.now())}
            cs[worker["side"]] = side_state
            cur.execute("UPDATE dq_control_state SET cluster_state=%s WHERE id=1", (jb(cs),))
    return {"directives": directives}
