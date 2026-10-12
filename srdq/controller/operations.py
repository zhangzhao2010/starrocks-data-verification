"""Task control operations with preview/apply (doc 03 §9), window and cluster actions, recovery (§14.4)."""
from __future__ import annotations

import datetime as _dt
from typing import Any

from ..db.db import jb, new_id
from . import evaluate, loadguard, planning, queue, window
from .common import (ApiError, Ctx, as_uuid, audit, close_incident, digest_json, incident, iso,
                     lock_control, parse_ts)

CONTROL_OPS = ("START", "RESUME", "PAUSE", "CANCEL", "ARCHIVE", "RESTORE", "SET_PRIORITY", "CLEAR_PRIORITY")
TARGETS_OF: dict[str, tuple[str, ...]] = {op: ("RUN", "RANGE_NODE", "RUN_CHECK") for op in CONTROL_OPS}
TARGETS_OF.update({"REGENERATE": ("RUN", "RUN_CHECK"), "CLOSE_WINDOW": ("GLOBAL",), "EXTEND_WINDOW": ("GLOBAL",),
                   "CONFIRM_RECOVERY": ("GLOBAL",), "RESUME_CLUSTER": ("CLUSTER",)})
FUTURE_OPS = ("RESUME", "PAUSE", "CANCEL", "ARCHIVE", "RESTORE", "SET_PRIORITY", "CLEAR_PRIORITY")
TRANSITIONS = {
    "START": ({"PAUSED"}, "ENABLED"),
    "RESUME": ({"PAUSED"}, "ENABLED"),
    "PAUSE": ({"ENABLED"}, "PAUSED"),
    "CANCEL": ({"ENABLED", "PAUSED"}, "CANCELLED"),
    "ARCHIVE": ({"ENABLED", "PAUSED", "CANCELLED"}, "ARCHIVED"),
    "RESTORE": ({"CANCELLED", "ARCHIVED"}, "PAUSED"),
}
TABLES = {"RUN": "dq_run", "RANGE_NODE": "dq_task_node", "RUN_CHECK": "dq_run_check"}
OBJECT_TYPE = {"dq_run": "run", "dq_task_node": "node", "dq_run_check": "run_check"}
MAX_TARGETS = 500
MAX_LISTED = 200
TERMINAL_RC = evaluate.TERMINAL_RC
TERMINAL_RUN = evaluate.TERMINAL_RUN


# ---- request normalization -------------------------------------------------------------------

def _normalize(ctx: Ctx, body: dict) -> dict:
    if not isinstance(body, dict):
        raise ApiError(422, "BAD_OPERATION", "operation must be an object")
    op, tt = body.get("op"), body.get("target_type")
    if op not in TARGETS_OF:
        raise ApiError(422, "BAD_OP", f"op must be one of {sorted(TARGETS_OF)}")
    if tt not in TARGETS_OF[op]:
        raise ApiError(422, "BAD_TARGET_TYPE", f"{op} applies to {TARGETS_OF[op]}")
    effect = body.get("effect") or ctx.cfg["control"]["default_effect"]
    if effect not in ("CURRENT_ONLY", "THIS_AND_FUTURE"):
        raise ApiError(422, "BAD_EFFECT", "effect must be CURRENT_ONLY or THIS_AND_FUTURE")
    if effect == "THIS_AND_FUTURE" and (op not in FUTURE_OPS or tt not in ("RANGE_NODE", "RUN_CHECK")):
        effect = "CURRENT_ONLY" if op not in FUTURE_OPS or tt not in ("RANGE_NODE", "RUN_CHECK") else effect
        if body.get("effect") == "THIS_AND_FUTURE":
            raise ApiError(422, "BAD_EFFECT", "THIS_AND_FUTURE needs a control/priority op on RANGE_NODE or RUN_CHECK")
    sel = planning.validate_check_selector(body.get("check_selector") or {"all": True}, allow_deep=True)
    raw_ids = body.get("target_ids") or []
    if not isinstance(raw_ids, list):
        raise ApiError(422, "BAD_TARGETS", "target_ids must be a list")
    if tt == "GLOBAL":
        ids: list[str] = []
    elif tt == "CLUSTER":
        if len(raw_ids) != 1 or raw_ids[0] not in ctx.cfg.cluster_ids:
            raise ApiError(422, "BAD_TARGETS", f"CLUSTER target_ids must be one of {ctx.cfg.cluster_ids}")
        ids = [raw_ids[0]]
    else:
        if not raw_ids or len(raw_ids) > MAX_TARGETS:
            raise ApiError(422, "BAD_TARGETS", f"target_ids must hold 1..{MAX_TARGETS} ids")
        ids = sorted({str(as_uuid(x, "target_id")) for x in raw_ids})
    params = body.get("params") or {}
    if not isinstance(params, dict):
        raise ApiError(422, "BAD_PARAMS", "params must be an object")
    if op == "SET_PRIORITY":
        p = params.get("priority")
        if not isinstance(p, int) or isinstance(p, bool) or not 0 <= p <= 100:
            raise ApiError(422, "BAD_PRIORITY", "params.priority must be an integer 0..100")
        params = {"priority": p}
    elif op == "EXTEND_WINDOW":
        if params.get("until") is not None:
            params = {"until": iso(parse_ts(params["until"], "until").astimezone(_dt.timezone.utc))}
        else:
            m = params.get("minutes")
            if not isinstance(m, int) or isinstance(m, bool) or not 1 <= m <= 1440:
                raise ApiError(422, "BAD_PARAMS", "EXTEND_WINDOW needs params.until or params.minutes (1..1440)")
            params = {"minutes": m}
    else:
        params = {}
    if tt == "RUN_CHECK" and not sel.get("all"):
        raise ApiError(422, "BAD_SELECTOR", "check_selector does not apply to RUN_CHECK targets")
    return {"op": op, "target_type": tt, "target_ids": ids, "effect": effect, "check_selector": sel,
            "params": params}


def _sel_ok(sel: dict, kind: str, rule_key: str) -> bool:
    if sel.get("all"):
        return True
    return kind in sel["kinds"] and (sel.get("rule_keys") is None or rule_key in sel["rule_keys"])


# ---- target resolution -----------------------------------------------------------------------

def _load_targets(cur, req: dict, lock: bool) -> list[dict]:
    tt = req["target_type"]
    if tt not in TABLES:
        return []
    cur.execute(f"SELECT * FROM {TABLES[tt]} WHERE id = ANY(%s::uuid[]) ORDER BY id{' FOR UPDATE' if lock else ''}",
                (req["target_ids"],))
    rows = cur.fetchall()
    if len(rows) != len(req["target_ids"]):
        raise ApiError(404, "NOT_FOUND", f"some {tt} targets do not exist")
    if tt == "RANGE_NODE" and any(r["kind"] == "SHARD" for r in rows):
        raise ApiError(422, "SHARD_TARGET", "DEEP shards are controlled through their partition node")
    run_ids = sorted({str(r["id"] if tt == "RUN" else r["run_id"]) for r in rows})
    cur.execute("SELECT id, purpose FROM dq_run WHERE id = ANY(%s::uuid[])", (run_ids,))
    if any(r["purpose"] == "METADATA_PREVIEW" for r in cur.fetchall()):
        raise ApiError(422, "METADATA_RUN", "discovery runs are not controlled through operations")
    return rows


def _affected_rcs(cur, req: dict, targets: list[dict], lock: bool) -> list[dict]:
    tt, ids = req["target_type"], req["target_ids"]
    suffix = " ORDER BY rc.id FOR UPDATE OF rc" if lock else " ORDER BY rc.id"
    if tt == "RUN":
        cur.execute("SELECT rc.* FROM dq_run_check rc WHERE rc.run_id = ANY(%s::uuid[])" + suffix, (ids,))
    elif tt == "RANGE_NODE":
        cur.execute("""SELECT rc.* FROM dq_run_check rc JOIN dq_task_node n ON n.id = rc.node_id
                       WHERE n.id = ANY(%s::uuid[]) OR n.parent_id = ANY(%s::uuid[])""" + suffix, (ids, ids))
    elif tt == "RUN_CHECK":
        cur.execute("SELECT rc.* FROM dq_run_check rc WHERE rc.id = ANY(%s::uuid[])" + suffix, (ids,))
    else:
        return []
    sel = req["check_selector"]
    return [r for r in cur.fetchall() if _sel_ok(sel, r["check_kind"], r["rule_key"])]


def _holders(cur, req: dict, targets: list[dict], rcs: list[dict]) -> tuple[str, list[dict]]:
    """Rows whose control_state/priority_override the operation writes."""
    tt, op = req["target_type"], req["op"]
    if tt == "RUN_CHECK" or not req["check_selector"].get("all"):
        return "dq_run_check", rcs
    if tt == "RUN" and op in ("SET_PRIORITY", "CLEAR_PRIORITY"):
        cur.execute("SELECT * FROM dq_task_node WHERE run_id = ANY(%s::uuid[]) AND kind='TABLE' ORDER BY id",
                    (req["target_ids"],))
        return "dq_task_node", cur.fetchall()
    return TABLES[tt], targets


def _changes(req: dict, table: str, holders: list[dict]) -> list[dict]:
    op = req["op"]
    out = []
    if op in TRANSITIONS:
        allowed, to = TRANSITIONS[op]
        for h in holders:
            if h["control_state"] in allowed:
                out.append({"object_type": OBJECT_TYPE[table], "id": str(h["id"]), "field": "control_state",
                            "from": h["control_state"], "to": to})
    elif op in ("SET_PRIORITY", "CLEAR_PRIORITY"):
        to = req["params"].get("priority")
        for h in holders:
            if h["priority_override"] != to:
                out.append({"object_type": OBJECT_TYPE[table], "id": str(h["id"]), "field": "priority_override",
                            "from": h["priority_override"], "to": to})
    return out


def _policy_keys(req: dict, targets: list[dict]) -> list[tuple]:
    if req["effect"] != "THIS_AND_FUTURE":
        return []
    keys = []
    for t in targets:
        if req["target_type"] == "RANGE_NODE":
            nk = t["kind"]
            keys.append((str(t["asset_id"]), nk, "TABLE" if nk == "TABLE" else t["scope_key"],
                         bytes(t["scope_key_digest"]), req["check_selector"]))
        else:
            keys.append((str(t["asset_id"]), "PARTITION", t["scope_key"], bytes(t["scope_key_digest"]),
                         {"kinds": [t["check_kind"]], "rule_keys": [t["rule_key"]]}))
    uniq = {}
    for k in keys:
        uniq[(k[0], k[1], k[2], digest_json(k[4]))] = k
    return [uniq[x] for x in sorted(uniq)]


def _active_policies(cur, key: tuple, lock: bool) -> list[dict]:
    aid, nk, sk, _dig, sel = key
    cur.execute(f"""SELECT * FROM dq_scope_policy WHERE asset_id=%s AND node_kind=%s AND scope_key=%s
                    AND status='ACTIVE' AND check_selector=%s ORDER BY created_at, id{' FOR UPDATE' if lock else ''}""",
                (aid, nk, sk, jb(sel)))
    return cur.fetchall()


def _policy_change(op: str, params: dict, old: list[dict]) -> tuple[str | None, int | None, str | None, int | None]:
    cs = next((p["control_state"] for p in reversed(old) if p["control_state"] is not None), None)
    pr = next((p["priority_override"] for p in reversed(old) if p["priority_override"] is not None), None)
    ncs, npr = cs, pr
    if op == "PAUSE" and cs not in ("CANCELLED", "ARCHIVED"):
        ncs = "PAUSED"
    elif op in ("RESUME", "START") and cs == "PAUSED":
        ncs = None
    elif op == "CANCEL" and cs != "ARCHIVED":
        ncs = "CANCELLED"
    elif op == "ARCHIVE":
        ncs = "ARCHIVED"
    elif op == "RESTORE" and cs in ("CANCELLED", "ARCHIVED"):
        ncs = "PAUSED"
    elif op == "SET_PRIORITY":
        npr = params["priority"]
    elif op == "CLEAR_PRIORITY":
        npr = None
    return cs, pr, ncs, npr


def _future_preview(cur, req: dict, targets: list[dict], lock: bool) -> list[dict]:
    out = []
    for key in _policy_keys(req, targets):
        old = _active_policies(cur, key, lock)
        cs, pr, ncs, npr = _policy_change(req["op"], req["params"], old)
        out.append({"asset_id": key[0], "node_kind": key[1], "scope_key": key[2], "check_selector": key[4],
                    "from": {"control_state": cs, "priority_override": pr},
                    "to": {"control_state": ncs, "priority_override": npr},
                    "revokes": [str(p["id"]) for p in old], "_key": key})
    return out


# ---- preview -------------------------------------------------------------------------------------

def _in_flight(cur, rc_ids: list[str]) -> dict:
    cur.execute("""SELECT count(*) FILTER (WHERE started_at IS NOT NULL) AS started,
                          count(*) FILTER (WHERE started_at IS NULL) AS unstarted
                   FROM dq_observation WHERE run_check_id = ANY(%s::uuid[]) AND status='OPEN'""", (rc_ids,))
    o = cur.fetchone()
    cur.execute("""SELECT a.cluster_id, count(*) AS n FROM dq_work_item w
                   JOIN dq_attempt a ON a.id = w.current_attempt_id
                   WHERE w.run_check_id = ANY(%s::uuid[]) AND a.status IN
                   ('LEASED','RUNNING','CANCEL_REQUESTED','CANCEL_PENDING') GROUP BY a.cluster_id""", (rc_ids,))
    live = {r["cluster_id"]: r["n"] for r in cur.fetchall()}
    cur.execute("""SELECT count(*) AS units, COALESCE(sum((rc.execution_snapshot->>'result_cap')::numeric), 0) AS cap
                   FROM dq_work_item w JOIN dq_observation o ON o.id = w.observation_id
                   JOIN dq_run_check rc ON rc.id = w.run_check_id
                   WHERE w.run_check_id = ANY(%s::uuid[]) AND o.status='OPEN' AND o.started_at IS NOT NULL
                     AND w.status='READY'""", (rc_ids,))
    d = cur.fetchone()
    return {"started_observations": o["started"], "unstarted_observations": o["unstarted"], "live_attempts": live,
            "drain_remaining_units": d["units"], "drain_result_bytes_cap": str(int(d["cap"]))}


def _target_row(tt: str, t: dict) -> dict:
    row = {"id": str(t["id"]), "control_state": t["control_state"]}
    if tt in ("RUN", "RANGE_NODE"):
        row["revision"] = t["revision"]
    if tt in ("RUN", "RUN_CHECK"):
        row["execution_state"] = t["execution_state"]
    if tt == "RANGE_NODE":
        row["kind"], row["label"] = t["kind"], t["label"]
    return row


def _build_preview(cur, ctx: Ctx, req: dict, lock: bool = False) -> dict:
    op, tt = req["op"], req["target_type"]
    ctl_cur = cur
    pv: dict[str, Any] = {k: req[k] for k in ("op", "target_type", "target_ids", "effect", "check_selector", "params")}
    warnings: list[str] = []
    if tt in TABLES:
        targets = _load_targets(cur, req, lock)
        rcs = _affected_rcs(cur, req, targets, lock)
        pv["targets"] = [_target_row(tt, t) for t in targets]
        by_state: dict[str, int] = {}
        for r in rcs:
            by_state[r["execution_state"]] = by_state.get(r["execution_state"], 0) + 1
        pv["run_checks"] = {"total": len(rcs), "by_execution_state": by_state}
        pv["in_flight"] = _in_flight(cur, [str(r["id"]) for r in rcs])
        if op == "REGENERATE":
            pv["regenerate"] = _regenerate_plan(cur, req, targets)
        else:
            table, holders = _holders(cur, req, targets, rcs)
            ch = _changes(req, table, holders)
            pv["changes"] = {"count": len(ch), "items": ch[:MAX_LISTED]}
            pv["_change_digest"] = digest_json(ch).hex()
            fut = _future_preview(cur, req, targets, lock)
            pv["future_policy"] = [{k: v for k, v in f.items() if k != "_key"} for f in fut]
            if not ch and not any(f["from"] != f["to"] for f in fut):
                warnings.append("nothing changes: targets are already in the requested state")
            if op == "PAUSE" and pv["in_flight"]["started_observations"]:
                warnings.append("started observations keep running until drained")
            if op in ("CANCEL", "ARCHIVE") and sum(pv["in_flight"]["live_attempts"].values()):
                warnings.append("running queries are cancelled; affected checks stay CANCEL_PENDING until confirmed")
    else:
        ctl = ctl_cur.execute("SELECT * FROM dq_control_state WHERE id=1").fetchone()
        pv["control"] = {"status": ctl["status"], "revision": ctl["revision"],
                         "control_generation": str(ctl["control_generation"])}
        if op in ("CLOSE_WINDOW", "EXTEND_WINDOW"):
            pv["window"] = _window_plan(ctx, ctl, req)
        elif op == "CONFIRM_RECOVERY":
            if ctl["status"] != "RECOVERY":
                raise ApiError(409, "NOT_IN_RECOVERY", "controller is not in recovery")
            pv["unknown_periods"] = ctl["unknown_periods"]
        elif op == "RESUME_CLUSTER":
            cluster_id = req["target_ids"][0]
            st = (ctl["cluster_state"] or {}).get(cluster_id) or {}
            pv["cluster"] = {"cluster_id": cluster_id, "hard_stop": bool(st.get("hard_stop"))}
            if not st.get("hard_stop"):
                warnings.append(f"{cluster_id} is not hard-stopped")
            cur.execute("""SELECT count(*) AS n FROM dq_attempt WHERE cluster_id=%s AND status IN
                           ('CANCEL_REQUESTED','CANCEL_PENDING')""", (cluster_id,))
            pv["cluster"]["unconfirmed_cancels"] = cur.fetchone()["n"]
    pv["warnings"] = warnings
    return pv


def _digest(pv: dict) -> bytes:
    basis = {k: pv.get(k) for k in ("op", "target_type", "target_ids", "effect", "check_selector", "params",
                                    "targets", "_change_digest", "future_policy", "regenerate", "control", "window",
                                    "cluster")}
    if basis["targets"] is not None:
        basis["targets"] = [{k: v for k, v in t.items() if k != "execution_state"} | (
            {"terminal": t.get("execution_state") in TERMINAL_RC + TERMINAL_RUN} if "execution_state" in t else {})
            for t in basis["targets"]]
    if basis["control"] is not None:
        basis["control"] = {k: v for k, v in basis["control"].items() if k != "revision"}
    if basis["cluster"] is not None:
        basis["cluster"] = {k: v for k, v in basis["cluster"].items() if k != "unconfirmed_cancels"}
    return digest_json(basis)


def _window_plan(ctx: Ctx, ctl: dict, req: dict) -> dict:
    now = ctx.now()
    ws = window.window_state(ctx.cfg, now, ctl["window_override"])
    if ws["state"] == "CLOSED":
        raise ApiError(409, "WINDOW_CLOSED", "the execution window is closed")
    out = {"key": ws["key"], "state": ws["state"], "close_at": iso(ws["close_at"])}
    if req["op"] == "CLOSE_WINDOW":
        out["new_close_at"] = iso(now)
        return out
    p = req["params"]
    until = parse_ts(p["until"]) if p.get("until") else ws["close_at"] + _dt.timedelta(minutes=p["minutes"])
    _o, _d, base_close = window.window_bounds(ctx.cfg, ws["key"], {})
    nxt = (_dt.date.fromisoformat(ws["key"]) + _dt.timedelta(days=1)).isoformat()
    next_open = window.window_bounds(ctx.cfg, nxt, {})[0]
    if until <= ws["close_at"]:
        raise ApiError(422, "BAD_EXTENSION", "the new close time must be later than the current one")
    if until > base_close + _dt.timedelta(hours=24) or until > next_open:
        raise ApiError(422, "BAD_EXTENSION", "a window can be extended by at most 24 hours and not into the next one")
    out["new_close_at"] = iso(until.astimezone(_dt.timezone.utc))
    return out


def _regenerate_plan(cur, req: dict, targets: list[dict]) -> dict:
    if req["target_type"] == "RUN":
        plans = []
        for r in targets:
            if r["execution_state"] not in TERMINAL_RUN:
                raise ApiError(409, "RUN_NOT_FINISHED", f"run {r['id']} is {r['execution_state']}; cancel it first")
            plans.append({"supersedes": str(r["id"]), "purpose": r["purpose"]})
        return {"runs": plans}
    assets = {str(t["asset_id"]) for t in targets}
    if len(assets) != 1:
        raise ApiError(422, "MULTI_ASSET", "regenerate run_checks of one asset per operation")
    rules = []
    for t in targets:
        cur.execute("SELECT id, revision FROM dq_check_rule WHERE asset_id=%s AND rule_key=%s AND status='APPROVED'",
                    (t["asset_id"], t["rule_key"]))
        r = cur.fetchone()
        if r is None:
            raise ApiError(422, "RULE_NOT_APPROVED", f"rule {t['rule_key']} has no approved revision")
        rules.append({"rule_key": t["rule_key"], "rule_id": str(r["id"]), "revision": r["revision"]})
    return {"asset_id": next(iter(assets)), "rules": sorted(rules, key=lambda x: x["rule_key"]),
            "scope_keys": sorted({t["scope_key"] for t in targets})}


def _public(pv: dict) -> dict:
    return {k: v for k, v in pv.items() if not k.startswith("_")}


def preview_operation(ctx: Ctx, body: dict) -> dict:
    req = _normalize(ctx, body)
    with ctx.db.tx() as cur:
        pv = _build_preview(cur, ctx, req)
        dig = _digest(pv)
        oid = new_id()
        cur.execute("""INSERT INTO dq_operation (id, op, target_type, target_ids, effect, check_selector, params,
                       preview, preview_digest) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (oid, req["op"], req["target_type"], jb(req["target_ids"]), req["effect"],
                     jb(req["check_selector"]), jb(req["params"]), jb(_public(pv)), dig))
    return {"id": str(oid), "status": "PREVIEWED", "preview_digest": dig.hex(), "preview": _public(pv)}


def get_operation(ctx: Ctx, op_id) -> dict:
    r = ctx.db.one("SELECT * FROM dq_operation WHERE id=%s", (as_uuid(op_id, "operation_id"),))
    if r is None:
        raise ApiError(404, "NOT_FOUND", "operation not found")
    return _op_out(r)


def _op_out(r: dict) -> dict:
    return {"id": str(r["id"]), "op": r["op"], "target_type": r["target_type"], "target_ids": r["target_ids"],
            "effect": r["effect"], "check_selector": r["check_selector"], "params": r["params"],
            "status": r["status"], "preview": r["preview"], "preview_digest": bytes(r["preview_digest"]).hex(),
            "result": r["result"], "reason": r["reason"], "created_at": iso(r["created_at"]),
            "applied_at": iso(r["applied_at"])}


def list_operations(ctx: Ctx, limit: int = 50) -> list[dict]:
    return [_op_out(r) for r in ctx.db.query("SELECT * FROM dq_operation ORDER BY created_at DESC LIMIT %s",
                                             (max(1, min(int(limit), 500)),))]


# ---- apply ---------------------------------------------------------------------------------------

def apply_operation(ctx: Ctx, op_id, *, idempotency_key: str | None, if_match: str | None,
                    reason: str | None) -> dict:
    from . import engine

    if not idempotency_key or len(idempotency_key) > 200:
        raise ApiError(422, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key header (1..200 chars) is required")
    if not reason or not reason.strip():
        raise ApiError(422, "REASON_REQUIRED", "a reason is required")
    oid = as_uuid(op_id, "operation_id")
    ex = ctx.db.one("SELECT * FROM dq_operation WHERE idempotency_key=%s", (idempotency_key,))
    if ex is not None:
        if ex["id"] == oid and ex["status"] == "APPLIED":
            return dict(ex["result"] or {}, idempotent_replay=True)
        raise ApiError(409, "IDEMPOTENCY_CONFLICT", "Idempotency-Key was used for another operation")
    stale = None
    to_eval: list = []
    post: dict = {}
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        cur.execute("SELECT * FROM dq_operation WHERE id=%s FOR UPDATE", (oid,))
        row = cur.fetchone()
        if row is None:
            raise ApiError(404, "NOT_FOUND", "operation not found")
        if row["status"] != "PREVIEWED":
            raise ApiError(409, "OPERATION_NOT_PREVIEWED", f"operation is {row['status']}; preview again")
        if ctl["status"] == "RECOVERY" and row["op"] != "CONFIRM_RECOVERY":
            raise ApiError(503, "RECOVERY", "controller is in recovery; confirm recovery first")
        if if_match is None or str(if_match).strip().strip('"') != bytes(row["preview_digest"]).hex():
            raise ApiError(409, "PRECONDITION_FAILED", "If-Match must carry the preview digest")
        req = {k: row[k] for k in ("op", "target_type", "target_ids", "effect", "check_selector", "params")}
        pv = _build_preview(cur, ctx, req, lock=True)
        if _digest(pv) != bytes(row["preview_digest"]):
            cur.execute("UPDATE dq_operation SET status='STALE' WHERE id=%s", (oid,))
            stale = _public(pv)
        else:
            result = _apply(cur, ctx, oid, req, pv, reason.strip(), to_eval, post)
            cur.execute("""UPDATE dq_operation SET status='APPLIED', idempotency_key=%s, result=%s, reason=%s,
                           applied_at=now() WHERE id=%s""", (idempotency_key, jb(result), reason.strip(), oid))
            audit(cur, "OPERATION_APPLIED", "operation", oid,
                  {"op": req["op"], "target_type": req["target_type"], "target_ids": req["target_ids"],
                   "effect": req["effect"], "summary": {k: v for k, v in result.items() if k != "changes"}},
                  reason.strip())
    if stale is not None:
        raise ApiError(409, "PREVIEW_STALE", "targets changed since the preview; preview again", {"preview": stale})
    for o in dict.fromkeys(to_eval):
        evaluate.evaluate_observation(ctx, o)
    if post.get("step_window"):
        engine.step_window(ctx)
    if post.get("complete_runs"):
        engine.step_complete_runs(ctx)
    return result


def _apply(cur, ctx: Ctx, op_id, req: dict, pv: dict, reason: str, to_eval: list, post: dict) -> dict:
    op, tt = req["op"], req["target_type"]
    if op == "CLOSE_WINDOW":
        return _apply_window(cur, ctx, pv, "close_now_at", post)
    if op == "EXTEND_WINDOW":
        return _apply_window(cur, ctx, pv, "extend_until", post)
    if op == "CONFIRM_RECOVERY":
        cur.execute("""UPDATE dq_control_state SET status='ACTIVE', revision=revision+1, updated_at=now()
                       WHERE id=1""")
        close_incident(cur, "RECOVERY", "recovery")
        audit(cur, "RECOVERY_CONFIRMED", "control", 1, {"operation_id": str(op_id)}, reason)
        return {"status": "ACTIVE"}
    if op == "RESUME_CLUSTER":
        st = loadguard.resume_cluster(cur, req["target_ids"][0], reason)
        return {"cluster_id": req["target_ids"][0], "hard_stop": False}
    targets = _load_targets(cur, req, True)
    if op == "REGENERATE":
        return _apply_regenerate(cur, ctx, req, targets, pv["regenerate"], reason)
    rcs = _affected_rcs(cur, req, targets, True)
    table, holders = _holders(cur, req, targets, rcs)
    changes = _changes(req, table, holders)
    for ch in changes:
        extra = ", updated_at=now()" if table == "dq_run_check" else ""
        cur.execute(f"UPDATE {table} SET {ch['field']}=%s, revision=revision+1{extra} WHERE id=%s",
                    (ch["to"], ch["id"]))
    policies = []
    for f in _future_preview(cur, req, targets, True):
        if f["from"] == f["to"] and len(f["revokes"]) <= 1:
            continue
        policies.append(_write_policy(cur, f, op_id, reason))
    out: dict = {"changed": len(changes), "policies": policies}
    run_ids = sorted({str(r["run_id"]) for r in rcs} | ({str(t["id"]) for t in targets} if tt == "RUN" else set()))
    if op == "PAUSE":
        cur.execute("""UPDATE dq_observation SET drain=true WHERE run_check_id = ANY(%s::uuid[]) AND status='OPEN'
                       AND started_at IS NOT NULL RETURNING id""", ([str(r["id"]) for r in rcs],))
        out["drain_observations"] = [str(r["id"]) for r in cur.fetchall()]
    elif op in ("START", "RESUME"):
        cur.execute("""UPDATE dq_run SET execution_state='READY', revision=revision+1
                       WHERE id = ANY(%s::uuid[]) AND execution_state='PLANNED'""", (run_ids,))
        cur.execute("""UPDATE dq_run_check SET execution_state='READY', revision=revision+1, updated_at=now()
                       WHERE id = ANY(%s::uuid[]) AND execution_state='PLANNED'""", ([str(r["id"]) for r in rcs],))
    elif op in ("CANCEL", "ARCHIVE"):
        out.update(_stop(cur, ctx, rcs, run_ids if tt == "RUN" else [], "OPERATION_CANCEL", to_eval))
        post["complete_runs"] = True
    elif op == "RESTORE":
        out["restored_checks"] = _restore(cur, rcs)
    cur.execute("UPDATE dq_control_state SET queue_revision=queue_revision+1 WHERE id=1")
    out["changes"] = changes[:MAX_LISTED]
    return out


def _write_policy(cur, f: dict, op_id, reason: str) -> dict:
    aid, nk, sk, sk_digest, sel = f["_key"]
    if f["revokes"]:
        cur.execute("UPDATE dq_scope_policy SET status='REVOKED', revoked_at=now() WHERE id = ANY(%s::uuid[])",
                    (f["revokes"],))
    to = f["to"]
    pid = None
    if to["control_state"] is not None or to["priority_override"] is not None:
        pid = new_id()
        cur.execute("""INSERT INTO dq_scope_policy (id, asset_id, scope_key, scope_key_digest, node_kind,
                       check_selector, control_state, priority_override, operation_id, reason)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (pid, aid, sk, sk_digest, nk, jb(sel), to["control_state"], to["priority_override"], op_id,
                     reason))
    audit(cur, "SCOPE_POLICY_CHANGED", "scope_policy", pid or (f["revokes"][-1] if f["revokes"] else None),
          {"asset_id": aid, "node_kind": nk, "scope_key": sk, "check_selector": sel, "to": to,
           "revoked": f["revokes"]}, reason)
    return {"id": None if pid is None else str(pid), "asset_id": aid, "node_kind": nk, "scope_key": sk,
            "check_selector": sel, **to, "revoked": f["revokes"]}


def _stop(cur, ctx: Ctx, rcs: list[dict], run_ids: list[str], reason: str, to_eval: list) -> dict:
    """Cancel in-flight work of run_checks whose control is now CANCELLED/ARCHIVED (no drain exception)."""
    live_rcs = [r for r in rcs if r["execution_state"] not in TERMINAL_RC]
    ids = [str(r["id"]) for r in live_rcs]
    failure = {"error_class": "CANCELLED", "error_code": reason, "message": "cancelled by operator"}
    cur.execute("""UPDATE dq_work_item w SET status='ABANDONED', updated_at=now(),
                   result = COALESCE(w.result, '{}'::jsonb) || jsonb_build_object('failure', %s::jsonb)
                   FROM dq_observation o WHERE o.id = w.observation_id AND o.status='OPEN' AND w.status='READY'
                     AND w.run_check_id = ANY(%s::uuid[]) RETURNING w.observation_id""", (jb(failure), ids))
    oids = {r["observation_id"] for r in cur.fetchall()}
    cur.execute("""SELECT a.id, w.observation_id FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                   WHERE w.run_check_id = ANY(%s::uuid[]) AND a.status IN ('LEASED','RUNNING')""", (ids,))
    cancelled = 0
    for r in cur.fetchall():
        cancelled += int(queue.request_cancel(cur, r["id"], reason))
        oids.add(r["observation_id"])
    pending = done = 0
    for rc in live_rcs:
        cur.execute("SELECT 1 FROM dq_observation WHERE run_check_id=%s AND status='OPEN' LIMIT 1", (rc["id"],))
        busy = cur.fetchone() is not None or evaluate.rc_has_live(cur, rc["id"])
        state = "CANCEL_PENDING" if busy else "CANCELLED"
        pending += busy
        done += not busy
        cur.execute("""UPDATE dq_run_check SET execution_state=%s, revision=revision+1, updated_at=now()
                       WHERE id=%s""", (state, rc["id"]))
    for rid in run_ids:
        cur.execute("""UPDATE dq_run SET execution_state='CANCEL_PENDING', revision=revision+1
                       WHERE id=%s AND execution_state NOT IN ('COMPLETED','FAILED','CANCELLED')
                       AND EXISTS (SELECT 1 FROM dq_work_item w JOIN dq_attempt a ON a.id = w.current_attempt_id
                                   WHERE w.run_id=%s AND a.status IN
                                   ('LEASED','RUNNING','CANCEL_REQUESTED','CANCEL_PENDING'))""", (rid, rid))
    to_eval.extend(sorted(oids, key=str))
    return {"cancel_requested": cancelled, "checks_cancel_pending": pending, "checks_cancelled": done}


def _restore(cur, rcs: list[dict]) -> int:
    n = 0
    runs = set()
    for rc in rcs:
        if rc["execution_state"] != "CANCELLED":
            continue
        cur.execute("SELECT * FROM dq_run_check WHERE id=%s", (rc["id"],))
        fresh = cur.fetchone()
        if evaluate.effective_control(cur, fresh) in evaluate.STOPPED:
            continue
        cur.execute("""UPDATE dq_run_check SET execution_state='READY', next_eligible_at=NULL, revision=revision+1,
                       updated_at=now() WHERE id=%s""", (rc["id"],))
        runs.add(str(rc["run_id"]))
        n += 1
    for rid in sorted(runs):
        cur.execute("""UPDATE dq_run SET execution_state='READY', completed_at=NULL, revision=revision+1
                       WHERE id=%s AND execution_state IN ('COMPLETED','CANCELLED','CANCEL_PENDING')""", (rid,))
        cur.execute("UPDATE dq_budget SET budget_state='OPEN', updated_at=now() WHERE run_id=%s "
                    "AND budget_state='CLOSED'", (rid,))
    return n


def _apply_window(cur, ctx: Ctx, pv: dict, field: str, post: dict) -> dict:
    ctl = lock_control(cur)
    w = pv["window"]
    ov = dict(ctl["window_override"] or {})
    entry = dict(ov.get(w["key"]) or {})
    entry[field] = w["new_close_at"]
    ov[w["key"]] = entry
    cur.execute("UPDATE dq_control_state SET window_override=%s, revision=revision+1, updated_at=now() WHERE id=1",
                (jb(ov),))
    post["step_window"] = True
    return {"window_key": w["key"], "close_at": w["new_close_at"], "previous_close_at": w["close_at"]}


def _apply_regenerate(cur, ctx: Ctx, req: dict, targets: list[dict], plan: dict, reason: str) -> dict:
    runs = []
    if req["target_type"] == "RUN":
        for r in targets:
            cur.execute("SELECT DISTINCT asset_id FROM dq_run_check WHERE run_id=%s", (r["id"],))
            aids = [str(x["asset_id"]) for x in cur.fetchall()]
            assets = planning.select_assets(cur, {"asset_ids": aids}) if aids else []
            if r["purpose"] == "MANUAL":
                cur.execute("""SELECT DISTINCT rc.rule_id, rc.scope_key, rc.execution_snapshot->>'detail_approval_ref' AS d
                               FROM dq_run_check rc JOIN dq_check_rule cr ON cr.id = rc.rule_id
                               WHERE rc.run_id=%s AND cr.status='APPROVED'""", (r["id"],))
                rows = cur.fetchall()
                if not rows:
                    raise ApiError(422, "RULE_NOT_APPROVED", "the rules of this run are no longer approved")
                runs.append(planning.build_run(
                    cur, ctx, purpose="MANUAL", manifest_id=r["manifest_id"], assets=assets, selector=None,
                    plan_id=r["plan_id"], reason=reason, supersedes=r["id"], manual_request_id=r["manual_request_id"],
                    rule_ids=[x["rule_id"] for x in rows], scope_keys={x["scope_key"] for x in rows},
                    detail_approval_ref=next((x["d"] for x in rows if x["d"]), None)))
            else:
                runs.append(planning.build_run(
                    cur, ctx, purpose="COMPARE", manifest_id=r["manifest_id"], assets=assets,
                    selector=(r["config_snapshot"] or {}).get("selector") or {"all": True}, plan_id=r["plan_id"],
                    reason=reason, supersedes=r["id"]))
    else:
        assets = planning.select_assets(cur, {"asset_ids": [plan["asset_id"]]})
        dref = next((t["execution_snapshot"].get("detail_approval_ref") for t in targets
                     if t["execution_snapshot"].get("detail_approval_ref")), None)
        cur.execute("SELECT manifest_id, plan_id FROM dq_run WHERE id=%s", (targets[0]["run_id"],))
        src = cur.fetchone()
        runs.append(planning.build_run(
            cur, ctx, purpose="MANUAL", manifest_id=src["manifest_id"], assets=assets, selector=None,
            plan_id=src["plan_id"], reason=reason, supersedes=targets[0]["run_id"],
            rule_ids=[x["rule_id"] for x in plan["rules"]], scope_keys=set(plan["scope_keys"]),
            detail_approval_ref=dref))
    return {"runs": runs}


# ---- helpers for other modules -------------------------------------------------------------------

def cancel_run(cur, ctx: Ctx, run_id, reason: str) -> list:
    """Cancel one run in the caller's transaction (control lock held). Returns observations to evaluate."""
    cur.execute("SELECT * FROM dq_run WHERE id=%s FOR UPDATE", (run_id,))
    run = cur.fetchone()
    if run is None or run["control_state"] in ("CANCELLED", "ARCHIVED") or run["execution_state"] in TERMINAL_RUN:
        return []
    cur.execute("UPDATE dq_run SET control_state='CANCELLED', revision=revision+1 WHERE id=%s", (run_id,))
    cur.execute("SELECT * FROM dq_run_check WHERE run_id=%s ORDER BY id FOR UPDATE", (run_id,))
    to_eval: list = []
    summary = _stop(cur, ctx, cur.fetchall(), [str(run_id)], reason, to_eval)
    audit(cur, "RUN_CANCELLED", "run", run_id, {"reason_code": reason, **summary}, actor="controller")
    return to_eval


def enter_recovery(ctx: Ctx, reason: str) -> dict:
    """After restoring the controller database from a backup: new generation, old leases void (doc 03 §14.4)."""
    if not reason or not reason.strip():
        raise ApiError(422, "REASON_REQUIRED", "a reason is required")
    to_eval = []
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        gen = new_id()
        cur.execute("SELECT max(created_at) AS t FROM dq_outbox")
        last = cur.fetchone()["t"]
        periods = list(ctl["unknown_periods"] or []) + [{"from": iso(last), "to": iso(ctx.now()),
                                                         "reason": reason.strip()[:500]}]
        cur.execute("""UPDATE dq_control_state SET status='RECOVERY', control_generation=%s, unknown_periods=%s,
                       revision=revision+1, updated_at=now() WHERE id=1""", (gen, jb(periods)))
        cur.execute("UPDATE dq_worker_registration SET status='SUPERSEDED' WHERE status='ACTIVE'")
        cur.execute("""UPDATE dq_attempt SET status='CANCEL_PENDING', cancel_reason=COALESCE(cancel_reason, 'RECOVERY')
                       WHERE status IN ('LEASED','RUNNING','CANCEL_REQUESTED') RETURNING id, work_item_id""")
        live = cur.fetchall()
        for a in live:
            cur.execute("UPDATE dq_work_item SET status='CANCEL_PENDING', updated_at=now() "
                        "WHERE id=%s AND current_attempt_id=%s", (a["work_item_id"], a["id"]))
        failure = {"error_class": "CANCELLED", "error_code": "RECOVERY", "message": "controller recovery"}
        cur.execute("""UPDATE dq_work_item w SET status='ABANDONED', updated_at=now(),
                       result = COALESCE(w.result, '{}'::jsonb) || jsonb_build_object('failure', %s::jsonb)
                       FROM dq_observation o WHERE o.id = w.observation_id AND o.status='OPEN' AND w.status='READY'
                       RETURNING w.observation_id""", (jb(failure),))
        to_eval = sorted({r["observation_id"] for r in cur.fetchall()}, key=str)
        cur.execute("UPDATE dq_work_item SET status='READY', updated_at=now() WHERE kind='METADATA' AND status='LEASED'")
        incident(cur, "RECOVERY", "recovery", "controller restored from backup; results since the backup are not "
                 "adopted and the gap is reported as UNKNOWN", severity="CRITICAL",
                 detail={"unknown_from": iso(last), "unknown_to": iso(ctx.now()), "live_attempts": len(live)})
        audit(cur, "RECOVERY_ENTERED", "control", 1, {"control_generation": str(gen), "unknown_from": iso(last),
                                                      "live_attempts": len(live)}, reason.strip())
    for o in to_eval:
        evaluate.evaluate_observation(ctx, o)
    return {"status": "RECOVERY", "control_generation": str(gen), "unknown_period": periods[-1],
            "cancel_pending_attempts": len(live)}
