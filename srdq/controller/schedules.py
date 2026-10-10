"""Schedules (doc 03 §10): CRON / INTERVAL / CONTINUOUS, overlap and misfire policies, bounded error backoff."""
from __future__ import annotations

import datetime as _dt
import logging
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..db.db import jb, new_id
from . import planning
from .common import ApiError, Ctx, as_uuid, audit, iso, lock_control, parse_ts

log = logging.getLogger("srdq.schedules")

UTC = _dt.timezone.utc
TERMINAL_RUN = ("COMPLETED", "FAILED", "CANCELLED")
MAX_BACKOFF_SECONDS = 3600
MAX_DUE_SCAN = 10000
ACTIONS = ("ENABLE", "DISABLE", "ARCHIVE", "RESTART")

# ---- cron ----------------------------------------------------------------------------------------

_MONTHS = {m: i + 1 for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split())}
_DAYS = {d: i for i, d in enumerate("SUN MON TUE WED THU FRI SAT".split())}
_FIELDS = (("minute", 0, 59, {}), ("hour", 0, 23, {}), ("day of month", 1, 31, {}), ("month", 1, 12, _MONTHS),
           ("day of week", 0, 7, _DAYS))


def _num(tok: str, lo: int, hi: int, names: dict, label: str) -> int:
    v = names.get(tok.upper()) if not tok.isdigit() else int(tok)
    if v is None or not lo <= v <= hi:
        raise ApiError(422, "BAD_CRON", f"cron {label}: {tok!r} is out of range {lo}-{hi}")
    return v


def _field(text: str, lo: int, hi: int, names: dict, label: str) -> set[int]:
    out: set[int] = set()
    for part in text.split(","):
        if not part:
            raise ApiError(422, "BAD_CRON", f"cron {label}: empty list element")
        rng, _, step_s = part.partition("/")
        step = 1
        if step_s:
            if not step_s.isdigit() or int(step_s) < 1:
                raise ApiError(422, "BAD_CRON", f"cron {label}: bad step {step_s!r}")
            step = int(step_s)
        if rng == "*":
            a, b = lo, hi
        elif "-" in rng:
            x, _, y = rng.partition("-")
            a, b = _num(x, lo, hi, names, label), _num(y, lo, hi, names, label)
            if a > b:
                raise ApiError(422, "BAD_CRON", f"cron {label}: range {rng!r} is reversed")
        else:
            a = _num(rng, lo, hi, names, label)
            b = hi if step_s else a
        out.update(range(a, b + 1, step))
    return out


class Cron:
    """Five-field cron (minute hour dom month dow) with Vixie semantics: when both day fields are restricted,
    a day matches if either does. Local times that do not exist (DST gap) are skipped; ambiguous local times
    (DST fold) fire once, at the first occurrence."""

    def __init__(self, expr: str):
        if not isinstance(expr, str):
            raise ApiError(422, "BAD_CRON", "cron_expr must be a string")
        parts = expr.split()
        if len(parts) != 5:
            raise ApiError(422, "BAD_CRON", "cron_expr needs 5 fields: minute hour day-of-month month day-of-week")
        sets = [_field(p, lo, hi, names, label) for p, (label, lo, hi, names) in zip(parts, _FIELDS)]
        self.expr = " ".join(parts)
        self.minutes, self.hours, self.dom, self.months, dow = (sorted(s) for s in sets)
        self.dow = {d % 7 for d in dow}
        self.dom_star = parts[2].startswith("*")
        self.dow_star = parts[4].startswith("*")

    def _day_ok(self, d: _dt.date) -> bool:
        if d.month not in self.months:
            return False
        dom_ok = d.day in self.dom
        dow_ok = (d.weekday() + 1) % 7 in self.dow
        if self.dom_star and self.dow_star:
            return True
        if self.dom_star:
            return dow_ok
        if self.dow_star:
            return dom_ok
        return dom_ok or dow_ok

    def next_after(self, after: _dt.datetime, tz: ZoneInfo) -> _dt.datetime | None:
        local = after.astimezone(tz)
        day = local.date()
        for _ in range(366 * 5):
            if self._day_ok(day):
                for h in self.hours:
                    for m in self.minutes:
                        naive = _dt.datetime(day.year, day.month, day.day, h, m)
                        cand = naive.replace(tzinfo=tz, fold=0)
                        u = cand.astimezone(UTC)
                        if u.astimezone(tz).replace(tzinfo=None) != naive:
                            continue
                        if u > after:
                            return u
            day += _dt.timedelta(days=1)
        return None


def _tz(name: Any) -> ZoneInfo:
    try:
        return ZoneInfo(str(name))
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ApiError(422, "BAD_TIMEZONE", f"unknown IANA timezone {name!r}") from e


# ---- due-time arithmetic -------------------------------------------------------------------------

def next_due(s: dict, after: _dt.datetime) -> _dt.datetime | None:
    """First fire time strictly after `after` (and not before starts_at), or None when the schedule has ended."""
    starts, ends = s["starts_at"], s["ends_at"]
    if s["kind"] == "CRON":
        t = Cron(s["cron_expr"]).next_after(max(after, starts - _dt.timedelta(microseconds=1)), _tz(s["timezone"]))
    elif s["kind"] == "INTERVAL":
        step = _dt.timedelta(seconds=int(s["interval_seconds"]))
        if after < starts:
            t = starts
        else:
            t = starts + step * ((after - starts) // step + 1)
    else:
        raise AssertionError("CONTINUOUS schedules are driven by run completion")
    if t is None or (ends is not None and t >= ends):
        return None
    return t


def preview_fires(s: dict, now: _dt.datetime, n: int = 5) -> list[str]:
    if s["kind"] == "CONTINUOUS":
        return []
    out, t = [], max(now, s["starts_at"]) - _dt.timedelta(microseconds=1)
    for _ in range(n):
        t = next_due(s, t)
        if t is None:
            break
        out.append(iso(t))
    return out


# ---- create / update / actions -------------------------------------------------------------------

def _schedule_out(s: dict, now: _dt.datetime | None = None, run: dict | None = None) -> dict:
    out = {k: s[k] for k in ("name", "kind", "cron_expr", "interval_seconds", "rest_seconds", "timezone",
                             "overlap", "misfire", "cancel_on_end", "check_selector", "enabled", "status",
                             "revision", "coalesced", "consecutive_errors")}
    out.update(id=str(s["id"]), plan_id=str(s["plan_id"]), starts_at=iso(s["starts_at"]), ends_at=iso(s["ends_at"]),
               next_due_at=iso(s["next_due_at"]), backoff_until=iso(s["backoff_until"]),
               last_run_id=None if s["last_run_id"] is None else str(s["last_run_id"]))
    if now is not None:
        out["next_fires"] = preview_fires(s, now) if s["status"] == "ACTIVE" and s["enabled"] else []
    out["waiting_reason"] = _waiting_reason(s, run)
    return out


def _waiting_reason(s: dict, run: dict | None) -> str | None:
    if s["status"] != "ACTIVE":
        return s["status"]
    if not s["enabled"]:
        return "DISABLED"
    if run is not None and run["execution_state"] not in TERMINAL_RUN:
        return "RUN_IN_FLIGHT"
    if s["kind"] == "CONTINUOUS" and s["next_due_at"] is None and run is not None:
        if run["execution_state"] != "COMPLETED" or run["control_state"] != "ENABLED":
            return f"LAST_RUN_{run['execution_state']}_{run['control_state']}: restart required"
    if s["backoff_until"] is not None:
        return "ERROR_BACKOFF"
    return None


def _fields(ctx: Ctx, cur, body: dict, base: dict | None) -> dict:
    d = ctx.cfg["schedule_defaults"]
    base = base or {}

    def pick(k, default=None):
        return body[k] if k in body else base.get(k, default)

    name = pick("name")
    if not isinstance(name, str) or not 1 <= len(name.strip()) <= 128:
        raise ApiError(422, "BAD_NAME", "name must be 1..128 characters")
    pid = as_uuid(pick("plan_id"), "plan_id")
    cur.execute("SELECT * FROM dq_plan WHERE id=%s", (pid,))
    plan = cur.fetchone()
    if plan is None:
        raise ApiError(404, "NOT_FOUND", "plan not found")
    if plan["status"] != "ACTIVE":
        raise ApiError(409, "PLAN_ARCHIVED", "plan is archived")
    kind = pick("kind")
    if kind not in ("CRON", "INTERVAL", "CONTINUOUS"):
        raise ApiError(422, "BAD_KIND", "kind must be CRON, INTERVAL or CONTINUOUS")
    f: dict = {"name": name.strip(), "plan_id": pid, "kind": kind, "cron_expr": None, "interval_seconds": None,
               "rest_seconds": None}
    if kind == "CRON":
        f["cron_expr"] = Cron(pick("cron_expr")).expr
    elif kind == "INTERVAL":
        v = pick("interval_seconds")
        if not isinstance(v, int) or isinstance(v, bool) or v < 60:
            raise ApiError(422, "BAD_INTERVAL", "interval_seconds must be an integer >= 60")
        f["interval_seconds"] = v
    else:
        v = pick("rest_seconds", d["continuous_rest_seconds"])
        if not isinstance(v, int) or isinstance(v, bool) or v < 60:
            raise ApiError(422, "BAD_REST", "rest_seconds must be an integer >= 60")
        f["rest_seconds"] = v
    tzname = pick("timezone", d["timezone"])
    _tz(tzname)
    f["timezone"] = tzname
    starts = pick("starts_at")
    f["starts_at"] = ctx.now() if starts is None else parse_ts(starts, "starts_at")
    ends = pick("ends_at")
    f["ends_at"] = None if ends is None else parse_ts(ends, "ends_at")
    if f["ends_at"] is not None and f["ends_at"] <= f["starts_at"]:
        raise ApiError(422, "BAD_ENDS_AT", "ends_at must be after starts_at")
    f["overlap"] = pick("overlap", d["overlap"])
    if f["overlap"] not in ("SKIP", "COALESCE"):
        raise ApiError(422, "BAD_OVERLAP", "overlap must be SKIP or COALESCE")
    f["misfire"] = pick("misfire", d["misfire"])
    if f["misfire"] not in ("SKIP", "LATEST"):
        raise ApiError(422, "BAD_MISFIRE", "misfire must be SKIP or LATEST")
    coe = pick("cancel_on_end", d["cancel_on_end"])
    if not isinstance(coe, bool):
        raise ApiError(422, "BAD_CANCEL_ON_END", "cancel_on_end must be a boolean")
    f["cancel_on_end"] = coe
    sel = pick("check_selector")
    f["check_selector"] = plan["check_selector"] if sel is None else planning.validate_check_selector(sel)
    en = pick("enabled", True)
    if not isinstance(en, bool):
        raise ApiError(422, "BAD_ENABLED", "enabled must be a boolean")
    f["enabled"] = en
    return f


def _initial_due(f: dict, now: _dt.datetime) -> _dt.datetime | None:
    if f["kind"] == "CONTINUOUS":
        return max(now, f["starts_at"])
    return next_due(f, now - _dt.timedelta(microseconds=1))


def create_schedule(ctx: Ctx, body: dict, reason: str | None = None) -> dict:
    if not isinstance(body, dict):
        raise ApiError(422, "BAD_BODY", "body must be an object")
    with ctx.db.tx() as cur:
        lock_control(cur)
        f = _fields(ctx, cur, body, None)
        cur.execute("SELECT 1 FROM dq_schedule WHERE name=%s", (f["name"],))
        if cur.fetchone():
            raise ApiError(409, "NAME_TAKEN", "a schedule with this name exists")
        sid = new_id()
        cur.execute("""INSERT INTO dq_schedule (id, plan_id, name, kind, cron_expr, interval_seconds, rest_seconds,
                       timezone, starts_at, ends_at, overlap, misfire, cancel_on_end, check_selector, enabled,
                       next_due_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                    (sid, f["plan_id"], f["name"], f["kind"], f["cron_expr"], f["interval_seconds"], f["rest_seconds"],
                     f["timezone"], f["starts_at"], f["ends_at"], f["overlap"], f["misfire"], f["cancel_on_end"],
                     jb(f["check_selector"]), f["enabled"], _initial_due(f, ctx.now())))
        s = cur.fetchone()
        out = _schedule_out(s, ctx.now())
        audit(cur, "SCHEDULE_CREATED", "schedule", sid, out, reason)
        return out


def _lock_schedule(cur, sid) -> dict:
    cur.execute("SELECT * FROM dq_schedule WHERE id=%s FOR UPDATE", (sid,))
    s = cur.fetchone()
    if s is None:
        raise ApiError(404, "NOT_FOUND", "schedule not found")
    return s


def _check_revision(s: dict, if_match: Any) -> None:
    if if_match is None or str(if_match).strip().strip('"') != str(s["revision"]):
        raise ApiError(409, "REVISION_MISMATCH", "If-Match does not match the schedule revision",
                       {"revision": s["revision"]})


def update_schedule(ctx: Ctx, schedule_id, body: dict, if_match: Any, reason: str | None = None) -> dict:
    sid = as_uuid(schedule_id, "schedule_id")
    if not isinstance(body, dict):
        raise ApiError(422, "BAD_BODY", "body must be an object")
    with ctx.db.tx() as cur:
        lock_control(cur)
        s = _lock_schedule(cur, sid)
        _check_revision(s, if_match)
        if s["status"] == "ARCHIVED":
            raise ApiError(409, "SCHEDULE_ARCHIVED", "schedule is archived")
        f = _fields(ctx, cur, body, s)
        if f["name"] != s["name"]:
            cur.execute("SELECT 1 FROM dq_schedule WHERE name=%s", (f["name"],))
            if cur.fetchone():
                raise ApiError(409, "NAME_TAKEN", "a schedule with this name exists")
        now = ctx.now()
        status = "ACTIVE" if s["status"] == "ENDED" and (f["ends_at"] is None or f["ends_at"] > now) else s["status"]
        due = s["next_due_at"] if f["kind"] == "CONTINUOUS" and s["kind"] == "CONTINUOUS" else _initial_due(f, now)
        if f["kind"] == "CONTINUOUS" and s["kind"] != "CONTINUOUS":
            due = _initial_due(f, now) if _in_flight(cur, s) is None else None
        cur.execute("""UPDATE dq_schedule SET plan_id=%s, name=%s, kind=%s, cron_expr=%s, interval_seconds=%s,
                       rest_seconds=%s, timezone=%s, starts_at=%s, ends_at=%s, overlap=%s, misfire=%s,
                       cancel_on_end=%s, check_selector=%s, enabled=%s, status=%s, next_due_at=%s, coalesced=NULL,
                       revision=revision+1, updated_at=now() WHERE id=%s RETURNING *""",
                    (f["plan_id"], f["name"], f["kind"], f["cron_expr"], f["interval_seconds"], f["rest_seconds"],
                     f["timezone"], f["starts_at"], f["ends_at"], f["overlap"], f["misfire"], f["cancel_on_end"],
                     jb(f["check_selector"]), f["enabled"], status, due, sid))
        out = _schedule_out(cur.fetchone(), now)
        audit(cur, "SCHEDULE_UPDATED", "schedule", sid, out, reason)
        return out


def schedule_action(ctx: Ctx, schedule_id, action: str, if_match: Any, reason: str | None = None) -> dict:
    sid = as_uuid(schedule_id, "schedule_id")
    if action not in ACTIONS:
        raise ApiError(422, "BAD_ACTION", f"action must be one of {ACTIONS}")
    with ctx.db.tx() as cur:
        lock_control(cur)
        s = _lock_schedule(cur, sid)
        _check_revision(s, if_match)
        now = ctx.now()
        if s["status"] == "ARCHIVED" and action != "ARCHIVE":
            raise ApiError(409, "SCHEDULE_ARCHIVED", "schedule is archived")
        sets: dict = {}
        if action == "ENABLE":
            sets["enabled"] = True
            if s["kind"] != "CONTINUOUS":
                sets["next_due_at"] = next_due(s, now - _dt.timedelta(microseconds=1))
                sets["coalesced"] = None
        elif action == "DISABLE":
            sets["enabled"] = False
        elif action == "ARCHIVE":
            sets.update(status="ARCHIVED", enabled=False, next_due_at=None, coalesced=None)
        else:
            if s["status"] == "ENDED":
                raise ApiError(409, "SCHEDULE_ENDED", "schedule has ended; move ends_at first")
            run = _in_flight(cur, s)
            if run is not None:
                raise ApiError(409, "RUN_IN_FLIGHT", "the last run is still in flight", {"run_id": str(run["id"])})
            sets.update(consecutive_errors=0, backoff_until=None, coalesced=None,
                        next_due_at=max(now, s["starts_at"]) if s["kind"] == "CONTINUOUS"
                        else next_due(s, now - _dt.timedelta(microseconds=1)))
        cols = ", ".join(f"{k}=%s" for k in sets)
        vals = [jb(v) if k == "coalesced" and v is not None else v for k, v in sets.items()]
        cur.execute(f"UPDATE dq_schedule SET {cols}, revision=revision+1, updated_at=now() WHERE id=%s RETURNING *",
                    vals + [sid])
        out = _schedule_out(cur.fetchone(), now)
        audit(cur, f"SCHEDULE_{action}", "schedule", sid, out, reason)
        return out


def get_schedule(ctx: Ctx, schedule_id) -> dict:
    sid = as_uuid(schedule_id, "schedule_id")
    s = ctx.db.one("SELECT * FROM dq_schedule WHERE id=%s", (sid,))
    if s is None:
        raise ApiError(404, "NOT_FOUND", "schedule not found")
    run = None if s["last_run_id"] is None else ctx.db.one("SELECT * FROM dq_run WHERE id=%s", (s["last_run_id"],))
    out = _schedule_out(s, ctx.now(), run)
    out["fires"] = [_fire_out(f) for f in ctx.db.query(
        "SELECT * FROM dq_schedule_fire WHERE schedule_id=%s ORDER BY fired_at DESC, due_at DESC LIMIT 50", (sid,))]
    return out


def list_schedules(ctx: Ctx) -> list[dict]:
    rows = ctx.db.query("""SELECT s.*, r.execution_state AS run_execution_state, r.control_state AS run_control_state
                           FROM dq_schedule s LEFT JOIN dq_run r ON r.id = s.last_run_id
                           ORDER BY s.status, s.name""")
    out = []
    for s in rows:
        run = None if s["run_execution_state"] is None else {"execution_state": s["run_execution_state"],
                                                             "control_state": s["run_control_state"]}
        out.append(_schedule_out(s, ctx.now(), run))
    return out


def _fire_out(f: dict) -> dict:
    return {"id": str(f["id"]), "revision": f["revision"], "fire_key": f["fire_key"], "due_at": iso(f["due_at"]),
            "fired_at": iso(f["fired_at"]), "outcome": f["outcome"],
            "run_id": None if f["run_id"] is None else str(f["run_id"]), "detail": f["detail"]}


def preview_schedule(ctx: Ctx, body: dict) -> dict:
    """Validate a schedule body without saving it and show the next five fire times."""
    with ctx.db.tx() as cur:
        f = _fields(ctx, cur, body, None)
    return {"kind": f["kind"], "timezone": f["timezone"], "next_fires": preview_fires(f, ctx.now())}


# ---- tick ----------------------------------------------------------------------------------------

def _in_flight(cur, s: dict) -> dict | None:
    if s["last_run_id"] is None:
        return None
    cur.execute("SELECT * FROM dq_run WHERE id=%s", (s["last_run_id"],))
    run = cur.fetchone()
    return run if run is not None and run["execution_state"] not in TERMINAL_RUN else None


def tick(ctx: Ctx) -> dict:
    if not ctx.cfg["run_enabled"]:
        return {}
    out: dict = {}
    grace = max(60.0, 2 * float(ctx.cfg["controller"]["tick_seconds"]))
    for r in ctx.db.query("SELECT id FROM dq_schedule WHERE status='ACTIVE' ORDER BY name"):
        to_eval: list = []
        with ctx.db.tx() as cur:
            lock_control(cur)
            s = _lock_schedule(cur, r["id"])
            if s["status"] != "ACTIVE":
                continue
            res = _tick_one(cur, ctx, s, grace, to_eval)
        if res:
            out[str(r["id"])] = res
        if to_eval:
            from . import evaluate
            for oid in to_eval:
                evaluate.evaluate_observation(ctx, oid)
    return out


def _tick_one(cur, ctx: Ctx, s: dict, grace: float, to_eval: list) -> list[str]:
    from . import operations

    now = ctx.now()
    if s["ends_at"] is not None and now >= s["ends_at"]:
        run = _in_flight(cur, s)
        cancelled = None
        if s["cancel_on_end"] and run is not None:
            to_eval.extend(operations.cancel_run(cur, ctx, run["id"], "SCHEDULE_ENDED"))
            cancelled = str(run["id"])
        cur.execute("""UPDATE dq_schedule SET status='ENDED', next_due_at=NULL, coalesced=NULL, revision=revision+1,
                       updated_at=now() WHERE id=%s""", (s["id"],))
        audit(cur, "SCHEDULE_ENDED", "schedule", s["id"], {"ends_at": iso(s["ends_at"]), "cancelled_run": cancelled,
                                                           "in_flight_run": None if run is None else str(run["id"])},
              actor="controller")
        return ["ENDED"]
    if not s["enabled"] or (s["backoff_until"] is not None and now < s["backoff_until"]):
        return []
    if s["kind"] == "CONTINUOUS":
        return _tick_continuous(cur, ctx, s, now)
    return _tick_timed(cur, ctx, s, now, grace)


def _tick_continuous(cur, ctx: Ctx, s: dict, now: _dt.datetime) -> list[str]:
    run = None
    if s["last_run_id"] is not None:
        cur.execute("SELECT * FROM dq_run WHERE id=%s", (s["last_run_id"],))
        run = cur.fetchone()
    if run is not None and run["execution_state"] not in TERMINAL_RUN:
        return []
    due = s["next_due_at"]
    if due is None:
        if run is None:
            due = max(now, s["starts_at"])
        elif run["execution_state"] == "COMPLETED" and run["control_state"] == "ENABLED":
            due = now + _dt.timedelta(seconds=int(s["rest_seconds"]))
        else:
            return []
        cur.execute("UPDATE dq_schedule SET next_due_at=%s, updated_at=now() WHERE id=%s", (due, s["id"]))
        s["next_due_at"] = due
    if due > now:
        return []
    key = f"cont:{s['last_run_id'] or 'start'}:{s['consecutive_errors']}"
    outcome = _fire(cur, ctx, s, key, due, {})
    if outcome == "RUN_CREATED":
        cur.execute("UPDATE dq_schedule SET next_due_at=NULL, updated_at=now() WHERE id=%s", (s["id"],))
    return [outcome]


def _tick_timed(cur, ctx: Ctx, s: dict, now: _dt.datetime, grace: float) -> list[str]:
    due = s["next_due_at"]
    if due is None:
        due = next_due(s, now - _dt.timedelta(seconds=grace))
    dues: list[_dt.datetime] = []
    nxt = due
    while nxt is not None and nxt <= now and len(dues) < MAX_DUE_SCAN:
        dues.append(nxt)
        nxt = next_due(s, nxt)
    run = _in_flight(cur, s)
    coalesced = s["coalesced"]
    results: list[str] = []
    limit = now - _dt.timedelta(seconds=grace)
    fire_at = None
    if dues:
        if s["misfire"] == "LATEST":
            fire_at = dues[-1]
            missed = dues[:-1]
        else:
            missed = [d for d in dues if d < limit]
            on_time = [d for d in dues if d >= limit]
            fire_at = on_time[-1] if on_time else None
            missed += on_time[:-1]
        if missed:
            _record(cur, s, f"misfire:{iso(missed[0])}", missed[0], "MISFIRE_SKIPPED", None,
                    {"count": len(missed), "first": iso(missed[0]), "last": iso(missed[-1]),
                     "policy": s["misfire"]})
            results.append("MISFIRE_SKIPPED")
    if fire_at is not None and run is not None:
        key = iso(fire_at)
        if s["overlap"] == "SKIP":
            _record(cur, s, key, fire_at, "SKIPPED_OVERLAP", None, {"in_flight_run": str(run["id"])})
            results.append("SKIPPED_OVERLAP")
        else:
            c = dict(coalesced or {"due_at": key, "sources": []})
            c["sources"] = (c["sources"] + [key])[-100:]
            c["count"] = int(c.get("count", 0)) + 1
            _record(cur, s, key, fire_at, "COALESCED", None, {"in_flight_run": str(run["id"]),
                                                              "pending_due_at": c["due_at"]})
            coalesced = c
            cur.execute("UPDATE dq_schedule SET coalesced=%s WHERE id=%s", (jb(c), s["id"]))
            results.append("COALESCED")
    elif run is None and (fire_at is not None or coalesced):
        detail = {"coalesced_from": coalesced["sources"], "coalesced_count": coalesced["count"]} if coalesced else {}
        if fire_at is not None:
            key, at = iso(fire_at), fire_at
        else:
            key, at = f"coalesced:{coalesced['due_at']}", parse_ts(coalesced["due_at"])
        results.append(_fire(cur, ctx, s, key, at, detail))
        cur.execute("UPDATE dq_schedule SET coalesced=NULL WHERE id=%s", (s["id"],))
    if nxt != s["next_due_at"]:
        cur.execute("UPDATE dq_schedule SET next_due_at=%s, updated_at=now() WHERE id=%s", (nxt, s["id"]))
    return results


def _record(cur, s: dict, key: str, due: _dt.datetime, outcome: str, run_id, detail: dict):
    fid = new_id()
    cur.execute("""INSERT INTO dq_schedule_fire (id, schedule_id, revision, fire_key, due_at, outcome, run_id,
                   selector_digest, detail) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (schedule_id, revision, fire_key) DO NOTHING RETURNING id""",
                (fid, s["id"], s["revision"], key, due, outcome, run_id,
                 planning.selector_digest(s["check_selector"]), jb(detail)))
    row = cur.fetchone()
    return None if row is None else row["id"]


def _fire(cur, ctx: Ctx, s: dict, key: str, due: _dt.datetime, detail: dict) -> str:
    """Insert the fire and (in a savepoint) its run. An error becomes an ERROR fire and a bounded backoff."""
    fid = _record(cur, s, key, due, "RUN_CREATED", None, detail)
    if fid is None:
        return "DUPLICATE"
    err = None
    run = None
    try:
        with cur.connection.transaction():
            cur.execute("SELECT * FROM dq_plan WHERE id=%s", (s["plan_id"],))
            plan = cur.fetchone()
            if plan["status"] != "ACTIVE":
                raise ApiError(409, "PLAN_ARCHIVED", "plan is archived")
            m = planning.plan_manifest(cur, plan)
            assets = planning.select_assets(cur, plan["asset_selector"])
            run = planning.build_run(cur, ctx, purpose="COMPARE", manifest_id=m["id"], assets=assets,
                                     selector=s["check_selector"], plan_id=plan["id"], schedule_id=s["id"],
                                     fire_id=fid, request_key=f"schedule:{s['id']}:{s['revision']}:{key}",
                                     reason=f"schedule {s['name']}")
    except ApiError as e:
        err = {"code": e.code, "message": e.message}
    except Exception as e:  # noqa: BLE001 - a broken fire must not stop the other schedules
        log.exception("schedule %s fire failed", s["id"])
        err = {"code": "INTERNAL", "message": f"{type(e).__name__}: {e}"[:500]}
    if err is not None:
        n = int(s["consecutive_errors"]) + 1
        back = min(float(ctx.cfg["execution"]["retry_backoff_seconds"]) * 2 ** (n - 1), MAX_BACKOFF_SECONDS)
        until = ctx.now() + _dt.timedelta(seconds=back)
        cur.execute("UPDATE dq_schedule_fire SET outcome='ERROR', detail = detail || %s WHERE id=%s",
                    (jb({"error": err}), fid))
        cur.execute("""UPDATE dq_schedule SET consecutive_errors=%s, backoff_until=%s,
                       next_due_at = CASE WHEN kind='CONTINUOUS' THEN %s ELSE next_due_at END, updated_at=now()
                       WHERE id=%s""", (n, until, until, s["id"]))
        audit(cur, "SCHEDULE_FIRE_ERROR", "schedule", s["id"], {"fire_key": key, "error": err,
                                                                "consecutive_errors": n, "backoff_until": iso(until)},
              actor="controller")
        return "ERROR"
    cur.execute("UPDATE dq_schedule_fire SET run_id=%s WHERE id=%s", (run["id"], fid))
    cur.execute("""UPDATE dq_schedule SET last_run_id=%s, consecutive_errors=0, backoff_until=NULL, updated_at=now()
                   WHERE id=%s""", (run["id"], s["id"]))
    s["last_run_id"] = run["id"]
    return "RUN_CREATED"
