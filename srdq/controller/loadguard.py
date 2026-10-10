"""Adaptive per-cluster concurrency and hard stop (doc 03 load guard)."""
from __future__ import annotations

import statistics

from ..db.db import jb
from .common import SIDES, Ctx, audit, close_incident, incident, iso, lock_control, pg_now, parse_ts


def _side_state(ctl: dict, side: str, cfg) -> dict:
    st = dict((ctl["cluster_state"] or {}).get(side) or {})
    st.setdefault("concurrency", int(cfg["execution"]["concurrency_per_cluster"]))
    st.setdefault("hard_stop", False)
    return st


def _signal_triggers(cfg, signals: dict | None) -> list[str]:
    sig = cfg["load_guard"]["sr_signals"]
    if not sig["enabled"] or not isinstance(signals, dict):
        return []
    out = []
    for key, th in (("running_queries", sig["running_queries_threshold"]),
                    ("pending_queries", sig["pending_queries_threshold"])):
        v = signals.get(key)
        if th is not None and isinstance(v, (int, float)) and not isinstance(v, bool) and v > th:
            out.append(f"{key}={v}>{th}")
    return out


def evaluate(ctx: Ctx) -> dict:
    cfg = ctx.cfg
    lg = cfg["load_guard"]
    maximum = int(cfg["execution"]["concurrency_per_cluster"])
    minimum = max(1, min(int(lg["min_concurrency"]), maximum))
    out = {}
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        now = pg_now(cur)
        cs = dict(ctl["cluster_state"] or {})
        changed = False
        for side in SIDES:
            st = _side_state(ctl, side, cfg)
            since = st.get("last_change_at") or st.get("hard_stop_cleared_at")
            cur.execute("""SELECT a.status, a.error_class, a.duration_ms, w.asset_id, w.check_kind
                           FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                           WHERE a.side=%s AND a.finished_at IS NOT NULL AND w.kind='CHECK'
                             AND a.status IN ('SUCCEEDED','FAILED')
                             AND (%s::timestamptz IS NULL OR a.finished_at > %s::timestamptz)
                           ORDER BY a.finished_at DESC LIMIT %s""",
                        (side, since, since, int(lg["window_queries"])))
            recent = cur.fetchall()
            triggers = []
            k = int(lg["hard_stop_consecutive_timeouts"])
            hs_since = st.get("hard_stop_cleared_at")
            cur.execute("""SELECT a.error_class FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                           WHERE a.side=%s AND w.kind='CHECK' AND a.status IN ('SUCCEEDED','FAILED')
                             AND (%s::timestamptz IS NULL OR a.finished_at > %s::timestamptz)
                           ORDER BY a.finished_at DESC LIMIT %s""", (side, hs_since, hs_since, k))
            last_k = [r["error_class"] for r in cur.fetchall()]
            if k > 0 and len(last_k) == k and all(x == "TIMEOUT" for x in last_k) and not st["hard_stop"]:
                st["hard_stop"] = True
                st["hard_stop_at"] = iso(now)
                incident(cur, "HARD_STOP", side, f"{k} consecutive timeouts on {side}; claims stopped until "
                         "an operator resumes the cluster", side=side, severity="CRITICAL")
                audit(cur, "CLUSTER_HARD_STOP", "cluster", side, {"consecutive_timeouts": k}, actor="controller")
                changed = True
            if lg["adaptive_concurrency"] and not st["hard_stop"]:
                if len(recent) >= 5:
                    rate = sum(1 for r in recent if r["error_class"] == "TIMEOUT") / len(recent)
                    if rate > float(lg["timeout_rate_threshold"]):
                        triggers.append(f"timeout_rate={rate:.2f}")
                    ratio = _slowdown(cur, side, recent)
                    if ratio is not None and ratio > float(lg["slowdown_factor_threshold"]):
                        triggers.append(f"slowdown={ratio:.1f}x")
                sig = _signal_triggers(cfg, st.get("signals"))
                if sig:
                    incident(cur, "SR_SIGNAL", side, f"SR load signals on {side}: {', '.join(sig)}", side=side)
                else:
                    close_incident(cur, "SR_SIGNAL", side)
                triggers += sig
                last = parse_ts(st["last_change_at"]) if st.get("last_change_at") else None
                if triggers and st["concurrency"] > minimum:
                    new = max(minimum, st["concurrency"] // 2)
                    incident(cur, "LOAD_DEGRADE", side, f"{side} concurrency {st['concurrency']} -> {new}: "
                             f"{', '.join(triggers)}", side=side, detail={"triggers": triggers})
                    audit(cur, "LOAD_DEGRADE", "cluster", side, {"from": st["concurrency"], "to": new,
                                                                 "triggers": triggers}, actor="controller")
                    st["concurrency"], st["last_change_at"] = new, iso(now)
                    changed = True
                elif not triggers and st["concurrency"] < maximum and \
                        (last is None or (now - last).total_seconds() >= float(lg["recover_after_seconds"])):
                    new = st["concurrency"] + 1
                    incident(cur, "LOAD_RECOVER", side, f"{side} concurrency {st['concurrency']} -> {new}",
                             side=side, severity="INFO")
                    close_incident(cur, "LOAD_RECOVER", side)
                    audit(cur, "LOAD_RECOVER", "cluster", side, {"from": st["concurrency"], "to": new},
                          actor="controller")
                    st["concurrency"], st["last_change_at"] = new, iso(now)
                    if new >= maximum:
                        close_incident(cur, "LOAD_DEGRADE", side)
                    changed = True
            if st["concurrency"] > maximum:
                st["concurrency"] = maximum
                changed = True
            st["triggers"] = triggers
            if cs.get(side) != st:
                changed = True
            cs[side] = st
            out[side] = {"concurrency": st["concurrency"], "hard_stop": st["hard_stop"], "triggers": triggers}
        if changed:
            cur.execute("UPDATE dq_control_state SET cluster_state=%s WHERE id=1", (jb(cs),))
    return out


def _slowdown(cur, side: str, recent: list[dict]) -> float | None:
    """Median of recent successes against each (asset, kind)'s 30-day median."""
    ratios = []
    seen: dict = {}
    for r in recent:
        if r["duration_ms"] is None or r["error_class"] is not None:
            continue
        key = (r["asset_id"], r["check_kind"])
        if key not in seen:
            cur.execute("""SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a.duration_ms) AS m, count(*) AS n
                           FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                           WHERE a.side=%s AND w.asset_id=%s AND w.check_kind=%s AND a.status='SUCCEEDED'
                             AND a.finished_at > now() - interval '30 days'""", (side, *key))
            h = cur.fetchone()
            seen[key] = float(h["m"]) if h["n"] >= 5 and h["m"] else None
        base = seen[key]
        if base:
            ratios.append(r["duration_ms"] / base)
    return statistics.median(ratios) if len(ratios) >= 3 else None


def resume_cluster(cur, side: str, reason: str | None) -> dict:
    """Clear a hard stop (operator action). Concurrency restarts at the minimum."""
    ctl = lock_control(cur)
    cs = dict(ctl["cluster_state"] or {})
    st = dict(cs.get(side) or {})
    now = pg_now(cur)
    st.update(hard_stop=False, hard_stop_cleared_at=iso(now), last_change_at=iso(now))
    st["concurrency"] = max(1, int(st.get("concurrency") or 1))
    cs[side] = st
    cur.execute("UPDATE dq_control_state SET cluster_state=%s, revision=revision+1 WHERE id=1", (jb(cs),))
    close_incident(cur, "HARD_STOP", side)
    incident(cur, "CLUSTER_RESUMED", side, f"{side} resumed by operator", side=side, severity="INFO")
    close_incident(cur, "CLUSTER_RESUMED", side)
    audit(cur, "CLUSTER_RESUMED", "cluster", side, {"concurrency": st["concurrency"]}, reason)
    return st
