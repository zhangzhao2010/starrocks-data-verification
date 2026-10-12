"""Per-cluster and per-kind load monitoring without automatic concurrency changes."""
from __future__ import annotations

import statistics

from ..db.db import jb
from .common import Ctx, audit, close_incident, incident, iso, lock_control, pg_now

CHECK_KINDS = ("COUNT", "METRIC", "SAMPLE", "DEEP")


def _signal_triggers(cfg, signals: dict | None) -> list[str]:
    sig = cfg["load_guard"]["sr_signals"]
    if not sig["enabled"] or not isinstance(signals, dict):
        return []
    out = []
    for key, threshold in (("running_queries", sig["running_queries_threshold"]),
                           ("pending_queries", sig["pending_queries_threshold"])):
        value = signals.get(key)
        if threshold is not None and isinstance(value, (int, float)) and not isinstance(value, bool) \
                and value > threshold:
            out.append(f"{key}={value}>{threshold}")
    return out


def evaluate(ctx: Ctx) -> dict:
    cfg = ctx.cfg
    guard = cfg["load_guard"]
    out: dict = {}
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        now = pg_now(cur)
        states = dict(ctl["cluster_state"] or {})
        changed = False
        for cluster_id in cfg.cluster_ids:
            state = dict(states.get(cluster_id) or {})
            state.setdefault("hard_stop", False)
            signals = _signal_triggers(cfg, state.get("signals"))
            if signals:
                incident(cur, "SR_SIGNAL", cluster_id,
                         f"SR load signals on {cluster_id}: {', '.join(signals)}", cluster_id=cluster_id,
                         detail={"triggers": signals})
            else:
                close_incident(cur, "SR_SIGNAL", cluster_id)
            kinds: dict = {}
            for kind in CHECK_KINDS:
                cur.execute("""SELECT a.status, a.error_class, a.duration_ms, w.asset_id
                               FROM dq_attempt a JOIN dq_work_item w ON w.id=a.work_item_id
                               WHERE a.cluster_id=%s AND w.check_kind=%s AND a.finished_at IS NOT NULL
                                 AND a.status IN ('SUCCEEDED','FAILED')
                               ORDER BY a.finished_at DESC LIMIT %s""",
                            (cluster_id, kind, int(guard["window_queries"])))
                recent = cur.fetchall()
                timeout_rate = (sum(1 for row in recent if row["error_class"] == "TIMEOUT") / len(recent)
                                if recent else 0.0)
                durations = [int(row["duration_ms"]) for row in recent if row["duration_ms"] is not None]
                triggers = []
                if len(recent) >= 5 and timeout_rate > float(guard["timeout_rate_threshold"]):
                    triggers.append(f"timeout_rate={timeout_rate:.2f}")
                slowdown = _slowdown(cur, cluster_id, kind, recent)
                if slowdown is not None and slowdown > float(guard["slowdown_factor_threshold"]):
                    triggers.append(f"slowdown={slowdown:.1f}x")
                key = f"{cluster_id}:{kind}"
                if triggers:
                    incident(cur, "LOAD_DEGRADE", key,
                             f"load warning for {cluster_id}/{kind}: {', '.join(triggers)}",
                             cluster_id=cluster_id, detail={"check_kind": kind, "triggers": triggers})
                else:
                    close_incident(cur, "LOAD_DEGRADE", key)
                kinds[kind] = {
                    "limit": cfg.concurrency(cluster_id, kind),
                    "samples": len(recent),
                    "timeout_rate": timeout_rate,
                    "median_ms": statistics.median(durations) if durations else None,
                    "triggers": triggers,
                }
            state["observed_at"] = iso(now)
            state["triggers"] = signals
            state["kinds"] = kinds
            if states.get(cluster_id) != state:
                states[cluster_id] = state
                changed = True
            out[cluster_id] = {"hard_stop": state["hard_stop"], "signals": signals, "kinds": kinds}
        if changed:
            cur.execute("UPDATE dq_control_state SET cluster_state=%s WHERE id=1", (jb(states),))
    return out


def _slowdown(cur, cluster_id: str, kind: str, recent: list[dict]) -> float | None:
    ratios = []
    medians: dict = {}
    for row in recent:
        if row["duration_ms"] is None or row["error_class"] is not None:
            continue
        asset_id = row["asset_id"]
        if asset_id not in medians:
            cur.execute("""SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a.duration_ms) AS m, count(*) AS n
                           FROM dq_attempt a JOIN dq_work_item w ON w.id=a.work_item_id
                           WHERE a.cluster_id=%s AND w.asset_id=%s AND w.check_kind=%s AND a.status='SUCCEEDED'
                             AND a.finished_at > now() - interval '30 days'""",
                        (cluster_id, asset_id, kind))
            history = cur.fetchone()
            medians[asset_id] = float(history["m"]) if history["n"] >= 5 and history["m"] else None
        baseline = medians[asset_id]
        if baseline:
            ratios.append(row["duration_ms"] / baseline)
    return statistics.median(ratios) if len(ratios) >= 3 else None


def resume_cluster(cur, cluster_id: str, reason: str | None) -> dict:
    ctl = lock_control(cur)
    states = dict(ctl["cluster_state"] or {})
    state = dict(states.get(cluster_id) or {})
    state.update(hard_stop=False, hard_stop_cleared_at=iso(pg_now(cur)))
    states[cluster_id] = state
    cur.execute("UPDATE dq_control_state SET cluster_state=%s, revision=revision+1 WHERE id=1", (jb(states),))
    close_incident(cur, "HARD_STOP", cluster_id)
    incident(cur, "CLUSTER_RESUMED", cluster_id, f"{cluster_id} resumed by operator",
             cluster_id=cluster_id, severity="INFO")
    close_incident(cur, "CLUSTER_RESUMED", cluster_id)
    audit(cur, "CLUSTER_RESUMED", "cluster", cluster_id, {}, reason)
    return state
