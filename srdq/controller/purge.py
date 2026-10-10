"""Daily expiry of raw values (doc 03 §14.3): material files and metric group rows; conclusions stay."""
from __future__ import annotations

import shutil
import uuid

from . import window
from .common import Ctx, audit, close_incident, incident, iso, lock_control

BATCH = 500


def maybe_purge(ctx: Ctx, *, force: bool = False) -> dict | None:
    cfg = ctx.cfg
    today = window.local_date(cfg, ctx.now())
    ctl = ctx.db.one("SELECT window_state, last_purge_date FROM dq_control_state WHERE id=1")
    if ctl is None:
        return None
    if not force:
        if ctl["last_purge_date"] == today:
            return None
        if cfg["retention"]["purge_outside_window_only"] and ctl["window_state"] != "CLOSED":
            return None
    out = {"artifacts_purged": 0, "artifacts_deferred": 0, "metric_results_purged": 0, "metric_rows_deleted": 0}
    while True:
        n, d = _artifact_batch(ctx)
        out["artifacts_purged"] += n
        out["artifacts_deferred"] = max(out["artifacts_deferred"], d)
        if n == 0 or n + d < BATCH:
            break
    while True:
        n, rows = _metric_batch(ctx)
        out["metric_results_purged"] += n
        out["metric_rows_deleted"] += rows
        if n < BATCH:
            break
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("""SELECT count(*) AS n FROM dq_artifact WHERE purge_state='PURGE_DEFERRED'""")
        deferred = cur.fetchone()["n"]
        if deferred == 0:
            close_incident(cur, "PURGE_DEFERRED", "material")
        cur.execute("UPDATE dq_control_state SET last_purge_date=%s, updated_at=now() WHERE id=1", (today,))
    out["deferred_total"] = deferred
    return out


_BLOCKING = """EXISTS (SELECT 1 FROM dq_side_result sr JOIN dq_observation o ON o.id = sr.observation_id
                       WHERE sr.id = {sr} AND o.status = 'OPEN')"""


def _material_file(ctx: Ctx, rel_path: str):
    """Material lives at material_dir/<side_result uuid>; anything else is refused."""
    try:
        name = str(uuid.UUID(rel_path))
    except ValueError:
        return None
    return ctx.material_dir / name


def _artifact_batch(ctx: Ctx) -> tuple[int, int]:
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute(f"""SELECT a.id, a.rel_path, a.side_result_id, a.material_scope, a.accepted_at, a.purge_state,
                               {_BLOCKING.format(sr='a.side_result_id')} AS blocked
                        FROM dq_artifact a
                        WHERE a.material_scope <> 'REPORT' AND a.purge_state IN ('ACTIVE','PURGE_DEFERRED')
                          AND a.purge_after <= now()
                        ORDER BY a.purge_after, a.id LIMIT %s FOR UPDATE OF a SKIP LOCKED""", (BATCH,))
        rows = cur.fetchall()
        purge = [r for r in rows if not r["blocked"]]
        defer = [r for r in rows if r["blocked"] and r["purge_state"] == "ACTIVE"]
        still = [r for r in rows if r["blocked"]]
        for r in purge:
            p = _material_file(ctx, r["rel_path"])
            if p is not None and p.exists() and not p.is_symlink():
                shutil.rmtree(p) if p.is_dir() else p.unlink()
        if purge:
            ids = [str(r["id"]) for r in purge]
            cur.execute("""UPDATE dq_artifact SET purge_state='PURGED', purged_at=now()
                           WHERE id = ANY(%s::uuid[])""", (ids,))
            cur.execute("""UPDATE dq_side_result SET raw_purged_at=now() WHERE raw_purged_at IS NULL
                           AND id = ANY(%s::uuid[])""", ([str(r["side_result_id"]) for r in purge
                                                         if r["side_result_id"] is not None],))
            scopes: dict[str, int] = {}
            for r in purge:
                scopes[r["material_scope"]] = scopes.get(r["material_scope"], 0) + 1
            stamps = [r["accepted_at"] for r in purge if r["accepted_at"]]
            audit(cur, "RAW_VALUES_PURGED", "artifact", None,
                  {"category": "MATERIAL", "count": len(purge), "by_scope": scopes,
                   "accepted_from": iso(min(stamps)) if stamps else None,
                   "accepted_to": iso(max(stamps)) if stamps else None}, actor="controller")
        if defer:
            cur.execute("UPDATE dq_artifact SET purge_state='PURGE_DEFERRED' WHERE id = ANY(%s::uuid[])",
                        ([str(r["id"]) for r in defer],))
        if still:
            incident(cur, "PURGE_DEFERRED", "material",
                     f"{len(still)} expired material files are still referenced by unfinished observations; "
                     "deletion is deferred", detail={"count": len(still)})
        return len(purge), len(still)


def _metric_batch(ctx: Ctx) -> tuple[int, int]:
    days = int(ctx.cfg["retention"]["raw_value_days"])
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute(f"""SELECT sr.id, sr.accepted_at FROM dq_side_result sr
                        WHERE sr.check_kind='METRIC' AND sr.validity <> 'STAGED'
                          AND (sr.raw_purged_at IS NULL
                               OR EXISTS (SELECT 1 FROM dq_metric_group g WHERE g.side_result_id = sr.id))
                          AND COALESCE(sr.accepted_at, sr.created_at) <= now() - make_interval(days => %s)
                          AND NOT {_BLOCKING.format(sr='sr.id')}
                        ORDER BY sr.accepted_at, sr.id LIMIT %s FOR UPDATE OF sr SKIP LOCKED""", (days, BATCH))
        rows = cur.fetchall()
        if not rows:
            return 0, 0
        ids = [str(r["id"]) for r in rows]
        cur.execute("DELETE FROM dq_metric_value WHERE side_result_id = ANY(%s::uuid[])", (ids,))
        nv = cur.rowcount
        cur.execute("DELETE FROM dq_metric_group WHERE side_result_id = ANY(%s::uuid[])", (ids,))
        ng = cur.rowcount
        cur.execute("UPDATE dq_side_result SET raw_purged_at=COALESCE(raw_purged_at, now()) WHERE id = ANY(%s::uuid[])",
                    (ids,))
        stamps = [r["accepted_at"] for r in rows if r["accepted_at"]]
        audit(cur, "RAW_VALUES_PURGED", "side_result", None,
              {"category": "METRIC_GROUPS", "count": len(rows), "groups": ng, "values": nv,
               "accepted_from": iso(min(stamps)) if stamps else None,
               "accepted_to": iso(max(stamps)) if stamps else None}, actor="controller")
        return len(rows), ng + nv
