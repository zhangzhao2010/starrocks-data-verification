"""Scope discovery: METADATA work per asset/side, draft manifests from both inventories, confirm/reject."""
from __future__ import annotations

from typing import Any

from ..common import dsl
from ..db.db import jb, new_id
from .common import ApiError, Ctx, as_uuid, audit, digest_json, lock_control
from .importer import common_columns

ORDERABLE = ("INTEGER", "DECIMAL", "DATE", "DATETIME", "STRING")
LISTABLE = ORDERABLE + ("BOOLEAN",)
TERMINAL_RUN = ("COMPLETED", "FAILED", "CANCELLED")


# ---- discovery ---------------------------------------------------------------------

def discover(ctx: Ctx, reason: str | None = None) -> dict:
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("SELECT id FROM dq_run WHERE purpose='METADATA_PREVIEW' AND execution_state <> ALL(%s) "
                    "FOR UPDATE", (list(TERMINAL_RUN),))
        busy = cur.fetchone()
        if busy:
            raise ApiError(409, "DISCOVERY_RUNNING", "a discovery run is already in progress",
                           {"run_id": str(busy["id"])})
        cur.execute("SELECT * FROM dq_asset WHERE status='ACTIVE' ORDER BY layer, source_db, source_table")
        assets = cur.fetchall()
        if not assets:
            raise ApiError(422, "NO_ASSETS", "import an asset list first")
        rid = new_id()
        cur.execute("""INSERT INTO dq_run (id, purpose, config_snapshot, execution_state, reason)
                       VALUES (%s, 'METADATA_PREVIEW', %s, 'READY', %s)""", (rid, jb(dict(ctx.cfg)), reason))
        for a in assets:
            cur.execute("SELECT source_cluster_id, target_cluster_id FROM dq_cluster_pair WHERE id=%s AND enabled",
                        (a["pair_id"],))
            pair = cur.fetchone()
            if pair is None:
                raise ApiError(422, "CLUSTER_PAIR_DISABLED", f"cluster pair {a['pair_id']} is unavailable")
            endpoints = ((pair["source_cluster_id"], a["source_db"], a["source_table"]),
                         (pair["target_cluster_id"], a["target_db"], a["target_table"]))
            for cluster_id, db, tbl in endpoints:
                cur.execute("""INSERT INTO dq_work_item (id, run_id, asset_id, cluster_id, kind, scan_unit_id, spec)
                               VALUES (%s,%s,%s,%s,'METADATA',%s,%s)""",
                            (new_id(), rid, a["id"], cluster_id, new_id(),
                             jb({"kind": "METADATA", "cluster_id": cluster_id,
                                 "table": {"db": db, "table": tbl}})))
        audit(cur, "DISCOVERY_STARTED", "run", rid, {"assets": len(assets)}, reason)
    return {"run_id": str(rid), "assets": len(assets), "work_items": len(assets) * 2}


# ---- scope computation (pure) ----------------------------------------------------------

def _entry(kind: str, pred: list, ps: str, pt: str, reason: str, *, status: str = "INCLUDED",
           status_reason: str | None = None, physical: dict | None = None, label: str | None = None) -> dict:
    return {"scope_kind": kind, "predicate": pred, "scope_key": dsl.scope_key(pred),
            "label": label or dsl.scope_label(pred), "presence_source": ps, "presence_target": pt,
            "expected_reason": reason, "status": status, "status_reason": status_reason,
            "physical": physical or {}}


def _norm(lt: str, v: Any) -> str:
    if lt == "STRING":
        return str(v)
    if lt == "DECIMAL":
        return format(dsl.sort_key(lt, v).normalize(), "f")
    return str(dsl.typed_value(lt, v))


def _bound(lt: str, v: Any, lower: bool) -> str | None:
    """Normalize a partition bound; SR min/max sentinels become unbounded."""
    if v is None:
        return None
    s = str(v).strip()
    if s.upper() in ("MAXVALUE", "MINVALUE", ""):
        return None
    try:
        return _norm(lt, s)
    except (dsl.DslError, ValueError, ArithmeticError):
        if lower and (s.startswith("0000-") or s.startswith("-")):
            return None
        raise


def _range_entries(col: str, lt: str, src: dict, tgt: dict) -> list[dict] | None:
    key = lambda v: dsl.sort_key(lt, v)  # noqa: E731
    sides = []
    try:
        for part in (src, tgt):
            sides.append([(_bound(lt, p.get("lower"), True), _bound(lt, p.get("upper"), False), p.get("name"))
                          for p in part.get("partitions") or []])
    except (dsl.DslError, ValueError, ArithmeticError):
        return None
    if not sides[0] and not sides[1]:
        return None
    points: dict = {}
    for parts in sides:
        for lo, hi, _n in parts:
            for v in (lo, hi):
                if v is not None:
                    points.setdefault(key(v), v)
    order = sorted(points)
    pts = [points[k] for k in order]
    intervals = [(None, pts[0] if pts else None)] + list(zip(pts, pts[1:])) + [(pts[-1] if pts else None, None)]
    # Interval i spans (pts[i-1], pts[i]); a partition [lo, hi) covers intervals idx(lo)+1 .. idx(hi).
    idx = {k: i for i, k in enumerate(order)}
    last = len(intervals) - 1
    covers: list[list[list]] = [[[] for _ in intervals] for _ in sides]
    for si, parts in enumerate(sides):
        for lo, hi, n in parts:
            start = 0 if lo is None else idx[key(lo)] + 1
            end = last if hi is None else idx[key(hi)]
            for i in range(start, end + 1):
                covers[si][i].append(n)
    out = []
    for i, (a, b) in enumerate(intervals):
        cover = [covers[0][i], covers[1][i]]
        pred = [dsl.scope_range(col, lt, a, b)]
        phys = {"source": cover[0], "target": cover[1]}
        ps = "PRESENT" if cover[0] else "ABSENT_CONFIRMED"
        pt = "PRESENT" if cover[1] else "ABSENT_CONFIRMED"
        if cover[0] and cover[1]:
            out.append(_entry("RANGE", pred, ps, pt, "IN_SCOPE", physical=phys))
        elif cover[0]:
            out.append(_entry("RANGE", pred, ps, pt, "SOURCE_ONLY", physical=phys))
        elif cover[1]:
            out.append(_entry("RANGE", pred, ps, pt, "TARGET_ONLY", physical=phys))
        else:
            out.append(_entry("OUTSIDE", pred, ps, pt, "OUTSIDE", status="EXCLUDED",
                              status_reason="NOT_COVERED_BY_ANY_PARTITION", physical=phys))
    return out


def _list_entries(col: str, lt: str, src: dict, tgt: dict) -> list[dict] | None:
    sides = []
    try:
        for part in (src, tgt):
            sides.append({p.get("name"): frozenset(_norm(lt, v) for v in p.get("values") or [])
                          for p in part.get("partitions") or []})
    except (dsl.DslError, ValueError, ArithmeticError):
        return None
    for parts in sides:
        seen: set = set()
        for vals in parts.values():
            if vals & seen:
                return None
            seen |= vals
    out = []
    if sorted(map(sorted, sides[0].values())) == sorted(map(sorted, sides[1].values())):
        tgt_by_vals = {v: n for n, v in sides[1].items()}
        for name, vals in sorted(sides[0].items(), key=lambda kv: sorted(kv[1])):
            if not vals:
                continue
            out.append(_entry("LIST", [dsl.scope_list(col, lt, list(vals))], "PRESENT", "PRESENT", "IN_SCOPE",
                              physical={"source": [name], "target": [tgt_by_vals[vals]]}))
        return out
    where = [{v: n for n, vals in parts.items() for v in vals} for parts in sides]
    allv = set(where[0]) | set(where[1])
    for v in sorted(allv, key=(lambda x: dsl.sort_key(lt, x)) if lt != "BOOLEAN" else str):
        s, t = v in where[0], v in where[1]
        phys = {"source": [where[0][v]] if s else [], "target": [where[1][v]] if t else []}
        reason = "IN_SCOPE" if s and t else "SOURCE_ONLY" if s else "TARGET_ONLY"
        out.append(_entry("LIST", [dsl.scope_list(col, lt, [v])], "PRESENT" if s else "ABSENT_CONFIRMED",
                          "PRESENT" if t else "ABSENT_CONFIRMED", reason, physical=phys))
    return out


def compute_scopes(src: dict | None, tgt: dict | None) -> tuple[list[dict], bool]:
    """Scope entries for one asset from both sides' TableMeta JSON. Returns (entries, inventory_known)."""
    src = src or {"status": "UNKNOWN"}
    tgt = tgt or {"status": "UNKNOWN"}
    s, t = src.get("status", "UNKNOWN"), tgt.get("status", "UNKNOWN")
    all_pred = [dsl.scope_all()]
    if s in ("UNKNOWN", "QUERY_FAILED") or t in ("UNKNOWN", "QUERY_FAILED"):
        return [_entry("INVENTORY_BLOCKER", all_pred, s, t, "UNKNOWN", label="INVENTORY_BLOCKER (metadata unknown)")], \
            False
    if s == "ACCESS_DENIED" or t == "ACCESS_DENIED":
        return [_entry("ALL", all_pred, s, t, "ACCESS_DENIED")], False
    if s != "PRESENT" or t != "PRESENT":
        return [_entry("ALL", all_pred, s, t, "TABLE_ABSENT")], True
    cols, _mism = common_columns({"source": src, "target": tgt})
    ps, pt = src.get("partition") or {}, tgt.get("partition") or {}
    kinds = (ps.get("kind"), pt.get("kind"))
    pcols = (ps.get("columns") or [], pt.get("columns") or [])
    entries = None
    col = pcols[0][0] if len(pcols[0]) == 1 else None
    if kinds[0] == kinds[1] and kinds[0] in ("RANGE", "LIST") and pcols[0] == pcols[1] and col in cols:
        lt = cols[col]["type"]
        if kinds[0] == "RANGE" and lt in ORDERABLE:
            entries = _range_entries(col, lt, ps, pt)
        elif kinds[0] == "LIST" and lt in LISTABLE:
            entries = _list_entries(col, lt, ps, pt)
        if entries is not None and cols[col]["nullable"]:
            entries.append(_entry("NULL_SCOPE", [dsl.scope_null(col)], "PRESENT", "PRESENT", "IN_SCOPE"))
    if entries is None:
        entries = [_entry("ALL", all_pred, "PRESENT", "PRESENT", "IN_SCOPE")]
    return entries, True


# ---- draft manifest ---------------------------------------------------------------------

def _carry(base_entries: list[dict], current: list[dict], blocked: bool, s: str, t: str) -> list[dict]:
    cur_keys = {e["scope_key"] for e in current}
    included = [b for b in base_entries if b["status"] == "INCLUDED"]
    if blocked and included:
        return [_entry(b["scope_kind"] if b["scope_kind"] != "INVENTORY_BLOCKER" else "INVENTORY_BLOCKER",
                       b["predicate"], s, t, "UNKNOWN", label=b["label"],
                       status_reason="CARRIED_FROM_BASE_METADATA_UNKNOWN", physical=b["physical"])
                for b in included]
    out = list(current)
    for b in included:
        if b["scope_key"] in cur_keys or b["expected_reason"] == "DISAPPEARED" \
                or b["scope_kind"] == "INVENTORY_BLOCKER":
            continue
        out.append(_entry(b["scope_kind"], b["predicate"], "ABSENT_CONFIRMED", "ABSENT_CONFIRMED", "DISAPPEARED",
                          label=b["label"], status_reason="NOT_FOUND_IN_CURRENT_METADATA", physical=b["physical"]))
    return out


def build_draft(cur, ctx: Ctx, run_id) -> dict:
    """Build a DRAFT manifest from a finished discovery run (caller holds the run row lock)."""
    cur.execute("""SELECT w.asset_id, w.cluster_id, w.status, w.result,
                          p.source_cluster_id, p.target_cluster_id
                   FROM dq_work_item w JOIN dq_asset a ON a.id=w.asset_id
                   JOIN dq_cluster_pair p ON p.id=a.pair_id
                   WHERE w.run_id=%s AND w.kind='METADATA'""", (run_id,))
    metas: dict[str, dict] = {}
    for w in cur.fetchall():
        res = w["result"] or {}
        meta = res.get("meta") or {"status": "UNKNOWN", "message": (res.get("failure") or {}).get("message")}
        role = "source" if w["cluster_id"] == w["source_cluster_id"] else "target"
        metas.setdefault(str(w["asset_id"]), {})[role] = meta
    cur.execute("SELECT id FROM dq_manifest WHERE status='CONFIRMED'")
    base = cur.fetchone()
    base_entries: dict[str, list[dict]] = {}
    if base:
        cur.execute("SELECT * FROM dq_scope_entry WHERE manifest_id=%s", (base["id"],))
        for e in cur.fetchall():
            base_entries.setdefault(str(e["asset_id"]), []).append(e)
    complete = True
    all_entries: dict[str, list[dict]] = {}
    for aid, m in sorted(metas.items()):
        entries, known = compute_scopes(m.get("source"), m.get("target"))
        complete = complete and known
        s = (m.get("source") or {}).get("status", "UNKNOWN")
        t = (m.get("target") or {}).get("status", "UNKNOWN")
        blocked = entries[0]["scope_kind"] == "INVENTORY_BLOCKER"
        entries = _carry(base_entries.get(aid, []), entries, blocked, s, t)
        base_keys = {b["scope_key"] for b in base_entries.get(aid, [])}
        for e in entries:
            e["is_new"] = bool(base) and e["scope_key"] not in base_keys
        all_entries[aid] = entries
    inventory = {"assets": metas}
    cur.execute("SELECT COALESCE(max(version), 0) + 1 AS v FROM dq_manifest")
    version = cur.fetchone()["v"]
    summary: dict[str, Any] = {"assets": len(metas), "entries": 0, "by_reason": {}, "new": 0, "excluded": 0,
                               "incomplete_assets": []}
    for aid, entries in all_entries.items():
        for e in entries:
            summary["entries"] += 1
            summary["by_reason"][e["expected_reason"]] = summary["by_reason"].get(e["expected_reason"], 0) + 1
            summary["new"] += int(e["is_new"])
            summary["excluded"] += int(e["status"] == "EXCLUDED")
            if e["expected_reason"] in ("UNKNOWN", "ACCESS_DENIED") and aid not in summary["incomplete_assets"]:
                summary["incomplete_assets"].append(aid)
    mid = new_id()
    cur.execute("""INSERT INTO dq_manifest (id, version, inventory_complete, inventory, digest, discovery_run_id,
                   based_on_id, summary) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (mid, version, complete, jb(inventory), digest_json({"inventory": inventory, "entries": all_entries}),
                 run_id, base["id"] if base else None, jb(summary)))
    for aid, entries in all_entries.items():
        cur.executemany("""INSERT INTO dq_scope_entry (id, manifest_id, asset_id, scope_kind, scope_key,
                           scope_key_digest, label, predicate, presence_source, presence_target, expected_reason,
                           status, status_reason, is_new, physical) VALUES
                           (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        [(new_id(), mid, aid, e["scope_kind"], e["scope_key"], dsl.scope_digest(e["predicate"]),
                          e["label"][:500], jb(e["predicate"]), e["presence_source"], e["presence_target"],
                          e["expected_reason"], e["status"], e["status_reason"], e["is_new"], jb(e["physical"]))
                         for e in entries])
    audit(cur, "MANIFEST_DRAFTED", "manifest", mid, {"version": version, "summary": summary})
    out = {"id": str(mid), "version": version, "inventory_complete": complete, "summary": summary,
           "auto_confirmed": False}
    if _auto_confirmable(ctx, base, base_entries, all_entries, complete):
        _confirm(cur, mid, "AUTO_ADD_COMPATIBLE")
        out["auto_confirmed"] = True
    return out


def _sig(e: dict) -> tuple:
    return (e["expected_reason"], e["status"], e["presence_source"], e["presence_target"])


def _auto_confirmable(ctx: Ctx, base, base_entries, all_entries, complete: bool) -> bool:
    if ctx.cfg["scope"]["new_scope_policy"] != "AUTO_ADD_COMPATIBLE" or not complete or base is None:
        return False
    if set(base_entries) - set(all_entries):
        return False
    for aid, entries in all_entries.items():
        old = {b["scope_key"]: b for b in base_entries.get(aid, [])}
        if not old:
            return False
        for e in entries:
            b = old.pop(e["scope_key"], None)
            if b is None:
                if e["expected_reason"] != "IN_SCOPE" or e["status"] != "INCLUDED":
                    return False
            elif _sig(b) != _sig(e):
                return False
        if any(b["status"] == "INCLUDED" for b in old.values()):
            return False
    return True


# ---- confirm / reject ----------------------------------------------------------------------

def _confirm(cur, mid, reason: str | None) -> None:
    cur.execute("UPDATE dq_manifest SET status='SUPERSEDED' WHERE status='CONFIRMED' RETURNING id")
    old = [str(r["id"]) for r in cur.fetchall()]
    cur.execute("UPDATE dq_manifest SET status='CONFIRMED', confirmed_at=now(), reason=%s WHERE id=%s",
                (reason, mid))
    audit(cur, "MANIFEST_CONFIRMED", "manifest", mid, {"superseded": old}, reason)


def confirm_manifest(ctx: Ctx, manifest_id, reason: str | None = None) -> dict:
    mid = as_uuid(manifest_id, "manifest_id")
    with ctx.db.tx() as cur:
        cur.execute("SELECT * FROM dq_manifest WHERE id=%s FOR UPDATE", (mid,))
        m = cur.fetchone()
        if m is None:
            raise ApiError(404, "NOT_FOUND", "manifest not found")
        if m["status"] != "DRAFT":
            raise ApiError(409, "MANIFEST_NOT_DRAFT", f"manifest is {m['status']}")
        cur.execute("SELECT version FROM dq_manifest WHERE status='CONFIRMED'")
        cv = cur.fetchone()
        if cv and cv["version"] > m["version"]:
            raise ApiError(409, "MANIFEST_STALE", "a newer manifest is already confirmed")
        _confirm(cur, mid, reason)
    return {"id": str(mid), "status": "CONFIRMED"}


def reject_manifest(ctx: Ctx, manifest_id, reason: str | None = None) -> dict:
    mid = as_uuid(manifest_id, "manifest_id")
    with ctx.db.tx() as cur:
        cur.execute("UPDATE dq_manifest SET status='REJECTED', reason=%s WHERE id=%s AND status='DRAFT' RETURNING id",
                    (reason, mid))
        if cur.fetchone() is None:
            raise ApiError(409, "MANIFEST_NOT_DRAFT", "manifest is not a DRAFT")
        audit(cur, "MANIFEST_REJECTED", "manifest", mid, {}, reason)
    return {"id": str(mid), "status": "REJECTED"}


def confirmed_manifest(cur) -> dict | None:
    cur.execute("SELECT * FROM dq_manifest WHERE status='CONFIRMED'")
    return cur.fetchone()
