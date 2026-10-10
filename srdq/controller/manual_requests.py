"""Manual re-checks and DEEP diagnosis (doc 03 §7.9): preview, then confirm into a MANUAL run (LIVE_REREAD only)."""
from __future__ import annotations

from typing import Any

from ..common import dsl
from ..db.db import jb, new_id
from . import importer, planning
from .common import ApiError, Ctx, as_uuid, audit, digest_json, hexd, iso, lock_control
from .manifest import confirmed_manifest

KINDS = ("COUNT", "METRIC", "SAMPLE", "DEEP")
DEEP_EVIDENCE = ("CONTENT_HASH", "EXACT")
DEFAULT_BUCKETS = 16
DEFAULT_MAX_ROWS = 10_000_000
MIN_SHARD_RESULT_BYTES = 1 << 20
LIVE_REREAD_NOTE = ("LIVE_REREAD: both sides are read again now; the result describes the new read, not the time "
                    "of the original comparison")


def _int(v: Any, name: str, lo: int, hi: int) -> int:
    try:
        n = int(str(v))
    except (TypeError, ValueError) as e:
        raise ApiError(422, "INVALID_FIELD", f"{name} must be an integer") from e
    if not lo <= n <= hi:
        raise ApiError(422, "INVALID_FIELD", f"{name} must be in [{lo}, {hi}]")
    return n


def _normalize(body: dict) -> dict:
    if not isinstance(body, dict):
        raise ApiError(422, "INVALID_BODY", "request body must be an object")
    key = body.get("request_key")
    if not isinstance(key, str) or not 1 <= len(key) <= 200:
        raise ApiError(422, "REQUEST_KEY_REQUIRED", "request_key (1..200 chars) is required")
    mode = body.get("source_mode", "LIVE_REREAD")
    if mode != "LIVE_REREAD":
        raise ApiError(422, "SOURCE_MODE_UNSUPPORTED", "only source_mode LIVE_REREAD is supported")
    kind = body.get("check_kind")
    if kind is not None and kind not in KINDS:
        raise ApiError(422, "INVALID_CHECK_KIND", f"check_kind must be one of {KINDS}")
    sk = body.get("scope_keys")
    if sk is not None and (not isinstance(sk, list) or not sk or not all(isinstance(x, str) for x in sk)):
        raise ApiError(422, "INVALID_SCOPE", "scope_keys must be a non-empty list of strings")
    allowed = {"request_key", "source_mode", "check_kind", "requested_evidence", "asset_id", "origin_comparison_id",
               "scope_keys", "rule_key", "deep", "detail_approval_ref", "transfer_class"}
    extra = sorted(set(body) - allowed)
    if extra:
        raise ApiError(422, "UNKNOWN_FIELD", f"unknown fields: {extra}")
    out = {k: body.get(k) for k in sorted(allowed) if body.get(k) is not None}
    out["source_mode"] = "LIVE_REREAD"
    return out


def _deep_definition(cfg, d: dict, inv_entry: dict, evidence: str, n_scopes: int = 1) -> tuple[dict, list[str]]:
    dg = cfg["diagnosis"]
    cols, mism = importer.common_columns(inv_entry)
    want = d.get("columns") or "*"
    notes: list[str] = []
    if want == "*":
        names = list(cols)
        if mism:
            notes.append(f"skipped columns: {', '.join(sorted(mism))}")
    else:
        if not isinstance(want, list) or not all(isinstance(x, str) for x in want) or len(set(want)) != len(want):
            raise ApiError(422, "INVALID_COLUMNS", "deep.columns must be '*' or a list of distinct names")
        for n in want:
            if n in mism:
                raise ApiError(422, "COLUMN_UNSUPPORTED", f"column {n}: {mism[n]}")
            if n not in cols:
                raise ApiError(422, "COLUMN_NOT_FOUND", f"column {n} not found on both sides")
        names = list(want)
    if not names:
        raise ApiError(422, "NO_COLUMNS", "no comparable column")
    key = None
    metas = [inv_entry.get("source") or {}, inv_entry.get("target") or {}]
    kc = metas[0].get("key_columns")
    if all(m.get("model") in ("PRI_KEYS", "UNQ_KEYS") and m.get("key_columns") == kc for m in metas) \
            and isinstance(kc, list) and len(kc) == 1 and kc[0] in cols \
            and cols[kc[0]]["type"] in ("INTEGER", "STRING"):
        key = kc[0]
        if key not in names:
            names.insert(0, key)
    else:
        notes.append("no single-column unique key on both sides: rows are compared by row hash (keyless)")
    mode = d.get("mode", "HASH_BUCKET")
    if mode not in ("HASH_BUCKET", "KEY_RANGE"):
        raise ApiError(422, "INVALID_MODE", "deep.mode must be HASH_BUCKET or KEY_RANGE")
    leaf_cap = int(dg["max_leaf_count"])
    exact = [n for n in names if not dsl.is_float(cols[n]["type"])]
    tolerant = [n for n in names if dsl.is_float(cols[n]["type"])]
    if key is not None:
        exact.remove(key)
        exact.insert(0, key)
    tol = None
    if tolerant:
        if key is None:
            raise ApiError(422, "KEYLESS_FLOAT", f"float columns {tolerant} need a unique key; exclude them")
        t = d.get("float_tolerance") or {}
        if t.get("rtol") is None or t.get("atol") is None:
            raise ApiError(422, "FLOAT_TOLERANCE_REQUIRED", f"float columns {tolerant} need deep.float_tolerance")
        try:
            tol = {"rtol": importer._tol(str(t["rtol"]), "rtol"), "atol": importer._tol(str(t["atol"]), "atol")}
        except ValueError as e:
            raise ApiError(422, "INVALID_TOLERANCE", str(e)) from e
    max_read = int(dg["max_read_bytes_per_scope"])
    defn: dict = {
        "mode": mode, "evidence": evidence,
        "projection": {"key_columns": [key] if key else [], "exact_columns": exact, "tolerant_columns": tolerant},
        "key_type": cols[key]["type"] if key else None, "float_tolerance": tol,
        "max_rows_per_side": _int(d.get("max_rows_per_side", DEFAULT_MAX_ROWS), "deep.max_rows_per_side", 1,
                                  10 ** 12),
        "max_read_bytes_per_scope": str(_int(d.get("max_read_bytes", max_read), "deep.max_read_bytes", 1, max_read)),
        "max_key_bytes": int(cfg["checks"]["sample"]["max_key_bytes"]),
    }
    if mode == "HASH_BUCKET":
        defn["buckets"] = _int(d.get("buckets", DEFAULT_BUCKETS), "deep.buckets", 1, leaf_cap)
    else:
        if key is None or defn["key_type"] != "INTEGER":
            raise ApiError(422, "KEY_RANGE_NEEDS_INTEGER_KEY", "KEY_RANGE needs a unique integer key; use HASH_BUCKET")
        rs = d.get("ranges")
        if not isinstance(rs, list) or not 1 <= len(rs) <= leaf_cap:
            raise ApiError(422, "INVALID_RANGES", f"deep.ranges must list 1..{leaf_cap} [lower, upper] pairs")
        ranges = []
        prev_hi: int | None = None
        for i, r in enumerate(rs):
            if not isinstance(r, list) or len(r) != 2:
                raise ApiError(422, "INVALID_RANGES", "each range is [lower, upper]; null means unbounded")
            lo = None if r[0] is None else _int(r[0], "range lower", -2 ** 127, 2 ** 127)
            hi = None if r[1] is None else _int(r[1], "range upper", -2 ** 127, 2 ** 127)
            if lo is not None and hi is not None and lo >= hi:
                raise ApiError(422, "INVALID_RANGES", "range lower must be below upper")
            if i > 0 and (lo is None or prev_hi is None or lo < prev_hi):
                raise ApiError(422, "INVALID_RANGES", "ranges must be ascending and must not overlap")
            prev_hi = hi
            ranges.append([None if lo is None else str(lo), None if hi is None else str(hi)])
        defn["ranges"] = ranges
    shards = len(defn["ranges"]) if mode == "KEY_RANGE" else defn["buckets"]
    # every shard reserves its cap against the scope read budget and the run transfer budget at once
    per_shard = min(int(defn["max_read_bytes_per_scope"]) // shards,
                    int(dg["max_transfer_bytes_per_run"]) // (shards * max(1, n_scopes)))
    defn["max_result_bytes_per_side"] = str(max(MIN_SHARD_RESULT_BYTES, per_shard))
    return defn, notes


def _build(cur, ctx: Ctx, req: dict) -> dict:
    """Validate a normalized request against current state and return the preview."""
    origin = None
    if req.get("origin_comparison_id"):
        cur.execute("SELECT * FROM dq_comparison WHERE id=%s", (as_uuid(req["origin_comparison_id"],
                                                                         "origin_comparison_id"),))
        origin = cur.fetchone()
        if origin is None:
            raise ApiError(404, "NOT_FOUND", "origin comparison not found")
    aid = req.get("asset_id") or (str(origin["asset_id"]) if origin else None)
    if aid is None:
        raise ApiError(422, "ASSET_REQUIRED", "asset_id or origin_comparison_id is required")
    aid = as_uuid(aid, "asset_id")
    if origin is not None and origin["asset_id"] != aid:
        raise ApiError(422, "ORIGIN_MISMATCH", "origin comparison belongs to another asset")
    cur.execute("SELECT * FROM dq_asset WHERE id=%s", (aid,))
    asset = cur.fetchone()
    if asset is None or asset["status"] != "ACTIVE":
        raise ApiError(404, "NOT_FOUND", "asset not found or not active")
    kind = req.get("check_kind") or (origin["check_kind"] if origin else None)
    if kind is None:
        raise ApiError(422, "CHECK_KIND_REQUIRED", "check_kind is required without an origin comparison")
    manifest = confirmed_manifest(cur)
    if manifest is None:
        raise ApiError(422, "NO_CONFIRMED_MANIFEST", "confirm a scope manifest first")
    cur.execute("SELECT scope_key, label, status FROM dq_scope_entry WHERE manifest_id=%s AND asset_id=%s "
                "ORDER BY scope_key", (manifest["id"], aid))
    entries = {e["scope_key"]: e for e in cur.fetchall()}
    keys = req.get("scope_keys") or ([origin["scope_key"]] if origin else sorted(entries))
    if not entries:
        raise ApiError(422, "SCOPE_NOT_FOUND", "the asset has no scope in the confirmed manifest")
    for k in keys:
        if k not in entries:
            raise ApiError(422, "SCOPE_NOT_FOUND", f"scope {k} is not in the confirmed manifest")
    out: dict = {"asset_id": str(aid), "source_table": f"{asset['source_db']}.{asset['source_table']}",
                 "target_table": f"{asset['target_db']}.{asset['target_table']}", "check_kind": kind,
                 "source_mode": "LIVE_REREAD", "manifest_id": str(manifest["id"]),
                 "manifest_version": manifest["version"],
                 "scopes": [{"scope_key": k, "label": entries[k]["label"], "status": entries[k]["status"]}
                            for k in keys],
                 "origin_comparison_id": None if origin is None else str(origin["id"]),
                 "notes": [LIVE_REREAD_NOTE]}
    if kind == "DEEP":
        ev = req.get("requested_evidence", "CONTENT_HASH")
        if ev not in DEEP_EVIDENCE:
            raise ApiError(422, "INVALID_EVIDENCE", f"DEEP evidence must be one of {DEEP_EVIDENCE}")
        inv_entry = (manifest["inventory"].get("assets") or {}).get(str(aid))
        if inv_entry is None:
            raise ApiError(422, "NO_METADATA", "the confirmed manifest has no column metadata for this asset")
        defn, notes = _deep_definition(ctx.cfg, req.get("deep") or {}, inv_entry, ev, len(keys))
        shards = len(defn["ranges"]) if defn["mode"] == "KEY_RANGE" else defn["buckets"]
        out.update(requested_evidence=ev, transfer_class="DETAIL_APPROVED", rule_definition=defn,
                   shards_per_scope=shards, leaf_count=shards * len(keys),
                   limits={"max_rows_per_side": defn["max_rows_per_side"],
                           "max_read_bytes_per_scope": defn["max_read_bytes_per_scope"],
                           "max_result_bytes_per_shard": defn["max_result_bytes_per_side"],
                           "max_transfer_bytes_per_run": str(ctx.cfg["diagnosis"]["max_transfer_bytes_per_run"])},
                   detail_approval_ref=req.get("detail_approval_ref"))
        if shards * len(keys) > int(ctx.cfg["diagnosis"]["max_leaf_count"]):
            raise ApiError(422, "TOO_MANY_LEAVES", "scopes × shards exceeds diagnosis.max_leaf_count")
        out["notes"] += notes
        return out
    ev = req.get("requested_evidence", kind)
    if ev != kind:
        raise ApiError(422, "INVALID_EVIDENCE", f"{kind} evidence is {kind}")
    rk = req.get("rule_key") or (origin["rule_key"] if origin else None)
    cur.execute("""SELECT * FROM dq_check_rule WHERE asset_id=%s AND check_kind=%s AND status='APPROVED'
                   ORDER BY rule_key""", (aid, kind))
    rules = [r for r in cur.fetchall() if rk is None or r["rule_key"] == rk]
    if len(rules) != 1:
        raise ApiError(422, "RULE_NOT_FOUND" if not rules else "RULE_AMBIGUOUS",
                       f"need exactly one approved {kind} rule" + ("" if rk else "; give rule_key"))
    r = rules[0]
    out.update(requested_evidence=ev, transfer_class=r["transfer_class"], rule_id=str(r["id"]),
               rule_key=r["rule_key"], rule_revision=r["revision"])
    return out


def _out(row: dict) -> dict:
    return {"id": str(row["id"]), "request_key": row["request_key"], "status": row["status"],
            "check_kind": row["check_kind"], "requested_evidence": row["requested_evidence"],
            "source_mode": row["source_mode"], "asset_id": str(row["asset_id"]),
            "origin_comparison_id": None if row["origin_comparison_id"] is None else str(row["origin_comparison_id"]),
            "request": row["request"], "preview": row["preview"],
            "rule_id": None if row["rule_id"] is None else str(row["rule_id"]),
            "detail_approval_ref": row["detail_approval_ref"],
            "run_id": None if row["run_id"] is None else str(row["run_id"]), "reason": row["reason"],
            "created_at": iso(row["created_at"]), "confirmed_at": iso(row["confirmed_at"]),
            "preview_digest": hexd(digest_json(row["preview"]))}


def preview(ctx: Ctx, body: dict, reason: str | None) -> dict:
    if not reason or not reason.strip():
        raise ApiError(422, "REASON_REQUIRED", "a reason is required")
    req = _normalize(body)
    rdig = digest_json(req)
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("SELECT * FROM dq_manual_request WHERE request_key=%s", (req["request_key"],))
        ex = cur.fetchone()
        if ex is not None:
            if bytes(ex["request_digest"]) != rdig:
                raise ApiError(409, "REQUEST_KEY_CONFLICT", "request_key was used for a different request")
            return _out(ex) | {"idempotent_replay": True}
        pv = _build(cur, ctx, req)
        mid = new_id()
        cur.execute("""INSERT INTO dq_manual_request (id, request_key, check_kind, requested_evidence, asset_id,
                       origin_comparison_id, request, request_digest, rule_id, detail_approval_ref, preview, reason)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *""",
                    (mid, req["request_key"], pv["check_kind"], pv["requested_evidence"], pv["asset_id"],
                     pv["origin_comparison_id"], jb(req), rdig, pv.get("rule_id"), req.get("detail_approval_ref"),
                     jb(pv), reason.strip()))
        row = cur.fetchone()
        audit(cur, "MANUAL_REQUEST_PREVIEWED", "manual_request", mid,
              {"check_kind": pv["check_kind"], "asset_id": pv["asset_id"],
               "scopes": [s["scope_key"] for s in pv["scopes"]]}, reason.strip())
    return _out(row)


def confirm(ctx: Ctx, request_id, *, reason: str | None, if_match: str | None = None,
            detail_approval_ref: str | None = None) -> dict:
    if not reason or not reason.strip():
        raise ApiError(422, "REASON_REQUIRED", "a reason is required")
    mid = as_uuid(request_id, "manual_request_id")
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("SELECT * FROM dq_manual_request WHERE id=%s FOR UPDATE", (mid,))
        row = cur.fetchone()
        if row is None:
            raise ApiError(404, "NOT_FOUND", "manual request not found")
        if row["status"] == "CONFIRMED":
            return _out(row) | {"idempotent_replay": True}
        if row["status"] != "PREVIEWED":
            raise ApiError(409, "REQUEST_NOT_PREVIEWED", f"request is {row['status']}")
        if if_match is not None and str(if_match).strip().strip('"') != digest_json(row["preview"]).hex():
            raise ApiError(409, "PRECONDITION_FAILED", "If-Match does not carry the current preview digest")
        req = row["request"]
        pv = _build(cur, ctx, req)
        if digest_json(pv) != digest_json(row["preview"]):
            cur.execute("UPDATE dq_manual_request SET preview=%s WHERE id=%s", (jb(pv), mid))
            audit(cur, "MANUAL_REQUEST_STALE", "manual_request", mid, {}, reason.strip())
            stale = pv
        else:
            stale = None
        if stale is None:
            out = _confirm(cur, ctx, row, pv, reason.strip(), detail_approval_ref)
    if stale is not None:
        raise ApiError(409, "PREVIEW_STALE", "the target changed since the preview; review the new preview",
                       {"preview": stale})
    return out


def _confirm(cur, ctx: Ctx, row: dict, pv: dict, reason: str, detail_ref: str | None) -> dict:
    kind = pv["check_kind"]
    dref = (detail_ref or row["detail_approval_ref"] or "").strip() or None
    cur.execute("SELECT * FROM dq_asset WHERE id=%s", (pv["asset_id"],))
    asset = cur.fetchone()
    if kind == "DEEP":
        if not dref:
            raise ApiError(422, "DETAIL_APPROVAL_REQUIRED", "DEEP needs a DETAIL_APPROVED reference")
        rule_key = f"deep_{row['id'].hex[:16]}"
        rid, _created = importer.register_rule(cur, asset["id"], rule_key, "DEEP", pv["rule_definition"])
        cur.execute("""UPDATE dq_check_rule SET status='APPROVED', approval_ref=%s, approved_at=now(), updated_at=now()
                       WHERE id=%s AND status='DRAFT'""", (dref, rid))
        audit(cur, "RULE_APPROVED", "check_rule", rid, {"approval_ref": dref, "manual_request_id": str(row["id"]),
                                                        "kind": "DEEP"}, reason)
    else:
        rid = as_uuid(pv["rule_id"])
    run = planning.build_run(cur, ctx, purpose="MANUAL", manifest_id=as_uuid(pv["manifest_id"]),
                             assets=[asset], selector=None, request_key=f"manual:{row['request_key']}",
                             reason=reason, manual_request_id=row["id"], rule_ids=[rid],
                             scope_keys={s["scope_key"] for s in pv["scopes"]}, detail_approval_ref=dref)
    cur.execute("SELECT * FROM dq_check_rule WHERE id=%s", (rid,))
    snap = planning.rule_snapshot_of(cur.fetchone())
    cur.execute("""UPDATE dq_manual_request SET status='CONFIRMED', rule_id=%s, detail_approval_ref=%s,
                   rule_snapshot=%s, run_id=%s, confirmed_at=now() WHERE id=%s RETURNING *""",
                (rid, dref, jb(snap), run["id"], row["id"]))
    done = cur.fetchone()
    audit(cur, "MANUAL_REQUEST_CONFIRMED", "manual_request", row["id"],
          {"run_id": run["id"], "rule_id": str(rid), "check_kind": kind, "detail_approval_ref": dref}, reason)
    return _out(done) | {"run": run}


def reject(ctx: Ctx, request_id, reason: str | None) -> dict:
    if not reason or not reason.strip():
        raise ApiError(422, "REASON_REQUIRED", "a reason is required")
    mid = as_uuid(request_id, "manual_request_id")
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("UPDATE dq_manual_request SET status='REJECTED' WHERE id=%s AND status='PREVIEWED' RETURNING *",
                    (mid,))
        row = cur.fetchone()
        if row is None:
            raise ApiError(409, "REQUEST_NOT_PREVIEWED", "request not found or not in PREVIEWED")
        audit(cur, "MANUAL_REQUEST_REJECTED", "manual_request", mid, {}, reason.strip())
    return _out(row)


def get_request(ctx: Ctx, request_id) -> dict:
    row = ctx.db.one("SELECT * FROM dq_manual_request WHERE id=%s", (as_uuid(request_id, "manual_request_id"),))
    if row is None:
        raise ApiError(404, "NOT_FOUND", "manual request not found")
    return _out(row)


def list_requests(ctx: Ctx, limit: int = 100) -> list[dict]:
    return [_out(r) for r in ctx.db.query("SELECT * FROM dq_manual_request ORDER BY created_at DESC LIMIT %s",
                                          (int(limit),))]
