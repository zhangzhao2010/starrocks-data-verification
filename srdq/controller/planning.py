"""Plans, run creation (frozen denominator), and lazy observation materialization."""
from __future__ import annotations

from typing import Any

from ..common import canonical as c
from ..common import dsl
from ..db.db import jb, new_id
from .common import (KIND_RANK, LAYER_RANK, SIDES, TRANSFER_CLASS, ApiError, Ctx, as_uuid, audit, digest_json,
                     lock_control, transfer_allowed)
from .importer import common_columns, spec_columns
from .manifest import confirmed_manifest

PLAN_KINDS = ("COUNT", "METRIC", "SAMPLE")
TERMINAL_RUN = ("COMPLETED", "FAILED", "CANCELLED")
COUNT_RESULT_CAP = 4096


# ---- selectors ------------------------------------------------------------------------

def validate_check_selector(sel: Any, *, allow_deep: bool = False) -> dict:
    if not isinstance(sel, dict) or not sel:
        raise ApiError(422, "BAD_SELECTOR", "check_selector must be an object")
    if sel.get("all") is True and set(sel) == {"all"}:
        return {"all": True}
    if set(sel) - {"kinds", "rule_keys"} or "kinds" not in sel:
        raise ApiError(422, "BAD_SELECTOR", 'check_selector is {"all": true} or {"kinds": [...], "rule_keys": [...]}')
    allowed = PLAN_KINDS + (("DEEP",) if allow_deep else ())
    kinds = sel["kinds"]
    if not isinstance(kinds, list) or not kinds or not set(kinds) <= set(allowed):
        raise ApiError(422, "BAD_SELECTOR", f"kinds must be a non-empty subset of {allowed}")
    out: dict = {"kinds": sorted(set(kinds), key=KIND_RANK.get)}
    if sel.get("rule_keys") is not None:
        rk = sel["rule_keys"]
        if not isinstance(rk, list) or not all(isinstance(x, str) and x for x in rk):
            raise ApiError(422, "BAD_SELECTOR", "rule_keys must be a list of strings")
        out["rule_keys"] = sorted(set(rk))
    return out


def selector_matches(sel: dict, kind: str, rule_key: str) -> bool:
    if sel.get("all"):
        return kind != "DEEP"
    if kind not in sel.get("kinds", []):
        return False
    return sel.get("rule_keys") is None or rule_key in sel["rule_keys"]


def validate_asset_selector(sel: Any) -> dict:
    if sel is None:
        return {"all": True}
    if not isinstance(sel, dict) or len(sel) != 1:
        raise ApiError(422, "BAD_ASSET_SELECTOR", 'asset_selector is {"all": true}, {"asset_ids": [...]} or '
                                                  '{"layers": [...]}')
    if sel.get("all") is True:
        return {"all": True}
    if "asset_ids" in sel:
        ids = sel["asset_ids"]
        if not isinstance(ids, list) or not ids:
            raise ApiError(422, "BAD_ASSET_SELECTOR", "asset_ids must be a non-empty list")
        return {"asset_ids": sorted({str(as_uuid(x, "asset_id")) for x in ids})}
    if "layers" in sel:
        ls = sel["layers"]
        if not isinstance(ls, list) or not ls or not set(ls) <= set(LAYER_RANK):
            raise ApiError(422, "BAD_ASSET_SELECTOR", f"layers must be a subset of {list(LAYER_RANK)}")
        return {"layers": sorted(set(ls), key=LAYER_RANK.get)}
    raise ApiError(422, "BAD_ASSET_SELECTOR", "unknown asset_selector")


def select_assets(cur, sel: dict) -> list[dict]:
    if sel.get("asset_ids"):
        cur.execute("SELECT * FROM dq_asset WHERE status='ACTIVE' AND id = ANY(%s::uuid[]) ORDER BY id",
                    (sel["asset_ids"],))
    elif sel.get("layers"):
        cur.execute("SELECT * FROM dq_asset WHERE status='ACTIVE' AND layer = ANY(%s) ORDER BY id", (sel["layers"],))
    else:
        cur.execute("SELECT * FROM dq_asset WHERE status='ACTIVE' ORDER BY id")
    return cur.fetchall()


# ---- plans ----------------------------------------------------------------------------

def _plan_out(p: dict) -> dict:
    return {"id": str(p["id"]), "name": p["name"], "manifest_mode": p["manifest_mode"],
            "manifest_id": None if p["manifest_id"] is None else str(p["manifest_id"]),
            "check_selector": p["check_selector"], "asset_selector": p["asset_selector"], "status": p["status"],
            "revision": p["revision"]}


def _plan_fields(cur, body: dict, base: dict | None) -> dict:
    base = base or {}
    name = body.get("name", base.get("name"))
    if not isinstance(name, str) or not 1 <= len(name.strip()) <= 128:
        raise ApiError(422, "BAD_NAME", "name must be 1..128 characters")
    mode = body.get("manifest_mode", base.get("manifest_mode", "LATEST_CONFIRMED"))
    if mode not in ("LATEST_CONFIRMED", "FIXED"):
        raise ApiError(422, "BAD_MANIFEST_MODE", "manifest_mode must be LATEST_CONFIRMED or FIXED")
    mid = body.get("manifest_id", base.get("manifest_id"))
    if mode == "FIXED":
        if mid is None:
            raise ApiError(422, "MANIFEST_REQUIRED", "FIXED plans need manifest_id")
        mid = as_uuid(mid, "manifest_id")
        cur.execute("SELECT status FROM dq_manifest WHERE id=%s", (mid,))
        m = cur.fetchone()
        if m is None or m["status"] not in ("CONFIRMED", "SUPERSEDED"):
            raise ApiError(422, "MANIFEST_NOT_CONFIRMED", "FIXED plans must point at a confirmed manifest")
    else:
        mid = None
    return {"name": name.strip(), "manifest_mode": mode, "manifest_id": mid,
            "check_selector": validate_check_selector(body.get("check_selector", base.get("check_selector"))),
            "asset_selector": validate_asset_selector(body.get("asset_selector", base.get("asset_selector")))}


def create_plan(ctx: Ctx, body: dict, reason: str | None = None) -> dict:
    with ctx.db.tx() as cur:
        f = _plan_fields(cur, body, None)
        cur.execute("SELECT 1 FROM dq_plan WHERE name=%s", (f["name"],))
        if cur.fetchone():
            raise ApiError(409, "NAME_TAKEN", "a plan with this name exists")
        pid = new_id()
        cur.execute("""INSERT INTO dq_plan (id, name, manifest_mode, manifest_id, check_selector, asset_selector)
                       VALUES (%s,%s,%s,%s,%s,%s) RETURNING *""",
                    (pid, f["name"], f["manifest_mode"], f["manifest_id"], jb(f["check_selector"]),
                     jb(f["asset_selector"])))
        p = cur.fetchone()
        audit(cur, "PLAN_CREATED", "plan", pid, _plan_out(p), reason)
        return _plan_out(p)


def update_plan(ctx: Ctx, plan_id, body: dict, if_match: Any, reason: str | None = None) -> dict:
    pid = as_uuid(plan_id, "plan_id")
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("SELECT * FROM dq_plan WHERE id=%s FOR UPDATE", (pid,))
        p = cur.fetchone()
        if p is None:
            raise ApiError(404, "NOT_FOUND", "plan not found")
        if if_match is None or str(if_match).strip('"') != str(p["revision"]):
            raise ApiError(409, "REVISION_MISMATCH", "If-Match does not match the plan revision",
                           {"revision": p["revision"]})
        if p["status"] != "ACTIVE":
            raise ApiError(409, "PLAN_ARCHIVED", "plan is archived")
        if body.get("status") == "ARCHIVED":
            cur.execute("UPDATE dq_plan SET status='ARCHIVED', revision=revision+1, updated_at=now() WHERE id=%s "
                        "RETURNING *", (pid,))
            out = _plan_out(cur.fetchone())
            audit(cur, "PLAN_ARCHIVED", "plan", pid, out, reason)
            return out
        f = _plan_fields(cur, body, p)
        if f["name"] != p["name"]:
            cur.execute("SELECT 1 FROM dq_plan WHERE name=%s", (f["name"],))
            if cur.fetchone():
                raise ApiError(409, "NAME_TAKEN", "a plan with this name exists")
        cur.execute("""UPDATE dq_plan SET name=%s, manifest_mode=%s, manifest_id=%s, check_selector=%s,
                       asset_selector=%s, revision=revision+1, updated_at=now() WHERE id=%s RETURNING *""",
                    (f["name"], f["manifest_mode"], f["manifest_id"], jb(f["check_selector"]), jb(f["asset_selector"]),
                     pid))
        out = _plan_out(cur.fetchone())
        audit(cur, "PLAN_UPDATED", "plan", pid, out, reason)
        return out


def plan_manifest(cur, plan: dict) -> dict:
    if plan["manifest_mode"] == "FIXED":
        cur.execute("SELECT * FROM dq_manifest WHERE id=%s", (plan["manifest_id"],))
        m = cur.fetchone()
    else:
        m = confirmed_manifest(cur)
    if m is None:
        raise ApiError(422, "NO_CONFIRMED_MANIFEST", "confirm a scope manifest first")
    return m


def start_plan_run(ctx: Ctx, plan_id, *, request_key: str | None = None, reason: str | None = None,
                   check_selector: dict | None = None) -> dict:
    pid = as_uuid(plan_id, "plan_id")
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("SELECT * FROM dq_plan WHERE id=%s FOR UPDATE", (pid,))
        plan = cur.fetchone()
        if plan is None:
            raise ApiError(404, "NOT_FOUND", "plan not found")
        if plan["status"] != "ACTIVE":
            raise ApiError(409, "PLAN_ARCHIVED", "plan is archived")
        m = plan_manifest(cur, plan)
        sel = validate_check_selector(check_selector) if check_selector else plan["check_selector"]
        assets = select_assets(cur, plan["asset_selector"])
        return build_run(cur, ctx, purpose="COMPARE", manifest_id=m["id"], assets=assets, selector=sel,
                         plan_id=pid, request_key=request_key, reason=reason)


# ---- run construction ------------------------------------------------------------------

def _policies(cur, asset_id) -> list[dict]:
    cur.execute("SELECT * FROM dq_scope_policy WHERE asset_id=%s AND status='ACTIVE' ORDER BY created_at",
                (asset_id,))
    return cur.fetchall()


def matching_policy(policies: list[dict], scope_key: str, kind: str, rule_key: str) -> dict:
    """Merge ACTIVE policies; PARTITION policies override TABLE ones, later ones override earlier."""
    out: dict = {"control_state": None, "priority": None, "policy_ids": []}
    for level in ("TABLE", "PARTITION"):
        for p in policies:
            if p["node_kind"] != level or not selector_matches(p["check_selector"], kind, rule_key):
                continue
            if level == "PARTITION" and p["scope_key"] != scope_key:
                continue
            if p["control_state"] is not None:
                out["control_state"] = p["control_state"]
            if p["priority_override"] is not None:
                out["priority"] = p["priority_override"]
            out["policy_ids"].append(str(p["id"]))
    return out


def rule_cap(rule_snapshot: dict) -> int:
    kind = rule_snapshot["check_kind"]
    d = rule_snapshot["definition"]
    if kind == "COUNT":
        return COUNT_RESULT_CAP
    return int(d.get("max_result_bytes_per_side") or 0) or COUNT_RESULT_CAP


def _needed_columns(kind: str, d: dict) -> list[str]:
    if kind == "METRIC":
        names = list(d.get("group_by") or []) + [m["column"] for m in d["metrics"] if m.get("column")]
        return names + sorted(dsl.filter_columns(d.get("filter") or {"op": "TRUE"}))
    if kind in ("SAMPLE", "DEEP"):
        p = d["projection"]
        return list(dict.fromkeys((p.get("key_columns") or []) + p["exact_columns"] + (p.get("tolerant_columns") or [])))
    return []


def column_contract(kind: str, d: dict, inv_entry: dict | None) -> tuple[dict, str | None]:
    """Columns for the spec, or a reason the frozen rule no longer fits the current metadata."""
    if kind == "COUNT":
        return {}, None
    if inv_entry is None:
        return {}, "NO_METADATA"
    cols, mism = common_columns(inv_entry)
    names = _needed_columns(kind, d)
    for n in names:
        if n in mism:
            return {}, f"COLUMN_CONTRACT_CHANGED:{n}:{mism[n]}"
        if n not in cols:
            return {}, f"COLUMN_CONTRACT_CHANGED:{n}:MISSING"
    if kind == "METRIC":
        for g in d.get("group_by") or []:
            if dsl.is_float(cols[g]["type"]):
                return {}, f"COLUMN_CONTRACT_CHANGED:{g}:FLOAT_GROUP"
        for m in d["metrics"]:
            if m["function"] != "SUM":
                continue
            lt = cols[m["column"]]["type"]
            want = {"INTEGER": "INTEGER", "DECIMAL": "DECIMAL", "FLOAT32": "FLOAT64", "FLOAT64": "FLOAT64"}.get(lt)
            if want != m["value_type"] or (lt == "DECIMAL" and cols[m["column"]]["scale"] > int(m["scale"])):
                return {}, f"COLUMN_CONTRACT_CHANGED:{m['column']}:SUM_TYPE"
    if kind in ("SAMPLE", "DEEP"):
        p = d["projection"]
        for n in p["exact_columns"]:
            if dsl.is_float(cols[n]["type"]):
                return {}, f"COLUMN_CONTRACT_CHANGED:{n}:FLOAT_EXACT"
        for n in p.get("tolerant_columns") or []:
            if not dsl.is_float(cols[n]["type"]):
                return {}, f"COLUMN_CONTRACT_CHANGED:{n}:NOT_FLOAT"
        keys = p.get("key_columns") or []
        if keys:
            if cols[keys[0]]["type"] != d.get("key_type"):
                return {}, f"COLUMN_CONTRACT_CHANGED:{keys[0]}:KEY_TYPE"
            if kind == "SAMPLE":
                for side in ("source", "target"):
                    meta = inv_entry.get(side) or {}
                    if meta.get("model") not in ("PRI_KEYS", "UNQ_KEYS") or meta.get("key_columns") != keys:
                        return {}, f"KEY_CONTRACT_LOST:{side}"
    return spec_columns(cols, names), None


def spec_limits(cfg, kind: str, d: dict) -> dict:
    ex = cfg["execution"]
    lim = {"batch_rows": ex["batch_rows"], "batch_bytes": ex["batch_bytes"],
           "query_timeout_seconds": ex["query_timeout_seconds"], "max_value_bytes": ex["max_value_bytes"],
           "max_row_bytes": ex["max_row_bytes"]}
    if kind == "METRIC":
        lim.update(max_groups=int(d["max_groups_per_side"]), max_group_key_bytes=int(d["max_group_key_bytes"]),
                   max_result_bytes=int(d["max_result_bytes_per_side"]))
    elif kind == "SAMPLE":
        lim.update(max_key_bytes=int(d["max_key_bytes"]), max_rows=int(d["max_sample_rows_per_side"]),
                   max_result_bytes=int(d["max_result_bytes_per_side"]))
    elif kind == "DEEP":
        lim.update(max_key_bytes=int(d.get("max_key_bytes", 256)), max_rows=int(d["max_rows_per_side"]),
                   max_result_bytes=int(d["max_result_bytes_per_side"]))
    return lim


def rule_snapshot_of(rule: dict) -> dict:
    return {"rule_id": str(rule["id"]), "asset_id": str(rule["asset_id"]), "rule_key": rule["rule_key"],
            "revision": rule["revision"], "check_kind": rule["check_kind"], "definition": rule["definition"],
            "definition_digest_hex": bytes(rule["definition_digest"]).hex(),
            "execution_policy": rule["execution_policy"], "transfer_class": rule["transfer_class"],
            "approval_ref": rule["approval_ref"]}


def _classify(cfg, *, entry: dict, asset: dict, rule: dict, policy: dict, contract_issue: str | None,
              transfer_ok: bool) -> tuple[str, str, str, str | None]:
    """(control_state, execution_state, coverage_state, coverage_reason) for a new run_check."""
    reason = entry["expected_reason"]
    if entry["status"] == "EXCLUDED":
        return "ENABLED", "COMPLETED", "EXCLUDED", entry.get("status_reason") or reason
    if not asset["enabled"]:
        return "ENABLED", "COMPLETED", "EXCLUDED", "ASSET_DISABLED"
    if not rule["enabled"]:
        return "ENABLED", "COMPLETED", "EXCLUDED", "RULE_DISABLED"
    both_present = entry["presence_source"] == "PRESENT" and entry["presence_target"] == "PRESENT"
    if rule["support_state"] == "UNSUPPORTED":
        return "ENABLED", "COMPLETED", "UNSUPPORTED", (rule["support_reason"] or "UNSUPPORTED")[:500]
    if contract_issue and both_present and reason not in ("TABLE_ABSENT", "ACCESS_DENIED", "UNKNOWN"):
        return "ENABLED", "COMPLETED", "UNSUPPORTED", contract_issue
    if reason in ("TABLE_ABSENT", "ACCESS_DENIED", "UNKNOWN", "NOT_IN_MANIFEST") \
            or entry["scope_kind"] == "INVENTORY_BLOCKER":
        return "ENABLED", "BLOCKED", "BLOCKED", reason
    if contract_issue:
        return "ENABLED", "BLOCKED", "BLOCKED", contract_issue
    if not transfer_ok:
        return "ENABLED", "BLOCKED", "BLOCKED", "TRANSFER_NOT_APPROVED"
    if policy["control_state"] in ("CANCELLED", "ARCHIVED"):
        return policy["control_state"], "CANCELLED", "PENDING", "CANCELLED_BY_POLICY"
    if policy["control_state"] == "PAUSED":
        return "PAUSED", "READY", "PENDING", None
    return "ENABLED", "READY", "PENDING", None


def _insert_node(cur, run_id, asset_id, parent_id, kind: str, scope_key: str, pred: list, label: str,
                 entry_id=None, refinement_path: str | None = None) -> Any:
    nid = new_id()
    cur.execute("""INSERT INTO dq_task_node (id, run_id, asset_id, parent_id, kind, scope_key, scope_key_digest,
                   scope_entry_id, label, predicate, refinement_path) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (nid, run_id, asset_id, parent_id, kind, scope_key, c.sha256(scope_key.encode("utf-8")), entry_id,
                 label[:500], jb(pred), refinement_path))
    return nid


def deep_shards(d: dict, base_pred: list) -> list[tuple[str, list, str, int | None]]:
    """(scope_key, predicate, label, bucket) per shard of a DEEP rule."""
    out = []
    if d["mode"] == "HASH_BUCKET":
        n = int(d["buckets"])
        for b in range(n):
            atom = {"kind": "HASH_BUCKET", "buckets": n, "bucket": b}
            pred = base_pred + [atom]
            out.append((dsl.scope_key(pred), pred, dsl.scope_label(pred), b))
    else:
        key, lt = d["projection"]["key_columns"][0], d["key_type"]
        for lo, hi in d["ranges"]:
            atom = {"kind": "KEY_RANGE", "column": key, "type": lt, "lower": lo, "upper": hi}
            pred = base_pred + [atom]
            out.append((dsl.scope_key(pred), pred, dsl.scope_label(pred), None))
    return out


def _pseudo_entry() -> dict:
    pred = [dsl.scope_all()]
    return {"id": None, "scope_kind": "ALL", "scope_key": dsl.scope_key(pred), "predicate": pred,
            "label": "ALL (not in manifest)", "presence_source": "UNKNOWN", "presence_target": "UNKNOWN",
            "expected_reason": "NOT_IN_MANIFEST", "status": "INCLUDED", "status_reason": None}


def build_run(cur, ctx: Ctx, *, purpose: str, manifest_id, assets: list[dict], selector: dict | None,
              plan_id=None, schedule_id=None, fire_id=None, request_key: str | None = None,
              reason: str | None = None, supersedes=None, manual_request_id=None, rule_ids: list | None = None,
              scope_keys: set[str] | None = None, detail_approval_ref: str | None = None) -> dict:
    """Create a run with its frozen denominator. Caller holds the control row lock."""
    cfg = ctx.cfg
    if request_key:
        cur.execute("SELECT id FROM dq_run WHERE request_key=%s", (request_key,))
        ex = cur.fetchone()
        if ex:
            return run_summary(cur, ex["id"]) | {"idempotent_replay": True}
    cur.execute("SELECT * FROM dq_manifest WHERE id=%s", (manifest_id,))
    manifest = cur.fetchone()
    if manifest is None:
        raise ApiError(422, "NO_CONFIRMED_MANIFEST", "manifest not found")
    if not assets:
        raise ApiError(422, "NO_ASSETS", "no assets selected")
    cur.execute("SELECT transfer_approval_ref FROM dq_control_state WHERE id=1")
    approval_ref = cur.fetchone()["transfer_approval_ref"]
    rid = new_id()
    snapshot = {k: cfg[k] for k in ("execution", "transfer", "comparison", "retention", "execution_window",
                                    "diagnosis")}
    cur.execute("""INSERT INTO dq_run (id, purpose, plan_id, schedule_id, fire_id, manifest_id, manual_request_id,
                   request_key, config_snapshot, execution_state, supersedes_run_id, reason)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'READY',%s,%s)""",
                (rid, purpose, plan_id, schedule_id, fire_id, manifest_id, manual_request_id, request_key,
                 jb({"selector": selector, "config": snapshot, "manifest_version": manifest["version"]}),
                 supersedes, reason))
    inventory = manifest["inventory"].get("assets", {})
    counts: dict[str, int] = {}
    deep_nodes: list = []
    rid_filter = None if rule_ids is None else {str(x) for x in rule_ids}
    for asset in assets:
        aid = asset["id"]
        cur.execute("INSERT INTO dq_asset_slot (asset_id) VALUES (%s) ON CONFLICT DO NOTHING", (aid,))
        cur.execute("SELECT * FROM dq_scope_entry WHERE manifest_id=%s AND asset_id=%s ORDER BY scope_key",
                    (manifest_id, aid))
        entries = cur.fetchall() or [_pseudo_entry()]
        if scope_keys is not None:
            entries = [e for e in entries if e["scope_key"] in scope_keys]
            if not entries:
                raise ApiError(422, "SCOPE_NOT_FOUND", "scope is not in the manifest")
        cur.execute("SELECT * FROM dq_check_rule WHERE asset_id=%s AND status='APPROVED' ORDER BY rule_key",
                    (aid,))
        rules = [r for r in cur.fetchall()
                 if (rid_filter is not None and str(r["id"]) in rid_filter)
                 or (rid_filter is None and selector_matches(selector, r["check_kind"], r["rule_key"]))]
        if not rules:
            continue
        table_pred = [dsl.scope_all()]
        table_node = _insert_node(cur, rid, aid, None, "TABLE", "TABLE", table_pred, "TABLE")
        policies = _policies(cur, aid)
        inv_entry = inventory.get(str(aid))
        tables = {"huawei": {"db": asset["source_db"], "table": asset["source_table"]},
                  "aliyun": {"db": asset["target_db"], "table": asset["target_table"]}}
        for e in entries:
            pred = list(e["predicate"])
            node = _insert_node(cur, rid, aid, table_node, "PARTITION", e["scope_key"], pred, e["label"], e["id"])
            for rule in rules:
                kind = rule["check_kind"]
                d = rule["definition"]
                rs = rule_snapshot_of(rule)
                cols, issue = column_contract(kind, d, inv_entry)
                tc = TRANSFER_CLASS[kind]
                ok = transfer_allowed(cfg, tc, detail_approval_ref) and approval_ref is not None
                policy = matching_policy(policies, e["scope_key"], kind, rule["rule_key"])
                cs, es, cov, cov_reason = _classify(cfg, entry=e, asset=asset, rule=rule, policy=policy,
                                                    contract_issue=issue, transfer_ok=ok)
                prio = policy["priority"] if policy["priority"] is not None else asset["priority"]
                evidence = d.get("evidence", "CONTENT_HASH") if kind == "DEEP" else kind
                xs = {"tables": tables, "columns": cols, "transfer_class": tc, "transfer_approval_ref": approval_ref,
                      "detail_approval_ref": detail_approval_ref, "policy_priority": policy["priority"],
                      "policy_control_state": policy["control_state"], "policy_ids": policy["policy_ids"],
                      "limits": spec_limits(cfg, kind, d), "result_cap": str(rule_cap(rs)),
                      "scope_label": e["label"], "layer": asset["layer"]}
                cur.execute("""INSERT INTO dq_run_check (id, run_id, asset_id, node_id, rule_id, rule_key, rule_revision,
                               check_kind, scope_key, scope_key_digest, expected_evidence, expected_reason,
                               rule_snapshot, execution_snapshot, rule_digest, frozen_at, control_state,
                               execution_state, effective_priority, kind_rank, layer_rank, ready_seq, coverage_state,
                               coverage_reason, coverage_updated_at)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s,%s,%s,%s,%s,
                               nextval('srdq.ready_seq'),%s,%s,
                               CASE WHEN %s <> 'PENDING' THEN now() END)""",
                            (new_id(), rid, aid, node, rule["id"], rule["rule_key"], rule["revision"], kind,
                             e["scope_key"], c.sha256(e["scope_key"].encode("utf-8")), evidence,
                             e["expected_reason"], jb(rs), jb(xs), c.json_digest(rs), cs, es, prio,
                             KIND_RANK[kind], LAYER_RANK[asset["layer"]], cov, cov_reason, cov))
                counts[cov] = counts.get(cov, 0) + 1
                if kind == "DEEP":
                    for sk, spred, label, _b in deep_shards(d, pred):
                        _insert_node(cur, rid, aid, node, "SHARD", sk, spred, label,
                                     refinement_path=f"{d['mode']}")
                    deep_nodes.append((node, int(d.get("max_read_bytes_per_scope")
                                                 or cfg["diagnosis"]["max_read_bytes_per_scope"])))
    if not counts:
        raise ApiError(422, "NOTHING_TO_CHECK", "no approved rule matches the selector")
    _budgets(cur, cfg, rid, purpose, deep_nodes)
    cur.execute("SELECT count(*) AS n FROM dq_run_check WHERE run_id=%s AND execution_state IN ('READY','PLANNED')",
                (rid,))
    if cur.fetchone()["n"] == 0:
        cur.execute("UPDATE dq_run SET execution_state='COMPLETED', completed_at=now() WHERE id=%s", (rid,))
    cur.execute("UPDATE dq_control_state SET queue_revision=queue_revision+1 WHERE id=1")
    audit(cur, "RUN_CREATED", "run", rid, {"purpose": purpose, "plan_id": None if plan_id is None else str(plan_id),
                                            "manifest_id": str(manifest_id), "coverage": counts,
                                            "selector": selector}, reason)
    return run_summary(cur, rid)


def _budgets(cur, cfg, run_id, purpose: str, deep_nodes: list) -> None:
    tr = cfg["transfer"]
    transfer_limit = int(tr["max_cross_cloud_bytes_per_run"])
    if deep_nodes:
        transfer_limit = min(transfer_limit, int(cfg["diagnosis"]["max_transfer_bytes_per_run"]))
    for res, lim in (("RESULT_BYTES", int(tr["max_result_bytes_per_run"])), ("TRANSFER_BYTES", transfer_limit)):
        cur.execute("INSERT INTO dq_budget (id, run_id, level, resource, limit_amount) VALUES (%s,%s,'RUN',%s,%s)",
                    (new_id(), run_id, res, lim))
    for node, read_limit in deep_nodes:
        for res, lim in (("RESULT_BYTES", read_limit),
                         ("TRANSFER_BYTES", int(cfg["diagnosis"]["max_transfer_bytes_per_run"]))):
            cur.execute("""INSERT INTO dq_budget (id, run_id, node_id, level, resource, limit_amount)
                           VALUES (%s,%s,%s,'NODE',%s,%s)""", (new_id(), run_id, node, res, lim))


def run_summary(cur, run_id) -> dict:
    cur.execute("SELECT * FROM dq_run WHERE id=%s", (run_id,))
    r = cur.fetchone()
    cur.execute("""SELECT check_kind, coverage_state, count(*) AS n FROM dq_run_check WHERE run_id=%s
                   GROUP BY 1, 2""", (run_id,))
    cov: dict = {}
    for row in cur.fetchall():
        cov.setdefault(row["check_kind"], {})[row["coverage_state"]] = row["n"]
    return {"id": str(r["id"]), "purpose": r["purpose"], "execution_state": r["execution_state"],
            "control_state": r["control_state"], "revision": r["revision"], "coverage": cov,
            "manifest_id": None if r["manifest_id"] is None else str(r["manifest_id"])}


# ---- observation materialization ---------------------------------------------------------

def observation_context(rc: dict, seq: int, control_generation, members: list[str]) -> dict:
    return {"schema": "SRDQ-OBS-2", "run_id": str(rc["run_id"]), "run_check_id": str(rc["id"]),
            "observation_seq": seq, "rule_digest": bytes(rc["rule_digest"]).hex(), "check_kind": rc["check_kind"],
            "evidence": rc["expected_evidence"], "scope_key": rc["scope_key"],
            "control_generation": str(control_generation), "members": members}


def member_context(obs_digest: bytes, node_id: str) -> dict:
    return {"schema": "SRDQ-OBS-2-MEMBER", "observation": obs_digest.hex(), "node_id": node_id}


def build_spec(rc: dict, side: str, members: list[dict]) -> dict:
    rs, xs = rc["rule_snapshot"], rc["execution_snapshot"]
    kind = rc["check_kind"]
    d = rs["definition"]
    spec: dict = {"kind": "CHECK", "check_kind": kind, "side": side, "table": xs["tables"][side],
                  "columns": xs["columns"], "limits": xs["limits"], "rule_digest_hex": bytes(rc["rule_digest"]).hex(),
                  "members": members}
    if kind == "METRIC":
        spec["rule"] = {"group_by": d.get("group_by") or [], "filter": d.get("filter") or {"op": "TRUE"},
                        "metrics": [{k: m.get(k) for k in ("id", "function", "column", "value_type", "scale")}
                                    for m in d["metrics"]]}
    elif kind == "SAMPLE":
        spec["sample"] = {"key_column": d["key_columns"][0], "key_type": d["key_type"], "seed_hex": d["seed_hex"],
                          "bucket_count": d["bucket_count"], "selected_bucket": d["selected_bucket"]}
        spec["projection"] = d["projection"]
    elif kind == "DEEP":
        spec["projection"] = d["projection"]
        spec["deep"] = {"mode": d["mode"], "buckets": d.get("buckets")}
        spec["evidence"] = rc["expected_evidence"]
    return spec


def materialize_observations(cur, ctx: Ctx, limit: int = 500) -> int:
    """Create the next observation for READY run_checks that have none open. Caller holds the control lock."""
    cur.execute("SELECT control_generation FROM dq_control_state WHERE id=1")
    gen = cur.fetchone()["control_generation"]
    cur.execute("""SELECT rc.* FROM dq_run_check rc
                   JOIN dq_run r ON r.id = rc.run_id
                   JOIN dq_task_node n ON n.id = rc.node_id
                   LEFT JOIN dq_task_node p ON p.id = n.parent_id
                   WHERE rc.execution_state = 'READY' AND rc.control_state = 'ENABLED'
                     AND r.control_state = 'ENABLED' AND r.execution_state IN ('READY','RUNNING')
                     AND n.control_state = 'ENABLED' AND COALESCE(p.control_state, 'ENABLED') = 'ENABLED'
                     AND (rc.next_eligible_at IS NULL OR rc.next_eligible_at <= %s)
                     AND NOT EXISTS (SELECT 1 FROM dq_observation o WHERE o.run_check_id = rc.id AND o.status='OPEN')
                     AND NOT EXISTS (SELECT 1 FROM dq_work_item w JOIN dq_attempt a ON a.id = w.current_attempt_id
                                     WHERE w.run_check_id = rc.id AND a.status IN
                                     ('LEASED','RUNNING','CANCEL_REQUESTED','CANCEL_PENDING'))
                   ORDER BY rc.kind_rank, rc.effective_priority DESC, rc.layer_rank, rc.ready_seq
                   LIMIT %s FOR UPDATE OF rc SKIP LOCKED""", (ctx.now(), limit))
    made = 0
    for rc in cur.fetchall():
        create_observation(cur, rc, gen)
        made += 1
    return made


def create_observation(cur, rc: dict, gen) -> Any:
    seq = rc["observation_seq"] + 1
    kind = rc["check_kind"]
    cur.execute("SELECT * FROM dq_task_node WHERE id=%s", (rc["node_id"],))
    node = cur.fetchone()
    d = rc["rule_snapshot"]["definition"]
    if kind == "DEEP":
        cur.execute("SELECT * FROM dq_task_node WHERE parent_id=%s AND kind='SHARD' ORDER BY created_at, scope_key",
                    (node["id"],))
        nodes = cur.fetchall()
        if d["mode"] == "HASH_BUCKET":
            nodes.sort(key=lambda n: next(a["bucket"] for a in n["predicate"] if a["kind"] == "HASH_BUCKET"))
    else:
        nodes = [node]
    ids = [str(n["id"]) for n in nodes]
    octx = observation_context(rc, seq, gen, ids)
    ojson = c.canonical_json(octx).decode("utf-8")
    odig = c.json_digest(octx)
    oid = new_id()
    if kind == "DEEP" and d["mode"] == "HASH_BUCKET":
        unit_ids = [new_id()] * len(nodes)
    else:
        unit_ids = [new_id() for _ in nodes]
    members = []
    for n, uid in zip(nodes, unit_ids):
        mctx = member_context(odig, str(n["id"]))
        m = {"node_id": str(n["id"]), "scan_unit_id": str(uid), "predicate": n["predicate"],
             "context_json": c.canonical_json(mctx).decode("utf-8"), "context_digest_hex": c.json_digest(mctx).hex()}
        if kind == "DEEP" and d["mode"] == "HASH_BUCKET":
            m["bucket"] = next(a["bucket"] for a in n["predicate"] if a["kind"] == "HASH_BUCKET")
        members.append(m)
    units: dict[str, list[dict]] = {}
    for m in members:
        units.setdefault(m["scan_unit_id"], []).append(m)
    contract = {"execution_units": [{"scan_unit_id": u, "member_node_ids": [m["node_id"] for m in ms]}
                                    for u, ms in units.items()],
                "members": {m["node_id"]: {"scan_unit_id": m["scan_unit_id"],
                                           "context_digest_hex": m["context_digest_hex"],
                                           "predicate": m["predicate"], "bucket": m.get("bucket")}
                            for m in members},
                "rule_digest": bytes(rc["rule_digest"]).hex(), "check_kind": kind,
                "evidence": rc["expected_evidence"]}
    cur.execute("""INSERT INTO dq_observation (id, run_id, asset_id, run_check_id, rule_id, rule_digest, check_kind,
                   observation_seq, contract, context_json, context_digest) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (oid, rc["run_id"], rc["asset_id"], rc["id"], rc["rule_id"], rc["rule_digest"], kind, seq,
                 jb(contract), ojson, odig))
    for m in members:
        cur.execute("""INSERT INTO dq_observation_member (id, observation_id, node_id, scan_unit_id, context_json,
                       context_digest) VALUES (%s,%s,%s,%s,%s,%s)""",
                    (new_id(), oid, m["node_id"], m["scan_unit_id"], m["context_json"],
                     bytes.fromhex(m["context_digest_hex"])))
    for uid, ms in units.items():
        spec_members = [{k: m[k] for k in ("node_id", "predicate", "context_digest_hex", "bucket") if k in m}
                        for m in ms]
        for side in SIDES:
            cur.execute("""INSERT INTO dq_work_item (id, run_id, asset_id, run_check_id, observation_id, side, kind,
                           check_kind, scan_unit_id, spec) VALUES (%s,%s,%s,%s,%s,%s,'CHECK',%s,%s,%s)""",
                        (new_id(), rc["run_id"], rc["asset_id"], rc["id"], oid, side, kind, uid,
                         jb(build_spec(rc, side, spec_members))))
    cur.execute("""UPDATE dq_run_check SET observation_seq=%s, current_observation_id=%s, revision=revision+1,
                   updated_at=now() WHERE id=%s""", (seq, oid, rc["id"]))
    return oid


def refresh_queue(cur) -> None:
    """Recompute effective priorities and mark the queue materialized. Caller holds the control lock."""
    cur.execute("""UPDATE dq_run_check rc SET effective_priority = COALESCE(rc.priority_override, n.priority_override,
                       p.priority_override, (rc.execution_snapshot->>'policy_priority')::int, a.priority)
                   FROM dq_task_node n LEFT JOIN dq_task_node p ON p.id = n.parent_id, dq_asset a, dq_run r
                   WHERE n.id = rc.node_id AND a.id = rc.asset_id AND r.id = rc.run_id
                     AND r.execution_state NOT IN ('COMPLETED','FAILED','CANCELLED')
                     AND rc.execution_state IN ('READY','RUNNING','PLANNED')
                     AND rc.effective_priority IS DISTINCT FROM COALESCE(rc.priority_override, n.priority_override,
                       p.priority_override, (rc.execution_snapshot->>'policy_priority')::int, a.priority)""")
    cur.execute("UPDATE dq_control_state SET queue_materialized_revision = queue_revision WHERE id=1 "
                "AND queue_materialized_revision <> queue_revision")


def selector_digest(sel: dict) -> bytes:
    return digest_json(sel)
