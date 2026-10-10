"""Asset list and rule template import: PREVIEW then CONFIRM; rule registration and approval."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
from decimal import Decimal, InvalidOperation
from typing import Any

from ..common import dsl
from ..db.db import jb, new_id
from .common import (TRANSFER_CLASS, ApiError, Ctx, as_uuid, audit, digest_json, rule_definition_digest)

ASSET_COLUMNS = ["layer", "source_db", "source_table", "target_db", "target_table", "priority", "enabled"]
METRIC_COLUMNS = ["source_db", "source_table", "rule_key", "group_by", "filter", "metric_id", "function", "column",
                  "scale", "compare", "rtol", "atol"]
SAMPLE_COLUMNS = ["source_db", "source_table", "key_column", "compare_columns", "float_rtol", "float_atol"]
LAYERS = ("ODS", "DWD", "DWS", "ADS", "DIM", "OTHER")
METRIC_ID_RE = re.compile(r"[A-Za-z0-9_]{1,64}")
RULE_KEY_RE = re.compile(r"[A-Za-z0-9_]{1,64}")
RESERVED_RULE_KEYS = ("baseline_count", "sample")
MAX_FILE_BYTES = 16 * 1024 * 1024


# ---- parsing ------------------------------------------------------------------------

def _decode(data: bytes) -> str:
    if len(data) > MAX_FILE_BYTES:
        raise ApiError(422, "FILE_TOO_LARGE", f"file exceeds {MAX_FILE_BYTES} bytes")
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        raise ApiError(422, "NOT_UTF8", "file must be UTF-8 encoded") from e


def _csv_rows(text: str, header: list[str]) -> list[dict]:
    reader = csv.reader(io.StringIO(text))
    try:
        got = [h.strip() for h in next(reader)]
    except StopIteration as e:
        raise ApiError(422, "EMPTY_FILE", "file is empty") from e
    if got != header:
        raise ApiError(422, "BAD_HEADER", f"header must be exactly: {','.join(header)}", {"got": got})
    out = []
    for n, rec in enumerate(reader, start=2):
        if not any(x.strip() for x in rec):
            continue
        errors = []
        if len(rec) != len(header):
            errors.append(f"expected {len(header)} columns, got {len(rec)}")
            rec = (rec + [""] * len(header))[:len(header)]
        out.append({"line": n, "raw": {h: rec[i].strip() for i, h in enumerate(header)}, "parse_errors": errors})
    return out


def parse_asset_file(file_name: str, data: bytes) -> list[dict]:
    text = _decode(data)
    if file_name.lower().endswith(".json"):
        try:
            doc = json.loads(text)
        except json.JSONDecodeError as e:
            raise ApiError(422, "BAD_JSON", str(e)) from e
        if not isinstance(doc, dict) or doc.get("format") != "asset-list-v1" or not isinstance(doc.get("assets"), list):
            raise ApiError(422, "BAD_FORMAT", 'JSON must be {"format": "asset-list-v1", "assets": [...]}')
        rows = []
        for i, a in enumerate(doc["assets"], start=1):
            errs = []
            if not isinstance(a, dict) or set(a) - set(ASSET_COLUMNS):
                errs.append(f"asset entry must use keys {ASSET_COLUMNS}")
                a = a if isinstance(a, dict) else {}
            raw = {h: "" if a.get(h) is None else str(a.get(h)).lower() if isinstance(a.get(h), bool)
                   else str(a.get(h)).strip() for h in ASSET_COLUMNS}
            rows.append({"line": i, "raw": raw, "parse_errors": errs})
        return rows
    return _csv_rows(text, ASSET_COLUMNS)


def _validate_asset_raw(raw: dict, default_priority: int) -> tuple[dict, list[str]]:
    errs = []
    layer = raw["layer"].upper()
    if layer not in LAYERS:
        errs.append(f"layer must be one of {LAYERS}")
    for k in ("source_db", "source_table", "target_db", "target_table"):
        try:
            dsl.check_ident(raw[k])
        except dsl.DslError:
            errs.append(f"{k} is not a valid identifier")
    pr = raw["priority"]
    priority = default_priority
    if pr != "":
        try:
            priority = int(pr)
            if not 0 <= priority <= 100:
                raise ValueError
        except ValueError:
            errs.append("priority must be an integer 0..100")
    en = raw["enabled"].lower()
    if en in ("true", "1", ""):
        enabled = True
    elif en in ("false", "0"):
        enabled = False
    else:
        enabled = None
        errs.append("enabled must be true/false/1/0")
    return ({"layer": layer, "source_db": raw["source_db"], "source_table": raw["source_table"],
             "target_db": raw["target_db"], "target_table": raw["target_table"], "priority": priority,
             "enabled": enabled}, errs)


def _evaluate_assets(cur, parsed: list[dict], mode: str, default_priority: int) -> list[dict]:
    seen_src: dict[tuple, int] = {}
    seen_tgt: dict[tuple, int] = {}
    out = []
    for p in parsed:
        values, errs = _validate_asset_raw(p["raw"], default_priority)
        errs = list(p["parse_errors"]) + errs
        row = {"line": p["line"], "values": values, "status": "ERROR", "errors": errs, "asset_id": None,
               "changes": {}}
        src, tgt = (values["source_db"], values["source_table"]), (values["target_db"], values["target_table"])
        if src in seen_src:
            errs.append(f"duplicate source table (line {seen_src[src]})")
        if tgt in seen_tgt:
            errs.append(f"duplicate target table (line {seen_tgt[tgt]})")
        seen_src.setdefault(src, p["line"])
        seen_tgt.setdefault(tgt, p["line"])
        if not errs:
            cur.execute("SELECT * FROM dq_asset WHERE source_db=%s AND source_table=%s", src)
            ex = cur.fetchone()
            cur.execute("SELECT id FROM dq_asset WHERE target_db=%s AND target_table=%s", tgt)
            tex = cur.fetchone()
            if tex and (ex is None or tex["id"] != ex["id"]):
                errs.append("target table is already paired with another source table")
            elif ex is None:
                row["status"] = "NEW"
            else:
                row["asset_id"] = str(ex["id"])
                changes = {k: [ex[k], values[k]] for k in ("layer", "target_db", "target_table", "priority",
                                                            "enabled") if ex[k] != values[k]}
                if ex["status"] != "ACTIVE":
                    changes["status"] = [ex["status"], "ACTIVE"]
                row["changes"] = changes
                if not changes:
                    row["status"] = "UNCHANGED"
                elif mode != "UPDATE_EXPLICIT":
                    errs.append("asset exists with different values; re-import with mode UPDATE_EXPLICIT")
                else:
                    row["status"] = "UPDATE"
        if errs:
            row["status"] = "ERROR"
        out.append(row)
    return out


def _summary(rows: list[dict]) -> dict:
    s: dict[str, int] = {}
    for r in rows:
        s[r["status"]] = s.get(r["status"], 0) + 1
    s["total"] = len(rows)
    return s


def _insert_batch(cur, kind: str, file_name: str, data: bytes, mode: str, parsed: list[dict], rows: list[dict],
                  options: dict | None = None) -> dict:
    bid = new_id()
    preview = {"parsed": parsed, "rows": rows}
    summary = _summary(rows)
    cur.execute("""INSERT INTO dq_import_batch (id, kind, file_name, file_digest, mode, options, preview,
                   preview_digest, summary) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (bid, kind, file_name[:255], hashlib.sha256(data).digest(), mode, jb(options or {}), jb(preview),
                 digest_json(rows), jb(summary)))
    audit(cur, "IMPORT_PREVIEWED", "import_batch", bid, {"kind": kind, "summary": summary})
    return {"id": str(bid), "kind": kind, "status": "PREVIEWED", "mode": mode, "summary": summary, "rows": rows}


def preview_assets(ctx: Ctx, file_name: str, data: bytes, mode: str = "CREATE_OR_VALIDATE") -> dict:
    if mode not in ("CREATE_OR_VALIDATE", "UPDATE_EXPLICIT"):
        raise ApiError(422, "BAD_MODE", "mode must be CREATE_OR_VALIDATE or UPDATE_EXPLICIT")
    parsed = parse_asset_file(file_name, data)
    with ctx.db.tx() as cur:
        rows = _evaluate_assets(cur, parsed, mode, ctx.cfg["scheduler"]["default_priority"])
        return _insert_batch(cur, "ASSET_LIST", file_name, data, mode, parsed, rows)


def ensure_baseline_rule(cur, asset_id) -> None:
    cur.execute("SELECT 1 FROM dq_check_rule WHERE asset_id=%s AND rule_key='baseline_count'", (asset_id,))
    if cur.fetchone():
        return
    definition = {"filter": {"op": "TRUE"}}
    cur.execute("""INSERT INTO dq_check_rule (id, asset_id, rule_key, revision, check_kind, definition,
                   definition_digest, transfer_class, status, approval_ref, approved_at)
                   VALUES (%s,%s,'baseline_count',1,'COUNT',%s,%s,'SUMMARY_ONLY','APPROVED','baseline_count:auto',now())""",
                (new_id(), asset_id, jb(definition),
                 rule_definition_digest("COUNT", definition, "CENTRAL", "SUMMARY_ONLY")))


def _apply_assets(cur, batch: dict, rows: list[dict]) -> dict:
    applied = {"created": 0, "updated": 0, "unchanged": 0, "errors": 0}
    for r in rows:
        v = r["values"]
        if r["status"] == "NEW":
            aid = new_id()
            cur.execute("""INSERT INTO dq_asset (id, layer, source_db, source_table, target_db, target_table, priority,
                           enabled, import_batch_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (aid, v["layer"], v["source_db"], v["source_table"], v["target_db"], v["target_table"],
                         v["priority"], v["enabled"], batch["id"]))
            cur.execute("INSERT INTO dq_asset_slot (asset_id) VALUES (%s) ON CONFLICT DO NOTHING", (aid,))
            ensure_baseline_rule(cur, aid)
            audit(cur, "ASSET_CREATED", "asset", aid, v)
            applied["created"] += 1
        elif r["status"] == "UPDATE":
            cur.execute("""UPDATE dq_asset SET layer=%s, target_db=%s, target_table=%s, priority=%s, enabled=%s,
                           status='ACTIVE', import_batch_id=%s, revision=revision+1, updated_at=now() WHERE id=%s""",
                        (v["layer"], v["target_db"], v["target_table"], v["priority"], v["enabled"], batch["id"],
                         r["asset_id"]))
            cur.execute("INSERT INTO dq_asset_slot (asset_id) VALUES (%s) ON CONFLICT DO NOTHING", (r["asset_id"],))
            ensure_baseline_rule(cur, r["asset_id"])
            audit(cur, "ASSET_UPDATED", "asset", r["asset_id"], r["changes"])
            applied["updated"] += 1
        elif r["status"] == "UNCHANGED":
            applied["unchanged"] += 1
        else:
            applied["errors"] += 1
    return applied


def confirm_import(ctx: Ctx, batch_id, reason: str | None = None) -> dict:
    bid = as_uuid(batch_id, "batch_id")
    with ctx.db.tx() as cur:
        cur.execute("SELECT * FROM dq_import_batch WHERE id=%s FOR UPDATE", (bid,))
        batch = cur.fetchone()
        if batch is None:
            raise ApiError(404, "NOT_FOUND", "import batch not found")
        if batch["status"] != "PREVIEWED":
            raise ApiError(409, "BATCH_NOT_PREVIEWED", f"batch is {batch['status']}")
        parsed = batch["preview"]["parsed"]
        if batch["kind"] == "ASSET_LIST":
            rows = _evaluate_assets(cur, parsed, batch["mode"], ctx.cfg["scheduler"]["default_priority"])
        elif batch["kind"] == "METRIC_TEMPLATE":
            rows = _evaluate_metric(cur, ctx, parsed)
        else:
            rows = _evaluate_sample(cur, ctx, parsed)
        if digest_json(rows) != bytes(batch["preview_digest"]):
            raise ApiError(409, "PREVIEW_STALE", "data changed since preview; preview again")
        if batch["kind"] == "ASSET_LIST":
            applied = _apply_assets(cur, batch, rows)
        else:
            kind = "METRIC" if batch["kind"] == "METRIC_TEMPLATE" else "SAMPLE"
            applied = _apply_rules(cur, batch, kind, rows)
        summary = dict(batch["summary"], applied=applied)
        cur.execute("UPDATE dq_import_batch SET status='CONFIRMED', confirmed_at=now(), reason=%s, summary=%s "
                    "WHERE id=%s", (reason, jb(summary), bid))
        audit(cur, "IMPORT_CONFIRMED", "import_batch", bid, {"kind": batch["kind"], "applied": applied}, reason)
        return {"id": str(bid), "status": "CONFIRMED", "summary": summary}


def reject_import(ctx: Ctx, batch_id, reason: str | None = None) -> dict:
    bid = as_uuid(batch_id, "batch_id")
    with ctx.db.tx() as cur:
        cur.execute("UPDATE dq_import_batch SET status='REJECTED', reason=%s WHERE id=%s AND status='PREVIEWED' "
                    "RETURNING id", (reason, bid))
        if cur.fetchone() is None:
            raise ApiError(409, "BATCH_NOT_PREVIEWED", "batch is not in PREVIEWED state")
        audit(cur, "IMPORT_REJECTED", "import_batch", bid, {}, reason)
    return {"id": str(bid), "status": "REJECTED"}


# ---- metadata helpers ----------------------------------------------------------------

def latest_inventory(cur) -> dict | None:
    cur.execute("SELECT id, inventory FROM dq_manifest WHERE status IN ('DRAFT','CONFIRMED') "
                "ORDER BY version DESC LIMIT 1")
    r = cur.fetchone()
    return None if r is None else r["inventory"]


def common_columns(entry: dict) -> tuple[dict, dict]:
    """Columns present on both sides with their shared logical type; mismatches maps name -> reason."""
    src = {c["name"]: c for c in (entry.get("source") or {}).get("columns") or []}
    tgt = {c["name"]: c for c in (entry.get("target") or {}).get("columns") or []}
    cols: dict[str, dict] = {}
    mism: dict[str, str] = {}
    for name in sorted(set(src) | set(tgt), key=lambda n: (src.get(n, tgt.get(n))["ordinal"], n)):
        if name not in tgt:
            mism[name] = "MISSING_ON_TARGET"
            continue
        if name not in src:
            mism[name] = "MISSING_ON_SOURCE"
            continue
        ls, ss = dsl.logical_type(src[name]["data_type"], src[name]["column_type"])
        lt_, st = dsl.logical_type(tgt[name]["data_type"], tgt[name]["column_type"])
        if ls != lt_ or ss != st:
            mism[name] = f"TYPE_MISMATCH:{src[name]['column_type']}|{tgt[name]['column_type']}"
            continue
        if ls == "UNSUPPORTED":
            mism[name] = f"UNSUPPORTED_TYPE:{src[name]['column_type']}"
            continue
        cols[name] = {"type": ls, "scale": ss, "fsp": 6,
                      "nullable": bool(src[name]["nullable"] or tgt[name]["nullable"])}
    return cols, mism


def spec_columns(cols: dict, names) -> dict:
    return {n: {"type": cols[n]["type"], "scale": cols[n]["scale"], "fsp": cols[n].get("fsp", 6)}
            for n in sorted(set(names))}


def _asset_and_entry(cur, inv: dict | None, db: str, table: str) -> tuple[dict | None, dict | None, str | None]:
    cur.execute("SELECT * FROM dq_asset WHERE source_db=%s AND source_table=%s AND status='ACTIVE'", (db, table))
    asset = cur.fetchone()
    if asset is None:
        return None, None, "ASSET_NOT_FOUND: table is not in the asset list"
    if inv is None:
        return asset, None, "NO_METADATA: run discovery first"
    entry = inv.get("assets", {}).get(str(asset["id"]))
    if entry is None:
        return asset, None, "NO_METADATA: asset not in the latest discovery"
    for side in ("source", "target"):
        st = (entry.get(side) or {}).get("status")
        if st != "PRESENT":
            return asset, None, f"TABLE_NOT_PRESENT: {side} status {st}"
    return asset, entry, None


def _tol(text: str, name: str) -> str:
    try:
        d = Decimal(text)
    except InvalidOperation as e:
        raise ValueError(f"{name} must be a number") from e
    if not d.is_finite() or d < 0:
        raise ValueError(f"{name} must be a finite non-negative number")
    return text


# ---- METRIC templates --------------------------------------------------------------------

def preview_metric_rules(ctx: Ctx, file_name: str, data: bytes) -> dict:
    parsed = _csv_rows(_decode(data), METRIC_COLUMNS)
    with ctx.db.tx() as cur:
        rows = _evaluate_metric(cur, ctx, parsed)
        return _insert_batch(cur, "METRIC_TEMPLATE", file_name, data, "CREATE_OR_VALIDATE", parsed, rows)


def _evaluate_metric(cur, ctx: Ctx, parsed: list[dict]) -> list[dict]:
    mcfg = ctx.cfg["checks"]["metric"]
    inv = latest_inventory(cur)
    groups: dict[tuple, list[dict]] = {}
    for p in parsed:
        r = p["raw"]
        groups.setdefault((r["source_db"], r["source_table"], r["rule_key"]), []).append(p)
    out = []
    for (db, table, rule_key), items in groups.items():
        row = {"lines": [p["line"] for p in items], "source_db": db, "source_table": table, "rule_key": rule_key,
               "status": "OK", "errors": [], "unsupported": [], "asset_id": None, "definition": None}
        errs, unsup = row["errors"], row["unsupported"]
        for p in items:
            errs += [f"line {p['line']}: {e}" for e in p["parse_errors"]]
        if not RULE_KEY_RE.fullmatch(rule_key or "") or rule_key in RESERVED_RULE_KEYS:
            errs.append("rule_key must match [A-Za-z0-9_]{1,64} and not be a reserved key")
        asset, entry, why = _asset_and_entry(cur, inv, db, table)
        if asset is not None:
            row["asset_id"] = str(asset["id"])
        if why:
            errs.append(why)
        if len({(p["raw"]["group_by"], p["raw"]["filter"]) for p in items}) > 1:
            errs.append("group_by and filter must be identical on every row of the same rule")
        if errs:
            row["status"] = "ERROR"
            out.append(row)
            continue
        cols, mism = common_columns(entry)
        first = items[0]["raw"]
        group_by = [g.strip() for g in first["group_by"].split(";") if g.strip()]
        for g in group_by:
            if g in mism:
                unsup.append(f"group column {g}: {mism[g]}")
            elif g not in cols:
                errs.append(f"group column {g} not found")
            elif dsl.is_float(cols[g]["type"]):
                unsup.append(f"group column {g} is a float column")
        if len(set(group_by)) != len(group_by):
            errs.append("duplicate group_by column")
        filt: Any = {"op": "TRUE"}
        if first["filter"]:
            try:
                filt = json.loads(first["filter"])
                dsl.validate_filter(filt, cols)
            except (json.JSONDecodeError, dsl.DslError) as e:
                errs.append(f"invalid filter: {e}")
        metrics: list[dict] = []
        ids = set()
        for p in items:
            r = p["raw"]
            mid, fn, col = r["metric_id"], r["function"].upper(), r["column"]
            compare = (r["compare"] or "EXACT").upper()
            if not METRIC_ID_RE.fullmatch(mid) or mid in ids:
                errs.append(f"line {p['line']}: metric_id must be unique and match [A-Za-z0-9_]{{1,64}}")
                continue
            ids.add(mid)
            if compare not in ("EXACT", "TOLERANCE"):
                errs.append(f"line {p['line']}: compare must be EXACT or TOLERANCE")
                continue
            m = {"id": mid, "function": fn, "column": None, "value_type": "INTEGER", "scale": None,
                 "compare": "EXACT", "rtol": None, "atol": None}
            if fn == "COUNT_ROWS":
                if col:
                    errs.append(f"line {p['line']}: COUNT_ROWS takes no column")
            elif fn in ("COUNT_NONNULL", "SUM"):
                if not col:
                    errs.append(f"line {p['line']}: {fn} needs a column")
                    continue
                if col in mism:
                    unsup.append(f"metric {mid}: column {col} {mism[col]}")
                    m["column"] = col
                elif col not in cols:
                    errs.append(f"line {p['line']}: column {col} not found")
                    continue
                m["column"] = col
                if fn == "SUM" and col in cols:
                    lt = cols[col]["type"]
                    if lt == "INTEGER":
                        m["value_type"] = "INTEGER"
                    elif lt == "DECIMAL":
                        scale = cols[col]["scale"]
                        if r["scale"]:
                            try:
                                s = int(r["scale"])
                            except ValueError:
                                errs.append(f"line {p['line']}: scale must be an integer")
                                continue
                            if s < scale:
                                errs.append(f"line {p['line']}: scale {s} is below the column scale {scale}")
                                continue
                            scale = s
                        m["value_type"], m["scale"] = "DECIMAL", scale
                    elif dsl.is_float(lt):
                        m["value_type"] = "FLOAT64"
                        if compare != "TOLERANCE" or not r["rtol"] or not r["atol"]:
                            unsup.append(f"metric {mid}: float SUM needs compare=TOLERANCE with rtol/atol")
                    else:
                        unsup.append(f"metric {mid}: SUM over {lt} is not supported")
            else:
                errs.append(f"line {p['line']}: function must be COUNT_ROWS, COUNT_NONNULL or SUM")
                continue
            if compare == "TOLERANCE":
                if m["value_type"] != "FLOAT64":
                    errs.append(f"line {p['line']}: TOLERANCE is only allowed for float SUM")
                    continue
                try:
                    m["compare"], m["rtol"], m["atol"] = ("TOLERANCE", _tol(r["rtol"] or "x", "rtol"),
                                                          _tol(r["atol"] or "x", "atol"))
                except ValueError:
                    m["compare"] = "TOLERANCE"
            metrics.append(m)
        if not any(m["function"] == "COUNT_ROWS" for m in metrics):
            if "group_rows" in ids:
                errs.append("metric id group_rows is reserved for the automatic COUNT_ROWS")
            metrics.insert(0, {"id": "group_rows", "function": "COUNT_ROWS", "column": None, "value_type": "INTEGER",
                               "scale": None, "compare": "EXACT", "rtol": None, "atol": None})
        row["definition"] = {
            "group_by": group_by, "filter": filt, "metrics": metrics, "include_group_row_count": True,
            "sum_non_null_guard": True, "max_groups_per_side": mcfg["max_groups_per_side"],
            "max_group_key_bytes": mcfg["max_group_key_bytes"],
            "max_result_bytes_per_side": str(mcfg["max_result_bytes_per_side"])}
        row["status"] = "ERROR" if errs else "UNSUPPORTED" if unsup else "OK"
        out.append(row)
    return out


# ---- SAMPLE templates --------------------------------------------------------------------

def preview_sample_rules(ctx: Ctx, file_name: str, data: bytes) -> dict:
    parsed = _csv_rows(_decode(data), SAMPLE_COLUMNS)
    with ctx.db.tx() as cur:
        rows = _evaluate_sample(cur, ctx, parsed)
        return _insert_batch(cur, "SAMPLE_TEMPLATE", file_name, data, "CREATE_OR_VALIDATE", parsed, rows)


def _evaluate_sample(cur, ctx: Ctx, parsed: list[dict]) -> list[dict]:
    scfg = ctx.cfg["checks"]["sample"]
    inv = latest_inventory(cur)
    seen = set()
    out = []
    for p in parsed:
        r = p["raw"]
        db, table, key = r["source_db"], r["source_table"], r["key_column"]
        row = {"lines": [p["line"]], "source_db": db, "source_table": table, "rule_key": "sample", "status": "OK",
               "errors": list(p["parse_errors"]), "unsupported": [], "asset_id": None, "definition": None}
        errs, unsup = row["errors"], row["unsupported"]
        if (db, table) in seen:
            errs.append("duplicate table in template")
        seen.add((db, table))
        asset, entry, why = _asset_and_entry(cur, inv, db, table)
        if asset is not None:
            row["asset_id"] = str(asset["id"])
        if why:
            errs.append(why)
        if errs:
            row["status"] = "ERROR"
            out.append(row)
            continue
        cols, mism = common_columns(entry)
        key_type = None
        if key in mism:
            unsup.append(f"key column {key}: {mism[key]}")
        elif key not in cols:
            errs.append(f"key column {key} not found")
        else:
            key_type = cols[key]["type"]
            if key_type not in ("INTEGER", "STRING"):
                unsup.append(f"key column type {key_type} is not a builtin key contract")
                key_type = None
        for side in ("source", "target"):
            meta = entry[side]
            if meta.get("model") not in ("PRI_KEYS", "UNQ_KEYS") or meta.get("key_columns") != [key]:
                unsup.append(f"{side} table model {meta.get('model')} keys {meta.get('key_columns')} is not a "
                             f"single-column primary/unique key on {key}")
        spec = r["compare_columns"]
        skipped = []
        if spec.strip() == "*":
            wanted = list(cols)
            skipped = sorted(mism)
        else:
            wanted = [x.strip() for x in spec.split(";") if x.strip()]
            if len(set(wanted)) != len(wanted):
                errs.append("duplicate compare column")
            for w in wanted:
                if w in mism:
                    unsup.append(f"compare column {w}: {mism[w]}")
                elif w not in cols:
                    errs.append(f"compare column {w} not found")
            if key not in wanted:
                errs.append("compare_columns must include the key column")
        wanted = [w for w in wanted if w in cols]
        exact = [w for w in wanted if not dsl.is_float(cols[w]["type"])]
        tolerant = [w for w in wanted if dsl.is_float(cols[w]["type"])]
        if key in exact:
            exact.remove(key)
        exact.insert(0, key)
        tol = None
        if tolerant:
            if not r["float_rtol"] or not r["float_atol"]:
                unsup.append(f"float columns {tolerant} need float_rtol/float_atol")
            else:
                try:
                    tol = {"rtol": _tol(r["float_rtol"], "float_rtol"), "atol": _tol(r["float_atol"], "float_atol")}
                except ValueError as e:
                    errs.append(str(e))
        contract = {"INTEGER": "NON_NULL_UNIQUE_INTEGER", "STRING": "NON_NULL_UNIQUE_STRING"}.get(key_type)
        if contract and contract not in scfg["builtin_key_contract"]:
            unsup.append(f"key contract {contract} is not enabled")
        row["definition"] = {
            "key_columns": [key], "compare_columns": [key] + [w for w in wanted if w != key],
            "key_type": key_type, "key_contract": contract, "sample_protocol": scfg["protocol"],
            "seed_hex": scfg["default_seed_hex"], "bucket_count": scfg["bucket_count"],
            "selected_bucket": scfg["selected_bucket"],
            "projection": {"key_columns": [key], "exact_columns": exact, "tolerant_columns": tolerant},
            "float_tolerance": tol, "skipped_columns": skipped,
            "min_sample_rows_per_side": scfg["min_sample_rows_per_side"],
            "max_sample_rows_per_side": scfg["max_sample_rows_per_side"],
            "max_result_bytes_per_side": str(scfg["max_result_bytes_per_side"]),
            "max_key_bytes": scfg["max_key_bytes"], "filter_location": scfg["filter_location"],
            "fingerprint_location": scfg["fingerprint_location"], "empty_sample_policy": scfg["empty_sample_policy"],
            "uniqueness_evidence": scfg["uniqueness_evidence"]}
        row["status"] = "ERROR" if errs else "UNSUPPORTED" if unsup else "OK"
        out.append(row)
    return out


# ---- rule registration & approval ---------------------------------------------------------

def register_rule(cur, asset_id, rule_key: str, kind: str, definition: dict, *, support_state: str = "SUPPORTED",
                  support_reason: str | None = None, batch_id=None) -> tuple[Any, bool]:
    """Create a DRAFT revision unless an identical definition already exists. Returns (rule_id, created)."""
    tc = TRANSFER_CLASS[kind]
    digest = rule_definition_digest(kind, definition, "CENTRAL", tc)
    cur.execute("""SELECT id FROM dq_check_rule WHERE asset_id=%s AND rule_key=%s AND definition_digest=%s
                   AND support_state=%s AND status IN ('DRAFT','APPROVED') ORDER BY revision DESC LIMIT 1""",
                (asset_id, rule_key, digest, support_state))
    ex = cur.fetchone()
    if ex:
        return ex["id"], False
    cur.execute("""SELECT id, revision, status, check_kind FROM dq_check_rule WHERE asset_id=%s AND rule_key=%s
                   ORDER BY revision DESC""", (asset_id, rule_key))
    prev = cur.fetchall()
    if prev and prev[0]["check_kind"] != kind:
        raise ApiError(409, "RULE_KIND_CONFLICT", f"rule_key {rule_key} already used for {prev[0]['check_kind']}")
    approved = next((p for p in prev if p["status"] == "APPROVED"), None)
    supersedes = approved["id"] if approved else (prev[0]["id"] if prev else None)
    rid = new_id()
    cur.execute("""INSERT INTO dq_check_rule (id, asset_id, rule_key, revision, check_kind, definition,
                   definition_digest, transfer_class, status, support_state, support_reason, supersedes_id,
                   import_batch_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'DRAFT',%s,%s,%s,%s)""",
                (rid, asset_id, rule_key, (prev[0]["revision"] + 1) if prev else 1, kind, jb(definition), digest, tc,
                 support_state, support_reason, supersedes, batch_id))
    audit(cur, "RULE_DRAFTED", "check_rule", rid, {"asset_id": str(asset_id), "rule_key": rule_key, "kind": kind,
                                                   "support_state": support_state})
    return rid, True


def _apply_rules(cur, batch: dict, kind: str, rows: list[dict]) -> dict:
    applied = {"drafted": 0, "unchanged": 0, "errors": 0}
    for r in rows:
        if r["status"] == "ERROR":
            applied["errors"] += 1
            continue
        unsupported = r["status"] == "UNSUPPORTED"
        _rid, created = register_rule(cur, r["asset_id"], r["rule_key"], kind, r["definition"],
                                      support_state="UNSUPPORTED" if unsupported else "SUPPORTED",
                                      support_reason="; ".join(r["unsupported"])[:2000] if unsupported else None,
                                      batch_id=batch["id"])
        applied["drafted" if created else "unchanged"] += 1
    return applied


def approve_rules(ctx: Ctx, rule_ids: list, approval_ref: str | None = None, reason: str | None = None) -> dict:
    ref = approval_ref or (ctx.cfg["transfer"]["global_approval_ref"] if ctx.cfg.transfer_approved else None)
    if not ref:
        raise ApiError(422, "APPROVAL_REF_REQUIRED", "no approval_ref given and the global approval is not registered")
    ids = [as_uuid(r, "rule_id") for r in rule_ids]
    out = []
    with ctx.db.tx() as cur:
        for rid in ids:
            cur.execute("SELECT * FROM dq_check_rule WHERE id=%s FOR UPDATE", (rid,))
            rule = cur.fetchone()
            if rule is None:
                raise ApiError(404, "NOT_FOUND", f"rule {rid} not found")
            if rule["status"] == "APPROVED":
                out.append({"id": str(rid), "status": "APPROVED", "changed": False})
                continue
            if rule["status"] != "DRAFT":
                raise ApiError(409, "RULE_NOT_DRAFT", f"rule {rid} is {rule['status']}")
            if rule["check_kind"] == "DEEP":
                raise ApiError(422, "DEEP_RULE", "DEEP rules are approved through manual requests only")
            cur.execute("""UPDATE dq_check_rule SET status='SUPERSEDED', updated_at=now()
                           WHERE asset_id=%s AND rule_key=%s AND status='APPROVED' RETURNING id""",
                        (rule["asset_id"], rule["rule_key"]))
            old = [str(r["id"]) for r in cur.fetchall()]
            cur.execute("""UPDATE dq_check_rule SET status='APPROVED', approval_ref=%s, approved_at=now(),
                           updated_at=now() WHERE id=%s""", (ref, rid))
            audit(cur, "RULE_APPROVED", "check_rule", rid, {"approval_ref": ref, "superseded": old}, reason)
            out.append({"id": str(rid), "status": "APPROVED", "changed": True, "superseded": old})
    return {"rules": out}


def set_rule_enabled(ctx: Ctx, rule_id, enabled: bool, reason: str | None = None) -> dict:
    rid = as_uuid(rule_id, "rule_id")
    with ctx.db.tx() as cur:
        cur.execute("UPDATE dq_check_rule SET enabled=%s, updated_at=now() WHERE id=%s RETURNING id, enabled",
                    (bool(enabled), rid))
        if cur.fetchone() is None:
            raise ApiError(404, "NOT_FOUND", "rule not found")
        audit(cur, "RULE_ENABLED_CHANGED", "check_rule", rid, {"enabled": bool(enabled)}, reason)
    return {"id": str(rid), "enabled": bool(enabled)}
