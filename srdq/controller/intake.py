"""Result intake (doc 03 §13.3): chunk staging, envelope gate, streamed material verification, publication."""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..common import canonical as c
from ..common import dsl
from ..db.db import jb, new_id
from ..executor import encoder as enc
from . import budgets, queue
from .common import (TRANSFER_CLASS, ApiError, Ctx, as_uuid, digest_json, incident, lock_control, parse_ts, pg_now,
                     protocol_ok)

MAX_CHUNK_NO = 100_000
LIVE = ("LEASED", "RUNNING")
CANCELLING = ("CANCEL_REQUESTED", "CANCEL_PENDING")
INT63 = 2**63
ERROR_CLASSES = ("TIMEOUT", "CANCELLED", "ACCESS_DENIED", "NETWORK", "QUERY_ERROR", "STORAGE_SUSPECT",
                 "BUDGET_EXCEEDED", "CONTRACT_VIOLATION", "UNSUPPORTED", "LOCAL_RESOURCE", "INTERNAL")
METADATA_NO_RETRY = ("ACCESS_DENIED", "CONTRACT_VIOLATION", "UNSUPPORTED")


class Reject(Exception):
    """A result that fails verification; the attempt fails with error_class (default CONTRACT_VIOLATION)."""

    def __init__(self, code: str, message: str, error_class: str = "CONTRACT_VIOLATION"):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.error_class = error_class


@dataclass
class Verified:
    node_id: str
    payload: dict
    evidence: str
    context_digest: bytes
    read_started: _dt.datetime
    read_finished: _dt.datetime
    total_bytes: int
    row_count: int | None
    chunk_dir: Path | None
    groups: list[dict] | None = None


def chunk_name(n: int) -> str:
    return f"chunk_{int(n):05d}.jsonl"


def staging_path(ctx: Ctx, attempt_id, node_id) -> Path:
    return ctx.staging_dir / str(attempt_id) / str(node_id)


def material_path(ctx: Ctx, side_result_id) -> Path:
    return ctx.material_dir / str(side_result_id)


def material_files(ctx: Ctx, side_result_id, payload: dict) -> list[Path]:
    d = material_path(ctx, side_result_id)
    return [d / chunk_name(ch["chunk_no"]) for ch in payload.get("chunks_manifest") or []]


# ---- chunk staging ------------------------------------------------------------------------

def stage_chunk(ctx: Ctx, worker: dict, attempt_id, node_id, chunk_no, data: bytes, sha256_hex: str | None) -> dict:
    aid = as_uuid(attempt_id, "attempt_id")
    nid = as_uuid(node_id, "node_id")
    try:
        n = int(chunk_no)
    except (TypeError, ValueError) as e:
        raise ApiError(422, "BAD_CHUNK_NO", "chunk_no must be an integer") from e
    if not 1 <= n <= MAX_CHUNK_NO:
        raise ApiError(422, "BAD_CHUNK_NO", f"chunk_no must be 1..{MAX_CHUNK_NO}")
    ex = ctx.cfg["execution"]
    cap = 2 * int(ex["max_row_bytes"]) + int(ex["batch_bytes"]) + (1 << 20)
    if len(data) > cap:
        raise ApiError(413, "CHUNK_TOO_LARGE", f"chunk exceeds {cap} bytes")
    digest = hashlib.sha256(data).hexdigest()
    if not sha256_hex or digest != str(sha256_hex).lower():
        raise ApiError(422, "CHUNK_DIGEST_MISMATCH", "X-Content-SHA256 does not match the body")
    a = ctx.db.one("""SELECT a.status, w.spec FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                      WHERE a.id=%s AND a.worker_id=%s""", (aid, worker["id"]))
    if a is None:
        raise ApiError(404, "UNKNOWN_ATTEMPT", "attempt not found for this worker")
    if a["status"] not in LIVE:
        raise ApiError(409, "ATTEMPT_NOT_LIVE", f"attempt is {a['status']}")
    if str(nid) not in {m["node_id"] for m in (a["spec"] or {}).get("members") or []}:
        raise ApiError(422, "UNKNOWN_MEMBER", "node is not a member of this work item")
    d = staging_path(ctx, aid, nid)
    d.mkdir(parents=True, exist_ok=True)
    tmp = d / f".{chunk_name(n)}.{os.getpid()}.tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, d / chunk_name(n))
    return {"stored": True, "chunk_no": n, "bytes": len(data), "sha256": digest}


# ---- attempt state helpers --------------------------------------------------------------------

def _close_attempt(cur, a: dict, status: str, error_class: str | None = None, code: str | None = None,
                   message: str | None = None) -> None:
    entries = budgets.progress_entries(a["progress"])
    budgets.settle(cur, entries)
    prog = budgets.with_entries(a["progress"], entries)
    cur.execute("""UPDATE dq_attempt SET status=%s, progress=%s, finished_at=now(),
                   duration_ms = (extract(epoch FROM now() - claimed_at) * 1000)::bigint,
                   error_class = COALESCE(%s, error_class), error_code = COALESCE(%s, error_code),
                   error_message = COALESCE(%s, error_message) WHERE id=%s""",
                (status, jb(prog), error_class, code, None if message is None else message[:2000], a["id"]))
    a["progress"] = prog
    a["status"] = status


def _work_current(w: dict, a: dict) -> bool:
    return w["current_attempt_id"] == a["id"]


def _metadata_retry(cur, ctx: Ctx, w: dict, error_class: str, code: str | None, message: str) -> None:
    cur.execute("SELECT count(*) AS n FROM dq_attempt WHERE work_item_id=%s AND status IN ('FAILED','REJECTED')",
                (w["id"],))
    failed = int(cur.fetchone()["n"])
    if error_class in METADATA_NO_RETRY or failed > int(ctx.cfg["execution"]["max_retries"]):
        queue.fail_work(cur, w["id"], error_class, code, message)
    else:
        cur.execute("UPDATE dq_work_item SET status='READY', updated_at=now() WHERE id=%s", (w["id"],))


def finish_cancelled(cur, ctx: Ctx, a: dict, w: dict | None = None) -> None:
    """The attempt's query is known to have ended after a cancel request (or a lost lease)."""
    if w is None:
        cur.execute("SELECT * FROM dq_work_item WHERE id=%s FOR UPDATE", (a["work_item_id"],))
        w = cur.fetchone()
    reason = a["cancel_reason"] or "EXECUTOR_CANCELLED"
    if reason == "FAILED_UNCONFIRMED":
        _close_attempt(cur, a, "FAILED")
        if _work_current(w, a):
            if w["kind"] == "METADATA":
                _metadata_retry(cur, ctx, w, a["error_class"] or "INTERNAL", a["error_code"],
                                a["error_message"] or "failed")
            else:
                cur.execute("UPDATE dq_work_item SET status='FAILED', updated_at=now() WHERE id=%s", (w["id"],))
        return
    cls = "BUDGET_EXCEEDED" if reason == "BUDGET_EXCEEDED" else "CANCELLED"
    _close_attempt(cur, a, "CANCELLED", cls, reason, f"cancelled: {reason}")
    if not _work_current(w, a) or w["status"] in ("DONE", "FAILED", "ABANDONED"):
        return
    if w["kind"] == "METADATA":
        cur.execute("UPDATE dq_work_item SET status='READY', updated_at=now() WHERE id=%s", (w["id"],))
    elif cls == "BUDGET_EXCEEDED":
        queue.fail_work(cur, w["id"], cls, reason, "cancelled because a budget was exceeded")
    else:
        cur.execute("""UPDATE dq_work_item SET status='CANCELLED', updated_at=now(),
                       result = COALESCE(result, '{}'::jsonb) || jsonb_build_object('failure', %s::jsonb)
                       WHERE id=%s""", (jb({"error_class": cls, "error_code": reason,
                                             "message": f"cancelled: {reason}"}), w["id"]))


def _gate(cur, ctx: Ctx, worker: dict, aid, wid, lg: int, result_kind: str):
    """Lock and check attempt/work/observation. Returns (attempt, work, observation, error, observation_to_eval)."""
    cur.execute("SELECT * FROM dq_attempt WHERE id=%s AND worker_id=%s FOR UPDATE", (aid, worker["id"]))
    a = cur.fetchone()
    if a is None:
        raise ApiError(404, "UNKNOWN_ATTEMPT", "attempt not found for this worker")
    if a["lease_generation"] != lg or (wid is not None and a["work_item_id"] != wid):
        raise ApiError(409, "LEASE_MISMATCH", "lease generation or work item does not match the attempt")
    cur.execute("SELECT * FROM dq_work_item WHERE id=%s FOR UPDATE", (a["work_item_id"],))
    w = cur.fetchone()
    if result_kind is not None and (w["kind"] == "METADATA") != (result_kind == "METADATA"):
        raise ApiError(422, "RESULT_KIND_MISMATCH", f"work item is {w['kind']}")
    if a["status"] in CANCELLING:
        finish_cancelled(cur, ctx, a, w)
        return a, w, None, ApiError(409, "ATTEMPT_CANCELLED", f"attempt was cancelled ({a['cancel_reason']})"), \
            w["observation_id"]
    if a["status"] not in LIVE:
        raise ApiError(409, "ATTEMPT_NOT_LIVE", f"attempt is {a['status']}")
    if a["lease_expires_at"] <= pg_now(cur):
        _close_attempt(cur, a, "REJECTED", "TIMEOUT", "LEASE_EXPIRED", "result arrived after the lease expired")
        if _work_current(w, a):
            if w["kind"] == "METADATA":
                cur.execute("UPDATE dq_work_item SET status='READY', updated_at=now() WHERE id=%s", (w["id"],))
            else:
                queue.fail_work(cur, w["id"], "TIMEOUT", "LEASE_EXPIRED", "result arrived after the lease expired")
        return a, w, None, ApiError(409, "LEASE_EXPIRED", "lease expired before the result arrived"), \
            w["observation_id"]
    o = None
    if w["kind"] == "CHECK":
        cur.execute("SELECT * FROM dq_observation WHERE id=%s FOR UPDATE", (w["observation_id"],))
        o = cur.fetchone()
        if o["status"] != "OPEN":
            _close_attempt(cur, a, "REJECTED", None, "OBSERVATION_CLOSED", f"observation is {o['status']}")
            cur.execute("UPDATE dq_work_item SET status='ABANDONED', updated_at=now() WHERE id=%s "
                        "AND status IN ('READY','LEASED')", (w["id"],))
            return a, w, o, ApiError(409, "OBSERVATION_CLOSED", f"observation is {o['status']}"), None
    return a, w, o, None, None


def _fail_check(cur, a: dict, w: dict, error_class: str, code: str, message: str) -> None:
    _close_attempt(cur, a, "FAILED", error_class, code, message)
    if _work_current(w, a):
        queue.fail_work(cur, w["id"], error_class, code, message)


# ---- result envelope ---------------------------------------------------------------------------

def _envelope_ids(env: Any) -> tuple[Any, Any, int, str]:
    if not isinstance(env, dict):
        raise ApiError(422, "BAD_ENVELOPE", "result envelope must be an object")
    protocol_ok(env.get("protocol_version"))
    kind = env.get("result_kind")
    if kind not in ("METADATA", "CHECK"):
        raise ApiError(422, "BAD_RESULT_KIND", "result_kind must be METADATA or CHECK")
    aid = as_uuid(env.get("attempt_id"), "attempt_id")
    wid = as_uuid(env.get("work_item_id"), "work_item_id")
    lg = env.get("lease_generation")
    if not isinstance(lg, int) or isinstance(lg, bool):
        raise ApiError(422, "BAD_LEASE_GENERATION", "lease_generation must be an integer")
    return aid, wid, lg, kind


def _replay(ctx: Ctx, worker: dict, aid, edig: bytes) -> dict | None:
    r = ctx.db.one("""SELECT a.status, w.result FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                      WHERE a.id=%s AND a.worker_id=%s""", (aid, worker["id"]))
    if r is None or r["status"] != "SUCCEEDED":
        return None
    res = r["result"] or {}
    receipt = res.get("receipt")
    if receipt is None or receipt.get("attempt_id") != str(aid):
        return None
    if res.get("envelope_digest") != edig.hex():
        raise ApiError(409, "ENVELOPE_CONFLICT", "a different result was already accepted for this attempt")
    return dict(receipt, replay=True)


def accept_result(ctx: Ctx, worker: dict, env: dict) -> dict:
    from . import evaluate

    aid, wid, lg, kind = _envelope_ids(env)
    edig = digest_json(env)
    replay = _replay(ctx, worker, aid, edig)
    if replay is not None:
        return replay
    if kind == "METADATA":
        return _accept_metadata(ctx, worker, env, aid, wid, lg, edig)
    err = None
    eval_oid = None
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        _generation_ok(ctl, env)
        a, w, o, err, eval_oid = _gate(cur, ctx, worker, aid, wid, lg, kind)
        if err is None:
            try:
                members = _check_envelope(env, w, o)
            except Reject as rj:
                _fail_check(cur, a, w, rj.error_class, rj.code, rj.message)
                err, eval_oid = ApiError(422, rj.code, rj.message), w["observation_id"]
        spec = w["spec"]
    if err is not None:
        _after(ctx, evaluate, eval_oid)
        raise err
    try:
        verified = [_verify_member(ctx, spec, m, staging_path(ctx, aid, m["node_id"])) for m in members]
    except Reject as rj:
        with ctx.db.tx() as cur:
            lock_control(cur)
            a, w, o, err, eval_oid = _gate(cur, ctx, worker, aid, wid, lg, kind)
            if err is None:
                _fail_check(cur, a, w, rj.error_class, rj.code, rj.message)
                err, eval_oid = ApiError(422, rj.code, rj.message), w["observation_id"]
        shutil.rmtree(ctx.staging_dir / str(aid), ignore_errors=True)
        _after(ctx, evaluate, eval_oid)
        raise err from rj
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        _generation_ok(ctl, env)
        a, w, o, err, eval_oid = _gate(cur, ctx, worker, aid, wid, lg, kind)
        if err is None:
            receipt, moves = _publish(cur, ctx, ctl, env, edig, a, w, verified)
            eval_oid = w["observation_id"]
    if err is not None:
        _after(ctx, evaluate, eval_oid)
        raise err
    for src, dst in moves:
        _move_material(src, dst)
    shutil.rmtree(ctx.staging_dir / str(aid), ignore_errors=True)
    _after(ctx, evaluate, eval_oid)
    return receipt


def _after(ctx: Ctx, evaluate, oid) -> None:
    if oid is not None:
        evaluate.evaluate_observation(ctx, oid)


def _generation_ok(ctl: dict, env: dict) -> None:
    if ctl["status"] == "RECOVERY":
        raise ApiError(503, "RECOVERY", "controller is in recovery; retry later")
    if str(env.get("control_generation")) != str(ctl["control_generation"]):
        raise ApiError(409, "GENERATION_MISMATCH", "result belongs to another control generation")


def _move_material(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        shutil.rmtree(dst)
    if src.exists():
        os.replace(src, dst)
    else:
        dst.mkdir(parents=True, exist_ok=True)


def _check_envelope(env: dict, w: dict, o: dict) -> list[dict]:
    for k in ("observation_id", "run_check_id", "scan_unit_id"):
        if str(env.get(k)) != str(w[k]):
            raise Reject("ENVELOPE_MISMATCH", f"{k} does not match the work item")
    if env.get("side") != w["side"] or env.get("check_kind") != w["check_kind"]:
        raise Reject("ENVELOPE_MISMATCH", "side or check_kind does not match the work item")
    if env.get("rule_id") is not None and str(env.get("rule_id")) != str(o["rule_id"]):
        raise Reject("ENVELOPE_MISMATCH", "rule_id does not match the observation")
    contract = o["contract"]
    unit = next((u for u in contract["execution_units"] if u["scan_unit_id"] == str(w["scan_unit_id"])), None)
    members = env.get("members")
    if unit is None or not isinstance(members, list):
        raise Reject("MEMBER_SET", "members must be a list for a known scan unit")
    ids = [m.get("node_id") if isinstance(m, dict) else None for m in members]
    if len(set(ids)) != len(ids) or set(ids) != set(unit["member_node_ids"]):
        raise Reject("MEMBER_SET", "members do not equal the scan unit members")
    out = []
    for m in members:
        cm = contract["members"][m["node_id"]]
        if m.get("context_digest_hex") != cm["context_digest_hex"]:
            raise Reject("CONTEXT_MISMATCH", f"member {m['node_id']} context digest differs from the contract")
        if m.get("evidence") != contract["evidence"]:
            raise Reject("EVIDENCE_MISMATCH", f"member {m['node_id']} evidence differs from the contract")
        if m.get("read_complete") is not True or m.get("column_complete") is not True:
            raise Reject("INCOMPLETE_READ", f"member {m['node_id']} is not a verified complete read")
        try:
            rs = parse_ts(m.get("read_started"), "read_started")
            rf = parse_ts(m.get("read_finished"), "read_finished")
        except ApiError as e:
            raise Reject("BAD_READ_TIME", e.message) from e
        if rf < rs:
            raise Reject("BAD_READ_TIME", "read_finished is before read_started")
        if not isinstance(m.get("payload"), dict):
            raise Reject("BAD_PAYLOAD", "payload must be an object")
        out.append({"node_id": m["node_id"], "payload": m["payload"], "evidence": m["evidence"],
                    "context_digest_hex": cm["context_digest_hex"], "read_started": rs, "read_finished": rf,
                    "contract": cm})
    return out


# ---- verification (outside transactions) ------------------------------------------------------

def _int(v: Any, name: str, *, maximum: int = INT63) -> int:
    if isinstance(v, bool):
        raise Reject("BAD_PAYLOAD", f"{name} must be an integer")
    if isinstance(v, int):
        n = v
    elif isinstance(v, str) and v.isdigit() and (v == "0" or not v.startswith("0")):
        n = int(v)
    else:
        raise Reject("BAD_PAYLOAD", f"{name} must be a canonical non-negative integer")
    if not 0 <= n < maximum:
        raise Reject("BAD_PAYLOAD", f"{name} is out of range")
    return n


def _chunk_paths(chunk_dir: Path, manifest: Any, max_total: int | None) -> tuple[list[Path], int, int]:
    if not isinstance(manifest, list):
        raise Reject("BAD_MANIFEST", "chunks_manifest must be a list")
    paths, total, rows = [], 0, 0
    for i, ch in enumerate(manifest, 1):
        if not isinstance(ch, dict) or _int(ch.get("chunk_no"), "chunk_no") != i:
            raise Reject("CHUNK_SEQUENCE", "chunk numbers must be consecutive from 1")
        p = chunk_dir / chunk_name(i)
        if not p.is_file():
            raise Reject("CHUNK_MISSING", f"chunk {i} was not uploaded")
        h = hashlib.sha256()
        size = 0
        lines = 0
        with open(p, "rb") as f:
            for raw in f:
                h.update(raw)
                size += len(raw)
                if raw.strip():
                    lines += 1
        if h.hexdigest() != ch.get("sha256") or size != _int(ch.get("bytes"), "bytes") \
                or lines != _int(ch.get("rows"), "rows"):
            raise Reject("CHUNK_MISMATCH", f"chunk {i} does not match its manifest entry")
        paths.append(p)
        total += size
        rows += lines
        if max_total is not None and total > max_total:
            raise Reject("RESULT_TOO_LARGE", f"result exceeds max_result_bytes={max_total}", "BUDGET_EXCEEDED")
    return paths, total, rows


def _lines(paths: list[Path]):
    for p in paths:
        with open(p, "rb") as f:
            for raw in f:
                if raw.strip():
                    try:
                        line = json.loads(raw)
                    except (ValueError, UnicodeDecodeError) as e:
                        raise Reject("BAD_LINE", f"invalid JSON line in {p.name}") from e
                    if not isinstance(line, dict):
                        raise Reject("BAD_LINE", "material lines must be objects")
                    yield line


def _verify_member(ctx: Ctx, spec: dict, m: dict, chunk_dir: Path) -> Verified:
    kind = spec["check_kind"]
    payload = m["payload"]
    try:
        if kind == "COUNT":
            if set(payload) != {"row_count"} or not isinstance(payload["row_count"], str):
                raise Reject("BAD_PAYLOAD", "COUNT payload is exactly {row_count: string}")
            n = _int(payload["row_count"], "row_count")
            return Verified(m["node_id"], payload, m["evidence"], bytes.fromhex(m["context_digest_hex"]),
                            m["read_started"], m["read_finished"], 0, n, None)
        limits = spec.get("limits") or {}
        max_total = limits.get("max_result_bytes")
        paths, total, _rows = _chunk_paths(chunk_dir, payload.get("chunks_manifest"),
                                           None if max_total is None else int(max_total))
        if _int(payload.get("total_bytes"), "total_bytes") != total:
            raise Reject("BAD_PAYLOAD", "total_bytes does not equal the sum of chunk sizes")
        groups = None
        if kind == "METRIC":
            groups = _verify_metric(spec, payload, paths, limits)
            row_count = None
        elif kind == "SAMPLE":
            row_count = _verify_sample(spec, m, payload, paths, limits)
        else:
            row_count = _verify_deep(spec, m, payload, paths, limits)
        return Verified(m["node_id"], payload, m["evidence"], bytes.fromhex(m["context_digest_hex"]),
                        m["read_started"], m["read_finished"], total, row_count, chunk_dir, groups)
    except Reject:
        raise
    except (ValueError, KeyError, TypeError, IndexError, AttributeError, OverflowError) as e:
        raise Reject("BAD_MATERIAL", f"{type(e).__name__}: {e}") from e


def _verify_metric(spec: dict, payload: dict, paths: list[Path], limits: dict) -> list[dict]:
    rule, cols = spec["rule"], spec["columns"]
    want = {"group_count", "metrics_schema_digest", "chunks_manifest", "total_bytes"}
    if set(payload) != want:
        raise Reject("BAD_PAYLOAD", f"METRIC payload keys must be {sorted(want)}")
    if payload["metrics_schema_digest"] != enc.metrics_schema_digest(rule, cols):
        raise Reject("SCHEMA_DIGEST", "metrics_schema_digest differs from the frozen rule")
    group = rule.get("group_by") or []
    tags = [c.LOGICAL_TAGS[cols[g]["type"]] for g in group]
    metrics = {m["id"]: m for m in rule["metrics"]}
    sums = {mid for mid, m in metrics.items() if m["function"] == "SUM"}
    max_key = int(limits.get("max_group_key_bytes", 512))
    max_groups = int(limits.get("max_groups", 10000))
    out: list[dict] = []
    prev = None
    for line in _lines(paths):
        if set(line) != {"k", "v", "rows", "nn", "m"}:
            raise Reject("BAD_LINE", "METRIC lines have keys k, v, rows, nn, m")
        key = bytes.fromhex(line["k"])
        if prev is not None and key <= prev:
            raise Reject("ORDER", "group keys must be strictly increasing")
        prev = key
        if len(key) > max_key:
            raise Reject("GROUP_KEY_TOO_LARGE", f"group key exceeds {max_key} bytes", "BUDGET_EXCEEDED")
        try:
            vals = c.decode_key(key)
        except c.CanonicalError as e:
            raise Reject("BAD_GROUP_KEY", str(e)) from e
        if [t for t, _ in vals] != tags:
            raise Reject("BAD_GROUP_KEY", "group key tags differ from the group columns")
        disp = line["v"]
        if not isinstance(disp, list) or len(disp) != len(group):
            raise Reject("BAD_GROUP_VALUES", "group values must match group_by")
        for g, (_t, p), shown in zip(group, vals, disp):
            if c.payload_display(cols[g]["type"], p, scale=cols[g].get("scale")) != shown:
                raise Reject("BAD_GROUP_VALUES", f"display value of {g} does not match the canonical key")
        if not isinstance(line["rows"], str):
            raise Reject("BAD_LINE", "rows must be a string")
        rows = _int(line["rows"], "rows")
        if group and rows == 0:
            raise Reject("EMPTY_GROUP", "a real group must have rows > 0")
        nn = line["nn"]
        if not isinstance(nn, dict) or set(nn) != sums:
            raise Reject("BAD_GUARDS", "non-null guards must cover exactly the SUM metrics")
        for mid, v in nn.items():
            if not isinstance(v, str) or _int(v, f"nn.{mid}") > rows:
                raise Reject("BAD_GUARDS", f"guard {mid} must be a count within 0..rows")
        mv = line["m"]
        if not isinstance(mv, dict) or set(mv) != set(metrics):
            raise Reject("BAD_METRICS", "metric values must cover exactly the frozen metrics")
        for mid, v in mv.items():
            _check_metric_value(metrics[mid], v, rows, nn)
        out.append({"key": key, "values": disp, "rows": rows, "nn": nn, "m": mv, "ordinal": len(out)})
        if len(out) > max_groups:
            raise Reject("TOO_MANY_GROUPS", f"more than {max_groups} groups", "BUDGET_EXCEEDED")
    if not group and (len(out) != 1 or out[0]["key"] != c.EMPTY_GROUP_KEY):
        raise Reject("EMPTY_GROUP", "an ungrouped METRIC result has exactly one empty-key group")
    if _int(payload["group_count"], "group_count") != len(out):
        raise Reject("BAD_PAYLOAD", "group_count does not match the material")
    return out


def _check_metric_value(m: dict, v: Any, rows: int, nn: dict) -> None:
    mid = m["id"]
    if not isinstance(v, dict) or set(v) != {"type", "state", "text", "scale"}:
        raise Reject("BAD_METRICS", f"metric {mid} value must have type/state/text/scale")
    fn = m["function"]
    if fn == "COUNT_ROWS":
        if v != {"type": "INTEGER", "state": "FINITE", "text": str(rows), "scale": None}:
            raise Reject("BAD_METRICS", f"COUNT_ROWS {mid} must equal rows")
        return
    if fn == "COUNT_NONNULL":
        if v["type"] != "INTEGER" or v["state"] != "FINITE" or v["scale"] is not None \
                or not isinstance(v["text"], str) or _int(v["text"], mid) > rows:
            raise Reject("BAD_METRICS", f"COUNT_NONNULL {mid} must be a count within 0..rows")
        return
    vt = m["value_type"]
    if v["type"] != vt:
        raise Reject("BAD_METRICS", f"metric {mid} type differs from the frozen rule")
    state, text = v["state"], v["text"]
    if (state == "NULL") != (nn[mid] == "0"):
        raise Reject("BAD_METRICS", f"SUM {mid} is NULL exactly when its non-null guard is 0")
    if vt in ("FLOAT32", "FLOAT64"):
        if state not in ("FINITE", "NULL", "NAN", "POS_INF", "NEG_INF") or v["scale"] is not None:
            raise Reject("BAD_METRICS", f"metric {mid} has an invalid float state")
        if state == "FINITE":
            if not isinstance(text, str) or c.float_state(float(text)) != "FINITE" \
                    or c.float_text(float(text)) != text:
                raise Reject("BAD_METRICS", f"metric {mid} float text is not canonical")
        elif text is not None:
            raise Reject("BAD_METRICS", f"metric {mid} has text for state {state}")
        return
    if state not in ("FINITE", "NULL"):
        raise Reject("BAD_METRICS", f"exact metric {mid} must be FINITE or NULL")
    want_scale = int(m["scale"]) if vt == "DECIMAL" else None
    if v["scale"] != want_scale:
        raise Reject("BAD_METRICS", f"metric {mid} scale differs from the frozen rule")
    if state == "NULL":
        if text is not None:
            raise Reject("BAD_METRICS", f"NULL metric {mid} carries text")
        return
    if not isinstance(text, str):
        raise Reject("BAD_METRICS", f"metric {mid} text must be a string")
    try:
        canon = c.canonical_int_text(text) if vt == "INTEGER" else c.decimal_text(text, want_scale)
    except c.CanonicalError as e:
        raise Reject("BAD_METRICS", f"metric {mid}: {e}") from e
    if canon != text:
        raise Reject("BAD_METRICS", f"metric {mid} text is not canonical")


def _float_text_ok(x: Any) -> bool:
    if x is None:
        return True
    if not isinstance(x, str):
        return False
    try:
        return c.float_text(float(x)) == x
    except ValueError:
        return False


def _key_value(lt: str, payload: bytes):
    return int(payload.decode("ascii")) if lt == "INTEGER" else payload


def _scan_rows(spec: dict, cm: dict, paths: list[Path], limits: dict, *, keyless: bool,
               sample: dict | None = None, key_range: dict | None = None,
               bucket: tuple[int, int] | None = None) -> tuple[int, str, str | None]:
    """Verify every row line; recompute D1 digests. Returns (rows, row_digest_hex, key_digest_hex)."""
    proj, cols = spec["projection"], spec["columns"]
    exact = proj["exact_columns"]
    tolerant = proj.get("tolerant_columns") or []
    keys = proj.get("key_columns") or []
    tags = [c.LOGICAL_TAGS[cols[n]["type"]] for n in exact]
    ctx32 = bytes.fromhex(cm["context_digest_hex"])
    rows_d1 = c.D1(ctx32)
    keys_d1 = None if keyless else c.D1(c.key_projection_context(ctx32))
    key_lt = None if keyless else cols[keys[0]]["type"]
    key_tag = None if keyless else c.LOGICAL_TAGS[key_lt]
    key_idx = exact.index(keys[0]) if keys and keys[0] in exact else None
    max_key = int(limits.get("max_key_bytes", 256))
    max_rows = int(limits.get("max_rows", 2_000_000))
    lo = hi = None
    if key_range is not None:
        lo = None if key_range.get("lower") is None else dsl.sort_key(key_lt, key_range["lower"])
        hi = None if key_range.get("upper") is None else dsl.sort_key(key_lt, key_range["upper"])
    want_keys = {"h", "n", "v"} if keyless else ({"h", "v", "k", "t"} if tolerant else {"h", "v", "k"})
    prev = None
    n_rows = 0
    for line in _lines(paths):
        if set(line) != want_keys:
            raise Reject("BAD_LINE", f"row lines have keys {sorted(want_keys)}")
        vh = line["v"]
        if not isinstance(vh, list) or len(vh) != len(exact):
            raise Reject("BAD_LINE", "v must hold one VAL per exact column")
        vals = [bytes.fromhex(x) for x in vh]
        for vb, tag, name in zip(vals, tags, exact):
            try:
                dec, end = c.decode_vals(vb, 0, 1)
            except c.CanonicalError as e:
                raise Reject("BAD_VAL", f"{name}: {e}") from e
            if end != len(vb) or dec[0][0] != tag:
                raise Reject("BAD_VAL", f"{name} is not a single VAL of its logical type")
        h = c.sha256(c.row_bytes(vals))
        if h.hex() != line["h"]:
            raise Reject("ROW_HASH", "row hash does not match the canonical row")
        if tolerant:
            t = line["t"]
            if not isinstance(t, dict) or set(t) != set(tolerant) or not all(_float_text_ok(x) for x in t.values()):
                raise Reject("BAD_LINE", "t must hold canonical float text for every tolerant column")
        if keyless:
            if prev is not None and line["h"] <= prev:
                raise Reject("ORDER", "keyless lines must be strictly sorted by row hash")
            prev = line["h"]
            cnt = line["n"]
            if not isinstance(cnt, int) or isinstance(cnt, bool) or cnt < 1:
                raise Reject("BAD_LINE", "n must be a positive integer")
            if bucket is not None and int.from_bytes(h[:4], "big") % bucket[0] != bucket[1]:
                raise Reject("BUCKET", "row routed to the wrong hash bucket")
            for _ in range(cnt):
                rows_d1.add(h)
            n_rows += cnt
        else:
            k = bytes.fromhex(line["k"])
            try:
                kv = c.decode_key(k)
            except c.CanonicalError as e:
                raise Reject("BAD_KEY", str(e)) from e
            if len(kv) != 1 or kv[0][0] != key_tag or kv[0][1] is None:
                raise Reject("BAD_KEY", "key must be one non-null VAL of the key type")
            kp = kv[0][1]
            if key_idx is not None and vals[key_idx] != c.val(key_tag, kp):
                raise Reject("BAD_KEY", "key does not match the key column value")
            if key_lt == "STRING" and len(kp) > max_key:
                raise Reject("KEY_TOO_LARGE", f"key exceeds max_key_bytes={max_key}")
            if prev is not None and k <= prev:
                raise Reject("ORDER", "keys must be strictly increasing (duplicate key or unsorted)")
            prev = k
            if sample is not None and not c.s1_selected(k, sample["seed_hex"], int(sample["bucket_count"]),
                                                       int(sample["selected_bucket"])):
                raise Reject("S1_SELECTION", "row is not selected by SRDQ-SAMPLE-1")
            if key_range is not None:
                kval = _key_value(key_lt, kp)
                if (lo is not None and kval < lo) or (hi is not None and kval >= hi):
                    raise Reject("KEY_RANGE", "row key lies outside the member key range")
            if bucket is not None and int.from_bytes(c.sha256(k)[:4], "big") % bucket[0] != bucket[1]:
                raise Reject("BUCKET", "row routed to the wrong hash bucket")
            rows_d1.add(h)
            keys_d1.add(c.sha256(k))
            n_rows += 1
        if n_rows > max_rows:
            raise Reject("TOO_MANY_ROWS", f"more than {max_rows} rows", "BUDGET_EXCEEDED")
    return n_rows, rows_d1.finish().hex(), None if keys_d1 is None else keys_d1.finish().hex()


def _verify_sample(spec: dict, m: dict, payload: dict, paths: list[Path], limits: dict) -> int:
    want = {"sample_rows", "sample_protocol", "rule_digest", "sample_key_digest", "sample_row_digest",
            "projection_digest", "chunks_manifest", "total_bytes"}
    if set(payload) != want:
        raise Reject("BAD_PAYLOAD", f"SAMPLE payload keys must be {sorted(want)}")
    if payload["sample_protocol"] != "SRDQ-SAMPLE-1" or payload["rule_digest"] != spec["rule_digest_hex"]:
        raise Reject("BAD_PAYLOAD", "sample protocol or rule digest differs from the frozen rule")
    if payload["projection_digest"] != enc.projection_digest(spec["projection"], spec["columns"]):
        raise Reject("PROJECTION_DIGEST", "projection digest differs from the frozen projection")
    if not (spec["projection"].get("key_columns") or []):
        raise Reject("BAD_SPEC", "SAMPLE requires a key column")
    n, rd, kd = _scan_rows(spec, m["contract"], paths, limits, keyless=False, sample=spec["sample"])
    if _int(payload["sample_rows"], "sample_rows") != n or payload["sample_row_digest"] != rd \
            or payload["sample_key_digest"] != kd:
        raise Reject("DIGEST_MISMATCH", "sample row count or digests do not match the material")
    return n


def _verify_deep(spec: dict, m: dict, payload: dict, paths: list[Path], limits: dict) -> int:
    want = {"row_count", "digest", "projection_digest", "sort", "keyless", "chunks_manifest", "total_bytes"}
    if set(payload) != want:
        raise Reject("BAD_PAYLOAD", f"DEEP payload keys must be {sorted(want)}")
    keyless = not (spec["projection"].get("key_columns") or [])
    if payload["keyless"] is not keyless or payload["sort"] != ("ROW_HASH" if keyless else "C1_KEY_BYTES"):
        raise Reject("BAD_PAYLOAD", "sort/keyless do not match the frozen projection")
    if payload["projection_digest"] != enc.projection_digest(spec["projection"], spec["columns"]):
        raise Reject("PROJECTION_DIGEST", "projection digest differs from the frozen projection")
    cm = m["contract"]
    atoms = cm["predicate"] if isinstance(cm["predicate"], list) else [cm["predicate"]]
    key_range = next((a for a in atoms if a["kind"] == "KEY_RANGE"), None)
    bucket = None
    if spec["deep"]["mode"] == "HASH_BUCKET":
        bucket = (int(spec["deep"]["buckets"]), int(cm["bucket"]))
    n, rd, _kd = _scan_rows(spec, cm, paths, limits, keyless=keyless, key_range=key_range, bucket=bucket)
    if _int(payload["row_count"], "row_count") != n or payload["digest"] != rd:
        raise Reject("DIGEST_MISMATCH", "row count or digest does not match the material")
    return n


# ---- publication -------------------------------------------------------------------------------

def _hex32(v: Any) -> bytes | None:
    try:
        b = bytes.fromhex(v) if isinstance(v, str) else None
    except ValueError:
        return None
    return b if b is not None and len(b) == 32 else None


def _publish(cur, ctx: Ctx, ctl: dict, env: dict, edig: bytes, a: dict, w: dict,
             verified: list[Verified]) -> tuple[dict, list[tuple[Path, Path]]]:
    kind = w["check_kind"]
    qref = env.get("query_ref") if isinstance(env.get("query_ref"), dict) else None
    tmpl = _hex32(env.get("template_digest"))
    days = int(ctx.cfg["retention"]["raw_value_days"])
    srids, moves = [], []
    for v in verified:
        srid = new_id()
        key = f"{ctl['control_generation']}:{w['id']}:{a['id']}:{a['lease_generation']}:{v.node_id}:{kind}"
        cur.execute("""INSERT INTO dq_side_result (id, attempt_id, work_item_id, observation_id, run_check_id, node_id,
                       side, check_kind, protocol_version, idempotency_key, envelope_digest, context_digest, evidence,
                       payload, result_complete, total_bytes, row_count, validity, read_started, read_finished,
                       query_ref, template_digest, accepted_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s,%s,'ACCEPTED',%s,%s,%s,%s,now())""",
                    (srid, a["id"], w["id"], w["observation_id"], w["run_check_id"], v.node_id, w["side"], kind,
                     env["protocol_version"], key, edig, v.context_digest, v.evidence, jb(v.payload), v.total_bytes,
                     v.row_count, v.read_started, v.read_finished, None if qref is None else jb(qref), tmpl))
        if v.groups is not None:
            _insert_groups(cur, srid, v.groups)
        if v.chunk_dir is not None:
            scope = {"METRIC": "GROUPS", "SAMPLE": "SAMPLE", "DEEP": "FULL"}[kind]
            cur.execute("""INSERT INTO dq_artifact (id, run_id, run_check_id, rule_id, side_result_id, side,
                           material_scope, rel_path, projection_digest, content_digest, size_bytes, row_count,
                           transfer_class, accepted_at, purge_after, detail)
                           SELECT %s, w.run_id, w.run_check_id, o.rule_id, %s, w.side, %s, %s, %s, %s, %s, %s, %s,
                                  now(), now() + make_interval(days => %s), %s
                           FROM dq_work_item w JOIN dq_observation o ON o.id = w.observation_id WHERE w.id=%s""",
                        (new_id(), srid, scope, str(srid), _hex32(v.payload.get("projection_digest")),
                         digest_json(v.payload.get("chunks_manifest")), v.total_bytes,
                         v.row_count if v.groups is None else len(v.groups), TRANSFER_CLASS[kind], days,
                         jb({"chunks": len(v.payload.get("chunks_manifest") or [])}), w["id"]))
            moves.append((v.chunk_dir, material_path(ctx, srid)))
        srids.append(str(srid))
    read_bytes = queue._int_text(env.get("bytes_read"))
    queue.account_usage(cur, a, read_bytes, None)
    entries = budgets.progress_entries(a["progress"])
    newly = budgets.consume(cur, entries, "RESULT_BYTES", sum(v.total_bytes for v in verified))
    budgets.settle(cur, entries)
    prog = budgets.with_entries(a["progress"], entries, result_bytes=sum(v.total_bytes for v in verified))
    cur.execute("""UPDATE dq_attempt SET status='SUCCEEDED', progress=%s, finished_at=now(),
                   duration_ms = (extract(epoch FROM now() - claimed_at) * 1000)::bigint,
                   read_started = %s, read_finished = %s, template_digest = COALESCE(%s, template_digest),
                   query_ref = COALESCE(%s, query_ref) WHERE id=%s""",
                (jb(prog), min(v.read_started for v in verified), max(v.read_finished for v in verified), tmpl,
                 None if qref is None else jb(qref), a["id"]))
    receipt = {"accepted": True, "attempt_id": str(a["id"]), "work_item_id": str(w["id"]),
               "side_result_ids": srids, "envelope_digest": edig.hex()}
    cur.execute("""UPDATE dq_work_item SET status='DONE', updated_at=now(),
                   result = COALESCE(result, '{}'::jsonb) || %s::jsonb WHERE id=%s""",
                (jb({"receipt": receipt, "envelope_digest": edig.hex()}), w["id"]))
    for b in newly:
        queue.budget_exceeded(cur, b)
    return receipt, moves


def _insert_groups(cur, srid, groups: list[dict]) -> None:
    grows, vrows = [], []
    for g in groups:
        gid = new_id()
        grows.append((gid, srid, g["key"], c.sha256(g["key"]), jb(g["values"]), g["rows"], jb(g["nn"]),
                      g["ordinal"]))
        for mid, v in g["m"].items():
            vrows.append((new_id(), srid, gid, mid, v["type"], v["state"], v["text"], v["scale"]))
    cur.executemany("""INSERT INTO dq_metric_group (id, side_result_id, canonical_group_key, group_key_digest,
                       group_values, group_rows, non_null_guards, ordinal) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", grows)
    cur.executemany("""INSERT INTO dq_metric_value (id, side_result_id, group_id, metric_id, value_type, value_state,
                       value_text, scale) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", vrows)


# ---- METADATA results -----------------------------------------------------------------------------

def _accept_metadata(ctx: Ctx, worker: dict, env: dict, aid, wid, lg: int, edig: bytes) -> dict:
    from . import evaluate

    meta = env.get("metadata")
    err = None
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        _generation_ok(ctl, env)
        a, w, _o, err, _ = _gate(cur, ctx, worker, aid, wid, lg, "METADATA")
        if err is None:
            if not isinstance(meta, dict) or not isinstance(meta.get("status"), str):
                _close_attempt(cur, a, "FAILED", "CONTRACT_VIOLATION", "BAD_METADATA", "metadata must be an object")
                queue.fail_work(cur, w["id"], "CONTRACT_VIOLATION", "BAD_METADATA", "metadata must be an object")
                err = ApiError(422, "BAD_METADATA", "metadata must be an object with a status")
            else:
                _close_attempt(cur, a, "SUCCEEDED")
                receipt = {"accepted": True, "attempt_id": str(aid), "work_item_id": str(w["id"]),
                           "side_result_ids": [], "envelope_digest": edig.hex()}
                cur.execute("""UPDATE dq_work_item SET status='DONE', updated_at=now(),
                               result = %s WHERE id=%s""",
                            (jb({"meta": meta, "receipt": receipt, "envelope_digest": edig.hex()}), w["id"]))
        run_id = w["run_id"]
    evaluate.finish_discovery(ctx, run_id)
    if err is not None:
        raise err
    return receipt


# ---- failures and cancellations -------------------------------------------------------------------

def _storage_suspect(ctx: Ctx, message: str) -> bool:
    low = (message or "").lower()
    return any(p.lower() in low for p in ctx.cfg["error_classification"]["storage_suspect_patterns"])


def report_failure(ctx: Ctx, worker: dict, body: dict) -> dict:
    from . import evaluate

    if not isinstance(body, dict):
        raise ApiError(422, "BAD_BODY", "failure body must be an object")
    aid = as_uuid(body.get("attempt_id"), "attempt_id")
    lg = body.get("lease_generation")
    cls = body.get("error_class") if body.get("error_class") in ERROR_CLASSES else "INTERNAL"
    code = body.get("error_code")
    code = None if code is None else str(code)[:200]
    msg = str(body.get("message") or "")[:2000]
    terminated = body.get("terminated") is not False
    usage = body.get("usage") or {}
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("SELECT * FROM dq_attempt WHERE id=%s AND worker_id=%s FOR UPDATE", (aid, worker["id"]))
        a = cur.fetchone()
        if a is None:
            raise ApiError(404, "UNKNOWN_ATTEMPT", "attempt not found for this worker")
        if a["lease_generation"] != lg:
            raise ApiError(409, "LEASE_MISMATCH", "lease generation does not match")
        if a["status"] not in LIVE + CANCELLING:
            return {"ignored": True, "status": a["status"]}
        cur.execute("SELECT * FROM dq_work_item WHERE id=%s FOR UPDATE", (a["work_item_id"],))
        w = cur.fetchone()
        queue.account_usage(cur, a, queue._int_text(usage.get("READ_BYTES")), None)
        if cls == "QUERY_ERROR" and _storage_suspect(ctx, msg):
            cls = "STORAGE_SUSPECT"
            incident(cur, "STORAGE_SUSPECT", f"{w['run_check_id'] or w['id']}",
                     f"possible storage fault on {a['side']}: {msg[:300]}", side=a["side"], severity="CRITICAL",
                     run_id=w["run_id"], run_check_id=w["run_check_id"], asset_id=w["asset_id"],
                     check_kind=w["check_kind"], error_code=code)
        if a["status"] in CANCELLING and a["cancel_reason"] != "FAILED_UNCONFIRMED" and terminated:
            # The failure is the executor's way of reporting the cancelled query ended.
            finish_cancelled(cur, ctx, a, w)
        elif terminated:
            _close_attempt(cur, a, "FAILED", cls, code, msg)
            if _work_current(w, a):
                if w["kind"] == "METADATA":
                    _metadata_retry(cur, ctx, w, cls, code, msg)
                else:
                    queue.fail_work(cur, w["id"], cls, code, msg)
        else:
            cur.execute("""UPDATE dq_attempt SET status='CANCEL_PENDING', cancel_reason='FAILED_UNCONFIRMED',
                           error_class=%s, error_code=%s, error_message=%s WHERE id=%s""", (cls, code, msg, aid))
            if _work_current(w, a):
                cur.execute("""UPDATE dq_work_item SET status='CANCEL_PENDING', updated_at=now(),
                               result = COALESCE(result, '{}'::jsonb) || jsonb_build_object('failure', %s::jsonb)
                               WHERE id=%s""", (jb({"error_class": cls, "error_code": code, "message": msg}),
                                                w["id"]))
        oid, run_id, wkind = w["observation_id"], w["run_id"], w["kind"]
    if wkind == "METADATA":
        evaluate.finish_discovery(ctx, run_id)
    elif oid is not None:
        evaluate.evaluate_observation(ctx, oid)
    return {"recorded": True, "error_class": cls, "terminated": terminated}


def report_cancelled(ctx: Ctx, worker: dict, body: dict) -> dict:
    from . import evaluate

    if not isinstance(body, dict):
        raise ApiError(422, "BAD_BODY", "body must be an object")
    aid = as_uuid(body.get("attempt_id"), "attempt_id")
    if body.get("confirmed") is not True:
        raise ApiError(422, "NOT_CONFIRMED", "only confirmed terminations are accepted")
    qref = body.get("query_ref")
    with ctx.db.tx() as cur:
        lock_control(cur)
        cur.execute("SELECT * FROM dq_attempt WHERE id=%s AND worker_id=%s FOR UPDATE", (aid, worker["id"]))
        a = cur.fetchone()
        if a is None:
            raise ApiError(404, "UNKNOWN_ATTEMPT", "attempt not found for this worker")
        if a["status"] not in LIVE + CANCELLING:
            raise ApiError(409, "ATTEMPT_NOT_LIVE", f"attempt is {a['status']}")
        if isinstance(qref, dict):
            cur.execute("UPDATE dq_attempt SET query_ref=COALESCE(query_ref, %s) WHERE id=%s", (jb(qref), aid))
        if a["status"] in LIVE:
            cur.execute("UPDATE dq_attempt SET cancel_reason=COALESCE(cancel_reason, 'EXECUTOR_CANCELLED') "
                        "WHERE id=%s RETURNING cancel_reason", (aid,))
            a["cancel_reason"] = cur.fetchone()["cancel_reason"]
        cur.execute("SELECT * FROM dq_work_item WHERE id=%s FOR UPDATE", (a["work_item_id"],))
        w = cur.fetchone()
        finish_cancelled(cur, ctx, a, w)
        oid, run_id, wkind = w["observation_id"], w["run_id"], w["kind"]
    if wkind == "METADATA":
        evaluate.finish_discovery(ctx, run_id)
    elif oid is not None:
        evaluate.evaluate_observation(ctx, oid)
    return {"recorded": True}
