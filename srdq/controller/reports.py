"""Daily and stage reports (doc 03 §11.5): report.md plus UTF-8-BOM CSVs, registered as REPORT artifacts."""
from __future__ import annotations

import csv
import datetime as _dt
import hashlib
import io
import math
import shutil
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from ..db.db import jb, new_id
from . import differences
from .common import KIND_RANK, LAYER_RANK, SIDES, ApiError, Ctx, as_uuid, audit, control, iso, lock_control

FILES = ("report.md", "progress.csv", "differences.csv", "incomplete.csv", "load.csv")
KINDS = ("COUNT", "METRIC", "SAMPLE", "DEEP")
STATE_ZH = {"NEW": "新出现", "PERSISTENT": "持续", "RESOLVED": "已消失"}
KIND_ZH = {"COUNT": "数量", "METRIC": "指标", "SAMPLE": "样本", "DEEP": "局部诊断"}
LOAD_EVENTS = ("LOAD_DEGRADE", "LOAD_RECOVER", "CLUSTER_HARD_STOP", "CLUSTER_RESUMED")
INCOMPLETE_STATES = ("ERROR", "INCONCLUSIVE", "UNSUPPORTED", "BLOCKED")


# ---- triggering ----------------------------------------------------------------------------------

def report_dir(ctx: Ctx) -> Path:
    return Path(ctx.cfg["report"]["dir"])


def last_daily_end(cur) -> _dt.datetime | None:
    cur.execute("""SELECT max((detail->>'interval_end')::timestamptz) AS t FROM dq_artifact
                   WHERE material_scope='REPORT' AND detail->>'kind'='DAILY'""")
    return cur.fetchone()["t"]


def maybe_generate(ctx: Ctx) -> dict | None:
    """Generate the daily report once per window: after CLOSED and all cancellations confirmed (or grace)."""
    rc = ctx.cfg["report"]
    if not rc["daily"]:
        return None
    ctl = control(ctx)
    key = ctl["window_key"]
    if ctl["window_state"] != "CLOSED" or key is None or ctl["last_report_window_key"] == key \
            or ctl["window_opened_at"] is None or ctl["window_closed_at"] is None:
        return None
    pending = ctx.db.one("SELECT count(*) AS n FROM dq_attempt WHERE status IN ('CANCEL_REQUESTED','CANCEL_PENDING')")
    grace_end = ctl["window_closed_at"] + _dt.timedelta(minutes=float(rc["grace_minutes"]))
    if pending["n"] and ctx.now() < grace_end:
        return None
    return generate(ctx, "DAILY", window_key=key)


# ---- generation ----------------------------------------------------------------------------------

def generate(ctx: Ctx, kind: str, *, window_key: str | None = None, start: _dt.datetime | None = None,
             end: _dt.datetime | None = None, reason: str | None = None) -> dict:
    if kind not in ("DAILY", "STAGE"):
        raise ApiError(422, "BAD_REPORT_KIND", "kind must be DAILY or STAGE")
    end = end or ctx.now()
    with ctx.db.tx() as cur:
        ctl = control_row(cur)
        if kind == "DAILY":
            window_key = window_key or ctl["window_key"]
            if start is None:
                start = last_daily_end(cur) or ctl["window_opened_at"] or (end - _dt.timedelta(days=1))
            start = min(start, end)  # a zero-length window still gets its (empty) report, never a failing tick
        elif start is None or start >= end:
            raise ApiError(422, "BAD_INTERVAL", "report interval is empty")
        data = collect(cur, ctx, start, end, ctl)
    rid = new_id()
    meta = {"report_id": str(rid), "kind": kind, "window_key": window_key, "interval_start": iso(start),
            "interval_end": iso(end), "generated_at": iso(ctx.now())}
    out_dir = report_dir(ctx) / str(rid)
    out_dir.mkdir(parents=True, exist_ok=True)
    files = _write(ctx, out_dir, meta, data)
    with ctx.db.tx() as cur:
        ctl = lock_control(cur)
        if kind == "DAILY" and reason is None and ctl["last_report_window_key"] == window_key:
            shutil.rmtree(out_dir, ignore_errors=True)
            return {"skipped": True, "window_key": window_key}
        for name, (digest, size, rows) in files.items():
            cur.execute("""INSERT INTO dq_artifact (id, material_scope, rel_path, content_digest, size_bytes, row_count,
                           detail) VALUES (%s,'REPORT',%s,%s,%s,%s,%s)""",
                        (new_id(), f"{rid}/{name}", digest, size, rows, jb(dict(meta, file=name))))
        if kind == "DAILY":
            cur.execute("UPDATE dq_control_state SET last_report_window_key=%s, revision=revision+1, updated_at=now() "
                        "WHERE id=1", (window_key,))
        audit(cur, "REPORT_GENERATED", "report", rid, dict(meta, files=sorted(files)), reason,
              actor="controller" if reason is None else "local-user")
    return dict(meta, files=sorted(files), summary=data["headline"])


def control_row(cur) -> dict:
    cur.execute("SELECT * FROM dq_control_state WHERE id=1")
    return cur.fetchone()


def generate_stage(ctx: Ctx, start_date: str, end_date: str, reason: str | None) -> dict:
    """Stage report over local dates [start_date, end_date] in the execution-window timezone."""
    if not reason or not str(reason).strip():
        raise ApiError(422, "REASON_REQUIRED", "a reason is required")
    tz = ZoneInfo(ctx.cfg["execution_window"]["timezone"])
    try:
        a, b = _dt.date.fromisoformat(str(start_date)), _dt.date.fromisoformat(str(end_date))
    except ValueError as e:
        raise ApiError(422, "BAD_DATE", "dates must be YYYY-MM-DD") from e
    if b < a or (b - a).days > 366:
        raise ApiError(422, "BAD_INTERVAL", "end_date must be on or after start_date and within 366 days")
    start = _dt.datetime(a.year, a.month, a.day, tzinfo=tz).astimezone(_dt.timezone.utc)
    end = _dt.datetime(b.year, b.month, b.day, tzinfo=tz).astimezone(_dt.timezone.utc) + _dt.timedelta(days=1)
    return generate(ctx, "STAGE", start=start, end=min(end, ctx.now()), reason=str(reason).strip())


def regenerate_daily(ctx: Ctx, window_key: str, reason: str | None) -> dict:
    """Manual re-generation of a daily report: a new version; the old files stay."""
    if not reason or not str(reason).strip():
        raise ApiError(422, "REASON_REQUIRED", "a reason is required")
    old = ctx.db.one("""SELECT detail FROM dq_artifact WHERE material_scope='REPORT' AND detail->>'kind'='DAILY'
                        AND detail->>'window_key'=%s ORDER BY created_at DESC LIMIT 1""", (window_key,))
    if old is None:
        raise ApiError(404, "NOT_FOUND", "no daily report for this window")
    d = old["detail"]
    return generate(ctx, "DAILY", window_key=window_key, start=_parse(d["interval_start"]),
                    end=_parse(d["interval_end"]), reason=str(reason).strip())


def _parse(s: str) -> _dt.datetime:
    return _dt.datetime.fromisoformat(s)


# ---- data collection -----------------------------------------------------------------------------

def collect(cur, ctx: Ctx, start: _dt.datetime, end: _dt.datetime, ctl: dict) -> dict:
    iv = {"s": start, "e": end}
    data: dict[str, Any] = {"start": start, "end": end}
    cur.execute("""SELECT check_kind, verdict, count(*) AS n FROM dq_comparison
                   WHERE compared_at > %(s)s AND compared_at <= %(e)s GROUP BY 1, 2""", iv)
    tonight: dict = {}
    for r in cur.fetchall():
        tonight.setdefault(r["check_kind"], {})[r["verdict"]] = r["n"]
    data["tonight"] = tonight
    cur.execute("""SELECT r.id, r.purpose, r.execution_state, r.control_state, r.windows_used, r.created_at,
                          r.completed_at, p.name AS plan_name, s.name AS schedule_name
                   FROM dq_run r LEFT JOIN dq_plan p ON p.id = r.plan_id LEFT JOIN dq_schedule s ON s.id = r.schedule_id
                   WHERE r.purpose <> 'METADATA_PREVIEW' AND r.created_at <= %(e)s
                     AND (r.execution_state NOT IN ('COMPLETED','FAILED','CANCELLED') OR r.completed_at > %(s)s)
                   ORDER BY r.created_at""", iv)
    runs = cur.fetchall()
    run_ids = [r["id"] for r in runs]
    cur.execute("""SELECT run_id, check_kind, count(*) AS total,
                          count(*) FILTER (WHERE coverage_state <> 'PENDING') AS done,
                          count(*) FILTER (WHERE coverage_state <> 'PENDING' AND coverage_updated_at > %(s)s
                                           AND coverage_updated_at <= %(e)s) AS tonight,
                          count(*) FILTER (WHERE coverage_state = 'COMPLETED_EQUAL') AS equal,
                          count(*) FILTER (WHERE coverage_state = 'COMPLETED_DIFFERENT') AS different
                   FROM dq_run_check WHERE run_id = ANY(%(ids)s) GROUP BY 1, 2""", dict(iv, ids=run_ids))
    per: dict = {}
    for r in cur.fetchall():
        per.setdefault(r["run_id"], {})[r["check_kind"]] = r
    progress = []
    for run in runs:
        for k in sorted(per.get(run["id"], {}), key=KIND_RANK.get):
            p = per[run["id"]][k]
            remaining = int(p["total"]) - int(p["done"])
            nights = None if remaining == 0 else (math.ceil(remaining / int(p["tonight"])) if p["tonight"] else None)
            progress.append({"run_id": str(run["id"]), "plan": run["plan_name"] or "", "schedule":
                             run["schedule_name"] or "", "purpose": run["purpose"],
                             "execution_state": run["execution_state"], "control_state": run["control_state"],
                             "check_kind": k, "total": int(p["total"]), "done": int(p["done"]),
                             "tonight": int(p["tonight"]), "equal": int(p["equal"]),
                             "different": int(p["different"]), "remaining": remaining,
                             "windows_used": run["windows_used"], "nights_remaining": nights,
                             "started_at": iso(run["created_at"]), "completed_at": iso(run["completed_at"])})
    data["progress"] = progress
    cur.execute("""WITH last AS (SELECT c.asset_id, c.scope_key, c.rule_key, c.check_kind, max(c.compared_at) AS t
                                 FROM dq_comparison c WHERE c.verdict IN ('OBSERVED_EQUAL','OBSERVED_DIFFERENT')
                                   AND c.compared_at <= %(e)s GROUP BY 1, 2, 3, 4),
                        ranked AS (SELECT last.*, row_number() OVER (PARTITION BY check_kind ORDER BY t) AS rn
                                   FROM last)
                   SELECT ranked.*, a.source_db, a.source_table FROM ranked JOIN dq_asset a ON a.id = ranked.asset_id
                   WHERE rn <= 5 ORDER BY check_kind, t""", iv)
    data["oldest"] = cur.fetchall()
    cur.execute("""SELECT rc.check_kind, count(DISTINCT (rc.asset_id, rc.scope_key, rc.rule_key)) AS n
                   FROM dq_run_check rc WHERE rc.run_id = ANY(%(ids)s) AND rc.coverage_state = 'PENDING'
                     AND NOT EXISTS (SELECT 1 FROM dq_comparison c WHERE c.asset_id = rc.asset_id
                                     AND c.scope_key = rc.scope_key AND c.rule_key = rc.rule_key
                                     AND c.verdict IN ('OBSERVED_EQUAL','OBSERVED_DIFFERENT'))
                   GROUP BY 1""", dict(iv, ids=run_ids))
    data["never_verified"] = {r["check_kind"]: r["n"] for r in cur.fetchall()}
    _, data["chains"] = differences.chains(cur, as_of=end, resolved_since=start, limit=1_000_000)
    data["incomplete"] = _incomplete(cur, run_ids, iv)
    data["storage_suspect"] = _storage_suspect(cur, iv)
    data["load"] = _load(cur, iv)
    data["statements"] = _statements(cur, iv)
    data["unknown_periods"] = [p for p in (ctl["unknown_periods"] or [])
                               if (p.get("to") or "") >= iso(start) and (p.get("from") or "") <= iso(end)]
    cur.execute("""SELECT a.id, a.side, a.status, a.cancel_reason, a.lease_expires_at, w.asset_id, w.check_kind,
                          ast.source_db, ast.source_table, a.query_ref
                   FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id JOIN dq_asset ast ON ast.id = w.asset_id
                   WHERE a.status IN ('CANCEL_REQUESTED','CANCEL_PENDING') ORDER BY a.side, a.claimed_at""")
    data["unconfirmed_cancels"] = cur.fetchall()
    counts: dict = {}
    for ch in data["chains"]:
        counts[ch["state"]] = counts.get(ch["state"], 0) + 1
    data["headline"] = {"tonight": tonight, "chains": counts, "incomplete": len(data["incomplete"]),
                        "storage_suspect": len(data["storage_suspect"]),
                        "unconfirmed_cancels": len(data["unconfirmed_cancels"]),
                        "unknown_periods": len(data["unknown_periods"])}
    return data


def _incomplete(cur, run_ids: list, iv: dict) -> list[dict]:
    cur.execute("""SELECT rc.run_id, rc.check_kind, rc.rule_key, rc.rule_revision, rc.scope_key, rc.coverage_state,
                          rc.coverage_reason, rc.issue, rc.execution_snapshot->>'scope_label' AS label,
                          a.layer, a.source_db, a.source_table
                   FROM dq_run_check rc JOIN dq_asset a ON a.id = rc.asset_id
                   WHERE rc.run_id = ANY(%(ids)s) AND rc.coverage_state = ANY(%(st)s)
                   ORDER BY rc.check_kind, a.layer, a.source_db, a.source_table, rc.scope_key""",
                dict(iv, ids=run_ids, st=list(INCOMPLETE_STATES)))
    out = []
    for r in cur.fetchall():
        reason = r["coverage_reason"] or ""
        err = ((r["issue"] or {}).get("last_error") or {})
        cat = r["coverage_state"]
        if err.get("error_class") == "STORAGE_SUSPECT" and cat == "ERROR":
            cat = "STORAGE_SUSPECT"
        elif "BUDGET_EXCEEDED" in reason:
            cat = "BUDGET_EXCEEDED"
        elif "INSUFFICIENT_SAMPLE" in reason:
            cat = "INSUFFICIENT_SAMPLE"
        out.append({"category": cat, "check_kind": r["check_kind"], "layer": r["layer"],
                    "table": f"{r['source_db']}.{r['source_table']}", "scope": r["label"] or r["scope_key"],
                    "rule": f"{r['rule_key']}@{r['rule_revision']}", "reason": reason,
                    "side": err.get("side") or "", "error_code": err.get("error_code") or "",
                    "run_id": str(r["run_id"])})
    cur.execute("""SELECT o.abandon_reason, o.check_kind, a.layer, a.source_db, a.source_table,
                          rc.scope_key, rc.rule_key, rc.rule_revision, rc.execution_snapshot->>'scope_label' AS label,
                          o.run_id
                   FROM dq_observation o JOIN dq_run_check rc ON rc.id = o.run_check_id
                   JOIN dq_asset a ON a.id = o.asset_id
                   WHERE o.status='ABANDONED' AND o.finished_at > %(s)s AND o.finished_at <= %(e)s
                   ORDER BY o.check_kind, a.layer, a.source_db, a.source_table""", iv)
    for r in cur.fetchall():
        out.append({"category": "ABANDONED", "check_kind": r["check_kind"], "layer": r["layer"],
                    "table": f"{r['source_db']}.{r['source_table']}", "scope": r["label"] or r["scope_key"],
                    "rule": f"{r['rule_key']}@{r['rule_revision']}", "reason": r["abandon_reason"] or "",
                    "side": "", "error_code": "", "run_id": str(r["run_id"])})
    return out


def _storage_suspect(cur, iv: dict) -> list[dict]:
    cur.execute("""SELECT a.side, ast.source_db, ast.source_table, ast.target_db, ast.target_table,
                          COALESCE(rc.execution_snapshot->>'scope_label', rc.scope_key, '') AS scope,
                          COALESCE(a.error_code, '') AS error_code, min(a.finished_at) AS first_seen,
                          max(a.finished_at) AS last_seen, count(*) AS n
                   FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                   JOIN dq_asset ast ON ast.id = w.asset_id LEFT JOIN dq_run_check rc ON rc.id = w.run_check_id
                   WHERE a.error_class='STORAGE_SUSPECT' AND a.finished_at > %(s)s AND a.finished_at <= %(e)s
                   GROUP BY 1, 2, 3, 4, 5, 6, 7 ORDER BY 1, 2, 3, 6""", iv)
    return cur.fetchall()


def _load(cur, iv: dict) -> dict:
    cur.execute("""SELECT event_type, object_id AS side, payload, created_at FROM dq_outbox
                   WHERE event_type = ANY(%(ev)s) AND created_at > %(s)s AND created_at <= %(e)s
                   ORDER BY created_at""", dict(iv, ev=list(LOAD_EVENTS)))
    events = cur.fetchall()
    cur.execute("""SELECT a.side, w.check_kind, count(*) AS n,
                          percentile_cont(0.5) WITHIN GROUP (ORDER BY a.duration_ms) AS p50,
                          percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms) AS p95,
                          max(a.duration_ms) AS mx,
                          count(*) FILTER (WHERE a.error_class='TIMEOUT') AS timeouts
                   FROM dq_attempt a JOIN dq_work_item w ON w.id = a.work_item_id
                   WHERE w.kind='CHECK' AND a.duration_ms IS NOT NULL AND a.finished_at > %(s)s
                     AND a.finished_at <= %(e)s
                   GROUP BY ROLLUP (1, 2) HAVING a.side IS NOT NULL ORDER BY 1, 2 NULLS FIRST""", iv)
    durations = cur.fetchall()
    cur.execute("""SELECT side, count(*) AS n FROM dq_attempt
                   WHERE cancel_reason='WINDOW_CLOSED' AND claimed_at <= %(e)s
                     AND (finished_at IS NULL OR finished_at > %(s)s) GROUP BY 1""", iv)
    deadline = {r["side"]: r["n"] for r in cur.fetchall()}
    per_side = {}
    for side in SIDES:
        ev = [e for e in events if e["side"] == side]
        per_side[side] = {"degrade": sum(e["event_type"] == "LOAD_DEGRADE" for e in ev),
                          "recover": sum(e["event_type"] == "LOAD_RECOVER" for e in ev),
                          "hard_stop": sum(e["event_type"] == "CLUSTER_HARD_STOP" for e in ev),
                          "resumed": sum(e["event_type"] == "CLUSTER_RESUMED" for e in ev),
                          "deadline_cancels": int(deadline.get(side, 0))}
    return {"events": events, "durations": durations, "per_side": per_side}


def _columns(kind: str, d: dict) -> str:
    if kind == "COUNT":
        return "行数"
    if kind == "METRIC":
        cols = [m.get("column") for m in d.get("metrics", []) if m.get("column")]
        g = d.get("group_by") or []
        return f"分组 {', '.join(g) or '（无）'}；度量 {', '.join(cols) or '仅行数'}"
    cols = d.get("compare_columns") or d.get("columns") or []
    return ", ".join(cols) if cols else "（规则定义）"


def _statements(cur, iv: dict) -> dict:
    cur.execute("""SELECT c.check_kind, c.evidence, c.rule_key, c.rule_revision, count(*) AS n,
                          min(LEAST(c.source_read_started, c.target_read_started)) AS read_from,
                          max(GREATEST(c.source_read_finished, c.target_read_finished)) AS read_to,
                          min(c.source_read_started) AS s_from, max(c.source_read_finished) AS s_to,
                          min(c.target_read_started) AS t_from, max(c.target_read_finished) AS t_to,
                          (array_agg(c.rule_id))[1] AS rule_id
                   FROM dq_comparison c WHERE c.compared_at > %(s)s AND c.compared_at <= %(e)s
                   GROUP BY 1, 2, 3, 4 ORDER BY 1, 3, 4""", iv)
    rules = cur.fetchall()
    defs = {}
    if rules:
        cur.execute("SELECT id, definition FROM dq_check_rule WHERE id = ANY(%s)", ([r["rule_id"] for r in rules],))
        defs = {r["id"]: r["definition"] for r in cur.fetchall()}
    rows = [{"check_kind": r["check_kind"], "evidence": r["evidence"],
             "rule": f"{r['rule_key']}@{r['rule_revision']}", "comparisons": r["n"],
             "columns": _columns(r["check_kind"], defs.get(r["rule_id"]) or {}),
             "source_read": f"{iso(r['s_from'])} ~ {iso(r['s_to'])}",
             "target_read": f"{iso(r['t_from'])} ~ {iso(r['t_to'])}"} for r in rules]
    return {"rules": rows, "evidence": sorted({r["evidence"] for r in rules})}


# ---- rendering -----------------------------------------------------------------------------------

def csv_cell(v: Any) -> Any:
    """Neutralize spreadsheet formulas in text cells (OWASP CSV injection)."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return v
    s = str(v)
    if s and s[0] in ("=", "+", "-", "@", "\t", "\r", "|", "%"):
        return "'" + s
    return s


def _csv(path: Path, header: list[str], rows: list[list[Any]]) -> tuple[bytes, int, int]:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\r\n")
    w.writerow(header)
    for r in rows:
        w.writerow([csv_cell(x) for x in r])
    data = ("\ufeff" + buf.getvalue()).encode("utf-8")
    path.write_bytes(data)
    return hashlib.sha256(data).digest(), len(data), len(rows)


def md_cell(v: Any) -> str:
    s = "" if v is None else str(v)
    return s.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").replace("\r", " ").replace("<", "&lt;")


def _table(header: list[str], rows: list[list[Any]]) -> list[str]:
    out = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    out += ["| " + " | ".join(md_cell(x) for x in r) + " |" for r in rows]
    return out


def _local(ctx: Ctx, t: Any) -> str:
    if t is None:
        return ""
    if isinstance(t, str):
        t = _dt.datetime.fromisoformat(t)
    return t.astimezone(ZoneInfo(ctx.cfg["execution_window"]["timezone"])).strftime("%Y-%m-%d %H:%M:%S")


def _write(ctx: Ctx, out_dir: Path, meta: dict, d: dict) -> dict[str, tuple[bytes, int, int]]:
    top = int(ctx.cfg["report"]["top_n_in_markdown"])
    formats = ctx.cfg["report"]["formats"]
    files: dict = {}
    chains = d["chains"]
    chains_sorted = sorted(chains, key=lambda c: (KIND_RANK[c["check_kind"]], LAYER_RANK[c["layer"]],
                                                  c["source_table"], c["scope_key"], c["rule_key"]))
    if "CSV_UTF8_BOM" in formats:
        files["progress.csv"] = _csv(out_dir / "progress.csv",
                                     ["run_id", "plan", "schedule", "purpose", "execution_state", "control_state",
                                      "check_kind", "total", "done", "tonight", "equal", "different", "remaining",
                                      "windows_used", "nights_remaining", "started_at", "completed_at"],
                                     [[p[k] for k in ("run_id", "plan", "schedule", "purpose", "execution_state",
                                                      "control_state", "check_kind", "total", "done", "tonight",
                                                      "equal", "different", "remaining", "windows_used")]
                                      + ["" if p["nights_remaining"] is None else p["nights_remaining"],
                                         p["started_at"], p["completed_at"] or ""] for p in d["progress"]])
        files["differences.csv"] = _csv(out_dir / "differences.csv",
                                        ["state", "check_kind", "layer", "source_table", "target_table", "scope",
                                         "rule", "evidence", "consecutive_different", "first_seen", "latest_at",
                                         "difference_count", "difference_count_mode", "summary", "comparison_id"],
                                        [[c["state"], c["check_kind"], c["layer"], c["source_table"],
                                          c["target_table"], c["scope_label"], f"{c['rule_key']}@{c['rule_revision']}",
                                          c["evidence"], c["consecutive_different"], c["first_seen"], c["latest_at"],
                                          c["difference_count"], c["difference_count_mode"], c["summary"],
                                          c["comparison_id"]] for c in chains_sorted])
        files["incomplete.csv"] = _csv(out_dir / "incomplete.csv",
                                       ["category", "check_kind", "layer", "table", "scope", "rule", "reason", "side",
                                        "error_code", "run_id"],
                                       [[x[k] for k in ("category", "check_kind", "layer", "table", "scope", "rule",
                                                        "reason", "side", "error_code", "run_id")]
                                        for x in d["incomplete"]]
                                       + [["STORAGE_SUSPECT_ATTEMPTS", "", "", f"{s['source_db']}.{s['source_table']}",
                                           s["scope"], "", f"count={s['n']}; first={iso(s['first_seen'])}; "
                                           f"last={iso(s['last_seen'])}", s["side"], s["error_code"], ""]
                                          for s in d["storage_suspect"]])
        load_rows = [["event", e["side"], e["event_type"], iso(e["created_at"]), _event_text(e), "", "", "", ""]
                     for e in d["load"]["events"]]
        load_rows += [["duration", r["side"], r["check_kind"] or "ALL", "", "", r["n"], _ms(r["p50"]), _ms(r["p95"]),
                       _ms(r["mx"])] for r in d["load"]["durations"]]
        load_rows += [["unconfirmed_cancel", a["side"], a["status"], iso(a["lease_expires_at"]),
                       f"{a['source_db']}.{a['source_table']} {a['check_kind'] or 'METADATA'} {a['cancel_reason']}",
                       "", "", "", ""] for a in d["unconfirmed_cancels"]]
        files["load.csv"] = _csv(out_dir / "load.csv", ["type", "side", "name", "at", "detail", "queries", "p50_ms",
                                                         "p95_ms", "max_ms"], load_rows)
    if "MARKDOWN" in formats:
        text = "\n".join(_markdown(ctx, meta, d, chains_sorted, top)) + "\n"
        data = text.encode("utf-8")
        (out_dir / "report.md").write_bytes(data)
        files["report.md"] = (hashlib.sha256(data).digest(), len(data), None)
    return files


def _ms(v: Any) -> Any:
    return "" if v is None else int(round(float(v)))


def _event_text(e: dict) -> str:
    p = e["payload"] or {}
    if e["event_type"] in ("LOAD_DEGRADE", "LOAD_RECOVER"):
        return f"并发 {p.get('from')} -> {p.get('to')} {', '.join(p.get('triggers') or [])}".strip()
    if e["event_type"] == "CLUSTER_HARD_STOP":
        return f"连续 {p.get('consecutive_timeouts')} 次超时，停止领取"
    return f"人工恢复，并发 {p.get('concurrency')}"


def _markdown(ctx: Ctx, meta: dict, d: dict, chains: list[dict], top: int) -> list[str]:
    title = "每日校验报告" if meta["kind"] == "DAILY" else "阶段校验报告"
    L = [f"# {title}", ""]
    L += [f"- 报告 ID：{meta['report_id']}", f"- 统计区间：{_local(ctx, d['start'])} ~ {_local(ctx, d['end'])}"
          f"（{ctx.cfg['execution_window']['timezone']}）"]
    if meta.get("window_key"):
        L.append(f"- 执行窗口：{meta['window_key']}")
    L.append(f"- 生成时间：{_local(ctx, meta['generated_at'])}")
    if d["unknown_periods"]:
        for p in d["unknown_periods"]:
            L.append(f"- **UNKNOWN 时间段**：{_local(ctx, p.get('from'))} ~ {_local(ctx, p.get('to'))}"
                     f"（{md_cell(p.get('reason'))}）。该时段的执行结果未被采纳，相关范围按未完成处理。")
    if d["unconfirmed_cancels"]:
        L.append(f"- **注意**：生成时仍有 {len(d['unconfirmed_cancels'])} 个查询未确认终止（见“负载与熔断”）。")
    L += ["", "## 1. 进度与轮巡周期", ""]
    tn = d["tonight"]
    L += _table(["检查类型", "本区间完成", "数量/指标/样本相同", "观察到差异", "不可判定", "错误", "不支持"],
                [[f"{k}（{KIND_ZH[k]}）", sum(tn.get(k, {}).values()), tn.get(k, {}).get("OBSERVED_EQUAL", 0),
                  tn.get(k, {}).get("OBSERVED_DIFFERENT", 0), tn.get(k, {}).get("INCONCLUSIVE", 0),
                  tn.get(k, {}).get("ERROR", 0), tn.get(k, {}).get("UNSUPPORTED", 0)] for k in KINDS if k in tn]
                or [["（本区间没有完成的检查）", "", "", "", "", "", ""]])
    L += ["", "本轮累计进度（未完成的轮次，以及本区间内结束的轮次）：", ""]
    L += _table(["run", "计划 / 周期", "状态", "类型", "已完成/总数", "本区间完成", "已用窗口", "按本区间速度还需"],
                [[p["run_id"][:8], f"{p['plan']} / {p['schedule'] or '-'}", f"{p['execution_state']}"
                  f"{'' if p['control_state'] == 'ENABLED' else ' (' + p['control_state'] + ')'}", p["check_kind"],
                  f"{p['done']}/{p['total']}", p["tonight"], p["windows_used"],
                  "已完成" if p["remaining"] == 0 else ("无法推算" if p["nights_remaining"] is None
                                                      else f"约 {p['nights_remaining']} 夜")]
                 for p in d["progress"][:top]] or [["（无）", "", "", "", "", "", "", ""]])
    if len(d["progress"]) > top:
        L.append(f"\n共 {len(d['progress'])} 行，此处列出前 {top} 行，完整内容见 progress.csv。")
    L += ["", "最久未验证的范围（按检查类型，最近一次有完整结论的时间）：", ""]
    L += _table(["检查类型", "表", "范围", "规则", "最近一次完整结论"],
                [[o["check_kind"], f"{o['source_db']}.{o['source_table']}", o["scope_key"], o["rule_key"],
                  _local(ctx, o["t"])] for o in d["oldest"]] or [["（无）", "", "", "", ""]])
    if d["never_verified"]:
        L.append("")
        L.append("尚未有过完整结论的范围：" + "，".join(f"{k} {n} 个" for k, n in
                                                 sorted(d["never_verified"].items(), key=lambda x: KIND_RANK[x[0]])))
    L += ["", "## 2. 差异清单", "",
          "新出现：最近一次观察到差异、上一次相同或没有上一次；持续：最近两次都观察到差异；"
          "已消失：最近一次相同、上一次观察到差异（只在本报告列出）。两端都在线写入、没有共同快照，"
          "差异可能来自在线写入时差，单次差异不代表数据错误。", ""]
    counts = {s: sum(1 for c in chains if c["state"] == s) for s in differences.STATES}
    L.append("合计：" + "，".join(f"{STATE_ZH[s]} {counts[s]}" for s in differences.STATES))
    for k in KINDS:
        ks = [c for c in chains if c["check_kind"] == k]
        if not ks:
            continue
        L += ["", f"### {k}（{KIND_ZH[k]}）", ""]
        shown = 0
        for layer in LAYER_RANK:
            ls = [c for c in ks if c["layer"] == layer]
            if not ls or shown >= top:
                continue
            L += [f"#### {layer}", ""]
            rows = ls[:top - shown]
            shown += len(rows)
            L += _table(["表", "范围", "规则", "状态", "连续不同次数", "首次发现", "最近一次", "差异摘要"],
                        [[c["source_table"], c["scope_label"], f"{c['rule_key']}@{c['rule_revision']}",
                          STATE_ZH[c["state"]], c["consecutive_different"], _local(ctx, c["first_seen"]),
                          _local(ctx, c["latest_at"]), c["summary"]] for c in rows])
            L.append("")
        if len(ks) > shown:
            L.append(f"{k} 共 {len(ks)} 条，此处列出前 {shown} 条，完整内容见 differences.csv。")
    L += ["", "## 3. 未完成与不支持", ""]
    inc = d["incomplete"]
    cats: dict = {}
    for x in inc:
        cats[x["category"]] = cats.get(x["category"], 0) + 1
    L.append("合计：" + ("，".join(f"{k} {v}" for k, v in sorted(cats.items())) or "无"))
    L.append("")
    L += _table(["类别", "类型", "层级", "表", "范围", "规则", "原因"],
                [[x["category"], x["check_kind"], x["layer"], x["table"], x["scope"], x["rule"], x["reason"]]
                 for x in inc[:top]] or [["（无）", "", "", "", "", "", ""]])
    if len(inc) > top:
        L.append(f"\n共 {len(inc)} 条，此处列出前 {top} 条，完整内容见 incomplete.csv。")
    L += ["", "### 疑似存储类错误（STORAGE_SUSPECT）", "",
          "查询返回了疑似存储类错误，需转运维线下确认；这不是“数据已损坏”的结论，没有此类错误也不说明存储健康。", ""]
    L += _table(["side", "表", "范围", "错误码", "首次", "最近", "次数"],
                [[s["side"], f"{s['source_db']}.{s['source_table']}" if s["side"] == "huawei"
                  else f"{s['target_db']}.{s['target_table']}", s["scope"], s["error_code"],
                  _local(ctx, s["first_seen"]), _local(ctx, s["last_seen"]), s["n"]]
                 for s in d["storage_suspect"][:top]] or [["（无）", "", "", "", "", "", ""]])
    L += ["", "## 4. 负载与熔断", ""]
    ps = d["load"]["per_side"]
    L += _table(["集群", "降并发", "恢复", "硬熔断", "人工恢复", "截止取消"],
                [[s, ps[s]["degrade"], ps[s]["recover"], ps[s]["hard_stop"], ps[s]["resumed"],
                  ps[s]["deadline_cancels"]] for s in SIDES])
    if d["load"]["events"]:
        L += ["", "时间点：", ""]
        L += _table(["时间", "集群", "事件", "说明"],
                    [[_local(ctx, e["created_at"]), e["side"], e["event_type"], _event_text(e)]
                     for e in d["load"]["events"][:top]])
    L += ["", "查询耗时（毫秒）：", ""]
    L += _table(["集群", "类型", "查询数", "P50", "P95", "最大", "超时"],
                [[r["side"], r["check_kind"] or "全部", r["n"], _ms(r["p50"]), _ms(r["p95"]), _ms(r["mx"]),
                  r["timeouts"]] for r in d["load"]["durations"]] or [["（无）", "", "", "", "", "", ""]])
    if d["unconfirmed_cancels"]:
        L += ["", "未确认终止的查询：", ""]
        L += _table(["集群", "状态", "表", "类型", "取消原因"],
                    [[a["side"], a["status"], f"{a['source_db']}.{a['source_table']}", a["check_kind"] or "METADATA",
                      a["cancel_reason"]] for a in d["unconfirmed_cancels"][:top]])
    L += ["", "## 5. 声明", ""]
    st = d["statements"]
    L += ["- 共同快照保证：无。两端在各自的在线状态下读取，读取时间不同；差异可能来自在线写入时差。",
          f"- 证据类型：{', '.join(st['evidence']) or '（本区间无比较）'}。COUNT 比较行数，METRIC 比较分组聚合值，"
          "SAMPLE 比较按固定种子选中的样本行；抽样结果只代表样本，不代表全部行已比较。",
          "- 比较的列范围、规则版本与双端读取的时间区间如下：", ""]
    L += _table(["类型", "规则@版本", "证据", "比较次数", "列范围", "源端读取区间", "目标端读取区间"],
                [[r["check_kind"], r["rule"], r["evidence"], r["comparisons"], r["columns"], r["source_read"],
                  r["target_read"]] for r in st["rules"][:top]] or [["（无）", "", "", "", "", "", ""]])
    return L


# ---- listing and download ------------------------------------------------------------------------

def list_reports(ctx: Ctx, limit: int = 100) -> list[dict]:
    rows = ctx.db.query("""SELECT detail->>'report_id' AS id, min(detail->>'kind') AS kind,
                                  min(detail->>'window_key') AS window_key, min(detail->>'interval_start') AS start,
                                  min(detail->>'interval_end') AS "end", min(created_at) AS created_at,
                                  array_agg(detail->>'file' ORDER BY detail->>'file') AS files,
                                  sum(size_bytes) AS size_bytes
                           FROM dq_artifact WHERE material_scope='REPORT' GROUP BY 1
                           ORDER BY min(created_at) DESC LIMIT %s""", (int(limit),))
    return [{"id": r["id"], "kind": r["kind"], "window_key": r["window_key"], "interval_start": r["start"],
             "interval_end": r["end"], "created_at": iso(r["created_at"]), "files": r["files"],
             "size_bytes": int(r["size_bytes"])} for r in rows]


def report_file(ctx: Ctx, report_id, name: str) -> tuple[Path, str]:
    rid = as_uuid(report_id, "report_id")
    if name not in FILES:
        raise ApiError(404, "NOT_FOUND", "unknown report file")
    row = ctx.db.one("SELECT rel_path, content_digest FROM dq_artifact WHERE material_scope='REPORT' AND rel_path=%s",
                     (f"{rid}/{name}",))
    if row is None:
        raise ApiError(404, "NOT_FOUND", "report file not found")
    path = report_dir(ctx) / row["rel_path"]
    if not path.is_file():
        raise ApiError(404, "NOT_FOUND", "report file is missing on disk")
    media = "text/markdown; charset=utf-8" if name.endswith(".md") else "text/csv; charset=utf-8"
    return path, media
