"""Server-rendered pages and form actions (Chinese UI). No inline script or style: the CSP forbids them."""
from __future__ import annotations

import datetime as _dt
import json
import uuid
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import FormData, UploadFile

from ..controller import (differences, importer, manifest, manual_requests, operations, planning, queue, reports,
                          schedules, views)
from ..controller.common import KIND_RANK, LAYER_RANK, ApiError, Ctx
from .api import MAX_UPLOAD, preview_import

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
KINDS = list(KIND_RANK)
LAYERS = list(LAYER_RANK)


def _ctx(request: Request) -> Ctx:
    return request.app.state.ctx


def _tz(request: Request) -> ZoneInfo:
    return ZoneInfo(_ctx(request).cfg["execution_window"]["timezone"])


def _ts(v: Any, tz: ZoneInfo | None = None) -> str:
    if v in (None, ""):
        return "—"
    if isinstance(v, str):
        try:
            v = _dt.datetime.fromisoformat(v)
        except ValueError:
            return v
    if isinstance(v, _dt.datetime):
        if v.tzinfo is not None and tz is not None:
            v = v.astimezone(tz)
        return v.strftime("%Y-%m-%d %H:%M:%S")
    return str(v)


def _pj(v: Any) -> str:
    return json.dumps(views.clean(v), ensure_ascii=False, indent=2, sort_keys=True)


def _pct(a: Any, b: Any) -> str:
    try:
        a, b = float(a), float(b)
    except (TypeError, ValueError):
        return "—"
    return "—" if b <= 0 else f"{100.0 * a / b:.1f}%"


templates.env.filters["pj"] = _pj
templates.env.filters["pct"] = _pct
templates.env.globals.update(KINDS=KINDS, LAYERS=LAYERS)


def page(request: Request, name: str, status: int = 200, **context: Any) -> HTMLResponse:
    ctx = _ctx(request)
    tz = _tz(request)
    context.setdefault("title", "")
    context["ts"] = lambda v: _ts(v, tz)
    context["tzname"] = str(tz)
    context["CLUSTERS"] = list(ctx.cfg["physical_clusters"])
    context["PAIRS"] = list(ctx.cfg["cluster_pairs"])
    context["query"] = dict(request.query_params)
    context["path"] = request.url.path
    return templates.TemplateResponse(request, name, context, status_code=status)


def render_error(request: Request, status: int, body: dict) -> HTMLResponse:
    try:
        return page(request, "error.html", status=status, title="出错了", error=body)
    except Exception:  # noqa: BLE001 - the error page must never fail on its own
        from html import escape

        return HTMLResponse(f"<h1>{status}</h1><pre>{escape(json.dumps(body, ensure_ascii=False))}</pre>",
                            status_code=status)


def go(url: str) -> RedirectResponse:
    return RedirectResponse(url, status_code=303)


def qs_without(query: dict, *keys: str) -> str:
    from urllib.parse import urlencode

    return urlencode({k: v for k, v in query.items() if k not in keys and v not in (None, "")})


templates.env.globals["qs_without"] = qs_without


async def call(fn, *args, **kw):
    return await run_in_threadpool(fn, *args, **kw)


def text(form: FormData, key: str) -> str | None:
    v = form.get(key)
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def need(form: FormData, key: str, label: str) -> str:
    v = text(form, key)
    if v is None:
        raise ApiError(422, "FIELD_REQUIRED", f"请填写{label}")
    return v


def json_field(form: FormData, key: str) -> Any:
    v = text(form, key)
    if v is None:
        return None
    try:
        return json.loads(v)
    except json.JSONDecodeError as e:
        raise ApiError(422, "BAD_JSON", f"{key} 不是合法的 JSON：{e.msg}") from e


def check_selector_of(form: FormData) -> dict:
    kinds = [k for k in form.getlist("kinds") if k in KINDS]
    keys = [k.strip() for k in (text(form, "rule_keys") or "").split(",") if k.strip()]
    if not kinds and not keys:
        return {"all": True}
    sel: dict = {}
    if kinds:
        sel["kinds"] = kinds
    if keys:
        sel["rule_keys"] = keys
    return sel


def asset_selector_of(form: FormData, ctx: Ctx) -> dict:
    layers = [x for x in form.getlist("layers") if x in LAYERS]
    pair_ids = [x for x in form.getlist("pair_ids") if x in {p["id"] for p in ctx.cfg["cluster_pairs"]}]
    ids = [x.strip() for x in (text(form, "asset_ids") or "").replace("\n", ",").split(",") if x.strip()]
    if ids:
        return {"asset_ids": ids}
    if pair_ids:
        return {"pair_ids": pair_ids}
    if layers:
        return {"layers": layers}
    return {"all": True}


# ---- dashboard -----------------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    d = await call(views.dashboard, _ctx(request))
    return page(request, "dashboard.html", title="看板", d=d, refresh=30)


@router.get("/favicon.ico", include_in_schema=False)
def favicon():
    return Response(status_code=204)


# ---- assets, imports, rules ----------------------------------------------------------------------

@router.get("/assets", response_class=HTMLResponse)
async def assets(request: Request):
    data = await call(views.list_assets, _ctx(request), dict(request.query_params))
    return page(request, "assets.html", title="资产", data=data)


@router.get("/assets/{asset_id}", response_class=HTMLResponse)
async def asset(request: Request, asset_id: str):
    a = await call(views.get_asset, _ctx(request), asset_id)
    return page(request, "asset.html", title=f"{a['source_db']}.{a['source_table']}", a=a)


@router.get("/imports", response_class=HTMLResponse)
async def imports(request: Request):
    items = await call(views.list_imports, _ctx(request))
    return page(request, "imports.html", title="导入", items=items)


@router.get("/imports/{batch_id}", response_class=HTMLResponse)
async def import_batch(request: Request, batch_id: str):
    b = await call(views.get_import, _ctx(request), batch_id)
    return page(request, "import.html", title="导入预览", b=b)


@router.post("/actions/imports/preview")
async def act_import_preview(request: Request):
    form = await request.form()
    f = form.get("file")
    if not isinstance(f, UploadFile):
        raise ApiError(422, "FILE_REQUIRED", "请选择文件")
    data = await f.read(MAX_UPLOAD + 1)
    if len(data) > MAX_UPLOAD:
        raise ApiError(413, "FILE_TOO_LARGE", "文件超过 64 MiB")
    if not data:
        raise ApiError(422, "EMPTY_FILE", "文件为空")
    out = await call(preview_import, _ctx(request), text(form, "kind") or "ASSET_LIST", f.filename or "upload.csv",
                     data, text(form, "mode"))
    return go(f"/imports/{out['id']}")


@router.post("/actions/imports/{batch_id}/confirm")
async def act_import_confirm(request: Request, batch_id: str):
    form = await request.form()
    await call(importer.confirm_import, _ctx(request), batch_id, reason=text(form, "reason"))
    return go(f"/imports/{batch_id}")


@router.post("/actions/imports/{batch_id}/reject")
async def act_import_reject(request: Request, batch_id: str):
    form = await request.form()
    await call(importer.reject_import, _ctx(request), batch_id, reason=text(form, "reason"))
    return go(f"/imports/{batch_id}")


@router.get("/rules", response_class=HTMLResponse)
async def rules(request: Request):
    data = await call(views.list_rules, _ctx(request), dict(request.query_params))
    return page(request, "rules.html", title="检查规则", data=data)


@router.post("/actions/rules/approve")
async def act_rules_approve(request: Request):
    form = await request.form()
    ids = [x for x in form.getlist("rule_ids") if isinstance(x, str) and x]
    if not ids:
        raise ApiError(422, "FIELD_REQUIRED", "请勾选要批准的规则")
    await call(importer.approve_rules, _ctx(request), ids, approval_ref=text(form, "approval_ref"),
               reason=text(form, "reason"))
    return go(text(form, "back") or "/rules")


@router.post("/actions/rules/{rule_id}/{action}")
async def act_rule_toggle(request: Request, rule_id: str, action: str):
    if action not in ("enable", "disable"):
        raise ApiError(404, "NOT_FOUND", "unknown action")
    form = await request.form()
    await call(importer.set_rule_enabled, _ctx(request), rule_id, action == "enable", reason=text(form, "reason"))
    return go(text(form, "back") or "/rules")


# ---- manifests -----------------------------------------------------------------------------------

@router.get("/manifests", response_class=HTMLResponse)
async def manifests(request: Request):
    ctx = _ctx(request)
    items = await call(views.list_manifests, ctx)
    runs = await call(views.list_runs, ctx, {"purpose": "METADATA_PREVIEW", "page_size": 5})
    return page(request, "manifests.html", title="范围清单", items=items, discoveries=runs["items"])


@router.get("/manifests/{manifest_id}", response_class=HTMLResponse)
async def manifest_page(request: Request, manifest_id: str):
    m = await call(views.get_manifest, _ctx(request), manifest_id, dict(request.query_params))
    return page(request, "manifest.html", title=f"范围清单 v{m['version']}", mf=m)


@router.post("/actions/manifests/discover")
async def act_discover(request: Request):
    form = await request.form()
    await call(manifest.discover, _ctx(request), reason=text(form, "reason"))
    return go("/manifests")


@router.post("/actions/manifests/{manifest_id}/{action}")
async def act_manifest(request: Request, manifest_id: str, action: str):
    fn = {"confirm": manifest.confirm_manifest, "reject": manifest.reject_manifest}.get(action)
    if fn is None:
        raise ApiError(404, "NOT_FOUND", "unknown action")
    form = await request.form()
    await call(fn, _ctx(request), manifest_id, reason=text(form, "reason"))
    return go(f"/manifests/{manifest_id}")


# ---- plans and schedules -------------------------------------------------------------------------

@router.get("/plans", response_class=HTMLResponse)
async def plans(request: Request):
    ctx = _ctx(request)
    items = await call(views.list_plans, ctx)
    mans = [m for m in await call(views.list_manifests, ctx) if m["status"] in ("CONFIRMED", "SUPERSEDED")]
    return page(request, "plans.html", title="计划", items=items, manifests=mans)


@router.get("/plans/{plan_id}", response_class=HTMLResponse)
async def plan_page(request: Request, plan_id: str):
    p = await call(views.get_plan, _ctx(request), plan_id)
    return page(request, "plan.html", title=f"计划 {p['name']}", p=p, new_key=uuid.uuid4().hex)


@router.post("/actions/plans")
async def act_plan_create(request: Request):
    form = await request.form()
    ctx = _ctx(request)
    body = {"name": need(form, "name", "计划名称"), "manifest_mode": text(form, "manifest_mode") or "LATEST_CONFIRMED",
            "manifest_id": text(form, "manifest_id"), "check_selector": check_selector_of(form),
            "asset_selector": asset_selector_of(form, ctx)}
    out = await call(planning.create_plan, ctx, body, reason=text(form, "reason"))
    return go(f"/plans/{out['id']}")


@router.post("/actions/plans/{plan_id}/archive")
async def act_plan_archive(request: Request, plan_id: str):
    form = await request.form()
    await call(planning.update_plan, _ctx(request), plan_id, {"status": "ARCHIVED"}, text(form, "revision"),
               reason=text(form, "reason"))
    return go(f"/plans/{plan_id}")


@router.post("/actions/plans/{plan_id}/run")
async def act_plan_run(request: Request, plan_id: str):
    form = await request.form()
    sel = check_selector_of(form)
    out = await call(planning.start_plan_run, _ctx(request), plan_id, request_key=text(form, "request_key"),
                     reason=text(form, "reason"), check_selector=None if sel.get("all") else sel)
    return go(f"/runs/{out['id']}")


@router.get("/schedules", response_class=HTMLResponse)
async def schedules_page(request: Request):
    ctx = _ctx(request)
    items = await call(schedules.list_schedules, ctx)
    plan_list = [p for p in await call(views.list_plans, ctx) if p["status"] == "ACTIVE"]
    names = {p["id"]: p["name"] for p in await call(views.list_plans, ctx)}
    return page(request, "schedules.html", title="周期", items=items, plans=plan_list, plan_names=names,
                default_tz=ctx.cfg["execution_window"]["timezone"])


@router.get("/schedules/{schedule_id}", response_class=HTMLResponse)
async def schedule_page(request: Request, schedule_id: str):
    s = await call(schedules.get_schedule, _ctx(request), schedule_id)
    return page(request, "schedule.html", title=f"周期 {s['name']}", s=s)


def _schedule_body(form: FormData) -> dict:
    body: dict = {"name": need(form, "name", "周期名称"), "plan_id": need(form, "plan_id", "计划"),
                  "kind": need(form, "kind", "周期类型")}
    for k in ("cron_expr", "timezone", "starts_at", "ends_at", "overlap", "misfire"):
        if text(form, k):
            body[k] = text(form, k)
    for k in ("interval_seconds", "rest_seconds"):
        if text(form, k):
            try:
                body[k] = int(text(form, k) or "")
            except ValueError as e:
                raise ApiError(422, "BAD_FIELD", f"{k} 必须是整数") from e
    body["cancel_on_end"] = form.get("cancel_on_end") == "1"
    body["enabled"] = form.get("enabled", "1") == "1"
    sel = check_selector_of(form)
    if not sel.get("all"):
        body["check_selector"] = sel
    return body


@router.post("/actions/schedules")
async def act_schedule_create(request: Request):
    form = await request.form()
    body = _schedule_body(form)
    ctx = _ctx(request)
    if form.get("preview_only") == "1":
        pv = await call(schedules.preview_schedule, ctx, body)
        items = await call(schedules.list_schedules, ctx)
        plan_list = [p for p in await call(views.list_plans, ctx) if p["status"] == "ACTIVE"]
        names = {p["id"]: p["name"] for p in await call(views.list_plans, ctx)}
        return page(request, "schedules.html", title="周期", items=items, plans=plan_list, plan_names=names,
                    default_tz=ctx.cfg["execution_window"]["timezone"], preview=pv, draft=dict(form))
    out = await call(schedules.create_schedule, ctx, body, reason=text(form, "reason"))
    return go(f"/schedules/{out['id']}")


@router.post("/actions/schedules/{schedule_id}/action")
async def act_schedule_action(request: Request, schedule_id: str):
    form = await request.form()
    await call(schedules.schedule_action, _ctx(request), schedule_id, need(form, "action", "动作"),
               text(form, "revision"), reason=text(form, "reason"))
    return go(f"/schedules/{schedule_id}")


# ---- runs and operations -------------------------------------------------------------------------

@router.get("/runs", response_class=HTMLResponse)
async def runs(request: Request):
    data = await call(views.list_runs, _ctx(request), dict(request.query_params))
    return page(request, "runs.html", title="运行", data=data)


@router.get("/runs/{run_id}", response_class=HTMLResponse)
async def run_page(request: Request, run_id: str):
    ctx = _ctx(request)
    q = dict(request.query_params)
    r = await call(views.get_run, ctx, run_id)
    nodes = await call(views.run_nodes, ctx, run_id, {k: q[k] for k in ("asset_id", "layer", "q") if k in q}
                       | {"page": q.get("npage", 1), "page_size": q.get("npage_size", 50)})
    checks = await call(views.run_checks, ctx, run_id,
                        {k: q[k] for k in ("asset_id", "check_kind", "coverage_state", "node_id") if k in q}
                        | {"page": q.get("cpage", 1), "page_size": q.get("cpage_size", 50)})
    return page(request, "run.html", title="运行详情", r=r, nodes=nodes, checks=checks,
                coverage_states=views.COVERAGE_STATES)


@router.get("/operations", response_class=HTMLResponse)
async def operations_page(request: Request):
    items = await call(operations.list_operations, _ctx(request), 100)
    return page(request, "operations.html", title="控制操作", items=items)


@router.get("/operations/{op_id}", response_class=HTMLResponse)
async def operation_page(request: Request, op_id: str):
    o = await call(operations.get_operation, _ctx(request), op_id)
    return page(request, "operation.html", title=f"操作 {o['op']}", o=o, idem=uuid.uuid4().hex)


@router.post("/actions/operations/preview")
async def act_operation_preview(request: Request):
    form = await request.form()
    op = need(form, "op", "操作")
    body: dict = {"op": op, "target_type": need(form, "target_type", "目标类型"),
                  "target_ids": [x for x in form.getlist("target_ids") if isinstance(x, str) and x],
                  "effect": text(form, "effect"), "check_selector": check_selector_of(form), "params": {}}
    if op == "SET_PRIORITY":
        try:
            body["params"] = {"priority": int(need(form, "priority", "优先级"))}
        except ValueError as e:
            raise ApiError(422, "BAD_PRIORITY", "优先级必须是 0..100 的整数") from e
    elif op == "EXTEND_WINDOW":
        if text(form, "until"):
            body["params"] = {"until": text(form, "until")}
        else:
            try:
                body["params"] = {"minutes": int(need(form, "minutes", "延长分钟数"))}
            except ValueError as e:
                raise ApiError(422, "BAD_PARAMS", "延长分钟数必须是整数") from e
    out = await call(operations.preview_operation, _ctx(request), body)
    return go(f"/operations/{out['id']}")


@router.post("/actions/operations/{op_id}/apply")
async def act_operation_apply(request: Request, op_id: str):
    form = await request.form()
    await call(operations.apply_operation, _ctx(request), op_id, idempotency_key=text(form, "idempotency_key"),
               if_match=text(form, "if_match"), reason=text(form, "reason"))
    return go(f"/operations/{op_id}")


@router.post("/actions/recovery")
async def act_recovery(request: Request):
    form = await request.form()
    await call(operations.enter_recovery, _ctx(request), need(form, "reason", "原因"))
    return go("/")


# ---- results -------------------------------------------------------------------------------------

@router.get("/differences", response_class=HTMLResponse)
async def differences_page(request: Request):
    data = await call(differences.list_differences, _ctx(request), dict(request.query_params))
    pages = max(1, -(-data["total"] // data["page_size"]))
    return page(request, "differences.html", title="差异清单", data=dict(data, pages=pages),
                states=differences.STATES)


@router.get("/comparisons/{comparison_id}", response_class=HTMLResponse)
async def comparison_page(request: Request, comparison_id: str):
    ctx = _ctx(request)
    c = await call(views.get_comparison, ctx, comparison_id)
    diffs = await call(views.comparison_differences, ctx, comparison_id, dict(request.query_params))
    return page(request, "comparison.html", title="比较结果", c=c, diffs=diffs, new_key=uuid.uuid4().hex)


@router.get("/manual-requests", response_class=HTMLResponse)
async def manual_page(request: Request):
    items = await call(manual_requests.list_requests, _ctx(request))
    return page(request, "manual_requests.html", title="手工复核", items=items, new_key=uuid.uuid4().hex)


@router.get("/manual-requests/{request_id}", response_class=HTMLResponse)
async def manual_request_page(request: Request, request_id: str):
    m = await call(manual_requests.get_request, _ctx(request), request_id)
    return page(request, "manual_request.html", title="手工复核请求", mr=m)


@router.post("/actions/manual-requests/preview")
async def act_manual_preview(request: Request):
    form = await request.form()
    body: dict = {"request_key": need(form, "request_key", "请求键")}
    for k in ("asset_id", "origin_comparison_id", "check_kind", "requested_evidence", "rule_key",
              "detail_approval_ref", "transfer_class"):
        if text(form, k):
            body[k] = text(form, k)
    sk = [x.strip() for x in (text(form, "scope_keys") or "").splitlines() if x.strip()]
    if sk:
        body["scope_keys"] = sk
    if body.get("check_kind") == "DEEP":
        deep: dict = {"columns": [c.strip() for c in (text(form, "deep_columns") or "").split(",") if c.strip()],
                      "mode": text(form, "deep_mode") or "HASH_BUCKET"}
        for k in ("buckets", "max_rows_per_side", "max_read_bytes"):
            if text(form, f"deep_{k}"):
                deep[k] = text(form, f"deep_{k}")
        if text(form, "deep_float_tolerance"):
            deep["float_tolerance"] = text(form, "deep_float_tolerance")
        ranges = json_field(form, "deep_ranges")
        if ranges is not None:
            deep["ranges"] = ranges
        body["deep"] = deep
    out = await call(manual_requests.preview, _ctx(request), body, text(form, "reason"))
    return go(f"/manual-requests/{out['id']}")


@router.post("/actions/manual-requests/{request_id}/confirm")
async def act_manual_confirm(request: Request, request_id: str):
    form = await request.form()
    await call(manual_requests.confirm, _ctx(request), request_id, reason=text(form, "reason"),
               if_match=text(form, "if_match"), detail_approval_ref=text(form, "detail_approval_ref"))
    return go(f"/manual-requests/{request_id}")


@router.post("/actions/manual-requests/{request_id}/reject")
async def act_manual_reject(request: Request, request_id: str):
    form = await request.form()
    await call(manual_requests.reject, _ctx(request), request_id, text(form, "reason"))
    return go(f"/manual-requests/{request_id}")


@router.get("/reports", response_class=HTMLResponse)
async def reports_page(request: Request):
    items = await call(reports.list_reports, _ctx(request))
    return page(request, "reports.html", title="报告", items=items)


@router.post("/actions/reports/stage")
async def act_report_stage(request: Request):
    form = await request.form()
    await call(reports.generate_stage, _ctx(request), need(form, "start_date", "开始日期"),
               need(form, "end_date", "结束日期"), text(form, "reason"))
    return go("/reports")


@router.post("/actions/reports/daily/{window_key}/regenerate")
async def act_report_regenerate(request: Request, window_key: str):
    form = await request.form()
    await call(reports.regenerate_daily, _ctx(request), window_key, text(form, "reason"))
    return go("/reports")


# ---- incidents, audit, workers -------------------------------------------------------------------

@router.get("/incidents", response_class=HTMLResponse)
async def incidents(request: Request):
    data = await call(views.list_incidents, _ctx(request), dict(request.query_params))
    return page(request, "incidents.html", title="告警与事件", data=data)


@router.get("/audit", response_class=HTMLResponse)
async def audit_page(request: Request):
    data = await call(views.list_audit, _ctx(request), dict(request.query_params))
    return page(request, "audit.html", title="审计日志", data=data)


@router.get("/workers", response_class=HTMLResponse)
async def workers(request: Request):
    items = await call(views.list_workers, _ctx(request))
    return page(request, "workers.html", title="执行器", items=items)


@router.post("/actions/workers/{name}/disable")
async def act_worker_disable(request: Request, name: str):
    form = await request.form()
    await call(queue.disable_worker, _ctx(request), name, reason=text(form, "reason"))
    return go("/workers")
