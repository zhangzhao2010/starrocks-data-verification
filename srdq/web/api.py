"""REST /v1 for the single local user (doc 03 §11.1). Thin wrappers over the controller modules."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, File, Form, Header, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from ..controller import (differences, importer, manifest, manual_requests, operations, planning, queue, reports,
                          schedules, views)
from ..controller.common import ApiError, Ctx
from ..db.db import schema_version

router = APIRouter(prefix="/v1")

MAX_UPLOAD = 64 * 1024 * 1024
IMPORT_KINDS = {"ASSET_LIST", "METRIC_TEMPLATE", "SAMPLE_TEMPLATE"}


def ctx_of(request: Request) -> Ctx:
    return request.app.state.ctx


def ok(out: Any, status: int = 200) -> JSONResponse:
    return JSONResponse(views.clean(out), status_code=status)


def params(request: Request) -> dict:
    return dict(request.query_params)


def reason_of(body: dict) -> str | None:
    r = body.get("reason")
    return r.strip() if isinstance(r, str) and r.strip() else None


def need_reason(body: dict) -> str:
    r = reason_of(body)
    if r is None:
        raise ApiError(422, "REASON_REQUIRED", "a reason is required")
    return r


def without_reason(body: dict) -> dict:
    return {k: v for k, v in body.items() if k != "reason"}


def read_upload(file: UploadFile) -> bytes:
    data = file.file.read(MAX_UPLOAD + 1)
    if len(data) > MAX_UPLOAD:
        raise ApiError(413, "FILE_TOO_LARGE", f"uploads are limited to {MAX_UPLOAD // (1024 * 1024)} MiB")
    if not data:
        raise ApiError(422, "EMPTY_FILE", "the uploaded file is empty")
    return data


def preview_import(ctx: Ctx, kind: str, file_name: str, data: bytes, mode: str | None) -> dict:
    if kind == "ASSET_LIST":
        return importer.preview_assets(ctx, file_name, data, mode or "CREATE_OR_VALIDATE")
    if kind == "METRIC_TEMPLATE":
        return importer.preview_metric_rules(ctx, file_name, data)
    if kind == "SAMPLE_TEMPLATE":
        return importer.preview_sample_rules(ctx, file_name, data)
    raise ApiError(422, "BAD_KIND", f"kind must be one of {sorted(IMPORT_KINDS)}")


# ---- status --------------------------------------------------------------------------------------

@router.get("/health")
def health(request: Request):
    ctx = ctx_of(request)
    return ok({"status": "ok", "schema_version": schema_version(ctx.db), "now": ctx.now()})


@router.get("/dashboard")
def dashboard(request: Request):
    return ok(views.dashboard(ctx_of(request)))


@router.get("/control")
def control(request: Request):
    return ok(views.get_control(ctx_of(request)))


@router.post("/recovery")
def recovery(request: Request, body: dict = Body(default={})):
    return ok(operations.enter_recovery(ctx_of(request), need_reason(body)))


# ---- imports -------------------------------------------------------------------------------------

@router.post("/imports/preview", status_code=201)
def imports_preview(request: Request, file: UploadFile = File(...), kind: str = Form("ASSET_LIST"),
                    mode: str | None = Form(None)):
    data = read_upload(file)
    return ok(preview_import(ctx_of(request), kind, file.filename or "upload.csv", data, mode), 201)


@router.get("/imports")
def imports_list(request: Request):
    return ok(views.list_imports(ctx_of(request)))


@router.get("/imports/{batch_id}")
def imports_get(request: Request, batch_id: str):
    return ok(views.get_import(ctx_of(request), batch_id))


@router.post("/imports/{batch_id}/confirm")
def imports_confirm(request: Request, batch_id: str, body: dict = Body(default={})):
    return ok(importer.confirm_import(ctx_of(request), batch_id, reason=reason_of(body)))


@router.post("/imports/{batch_id}/reject")
def imports_reject(request: Request, batch_id: str, body: dict = Body(default={})):
    return ok(importer.reject_import(ctx_of(request), batch_id, reason=reason_of(body)))


# ---- manifests -----------------------------------------------------------------------------------

@router.post("/manifests/discover", status_code=201)
def manifests_discover(request: Request, body: dict = Body(default={})):
    return ok(manifest.discover(ctx_of(request), reason=reason_of(body)), 201)


@router.get("/manifests")
def manifests_list(request: Request):
    return ok(views.list_manifests(ctx_of(request)))


@router.get("/manifests/{manifest_id}")
def manifests_get(request: Request, manifest_id: str):
    return ok(views.get_manifest(ctx_of(request), manifest_id, params(request)))


@router.get("/manifests/{manifest_id}/scopes")
def manifests_scopes(request: Request, manifest_id: str):
    return ok(views.manifest_scopes(ctx_of(request), manifest_id, params(request)))


@router.post("/manifests/{manifest_id}/confirm")
def manifests_confirm(request: Request, manifest_id: str, body: dict = Body(default={})):
    return ok(manifest.confirm_manifest(ctx_of(request), manifest_id, reason=reason_of(body)))


@router.post("/manifests/{manifest_id}/reject")
def manifests_reject(request: Request, manifest_id: str, body: dict = Body(default={})):
    return ok(manifest.reject_manifest(ctx_of(request), manifest_id, reason=reason_of(body)))


# ---- assets and check rules ----------------------------------------------------------------------

@router.get("/assets")
def assets_list(request: Request):
    return ok(views.list_assets(ctx_of(request), params(request)))


@router.get("/assets/{asset_id}")
def assets_get(request: Request, asset_id: str):
    return ok(views.get_asset(ctx_of(request), asset_id))


@router.get("/assets/{asset_id}/check-rules")
def assets_rules(request: Request, asset_id: str):
    return ok(views.asset_rules(ctx_of(request), asset_id))


@router.get("/check-rules")
def rules_list(request: Request):
    return ok(views.list_rules(ctx_of(request), params(request)))


@router.post("/check-rules/preview", status_code=201)
def rules_preview(request: Request, file: UploadFile = File(...), kind: str = Form(...)):
    k = {"METRIC": "METRIC_TEMPLATE", "SAMPLE": "SAMPLE_TEMPLATE"}.get(kind)
    if k is None:
        raise ApiError(422, "BAD_KIND", "kind must be METRIC or SAMPLE")
    data = read_upload(file)
    return ok(preview_import(ctx_of(request), k, file.filename or "rules.csv", data, None), 201)


@router.post("/check-rules/approve")
def rules_approve(request: Request, body: dict = Body(default={})):
    ids = body.get("rule_ids")
    if not isinstance(ids, list) or not ids:
        raise ApiError(422, "BAD_RULE_IDS", "rule_ids must be a non-empty list")
    return ok(importer.approve_rules(ctx_of(request), ids, approval_ref=body.get("approval_ref"),
                                     reason=reason_of(body)))


@router.post("/check-rules/{rule_id}/approve")
def rule_approve(request: Request, rule_id: str, body: dict = Body(default={})):
    return ok(importer.approve_rules(ctx_of(request), [rule_id], approval_ref=body.get("approval_ref"),
                                     reason=reason_of(body)))


@router.post("/check-rules/{rule_id}/enable")
def rule_enable(request: Request, rule_id: str, body: dict = Body(default={})):
    return ok(importer.set_rule_enabled(ctx_of(request), rule_id, True, reason=reason_of(body)))


@router.post("/check-rules/{rule_id}/disable")
def rule_disable(request: Request, rule_id: str, body: dict = Body(default={})):
    return ok(importer.set_rule_enabled(ctx_of(request), rule_id, False, reason=reason_of(body)))


# ---- plans, schedules, runs ----------------------------------------------------------------------

@router.get("/plans")
def plans_list(request: Request):
    return ok(views.list_plans(ctx_of(request)))


@router.post("/plans", status_code=201)
def plans_create(request: Request, body: dict = Body(default={})):
    return ok(planning.create_plan(ctx_of(request), without_reason(body), reason=reason_of(body)), 201)


@router.get("/plans/{plan_id}")
def plans_get(request: Request, plan_id: str):
    return ok(views.get_plan(ctx_of(request), plan_id))


@router.patch("/plans/{plan_id}")
def plans_update(request: Request, plan_id: str, body: dict = Body(default={}),
                 if_match: str | None = Header(None)):
    return ok(planning.update_plan(ctx_of(request), plan_id, without_reason(body), if_match, reason=reason_of(body)))


@router.post("/schedules/preview")
def schedules_preview(request: Request, body: dict = Body(default={})):
    return ok(schedules.preview_schedule(ctx_of(request), without_reason(body)))


@router.get("/schedules")
def schedules_list(request: Request):
    return ok(schedules.list_schedules(ctx_of(request)))


@router.post("/schedules", status_code=201)
def schedules_create(request: Request, body: dict = Body(default={})):
    return ok(schedules.create_schedule(ctx_of(request), without_reason(body), reason=reason_of(body)), 201)


@router.get("/schedules/{schedule_id}")
def schedules_get(request: Request, schedule_id: str):
    return ok(schedules.get_schedule(ctx_of(request), schedule_id))


@router.patch("/schedules/{schedule_id}")
def schedules_update(request: Request, schedule_id: str, body: dict = Body(default={}),
                     if_match: str | None = Header(None)):
    return ok(schedules.update_schedule(ctx_of(request), schedule_id, without_reason(body), if_match,
                                        reason=reason_of(body)))


@router.post("/schedules/{schedule_id}/actions")
def schedules_action(request: Request, schedule_id: str, body: dict = Body(default={}),
                     if_match: str | None = Header(None)):
    return ok(schedules.schedule_action(ctx_of(request), schedule_id, str(body.get("action") or ""),
                                        if_match if if_match is not None else body.get("revision"),
                                        reason=reason_of(body)))


@router.get("/runs")
def runs_list(request: Request):
    return ok(views.list_runs(ctx_of(request), params(request)))


@router.post("/runs", status_code=201)
def runs_create(request: Request, body: dict = Body(default={})):
    if not body.get("plan_id"):
        raise ApiError(422, "PLAN_REQUIRED", "plan_id is required")
    return ok(planning.start_plan_run(ctx_of(request), body["plan_id"], request_key=body.get("request_key"),
                                      reason=reason_of(body), check_selector=body.get("check_selector")), 201)


@router.get("/runs/{run_id}")
def runs_get(request: Request, run_id: str):
    return ok(views.get_run(ctx_of(request), run_id))


@router.get("/runs/{run_id}/nodes")
def runs_nodes(request: Request, run_id: str):
    return ok(views.run_nodes(ctx_of(request), run_id, params(request)))


@router.get("/runs/{run_id}/checks")
def runs_checks(request: Request, run_id: str):
    return ok(views.run_checks(ctx_of(request), run_id, params(request)))


# ---- operations and manual requests --------------------------------------------------------------

@router.post("/operations/preview", status_code=201)
def operations_preview(request: Request, body: dict = Body(default={})):
    return ok(operations.preview_operation(ctx_of(request), without_reason(body)), 201)


@router.get("/operations")
def operations_list(request: Request):
    return ok(operations.list_operations(ctx_of(request), int(request.query_params.get("limit") or 50)))


@router.get("/operations/{op_id}")
def operations_get(request: Request, op_id: str):
    return ok(operations.get_operation(ctx_of(request), op_id))


@router.post("/operations/{op_id}/apply")
def operations_apply(request: Request, op_id: str, body: dict = Body(default={}),
                     idempotency_key: str | None = Header(None), if_match: str | None = Header(None)):
    return ok(operations.apply_operation(ctx_of(request), op_id, idempotency_key=idempotency_key,
                                         if_match=if_match, reason=reason_of(body)))


@router.post("/manual-requests/preview", status_code=201)
def manual_preview(request: Request, body: dict = Body(default={})):
    return ok(manual_requests.preview(ctx_of(request), without_reason(body), reason_of(body)), 201)


@router.get("/manual-requests")
def manual_list(request: Request):
    return ok(manual_requests.list_requests(ctx_of(request)))


@router.get("/manual-requests/{request_id}")
def manual_get(request: Request, request_id: str):
    return ok(manual_requests.get_request(ctx_of(request), request_id))


@router.post("/manual-requests/{request_id}/confirm")
def manual_confirm(request: Request, request_id: str, body: dict = Body(default={}),
                   if_match: str | None = Header(None)):
    return ok(manual_requests.confirm(ctx_of(request), request_id, reason=reason_of(body), if_match=if_match,
                                      detail_approval_ref=body.get("detail_approval_ref")))


@router.post("/manual-requests/{request_id}/reject")
def manual_reject(request: Request, request_id: str, body: dict = Body(default={})):
    return ok(manual_requests.reject(ctx_of(request), request_id, reason_of(body)))


# ---- results -------------------------------------------------------------------------------------

@router.get("/comparisons/{comparison_id}")
def comparisons_get(request: Request, comparison_id: str):
    return ok(views.get_comparison(ctx_of(request), comparison_id))


@router.get("/comparisons/{comparison_id}/differences")
def comparisons_differences(request: Request, comparison_id: str):
    return ok(views.comparison_differences(ctx_of(request), comparison_id, params(request)))


@router.get("/differences")
def differences_list(request: Request):
    return ok(differences.list_differences(ctx_of(request), params(request)))


@router.get("/reports")
def reports_list(request: Request):
    return ok(reports.list_reports(ctx_of(request)))


@router.post("/reports", status_code=201)
def reports_create(request: Request, body: dict = Body(default={})):
    return ok(reports.generate_stage(ctx_of(request), str(body.get("start_date") or ""),
                                     str(body.get("end_date") or ""), reason_of(body)), 201)


@router.post("/reports/daily/{window_key}/regenerate", status_code=201)
def reports_regenerate(request: Request, window_key: str, body: dict = Body(default={})):
    return ok(reports.regenerate_daily(ctx_of(request), window_key, reason_of(body)), 201)


@router.get("/reports/{report_id}/files/{name}")
def reports_file(request: Request, report_id: str, name: str):
    path, media = reports.report_file(ctx_of(request), report_id, name)
    return FileResponse(path, media_type=media, filename=name)


# ---- incidents, audit, workers -------------------------------------------------------------------

@router.get("/incidents")
def incidents_list(request: Request):
    return ok(views.list_incidents(ctx_of(request), params(request)))


@router.get("/audit")
def audit_list(request: Request):
    return ok(views.list_audit(ctx_of(request), params(request)))


@router.get("/workers")
def workers_list(request: Request):
    return ok(views.list_workers(ctx_of(request)))


@router.post("/workers/{name}/disable")
def workers_disable(request: Request, name: str, body: dict = Body(default={})):
    return ok(queue.disable_worker(ctx_of(request), name, reason=reason_of(body)))
