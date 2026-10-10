"""Worker-facing HTTP API (doc 03 §12). Side-bound bearer tokens; JSON errors {code, message}."""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from . import intake, queue
from .common import ApiError, Ctx

log = logging.getLogger("srdq.worker_api")

MAX_JSON_BYTES = 64 * 1024 * 1024
MAX_CHUNK_BYTES = 256 * 1024 * 1024


def _token(request: Request) -> str | None:
    h = request.headers.get("authorization") or ""
    if not h.lower().startswith("bearer "):
        return None
    return h[7:].strip() or None


async def _json(request: Request) -> dict:
    raw = await request.body()
    if len(raw) > MAX_JSON_BYTES:
        raise ApiError(413, "TOO_LARGE", "request body too large")
    try:
        body = json.loads(raw or b"{}")
    except ValueError as e:
        raise ApiError(422, "BAD_JSON", "request body is not valid JSON") from e
    if not isinstance(body, dict):
        raise ApiError(422, "BAD_JSON", "request body must be an object")
    return body


def create_app(ctx: Ctx) -> FastAPI:
    app = FastAPI(title="srdq worker api", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.ctx = ctx

    @app.exception_handler(ApiError)
    async def _api_error(_request: Request, exc: ApiError):
        return JSONResponse(exc.body(), status_code=exc.status)

    @app.exception_handler(Exception)
    async def _internal(_request: Request, exc: Exception):
        log.exception("worker api internal error")
        return JSONResponse({"code": "INTERNAL", "message": type(exc).__name__}, status_code=500)

    async def _call(request: Request, fn, *args: Any) -> JSONResponse:
        worker = await run_in_threadpool(queue.authenticate, ctx, _token(request))
        body = await _json(request)
        out = await run_in_threadpool(fn, ctx, worker, body, *args)
        return JSONResponse(out)

    @app.post("/v1/worker/registrations")
    async def registrations(request: Request):
        return await _call(request, queue.register)

    @app.post("/v1/worker/claims")
    async def claims(request: Request):
        return await _call(request, queue.claim)

    @app.post("/v1/worker/heartbeats")
    async def heartbeats(request: Request):
        return await _call(request, queue.heartbeat)

    @app.post("/v1/worker/results")
    async def results(request: Request):
        return await _call(request, intake.accept_result)

    @app.post("/v1/worker/failures")
    async def failures(request: Request):
        return await _call(request, intake.report_failure)

    @app.post("/v1/worker/cancellations")
    async def cancellations(request: Request):
        return await _call(request, intake.report_cancelled)

    @app.put("/v1/worker/result-chunks/{attempt_id}/{node_id}/{chunk_no}")
    async def chunk(attempt_id: str, node_id: str, chunk_no: str, request: Request):
        worker = await run_in_threadpool(queue.authenticate, ctx, _token(request))
        length = request.headers.get("content-length")
        if length is not None and length.isdigit() and int(length) > MAX_CHUNK_BYTES:
            raise ApiError(413, "TOO_LARGE", "chunk too large")
        data = await request.body()
        if len(data) > MAX_CHUNK_BYTES:
            raise ApiError(413, "TOO_LARGE", "chunk too large")
        out = await run_in_threadpool(intake.stage_chunk, ctx, worker, attempt_id, node_id, chunk_no, data,
                                      request.headers.get("x-content-sha256"))
        return JSONResponse(out)

    @app.get("/v1/worker/health")
    async def health():
        return {"status": "ok"}

    return app
