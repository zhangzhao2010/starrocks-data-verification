"""Web / API process (doc 03 §2.1, §16): single user, no login, no access restrictions; strict CSP."""
from __future__ import annotations

import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from ..controller.common import ApiError, Ctx
from . import api, ui

log = logging.getLogger("srdq.web")

HERE = Path(__file__).parent
CSP = ("default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; object-src 'none'; "
       "base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
SECURITY_HEADERS = {
    "Content-Security-Policy": CSP,
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "same-origin",
    "Cache-Control": "no-store",
    "Cross-Origin-Opener-Policy": "same-origin",
    "Cross-Origin-Resource-Policy": "same-origin",
}


def _wants_html(request: Request) -> bool:
    return not request.url.path.startswith("/v1/") and "text/html" in request.headers.get("accept", "text/html")


def _error_response(request: Request, status: int, body: dict) -> HTMLResponse | JSONResponse:
    if _wants_html(request):
        return ui.render_error(request, status, body)
    return JSONResponse(body, status_code=status)


def create_web_app(ctx: Ctx) -> FastAPI:
    app = FastAPI(title="srdq", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.ctx = ctx

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        resp = await call_next(request)
        for k, v in SECURITY_HEADERS.items():
            resp.headers.setdefault(k, v)
        return resp

    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError):
        return _error_response(request, exc.status, exc.body())

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError):
        errs = [{"loc": [str(x) for x in e.get("loc", ())], "msg": str(e.get("msg", ""))} for e in exc.errors()]
        return _error_response(request, 422, {"code": "BAD_REQUEST", "message": "request validation failed",
                                              "detail": errs})

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException):
        code = {404: "NOT_FOUND", 405: "METHOD_NOT_ALLOWED"}.get(exc.status_code, "HTTP_ERROR")
        return _error_response(request, exc.status_code, {"code": code, "message": str(exc.detail)})

    @app.exception_handler(Exception)
    async def _internal(request: Request, exc: Exception):
        log.exception("web internal error")
        return _error_response(request, 500, {"code": "INTERNAL", "message": type(exc).__name__})

    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")
    app.include_router(api.router)
    app.include_router(ui.router)
    return app
