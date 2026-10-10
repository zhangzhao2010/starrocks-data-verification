"""Web / API process (doc 03 §2.1, §16): loopback only, single user, no login; Origin/Host checks; strict CSP."""
from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from ..controller.common import ApiError, Ctx
from . import api, ui

log = logging.getLogger("srdq.web")

HERE = Path(__file__).parent
LOOPBACK_NAMES = {"127.0.0.1", "localhost", "::1"}
SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
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


def host_name(host: str) -> str:
    host = host.strip().lower()
    if host.startswith("["):
        return host[1:host.find("]")] if "]" in host else ""
    return host.rsplit(":", 1)[0] if host.count(":") == 1 else host


def origin_ok(origin: str, host: str) -> bool:
    """Same-origin only: the Origin's host:port must be the request Host (browsers behind an SSH tunnel included)."""
    try:
        u = urlsplit(origin)
    except ValueError:
        return False
    return u.scheme in ("http", "https") and u.netloc.lower() == host.strip().lower() \
        and host_name(u.netloc) in LOOPBACK_NAMES


def _wants_html(request: Request) -> bool:
    return not request.url.path.startswith("/v1/") and "text/html" in request.headers.get("accept", "text/html")


def _error_response(request: Request, status: int, body: dict) -> HTMLResponse | JSONResponse:
    if _wants_html(request):
        return ui.render_error(request, status, body)
    return JSONResponse(body, status_code=status)


def create_web_app(ctx: Ctx) -> FastAPI:
    app = FastAPI(title="srdq", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.ctx = ctx
    check_origin = bool(ctx.cfg["web"]["origin_check"])

    @app.middleware("http")
    async def guard(request: Request, call_next):
        host = request.headers.get("host", "")
        if host_name(host) not in LOOPBACK_NAMES:
            resp = JSONResponse({"code": "HOST_REJECTED", "message": "only loopback Host headers are served"},
                                status_code=403)
        elif check_origin and request.method not in SAFE_METHODS and not _same_origin(request, host):
            resp = JSONResponse({"code": "ORIGIN_REJECTED", "message": "cross-origin write rejected"},
                                status_code=403)
        else:
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


def _same_origin(request: Request, host: str) -> bool:
    if request.headers.get("sec-fetch-site") == "cross-site":
        return False
    origin = request.headers.get("origin")
    if origin is not None:
        return origin_ok(origin, host)
    ref = request.headers.get("referer")
    if ref is not None:
        u = urlsplit(ref)
        return origin_ok(f"{u.scheme}://{u.netloc}", host)
    return True  # non-browser client on the same host (curl, CLI); browsers always send Origin on POST
