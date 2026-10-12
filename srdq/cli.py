"""Command line entry point: migrate, controller, web, executor, and a few operator commands."""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import stat
import sys
from pathlib import Path
from typing import Any

from . import config

log = logging.getLogger("srdq")

DEFAULT_CONFIG = "/etc/srdq/config.yaml"
SIDES = ("huawei", "aliyun")


class CliError(SystemExit):
    def __init__(self, message: str):
        super().__init__(f"srdq: {message}")


# ---- helpers -------------------------------------------------------------------------------------

def _private_file(path: str | Path, what: str) -> Path:
    """The file must exist, be a regular file owned by this OS user and not be accessible by group/other."""
    p = Path(path)
    try:
        st = p.stat()
    except FileNotFoundError as e:
        raise CliError(f"{what} {p} does not exist") from e
    if not stat.S_ISREG(st.st_mode):
        raise CliError(f"{what} {p} is not a regular file")
    if stat.S_IMODE(st.st_mode) & 0o077:
        raise CliError(f"{what} {p} must have mode 0600 (it is {oct(stat.S_IMODE(st.st_mode))})")
    if hasattr(os, "getuid") and st.st_uid != os.getuid():
        raise CliError(f"{what} {p} must be owned by the OS user running this process")
    return p


def _read_json(p: Path, what: str) -> dict:
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise CliError(f"{what} {p} is not valid JSON") from e
    if not isinstance(data, dict):
        raise CliError(f"{what} {p} must hold a JSON object")
    return data


def load_config(args) -> config.Config:
    path = args.config or os.environ.get("SRDQ_CONFIG") or DEFAULT_CONFIG
    try:
        return config.load(path)
    except FileNotFoundError as e:
        raise CliError(f"config file {path} not found (use --config or SRDQ_CONFIG)") from e
    except config.ConfigError as e:
        raise CliError(f"invalid config {path}: {e}") from e


def pg_dsn(cfg) -> str:
    """Controller PG connection from the 0600 credential file; SRDQ_PG_DSN overrides it for development."""
    env = os.environ.get("SRDQ_PG_DSN")
    if env:
        return env
    from psycopg.conninfo import make_conninfo

    p = _private_file(cfg["credentials"]["controller_pg"]["file"], "PG credential file")
    data = _read_json(p, "PG credential file")
    if data.get("dsn"):
        return str(data["dsn"])
    allowed = {"host", "port", "dbname", "user", "password", "sslmode", "sslrootcert", "connect_timeout",
               "application_name"}
    unknown = set(data) - allowed
    if unknown:
        raise CliError(f"PG credential file has unknown keys {sorted(unknown)}")
    if not data.get("dbname") or not data.get("user"):
        raise CliError("PG credential file needs at least dbname and user (or dsn)")
    data.setdefault("host", "127.0.0.1")
    data.setdefault("application_name", "srdq")
    return make_conninfo(**{k: str(v) for k, v in data.items()})


def refuse_sr_credentials(cfg, dev: bool) -> None:
    """Controller and Web must not be able to read either side's SR credentials (side isolation)."""
    readable = [s for s in SIDES if os.access(cfg["credentials"][s]["file"], os.R_OK)]
    if readable and not dev:
        raise CliError(f"this OS user can read the SR credential file(s) of {readable}; run the controller and web "
                       "as srdq-ctl (or pass --dev on a development machine)")


def make_ctx(cfg, *, max_connections: int = 8):
    from .controller.common import Ctx
    from .db.db import Database, _migrations, schema_version

    db = Database(pg_dsn(cfg), max_connections=max_connections)
    try:
        have = schema_version(db)
    except Exception as e:  # noqa: BLE001 - surfaced as a clear startup error
        raise CliError(f"cannot read the schema version ({type(e).__name__}); run `srdq migrate` first") from e
    want = len(_migrations())
    if have != want:
        raise CliError(f"database schema is at version {have}, this release needs {want}; run `srdq migrate`")
    return Ctx(cfg, db)


def _print(obj: Any) -> None:
    from .controller.views import clean

    print(json.dumps(clean(obj), ensure_ascii=False, indent=2, sort_keys=True))


def _api(fn, *a, **kw):
    from .controller.common import ApiError

    try:
        return fn(*a, **kw)
    except ApiError as e:
        raise CliError(f"{e.status} {e.code}: {e.message}") from e


def _serve(app, host: str, port: int) -> None:
    import uvicorn

    uvicorn.run(app, host=host, port=port, log_level="info", proxy_headers=False, server_header=False,
                date_header=False, access_log=False)


# ---- commands ------------------------------------------------------------------------------------

def cmd_check_config(args) -> None:
    cfg = load_config(args)
    w = cfg["execution_window"]
    out = {"config": "ok", "spec_version": cfg["spec_version"], "deployment": cfg["deployment"]["mode"],
           "run_enabled": cfg["run_enabled"], "transfer_approved": cfg.transfer_approved,
           "window": f"{w['open_at']}-{w['drain_at']}-{w['close_at']} {w['timezone']}",
           "web": f"{cfg['web']['bind_address']}:{cfg['web']['port']}",
           "worker_api": f"{cfg['controller']['worker_api_bind']}:{cfg['controller']['worker_api_port']}",
           "credentials": {}}
    for name in (*SIDES, "controller_pg"):
        f = cfg["credentials"][name]["file"]
        try:
            st = Path(f).stat()
            mode = oct(stat.S_IMODE(st.st_mode))
            out["credentials"][name] = {"file": f, "mode": mode, "readable_by_me": os.access(f, os.R_OK),
                                        "ok": not stat.S_IMODE(st.st_mode) & 0o077}
        except FileNotFoundError:
            out["credentials"][name] = {"file": f, "exists": False}
        except PermissionError:
            out["credentials"][name] = {"file": f, "readable_by_me": False}
    _print(out)


def cmd_migrate(args) -> None:
    from .db.db import migrate

    cfg = load_config(args)
    applied = migrate(pg_dsn(cfg))
    print("applied: " + (", ".join(applied) if applied else "nothing (schema is current)"))


def cmd_controller(args) -> None:
    from .controller.common import ensure_control
    from .controller.engine import TickLoop
    from .controller.worker_api import create_app

    cfg = load_config(args)
    refuse_sr_credentials(cfg, args.dev)
    ctx = make_ctx(cfg, max_connections=16)
    for d in (ctx.staging_dir, ctx.material_dir, Path(cfg["report"]["dir"])):
        d.mkdir(parents=True, exist_ok=True)
    ensure_control(ctx)
    loop = TickLoop(ctx)
    loop.start()
    try:
        _serve(create_app(ctx), cfg["controller"]["worker_api_bind"], int(cfg["controller"]["worker_api_port"]))
    finally:
        loop.stop()


def cmd_web(args) -> None:
    from .controller.common import ensure_control
    from .web.app import create_web_app

    cfg = load_config(args)
    refuse_sr_credentials(cfg, args.dev)
    ctx = make_ctx(cfg)
    ensure_control(ctx)
    _serve(create_web_app(ctx), cfg["web"]["bind_address"], int(cfg["web"]["port"]))


def _executor_token(args, creds: dict | None) -> str:
    if args.token_file:
        tok = _private_file(args.token_file, "token file").read_text(encoding="utf-8").strip()
    elif os.environ.get("SRDQ_WORKER_TOKEN") and args.sim:
        tok = os.environ["SRDQ_WORKER_TOKEN"].strip()
    else:
        tok = str((creds or {}).get("worker_token") or "").strip()
    if not tok:
        raise CliError("no worker token: put \"worker_token\" in the side credential file or pass --token-file")
    return tok


def cmd_executor(args) -> None:
    from .executor.client import WorkerClient
    from .executor.runner import Executor

    cfg = load_config(args)
    side = args.side
    other = next(s for s in SIDES if s != side)
    if os.access(cfg["credentials"][other]["file"], os.R_OK) and not args.dev:
        raise CliError(f"the {side} executor can read the {other} credential file; run it as "
                       f"{cfg['credentials'][side]['os_user']}")
    creds = None
    if args.sim:
        from .executor.adapter.sim import SimAdapter

        adapter = SimAdapter(side, Path(args.sim))
    else:
        from .executor.adapter.starrocks import CredentialError, StarRocksAdapter

        cred_file = _private_file(cfg["credentials"][side]["file"], f"{side} credential file")
        creds = _read_json(cred_file, f"{side} credential file")
        try:
            adapter = StarRocksAdapter(side, cred_file,
                                       storage_patterns=cfg["error_classification"]["storage_suspect_patterns"],
                                       read_timeout=int(cfg["execution"]["read_timeout_seconds"]))
        except CredentialError as e:
            raise CliError(str(e)) from e
    token = _executor_token(args, creds)
    creds = None
    client = WorkerClient(cfg["executor"]["controller_url"], token)
    data_dir = Path(args.data_dir or cfg["executor"]["data_dir"]) / side
    data_dir.mkdir(parents=True, exist_ok=True)
    os.chmod(data_dir, 0o700)
    ex = Executor(cfg, side, adapter, client, data_dir)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: ex.request_stop())
    log.info("executor side=%s controller=%s data_dir=%s", side, cfg["executor"]["controller_url"], data_dir)
    ex.run_forever()


def cmd_worker_token(args) -> None:
    from .controller import queue

    ctx = make_ctx(load_config(args), max_connections=2)
    if args.action == "disable":
        _print(_api(queue.disable_worker, ctx, args.name, reason=args.reason))
        return
    out = _api(queue.create_worker, ctx, args.name, args.side, execution_location=args.location,
               rotate=args.rotate)
    token = out.pop("token")
    if args.out:
        fd = os.open(args.out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(token + "\n")
        out["token_file"] = args.out
        _print(out)
    else:
        _print(out)
        print("\nworker token (shown once, store it as \"worker_token\" in the side credential file):",
              file=sys.stderr)
        print(token)


def cmd_import(args) -> None:
    from .controller import importer

    ctx = make_ctx(load_config(args), max_connections=2)
    data = Path(args.file).read_bytes()
    name = Path(args.file).name
    if args.kind == "assets":
        pv = _api(importer.preview_assets, ctx, name, data, args.mode)
    elif args.kind == "metric":
        pv = _api(importer.preview_metric_rules, ctx, name, data)
    else:
        pv = _api(importer.preview_sample_rules, ctx, name, data)
    rows = pv.get("rows", [])
    for r in rows:
        if r.get("status") == "ERROR" or r.get("errors"):
            where = r.get("line") or r.get("lines")
            print(f"line {where}: {r.get('status')}: {'; '.join(r.get('errors') or [])}", file=sys.stderr)
    _print({"id": pv["id"], "status": pv["status"], "summary": pv.get("summary")})
    if args.confirm:
        if not args.reason:
            raise CliError("--confirm needs --reason")
        if (pv.get("summary") or {}).get("ERROR"):
            raise CliError(f"batch {pv['id']} has ERROR rows; fix the file and re-import (not confirmed)")
        _print(_api(importer.confirm_import, ctx, pv["id"], reason=args.reason))


def cmd_recovery(args) -> None:
    from .controller import operations

    ctx = make_ctx(load_config(args), max_connections=2)
    if args.action == "enter":
        _print(_api(operations.enter_recovery, ctx, args.reason))
        return
    pv = _api(operations.preview_operation, ctx, {"op": "CONFIRM_RECOVERY", "target_type": "GLOBAL",
                                                  "target_ids": []})
    _print(_api(operations.apply_operation, ctx, pv["id"], idempotency_key=pv["id"],
                if_match=pv["preview_digest"], reason=args.reason))


def cmd_tick(args) -> None:
    from .controller import engine

    ctx = make_ctx(load_config(args), max_connections=2)
    _print(engine.tick(ctx))


def cmd_report(args) -> None:
    from .controller import reports

    ctx = make_ctx(load_config(args), max_connections=2)
    if args.action == "stage":
        _print(_api(reports.generate_stage, ctx, args.start, args.end, args.reason))
    else:
        _print(_api(reports.regenerate_daily, ctx, args.window_key, args.reason))


def cmd_status(args) -> None:
    from .controller import views

    ctx = make_ctx(load_config(args), max_connections=2)
    d = views.dashboard(ctx)
    _print({"control": d["control"], "window": d["window"], "clusters": d["clusters"],
            "alerts": [{"severity": a["severity"], "kind": a["kind"], "message": a["message"]} for a in d["alerts"]],
            "active_runs": [{"id": r["id"], "plan": r.get("plan_name"), "state": r["execution_state"],
                             "progress": r["progress"]} for r in d["active_runs"]]})


# ---- parser --------------------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="srdq", description="StarRocks dual-cloud data verification")
    p.add_argument("--config", help=f"config file (default: $SRDQ_CONFIG or {DEFAULT_CONFIG})")
    p.add_argument("--log-level", default=os.environ.get("SRDQ_LOG_LEVEL", "INFO"))
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("check-config", help="validate the config and credential file permissions").set_defaults(
        fn=cmd_check_config)
    sub.add_parser("migrate", help="apply pending schema migrations").set_defaults(fn=cmd_migrate)
    for name, fn, help_ in (("controller", cmd_controller, "tick loop + executor-facing API (127.0.0.1:8471)"),
                            ("web", cmd_web, "dashboard and REST API (0.0.0.0:8470)")):
        sp = sub.add_parser(name, help=help_)
        sp.add_argument("--dev", action="store_true", help="skip the SR credential isolation check")
        sp.set_defaults(fn=fn)

    sp = sub.add_parser("executor", help="run one side's executor")
    sp.add_argument("--side", required=True, choices=SIDES)
    sp.add_argument("--token-file", help="0600 file holding the worker token (default: worker_token in the "
                                         "side credential file)")
    sp.add_argument("--data-dir", help="override executor.data_dir")
    sp.add_argument("--sim", metavar="DIR", help="use a simulated SR cluster stored in DIR (development only)")
    sp.add_argument("--dev", action="store_true", help="skip the cross-side credential isolation check")
    sp.set_defaults(fn=cmd_executor)

    sp = sub.add_parser("worker-token", help="create, rotate or disable an executor identity")
    wt = sp.add_subparsers(dest="action", required=True)
    c = wt.add_parser("create")
    c.add_argument("--name", required=True)
    c.add_argument("--side", required=True, choices=SIDES)
    c.add_argument("--location", default="ALIYUN_ECS", help="physical location (default ALIYUN_ECS)")
    c.add_argument("--rotate", action="store_true", help="issue a new token for an existing worker")
    c.add_argument("--out", help="write the token to this new 0600 file instead of stdout")
    d = wt.add_parser("disable")
    d.add_argument("--name", required=True)
    d.add_argument("--reason", required=True)
    sp.set_defaults(fn=cmd_worker_token)

    sp = sub.add_parser("import", help="preview (and optionally confirm) an asset list or rule template")
    sp.add_argument("kind", choices=("assets", "metric", "sample"))
    sp.add_argument("file")
    sp.add_argument("--mode", default="CREATE_OR_VALIDATE", choices=("CREATE_OR_VALIDATE", "UPDATE_EXPLICIT"))
    sp.add_argument("--confirm", action="store_true")
    sp.add_argument("--reason")
    sp.set_defaults(fn=cmd_import)

    sp = sub.add_parser("recovery", help="enter recovery after a restore, or confirm it")
    sp.add_argument("action", choices=("enter", "confirm"))
    sp.add_argument("--reason", required=True)
    sp.set_defaults(fn=cmd_recovery)

    sub.add_parser("tick", help="run one controller tick and print the result").set_defaults(fn=cmd_tick)
    sub.add_parser("status", help="print the dashboard summary").set_defaults(fn=cmd_status)

    sp = sub.add_parser("report", help="generate reports")
    rp = sp.add_subparsers(dest="action", required=True)
    s = rp.add_parser("stage")
    s.add_argument("--start", required=True, help="YYYY-MM-DD (window timezone)")
    s.add_argument("--end", required=True, help="YYYY-MM-DD, inclusive")
    s.add_argument("--reason", required=True)
    r = rp.add_parser("regenerate-daily")
    r.add_argument("window_key")
    r.add_argument("--reason", required=True)
    sp.set_defaults(fn=cmd_report)
    return p


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    args.fn(args)


if __name__ == "__main__":  # pragma: no cover
    main()
