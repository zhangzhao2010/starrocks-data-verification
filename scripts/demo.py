"""Development demo: local PG (pgserver), two simulated SR clusters, and the four real srdq processes.

    uv run python scripts/demo.py [DIR]

Then open http://127.0.0.1:8470 (Ctrl-C stops everything). Simulated clusters only; never point this at production.
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

import httpx
import psycopg
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from srdq.executor.adapter.sim import SimCluster  # noqa: E402

ORDER_COLS = [("id", "bigint", False), ("name", "varchar(64)"), ("amt", "decimal(18,2)"), ("dt", "date"),
              ("cat", "varchar(8)")]
DT_PARTS = {"kind": "RANGE", "column": "dt",
            "partitions": [{"name": f"p2026100{d}", "lower": f"2026-10-0{d}", "upper": f"2026-10-0{d + 1}"}
                           for d in range(1, 8)]}
ASSETS = """layer,source_db,source_table,target_db,target_table,priority,enabled
ODS,ods,orders,ods,orders,80,true
ODS,ods,customers,ods,customers,50,true
DWD,dwd,order_detail,dwd,order_detail,50,true
"""
METRIC = """source_db,source_table,rule_key,group_by,filter,metric_id,function,column,scale,compare,rtol,atol
ods,orders,amt_by_cat,cat,,amt_sum,SUM,amt,2,EXACT,,
ods,orders,amt_by_cat,cat,,id_cnt,COUNT_NONNULL,id,,EXACT,,
"""
SAMPLE = """source_db,source_table,key_column,compare_columns,float_rtol,float_atol
ods,orders,id,*,,
ods,customers,cid,*,,
"""


def rows(n: int, *, drop=(), bump=()):
    out = []
    for i in range(1, n + 1):
        if i in drop:
            continue
        amt = Decimal(i) / 4 + (Decimal("0.01") if i in bump else 0)
        out.append({"id": i, "name": f"n{i}", "amt": amt, "dt": f"2026-10-0{(i % 7) + 1}", "cat": "abc"[i % 3]})
    return out


def build_clusters(base: Path) -> None:
    for side in ("huawei", "aliyun"):
        c = SimCluster(base / f"sr_{side}")
        src = side == "huawei"
        c.create_table("ods", "orders", ORDER_COLS, model="PRI_KEYS", keys=["id"], partition=DT_PARTS)
        c.insert("ods", "orders", rows(2000) if src else rows(2000, drop=(17,), bump=(1001,)))
        c.create_table("ods", "customers", [("cid", "varchar(32)", False), ("city", "varchar(32)")],
                       model="UNQ_KEYS", keys=["cid"])
        c.insert("ods", "customers", [{"cid": f"C{i:05d}", "city": "hz" if i % 2 else "sh"} for i in range(500)])
        c.create_table("dwd", "order_detail", ORDER_COLS, partition=DT_PARTS)
        c.insert("dwd", "order_detail", rows(700))


def main() -> None:
    import pgserver

    base = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/srdq-demo").resolve()
    base.mkdir(parents=True, exist_ok=True)
    os.chmod(base, 0o700)
    srv = pgserver.get_server(str(base / "pg"), cleanup_mode="stop")
    uri = srv.get_uri()
    with psycopg.connect(uri, autocommit=True) as conn:
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname='srdq_demo'").fetchone():
            conn.execute("CREATE DATABASE srdq_demo")
    dsn = uri.replace("/postgres?", "/srdq_demo?")

    cfg = {
        "spec_version": "srdq-config-v1.3", "run_enabled": True,
        "execution_window": {"timezone": "Asia/Shanghai", "open_at": "00:00", "drain_at": "23:55",
                             "close_at": "23:59"},
        "transfer": {"global_approval_ref": "DEMO-APPROVAL"},
        "execution": {"retry_backoff_seconds": 5},
        "report": {"dir": str(base / "reports")},
        "credentials": {s: {"file": str(base / f"no-{s}.json")} for s in ("huawei", "aliyun")}
        | {"controller_pg": {"file": str(base / "pg.json")}},
        "controller": {"data_dir": str(base / "controller"), "tick_seconds": 1},
        "executor": {"data_dir": str(base / "executor"), "poll_seconds": 1},
    }
    (base / "config.yaml").write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    pg_file = base / "pg.json"
    pg_file.unlink(missing_ok=True)
    fd = os.open(pg_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump({"dsn": dsn}, f)
    env = dict(os.environ, SRDQ_CONFIG=str(base / "config.yaml"))
    srdq = [sys.executable, "-m", "srdq.cli"]
    first = not (base / "sr_huawei").exists()
    if first:
        build_clusters(base)
    subprocess.run(srdq + ["migrate"], env=env, check=True)
    for side in ("huawei", "aliyun"):
        tok = base / f"token-{side}"
        if not tok.exists():
            subprocess.run(srdq + ["worker-token", "create", "--name", f"demo-{side}", "--side", side,
                                   "--out", str(tok)], env=env, check=True, stdout=subprocess.DEVNULL)
    for name, text in (("assets.csv", ASSETS), ("metric_rules.csv", METRIC), ("sample_rules.csv", SAMPLE)):
        (base / name).write_text(text, encoding="utf-8")
    if first:
        subprocess.run(srdq + ["import", "assets", str(base / "assets.csv"), "--confirm", "--reason", "demo"],
                       env=env, check=True, stdout=subprocess.DEVNULL)

    procs = [subprocess.Popen(srdq + ["controller", "--dev"], env=env),
             subprocess.Popen(srdq + ["web", "--dev"], env=env)]
    for side in ("huawei", "aliyun"):
        procs.append(subprocess.Popen(srdq + ["executor", "--side", side, "--sim", str(base / f"sr_{side}"),
                                              "--token-file", str(base / f"token-{side}"), "--dev"], env=env))
    for _ in range(100):
        try:
            if httpx.get("http://127.0.0.1:8470/v1/health", timeout=1).status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.2)
    print(f"\nsrdq demo is up: http://127.0.0.1:8470   (data in {base}; rule templates: "
          f"{base / 'metric_rules.csv'}, {base / 'sample_rules.csv'})\n"
          "next: 范围清单 → 发现分区 → 确认；计划 → 新建 → 开始运行。Ctrl-C to stop.\n", flush=True)
    try:
        while all(p.poll() is None for p in procs):
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(30)
            except subprocess.TimeoutExpired:
                p.kill()


if __name__ == "__main__":
    main()
