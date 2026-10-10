"""In-process harness: controller ctx on a test PG, two simulated SR clusters, executors over the worker API."""
from __future__ import annotations

import datetime as _dt
import time
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

from starlette.testclient import TestClient

from srdq import config
from srdq.controller import engine, evaluate, importer, manifest, planning, queue
from srdq.controller.common import Ctx, FixedClock, ensure_control
from srdq.controller.worker_api import create_app
from srdq.executor.adapter.sim import SimAdapter
from srdq.executor.client import WorkerClient
from srdq.executor.runner import Executor

TZ = ZoneInfo("Asia/Shanghai")
ASSET_HEADER = "layer,source_db,source_table,target_db,target_table,priority,enabled\n"


def _hhmm(t: _dt.datetime) -> str:
    return t.astimezone(TZ).strftime("%H:%M")


def window_cfg(now: _dt.datetime, *, open_before_min: int = 60, drain_after_min: int = 240,
               close_after_min: int = 300) -> dict:
    return {"timezone": "Asia/Shanghai", "open_at": _hhmm(now - _dt.timedelta(minutes=open_before_min)),
            "drain_at": _hhmm(now + _dt.timedelta(minutes=drain_after_min)),
            "close_at": _hhmm(now + _dt.timedelta(minutes=close_after_min))}


class Harness:
    def __init__(self, db, root: Path, overrides: dict | None = None):
        self.root = Path(root)
        now = _dt.datetime.now(_dt.timezone.utc).replace(second=30, microsecond=0)
        self.clock = FixedClock(now)
        data = {"transfer": {"global_approval_ref": "APPROVAL-1"}, "run_enabled": True,
                "controller": {"data_dir": str(self.root / "ctl")},
                "report": {"dir": str(self.root / "reports")},
                "execution_window": window_cfg(now),
                "execution": {"retry_backoff_seconds": 1}}
        for k, v in (overrides or {}).items():
            data[k] = {**data.get(k, {}), **v} if isinstance(v, dict) else v
        self.cfg = config.from_dict(data)
        self.ctx = Ctx(self.cfg, db, self.clock)
        ensure_control(self.ctx)
        self.app = create_app(self.ctx)
        self.adapters = {s: SimAdapter(s, self.root / f"sr_{s}") for s in ("huawei", "aliyun")}
        self.executors: dict[str, Executor] = {}
        for side in ("huawei", "aliyun"):
            w = queue.create_worker(self.ctx, f"w-{side}", side)
            client = WorkerClient("http://testserver", w["token"])
            client._http = TestClient(self.app, base_url="http://testserver",
                                      headers={"Authorization": f"Bearer {w['token']}"})
            ex = Executor(self.cfg, side, self.adapters[side], client, self.root / f"ex_{side}")
            ex.poll_seconds = 0.05
            self.executors[side] = ex
            ex.register()

    def close(self) -> None:
        for ex in self.executors.values():
            ex._pool.shutdown(wait=True)
            ex.outbox.close()
            ex.client.close()

    # ---- SR fixtures -------------------------------------------------------------
    def table(self, side: str, db: str, tbl: str, columns: list[tuple], rows: list[dict], **kw) -> None:
        cl = self.adapters[side].cluster
        cl.create_table(db, tbl, columns, **kw)
        cl.insert(db, tbl, rows)

    def both(self, db: str, tbl: str, columns: list[tuple], rows: list[dict], *, target_rows=None, **kw) -> None:
        self.table("huawei", db, tbl, columns, rows, **kw)
        self.table("aliyun", db, tbl, columns, rows if target_rows is None else target_rows, **kw)

    # ---- driving -------------------------------------------------------------------
    def tick(self) -> dict:
        return engine.tick(self.ctx)

    def work(self, side: str | None = None, rounds: int = 50) -> int:
        """Claim and execute until neither side gets new work. Returns the number of claimed attempts."""
        total = 0
        sides = [side] if side else list(self.executors)
        for _ in range(rounds):
            self.tick()
            got = 0
            for s in sides:
                got += self.executors[s].claim_once()
            self.wait_idle()
            total += got
            if got == 0:
                self.tick()
                if not any(self.executors[s].claim_once() for s in sides):
                    self.wait_idle()
                    break
                self.wait_idle()
        return total

    def wait_idle(self, timeout: float = 30) -> None:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if all(not ex.active for ex in self.executors.values()):
                return
            time.sleep(0.01)
        raise AssertionError("executors did not become idle")

    def heartbeat(self) -> None:
        for ex in self.executors.values():
            ex.heartbeat_once()

    # ---- setup flows -----------------------------------------------------------------
    def import_assets(self, lines: list[str]) -> dict:
        data = (ASSET_HEADER + "\n".join(lines) + "\n").encode("utf-8")
        pv = importer.preview_assets(self.ctx, "assets.csv", data)
        return importer.confirm_import(self.ctx, pv["id"], reason="test import")

    def discover_and_confirm(self) -> dict:
        d = manifest.discover(self.ctx, reason="test discovery")
        self.work()
        for _ in range(20):
            self.tick()
            m = self.ctx.db.one("SELECT * FROM dq_manifest ORDER BY version DESC LIMIT 1")
            if m is not None:
                break
        assert m is not None, "no manifest drafted"
        if m["status"] == "DRAFT":
            manifest.confirm_manifest(self.ctx, m["id"], reason="test confirm")
        return {"discovery": d, "manifest_id": str(m["id"])}

    def import_rules(self, kind: str, header: list[str], lines: list[str], approve: bool = True) -> dict:
        data = (",".join(header) + "\n" + "\n".join(lines) + "\n").encode("utf-8")
        fn = importer.preview_metric_rules if kind == "METRIC" else importer.preview_sample_rules
        pv = fn(self.ctx, f"{kind.lower()}.csv", data)
        res = importer.confirm_import(self.ctx, pv["id"], reason="test rules")
        if approve:
            ids = [r["id"] for r in self.ctx.db.query(
                "SELECT id FROM dq_check_rule WHERE status='DRAFT' AND check_kind=%s AND support_state='SUPPORTED'",
                (kind,))]
            if ids:
                importer.approve_rules(self.ctx, ids, reason="test approve")
        return {"preview": pv, "confirm": res}

    def plan(self, name: str = "nightly", kinds: list[str] | None = None) -> dict:
        return planning.create_plan(self.ctx, {"name": name, "check_selector": {"kinds": kinds or ["COUNT"]},
                                               "asset_selector": {"all": True}}, reason="test plan")

    def run_plan(self, plan_id: str, **kw) -> dict:
        r = planning.start_plan_run(self.ctx, plan_id, reason="test run", **kw)
        self.work()
        return r

    def run_row(self, run_id) -> dict:
        return self.ctx.db.one("SELECT * FROM dq_run WHERE id=%s", (run_id,))

    def checks(self, run_id) -> list[dict]:
        return self.ctx.db.query("""SELECT rc.*, a.source_table FROM dq_run_check rc JOIN dq_asset a ON a.id=rc.asset_id
                                    WHERE rc.run_id=%s ORDER BY a.source_table, rc.check_kind, rc.scope_key""",
                                 (run_id,))

    def evaluate_open(self) -> None:
        for o in self.ctx.db.query("SELECT id FROM dq_observation WHERE status='OPEN'"):
            evaluate.evaluate_observation(self.ctx, o["id"])


def order_rows(n: int, mutate=None) -> list[dict]:
    out = []
    for i in range(1, n + 1):
        r = {"id": i, "name": f"n{i}", "amt": Decimal(i) / 4, "dt": f"2026-10-{(i % 3) + 1:02d}", "cat": "ab"[i % 2]}
        if mutate:
            r = mutate(r)
            if r is None:
                continue
        out.append(r)
    return out


ORDER_COLS = [("id", "bigint", False), ("name", "varchar(64)"), ("amt", "decimal(18,2)"), ("dt", "date"),
              ("cat", "varchar(8)")]
DT_PARTITIONS = {"kind": "RANGE", "column": "dt",
                 "partitions": [{"name": f"p{d}", "lower": f"2026-10-0{d}", "upper": f"2026-10-0{d + 1}"}
                                for d in (1, 2, 3)]}
