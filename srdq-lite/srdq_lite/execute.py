"""Stages 4/5: run SQL tasks on both clusters concurrently, stream results to gzip files, resume-safe."""
from __future__ import annotations

import datetime as dt
import decimal
import gzip
import logging
import os
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from pathlib import Path
from typing import Any

from .config import RunSettings
from .plan import SIDES, task_sides
from .sr import ConnPool, SRError
from .store import JsonlAppender, dumps, read_jsonl

log = logging.getLogger("srdq_lite")


def canon(v: Any) -> Any:
    if v is None or isinstance(v, (str, int)):
        return v
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, decimal.Decimal):
        return str(v)
    if isinstance(v, dt.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, (dt.date, dt.time)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    return str(v)


def result_file(results_dir: Path, task_id: str, side: str) -> Path:
    return results_dir / f"{task_id}.{side}.jsonl.gz"


def latest_records(path: Path) -> dict[tuple[str, str], dict]:
    return {(r["task_id"], r["side"]): r for r in read_jsonl(path)}


class _Abort(Exception):
    pass


def run_tasks(tasks: list[dict], *, phase: str, results_path: Path, results_dir: Path,
              pools: dict[str, ConnPool], run: RunSettings, retry_failed: bool) -> None:
    results_dir.mkdir(parents=True, exist_ok=True)
    done = latest_records(results_path)
    pending: list[tuple[dict, str]] = []
    total_sides = 0
    for t in tasks:
        for side in task_sides(t):
            total_sides += 1
            rec = done.get((t["task_id"], side))
            if rec and rec["status"] == "OK" and (results_dir.parent / rec["file"]).exists():
                continue
            if rec and rec["status"] == "FAILED" and not retry_failed:
                continue
            pending.append((t, side))
    skipped = total_sides - len(pending)
    log.info("[%s] %d tasks, %d side-runs pending, %d already recorded", phase, len(tasks), len(pending), skipped)
    if not pending:
        return

    stop = threading.Event()
    active: dict[int, tuple[str, int]] = {}
    active_lock = threading.Lock()

    def one(task: dict, side: str) -> dict | None:
        if stop.is_set():
            return None
        pool = pools[side]
        final = result_file(results_dir, task["task_id"], side)
        tmp = final.with_name(final.name + ".tmp")
        t0 = time.time()
        attempts = 0
        while True:
            attempts += 1
            rows = 0
            gen = None
            try:
                conn = pool.get()
                with active_lock:
                    active[threading.get_ident()] = (side, conn.connection_id)
                with gzip.open(tmp, "wt", encoding="utf-8", compresslevel=3) as f:
                    gen = conn.stream(task[f"{side}_sql"])
                    for row in gen:
                        rows += 1
                        if task.get("max_rows") and rows > task["max_rows"]:
                            raise _Abort(f"result exceeds {task['max_rows']} rows ({task.get('max_rows_note')})")
                        f.write(dumps([canon(v) for v in row]))
                        f.write("\n")
                os.replace(tmp, final)
                return _record(task, side, phase, "OK", attempts, rows, t0, None,
                               str(final.relative_to(results_dir.parent)))
            except _Abort as e:
                pool.discard()
                _close_quietly(gen)
                return _record(task, side, phase, "FAILED", attempts, rows, t0, str(e), None)
            except SRError as e:
                pool.discard()
                _close_quietly(gen)
                if stop.is_set():
                    return None
                if not e.retryable or attempts > run.retries:
                    return _record(task, side, phase, "FAILED", attempts, rows, t0, str(e), None)
                log.warning("[%s] %s %s attempt %d failed, retrying: %s", phase, task["task_id"], side, attempts, e)
                if stop.wait(min(30, 2 ** attempts)):
                    return None
            finally:
                with active_lock:
                    active.pop(threading.get_ident(), None)

    executors = {side: ThreadPoolExecutor(run.concurrency, thread_name_prefix=f"{phase}-{side}") for side in SIDES}
    futures: set[Future] = {executors[side].submit(one, t, side) for t, side in pending}
    total, ok, failed = len(pending), 0, 0
    last_log = time.time()
    try:
        with JsonlAppender(results_path) as out:
            while futures:
                finished, futures = wait(futures, timeout=5, return_when=FIRST_COMPLETED)
                for fut in finished:
                    rec = fut.result()
                    if rec is None:
                        continue
                    out.write(rec)
                    if rec["status"] == "OK":
                        ok += 1
                    else:
                        failed += 1
                        log.error("[%s] %s %s %s FAILED: %s", phase, rec["table_id"], rec["rule_key"], rec["side"],
                                  rec["error"])
                if time.time() - last_log >= 10 or not futures:
                    log.info("[%s] progress %d/%d (ok %d, failed %d)", phase, ok + failed, total, ok, failed)
                    last_log = time.time()
    except KeyboardInterrupt:
        log.warning("[%s] interrupted, cancelling running queries; finished results are kept", phase)
        stop.set()
        for ex in executors.values():
            ex.shutdown(wait=False, cancel_futures=True)
        with active_lock:
            running = list(active.values())
        for side, cid in running:
            pools[side].kill(cid)
        raise
    finally:
        for ex in executors.values():
            ex.shutdown(wait=True, cancel_futures=True)


def _close_quietly(gen) -> None:
    if gen is not None:
        try:
            gen.close()
        except Exception:
            pass


def _record(task: dict, side: str, phase: str, status: str, attempts: int, rows: int, t0: float,
            error: str | None, file: str | None) -> dict:
    return {"task_id": task["task_id"], "side": side, "phase": phase, "table_id": task["table_id"],
            "rule_key": task["rule_key"], "status": status, "attempts": attempts, "rows": rows,
            "elapsed_s": round(time.time() - t0, 3), "error": error, "file": file,
            "finished_at": dt.datetime.now().isoformat(timespec="seconds")}
