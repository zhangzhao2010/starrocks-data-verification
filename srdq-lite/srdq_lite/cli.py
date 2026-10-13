"""Command line: `run` executes/resumes all stages, `report` rebuilds report files from recorded results."""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import shutil
import signal
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from . import __version__
from .compare import Results, build_outputs, derive_drills
from .config import Config, ConfigError, TableSpec, canonical_hash, load_config, load_tables
from .execute import run_tasks
from .meta import build_entry, check_connectivity, fetch_table_meta
from .plan import plan_main
from .sr import ConnPool, SRError
from .store import JsonlAppender, RunLock, read_json, read_jsonl, write_json, write_jsonl

log = logging.getLogger("srdq_lite")

GENERATED = ["01_connectivity.json", "02_metadata.jsonl", "03_tasks.jsonl", "04_results.jsonl",
             "05_compare.jsonl", "06_drill_tasks.jsonl", "07_drill_results.jsonl", "results", "summary.json",
             "table_summary.jsonl", "table_summary.csv", "diffs.jsonl", "diffs.csv", "failed_tasks.jsonl",
             "run.json"]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="srdq_lite", description="Offline StarRocks source/target data comparison")
    p.add_argument("--version", action="version", version=__version__)
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="run or resume a comparison")
    r.add_argument("-c", "--config", required=True, help="config.yaml")
    r.add_argument("-t", "--tables", required=True, help="tables.jsonl")
    r.add_argument("-o", "--out", required=True, help="output directory (one per process)")
    r.add_argument("--dry-run", action="store_true", help="stop after generating SQL (03_tasks.jsonl)")
    r.add_argument("--retry-failed", action="store_true", help="re-execute tasks recorded as FAILED")
    r.add_argument("--replan", action="store_true", help="archive existing results in this directory and start over")
    rep = sub.add_parser("report", help="rebuild report files from recorded results, without querying StarRocks")
    rep.add_argument("-o", "--out", required=True)
    args = p.parse_args(argv)

    out = Path(args.out).resolve()
    if args.cmd == "report" and not (out / "run.json").exists():
        print(f"{out} has no run.json", file=sys.stderr)
        return 2
    out.mkdir(parents=True, exist_ok=True)
    _setup_logging(out)
    signal.signal(signal.SIGTERM, _raise_interrupt)
    try:
        with RunLock(out):
            return cmd_run(args, out) if args.cmd == "run" else cmd_report(out)
    except (ConfigError, RuntimeError) as e:
        log.error("%s", e)
        return 2
    except KeyboardInterrupt:
        log.warning("interrupted; rerun the same command to resume")
        return 130


def _raise_interrupt(signum, frame) -> None:
    raise KeyboardInterrupt


def _setup_logging(out: Path) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    for h in (logging.StreamHandler(sys.stderr), logging.FileHandler(out / "run.log", encoding="utf-8")):
        h.setFormatter(fmt)
        log.addHandler(h)


def _init_run(out: Path, cfg: Config, specs: list[TableSpec], args) -> dict:
    path = out / "run.json"
    hashes = {"config_hash": canonical_hash(cfg.fingerprint()),
              "tables_hash": canonical_hash([s.to_dict() for s in specs])}
    if path.exists():
        meta = read_json(path)
        same = all(meta.get(k) == v for k, v in hashes.items())
        if same and not args.replan:
            log.info("resuming run %s (created %s)", meta["run_id"], meta["created_at"])
            return meta
        if not args.replan:
            changed = [k.split("_")[0] for k, v in hashes.items() if meta.get(k) != v]
            raise ConfigError(f"{' and '.join(changed)} changed since {out} was created; use a new -o directory, "
                              f"or --replan to archive the old results and start over")
        dest = out / "archive" / dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        dest.mkdir(parents=True)
        for name in GENERATED:
            if (out / name).exists():
                shutil.move(str(out / name), str(dest / name))
        log.info("archived previous results to %s", dest)
    meta = {"run_id": out.name, "tool_version": __version__,
            "created_at": dt.datetime.now().isoformat(timespec="seconds"), **hashes,
            "settings": cfg.fingerprint(), "config_file": str(Path(args.config).resolve()),
            "tables_file": str(Path(args.tables).resolve()), "table_ids": [s.id for s in specs]}
    write_json(path, meta)
    log.info("new run %s: %d tables", meta["run_id"], len(specs))
    return meta


def cmd_run(args, out: Path) -> int:
    started = time.time()
    cfg = load_config(args.config)
    specs = load_tables(args.tables)
    meta = _init_run(out, cfg, specs, args)

    conn_info = [check_connectivity(c, cfg.run, cfg.session) for c in (cfg.source, cfg.target)]
    write_json(out / "01_connectivity.json", conn_info)
    for c in conn_info:
        if c["ok"]:
            log.info("[connect] %s %s:%s ok, version %s", c["side"], c["host"], c["port"], c["version"])
        else:
            log.error("[connect] %s %s:%s FAILED: %s", c["side"], c["host"], c["port"], c["error"])
    if not all(c["ok"] for c in conn_info):
        return 2

    pools = {"source": ConnPool(cfg.source, cfg.run, cfg.session),
             "target": ConnPool(cfg.target, cfg.run, cfg.session)}
    try:
        entries = _stage_metadata(out, cfg, specs, pools)
        tasks = plan_main(entries, cfg.run)
        write_jsonl(out / "03_tasks.jsonl", tasks)
        per_rule = Counter(t["rule"] for t in tasks)
        log.info("[plan] %d tasks (%s); see 03_tasks.jsonl", len(tasks),
                 ", ".join(f"{k} {v}" for k, v in sorted(per_rule.items())))
        if args.dry_run:
            log.info("dry run: stopping before execution")
            return 0
        run_tasks(tasks, phase="main", results_path=out / "04_results.jsonl", results_dir=out / "results",
                  pools=pools, run=cfg.run, retry_failed=args.retry_failed)
        drills = derive_drills(entries, tasks, Results(out, out / "04_results.jsonl"))
        write_jsonl(out / "06_drill_tasks.jsonl", drills)
        run_tasks(drills, phase="drill", results_path=out / "07_drill_results.jsonl", results_dir=out / "results",
                  pools=pools, run=cfg.run, retry_failed=args.retry_failed)
    finally:
        for p in pools.values():
            p.close_all()
    return _report(out, meta, entries, tasks, conn_info, started)


def _stage_metadata(out: Path, cfg: Config, specs: list[TableSpec], pools: dict[str, ConnPool]) -> list[dict]:
    path = out / "02_metadata.jsonl"
    done = {e["table_id"]: e for e in read_jsonl(path)}
    todo = [s for s in specs if s.id not in done or done[s.id]["meta_status"] == "ERROR"]
    log.info("[metadata] %d tables, %d to fetch", len(specs), len(todo))

    def work(spec: TableSpec) -> dict:
        err = None
        for _ in range(cfg.run.retries + 1):
            try:
                src = fetch_table_meta(pools["source"].get(), spec.source_db, spec.source_table)
                tgt = fetch_table_meta(pools["target"].get(), spec.target_db, spec.target_table)
                return build_entry(spec, src, tgt)
            except SRError as e:
                pools["source"].discard()
                pools["target"].discard()
                err = str(e)
                if not e.retryable:
                    break
        return build_entry(spec, None, None, error=err)

    if todo:
        with JsonlAppender(path) as w, ThreadPoolExecutor(cfg.run.concurrency, thread_name_prefix="meta") as ex:
            for i, entry in enumerate(ex.map(work, todo), 1):
                w.write(entry)
                done[entry["table_id"]] = entry
                if entry["meta_status"] in ("ERROR", "NOT_FOUND", "BLOCKED"):
                    log.warning("[metadata] %s: %s", entry["table_id"], entry["meta_status"])
                if i % 50 == 0 or i == len(todo):
                    log.info("[metadata] progress %d/%d", i, len(todo))
    entries = [done[s.id] for s in specs]
    log.info("[metadata] %s", ", ".join(f"{k} {v}" for k, v in sorted(Counter(e["meta_status"] for e in entries).items())))
    return entries


def cmd_report(out: Path) -> int:
    meta = read_json(out / "run.json")
    done = {e["table_id"]: e for e in read_jsonl(out / "02_metadata.jsonl")}
    entries = [done[t] for t in meta["table_ids"] if t in done]
    tasks = read_jsonl(out / "03_tasks.jsonl")
    conn_info = read_json(out / "01_connectivity.json") if (out / "01_connectivity.json").exists() else []
    return _report(out, meta, entries, tasks, conn_info, time.time())


def _report(out: Path, meta: dict, entries: list[dict], tasks: list[dict], conn_info: list[dict],
            started: float) -> int:
    main_res = Results(out, out / "04_results.jsonl")
    drills = derive_drills(entries, tasks, main_res)
    drill_res = Results(out, out / "07_drill_results.jsonl")
    stats = build_outputs(out, meta["run_id"], entries, tasks, drills, main_res, drill_res,
                          meta["settings"]["run"]["max_diffs_per_rule"])
    task_states = {name: Counter(res.state(t)[0] for t in ts)
                   for name, ts, res in (("main", tasks, main_res), ("drill", drills, drill_res))}
    tables = stats["tables"]
    code = 2 if tables.get("ERROR") or stats["failed_tasks"] else 1 if tables.get("DIFF") or tables.get("BLOCKED") else 0
    sides = {c["side"]: c for c in conn_info}
    summary = {
        "run_id": meta["run_id"], "tool_version": __version__, "created_at": meta["created_at"],
        "generated_at": dt.datetime.now().isoformat(timespec="seconds"), "elapsed_s": round(time.time() - started, 1),
        "source": sides.get("source"), "target": sides.get("target"), "settings": meta["settings"],
        "config_hash": meta["config_hash"], "tables_hash": meta["tables_hash"],
        "tables": {"total": len(entries), **{k: tables.get(k, 0) for k in ("PASS", "DIFF", "ERROR", "BLOCKED",
                                                                             "SKIPPED")}},
        "meta_status": dict(Counter(e["meta_status"] for e in entries)),
        "tasks": {k: {"total": sum(v.values()), **v} for k, v in task_states.items()},
        "diffs_by_type": stats["diffs_by_type"], "diffs_written": stats["diffs_written"],
        "diffs_truncated": stats["diffs_truncated"], "failed_tasks": stats["failed_tasks"], "exit_code": code,
    }
    write_json(out / "summary.json", summary)
    t = summary["tables"]
    print(f"\nrun   : {out}  ({summary['elapsed_s']}s)", file=sys.stderr)
    print(f"tables: {t['total']}  PASS {t['PASS']} | DIFF {t['DIFF']} | ERROR {t['ERROR']} | "
          f"BLOCKED {t['BLOCKED']} | SKIPPED {t['SKIPPED']}", file=sys.stderr)
    for k, v in summary["tasks"].items():
        print(f"tasks : {k} {v['total']} (" + ", ".join(f"{s} {n}" for s, n in v.items() if s != "total") + ")",
              file=sys.stderr)
    if stats["diffs_by_type"]:
        print("diffs : " + ", ".join(f"{k} {v}" for k, v in sorted(stats["diffs_by_type"].items()))
              + (f"  ({stats['diffs_truncated']} not written, max_diffs_per_rule)" if stats["diffs_truncated"] else ""),
              file=sys.stderr)
    print(f"report: {out}/table_summary.csv, diffs.csv, summary.json (open viewer.html to browse)", file=sys.stderr)
    return code
