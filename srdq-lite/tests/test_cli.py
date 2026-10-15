import datetime as dt
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from srdq_lite import cli
from srdq_lite.store import read_json, read_jsonl, write_json, write_jsonl

STARTED = dt.datetime(2026, 1, 1, 1, 30).timestamp() + 0.875


@pytest.fixture
def dry_run(tmp_path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("source: {host: source-fe, user: reader, password: fake}\n"
                      "target: {host: target-fe, user: reader, password: fake}\n"
                      "run: {concurrency: 1, retries: 0}\n", encoding="utf-8")
    tables = tmp_path / "tables.jsonl"
    write_jsonl(tables, [{"source_db": "s", "source_table": table,
                         "target_db": "t", "target_table": table,
                         "rules": [{"rule_name": "ROW_COUNT"}]}
                        for table in ("timed", "plain")])
    out = tmp_path / "out"
    argv = ["run", "-c", str(config), "-t", str(tables), "-o", str(out), "--dry-run"]
    clock = {"now": STARTED}
    pools = []
    real_load = cli.load_config

    def slow_load(path):
        clock["now"] += 2 * 3600
        return real_load(path)

    class FakePool:
        def __init__(self, cluster, run, session):
            self.side, self.closed = cluster.side, False
            pools.append(self)

        def get(self):
            return self.side

        def close_all(self):
            self.closed = True

    def fetch(conn, db, table):
        clock["now"] += 3600
        columns = [{"name": "id", "data_type": "bigint", "column_type": "bigint",
                    "category": "int", "nullable": False}]
        if table == "timed":
            columns.append({"name": "sharding_time", "data_type": "datetime",
                            "column_type": "datetime", "category": "datetime", "nullable": False})
        return {"exists": True, "model": "PRIMARY_KEYS", "keys": ["id"], "buckets": 1,
                "columns": columns, "partition_columns": [], "partitions": [], "rows": 10}

    fetch_mock = Mock(side_effect=fetch)
    execute = Mock(side_effect=AssertionError("dry-run must not execute queries"))
    monkeypatch.setattr(cli.time, "time", lambda: clock["now"])
    monkeypatch.setattr(cli, "load_config", slow_load)
    monkeypatch.setattr(cli, "_setup_logging", Mock())
    monkeypatch.setattr(cli.signal, "signal", Mock())
    monkeypatch.setattr(cli, "check_connectivity", lambda cluster, run, session:
                        {"side": cluster.side, **cluster.describe(), "ok": True, "version": "fake"})
    monkeypatch.setattr(cli, "ConnPool", FakePool)
    monkeypatch.setattr(cli, "fetch_table_meta", fetch_mock)
    monkeypatch.setattr(cli, "run_tasks", execute)
    yield SimpleNamespace(argv=argv, out=out, clock=clock, fetch=fetch_mock, pools=pools)
    execute.assert_not_called()
    assert all(pool.closed for pool in pools)


@pytest.mark.parametrize("wall_time", [dt.datetime(2026, 5, 2, 12), dt.datetime(2026, 1, 1, 1, 30)],
                         ids=["same-day", "previous-year"])
def test_dry_run_uses_start_time_before_slow_preparation(dry_run, wall_time):
    started = wall_time.timestamp() + 0.875
    dry_run.clock["now"] = started
    assert cli.main(dry_run.argv) == 0
    meta = read_json(dry_run.out / "run.json")
    cutoff = (dt.datetime.fromtimestamp(int(started), dt.timezone.utc)
              - dt.timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
    assert meta["created_at"] == wall_time.isoformat(timespec="seconds")
    assert meta["sharding_time_cutoff_utc"] == cutoff
    assert dry_run.clock["now"] > started + 3 * 3600
    assert sorted(call.args for call in dry_run.fetch.call_args_list) == [
        ("source", "s", "plain"), ("source", "s", "timed"),
        ("target", "t", "plain"), ("target", "t", "timed")]
    tasks = read_jsonl(dry_run.out / "03_tasks.jsonl")
    assert len(tasks) == 2
    for task in tasks:
        for side in ("source", "target"):
            sql = task[f"{side}_sql"]
            if "timed" in task["table_id"]:
                assert f"`sharding_time` < '{cutoff}'" in sql
            else:
                assert "sharding_time" not in sql
    assert not (dry_run.out / "04_results.jsonl").exists()


def test_cutoff_is_utc_regardless_of_beijing_wall_clock(dry_run):
    beijing = dt.timezone(dt.timedelta(hours=8))
    dry_run.clock["now"] = dt.datetime(2026, 10, 15, 15, 0, 30, tzinfo=beijing).timestamp()
    assert cli.main(dry_run.argv) == 0
    assert read_json(dry_run.out / "run.json")["sharding_time_cutoff_utc"] == "2026-10-15 04:00:30"
    timed = next(t for t in read_jsonl(dry_run.out / "03_tasks.jsonl") if "timed" in t["table_id"])
    assert "WHERE (`sharding_time` < '2026-10-15 04:00:30')" in timed["source_sql"]


def test_resume_keeps_original_cutoff_and_metadata_cache(dry_run):
    assert cli.main(dry_run.argv) == 0
    names = ["run.json", "02_metadata.jsonl", "03_tasks.jsonl"]
    original = {name: (dry_run.out / name).read_bytes() for name in names}
    dry_run.clock["now"] = STARTED + 86400
    dry_run.fetch.reset_mock()
    assert cli.main(dry_run.argv) == 0
    dry_run.fetch.assert_not_called()
    assert {name: (dry_run.out / name).read_bytes() for name in names} == original


def test_legacy_run_requires_explicit_replan_before_database_access(dry_run, monkeypatch, caplog):
    assert cli.main(dry_run.argv) == 0
    meta = read_json(dry_run.out / "run.json")
    meta["sharding_time_cutoff"] = meta.pop("sharding_time_cutoff_utc")
    write_json(dry_run.out / "run.json", meta)
    names = ["run.json", "01_connectivity.json", "02_metadata.jsonl", "03_tasks.jsonl"]
    original = {name: (dry_run.out / name).read_bytes() for name in names}
    connectivity = Mock(side_effect=AssertionError("legacy run must fail before connecting"))
    monkeypatch.setattr(cli, "check_connectivity", connectivity)
    dry_run.fetch.reset_mock()
    dry_run.clock["now"] = STARTED + 86400
    assert cli.main(dry_run.argv) == 2
    connectivity.assert_not_called()
    dry_run.fetch.assert_not_called()
    assert "cutoff" in caplog.text and "new -o" in caplog.text and "--replan" in caplog.text
    assert {name: (dry_run.out / name).read_bytes() for name in names} == original
    assert not (dry_run.out / "archive").exists()


@pytest.mark.parametrize("legacy", [False, True], ids=["current", "legacy"])
def test_replan_archives_previous_outputs_and_uses_new_start(dry_run, legacy):
    assert cli.main(dry_run.argv) == 0
    old_meta = read_json(dry_run.out / "run.json")
    if legacy:
        old_meta["sharding_time_cutoff"] = old_meta.pop("sharding_time_cutoff_utc")
        write_json(dry_run.out / "run.json", old_meta)
    write_jsonl(dry_run.out / "04_results.jsonl", [{"task_id": "old", "status": "SUCCESS"}])
    names = ["run.json", "02_metadata.jsonl", "03_tasks.jsonl", "04_results.jsonl"]
    original = {name: (dry_run.out / name).read_bytes() for name in names}
    started = STARTED + 86400
    dry_run.clock["now"] = started
    dry_run.fetch.reset_mock()
    assert cli.main([*dry_run.argv, "--replan"]) == 0
    archives = list((dry_run.out / "archive").iterdir())
    assert len(archives) == 1
    assert {name: (archives[0] / name).read_bytes() for name in names} == original
    meta = read_json(dry_run.out / "run.json")
    assert meta["created_at"] == dt.datetime.fromtimestamp(started).isoformat(timespec="seconds")
    assert meta["sharding_time_cutoff_utc"] == (dt.datetime.fromtimestamp(int(started), dt.timezone.utc)
                                                - dt.timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
    assert dry_run.fetch.call_count == 4
    assert (dry_run.out / "03_tasks.jsonl").read_bytes() != original["03_tasks.jsonl"]
    assert not (dry_run.out / "04_results.jsonl").exists()
