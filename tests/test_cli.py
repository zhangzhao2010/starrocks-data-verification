from __future__ import annotations

import json
import os
import threading
import time

import pytest
import yaml

from srdq import cli, config
from srdq.executor.adapter.sim import SimAdapter
from srdq.executor.client import WorkerClient
from srdq.executor.runner import Executor


def _write(path, text, mode):
    path.write_text(text, encoding="utf-8")
    os.chmod(path, mode)
    return path


def _cfg(tmp_path, **creds):
    files = {"huawei": tmp_path / "hw.json", "aliyun": tmp_path / "al.json", "controller_pg": tmp_path / "pg.json"}
    files.update(creds)
    return config.from_dict({"credentials": {k: {"file": str(v)} for k, v in files.items()}})


def test_private_file(tmp_path):
    with pytest.raises(cli.CliError, match="does not exist"):
        cli._private_file(tmp_path / "nope", "f")
    p = _write(tmp_path / "s.json", "{}", 0o644)
    with pytest.raises(cli.CliError, match="0600"):
        cli._private_file(p, "f")
    os.chmod(p, 0o600)
    assert cli._private_file(p, "f") == p
    with pytest.raises(cli.CliError, match="regular file"):
        cli._private_file(tmp_path, "f")


def test_pg_dsn_from_credential_file(tmp_path, monkeypatch):
    monkeypatch.delenv("SRDQ_PG_DSN", raising=False)
    cfg = _cfg(tmp_path)
    _write(tmp_path / "pg.json", json.dumps({"dbname": "srdq", "user": "srdq", "password": "p w"}), 0o600)
    dsn = cli.pg_dsn(cfg)
    assert "dbname=srdq" in dsn and "host=127.0.0.1" in dsn and "application_name=srdq" in dsn
    _write(tmp_path / "pg.json", json.dumps({"dbname": "x", "user": "y", "options": "-c x"}), 0o600)
    with pytest.raises(cli.CliError, match="unknown keys"):
        cli.pg_dsn(cfg)
    os.chmod(tmp_path / "pg.json", 0o640)
    with pytest.raises(cli.CliError, match="0600"):
        cli.pg_dsn(cfg)
    monkeypatch.setenv("SRDQ_PG_DSN", "dbname=dev")
    assert cli.pg_dsn(cfg) == "dbname=dev"


def test_refuse_sr_credentials(tmp_path):
    cfg = _cfg(tmp_path)
    cli.refuse_sr_credentials(cfg, dev=False)
    _write(tmp_path / "al.json", "{}", 0o600)
    with pytest.raises(cli.CliError, match="aliyun"):
        cli.refuse_sr_credentials(cfg, dev=False)
    cli.refuse_sr_credentials(cfg, dev=True)


def test_import_refuses_to_confirm_error_rows(tmp_path, pg_dsn, monkeypatch, capsys):
    monkeypatch.setenv("SRDQ_PG_DSN", pg_dsn)
    cfgfile = tmp_path / "config.yaml"
    cfgfile.write_text(yaml.safe_dump({"controller": {"data_dir": str(tmp_path / "ctl")}}), encoding="utf-8")
    bad = _write(tmp_path / "a.csv", "layer,source_db,source_table,target_db,target_table,priority,enabled\n"
                                     "NOPE,db1,t,db1,t,50,true\n", 0o644)
    with pytest.raises(cli.CliError, match="ERROR rows"):
        cli.main(["--config", str(cfgfile), "import", "assets", str(bad), "--confirm", "--reason", "r"])
    good = _write(tmp_path / "b.csv", "layer,source_db,source_table,target_db,target_table,priority,enabled\n"
                                      "ODS,db1,t,db1,t,50,true\n", 0o644)
    cli.main(["--config", str(cfgfile), "import", "assets", str(good), "--confirm", "--reason", "r"])
    assert '"CONFIRMED"' in capsys.readouterr().out


def test_executor_waits_for_unreachable_controller(tmp_path, caplog):
    cfg = config.from_dict({})
    client = WorkerClient("http://127.0.0.1:9", "tok", timeout=1)
    ex = Executor(cfg, "huawei", SimAdapter("huawei", tmp_path / "sr"), client, tmp_path / "ex")
    t = threading.Thread(target=ex.run_forever)
    with caplog.at_level("WARNING", logger="srdq.executor"):
        t.start()
        time.sleep(1.5)
        assert t.is_alive(), "an unreachable controller must not crash the executor"
        ex.request_stop()
        t.join(10)
    assert not t.is_alive()
    assert any("controller unreachable" in r.getMessage() for r in caplog.records)
