import json

import pytest

from srdq_lite.config import ConfigError, load_config, load_tables


def test_load_config_from_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("SRC_PASS", "source-secret")
    path = tmp_path / "config.yaml"
    path.write_text("""
source: {host: source-fe, user: reader, password_env: SRC_PASS}
target: {host: target-fe, port: 9031, user: reader, password: target-secret}
run: {concurrency: 8, retries: 0}
session: {time_zone: Asia/Shanghai}
""")
    cfg = load_config(path)
    assert cfg.source.password == "source-secret"
    assert cfg.target.port == 9031
    assert cfg.run.concurrency == 8
    assert cfg.run.retries == 0


def test_config_rejects_unknown_key(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("source: {host: a, user: u, password: p, typo: 1}\ntarget: {host: b, user: u, password: p}\n")
    with pytest.raises(ConfigError, match=r"source: unknown field\(s\) typo"):
        load_config(path)


def test_tables_normalizes_rules_mapping_and_filter(tmp_path):
    path = tmp_path / "tables.jsonl"
    row = {"source_db": "s", "source_table": "a", "target_db": "t", "target_table": "b",
           "column_mapping": {"OldName": "new_name"},
           "rule": {"rule_name": "sample", "config": {"ratio": 0.5, "key": "id",
                                                          "filter": {"source": "x=1", "target": "x=2"}}}}
    path.write_text(json.dumps(row) + "\n")
    spec = load_tables(path)[0]
    assert spec.column_mapping == {"oldname": "new_name"}
    assert spec.rules[0].name == "SAMPLE"
    assert spec.rules[0].config["key"] == ["id"]
    assert spec.rules[0].config["filter"] == {"source": "x=1", "target": "x=2"}


def test_metric_allows_group_by_without_explicit_metrics(tmp_path):
    path = tmp_path / "tables.jsonl"
    row = {"source_db": "s", "source_table": "a", "target_db": "t", "target_table": "b",
           "rules": [{"rule_name": "METRIC", "config": {"group_by": ["status"]}}]}
    path.write_text(json.dumps(row) + "\n")
    config = load_tables(path)[0].rules[0].config
    assert config["metrics"] == []
    assert config["group_by"] == ["status"]


def test_tables_rejects_duplicate_pairs(tmp_path):
    row = {"source_db": "s", "source_table": "a", "target_db": "t", "target_table": "b",
           "rules": [{"rule_name": "ROW_COUNT"}]}
    path = tmp_path / "tables.jsonl"
    path.write_text(json.dumps(row) + "\n" + json.dumps(row) + "\n")
    with pytest.raises(ConfigError, match="duplicate table pair"):
        load_tables(path)
