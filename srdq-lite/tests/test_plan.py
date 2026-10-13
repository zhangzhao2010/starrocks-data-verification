from srdq_lite.config import RunSettings
from srdq_lite.plan import (batch_partitions, metric_sql, plan_lookups, plan_main, plan_picks, row_count_sql,
                            sample_lookup_sql, sample_pick_sql, sample_size, sql_literal)


ENTRY = {"source_db": "sdb", "source_table": "src", "target_db": "tdb", "target_table": "tgt"}
PARTS = [{"key": "[0, 10)", "src_name": "p0", "tgt_name": "q0", "rows": 10},
         {"key": "[10, 20)", "src_name": "p1", "tgt_name": "q1", "rows": 10}]


def test_row_count_sql_merges_partitions_and_filters_by_side():
    resolved = {"filter": {"source": "x > 0", "target": "x >= 0"}}
    sql = row_count_sql(ENTRY, resolved, PARTS, "source")
    assert "FROM `sdb`.`src` PARTITION (`p0`) WHERE (x > 0)" in sql
    assert "UNION ALL" in sql
    assert "SELECT 1 AS __p" in sql


def test_metric_sql_uses_mapping_count_null_and_order():
    resolved = {"group_by": [{"src": "kind", "tgt": "type", "cat": "string"}],
                "metrics": [{"src": "tags", "tgt": "labels", "cat": "complex", "func": "COUNT_NULL"}],
                "filter": None}
    sql = metric_sql(ENTRY, resolved, PARTS[:1], "target")
    assert "CAST(`type` AS STRING) AS __g0" in sql
    assert "COUNT(*) - COUNT(`labels`) AS __m0" in sql
    assert "GROUP BY __g0" in sql
    assert sql.endswith("ORDER BY __p, __g0")


def test_metric_sql_supports_group_only_count():
    resolved = {"group_by": [{"src": "status", "tgt": "status", "cat": "string"}],
                "metrics": [], "filter": None}
    sql = metric_sql(ENTRY, resolved, PARTS[:1], "source")
    assert "COUNT(*) AS __cnt" in sql
    assert "GROUP BY __g0" in sql
    assert "AS __m0" not in sql


SAMPLE_RES = {"keys": [{"src": "dt", "tgt": "dt", "cat": "datetime"}, {"src": "id", "tgt": "id2", "cat": "int"}],
              "columns": [{"src": "payload", "tgt": "payload2", "cat": "complex"}],
              "ratio": 0.001, "max_rows": 10000, "filter": None}


def test_sample_size_bounds():
    assert sample_size(500, 0.001, 10000) == 1
    assert sample_size(20000, 0.001, 10000) == 20
    assert sample_size(10 ** 9, 0.001, 10000) == 10000


def test_sample_pick_sql_limits_each_partition_on_source():
    parts = [{**PARTS[0], "limit": 3}, {**PARTS[1], "limit": 5}]
    sql = sample_pick_sql(ENTRY, SAMPLE_RES, parts)
    assert "FROM `sdb`.`src` PARTITION (`p0`) LIMIT 3) __s0" in sql
    assert "SELECT 1 AS __p" in sql and "LIMIT 5) __s1" in sql
    assert "to_json(named_struct('v', `payload`))" in sql
    assert "murmur_hash3_32" not in sql
    assert sql.endswith("ORDER BY __p, __k0, __k1")


def test_sql_literal_quotes_and_escapes():
    assert sql_literal("42", "int") == "42"
    assert sql_literal("-1.50", "decimal") == "-1.50"
    assert sql_literal("1 OR 1=1", "int") == "'1 OR 1=1'"
    assert sql_literal("a'b\\c", "string") == "'a\\'b\\\\c'"
    assert sql_literal("2026-01-01 00:00:00", "datetime") == "'2026-01-01 00:00:00'"


def test_sample_lookup_sql_uses_key_in_lists_on_target():
    branch = {**PARTS[0], "p": 0, "values": [{"2026-01-01 00:00:00"}, {"2", "1", None}]}
    sql = sample_lookup_sql(ENTRY, SAMPLE_RES, [branch])
    assert "FROM `tdb`.`tgt` PARTITION (`q0`)" in sql
    assert "`dt` IN ('2026-01-01 00:00:00')" in sql
    assert "(`id2` IN (1, 2) OR `id2` IS NULL)" in sql
    assert sql.endswith("ORDER BY __p, __k0, __k1")


def _sample_entry():
    rules = [{"index": 0, "name": "SAMPLE", "key": "0:SAMPLE", "status": "READY", "resolved": SAMPLE_RES},
             {"index": 1, "name": "ROW_COUNT", "key": "1:ROW_COUNT", "status": "READY",
              "resolved": {"filter": None}}]
    return {**ENTRY, "table_id": "sdb.src=>tdb.tgt", "common_partitions": PARTS, "rules": rules}


def test_plan_main_sample_reuses_row_count_task():
    entry = _sample_entry()
    tasks = plan_main([entry], RunSettings())
    rc, sample = tasks
    assert rc["rule"] == "ROW_COUNT" and sample["rule"] == "SAMPLE"
    assert sample["count_from"] == rc["task_id"]
    assert sample["source_sql"] is None and sample["target_sql"] is None


def test_plan_picks_skips_empty_partitions_and_chunks():
    entry = _sample_entry()
    main = plan_main([entry], RunSettings())[1]
    picks = plan_picks(entry, entry["rules"][0], main, {0: 0, 1: 25000}, max_parts=10)
    assert len(picks) == 1
    assert [(p["key"], p["count"], p["limit"]) for p in picks[0]["parts"]] == [("[10, 20)", 25000, 25)]
    assert picks[0]["target_sql"] is None
    assert picks[0]["parent_task_id"] == main["task_id"]


def test_plan_lookups_splits_by_value_budget(monkeypatch):
    import srdq_lite.plan as plan
    monkeypatch.setattr(plan, "LOOKUP_VALUES_PER_SQL", 4)
    entry = _sample_entry()
    pick = {"task_id": "pick", "parts": [{**PARTS[0], "count": 9, "limit": 3}, {**PARTS[1], "count": 9, "limit": 1}]}
    rows = [[0, "d1", "1", "x"], [0, "d1", "2", "x"], [0, "d1", "3", "x"], [1, "d2", "9", "y"]]
    lookups = plan_lookups(entry, entry["rules"][0], pick, rows, max_parts=10)
    assert [[(b["p"], b["first"], b["last"], b["rows"]) for b in t["parts"]] for t in lookups] == [
        [(0, ["d1", "1"], ["d1", "3"], 3)], [(1, ["d2", "9"], ["d2", "9"], 1)]]
    assert all(t["source_sql"] is None and t["parent_task_id"] == "pick" for t in lookups)
    assert "`id2` IN (1, 2, 3)" in lookups[0]["target_sql"]


def test_batch_partitions_honors_count_and_row_limits():
    parts = [{"key": str(i), "rows": n} for i, n in enumerate([4, 4, 4, 20])]
    batches = batch_partitions(parts, max_parts=2, max_rows=10)
    assert [[p["key"] for p in b] for b in batches] == [["0", "1"], ["2"], ["3"]]
