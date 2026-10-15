import pytest

from srdq_lite.config import Rule, RunSettings, TableSpec
from srdq_lite.meta import build_entry, fetch_table_meta, normalize_partition_value
from srdq_lite.plan import plan_lookups, plan_main, plan_picks
from srdq_lite.sr import SRError


def test_normalize_partition_values():
    assert normalize_partition_value("keys: [1]; keys: [100];") == "[1, 100)"
    assert normalize_partition_value('[["us"], ["ca"]]') == 'IN ("ca", "us")'
    assert normalize_partition_value("expression partition") == "expression partition"


def test_show_partitions_list_matches_partitions_meta():
    expected = normalize_partition_value('[["us"], ["ca"]]')
    assert normalize_partition_value("(('us'), ('ca'))") == expected
    assert normalize_partition_value("('us', 'ca')") == expected
    assert normalize_partition_value("[[1], [2]]") == normalize_partition_value("(('1'), ('2'))")
    assert normalize_partition_value('[["a", "1"]]') == normalize_partition_value("(('a', '1'))")


class _OldSR:
    def query(self, sql, params=None):
        if "tables_config" in sql:
            return [("PRI_KEYS", "id", "dt", 4)]
        if "partitions_meta" in sql:
            raise SRError("[1064] Unknown table 'partitions_meta'", 1064, False)
        return [("id", "int", "int", "NO"), ("dt", "date", "date", "NO")]

    def query_dicts(self, sql, params=None):
        assert sql == "SHOW PARTITIONS FROM `s`.`a`"
        return [{"PartitionName": "p1", "Range": "[types: [DATE]; keys: [2024-01-01]; ..types: [DATE]; "
                 "keys: [2024-02-01]; )", "RowCount": "7"},
                {"PartitionName": "p2", "Range": "[types: [DATE]; keys: [2024-02-01]; ..types: [DATE]; "
                 "keys: [2024-03-01]; )", "RowCount": "3"}]


def test_falls_back_to_show_partitions():
    meta = fetch_table_meta(_OldSR(), "s", "a")
    assert meta["rows"] == 10
    assert meta["partitions"] == [{"name": "p1", "key": "[2024-01-01, 2024-02-01)", "rows": 7},
                                  {"name": "p2", "key": "[2024-02-01, 2024-03-01)", "rows": 3}]


def test_count_null_allows_complex_column():
    rule = Rule(0, "METRIC", {"metrics": [{"column": "tags", "func": "COUNT_NULL", "tolerance": None}],
                              "group_by": [], "filter": None, "max_groups": 100})
    spec = TableSpec(1, "s", "a", "t", "b", (rule,), {})
    col = {"name": "tags", "data_type": "array", "column_type": "array<int>",
           "nullable": True, "category": "complex"}
    meta = {"exists": True, "model": "PRIMARY_KEYS", "keys": ["id"], "partition_columns": [],
            "buckets": 1, "columns": [col], "partitions": [], "rows": 0}
    entry = build_entry(spec, meta, meta, sharding_time_cutoff_utc="1970-01-01 00:00:00")
    assert entry["rules"][0]["status"] == "READY"
    assert entry["rules"][0]["resolved"]["metrics"][0]["func"] == "COUNT_NULL"


CUTOFF = "2026-10-15 04:00:00"


def _time_spec(flt=None, mapping=None):
    rules = (Rule(0, "ROW_COUNT", {"filter": flt}),
             Rule(1, "METRIC", {"metrics": [{"column": "id", "func": "SUM", "tolerance": None}],
                                "group_by": [], "filter": flt, "max_groups": 100}),
             Rule(2, "SAMPLE", {"key": None, "columns": "*", "exclude_columns": [],
                                "ratio": 0.1, "max_rows": 100, "filter": flt}))
    return TableSpec(1, "s", "a", "t", "b", rules, mapping or {})


def _time_meta(time_name="sharding_time", partitioned=False):
    columns = [{"name": "id", "data_type": "int", "column_type": "int",
                "nullable": False, "category": "int"}]
    if time_name:
        columns.append({"name": time_name, "data_type": "datetime", "column_type": "datetime",
                        "nullable": True, "category": "datetime"})
    return {"exists": True, "model": "PRIMARY_KEYS", "keys": ["id"],
            "partition_columns": ["id"] if partitioned else [], "buckets": 1, "columns": columns,
            "partitions": [{"name": "p0", "key": "[0, 10)", "rows": 10}] if partitioned else [], "rows": 10}


@pytest.mark.parametrize("partitioned", [False, True])
@pytest.mark.parametrize("flt", [None, {"source": "id = 1 OR id = 2", "target": "id > 0"},
                                 {"source": None, "target": "id > 0"}])
def test_sharding_time_filter_applies_to_all_rules(partitioned, flt):
    spec = _time_spec(flt)
    meta = _time_meta(partitioned=partitioned)
    entry = build_entry(spec, meta, meta, sharding_time_cutoff_utc=CUTOFF)
    expected = {}
    for side in ("source", "target"):
        condition = f"`sharding_time` < '{CUTOFF}'"
        expected[side] = f"({flt[side]}) AND {condition}" if flt and flt.get(side) else condition
    assert entry["meta_status"] == "OK"
    assert entry["partitioned"] == partitioned
    for rule, original in zip(entry["rules"], spec.rules):
        assert rule["status"] == "READY"
        assert rule["resolved"]["filter"] == expected
        assert original.config["filter"] == flt


@pytest.mark.parametrize("flt", [None, {"source": "id > 0", "target": "id >= 0"}])
def test_without_sharding_time_preserves_queries(flt):
    spec = _time_spec(flt)
    meta = _time_meta(time_name=None)
    first = build_entry(spec, meta, meta, sharding_time_cutoff_utc=CUTOFF)
    later = build_entry(spec, meta, meta, sharding_time_cutoff_utc="2026-10-15 05:00:00")
    assert all(rule["resolved"]["filter"] == flt for rule in first["rules"])
    assert plan_main([first], RunSettings()) == plan_main([later], RunSettings())


def test_sharding_time_filter_uses_case_insensitive_mapping_and_quoted_names():
    spec = _time_spec(mapping={"sharding_time": "loaded`at"})
    src, tgt = _time_meta("SHARDING_TIME"), _time_meta("loaded`at")
    entry = build_entry(spec, src, tgt, sharding_time_cutoff_utc=CUTOFF)
    for rule in entry["rules"]:
        assert rule["status"] == "READY"
        assert rule["resolved"]["filter"] == {
            "source": f"`SHARDING_TIME` < '{CUTOFF}'",
            "target": f"`loaded``at` < '{CUTOFF}'"}


@pytest.mark.parametrize("side", ["source", "target"])
def test_sharding_time_missing_on_one_side_blocks_inconsistent_scope(side):
    src, tgt = _time_meta(), _time_meta()
    if side == "source":
        src = _time_meta(time_name=None)
    else:
        tgt = _time_meta(time_name=None)
    entry = build_entry(_time_spec(), src, tgt, sharding_time_cutoff_utc=CUTOFF)
    assert entry["meta_status"] == "BLOCKED"
    assert all(rule["status"] == "BLOCKED" for rule in entry["rules"])
    assert all("time filter" in rule["reason"] for rule in entry["rules"])


@pytest.mark.parametrize("partitioned", [False, True])
def test_sharding_time_filter_reaches_main_pick_and_lookup_queries(partitioned):
    meta = _time_meta(partitioned=partitioned)
    entry = build_entry(_time_spec(), meta, meta, sharding_time_cutoff_utc=CUTOFF)
    tasks = plan_main([entry], RunSettings())
    count, metric, sample = tasks
    condition = f"`sharding_time` < '{CUTOFF}'"
    assert sample["count_from"] == count["task_id"]
    for task in (count, metric):
        for side in ("source", "target"):
            assert f"WHERE ({condition})" in task[f"{side}_sql"]
            assert "NOW()" not in task[f"{side}_sql"]
            assert ("PARTITION (`p0`)" in task[f"{side}_sql"]) == partitioned
    rule = entry["rules"][2]
    picks = plan_picks(entry, rule, sample, {0: 10}, max_parts=10)
    assert f"WHERE ({condition}) LIMIT 1" in picks[0]["source_sql"]
    lookups = plan_lookups(entry, rule, picks[0], [[0, "1", "2026-10-14 01:00:00"]], max_parts=10)
    assert f"WHERE ({condition}) AND `id` IN (1)" in lookups[0]["target_sql"]


def test_sample_only_count_query_uses_sharding_time_filter():
    spec = _time_spec()
    spec = TableSpec(1, "s", "a", "t", "b", (spec.rules[2],), {})
    meta = _time_meta()
    entry = build_entry(spec, meta, meta, sharding_time_cutoff_utc=CUTOFF)
    task, = plan_main([entry], RunSettings())
    assert task["rule"] == "SAMPLE"
    assert f"WHERE (`sharding_time` < '{CUTOFF}')" in task["source_sql"]
    assert task["target_sql"] is None
    assert "count_from" not in task


def test_time_filter_keeps_partition_metadata_comparison_unchanged():
    src, tgt = _time_meta(partitioned=True), _time_meta(partitioned=True)
    tgt["partitions"] = []
    entry = build_entry(_time_spec(), src, tgt, sharding_time_cutoff_utc=CUTOFF)
    assert entry["common_partitions"] == []
    assert entry["source_only_partitions"] == [{"key": "[0, 10)", "name": "p0", "rows": 10}]
