from srdq_lite.config import Rule, TableSpec
from srdq_lite.meta import build_entry, normalize_partition_value


def test_normalize_partition_values():
    assert normalize_partition_value("keys: [1]; keys: [100];") == "[1, 100)"
    assert normalize_partition_value('[["us"], ["ca"]]') == 'IN ("ca", "us")'
    assert normalize_partition_value("expression partition") == "expression partition"


def test_count_null_allows_complex_column():
    rule = Rule(0, "METRIC", {"metrics": [{"column": "tags", "func": "COUNT_NULL", "tolerance": None}],
                              "group_by": [], "filter": None, "max_groups": 100})
    spec = TableSpec(1, "s", "a", "t", "b", (rule,), {})
    col = {"name": "tags", "data_type": "array", "column_type": "array<int>",
           "nullable": True, "category": "complex"}
    meta = {"exists": True, "model": "PRIMARY_KEYS", "keys": ["id"], "partition_columns": [],
            "buckets": 1, "columns": [col], "partitions": [], "rows": 0}
    entry = build_entry(spec, meta, meta)
    assert entry["rules"][0]["status"] == "READY"
    assert entry["rules"][0]["resolved"]["metrics"][0]["func"] == "COUNT_NULL"
