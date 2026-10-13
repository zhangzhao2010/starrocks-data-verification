from srdq_lite.plan import batch_partitions, metric_sql, row_count_sql, sample_drill_sql, sample_summary_sql


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


def test_sample_sql_has_stable_hash_bucket_and_stream_order():
    resolved = {"keys": [{"src": "id", "tgt": "id2", "cat": "int"}],
                "columns": [{"src": "payload", "tgt": "payload2", "cat": "complex"}],
                "buckets": 16, "threshold": 10000, "filter": None}
    summary = sample_summary_sql(ENTRY, resolved, PARTS[:1], "source")
    assert "murmur_hash3_32" in summary
    assert "% 16" in summary
    drill_parts = [{**PARTS[0], "buckets": [2, 7]}]
    drill = sample_drill_sql(ENTRY, resolved, drill_parts, "target")
    assert "to_json(named_struct('v', `payload2`))" in drill
    assert "IN (2, 7)" in drill
    assert drill.endswith("ORDER BY __p, __k0")


def test_batch_partitions_honors_count_and_row_limits():
    parts = [{"key": str(i), "rows": n} for i, n in enumerate([4, 4, 4, 20])]
    batches = batch_partitions(parts, max_parts=2, max_rows=10)
    assert [[p["key"] for p in b] for b in batches] == [["0", "1"], ["2"], ["3"]]
