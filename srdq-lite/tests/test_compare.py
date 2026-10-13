import json

import pytest

from srdq_lite.compare import OrderError, Sink, compare_table, merge, values_equal


def test_values_equal_numeric_relative_tolerance():
    assert values_equal("100.0", "100.00000005", True, 1e-9)
    assert not values_equal("100.0", "100.01", True, 1e-9)
    assert not values_equal("1", "1.0", False)
    assert values_equal(None, None, False)


def test_sorted_merge_and_order_check():
    rows = list(merge([[1, "a"], [3, "c"]], [[2, "b"], [3, "d"]], lambda r: r[0]))
    assert [(k, len(a), len(b)) for k, a, b in rows] == [(1, 1, 0), (2, 0, 1), (3, 1, 1)]
    with pytest.raises(OrderError):
        list(merge([[2], [1]], [], lambda r: r[0]))


class FakeResults:
    def __init__(self, rows=None):
        self.data = rows or {}
    def state(self, task):
        return ("OK", None) if task else ("PENDING", "not planned")
    def rows(self, task, side):
        return iter(self.data[side])
    def elapsed(self, task):
        return 0.1
    def failures(self, task):
        return []


def entry(rule_status="READY"):
    return {"table_id": "s.a=>t.a", "source_db": "s", "source_table": "a", "target_db": "t",
            "target_table": "a", "meta_status": "OK", "partitions_src": 1, "partitions_tgt": 1,
            "partitioned": True, "common_partitions": [{"key": "[0, 10)"}],
            "source_only_partitions": [], "target_only_partitions": [], "issues": [],
            "rules": [{"key": "0:ROW_COUNT", "name": "ROW_COUNT", "status": rule_status,
                       "reason": "blocked" if rule_status != "READY" else None,
                       "resolved": {"filter": None}}]}


def test_compare_row_count_writes_partition_diff(tmp_path):
    task = {"task_id": "task", "rule_key": "0:ROW_COUNT", "rule": "ROW_COUNT",
            "phase": "main", "parts": [{"key": "[0, 10)"}]}
    sink = Sink(tmp_path, "run", 100)
    row, failed = compare_table(sink, entry(), [task], {},
                                FakeResults({"source": [[0, 10]], "target": [[0, 9]]}), FakeResults())
    sink.close()
    assert row["status"] == "DIFF"
    assert row["row_count_src"] == 10
    assert row["row_count_tgt"] == 9
    assert row["row_count_diff_partitions"] == 1
    assert failed == []
    diff = json.loads((tmp_path / "diffs.jsonl").read_text())
    assert diff["diff_type"] == "COUNT_DIFF"
    assert diff["delta"] == "-1"


def test_blocked_rule_does_not_report_zero_counts(tmp_path):
    sink = Sink(tmp_path, "run", 100)
    row, _ = compare_table(sink, entry("BLOCKED"), [], {}, FakeResults(), FakeResults())
    sink.close()
    assert row["status"] == "BLOCKED"
    assert row["row_count_status"] == "BLOCKED"
    assert row["row_count_src"] is None
    assert row["row_count_tgt"] is None
    assert row["row_count_diff_partitions"] is None
