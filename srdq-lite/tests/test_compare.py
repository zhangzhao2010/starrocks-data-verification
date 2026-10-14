import gzip
import json

import pytest

from srdq_lite.compare import (OrderError, Results, SampleContext, Sink, build_outputs, compare_table,
                               derive_lookups, merge, values_equal)
from srdq_lite.store import dumps
from srdq_lite.plan import plan_lookups, plan_picks


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
    """rows: {side: rows} for any task, or {(task_id, side): rows}."""
    def __init__(self, rows=None):
        self.data = rows or {}
    def _get(self, task, side):
        return self.data.get((task["task_id"], side), self.data.get(side))
    def side_ok(self, task, side):
        return self._get(task, side) is not None
    def state(self, task):
        return ("OK", None) if task else ("PENDING", "not planned")
    def rows(self, task, side):
        return iter(self._get(task, side))
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


def test_compare_row_count_writes_partition_diff():
    task = {"task_id": "task", "rule_key": "0:ROW_COUNT", "rule": "ROW_COUNT",
            "phase": "main", "parts": [{"key": "[0, 10)"}]}
    sink = Sink("run", 100)
    res = FakeResults({"source": [[0, 10]], "target": [[0, 9]]})
    row, failed = compare_table(sink, entry(), [task], SampleContext([task], [], [], res, res, res))
    assert row["status"] == "DIFF"
    assert row["row_count_src"] == 10
    assert row["row_count_tgt"] == 9
    assert row["row_count_diff_partitions"] == 1
    assert failed == []
    diff = json.loads(sink.texts()["diffs.jsonl"])
    assert diff["diff_type"] == "COUNT_DIFF"
    assert diff["delta"] == "-1"


def test_blocked_rule_does_not_report_zero_counts():
    sink = Sink("run", 100)
    res = FakeResults()
    row, _ = compare_table(sink, entry("BLOCKED"), [], SampleContext([], [], [], res, res, res))
    assert row["status"] == "BLOCKED"
    assert row["row_count_status"] == "BLOCKED"
    assert row["row_count_src"] is None
    assert row["row_count_tgt"] is None
    assert row["row_count_diff_partitions"] is None


def test_sample_pick_lookup_compare(monkeypatch):
    import srdq_lite.plan as plan
    monkeypatch.setattr(plan, "LOOKUP_VALUES_PER_SQL", 2)
    resolved = {"keys": [{"src": "id", "tgt": "id", "cat": "int"}],
                "columns": [{"src": "v", "tgt": "v", "cat": "string"}],
                "ratio": 0.001, "max_rows": 10000, "filter": None}
    rule = {"key": "0:SAMPLE", "name": "SAMPLE", "status": "READY", "reason": None, "resolved": resolved}
    e = {**entry(), "common_partitions": [{"key": "p0"}, {"key": "p1"}], "rules": [rule]}
    parts = [{"key": "p0", "src_name": "p0", "tgt_name": "p0"}, {"key": "p1", "src_name": "p1", "tgt_name": "p1"}]
    main = {"task_id": "main", "rule_key": "0:SAMPLE", "rule": "SAMPLE", "phase": "main", "parts": parts,
            "source_sql": "count", "target_sql": None}
    picks = plan_picks(e, rule, main, {0: 3000, 1: 0}, max_parts=10)
    picked = [[0, "1", "a"], [0, "2", "b"], [0, "3", "c"]]
    lookups = plan_lookups(e, rule, picks[0], picked, max_parts=10)
    assert len(lookups) == 2
    # superset rows outside each lookup's range (id 3 in the first, 1/2 in the second) must be ignored
    pick_res = FakeResults({(picks[0]["task_id"], "source"): picked})
    lookup_res = FakeResults({(lookups[0]["task_id"], "target"): [[0, "1", "a"], [0, "2", "x"], [0, "4", "z"]],
                              (lookups[1]["task_id"], "target"): [[0, "1", "a"], [0, "2", "x"]]})
    main_res = FakeResults({("main", "source"): [[0, 3000], [1, 0]]})
    sink = Sink("run", 100)
    row, failed = compare_table(sink, e, [main], SampleContext([main], picks, lookups, main_res, pick_res,
                                                                lookup_res))
    assert failed == []
    assert row["sample_status"] == "DIFF"
    assert row["sample_rows"] == 3
    assert row["sample_diff_keys"] == 2
    diffs = [json.loads(l) for l in sink.texts()["diffs.jsonl"].splitlines()]
    assert [(d["diff_type"], d["key"]) for d in diffs] == [("SAMPLE_COLUMN_DIFF", "2"), ("SAMPLE_KEY_SOURCE_ONLY", "3")]
    cmp = {r["partition"]: r for r in map(json.loads, sink.texts()["05_compare.jsonl"].splitlines())}
    assert (cmp["p0"]["src"], cmp["p0"]["tgt"], cmp["p0"]["diffs"]) == (3, 2, 2)
    assert cmp["p1"]["status"] == "PASS" and cmp["p1"]["note"] == "empty partition"


def _put(out, records, task, side, rows):
    f = out / "results" / f"{task['task_id']}.{side}.jsonl.gz"
    f.parent.mkdir(exist_ok=True)
    with gzip.open(f, "wt") as fh:
        fh.writelines(dumps(r) + "\n" for r in rows)
    with open(out / records, "a") as fh:
        fh.write(dumps({"task_id": task["task_id"], "side": side, "status": "OK", "file": f"results/{f.name}",
                        "elapsed_s": 0.1, "error": None, "attempts": 1}) + "\n")


def test_parallel_lookups_and_report_from_files(tmp_path):
    resolved = {"keys": [{"src": "id", "tgt": "id", "cat": "int"}],
                "columns": [{"src": "v", "tgt": "v", "cat": "string"}],
                "ratio": 0.001, "max_rows": 10000, "filter": None}
    rule = {"key": "0:SAMPLE", "name": "SAMPLE", "status": "READY", "reason": None, "resolved": resolved}
    parts = [{"key": "p0", "src_name": "p0", "tgt_name": "p0"}]
    sample_entry = {**entry(), "table_id": "s.b=>t.b", "source_table": "b", "target_table": "b",
                    "common_partitions": [{"key": "p0"}], "rules": [rule]}
    count_entry = entry()
    main = {"task_id": "main", "table_id": "s.b=>t.b", "rule_key": "0:SAMPLE", "rule": "SAMPLE", "phase": "main",
            "parts": parts, "source_sql": "count", "target_sql": None}
    rc = {"task_id": "rc", "table_id": "s.a=>t.a", "rule_key": "0:ROW_COUNT", "rule": "ROW_COUNT",
          "phase": "main", "parts": [{"key": "[0, 10)"}], "source_sql": "c", "target_sql": "c"}
    _put(tmp_path, "04.jsonl", main, "source", [[0, 3000]])
    _put(tmp_path, "04.jsonl", rc, "source", [[0, 5]])
    _put(tmp_path, "04.jsonl", rc, "target", [[0, 5]])
    picks = plan_picks(sample_entry, rule, main, {0: 3000}, max_parts=10)
    picked = [[0, "1", "a"], [0, "2", "b"], [0, "3", "c"]]
    _put(tmp_path, "07.jsonl", picks[0], "source", picked)

    entries = [sample_entry, count_entry]
    lookups = derive_lookups(entries, picks, Results(tmp_path, tmp_path / "07.jsonl"), 10)
    assert lookups == plan_lookups(sample_entry, rule, picks[0], picked, max_parts=10)
    _put(tmp_path, "09.jsonl", lookups[0], "target", [[0, "1", "a"], [0, "2", "x"]])

    stats = build_outputs(tmp_path, "run", entries, [main, rc], picks, lookups,
                          Results(tmp_path, tmp_path / "04.jsonl"), Results(tmp_path, tmp_path / "07.jsonl"),
                          Results(tmp_path, tmp_path / "09.jsonl"), 100)
    summary = [json.loads(line) for line in (tmp_path / "table_summary.jsonl").read_text().splitlines()]
    assert [(r["table_id"], r["status"]) for r in summary] == [("s.b=>t.b", "DIFF"), ("s.a=>t.a", "PASS")]
    assert stats["diffs_by_type"] == {"SAMPLE_COLUMN_DIFF": 1, "SAMPLE_KEY_SOURCE_ONLY": 1}
    assert stats["diffs_written"] == 2 and stats["failed_tasks"] == 0
    csv_lines = (tmp_path / "diffs.csv").read_text(encoding="utf-8").splitlines()
    assert csv_lines[0].startswith("\ufeffrun_id,") and len(csv_lines) == 3
    cmp = [json.loads(line) for line in (tmp_path / "05_compare.jsonl").read_text().splitlines()]
    assert [(r["table_id"], r["status"]) for r in cmp] == [("s.b=>t.b", "DIFF"), ("s.a=>t.a", "PASS")]
