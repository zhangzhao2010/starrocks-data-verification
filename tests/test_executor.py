from __future__ import annotations

import threading
from decimal import Decimal

import pytest

from srdq.common import canonical as c
from srdq.executor import compiler, encoder, probe, tasks
from srdq.executor.adapter.base import AdapterError
from srdq.executor.adapter.sim import SimAdapter
from srdq.executor.adapter.starrocks import normalize_model, parse_show_partitions

SEED = "00000000000000000000000000000000"
CTX = "11" * 32

COLS = {"id": {"type": "INTEGER", "scale": None}, "name": {"type": "STRING", "scale": None},
        "amt": {"type": "DECIMAL", "scale": 2}, "dt": {"type": "DATE", "scale": None},
        "cat": {"type": "STRING", "scale": None}}


def make_side(path, rows):
    a = SimAdapter("huawei", path)
    a.cluster.create_table("db1", "t1", [("id", "bigint", False), ("name", "varchar(64)"), ("amt", "decimal(18,2)"),
                                         ("dt", "date"), ("cat", "varchar(8)")],
                           model="PRI_KEYS", keys=["id"])
    a.cluster.insert("db1", "t1", rows)
    return a


def rows(n, mutate=None):
    out = []
    for i in range(1, n + 1):
        r = {"id": i, "name": f"n{i}", "amt": Decimal(i) / 4, "dt": f"2026-10-{(i % 3) + 1:02d}", "cat": "ab"[i % 2]}
        if mutate:
            r = mutate(r)
        out.append(r)
    return out


def spec(kind, **kw):
    s = {"kind": "CHECK", "check_kind": kind, "side": "huawei", "table": {"db": "db1", "table": "t1"},
         "columns": COLS, "limits": {"batch_rows": 500}, "rule_digest_hex": "00" * 32,
         "members": [{"node_id": "n1", "predicate": [], "context_digest_hex": CTX}]}
    s.update(kw)
    return s


def run(adapter, sp, tmp_path):
    return tasks.run_check(sp, adapter, tmp_path / "work", cancel=threading.Event(), on_query_ref=lambda r: None,
                           usage=lambda n: None)


def test_probe_passes_on_sim(tmp_path):
    a = make_side(tmp_path / "hw", [])
    res = probe.run_probe(a)
    assert res["status"] == "PASS", res["failures"]


def test_s1_pushdown_matches_python(tmp_path):
    a = make_side(tmp_path / "hw", rows(5000))
    sp = spec("SAMPLE", sample={"key_column": "id", "key_type": "INTEGER", "seed_hex": SEED, "bucket_count": 1000,
                                "selected_bucket": 0},
              projection={"key_columns": ["id"], "exact_columns": ["id", "name", "amt"], "tolerant_columns": []})
    out = run(a, sp, tmp_path)
    want = sorted(c.single_key("INTEGER", i).hex() for i in range(1, 5001)
                  if c.s1_selected(c.single_key("INTEGER", i), SEED))
    m = out.members[0]
    assert m.payload["sample_rows"] == len(want) > 0
    lines = list(encoder.read_chunk_lines(sorted(m.chunk_dir.glob("chunk_*.jsonl"))))
    assert [ln["k"] for ln in lines] == want
    d = c.D1(bytes.fromhex(CTX))
    for ln in lines:
        d.add(bytes.fromhex(ln["h"]))
    assert d.finish().hex() == m.payload["sample_row_digest"]


def test_string_key_sample(tmp_path):
    a = SimAdapter("huawei", tmp_path / "hw")
    a.cluster.create_table("db1", "s1", [("k", "varchar(64)", False), ("v", "int")], model="PRI_KEYS", keys=["k"])
    keys = [f"键{i}" for i in range(3000)]
    a.cluster.insert("db1", "s1", [{"k": k, "v": i} for i, k in enumerate(keys)])
    cols = {"k": {"type": "STRING"}, "v": {"type": "INTEGER"}}
    sp = spec("SAMPLE", table={"db": "db1", "table": "s1"}, columns=cols,
              sample={"key_column": "k", "key_type": "STRING", "seed_hex": SEED, "bucket_count": 100,
                      "selected_bucket": 3},
              projection={"key_columns": ["k"], "exact_columns": ["k", "v"]})
    out = run(a, sp, tmp_path)
    want = sum(1 for k in keys if c.s1_selected(c.single_key("STRING", k), SEED, 100, 3))
    assert out.members[0].payload["sample_rows"] == want > 0


def test_count_and_predicate(tmp_path):
    a = make_side(tmp_path / "hw", rows(100))
    pred = [{"kind": "RANGE", "column": "id", "type": "INTEGER", "lower": "10", "upper": "20"}]
    sp = spec("COUNT", members=[{"node_id": "n1", "predicate": pred, "context_digest_hex": CTX},
                                {"node_id": "n2", "predicate": [], "context_digest_hex": CTX}])
    out = run(a, sp, tmp_path)
    assert [m.payload["row_count"] for m in out.members] == ["10", "100"]


def test_metric_groups(tmp_path):
    a = make_side(tmp_path / "hw", rows(10))
    rule = {"group_by": ["cat"], "filter": {"op": "TRUE"},
            "metrics": [{"id": "rows", "function": "COUNT_ROWS"},
                        {"id": "s_amt", "function": "SUM", "column": "amt", "value_type": "DECIMAL", "scale": 2},
                        {"id": "s_id", "function": "SUM", "column": "id", "value_type": "INTEGER"}]}
    out = run(a, spec("METRIC", rule=rule), tmp_path)
    m = out.members[0]
    assert m.payload["group_count"] == 2
    lines = list(encoder.read_chunk_lines(sorted(m.chunk_dir.glob("chunk_*.jsonl"))))
    by = {ln["v"][0]: ln for ln in lines}
    assert by["a"]["m"]["s_id"]["text"] == str(2 + 4 + 6 + 8 + 10)
    assert by["b"]["m"]["s_amt"]["text"] == "6.25"
    assert by["a"]["rows"] == "5"


def test_deep_key_range_and_hash_bucket_agree(tmp_path):
    a = make_side(tmp_path / "hw", rows(300))
    proj = {"key_columns": ["id"], "exact_columns": ["id", "name", "amt", "dt"]}
    kr = run(a, spec("DEEP", projection=proj, deep={"mode": "KEY_RANGE"}), tmp_path / "a")
    members = [{"node_id": f"b{i}", "predicate": [], "context_digest_hex": CTX, "bucket": i} for i in range(4)]
    hb = run(a, spec("DEEP", projection=proj, deep={"mode": "HASH_BUCKET", "buckets": 4}, members=members),
             tmp_path / "b")
    assert kr.members[0].payload["row_count"] == 300
    assert sum(m.payload["row_count"] for m in hb.members) == 300


def test_keyless_deep_counts_duplicates(tmp_path):
    a = SimAdapter("huawei", tmp_path / "hw")
    a.cluster.create_table("db1", "d", [("x", "int"), ("y", "varchar(4)")], model="DUP_KEYS")
    a.cluster.insert("db1", "d", [{"x": 1, "y": "a"}, {"x": 1, "y": "a"}, {"x": 2, "y": None}])
    cols = {"x": {"type": "INTEGER"}, "y": {"type": "STRING"}}
    out = run(a, spec("DEEP", table={"db": "db1", "table": "d"}, columns=cols,
                      projection={"key_columns": [], "exact_columns": ["x", "y"]}, deep={"mode": "KEY_RANGE"}),
              tmp_path)
    m = out.members[0]
    assert m.payload["row_count"] == 3 and m.payload["keyless"]
    lines = list(encoder.read_chunk_lines(sorted(m.chunk_dir.glob("chunk_*.jsonl"))))
    assert sorted(ln["n"] for ln in lines) == [1, 2]


def test_duplicate_key_is_contract_violation(tmp_path):
    a = SimAdapter("huawei", tmp_path / "hw")
    a.cluster.create_table("db1", "d", [("id", "bigint"), ("v", "int")], model="DUP_KEYS")
    a.cluster.insert("db1", "d", [{"id": 1, "v": 1}, {"id": 1, "v": 2}])
    cols = {"id": {"type": "INTEGER"}, "v": {"type": "INTEGER"}}
    with pytest.raises(encoder.EncodeError) as ei:
        run(a, spec("DEEP", table={"db": "db1", "table": "d"}, columns=cols,
                    projection={"key_columns": ["id"], "exact_columns": ["id", "v"]}, deep={"mode": "HASH_BUCKET",
                                                                                           "buckets": 1},
                    members=[{"node_id": "n1", "predicate": [], "context_digest_hex": CTX, "bucket": 0}]),
            tmp_path)
    assert ei.value.error_class == "CONTRACT_VIOLATION"


def test_result_byte_cap(tmp_path):
    a = make_side(tmp_path / "hw", rows(200))
    sp = spec("DEEP", projection={"key_columns": ["id"], "exact_columns": ["id", "name"]}, deep={"mode": "KEY_RANGE"})
    sp["limits"]["max_result_bytes"] = 2000
    with pytest.raises(encoder.EncodeError) as ei:
        run(a, sp, tmp_path)
    assert ei.value.error_class == "BUDGET_EXCEEDED"


def test_cancel_stops_query(tmp_path):
    a = make_side(tmp_path / "hw", rows(2000))
    a.cluster.set_setting("slow_ms_per_batch", "50")
    ev = threading.Event()
    refs = []
    sp = spec("DEEP", projection={"key_columns": ["id"], "exact_columns": ["id"]}, deep={"mode": "KEY_RANGE"})
    sp["limits"]["batch_rows"] = 10

    def go():
        try:
            tasks.run_check(sp, a, tmp_path / "w", cancel=ev, on_query_ref=refs.append, usage=lambda n: None)
        except (tasks.Cancelled, AdapterError) as e:
            refs.append(e)

    t = threading.Thread(target=go)
    t.start()
    while not refs:
        pass
    a.cancel(refs[0])
    ev.set()
    t.join(10)
    assert not t.is_alive()
    assert isinstance(refs[-1], (tasks.Cancelled, AdapterError))
    assert a.confirm_terminated(refs[0])


def test_fault_injection_storage(tmp_path):
    a = make_side(tmp_path / "hw", rows(5))
    a.cluster.add_fault("db1", "t1", "Tablet 12345 has no queryable replica", remaining=1)
    with pytest.raises(AdapterError):
        run(a, spec("COUNT"), tmp_path)
    assert run(a, spec("COUNT"), tmp_path).members[0].payload["row_count"] == "5"


def test_table_meta(tmp_path):
    a = make_side(tmp_path / "hw", [])
    a.cluster.set_partition("db1", "t1", {"kind": "RANGE", "column": "dt",
                                          "partitions": [{"name": "p1", "lower": "2026-10-01", "upper": "2026-10-02"}]})
    m = a.table_meta("db1", "t1")
    assert m.status == "PRESENT" and m.model == "PRI_KEYS" and m.key_columns == ["id"]
    assert m.partition["columns"] == ["dt"]
    assert a.table_meta("db1", "nope").status == "ABSENT_CONFIRMED"


def test_parse_show_partitions():
    rows = [
        {"PartitionName": "p20261001", "PartitionKey": "dt",
         "Range": "[types: [DATE]; keys: [2026-10-01]; ..types: [DATE]; keys: [2026-10-02]; )"},
        {"PartitionName": "p_min", "PartitionKey": "dt",
         "Range": "[types: [DATE]; keys: [0000-01-01]; ..types: [DATE]; keys: [2026-10-01]; )"},
    ]
    p = parse_show_partitions(rows)
    assert p["kind"] == "RANGE" and p["columns"] == ["dt"]
    got = {x["name"]: (x["lower"], x["upper"]) for x in p["partitions"]}
    assert got["p20261001"] == ("2026-10-01", "2026-10-02")
    assert got["p_min"][1] == "2026-10-01"
    lst = parse_show_partitions([{"PartitionName": "p_bj", "PartitionKey": "city", "List": "(('bj'), ('sh'))"}])
    assert lst["kind"] == "LIST" and lst["partitions"][0]["values"] == ["bj", "sh"]


def test_normalize_model_accepts_long_names():
    assert [normalize_model(m) for m in ("PRIMARY_KEYS", "unique_keys", "AGGREGATE_KEYS", "DUPLICATE_KEYS")] \
        == ["PRI_KEYS", "UNQ_KEYS", "AGG_KEYS", "DUP_KEYS"]
    assert [normalize_model(m) for m in ("PRI_KEYS", "UNQ_KEYS", "DUP_KEYS")] == ["PRI_KEYS", "UNQ_KEYS", "DUP_KEYS"]


def test_bucket_sql_matches_python_for_extremes(tmp_path):
    a = make_side(tmp_path / "hw", [])
    for n in (1000, 7, 997):
        for v in (0, 2**63 - 1, -(2**63)):
            sql, params = compiler.compile_s1_probe("INTEGER", v, SEED, n)
            row = a.scalar_row(sql, params)
            assert int(row[2]) == c.s1_bucket(c.single_key("INTEGER", v), SEED, n)
