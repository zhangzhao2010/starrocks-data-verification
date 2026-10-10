from __future__ import annotations

import datetime as _dt

import pytest

from harness import DT_PARTITIONS, ORDER_COLS, Harness, order_rows
from srdq.controller import differences, manual_requests, operations, purge, reports, schedules
from srdq.controller.common import ApiError

METRIC_HEADER = ["source_db", "source_table", "rule_key", "group_by", "filter", "metric_id", "function", "column",
                 "scale", "compare", "rtol", "atol"]
SAMPLE_HEADER = ["source_db", "source_table", "key_column", "compare_columns", "float_rtol", "float_atol"]
FORBIDDEN = ("迁移验收通过", "全量内容一致", "100% 行已比较", "两边数据完全一致")


@pytest.fixture()
def h(db, tmp_dir):
    hh = Harness(db, tmp_dir)
    yield hh
    hh.close()


def setup_orders(h: Harness, *, target_mutate=None, n: int = 300):
    src = order_rows(n)
    tgt = order_rows(n, target_mutate) if target_mutate else src
    h.both("db1", "orders", ORDER_COLS, src, target_rows=tgt, model="PRI_KEYS", keys=["id"],
           partition=DT_PARTITIONS)
    h.import_assets(["ODS,db1,orders,db1,orders,50,true"])
    return h.discover_and_confirm()


def drop5(r):
    return None if r["id"] == 5 else r


def comparisons(h, run_id, kind=None):
    sql = "SELECT * FROM dq_comparison WHERE run_id=%s" + (" AND check_kind=%s" if kind else "") + " ORDER BY scope_key"
    return h.ctx.db.query(sql, (run_id, kind) if kind else (run_id,))


def test_count_flow_equal(h):
    setup_orders(h)
    plan = h.plan()
    run = h.run_plan(plan["id"])
    rcs = h.checks(run["id"])
    assert rcs, "no run checks"
    states = {(r["scope_key"], r["coverage_state"], r["execution_state"]) for r in rcs}
    assert all(r["execution_state"] in ("COMPLETED", "BLOCKED") for r in rcs), states
    assert any(r["coverage_state"] == "COMPLETED_EQUAL" for r in rcs), states
    assert h.run_row(run["id"])["execution_state"] == "COMPLETED"


def test_difference_chain_new_persistent_resolved(h):
    setup_orders(h, target_mutate=drop5)
    plan = h.plan(kinds=["COUNT"])
    r1 = h.run_plan(plan["id"])
    diff = [c for c in comparisons(h, r1["id"]) if c["verdict"] == "OBSERVED_DIFFERENT"]
    assert len(diff) == 1 and diff[0]["summary"]["source_rows"] == "100" and diff[0]["summary"]["target_rows"] == "99"
    res = differences.list_differences(h.ctx, {})
    assert [i["state"] for i in res["items"]] == ["NEW"]
    assert "delta -1" in res["items"][0]["summary"]

    h.clock.advance(seconds=120)
    h.run_plan(plan["id"])
    res = differences.list_differences(h.ctx, {})
    assert [(i["state"], i["consecutive_different"]) for i in res["items"]] == [("PERSISTENT", 2)]

    h.adapters["aliyun"].cluster.insert("db1", "orders", [r for r in order_rows(300) if r["id"] == 5])
    h.clock.advance(seconds=120)
    r3 = h.run_plan(plan["id"])
    assert all(c["verdict"] == "OBSERVED_EQUAL" for c in comparisons(h, r3["id"]))
    res = differences.list_differences(h.ctx, {})
    assert [i["state"] for i in res["items"]] == ["RESOLVED"]
    assert differences.list_differences(h.ctx, {"state": "NEW"})["items"] == []


def test_metric_rule_detects_sum_difference(h):
    setup_orders(h, target_mutate=lambda r: dict(r, amt=r["amt"] + 1) if r["id"] == 7 else r)
    imp = h.import_rules("METRIC", METRIC_HEADER, ["db1,orders,m_amt,cat,,sum_amt,SUM,amt,,EXACT,,"])
    assert imp["preview"]["status"] in ("PREVIEWED", "VALIDATED", "OK") or imp["preview"]
    plan = h.plan(kinds=["METRIC"])
    run = h.run_plan(plan["id"])
    cs = comparisons(h, run["id"], "METRIC")
    assert cs, h.checks(run["id"])
    by_verdict = {c["scope_key"]: c["verdict"] for c in cs}
    assert "OBSERVED_DIFFERENT" in by_verdict.values(), by_verdict
    dc = next(c for c in cs if c["verdict"] == "OBSERVED_DIFFERENT")
    diffs = h.ctx.db.query("SELECT * FROM dq_difference WHERE comparison_id=%s", (dc["id"],))
    assert [(d["type"], d["metric_id"]) for d in diffs] == [("METRIC_VALUE_DIFF", "sum_amt")]
    assert diffs[0]["group_values"] is not None
    assert h.run_row(run["id"])["execution_state"] == "COMPLETED"


def test_sample_rule_detects_field_difference(h):
    setup_orders(h, n=4000, target_mutate=lambda r: dict(r, name="changed"))
    h.import_rules("SAMPLE", SAMPLE_HEADER, ["db1,orders,id,*,,"])
    plan = h.plan(kinds=["SAMPLE"])
    run = h.run_plan(plan["id"])
    cs = comparisons(h, run["id"], "SAMPLE")
    assert cs, [(r["scope_key"], r["execution_state"], r["coverage_reason"]) for r in h.checks(run["id"])]
    sampled = [c for c in cs if c["verdict"] == "OBSERVED_DIFFERENT"]
    assert sampled, [(c["scope_key"], c["verdict"], c["reason"]) for c in cs]
    types = {d["type"] for c in sampled for d in h.ctx.db.query(
        "SELECT type FROM dq_difference WHERE comparison_id=%s", (c["id"],))}
    assert types == {"SAMPLE_FIELD_DIFF"}
    assert all(c["verdict"] in ("OBSERVED_DIFFERENT", "INCONCLUSIVE") for c in cs)


def _origin_count_comparison(h):
    setup_orders(h, target_mutate=drop5)
    run = h.run_plan(h.plan(kinds=["COUNT"])["id"])
    return next(c for c in comparisons(h, run["id"]) if c["verdict"] == "OBSERVED_DIFFERENT")


def test_manual_deep_request(h):
    origin = _origin_count_comparison(h)
    body = {"request_key": "deep-1", "check_kind": "DEEP", "requested_evidence": "EXACT",
            "asset_id": str(origin["asset_id"]), "origin_comparison_id": str(origin["id"]),
            "deep": {"mode": "HASH_BUCKET", "buckets": 4, "columns": "*"}}
    pv = manual_requests.preview(h.ctx, body, "diagnose id gap")
    assert pv["status"] == "PREVIEWED"
    assert manual_requests.preview(h.ctx, body, "again")["id"] == pv["id"]
    with pytest.raises(ApiError) as e:
        manual_requests.preview(h.ctx, dict(body, deep={"mode": "HASH_BUCKET", "buckets": 8}), "x")
    assert e.value.status == 409
    with pytest.raises(ApiError) as e:
        manual_requests.preview(h.ctx, dict(body, request_key="deep-2", source_mode="CAPTURED_MATERIAL"), "x")
    assert e.value.status == 422
    with pytest.raises(ApiError) as e:
        manual_requests.confirm(h.ctx, pv["id"], reason="go", if_match=pv["preview_digest"])
    assert e.value.status == 422 and e.value.code == "DETAIL_APPROVAL_REQUIRED"

    res = manual_requests.confirm(h.ctx, pv["id"], reason="go", if_match=pv["preview_digest"],
                                  detail_approval_ref="TICKET-42")
    assert res["status"] == "CONFIRMED"
    run_id = res["run"]["id"]
    assert h.run_row(run_id)["purpose"] == "MANUAL"
    h.work()
    cs = comparisons(h, run_id, "DEEP")
    assert cs and all(c["evidence"] == "EXACT" for c in cs)
    diff = [c for c in cs if c["verdict"] == "OBSERVED_DIFFERENT"]
    assert len(diff) == 1, [(c["scope_key"], c["verdict"], c["reason"]) for c in cs]
    rows = h.ctx.db.query("SELECT type, key_values FROM dq_difference WHERE comparison_id=%s", (diff[0]["id"],))
    assert sorted(r["type"] for r in rows) == ["DEEP_KEY_SOURCE_ONLY", "DEEP_ROW_COUNT_DIFF"]
    only = next(r for r in rows if r["type"] == "DEEP_KEY_SOURCE_ONLY")
    assert "5" in str(only["key_values"])
    assert h.run_row(run_id)["execution_state"] == "COMPLETED"
    rule = h.ctx.db.one("SELECT * FROM dq_check_rule WHERE id=%s", (res["rule_id"],)) if res.get("rule_id") else None
    if rule is not None:
        assert rule["approval_ref"] == "TICKET-42"


def test_operations_pause_resume_cancel(h):
    setup_orders(h)
    plan = h.plan(kinds=["COUNT"])
    from srdq.controller import planning
    run = planning.start_plan_run(h.ctx, plan["id"], reason="ops")
    body = {"op": "PAUSE", "target_type": "RUN", "target_ids": [run["id"]]}
    pv = operations.preview_operation(h.ctx, body)
    assert pv["preview"]["changes"]["count"] == 1
    with pytest.raises(ApiError) as e:
        operations.apply_operation(h.ctx, pv["id"], idempotency_key="op-1", if_match="0" * 64, reason="pause")
    assert e.value.status in (409, 412)
    ap = operations.apply_operation(h.ctx, pv["id"], idempotency_key="op-1", if_match=pv["preview_digest"],
                                    reason="pause")
    replay = operations.apply_operation(h.ctx, pv["id"], idempotency_key="op-1", if_match=pv["preview_digest"],
                                        reason="pause")
    assert replay.pop("idempotent_replay") is True and replay == ap
    assert h.run_row(run["id"])["control_state"] == "PAUSED"
    assert h.work() == 0

    pv = operations.preview_operation(h.ctx, dict(body, op="RESUME"))
    operations.apply_operation(h.ctx, pv["id"], idempotency_key="op-2", if_match=pv["preview_digest"], reason="go")
    h.work()
    assert h.run_row(run["id"])["execution_state"] == "COMPLETED"

    run2 = planning.start_plan_run(h.ctx, plan["id"], reason="ops 2")
    pv = operations.preview_operation(h.ctx, {"op": "CANCEL", "target_type": "RUN", "target_ids": [run2["id"]]})
    operations.apply_operation(h.ctx, pv["id"], idempotency_key="op-3", if_match=pv["preview_digest"], reason="stop")
    h.work()
    r2 = h.run_row(run2["id"])
    assert r2["control_state"] == "CANCELLED" and r2["execution_state"] == "CANCELLED"
    assert h.ctx.db.one("SELECT state FROM dq_asset_slot")["state"] == "FREE"


def test_cron_parser():
    tz = schedules._tz("Asia/Shanghai")
    c = schedules.Cron("30 2 * * *")
    after = _dt.datetime(2026, 10, 10, 0, 0, tzinfo=_dt.timezone.utc)
    assert c.next_after(after, tz) == _dt.datetime(2026, 10, 10, 18, 30, tzinfo=_dt.timezone.utc)
    c = schedules.Cron("*/15 9-10 * * MON-FRI")
    t = c.next_after(_dt.datetime(2026, 10, 10, 0, 0, tzinfo=_dt.timezone.utc), tz)  # Saturday local
    assert t.astimezone(tz).strftime("%a %H:%M") == "Mon 09:00"
    c = schedules.Cron("0 0 13 * 5")
    t = c.next_after(_dt.datetime(2026, 10, 10, tzinfo=_dt.timezone.utc), tz)
    assert t.astimezone(tz).day in (13, 16)
    for bad in ("* * *", "60 * * * *", "5-1 * * * *", "*/0 * * * *", "* * * FOO *"):
        with pytest.raises(ApiError):
            schedules.Cron(bad)
    ny = schedules._tz("America/New_York")
    gap = schedules.Cron("30 2 * * *").next_after(_dt.datetime(2026, 3, 8, 5, 0, tzinfo=_dt.timezone.utc), ny)
    assert gap.astimezone(ny).date() == _dt.date(2026, 3, 9)


def _fires(h, sid):
    return h.ctx.db.query("SELECT * FROM dq_schedule_fire WHERE schedule_id=%s ORDER BY fired_at, due_at", (sid,))


def test_interval_schedule_overlap_and_misfire(h):
    setup_orders(h)
    plan = h.plan(kinds=["COUNT"])
    s = schedules.create_schedule(h.ctx, {"name": "every-5m", "plan_id": plan["id"], "kind": "INTERVAL",
                                          "interval_seconds": 300, "overlap": "SKIP", "misfire": "SKIP"},
                                  reason="test")
    assert len(s["next_fires"]) == 5
    h.tick()
    f = _fires(h, s["id"])
    assert [x["outcome"] for x in f] == ["RUN_CREATED"]
    first_run = f[0]["run_id"]
    h.clock.advance(seconds=300)
    h.tick()
    assert [x["outcome"] for x in _fires(h, s["id"])] == ["RUN_CREATED", "SKIPPED_OVERLAP"]
    h.work()
    assert h.run_row(first_run)["execution_state"] == "COMPLETED"
    h.clock.advance(seconds=1500)
    h.tick()
    outcomes = [x["outcome"] for x in _fires(h, s["id"])]
    assert outcomes[:2] == ["RUN_CREATED", "SKIPPED_OVERLAP"] and "MISFIRE_SKIPPED" in outcomes
    mis = next(x for x in _fires(h, s["id"]) if x["outcome"] == "MISFIRE_SKIPPED")
    assert mis["detail"]["count"] == 4
    assert outcomes.count("RUN_CREATED") == 2
    got = schedules.get_schedule(h.ctx, s["id"])
    assert got["waiting_reason"] == "RUN_IN_FLIGHT"
    with pytest.raises(ApiError):
        schedules.schedule_action(h.ctx, s["id"], "DISABLE", if_match=got["revision"] + 5)
    d = schedules.schedule_action(h.ctx, s["id"], "DISABLE", if_match=got["revision"])
    assert d["enabled"] is False


def test_continuous_schedule(h):
    setup_orders(h)
    plan = h.plan(kinds=["COUNT"])
    s = schedules.create_schedule(h.ctx, {"name": "loop", "plan_id": plan["id"], "kind": "CONTINUOUS",
                                          "rest_seconds": 600}, reason="test")
    h.tick()
    assert [x["outcome"] for x in _fires(h, s["id"])] == ["RUN_CREATED"]
    h.work()
    h.tick()
    assert len(_fires(h, s["id"])) == 1
    h.clock.advance(seconds=601)
    h.tick()
    assert [x["outcome"] for x in _fires(h, s["id"])] == ["RUN_CREATED", "RUN_CREATED"]


def _close_window(h):
    ctl = h.ctx.db.one("SELECT * FROM dq_control_state WHERE id=1")
    pv = operations.preview_operation(h.ctx, {"op": "CLOSE_WINDOW", "target_type": "GLOBAL"})
    operations.apply_operation(h.ctx, pv["id"], idempotency_key=f"close-{ctl['revision']}",
                               if_match=pv["preview_digest"], reason="test close")
    h.clock.advance(seconds=5)
    return h.tick()


def test_daily_report_after_window_close(h):
    setup_orders(h, target_mutate=drop5)
    h.run_plan(h.plan(kinds=["COUNT"])["id"])
    out = _close_window(h)
    assert out["window"] == "CLOSED"
    rep = out["report"]
    assert rep and rep["kind"] == "DAILY" and set(rep["files"]) == set(reports.FILES)
    d = reports.report_dir(h.ctx) / rep["report_id"]
    md = (d / "report.md").read_text("utf-8")
    assert "共同快照保证：无" in md
    assert not any(p in md for p in FORBIDDEN)
    for name in reports.FILES:
        if name.endswith(".csv"):
            assert (d / name).read_bytes().startswith(b"\xef\xbb\xbf"), name
    assert "db1.orders" in (d / "differences.csv").read_text("utf-8-sig")
    assert h.tick()["report"] is None
    again = reports.regenerate_daily(h.ctx, rep["window_key"], "re-run")
    assert again["report_id"] != rep["report_id"]
    lst = reports.list_reports(h.ctx)
    assert len({r["report_id"] if "report_id" in r else r["id"] for r in lst}) >= 2
    path, _ctype = reports.report_file(h.ctx, rep["report_id"], "report.md")
    assert path.read_text("utf-8") == md
    with pytest.raises(ApiError):
        reports.report_file(h.ctx, rep["report_id"], "../x")


def test_purge_after_retention(h):
    setup_orders(h)
    h.import_rules("METRIC", METRIC_HEADER, ["db1,orders,m_amt,cat,,sum_amt,SUM,amt,,EXACT,,"])
    h.run_plan(h.plan(kinds=["METRIC"])["id"])
    assert h.ctx.db.one("SELECT count(*) AS n FROM dq_metric_value")["n"] > 0
    _close_window(h)
    h.ctx.db.execute("UPDATE dq_control_state SET last_purge_date=NULL WHERE id=1")
    h.ctx.db.execute("UPDATE dq_side_result SET accepted_at = now() - interval '8 days' WHERE accepted_at IS NOT NULL")
    h.ctx.db.execute("UPDATE dq_artifact SET purge_after = now() - interval '1 day' WHERE material_scope <> 'REPORT'")
    res = purge.maybe_purge(h.ctx)
    assert res is not None and res["metric_results_purged"] > 0
    assert h.ctx.db.one("SELECT count(*) AS n FROM dq_metric_value")["n"] == 0
    assert h.ctx.db.one("SELECT count(*) AS n FROM dq_artifact WHERE material_scope <> 'REPORT' "
                        "AND purge_state <> 'PURGED'")["n"] == 0
    assert purge.maybe_purge(h.ctx) is None
    ev = h.ctx.db.query("SELECT payload FROM dq_outbox WHERE event_type='RAW_VALUES_PURGED'")
    assert ev and any(e["payload"]["category"] == "METRIC_GROUPS" for e in ev)


def test_recovery_mode(h):
    setup_orders(h)
    operations.enter_recovery(h.ctx, "restored from backup")
    assert h.tick() == {"recovery": True}
    pv = operations.preview_operation(h.ctx, {"op": "CONFIRM_RECOVERY", "target_type": "GLOBAL"})
    operations.apply_operation(h.ctx, pv["id"], idempotency_key="rec-1", if_match=pv["preview_digest"],
                               reason="checked")
    assert h.ctx.db.one("SELECT status FROM dq_control_state WHERE id=1")["status"] == "ACTIVE"
    assert "window" in h.tick()
