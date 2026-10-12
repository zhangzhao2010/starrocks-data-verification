from __future__ import annotations

from harness import DT_PARTITIONS, ORDER_COLS, Harness, order_rows
from srdq.controller import planning
from srdq.executor.runner import Active


def cluster(cluster_id: str, cloud: str) -> dict:
    prefix = "hw" if cloud == "HUAWEI" else "al"
    return {
        "id": cluster_id,
        "name": cluster_id,
        "cloud": cloud,
        "connection_ref": f"sr-{cluster_id}-ro",
        "credential_file": f"/tmp/{cluster_id}.json",
        "os_user": f"srdq-{prefix}{cluster_id[-1]}",
        "concurrency": {"COUNT": 2, "METRIC": 2, "SAMPLE": 2, "METADATA": 1, "DEEP": 1},
    }


def six_cluster_config() -> dict:
    clusters = []
    pairs = []
    for n in range(1, 4):
        source, target = f"huawei-{n}", f"aliyun-{n}"
        clusters.extend((cluster(source, "HUAWEI"), cluster(target, "ALIYUN")))
        pairs.append({"id": f"pair-{n}", "name": f"Pair {n}",
                      "source_cluster_id": source, "target_cluster_id": target})
    return {"physical_clusters": clusters, "cluster_pairs": pairs}


def test_three_pairs_keep_same_named_assets_isolated(db, tmp_dir):
    h = Harness(db, tmp_dir, six_cluster_config())
    try:
        columns = [("id", "bigint", False)]
        for n in range(1, 4):
            for cluster_id in (f"huawei-{n}", f"aliyun-{n}"):
                h.table(cluster_id, "db1", "orders", columns, [{"id": n}], model="PRI_KEYS", keys=["id"])
            h.import_assets(["ODS,db1,orders,db1,orders,50,true"], pair_id=f"pair-{n}")
        h.discover_and_confirm()
        assets = h.ctx.db.query("SELECT pair_id, source_table FROM dq_asset ORDER BY pair_id")
        assert [(a["pair_id"], a["source_table"]) for a in assets] == [
            ("pair-1", "orders"), ("pair-2", "orders"), ("pair-3", "orders")]

        plan = planning.create_plan(h.ctx, {"name": "pair-2-only", "check_selector": {"kinds": ["COUNT"]},
                                            "asset_selector": {"pair_ids": ["pair-2"]}}, reason="test")
        run = h.run_plan(plan["id"])
        run_assets = h.ctx.db.query("""SELECT DISTINCT a.pair_id FROM dq_run_check rc
                                       JOIN dq_asset a ON a.id=rc.asset_id WHERE rc.run_id=%s""", (run["id"],))
        assert [a["pair_id"] for a in run_assets] == ["pair-2"]
        attempts = h.ctx.db.query("SELECT DISTINCT cluster_id FROM dq_attempt WHERE work_item_id IN "
                                  "(SELECT id FROM dq_work_item WHERE run_id=%s) ORDER BY cluster_id", (run["id"],))
        assert [a["cluster_id"] for a in attempts] == ["aliyun-2", "huawei-2"]
        comparison = h.ctx.db.one("SELECT summary FROM dq_comparison WHERE run_id=%s", (run["id"],))
        assert comparison["summary"]["pair_id"] == "pair-2"
        assert comparison["summary"]["source_cluster_id"] == "huawei-2"
        assert comparison["summary"]["target_cluster_id"] == "aliyun-2"
    finally:
        h.close()


def test_metric_pool_claims_while_count_is_ready(db, tmp_dir):
    h = Harness(db, tmp_dir)
    try:
        rows = order_rows(300)
        h.both("db1", "orders", ORDER_COLS, rows, model="PRI_KEYS", keys=["id"], partition=DT_PARTITIONS)
        h.import_assets(["ODS,db1,orders,db1,orders,50,true"])
        h.discover_and_confirm()
        header = ["source_db", "source_table", "rule_key", "group_by", "filter", "metric_id", "function",
                  "column", "scale", "compare", "rtol", "atol"]
        h.import_rules("METRIC", header, ["db1,orders,m1,cat,,sum_amt,SUM,amt,2,EXACT,,"])
        plan = h.plan(kinds=["COUNT", "METRIC"])
        run = planning.start_plan_run(h.ctx, plan["id"], reason="test")
        h.tick()
        ready = h.ctx.db.query("""SELECT check_kind, count(*) AS n FROM dq_work_item
                                  WHERE run_id=%s AND cluster_id='huawei' AND status='READY' GROUP BY check_kind""",
                               (run["id"],))
        assert {r["check_kind"] for r in ready} == {"COUNT", "METRIC"}
        assert h.executors["huawei"].claim_once("METRIC") > 0
        h.wait_idle()
        attempts = h.ctx.db.query("""SELECT DISTINCT w.check_kind FROM dq_attempt a
                                     JOIN dq_work_item w ON w.id=a.work_item_id
                                     WHERE w.run_id=%s AND a.cluster_id='huawei'""", (run["id"],))
        assert {a["check_kind"] for a in attempts} == {"METRIC"}
        assert h.ctx.db.one("""SELECT count(*) AS n FROM dq_work_item WHERE run_id=%s AND cluster_id='huawei'
                               AND check_kind='COUNT' AND status='READY'""", (run["id"],))["n"] > 0
    finally:
        h.close()


def test_claim_scans_past_200_blocked_items_and_enforces_pool_limit(db, tmp_dir):
    source = cluster("huawei", "HUAWEI")
    target = cluster("aliyun", "ALIYUN")
    source["concurrency"].update({"COUNT": 2, "METADATA": 32})
    target["concurrency"].update({"COUNT": 2, "METADATA": 32})
    cfg = {"physical_clusters": [source, target],
           "cluster_pairs": [{"id": "legacy", "name": "Legacy", "source_cluster_id": "huawei",
                              "target_cluster_id": "aliyun"}]}
    h = Harness(db, tmp_dir, cfg)
    try:
        columns = [("id", "bigint", False)]
        assets = []
        for n in range(205):
            table = f"orders_{n:03d}"
            h.both("db1", table, columns, [{"id": n}], model="PRI_KEYS", keys=["id"])
            assets.append(f"ODS,db1,{table},db1,{table},50,true")
        h.import_assets(assets)
        h.discover_and_confirm()
        plan = h.plan(kinds=["COUNT"])
        run = planning.start_plan_run(h.ctx, plan["id"], reason="test")
        h.tick()
        blocked = [r["id"] for r in h.ctx.db.query(
            "SELECT id FROM dq_run_check WHERE run_id=%s ORDER BY ready_seq LIMIT 201", (run["id"],))]
        h.ctx.db.execute("UPDATE dq_run_check SET control_state='PAUSED' WHERE id = ANY(%s)", (blocked,))

        executor = h.executors["huawei"]
        claimed = executor.client.claim(executor.registration_id, executor.generation, 32, "COUNT")
        assert len(claimed["items"]) == 2
        assert not ({item["run_check_id"] for item in claimed["items"]} & {str(item) for item in blocked})
        limited = executor.client.claim(executor.registration_id, executor.generation, 32, "COUNT")
        assert limited == {"items": [], "reason": "CONCURRENCY"}
    finally:
        h.close()


def test_result_from_cluster_outside_frozen_pair_is_rejected(db, tmp_dir):
    h = Harness(db, tmp_dir, six_cluster_config())
    try:
        columns = [("id", "bigint", False)]
        for cluster_id in ("huawei-1", "aliyun-1"):
            h.table(cluster_id, "db1", "orders", columns, [{"id": 1}], model="PRI_KEYS", keys=["id"])
        h.import_assets(["ODS,db1,orders,db1,orders,50,true"], pair_id="pair-1")
        h.discover_and_confirm()
        plan = planning.create_plan(h.ctx, {"name": "pair-1", "check_selector": {"kinds": ["COUNT"]},
                                            "asset_selector": {"pair_ids": ["pair-1"]}}, reason="test")
        planning.start_plan_run(h.ctx, plan["id"], reason="test")
        h.tick()
        executor = h.executors["huawei-1"]
        response = executor.client.claim(executor.registration_id, executor.generation, 1, "COUNT")
        item = response["items"][0]
        h.ctx.db.execute("UPDATE dq_work_item SET cluster_id='huawei-2' WHERE id=%s", (item["work_item_id"],))
        executor.cluster_id = "huawei-2"

        active = Active(item)
        executor.active[active.attempt_id] = active
        executor._execute(active)
        attempt = h.ctx.db.one("SELECT status, error_code FROM dq_attempt WHERE id=%s", (item["attempt_id"],))
        assert attempt == {"status": "FAILED", "error_code": "PAIR_MISMATCH"}
        assert h.ctx.db.one("SELECT count(*) AS n FROM dq_side_result WHERE attempt_id=%s",
                            (item["attempt_id"],))["n"] == 0
    finally:
        h.close()
