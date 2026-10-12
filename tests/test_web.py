from __future__ import annotations

import datetime as _dt
import re
from html.parser import HTMLParser

import pytest
from starlette.testclient import TestClient

from harness import DT_PARTITIONS, ORDER_COLS, Harness, order_rows
from srdq.web.app import create_web_app

BASE = "http://127.0.0.1:8470"
ORIGIN = {"Origin": BASE}
CSV = b"layer,source_db,source_table,target_db,target_table,priority,enabled\nODS,db1,orders,db1,orders,50,true\n"


@pytest.fixture()
def h(db, tmp_dir):
    hh = Harness(db, tmp_dir)
    yield hh
    hh.close()


@pytest.fixture()
def web(h):
    with TestClient(create_web_app(h.ctx), base_url=BASE, follow_redirects=False) as c:
        yield c


def drop5(r):
    return None if r["id"] == 5 else r


def setup_diff(h: Harness) -> dict:
    h.both("db1", "orders", ORDER_COLS, order_rows(300), target_rows=order_rows(300, drop5), model="PRI_KEYS",
           keys=["id"], partition=DT_PARTITIONS)
    h.import_assets(["ODS,db1,orders,db1,orders,50,true"])
    h.discover_and_confirm()
    plan = h.plan(kinds=["COUNT"])
    run = h.run_plan(plan["id"])
    comp = h.ctx.db.one("SELECT * FROM dq_comparison WHERE run_id=%s AND verdict='OBSERVED_DIFFERENT'", (run["id"],))
    return {"plan": plan, "run": run, "comparison": comp}


def html_ok(resp) -> str:
    assert resp.status_code == 200, (resp.status_code, resp.text[:2000])
    assert resp.headers["content-type"].startswith("text/html")
    assert "<script" not in resp.text and " style=" not in resp.text
    return resp.text


# ---- security ----------------------------------------------------------------------------------------

def test_any_host_accepted(web):
    for host in ("127.0.0.1:8470", "localhost", "[::1]:8470", "10.0.0.5:8470", "srdq.example.com"):
        assert web.get("/v1/health", headers={"Host": host}).status_code == 200, host


def test_writes_from_instance_ip_accepted(web):
    origin = "http://10.0.0.5:8470"
    r = web.post("/v1/recovery", json={"reason": "x"}, headers={"Host": "10.0.0.5:8470", "Origin": origin})
    assert r.status_code not in (401, 403), r.text
    r = web.post("/actions/recovery", data={"reason": "x"}, headers={"Origin": "http://other.example.com"})
    assert r.status_code not in (401, 403), r.text


def test_security_headers(web):
    r = web.get("/")
    csp = r.headers["content-security-policy"]
    assert "default-src 'self'" in csp and "unsafe" not in csp and "frame-ancestors 'none'" in csp
    assert r.headers["x-content-type-options"] == "nosniff"
    assert r.headers["x-frame-options"] == "DENY"
    assert "access-control-allow-origin" not in r.headers
    r = web.options("/v1/health", headers={"Origin": "http://evil.example.com", "Access-Control-Request-Method": "POST"})
    assert "access-control-allow-origin" not in r.headers
    assert web.get("/static/app.css").status_code == 200
    assert web.get("/v1/does-not-exist").json()["code"] == "NOT_FOUND"
    assert "NOT_FOUND" in web.get("/does-not-exist", headers={"Accept": "text/html"}).text


def test_html_escaping(h, web):
    h.both("db1", "t", ORDER_COLS, order_rows(3), model="PRI_KEYS", keys=["id"], partition=DT_PARTITIONS)
    r = web.post("/v1/imports/preview", files={"file": ("<b>x</b>.csv", CSV.replace(b"orders,50", b"<i>t</i>,50"))},
                 data={"kind": "ASSET_LIST"}, headers=ORIGIN)
    assert r.status_code == 201, r.text
    assert "<i>t</i>" not in html_ok(web.get(f"/imports/{r.json()['id']}"))
    page = html_ok(web.get("/imports"))
    assert "<b>x</b>" not in page and "&lt;b&gt;x&lt;/b&gt;" in page


# ---- API ---------------------------------------------------------------------------------------------

def test_api_flow(h, web):
    h.both("db1", "orders", ORDER_COLS, order_rows(300), target_rows=order_rows(300, drop5), model="PRI_KEYS",
           keys=["id"], partition=DT_PARTITIONS)
    r = web.post("/v1/imports/preview", files={"file": ("a.csv", CSV)}, data={"kind": "ASSET_LIST"}, headers=ORIGIN)
    assert r.status_code == 201, r.text
    bid = r.json()["id"]
    r = web.post(f"/v1/imports/{bid}/confirm", json={"reason": "go"}, headers=ORIGIN)
    assert r.status_code == 200, r.text
    assets = web.get("/v1/assets").json()
    assert assets["total"] == 1
    aid = assets["items"][0]["id"]
    assert web.get(f"/v1/assets/{aid}").json()["source_table"] == "orders"
    assert web.get("/v1/assets/not-a-uuid").status_code == 422

    r = web.post("/v1/manifests/discover", json={"reason": "d"}, headers=ORIGIN)
    assert r.status_code == 201, r.text
    h.work()
    for _ in range(10):
        h.tick()
    mf = web.get("/v1/manifests").json()
    assert mf and mf[0]["status"] == "DRAFT", mf
    assert web.get(f"/v1/manifests/{mf[0]['id']}/scopes").json()["total"] >= 1
    r = web.post(f"/v1/manifests/{mf[0]['id']}/confirm", json={"reason": "ok"}, headers=ORIGIN)
    assert r.status_code == 200, r.text

    r = web.post("/v1/plans", json={"name": "p1", "check_selector": {"kinds": ["COUNT"]},
                                    "asset_selector": {"all": True}, "reason": "p"}, headers=ORIGIN)
    assert r.status_code == 201, r.text
    plan = r.json()
    r = web.patch(f"/v1/plans/{plan['id']}", json={"description": "x", "reason": "r"},
                  headers={**ORIGIN, "If-Match": str(plan["revision"] + 5)})
    assert r.status_code in (409, 422), r.text

    body = {"plan_id": plan["id"], "request_key": "rk-1", "reason": "run"}
    r1 = web.post("/v1/runs", json=body, headers=ORIGIN)
    assert r1.status_code == 201, r1.text
    r2 = web.post("/v1/runs", json=body, headers=ORIGIN)
    assert r2.json()["id"] == r1.json()["id"], "request_key makes run creation idempotent"
    run_id = r1.json()["id"]
    h.work()
    run = web.get(f"/v1/runs/{run_id}").json()
    assert run["execution_state"] == "COMPLETED", run
    assert web.get(f"/v1/runs/{run_id}/checks").json()["total"] >= 1
    assert web.get(f"/v1/runs/{run_id}/nodes").json()["total"] >= 1

    diffs = web.get("/v1/differences").json()
    assert diffs["total"] >= 1
    comp_id = diffs["items"][0]["comparison_id"]
    c = web.get(f"/v1/comparisons/{comp_id}").json()
    assert c["verdict"] == "OBSERVED_DIFFERENT"
    assert web.get(f"/v1/comparisons/{comp_id}/differences").json()["total"] >= 1

    dash = web.get("/v1/dashboard").json()
    assert "window" in dash and "clusters" in dash and set(dash["clusters"]) == {"huawei", "aliyun"}
    assert dash["latest_verdicts"]["COUNT"].get("OBSERVED_DIFFERENT", 0) >= 1
    assert web.get("/v1/control").json()["status"]
    assert len(web.get("/v1/workers").json()) == 2
    assert web.get("/v1/audit").json()["total"] > 0
    assert web.get("/v1/incidents").status_code == 200


def test_api_operation_preview_apply(h, web):
    s = setup_diff(h)
    r = web.post("/v1/runs", json={"plan_id": s["plan"]["id"], "request_key": "ops-1", "reason": "r"}, headers=ORIGIN)
    run_id = r.json()["id"]
    r = web.post("/v1/operations/preview", json={"op": "PAUSE", "target_type": "RUN", "target_ids": [run_id]},
                 headers=ORIGIN)
    assert r.status_code == 201, r.text
    op = r.json()
    r = web.post(f"/v1/operations/{op['id']}/apply", json={"reason": "go"},
                 headers={**ORIGIN, "Idempotency-Key": "k1", "If-Match": "wrong"})
    assert r.status_code in (409, 412, 422), r.text
    r = web.post(f"/v1/operations/{op['id']}/apply", json={"reason": "go"},
                 headers={**ORIGIN, "Idempotency-Key": "k1", "If-Match": op["preview_digest"]})
    assert r.status_code == 200, r.text
    again = web.post(f"/v1/operations/{op['id']}/apply", json={"reason": "go"},
                     headers={**ORIGIN, "Idempotency-Key": "k1", "If-Match": op["preview_digest"]})
    assert again.status_code == 200, again.text
    assert web.get(f"/v1/runs/{run_id}").json()["control_state"] == "PAUSED"
    assert web.get("/v1/operations").status_code == 200


def test_reports_api_and_download(h, web):
    setup_diff(h)
    today = h.clock.now().date().isoformat()
    r = web.post("/v1/reports", json={"start_date": today, "end_date": today, "reason": "stage"}, headers=ORIGIN)
    assert r.status_code == 201, r.text
    reps = web.get("/v1/reports").json()
    assert reps and "report.md" in reps[0]["files"]
    rid = reps[0]["id"]
    md = web.get(f"/v1/reports/{rid}/files/report.md")
    assert md.status_code == 200 and md.headers["content-type"].startswith("text/markdown")
    csv = web.get(f"/v1/reports/{rid}/files/differences.csv")
    assert csv.status_code == 200 and csv.content.startswith(b"\xef\xbb\xbf")
    assert web.get(f"/v1/reports/{rid}/files/..%2F..%2Fetc%2Fpasswd").status_code == 404
    assert web.get(f"/v1/reports/{rid}/files/secret.txt").status_code == 404


# ---- pages -------------------------------------------------------------------------------------------

def test_all_pages_render(h, web):
    s = setup_diff(h)
    comp = s["comparison"]
    run_id = s["run"]["id"]
    today = h.clock.now().date().isoformat()
    web.post("/v1/reports", json={"start_date": today, "end_date": today, "reason": "stage"}, headers=ORIGIN)
    web.post("/v1/schedules", json={"name": "nightly", "plan_id": s["plan"]["id"], "kind": "CRON",
                                    "cron": "0 1 * * *", "timezone": "Asia/Shanghai", "reason": "s"}, headers=ORIGIN)
    mr = web.post("/v1/manual-requests/preview", json={
        "request_key": "m1", "check_kind": "COUNT", "asset_id": str(comp["asset_id"]),
        "origin_comparison_id": str(comp["id"]), "scope_keys": [comp["scope_key"]], "reason": "recheck"},
        headers=ORIGIN)
    assert mr.status_code == 201, mr.text
    op = web.post("/v1/operations/preview", json={"op": "PAUSE", "target_type": "RUN", "target_ids": [run_id],
                                                  "reason": "x"}, headers=ORIGIN)

    db = h.ctx.db
    ids = {
        "asset": db.one("SELECT id FROM dq_asset LIMIT 1")["id"],
        "import": db.one("SELECT id FROM dq_import_batch LIMIT 1")["id"],
        "manifest": db.one("SELECT id FROM dq_manifest LIMIT 1")["id"],
        "schedule": (db.one("SELECT id FROM dq_schedule LIMIT 1") or {}).get("id"),
    }
    pages = ["/", "/assets", f"/assets/{ids['asset']}", "/assets?missing_kind=METRIC", "/imports",
             f"/imports/{ids['import']}", "/rules", "/manifests", f"/manifests/{ids['manifest']}", "/plans",
             f"/plans/{s['plan']['id']}", "/schedules", "/runs", f"/runs/{run_id}", "/runs?active=1",
             "/operations", "/differences", "/differences?kind=COUNT", f"/comparisons/{comp['id']}",
             f"/comparisons/{comp['id']}?show_values=1", "/manual-requests", f"/manual-requests/{mr.json()['id']}",
             "/reports", "/incidents", "/incidents?state=OPEN", "/audit", "/audit?object_type=dq_run", "/workers"]
    if ids["schedule"]:
        pages.append(f"/schedules/{ids['schedule']}")
    if op.status_code == 201:
        pages.append(f"/operations/{op.json()['id']}")
    for p in pages:
        text = html_ok(web.get(p))
        assert "SRDQ" in text, p
    dash = web.get("/").text
    assert "COUNT" in dash and "huawei" in dash


def test_differences_masked_by_default(h, web):
    s = setup_diff(h)
    comp = s["comparison"]
    api = web.get(f"/v1/comparisons/{comp['id']}/differences").json()
    assert api["items"] and all(i.get("masked") for i in api["items"])
    shown = web.get(f"/v1/comparisons/{comp['id']}/differences?show_values=1").json()
    assert shown["items"] and not any(i.get("masked") for i in shown["items"])


def test_form_actions_redirect(h, web):
    s = setup_diff(h)
    r = web.post(f"/actions/plans/{s['plan']['id']}/run", data={"request_key": "ui-1", "reason": "ui run"},
                 headers=ORIGIN)
    assert r.status_code == 303 and re.match(r"^/runs/[0-9a-f-]{36}$", r.headers["location"]), r.headers
    r = web.post("/actions/operations/preview", data={"op": "PAUSE", "target_type": "RUN",
                                                      "target_ids": s["run"]["id"]}, headers=ORIGIN)
    assert r.status_code in (303, 409, 422), r.text
    r = web.post("/actions/reports/stage", data={"start_date": "2026-01-02", "end_date": "2026-01-01"},
                 headers={**ORIGIN, "Accept": "text/html"})
    assert r.status_code in (303, 422)
    if r.status_code == 422:
        assert "出错了" in r.text
    r = web.post("/actions/recovery", data={}, headers={**ORIGIN, "Accept": "text/html"})
    assert r.status_code == 422 and "出错了" in r.text
    r = web.post("/actions/recovery", data={"reason": "restore drill"}, headers=ORIGIN)
    assert r.status_code == 303
    assert web.get("/v1/control").json()["status"] == "RECOVERY"
    assert "RECOVERY" in web.get("/").text


def test_schedule_preview_page(h, web):
    s = setup_diff(h)
    r = web.post("/actions/schedules", data={"name": "n1", "plan_id": s["plan"]["id"], "kind": "INTERVAL",
                                             "interval_minutes": "1440", "preview_only": "1"}, headers=ORIGIN)
    assert r.status_code in (200, 422), r.text[:1000]



# ---- UI flow: submit the forms exactly as rendered -----------------------------------------------------

class _Forms(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.forms: list[dict] = []
        self._cur: dict | None = None
        self._select: str | None = None
        self._first_option: str | None = None
        self._textarea: str | None = None

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if tag == "form":
            self._cur = {"action": a.get("action", ""), "method": a.get("method", "get"), "fields": [],
                         "enctype": a.get("enctype", "")}
            self.forms.append(self._cur)
        elif self._cur is None:
            return
        elif tag == "input" and a.get("name"):
            t = a.get("type", "text")
            if t in ("checkbox", "radio"):
                if "checked" in a:
                    self._cur["fields"].append((a["name"], a.get("value", "on")))
            elif t != "file":
                self._cur["fields"].append((a["name"], a.get("value", "")))
        elif tag == "select":
            self._select, self._first_option = a.get("name"), None
        elif tag == "option" and self._select:
            if self._first_option is None:
                self._first_option = a.get("value", "")
                self._cur["fields"].append((self._select, self._first_option))
            if "selected" in a:
                self._cur["fields"] = [f for f in self._cur["fields"] if f[0] != self._select]
                self._cur["fields"].append((self._select, a.get("value", "")))
        elif tag == "textarea":
            self._textarea = a.get("name")
            self._cur["fields"].append((self._textarea, ""))

    def handle_data(self, data):
        if self._textarea and self._cur:
            name, val = self._cur["fields"][-1]
            self._cur["fields"][-1] = (name, val + data)

    def handle_endtag(self, tag):
        if tag == "form":
            self._cur = None
        elif tag == "select":
            self._select = None
        elif tag == "textarea":
            self._textarea = None


def submit(web, url: str, action: str, fill: dict | None = None, *, contains: str | None = None, files=None,
           expect: int = 303):
    html = html_ok(web.get(url))
    p = _Forms()
    p.feed(html)
    forms = [f for f in p.forms if f["method"].lower() == "post" and re.fullmatch(action, f["action"])]
    if contains:
        forms = [f for f in forms if any(contains == v for _k, v in f["fields"])]
    assert forms, (url, action, [f["action"] for f in p.forms])
    form = forms[0]
    fill = dict(fill or {})
    fields = [(k, v) for k, v in form["fields"] if k not in fill]
    for k, v in fill.items():
        for x in (v if isinstance(v, list) else [v]):
            fields.append((k, x))
    data: dict = {}
    for k, v in fields:
        data.setdefault(k, []).append(v)
    r = web.post(form["action"], data=data, files=files, headers={**ORIGIN, "Accept": "text/html"})
    assert r.status_code == expect, (form["action"], data, r.status_code, r.text[:3000])
    return r.headers.get("location"), r


def test_ui_flow_end_to_end(h, web):
    h.both("db1", "orders", ORDER_COLS, order_rows(300), target_rows=order_rows(300, drop5), model="PRI_KEYS",
           keys=["id"], partition=DT_PARTITIONS)
    loc, _ = submit(web, "/imports", "/actions/imports/preview", {"kind": "ASSET_LIST"},
                    files={"file": ("assets.csv", CSV, "text/csv")})
    assert loc.startswith("/imports/")
    submit(web, loc, rf"/actions/imports/[^/]+/confirm", {"reason": "导入资产"})
    assert h.ctx.db.one("SELECT count(*) AS n FROM dq_asset")["n"] == 1

    submit(web, "/manifests", "/actions/manifests/discover", {"reason": "发现分区"})
    h.work()
    for _ in range(10):
        h.tick()
    mid = h.ctx.db.one("SELECT id FROM dq_manifest ORDER BY version DESC LIMIT 1")["id"]
    submit(web, f"/manifests/{mid}", r"/actions/manifests/[^/]+/confirm", {"reason": "确认"})
    assert h.ctx.db.one("SELECT status FROM dq_manifest WHERE id=%s", (mid,))["status"] == "CONFIRMED"

    metric = ("source_db,source_table,rule_key,group_by,filter,metric_id,function,column,scale,compare,rtol,atol\n"
              "db1,orders,m1,dt,,amt,SUM,amount,2,EXACT,,\n").encode()
    loc, _ = submit(web, "/imports", "/actions/imports/preview", {"kind": "METRIC_TEMPLATE"},
                    files={"file": ("metric.csv", metric, "text/csv")})
    submit(web, loc, r"/actions/imports/[^/]+/confirm", {"reason": "规则"})
    draft = h.ctx.db.query("SELECT id FROM dq_check_rule WHERE status='DRAFT'")
    if draft:
        submit(web, "/rules", "/actions/rules/approve",
               {"rule_ids": [str(d["id"]) for d in draft], "reason": "批准", "approval_ref": "APPROVAL-1"})
        assert not h.ctx.db.query("SELECT id FROM dq_check_rule WHERE status='DRAFT'")

    loc, _ = submit(web, "/plans", "/actions/plans", {"name": "夜间", "kinds": "COUNT", "reason": "建计划"})
    assert loc.startswith("/plans/")
    run_loc, _ = submit(web, loc, r"/actions/plans/[^/]+/run", {"reason": "开跑"})
    assert run_loc.startswith("/runs/")
    run_id = run_loc.rsplit("/", 1)[1]
    html_ok(web.get(run_loc))
    op_loc, _ = submit(web, run_loc, "/actions/operations/preview", {"op": "PAUSE"}, contains="RUN")
    assert op_loc.startswith("/operations/")
    submit(web, op_loc, r"/actions/operations/[^/]+/apply", {"reason": "暂停"})
    assert h.run_row(run_id)["control_state"] == "PAUSED"
    op_loc, _ = submit(web, run_loc, "/actions/operations/preview", {"op": "RESUME"}, contains="RUN")
    submit(web, op_loc, r"/actions/operations/[^/]+/apply", {"reason": "继续"})
    h.work()
    assert h.run_row(run_id)["execution_state"] == "COMPLETED"

    comp = h.ctx.db.one("SELECT id FROM dq_comparison WHERE verdict='OBSERVED_DIFFERENT' LIMIT 1")
    assert comp
    html_ok(web.get("/differences"))
    mr_loc, _ = submit(web, f"/comparisons/{comp['id']}", "/actions/manual-requests/preview", {"reason": "复核"})
    assert mr_loc.startswith("/manual-requests/")
    submit(web, mr_loc, r"/actions/manual-requests/[^/]+/confirm", {"reason": "确认复核"})
    assert h.ctx.db.one("SELECT count(*) AS n FROM dq_run WHERE purpose='MANUAL'")["n"] == 1

    plan_id = h.ctx.db.one("SELECT id FROM dq_plan LIMIT 1")["id"]
    sc_loc, _ = submit(web, "/schedules", "/actions/schedules",
                       {"name": "每晚", "plan_id": str(plan_id), "kind": "CRON", "cron_expr": "0 1 * * *",
                        "reason": "周期"})
    assert sc_loc.startswith("/schedules/"), sc_loc
    submit(web, sc_loc, r"/actions/schedules/[^/]+/action", {"reason": "停用周期"}, contains="DISABLE")
    assert not h.ctx.db.one("SELECT enabled FROM dq_schedule LIMIT 1")["enabled"]

    op_loc, _ = submit(web, "/", "/actions/operations/preview", contains="CLOSE_WINDOW")
    submit(web, op_loc, r"/actions/operations/[^/]+/apply", {"reason": "提前关窗"})
    h.tick()
    assert h.ctx.db.one("SELECT window_state FROM dq_control_state")["window_state"] in ("CLOSED", "DRAINING")

    today = h.clock.now().date().isoformat()
    submit(web, "/reports", "/actions/reports/stage", {"start_date": today, "end_date": today, "reason": "阶段"})
    assert "report.md" in html_ok(web.get("/reports"))

    submit(web, "/workers", r"/actions/workers/[^/]+/disable", {"reason": "下线"})
    submit(web, "/", "/actions/recovery", {"reason": "演练"})
    assert "RECOVERY" in html_ok(web.get("/"))
