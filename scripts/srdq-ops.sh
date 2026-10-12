#!/usr/bin/env bash
# srdq 运维脚本：在 ECS 本机通过 REST API 完成资产导入、范围发现、清单、规则、计划、周期和差异查看。
# 用法: ./srdq-ops.sh help
set -euo pipefail
shopt -s inherit_errexit

API="${SRDQ_API:-http://127.0.0.1:8470/v1}"
POLL="${SRDQ_POLL:-60}"
SELF="$(basename "$0")"
UUID_RE='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'

die() { echo "错误: $*" >&2; exit 1; }

PY=""
for c in /opt/srdq/current/venv/bin/python python3.11 python3; do
    if command -v "$c" >/dev/null 2>&1; then PY="$(command -v "$c")"; break; fi
done
[[ -n $PY ]] || die "找不到 python3"
command -v curl >/dev/null 2>&1 || die "找不到 curl"

TMP="$(mktemp -d "${TMPDIR:-/tmp}/srdq-ops.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
RESP="$TMP/resp.json"
BODY="$TMP/body.json"
HELPER="$TMP/helper.py"

cat >"$HELPER" <<'PYEOF'
import json, os, sys, unicodedata, urllib.parse
from datetime import datetime

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo(os.environ.get("SRDQ_TZ") or "Asia/Shanghai")
except Exception:
    TZ = None

STATE_ZH = {"NEW": "新出现", "PERSISTENT": "持续", "RESOLVED": "已恢复"}
VERDICT_ZH = {"OBSERVED_EQUAL": "一致", "OBSERVED_DIFFERENT": "不一致", "INCONCLUSIVE": "无法判定",
              "ERROR": "错误", "UNSUPPORTED": "不支持"}
HINTS = {
    "DISCOVERY_RUNNING": "已有范围发现在进行，用 discover-status 查看进度",
    "NO_ASSETS": "还没有资产，先执行 import-assets",
    "NO_CONFIRMED_MANIFEST": "还没有已确认的范围清单，先执行 manifest-confirm",
    "NOTHING_TO_CHECK": "没有已批准的规则匹配该计划，先执行 import-rules / approve",
    "PREVIEW_STALE": "预览后数据已变化，重新执行本命令即可",
    "MANIFEST_STALE": "清单已过期（有更新的发现结果），重新 discover 后再确认",
    "MANIFEST_NOT_DRAFT": "只有 DRAFT 状态的清单可以确认或驳回",
    "APPROVAL_REF_REQUIRED": "配置中未登记全局审批，请用 --approval-ref 指定审批单号",
    "NAME_TAKEN": "名称已被占用，换一个名称",
    "PLAN_ARCHIVED": "计划已归档",
    "REVISION_MISMATCH": "对象已被修改，重新执行本命令",
}


def load(p):
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def ts(v):
    if not v:
        return "-"
    try:
        d = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
        if d.tzinfo and TZ:
            d = d.astimezone(TZ)
        return d.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(v)


def dw(s):
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in s)


def cut(v, n=0):
    if v is None:
        s = "-"
    elif isinstance(v, bool):
        s = "是" if v else "否"
    elif isinstance(v, (dict, list)):
        s = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    else:
        s = str(v)
    s = s.replace("\r", " ").replace("\n", " ")
    if n and dw(s) > n:
        out, w = "", 0
        for c in s:
            if w + dw(c) > n - 1:
                break
            out += c
            w += dw(c)
        s = out + "…"
    return s


def table(rows, cols):
    cells = [[cut(g(r), m) for _, g, m in cols] for r in rows]
    heads = [h for h, _, _ in cols]
    widths = [max([dw(h)] + [dw(c[i]) for c in cells]) for i, h in enumerate(heads)]

    def line(vals):
        return "  ".join(v + " " * (widths[i] - dw(v)) for i, v in enumerate(vals)).rstrip()

    print(line(heads))
    print(line(["-" * w for w in widths]))
    for c in cells:
        print(line(c))
    if not rows:
        print("（无）")


def k(name):
    return lambda r: r.get(name)


def tbl(r):
    return f"{r.get('source_db')}.{r.get('source_table')}"


def compact(v):
    return json.dumps(v, ensure_ascii=False, separators=(",", ":"))


def pager(d):
    total, page, size = d.get("total", 0), d.get("page", 1), d.get("page_size", 0)
    pages = d.get("pages") or (max(1, -(-total // size)) if size else 1)
    print(f"共 {total} 条，第 {page}/{pages} 页（每页 {size}，用 --page / --limit 翻页）")


# ---- generic -------------------------------------------------------------------------------------

def c_error(a):
    path, code = a[0], a[1]
    try:
        b = load(path)
    except Exception:
        with open(path, encoding="utf-8", errors="replace") as f:
            print(f"HTTP {code}: {f.read()[:800]}")
        return
    if isinstance(b, dict) and "code" in b:
        print(f"HTTP {code} {b['code']}: {b.get('message', '')}")
        if b.get("detail") not in (None, {}, []):
            print("详情: " + compact(b["detail"]))
        if b["code"] in HINTS:
            print("提示: " + HINTS[b["code"]])
    else:
        print(f"HTTP {code}: {compact(b)}")


def c_get(a):
    obj = load(a[0])
    default = a[2] if len(a) > 2 else ""
    for part in a[1].split("."):
        if isinstance(obj, list):
            try:
                obj = obj[int(part)]
            except (ValueError, IndexError):
                obj = None
        elif isinstance(obj, dict):
            obj = obj.get(part)
        else:
            obj = None
        if obj is None:
            break
    if obj is None:
        print(default)
    elif isinstance(obj, (dict, list)):
        print(compact(obj))
    elif isinstance(obj, bool):
        print("true" if obj else "false")
    else:
        print(obj)


def c_pretty(a):
    print(json.dumps(load(a[0]), ensure_ascii=False, indent=2))


def c_tsv_table(a):
    with open(a[0], encoding="utf-8") as f:
        lines = [x.rstrip("\n").split("\t") for x in f if x.strip()]
    heads = lines[0]
    table(lines[1:], [(h, (lambda i: lambda r: r[i])(i), 0) for i, h in enumerate(heads)])


def c_body(a):
    out = {}
    for kv in a:
        i = kv.index("=")
        if i > 0 and kv[i - 1] == ":":
            out[kv[:i - 1]] = json.loads(kv[i + 1:])
        else:
            out[kv[:i]] = kv[i + 1:]
    print(json.dumps(out, ensure_ascii=False))


def c_qs(a):
    pairs = [kv.split("=", 1) for kv in a]
    pairs = [(x, y) for x, y in pairs if y != ""]
    print("?" + urllib.parse.urlencode(pairs) if pairs else "")


def split_list(s, upper=False):
    items = [x.strip() for x in (s or "").split(",") if x.strip()]
    return [x.upper() for x in items] if upper else items


def c_csel(a):
    kinds, keys = split_list(a[0], True), split_list(a[1])
    if not kinds and not keys:
        print(compact({"all": True}))
        return
    out = {"kinds": kinds or ["COUNT", "METRIC", "SAMPLE"]}
    if keys:
        out["rule_keys"] = keys
    print(compact(out))


def c_asel(a):
    layers, ids = split_list(a[0], True), split_list(a[1])
    if layers and ids:
        sys.exit("--layers 和 --assets/--tables 不能同时使用")
    if ids:
        print(compact({"asset_ids": ids}))
    elif layers:
        print(compact({"layers": layers}))
    else:
        print(compact({"all": True}))


def c_asset_exact(a):
    d, name = load(a[0]), a[1]
    hits = [r for r in d.get("items", []) if tbl(r).lower() == name.lower()]
    if not hits:
        sys.exit(f"找不到资产 {name}（格式: 源库.源表）")
    print(hits[0]["id"])


# ---- status --------------------------------------------------------------------------------------

def window_lines(c):
    w = c.get("window") or {}
    st = w.get("state")
    if st == "OPEN":
        print(f"执行窗口: 开启中（{ts(w.get('open_at'))} 至 {ts(w.get('close_at'))}），任务会尽快开始执行")
    elif st == "DRAINING":
        print(f"执行窗口: 收尾中，不再派发新任务；未完成部分在下次窗口 {ts(c.get('next_open'))} 继续")
    else:
        nxt = c.get("next_open")
        print(f"执行窗口: 已关闭。任务已登记，将在下次窗口 {ts(nxt)} 开始执行" if nxt else "执行窗口: 已关闭")
    if c.get("status") and c.get("status") != "ACTIVE":
        print(f"注意: 控制状态为 {c['status']}，不是 ACTIVE 时不会派发任务")


def c_window(a):
    window_lines(load(a[0]))


def c_status(a):
    d, c = load(a[0]), load(a[1])
    ctl = d.get("control", {})
    print(f"当前时间: {ts(d.get('now'))}")
    print(f"控制状态: {ctl.get('status')}    允许运行(run_enabled): {'是' if ctl.get('run_enabled') else '否'}")
    window_lines(c)
    print()
    print("执行器:")
    table(d.get("workers", []), [
        ("名称", k("name"), 30), ("侧", k("side"), 8), ("状态", k("status"), 10),
        ("注册", k("registration_status"), 12), ("最近心跳", lambda r: ts(r.get("last_heartbeat_at")), 19),
        ("SR版本", k("sr_version"), 20)])
    for side, st in (d.get("clusters") or {}).items():
        if st.get("hard_stop") or st.get("cancel_unconfirmed"):
            print(f"  {side}: hard_stop={st.get('hard_stop')} 未确认取消={st.get('cancel_unconfirmed')} "
                  f"触发={compact(st.get('triggers'))}")
    print()
    print("进行中的运行:")
    table(d.get("active_runs", []), [
        ("ID", k("id"), 36), ("类型", k("purpose"), 10), ("计划", k("plan_name"), 24),
        ("周期", k("schedule_name"), 20), ("执行", k("execution_state"), 14), ("控制", k("control_state"), 10),
        ("进度", progress_text, 60)])
    print()
    alerts = d.get("alerts", [])
    print(f"未处理告警: {len(alerts)} 条" + ("（前 10 条）" if len(alerts) > 10 else ""))
    if alerts:
        table(alerts[:10], [
            ("级别", k("severity"), 8), ("类型", k("kind"), 24), ("侧", k("side"), 8),
            ("次数", k("occurrences"), 6), ("最近", lambda r: ts(r.get("last_seen")), 19), ("信息", k("message"), 70)])
    gaps = d.get("rule_gaps", {})
    print()
    print(f"资产: {gaps.get('assets', 0)} 个启用；缺少已批准 METRIC 规则 {gaps.get('METRIC', 0)} 个，"
          f"缺少已批准 SAMPLE 规则 {gaps.get('SAMPLE', 0)} 个")
    lv = d.get("latest_verdicts") or {}
    if lv:
        print("最新结论: " + "；".join(
            f"{kind} 一致 {v.get('OBSERVED_EQUAL', 0)} / 不一致 {v.get('OBSERVED_DIFFERENT', 0)}"
            for kind, v in lv.items()))


# ---- imports -------------------------------------------------------------------------------------

def summary_text(s):
    return "，".join(f"{x} {y}" for x, y in s.items() if x not in ("applied", "total")) + f"，合计 {s.get('total', 0)}"


def c_import_preview(a):
    d, kind = load(a[0]), a[1]
    rows = d.get("rows", [])
    print(f"预览批次: {d.get('id')}  类型: {d.get('kind')}  模式: {d.get('mode')}")
    print("预览结果: " + summary_text(d.get("summary", {})))
    errs = [r for r in rows if r.get("status") == "ERROR"]
    if kind == "asset":
        if errs:
            print(f"ERROR 行（{len(errs)} 行，最多显示 30 行）:")
            for r in errs[:30]:
                v = r.get("values") or {}
                print(f"  第 {r.get('line')} 行 {v.get('source_db')}.{v.get('source_table')}: "
                      + "；".join(r.get("errors") or []))
        ups = [r for r in rows if r.get("status") == "UPDATE"]
        if ups:
            print(f"将更新的资产（{len(ups)} 个，最多显示 20 个）:")
            for r in ups[:20]:
                v = r.get("values") or {}
                ch = "，".join(f"{f}: {o} -> {n}" for f, (o, n) in (r.get("changes") or {}).items())
                print(f"  第 {r.get('line')} 行 {v.get('source_db')}.{v.get('source_table')}: {ch}")
    else:
        if errs:
            print(f"ERROR 规则（{len(errs)} 条，最多显示 30 条）:")
            for r in errs[:30]:
                print(f"  行 {cut(r.get('lines'), 20)} {tbl(r)} [{r.get('rule_key')}]: "
                      + "；".join(r.get("errors") or []))
        un = [r for r in rows if r.get("status") == "UNSUPPORTED"]
        if un:
            print(f"UNSUPPORTED 规则（{len(un)} 条，可导入但当前版本无法执行，最多显示 10 条）:")
            for r in un[:10]:
                print(f"  {tbl(r)} [{r.get('rule_key')}]: " + "；".join(r.get("unsupported") or []))


def c_import_confirm(a):
    d = load(a[0])
    ap = (d.get("summary") or {}).get("applied") or {}
    print(f"已确认批次 {d.get('id')}：" + "，".join(f"{x} {y}" for x, y in ap.items()))


def c_assets(a):
    d = load(a[0])
    table(d.get("items", []), [
        ("层", k("layer"), 6), ("源表", tbl, 50), ("目标表", lambda r: f"{r.get('target_db')}.{r.get('target_table')}", 50),
        ("优先级", k("priority"), 6), ("启用", k("enabled"), 4), ("状态", k("status"), 8),
        ("已批准规则", k("approved_rules"), 10), ("待批准", k("draft_rules"), 6), ("ID", k("id"), 36)])
    pager(d)


# ---- runs ----------------------------------------------------------------------------------------

def progress_text(r):
    parts = []
    for p in r.get("progress") or []:
        s = f"{p.get('check_kind')} {p.get('done', 0)}/{p.get('total', 0)}"
        if p.get("different"):
            s += f" 不一致{p['different']}"
        if p.get("incomplete"):
            s += f" 未完成{p['incomplete']}"
        parts.append(s)
    return "；".join(parts) or "-"


def c_discover(a):
    d = load(a[0])
    print(f"已发起范围发现: run_id={d.get('run_id')}，资产 {d.get('assets')} 个，工作项 {d.get('work_items')} 个")


def c_runs(a):
    d = load(a[0])
    table(d.get("items", []), [
        ("ID", k("id"), 36), ("类型", k("purpose"), 16), ("计划", k("plan_name"), 24), ("周期", k("schedule_name"), 20),
        ("执行", k("execution_state"), 14), ("控制", k("control_state"), 10), ("窗口数", k("windows_used"), 6),
        ("创建", lambda r: ts(r.get("created_at")), 19), ("完成", lambda r: ts(r.get("completed_at")), 19),
        ("进度", progress_text, 60)])
    pager(d)


def c_run(a):
    r = load(a[0])
    print(f"运行 ID:   {r.get('id')}")
    print(f"类型:      {r.get('purpose')}    计划: {r.get('plan_name') or '-'}    周期: {r.get('schedule_name') or '-'}")
    print(f"执行状态:  {r.get('execution_state')}    控制状态: {r.get('control_state')}    "
          f"已用窗口: {r.get('windows_used', 0)}")
    print(f"创建:      {ts(r.get('created_at'))}    完成: {ts(r.get('completed_at'))}")
    if r.get("manifest_id"):
        print(f"清单:      {r.get('manifest_id')}")
    if r.get("reason"):
        print(f"原因:      {r.get('reason')}")
    if r.get("progress"):
        print("进度:      " + progress_text(r))
    cov = r.get("coverage") or {}
    for kind, st in cov.items():
        print(f"  {kind}: " + "，".join(f"{x} {y}" for x, y in sorted(st.items())))
    live = r.get("live_attempts")
    if live:
        print(f"正在执行的尝试: {live} 个")


COVERAGE_ZH = {"PENDING": "待执行", "COMPLETED_EQUAL": "一致", "COMPLETED_DIFFERENT": "不一致",
               "INCONCLUSIVE": "无法判定", "ERROR": "错误", "UNSUPPORTED": "不支持", "BLOCKED": "阻塞",
               "EXCLUDED": "排除"}


def c_checks(a):
    d = load(a[0])
    table(d.get("items", []), [
        ("类型", k("check_kind"), 7), ("层", k("layer"), 5), ("源表", tbl, 44),
        ("范围", lambda r: r.get("scope_label") or r.get("scope_key"), 36), ("rule_key", k("rule_key"), 24),
        ("覆盖", lambda r: f"{r.get('coverage_state')}/{COVERAGE_ZH.get(r.get('coverage_state'), '')}", 30),
        ("原因", k("coverage_reason"), 28), ("执行", k("execution_state"), 12), ("控制", k("control_state"), 8),
        ("失败次数", k("attempts_failed"), 8), ("更新", lambda r: ts(r.get("coverage_updated_at")), 19),
        ("comparison_id", k("current_comparison_id"), 36)])
    pager(d)


def c_operation_preview(a):
    d = load(a[0])
    p = d.get("preview") or {}
    print(f"操作预览: {p.get('op')} {p.get('target_type')}    operation_id={d.get('id')}")
    checks = p.get("run_checks") or {}
    if checks:
        by = checks.get("by_execution_state") or {}
        print(f"涉及检查项: {checks.get('total', 0)}" +
              ("（" + "，".join(f"{x} {y}" for x, y in by.items()) + "）" if by else ""))
    flight = p.get("in_flight") or {}
    if flight:
        live = flight.get("live_attempts") or {}
        print("在途查询: " + ("，".join(f"{x} {y}" for x, y in live.items()) if live else "0"))
        if flight.get("started_observations"):
            print(f"已开始观察: {flight['started_observations']}")
    for w in p.get("warnings") or []:
        print(f"警告: {w}")


# ---- manifests -----------------------------------------------------------------------------------

def c_manifests(a):
    items = load(a[0])
    limit = int(a[1]) if len(a) > 1 else 20
    table(items[:limit], [
        ("版本", k("version"), 4), ("状态", k("status"), 10), ("盘点完整", k("inventory_complete"), 8),
        ("资产", lambda r: (r.get("summary") or {}).get("assets"), 6),
        ("范围条目", lambda r: (r.get("summary") or {}).get("entries"), 8),
        ("新增", lambda r: (r.get("summary") or {}).get("new"), 6),
        ("排除", lambda r: (r.get("summary") or {}).get("excluded"), 6),
        ("不完整资产", lambda r: len((r.get("summary") or {}).get("incomplete_assets") or []), 10),
        ("创建", lambda r: ts(r.get("created_at")), 19), ("确认", lambda r: ts(r.get("confirmed_at")), 19),
        ("ID", k("id"), 36)])


def c_manifest_ref(a):
    items, ref = load(a[0]), a[1].lower()
    if ref == "latest":
        hit = items[0] if items else None
    elif ref in ("draft", "confirmed"):
        hit = next((m for m in items if m.get("status") == ref.upper()), None)
    elif ref.lstrip("v").isdigit():
        hit = next((m for m in items if str(m.get("version")) == ref.lstrip("v")), None)
    else:
        hit = next((m for m in items if m.get("id") == ref), None)
    if not hit:
        sys.exit(f"找不到清单 {a[1]}（可用 latest / draft / confirmed / 版本号 / ID）")
    print(hit["id"])


def c_manifest_head(a):
    m = load(a[0])
    s = m.get("summary") or {}
    print(f"清单 v{m.get('version')}  状态: {m.get('status')}  盘点完整: {'是' if m.get('inventory_complete') else '否'}")
    print(f"ID: {m.get('id')}")
    print(f"创建: {ts(m.get('created_at'))}  确认: {ts(m.get('confirmed_at'))}  发现运行: {m.get('discovery_run_id') or '-'}")
    print(f"资产 {s.get('assets', 0)} 个，范围条目 {s.get('entries', 0)} 条，新增 {s.get('new', 0)}，排除 {s.get('excluded', 0)}")
    br = s.get("by_reason") or {}
    if br:
        print("按 expected_reason: " + "，".join(f"{x} {y}" for x, y in sorted(br.items())))
    inc = s.get("incomplete_assets") or []
    if inc:
        print(f"注意: {len(inc)} 个资产盘点不完整（例如权限不足或元数据读取失败），确认前请核对")
    if m.get("status") == "DRAFT":
        print(f"该清单待确认: {os.environ.get('SRDQ_SELF', 'srdq-ops.sh')} manifest-confirm {m.get('id')} --reason \"...\"")


SCOPE_COLS = [
    ("层", k("layer"), 6), ("源表", tbl, 48), ("类型", k("scope_kind"), 10),
    ("范围", lambda r: r.get("label") or r.get("scope_key"), 40),
    ("源/目标", lambda r: f"{r.get('presence_source')}/{r.get('presence_target')}", 30),
    ("expected_reason", k("expected_reason"), 15), ("状态", k("status"), 10), ("新增", k("is_new"), 4)]


def c_scopes(a):
    d = load(a[0])
    table(d.get("items", []), SCOPE_COLS)
    pager(d)


def c_scopes_filter(a):
    d, reason, acc = load(a[0]), a[1], a[2]
    with open(acc, "a", encoding="utf-8") as f:
        for r in d.get("items", []):
            if r.get("expected_reason") == reason:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    with open(acc, encoding="utf-8") as f:
        found = sum(1 for _ in f)
    print(d.get("pages") or 1, found)


def c_scopes_acc(a):
    with open(a[0], encoding="utf-8") as f:
        rows = [json.loads(x) for x in f]
    limit = int(a[1])
    table(rows[:limit], SCOPE_COLS)
    more = "（已达 --limit，可能还有更多）" if len(rows) >= limit else ""
    print(f"expected_reason={a[2]} 的条目: 显示 {min(len(rows), limit)} 条{more}")


# ---- rules ---------------------------------------------------------------------------------------

def c_rules(a):
    d = load(a[0])
    table(d.get("items", []), [
        ("层", k("layer"), 6), ("源表", tbl, 48), ("类型", k("check_kind"), 7), ("rule_key", k("rule_key"), 30),
        ("版本", k("revision"), 4), ("状态", k("status"), 10), ("支持", k("support_state"), 11),
        ("启用", k("enabled"), 4), ("审批", k("approval_ref"), 16), ("不支持原因", k("support_reason"), 40),
        ("ID", k("id"), 36)])
    pager(d)


def c_approve_body(a):
    d, ref, reason = load(a[0]), a[1], a[2]
    body = {"rule_ids": [r["id"] for r in d.get("items", [])], "reason": reason}
    if ref:
        body["approval_ref"] = ref
    print(json.dumps(body, ensure_ascii=False))


def c_approved_count(a):
    print(sum(1 for r in load(a[0]).get("rules", []) if r.get("changed")))


# ---- plans and schedules -------------------------------------------------------------------------

def c_plans(a):
    table(load(a[0]), [
        ("名称", k("name"), 30), ("状态", k("status"), 8), ("清单模式", k("manifest_mode"), 16),
        ("检查", k("check_selector"), 40), ("资产", k("asset_selector"), 40), ("周期数", k("schedules"), 6),
        ("最近运行", lambda r: ts(r.get("last_run_at")), 19), ("ID", k("id"), 36)])


def c_plan_ref(a):
    plans, ref = load(a[0]), a[1]
    hit = next((p for p in plans if p.get("id") == ref or p.get("name") == ref), None)
    if not hit:
        sys.exit(f"找不到计划 {ref}（用 plans 查看名称）")
    print(hit["id"])


def c_plan(a):
    p = load(a[0])
    print(f"计划: {p.get('name')}  状态: {p.get('status')}  版本: {p.get('revision')}")
    print(f"ID: {p.get('id')}")
    print(f"清单模式: {p.get('manifest_mode')}" + (f"  固定清单: {p.get('manifest_id')}" if p.get("manifest_id") else ""))
    print(f"检查选择: {compact(p.get('check_selector'))}")
    print(f"资产选择: {compact(p.get('asset_selector'))}")
    if "schedules" in p and isinstance(p["schedules"], list):
        print("\n周期:")
        table(p["schedules"], [("名称", k("name"), 30), ("类型", k("kind"), 10), ("状态", k("status"), 8),
                               ("启用", k("enabled"), 4), ("下次触发", lambda r: ts(r.get("next_due_at")), 19),
                               ("ID", k("id"), 36)])
    if "runs" in p:
        print("\n最近运行:")
        table(p["runs"], [("ID", k("id"), 36), ("执行", k("execution_state"), 14), ("控制", k("control_state"), 10),
                          ("窗口数", k("windows_used"), 6), ("创建", lambda r: ts(r.get("created_at")), 19),
                          ("完成", lambda r: ts(r.get("completed_at")), 19)])


def sched_expr(r):
    if r.get("kind") == "CRON":
        return r.get("cron_expr")
    if r.get("kind") == "INTERVAL":
        return f"每 {r.get('interval_seconds')} 秒"
    return f"连续，间隔休息 {r.get('rest_seconds')} 秒"


def c_sched_preview(a):
    d = load(a[0])
    fires = d.get("next_fires") or []
    print(f"类型: {d.get('kind')}  时区: {d.get('timezone')}")
    if not fires:
        print("预览: 没有未来触发时间（检查开始/结束时间）")
    for i, f in enumerate(fires, 1):
        print(f"  第 {i:>2} 次: {ts(f)}")
    print("说明: 触发后创建运行；运行只在执行窗口内派发，窗口外触发的运行会等到下次窗口")


def c_schedule(a):
    s = load(a[0])
    print(f"周期: {s.get('name')}  ID: {s.get('id')}")
    print(f"类型: {s.get('kind')}  规则: {sched_expr(s)}  时区: {s.get('timezone')}")
    print(f"开始: {ts(s.get('starts_at'))}  结束: {ts(s.get('ends_at'))}  重叠: {s.get('overlap')}  "
          f"错过: {s.get('misfire')}  结束时取消: {cut(s.get('cancel_on_end'))}")
    print(f"启用: {cut(s.get('enabled'))}  状态: {s.get('status')}  下次触发: {ts(s.get('next_due_at'))}  "
          f"等待原因: {s.get('waiting_reason') or '-'}")
    for f in (s.get("next_fires") or [])[:5]:
        print(f"  将触发: {ts(f)}")


def c_schedules(a):
    items, plans = load(a[0]), load(a[1])
    names = {p["id"]: p["name"] for p in plans}
    table(items, [
        ("名称", k("name"), 30), ("计划", lambda r: names.get(r.get("plan_id"), r.get("plan_id")), 24),
        ("类型", k("kind"), 10), ("规则", sched_expr, 26), ("时区", k("timezone"), 14), ("启用", k("enabled"), 4),
        ("状态", k("status"), 8), ("下次触发", lambda r: ts(r.get("next_due_at")), 19),
        ("等待原因", k("waiting_reason"), 30), ("ID", k("id"), 36)])


def c_sched_ref(a):
    items, ref = load(a[0]), a[1]
    hit = next((s for s in items if s.get("id") == ref or s.get("name") == ref), None)
    if not hit:
        sys.exit(f"找不到周期 {ref}（用 schedules 查看名称）")
    print(hit["id"], hit["revision"])


# ---- differences ---------------------------------------------------------------------------------

def c_diffs(a):
    d = load(a[0])
    rows = d.get("items", [])
    base = (d.get("page", 1) - 1) * d.get("page_size", 0)
    for i, r in enumerate(rows, 1):
        r["_n"] = base + i
    table(rows, [
        ("#", k("_n"), 4), ("状态", lambda r: f"{r.get('state')}/{STATE_ZH.get(r.get('state'), '')}", 16),
        ("类型", k("check_kind"), 7), ("层", k("layer"), 5), ("源表", k("source_table"), 36),
        ("范围", lambda r: r.get("scope_label") or r.get("scope_key"), 24), ("rule_key", k("rule_key"), 24),
        ("差异数", lambda r: f"{r.get('difference_count')}{'+' if r.get('difference_count_mode') == 'AT_LEAST' else ''}", 8),
        ("连续次数", k("consecutive_different"), 8), ("最近", lambda r: ts(r.get("latest_at")), 19),
        ("摘要", k("summary"), 50), ("comparison_id", k("comparison_id"), 36)])
    total, size = d.get("total", 0), d.get("page_size", 0) or 1
    print(f"共 {total} 条，第 {d.get('page', 1)}/{max(1, -(-total // size))} 页")
    if d.get("resolved_since"):
        print(f"未指定 --state 时，RESOLVED 只包含 {ts(d['resolved_since'])} 之后恢复的条目")
    print(f"查看详情: {os.environ.get('SRDQ_SELF', 'srdq-ops.sh')} diff-show <comparison_id>")


def c_comparison(a):
    c = load(a[0])
    v = c.get("verdict")
    print(f"比对 ID: {c.get('id')}")
    print(f"资产: [{c.get('layer')}] {c.get('source_db')}.{c.get('source_table')}  ->  "
          f"{c.get('target_db')}.{c.get('target_table')}")
    print(f"范围: {c.get('scope_label') or c.get('scope_key')}")
    print(f"检查: {c.get('check_kind')}  规则: {c.get('rule_key')} (版本 {c.get('rule_revision')})  证据: {c.get('evidence')}")
    print(f"结论: {v} {VERDICT_ZH.get(v, '')}  差异数: {c.get('difference_count')} ({c.get('difference_count_mode')})  "
          f"已存储: {c.get('differences_stored')}")
    print(f"比对时间: {ts(c.get('compared_at'))}  运行: {c.get('run_id')}")
    print(f"源端读取: {ts(c.get('source_read_started'))} ~ {ts(c.get('source_read_finished'))}")
    print(f"目标读取: {ts(c.get('target_read_started'))} ~ {ts(c.get('target_read_finished'))}")
    if c.get("reason"):
        print(f"原因: {c.get('reason')}")
    if c.get("summary"):
        print(f"摘要: {cut(c.get('summary'), 600)}")
    note = (c.get("statement") or {}).get("note")
    if note:
        print(f"说明: {note}")
    print("\n两端结果:")
    table(c.get("side_results") or [], [
        ("侧", k("side"), 8), ("证据", k("evidence"), 12), ("行数", k("row_count"), 14), ("有效性", k("validity"), 12),
        ("开始", lambda r: ts(r.get("read_started")), 19), ("结束", lambda r: ts(r.get("read_finished")), 19),
        ("query_ref", k("query_ref"), 30)])
    print("\n同一范围最近结论:")
    table(c.get("history") or [], [
        ("时间", lambda r: ts(r.get("compared_at")), 19),
        ("结论", lambda r: f"{r.get('verdict')}/{VERDICT_ZH.get(r.get('verdict'), '')}", 28),
        ("差异数", k("difference_count"), 8), ("运行", k("run_id"), 36), ("comparison_id", k("id"), 36)])


def c_comp_diffs(a):
    d = load(a[0])
    print("\n差异明细" + ("（值已脱敏，加 --show-values 显示原值）" if d.get("masked") else "") + ":")
    table(d.get("items", []), [
        ("序号", k("ordinal"), 6), ("类型", k("type"), 20),
        ("位置", lambda r: r.get("key_values") or r.get("group_values") or r.get("metric_id") or r.get("key_hex"), 40),
        ("列", k("column_name"), 24), ("源端值", k("source_value"), 30), ("目标值", k("target_value"), 30),
        ("详情", k("detail"), 50)])
    pager(d)


CMDS = {name[2:].replace("_", "-"): fn for name, fn in globals().items() if name.startswith("c_")}

if __name__ == "__main__":
    CMDS[sys.argv[1]](sys.argv[2:])
PYEOF

export SRDQ_SELF="$SELF"
py() { "$PY" "$HELPER" "$@"; }

http() {
    local code
    if ! code=$(curl -sS --max-time "${SRDQ_TIMEOUT:-300}" -o "$RESP" -w '%{http_code}' "$@"); then
        die "无法连接 $API。确认 srdq-web 正在运行（systemctl status srdq-web）；端口不是 8470 时设置 SRDQ_API=http://127.0.0.1:<端口>/v1"
    fi
    if ((code >= 400)); then
        py error "$RESP" "$code" >&2
        return 1
    fi
}

# api METHOD PATH [BODY_FILE]
api() {
    local a=(-X "$1" -H 'Accept: application/json')
    if [[ -n ${3-} ]]; then a+=(-H 'Content-Type: application/json' --data-binary "@$3"); fi
    http "${a[@]}" "${API}$2"
}

# upload PATH FILE FIELD=VALUE...
upload() {
    local path=$1 file=$2
    shift 2
    [[ -r $file ]] || die "无法读取文件 $file"
    local a=(-X POST -H 'Accept: application/json' -F "file=@$file")
    local f
    for f in "$@"; do a+=(--form-string "$f"); done
    http "${a[@]}" "${API}${path}"
}

need() { [[ $# -ge 2 && -n $2 ]] || die "$1 需要一个参数值"; }

ask() {
    if [[ ${YES:-} == 1 ]]; then return 0; fi
    [[ -t 0 ]] || die "非交互环境，请加 --yes 跳过确认"
    local ans
    read -r -p "$1 [y/N] " ans
    [[ $ans == y || $ans == Y || $ans == yes ]]
}

is_terminal() {
    case $1 in COMPLETED | FAILED | CANCELLED) return 0 ;; esac
    return 1
}

show_window() {
    api GET /control
    py window "$RESP"
}

resolve_manifest() {
    if [[ $1 =~ $UUID_RE ]]; then echo "$1"; return; fi
    api GET /manifests
    py manifest-ref "$RESP" "$1"
}

resolve_plan() {
    if [[ $1 =~ $UUID_RE ]]; then echo "$1"; return; fi
    api GET /plans
    py plan-ref "$RESP" "$1"
}

count_rules() {
    api GET "/check-rules$(py qs "$@" page_size=1)"
    py get "$RESP" total 0
}

# ---- commands ------------------------------------------------------------------------------------

cmd_status() {
    api GET /control
    cp "$RESP" "$TMP/control.json"
    api GET /dashboard
    py status "$RESP" "$TMP/control.json"
}

cmd_import_assets() {
    local file="" mode=CREATE_OR_VALIDATE allow="" preview_only="" reason="导入资产清单"
    while (($#)); do
        case $1 in
            --update) mode=UPDATE_EXPLICIT; shift ;;
            --allow-errors) allow=1; shift ;;
            --preview-only) preview_only=1; shift ;;
            --reason) need "$@"; reason=$2; shift 2 ;;
            -*) die "未知参数 $1" ;;
            *) [[ -z $file ]] || die "只能指定一个文件"; file=$1; shift ;;
        esac
    done
    [[ -n $file ]] || die "用法: $SELF import-assets FILE [--update] [--allow-errors] [--preview-only] [--reason R]"
    upload /imports/preview "$file" kind=ASSET_LIST "mode=$mode"
    py import-preview "$RESP" asset
    local bid errs changes
    bid=$(py get "$RESP" id)
    errs=$(py get "$RESP" summary.ERROR 0)
    changes=$(( $(py get "$RESP" summary.NEW 0) + $(py get "$RESP" summary.UPDATE 0) ))
    if [[ -n $preview_only ]]; then echo "仅预览，未确认。"; return 0; fi
    if ((errs > 0)) && [[ -z $allow ]]; then
        die "有 $errs 行 ERROR，未确认导入。修正文件后重试；资产已存在但字段变化时加 --update；只导入正确行加 --allow-errors"
    fi
    if ((changes == 0)); then echo "没有新增或更新的资产，无需确认。"; return 0; fi
    py body "reason=$reason" >"$BODY"
    api POST "/imports/$bid/confirm" "$BODY"
    py import-confirm "$RESP"
    echo "下一步: $SELF discover"
}

cmd_assets() {
    local layer="" q="" missing="" limit=50 page=1
    while (($#)); do
        case $1 in
            --layer) need "$@"; layer=${2^^}; shift 2 ;;
            --q) need "$@"; q=$2; shift 2 ;;
            --missing) need "$@"; missing=${2^^}; shift 2 ;;
            --limit) need "$@"; limit=$2; shift 2 ;;
            --page) need "$@"; page=$2; shift 2 ;;
            *) die "未知参数 $1" ;;
        esac
    done
    api GET "/assets$(py qs layer="$layer" q="$q" missing_kind="$missing" page="$page" page_size="$limit")"
    py assets "$RESP"
}

cmd_discover() {
    local reason="范围发现"
    while (($#)); do
        case $1 in
            --reason) need "$@"; reason=$2; shift 2 ;;
            *) die "未知参数 $1" ;;
        esac
    done
    py body "reason=$reason" >"$BODY"
    api POST /manifests/discover "$BODY"
    py discover "$RESP"
    show_window
    echo "查看进度: $SELF discover-status    等待完成: $SELF discover-status --wait"
}

cmd_discover_status() {
    local wait="" rid state
    while (($#)); do
        case $1 in
            --wait) wait=1; shift ;;
            *) die "未知参数 $1" ;;
        esac
    done
    while :; do
        api GET "/runs?purpose=METADATA_PREVIEW&page_size=1"
        rid=$(py get "$RESP" items.0.id)
        [[ -n $rid ]] || die "还没有发起过范围发现，先执行 $SELF discover"
        api GET "/runs/$rid"
        state=$(py get "$RESP" execution_state)
        if [[ -z $wait ]] || is_terminal "$state"; then break; fi
        echo "$(date '+%F %T')  发现运行 $rid 状态 $state，${POLL} 秒后再查（Ctrl-C 退出）"
        sleep "$POLL"
    done
    py run "$RESP"
    echo
    if [[ $state == COMPLETED ]]; then
        echo "发现已完成。最近的清单:"
        api GET /manifests
        py manifests "$RESP" 3
        echo "查看清单: $SELF manifest-show latest    确认清单: $SELF manifest-confirm draft --reason \"...\""
    elif ! is_terminal "$state"; then
        show_window
    fi
}

cmd_manifests() {
    local limit=20
    while (($#)); do
        case $1 in
            --limit) need "$@"; limit=$2; shift 2 ;;
            *) die "未知参数 $1" ;;
        esac
    done
    api GET /manifests
    py manifests "$RESP" "$limit"
}

cmd_manifest_show() {
    local ref=latest reason="" q="" new="" limit=50 page=1
    while (($#)); do
        case $1 in
            --reason) need "$@"; reason=${2^^}; shift 2 ;;
            --q) need "$@"; q=$2; shift 2 ;;
            --new) new=1; shift ;;
            --limit) need "$@"; limit=$2; shift 2 ;;
            --page) need "$@"; page=$2; shift 2 ;;
            -*) die "未知参数 $1" ;;
            *) ref=$1; shift ;;
        esac
    done
    local mid
    mid=$(resolve_manifest "$ref")
    api GET "/manifests/$mid?page_size=1"
    py manifest-head "$RESP"
    echo
    case $reason in
        "" | IN_SCOPE | SOURCE_ONLY | TARGET_ONLY | UNKNOWN | ACCESS_DENIED)
            api GET "/manifests/$mid/scopes$(py qs expected_reason="$reason" q="$q" new_only="$new" page="$page" page_size="$limit")"
            py scopes "$RESP"
            ;;
        TABLE_ABSENT | DISAPPEARED | OUTSIDE)
            # 服务端过滤不接受这几个值，逐页取回后在本地过滤
            : >"$TMP/acc.jsonl"
            local p=1 out pages found
            while :; do
                api GET "/manifests/$mid/scopes$(py qs q="$q" new_only="$new" page="$p" page_size=200)"
                out=$(py scopes-filter "$RESP" "$reason" "$TMP/acc.jsonl")
                pages=${out% *}
                found=${out#* }
                if ((found >= limit || p >= pages)); then break; fi
                p=$((p + 1))
            done
            py scopes-acc "$TMP/acc.jsonl" "$limit" "$reason"
            ;;
        *) die "--reason 可选: IN_SCOPE SOURCE_ONLY TARGET_ONLY UNKNOWN ACCESS_DENIED TABLE_ABSENT DISAPPEARED OUTSIDE" ;;
    esac
}

cmd_manifest_decide() {
    local action=$1
    shift
    local ref=draft reason="" verb
    if [[ $action == confirm ]]; then verb=确认; else verb=驳回; fi
    while (($#)); do
        case $1 in
            --reason) need "$@"; reason=$2; shift 2 ;;
            --yes) YES=1; shift ;;
            -*) die "未知参数 $1" ;;
            *) ref=$1; shift ;;
        esac
    done
    [[ -n $reason ]] || die "请用 --reason 说明${verb}原因"
    local mid
    mid=$(resolve_manifest "$ref")
    api GET "/manifests/$mid?page_size=1"
    py manifest-head "$RESP"
    echo
    ask "${verb}以上清单？" || die "已取消"
    py body "reason=$reason" >"$BODY"
    api POST "/manifests/$mid/$action" "$BODY"
    api GET "/manifests/$mid?page_size=1"
    echo "已${verb}: v$(py get "$RESP" version)，状态 $(py get "$RESP" status)"
    if [[ $action == confirm ]]; then echo "下一步: $SELF import-rules <规则目录或文件>"; fi
}

approve_all() {
    local kind=$1 ref=$2 reason=$3 total=0 n changed un
    while :; do
        api GET "/check-rules$(py qs status=DRAFT support_state=SUPPORTED kind="$kind" page_size=200)"
        n=$(py get "$RESP" total 0)
        ((n > 0)) || break
        py approve-body "$RESP" "$ref" "$reason" >"$BODY"
        api POST /check-rules/approve "$BODY"
        changed=$(py approved-count "$RESP")
        total=$((total + changed))
        echo "已批准 $total 条，剩余待批准 $((n - changed)) 条"
        ((changed > 0)) || break
    done
    echo "批准完成：本次共批准 $total 条规则。"
    un=$(count_rules status=DRAFT support_state=UNSUPPORTED kind="$kind")
    if ((un > 0)); then
        echo "另有 $un 条 DRAFT 规则为 UNSUPPORTED（当前版本无法执行），未批准。查看: $SELF rules --status DRAFT --support UNSUPPORTED"
    fi
}

rules_summary() {
    local k d a u s
    printf '类型\t待批准\t已批准\t待批准中不支持\n' >"$TMP/sum.tsv"
    for k in COUNT METRIC SAMPLE; do
        d=$(count_rules kind=$k status=DRAFT)
        a=$(count_rules kind=$k status=APPROVED)
        u=$(count_rules kind=$k status=DRAFT support_state=UNSUPPORTED)
        printf '%s\t%s\t%s\t%s\n' "$k" "$d" "$a" "$u" >>"$TMP/sum.tsv"
    done
    py tsv-table "$TMP/sum.tsv"
    api GET /dashboard
    s=$(py get "$RESP" rule_gaps.assets 0)
    echo "启用资产 $s 个；缺少已批准 METRIC 规则 $(py get "$RESP" rule_gaps.METRIC 0) 个，缺少已批准 SAMPLE 规则 $(py get "$RESP" rule_gaps.SAMPLE 0) 个"
}

cmd_import_rules() {
    local kind="" allow="" noapprove="" ref="" reason="导入检查规则" args=() files=() failed=0
    while (($#)); do
        case $1 in
            --kind) need "$@"; kind=${2^^}; shift 2 ;;
            --allow-errors) allow=1; shift ;;
            --no-approve) noapprove=1; shift ;;
            --approval-ref) need "$@"; ref=$2; shift 2 ;;
            --reason) need "$@"; reason=$2; shift 2 ;;
            -*) die "未知参数 $1" ;;
            *) args+=("$1"); shift ;;
        esac
    done
    ((${#args[@]})) || die "用法: $SELF import-rules <目录或文件>... [--kind METRIC|SAMPLE] [--allow-errors] [--no-approve] [--approval-ref R]"
    if [[ -n $kind && $kind != METRIC && $kind != SAMPLE ]]; then die "--kind 只能是 METRIC 或 SAMPLE"; fi
    local f
    shopt -s nullglob
    for f in "${args[@]}"; do
        if [[ -d $f ]]; then
            files+=("$f"/metric_rules_*.csv "$f"/sample_rules_*.csv)
        else
            files+=("$f")
        fi
    done
    shopt -u nullglob
    ((${#files[@]})) || die "目录中没有 metric_rules_*.csv / sample_rules_*.csv"
    local k bid errs good
    for f in "${files[@]}"; do
        k=$kind
        if [[ -z $k ]]; then
            case "$(basename "$f" | tr '[:upper:]' '[:lower:]')" in
                *metric*) k=METRIC ;;
                *sample*) k=SAMPLE ;;
                *) die "无法从文件名判断规则类型: $f，请加 --kind METRIC|SAMPLE" ;;
            esac
        fi
        echo "==== $f ($k) ===="
        upload /check-rules/preview "$f" "kind=$k"
        py import-preview "$RESP" rule
        bid=$(py get "$RESP" id)
        errs=$(py get "$RESP" summary.ERROR 0)
        good=$(( $(py get "$RESP" summary.OK 0) + $(py get "$RESP" summary.UNSUPPORTED 0) ))
        if ((errs > 0)) && [[ -z $allow ]]; then
            echo "有 $errs 条 ERROR，跳过该文件（修正后重试，或加 --allow-errors 只导入正确的规则）"
            failed=$((failed + 1))
            echo
            continue
        fi
        if ((good == 0)); then echo "没有可导入的规则，跳过"; echo; continue; fi
        py body "reason=$reason" >"$BODY"
        api POST "/imports/$bid/confirm" "$BODY"
        py import-confirm "$RESP"
        echo
    done
    if [[ -z $noapprove ]]; then
        echo "==== 自动批准 DRAFT 规则 ===="
        approve_all "$kind" "$ref" "$reason"
        echo
    fi
    echo "==== 规则统计 ===="
    rules_summary
    if ((failed > 0)); then die "$failed 个文件因 ERROR 未导入"; fi
}

cmd_approve() {
    local kind="" ref="" reason="批准检查规则"
    while (($#)); do
        case $1 in
            --kind) need "$@"; kind=${2^^}; shift 2 ;;
            --approval-ref) need "$@"; ref=$2; shift 2 ;;
            --reason) need "$@"; reason=$2; shift 2 ;;
            *) die "未知参数 $1" ;;
        esac
    done
    approve_all "$kind" "$ref" "$reason"
}

cmd_rules() {
    local kind="" status="" support="" q="" limit=50 page=1 summary=""
    while (($#)); do
        case $1 in
            --kind) need "$@"; kind=${2^^}; shift 2 ;;
            --status) need "$@"; status=${2^^}; shift 2 ;;
            --support) need "$@"; support=${2^^}; shift 2 ;;
            --q) need "$@"; q=$2; shift 2 ;;
            --limit) need "$@"; limit=$2; shift 2 ;;
            --page) need "$@"; page=$2; shift 2 ;;
            --summary) summary=1; shift ;;
            *) die "未知参数 $1" ;;
        esac
    done
    if [[ -n $summary ]]; then rules_summary; return 0; fi
    api GET "/check-rules$(py qs kind="$kind" status="$status" support_state="$support" q="$q" page="$page" page_size="$limit")"
    py rules "$RESP"
}

cmd_plan_create() {
    local name="" kinds="" rulekeys="" layers="" assets="" tables="" fixed="" reason="创建计划"
    while (($#)); do
        case $1 in
            --kinds) need "$@"; kinds=$2; shift 2 ;;
            --rule-keys) need "$@"; rulekeys=$2; shift 2 ;;
            --layers) need "$@"; layers=$2; shift 2 ;;
            --assets) need "$@"; assets=$2; shift 2 ;;
            --tables) need "$@"; tables=$2; shift 2 ;;
            --fixed) need "$@"; fixed=$2; shift 2 ;;
            --reason) need "$@"; reason=$2; shift 2 ;;
            -*) die "未知参数 $1" ;;
            *) [[ -z $name ]] || die "只能指定一个计划名称"; name=$1; shift ;;
        esac
    done
    [[ -n $name ]] || die "用法: $SELF plan-create NAME [--kinds COUNT,METRIC,SAMPLE] [--rule-keys K1,K2] [--layers ODS,DWD | --tables db.t1,db.t2 | --assets ID1,ID2] [--fixed 清单]"
    if [[ -n $tables ]]; then
        local t id ids=() tl=()
        IFS=',' read -r -a tl <<<"$tables"
        for t in "${tl[@]}"; do
            t=${t// /}
            [[ -n $t ]] || continue
            api GET "/assets$(py qs q="$t" page_size=200)"
            id=$(py asset-exact "$RESP" "$t")
            ids+=("$id")
        done
        assets="${assets:+$assets,}$(IFS=,; echo "${ids[*]}")"
    fi
    local csel asel
    csel=$(py csel "$kinds" "$rulekeys")
    asel=$(py asel "$layers" "$assets")
    local body=("name=$name" "check_selector:=$csel" "asset_selector:=$asel" "reason=$reason")
    if [[ -n $fixed ]]; then
        local mid
        mid=$(resolve_manifest "$fixed")
        body+=(manifest_mode=FIXED "manifest_id=$mid")
    else
        body+=(manifest_mode=LATEST_CONFIRMED)
    fi
    py body "${body[@]}" >"$BODY"
    api POST /plans "$BODY"
    py plan "$RESP"
    echo "手动运行: $SELF run \"$name\"    创建周期: $SELF schedule-create <周期名> --plan \"$name\" --cron \"0 1 * * *\""
}

cmd_plans() {
    api GET /plans
    py plans "$RESP"
}

cmd_plan_show() {
    [[ $# -eq 1 ]] || die "用法: $SELF plan-show <计划名称或ID>"
    local pid
    pid=$(resolve_plan "$1")
    api GET "/plans/$pid"
    py plan "$RESP"
}

cmd_run() {
    local plan="" kinds="" rulekeys="" reason="手动运行计划"
    while (($#)); do
        case $1 in
            --kinds) need "$@"; kinds=$2; shift 2 ;;
            --rule-keys) need "$@"; rulekeys=$2; shift 2 ;;
            --reason) need "$@"; reason=$2; shift 2 ;;
            -*) die "未知参数 $1" ;;
            *) [[ -z $plan ]] || die "只能指定一个计划"; plan=$1; shift ;;
        esac
    done
    [[ -n $plan ]] || die "用法: $SELF run <计划名称或ID> [--kinds COUNT,METRIC,SAMPLE] [--rule-keys K]"
    local pid
    pid=$(resolve_plan "$plan")
    local body=("plan_id=$pid" "request_key=ops-$(date +%Y%m%d%H%M%S)-$$-$RANDOM" "reason=$reason")
    if [[ -n $kinds || -n $rulekeys ]]; then body+=("check_selector:=$(py csel "$kinds" "$rulekeys")"); fi
    py body "${body[@]}" >"$BODY"
    api POST /runs "$BODY"
    local rid
    rid=$(py get "$RESP" id)
    api GET "/runs/$rid"
    py run "$RESP"
    show_window
    echo "查看进度: $SELF run-show $rid [--wait]"
}

cmd_runs() {
    local active="" plan="" purpose="" limit=20 page=1 pid=""
    while (($#)); do
        case $1 in
            --active) active=1; shift ;;
            --plan) need "$@"; plan=$2; shift 2 ;;
            --purpose) need "$@"; purpose=${2^^}; shift 2 ;;
            --limit) need "$@"; limit=$2; shift 2 ;;
            --page) need "$@"; page=$2; shift 2 ;;
            *) die "未知参数 $1" ;;
        esac
    done
    if [[ -n $plan ]]; then pid=$(resolve_plan "$plan"); fi
    api GET "/runs$(py qs active="$active" plan_id="$pid" purpose="$purpose" page="$page" page_size="$limit")"
    py runs "$RESP"
}

cmd_run_show() {
    local rid="" wait="" state
    while (($#)); do
        case $1 in
            --wait) wait=1; shift ;;
            -*) die "未知参数 $1" ;;
            *) rid=$1; shift ;;
        esac
    done
    if [[ -z $rid || $rid == latest ]]; then
        api GET "/runs?purpose=COMPARE&page_size=1"
        rid=$(py get "$RESP" items.0.id)
        [[ -n $rid ]] || die "还没有计划运行"
    fi
    while :; do
        api GET "/runs/$rid"
        state=$(py get "$RESP" execution_state)
        if [[ -z $wait ]] || is_terminal "$state"; then break; fi
        echo "$(date '+%F %T')  运行 $rid 状态 $state，${POLL} 秒后再查（Ctrl-C 退出）"
        sleep "$POLL"
    done
    py run "$RESP"
    if ! is_terminal "$state"; then show_window; fi
    if [[ $state == COMPLETED ]]; then echo "查看差异: $SELF diffs"; fi
    echo "检查项列表: $SELF checks $rid [--kind K] [--coverage STATE] [--table db.t]"
    if ! is_terminal "$state"; then
        echo "运行控制: $SELF run-action $rid pause|restart|cancel [--reason R] [--yes]"
    fi
}

cmd_run_action() {
    local rid="" action="" op="" reason="" preview_only="" oid digest state
    while (($#)); do
        case $1 in
            --reason) need "$@"; reason=$2; shift 2 ;;
            --preview-only) preview_only=1; shift ;;
            --yes) YES=1; shift ;;
            -*) die "未知参数 $1" ;;
            *)
                if [[ -z $rid ]]; then rid=$1; elif [[ -z $action ]]; then action=${1,,}; else die "多余参数 $1"; fi
                shift
                ;;
        esac
    done
    [[ -n $rid && -n $action ]] ||
        die "用法: $SELF run-action <运行ID|latest> pause|restart|resume|cancel [--reason R] [--preview-only] [--yes]"
    case $action in
        pause) op=PAUSE; reason=${reason:-资源控制：暂停运行} ;;
        restart | resume | continue) op=RESUME; reason=${reason:-资源恢复：继续运行} ;;
        cancel) op=CANCEL; reason=${reason:-资源控制：取消运行} ;;
        *) die "动作必须是 pause、restart（继续已暂停运行）、resume 或 cancel" ;;
    esac
    if [[ $rid == latest ]]; then
        api GET "/runs?purpose=COMPARE&active=true&page_size=1"
        rid=$(py get "$RESP" items.0.id)
        [[ -n $rid ]] || die "没有进行中或已暂停的计划运行（用 runs 查看）"
    fi
    [[ $rid =~ $UUID_RE ]] || die "运行ID格式错误（用 runs --active 查看）"

    py body "op=$op" "target_type=RUN" "target_ids:=[\"$rid\"]" "effect=CURRENT_ONLY" >"$BODY"
    api POST /operations/preview "$BODY"
    oid=$(py get "$RESP" id)
    digest=$(py get "$RESP" preview_digest)
    echo "==== 操作预览 ===="
    py operation-preview "$RESP"
    if [[ -n $preview_only ]]; then
        echo "仅预览，未应用。"
        return
    fi
    if [[ $op == PAUSE ]]; then
        echo "说明: 暂停后不再派发新检查，已经开始的查询会执行完。"
    elif [[ $op == RESUME ]]; then
        echo "说明: restart/resume 会继续同一轮运行，不会重新创建一轮。"
    else
        echo "说明: cancel 会取消在途查询并永久结束本轮运行。"
    fi
    ask "确认应用 $op 到运行 $rid？" || { echo "已取消，未应用。"; return; }

    py body "reason=$reason" >"$BODY"
    http -X POST -H 'Accept: application/json' -H 'Content-Type: application/json' \
        -H "Idempotency-Key: ops-${op,,}-$(date +%Y%m%d%H%M%S)-$$-$RANDOM" \
        -H "If-Match: $digest" --data-binary "@$BODY" "$API/operations/$oid/apply"
    echo "==== 已应用 ===="
    py pretty "$RESP"
    api GET "/runs/$rid"
    echo
    py run "$RESP"
}

cmd_checks() {
    local rid="" kind="" cov="" table="" limit=50 page=1 aid=""
    while (($#)); do
        case $1 in
            --kind) need "$@"; kind=${2^^}; shift 2 ;;
            --coverage) need "$@"; cov=${2^^}; shift 2 ;;
            --table) need "$@"; table=${2// /}; shift 2 ;;
            --limit) need "$@"; limit=$2; shift 2 ;;
            --page) need "$@"; page=$2; shift 2 ;;
            -*) die "未知参数 $1" ;;
            *) rid=$1; shift ;;
        esac
    done
    if [[ -z $rid || $rid == latest ]]; then
        api GET "/runs?purpose=COMPARE&page_size=1"
        rid=$(py get "$RESP" items.0.id)
        [[ -n $rid ]] || die "还没有计划运行"
    fi
    if [[ -n $table ]]; then
        api GET "/assets$(py qs q="$table" page_size=200)"
        aid=$(py asset-exact "$RESP" "$table")
    fi
    echo "运行 $rid 的检查项:"
    api GET "/runs/$rid/checks$(py qs check_kind="$kind" coverage_state="$cov" asset_id="$aid" page="$page" page_size="$limit")"
    py checks "$RESP"
}

cmd_schedule_create() {
    local name="" plan="" kind="" cron="" interval="" rest="" tz="" start="" end="" overlap="" misfire=""
    local coe="" kinds="" rulekeys="" disabled="" preview_only="" reason="创建周期"
    while (($#)); do
        case $1 in
            --plan) need "$@"; plan=$2; shift 2 ;;
            --cron) need "$@"; kind=CRON; cron=$2; shift 2 ;;
            --interval) need "$@"; kind=INTERVAL; interval=$2; shift 2 ;;
            --continuous) kind=CONTINUOUS; shift ;;
            --rest) need "$@"; rest=$2; shift 2 ;;
            --tz) need "$@"; tz=$2; shift 2 ;;
            --start) need "$@"; start=$2; shift 2 ;;
            --end) need "$@"; end=$2; shift 2 ;;
            --overlap) need "$@"; overlap=${2^^}; shift 2 ;;
            --misfire) need "$@"; misfire=${2^^}; shift 2 ;;
            --cancel-on-end) coe=1; shift ;;
            --kinds) need "$@"; kinds=$2; shift 2 ;;
            --rule-keys) need "$@"; rulekeys=$2; shift 2 ;;
            --disabled) disabled=1; shift ;;
            --preview-only) preview_only=1; shift ;;
            --yes) YES=1; shift ;;
            --reason) need "$@"; reason=$2; shift 2 ;;
            -*) die "未知参数 $1" ;;
            *) [[ -z $name ]] || die "只能指定一个周期名称"; name=$1; shift ;;
        esac
    done
    [[ -n $name && -n $plan && -n $kind ]] ||
        die "用法: $SELF schedule-create NAME --plan 计划 (--cron \"分 时 日 月 周\" | --interval 秒 | --continuous [--rest 秒]) [--tz Asia/Shanghai] [--start ISO] [--end ISO] [--overlap SKIP|COALESCE] [--misfire SKIP|LATEST] [--cancel-on-end] [--kinds K] [--disabled] [--preview-only] [--yes]"
    if [[ -n $interval && ! $interval =~ ^[0-9]+$ ]]; then die "--interval 必须是整数秒（>=60）"; fi
    if [[ -n $rest && ! $rest =~ ^[0-9]+$ ]]; then die "--rest 必须是整数秒（>=60）"; fi
    local pid
    pid=$(resolve_plan "$plan")
    local body=("name=$name" "plan_id=$pid" "kind=$kind")
    if [[ $kind == CRON ]]; then body+=("cron_expr=$cron"); fi
    if [[ $kind == INTERVAL ]]; then body+=("interval_seconds:=$interval"); fi
    if [[ $kind == CONTINUOUS && -n $rest ]]; then body+=("rest_seconds:=$rest"); fi
    if [[ -n $tz ]]; then body+=("timezone=$tz"); fi
    if [[ -n $start ]]; then body+=("starts_at=$start"); fi
    if [[ -n $end ]]; then body+=("ends_at=$end"); fi
    if [[ -n $overlap ]]; then body+=("overlap=$overlap"); fi
    if [[ -n $misfire ]]; then body+=("misfire=$misfire"); fi
    if [[ -n $coe ]]; then body+=("cancel_on_end:=true"); fi
    if [[ -n $disabled ]]; then body+=("enabled:=false"); fi
    if [[ -n $kinds || -n $rulekeys ]]; then body+=("check_selector:=$(py csel "$kinds" "$rulekeys")"); fi
    py body "${body[@]}" >"$BODY"
    echo "==== 触发时间预览 ===="
    api POST /schedules/preview "$BODY"
    py sched-preview "$RESP"
    if [[ -n $preview_only ]]; then echo "仅预览，未创建。"; return 0; fi
    echo
    ask "按以上触发时间创建周期 \"$name\"？" || die "已取消"
    py body "${body[@]}" "reason=$reason" >"$BODY"
    api POST /schedules "$BODY"
    echo "==== 已创建 ===="
    py schedule "$RESP"
}

cmd_schedules() {
    api GET /plans
    cp "$RESP" "$TMP/plans.json"
    api GET /schedules
    py schedules "$RESP" "$TMP/plans.json"
}

cmd_schedule_action() {
    local ref="" action="" reason=""
    while (($#)); do
        case $1 in
            --reason) need "$@"; reason=$2; shift 2 ;;
            -*) die "未知参数 $1" ;;
            *)
                if [[ -z $ref ]]; then ref=$1; elif [[ -z $action ]]; then action=${1^^}; else die "多余参数 $1"; fi
                shift
                ;;
        esac
    done
    case $action in ENABLE | DISABLE | ARCHIVE | RESTART) ;; *) die "用法: $SELF schedule-action <周期名称或ID> enable|disable|archive|restart [--reason R]" ;; esac
    api GET /schedules
    local out sid rev
    out=$(py sched-ref "$RESP" "$ref")
    sid=${out% *}
    rev=${out#* }
    py body "action=$action" "revision=$rev" "reason=${reason:-周期操作 $action}" >"$BODY"
    api POST "/schedules/$sid/actions" "$BODY"
    py schedule "$RESP"
}

cmd_diffs() {
    local state="" kind="" layer="" table="" asset="" limit=50 page=1
    while (($#)); do
        case $1 in
            --state) need "$@"; state=${2^^}; shift 2 ;;
            --kind) need "$@"; kind=${2^^}; shift 2 ;;
            --layer) need "$@"; layer=${2^^}; shift 2 ;;
            --table) need "$@"; table=$2; shift 2 ;;
            --asset) need "$@"; asset=$2; shift 2 ;;
            --limit) need "$@"; limit=$2; shift 2 ;;
            --page) need "$@"; page=$2; shift 2 ;;
            *) die "未知参数 $1" ;;
        esac
    done
    api GET "/differences$(py qs state="$state" kind="$kind" layer="$layer" table="$table" asset_id="$asset" page="$page" page_size="$limit")"
    py diffs "$RESP"
}

cmd_diff_show() {
    local cid="" show="" type="" limit=50 page=1
    while (($#)); do
        case $1 in
            --show-values) show=1; shift ;;
            --type) need "$@"; type=$2; shift 2 ;;
            --limit) need "$@"; limit=$2; shift 2 ;;
            --page) need "$@"; page=$2; shift 2 ;;
            -*) die "未知参数 $1" ;;
            *) cid=$1; shift ;;
        esac
    done
    [[ $cid =~ $UUID_RE ]] || die "用法: $SELF diff-show <comparison_id> [--show-values] [--type T] [--limit N] [--page P]（comparison_id 见 diffs 最后一列）"
    api GET "/comparisons/$cid"
    py comparison "$RESP"
    api GET "/comparisons/$cid/differences$(py qs show_values="$show" type="$type" page="$page" page_size="$limit")"
    py comp-diffs "$RESP"
}

cmd_raw_get() {
    [[ $# -eq 1 ]] || die "用法: $SELF get /路径?参数   例如: $SELF get /workers"
    api GET "$1"
    py pretty "$RESP"
}

cmd_raw_post() {
    [[ $# -ge 1 && $# -le 2 ]] || die "用法: $SELF post /路径 '{\"reason\":\"...\"}'"
    printf '%s' "${2:-"{}"}" >"$BODY"
    api POST "$1" "$BODY"
    py pretty "$RESP"
}

usage() {
    cat <<EOF
用法: $SELF <命令> [参数]
API 地址: $API（端口不是 8470 时设置 SRDQ_API=http://127.0.0.1:<端口>/v1）

概览
  status                               控制状态、执行窗口、执行器、进行中的运行、告警

1. 资产
  import-assets FILE [--update] [--allow-errors] [--preview-only] [--reason R]
                                       预览并确认资产清单；有 ERROR 行时默认不确认
  assets [--layer L] [--q 关键字] [--missing METRIC|SAMPLE] [--limit N] [--page P]

2. 范围发现
  discover [--reason R]                发起范围发现（只在执行窗口内执行）
  discover-status [--wait]             查看最近一次发现；--wait 每 ${POLL} 秒轮询直到结束

3. 清单
  manifests [--limit N]                列出清单版本
  manifest-show [REF] [--reason 取值] [--q 关键字] [--new] [--limit N] [--page P]
                                       REF: latest(默认) | draft | confirmed | 版本号 | ID
                                       --reason: IN_SCOPE SOURCE_ONLY TARGET_ONLY UNKNOWN ACCESS_DENIED
                                                 TABLE_ABSENT DISAPPEARED OUTSIDE（后三个在本地过滤）
  manifest-confirm [REF] --reason R [--yes]   确认清单（REF 默认 draft）
  manifest-reject  [REF] --reason R [--yes]

4. 规则
  import-rules <目录|文件>... [--kind METRIC|SAMPLE] [--allow-errors] [--no-approve] [--approval-ref R]
                                       目录会导入其中 metric_rules_*.csv 和 sample_rules_*.csv；
                                       导入后自动批准全部 DRAFT 且 SUPPORTED 的规则，并输出统计
  approve [--kind K] [--approval-ref R]   批准所有 DRAFT 且 SUPPORTED 的规则
  rules [--kind K] [--status DRAFT|APPROVED|SUPERSEDED|ARCHIVED] [--support SUPPORTED|UNSUPPORTED]
        [--q 关键字] [--limit N] [--page P] [--summary]

5. 计划与运行
  plan-create NAME [--kinds COUNT,METRIC,SAMPLE] [--rule-keys K1,K2]
              [--layers ODS,DWD | --tables db.t1,db.t2 | --assets ID1,ID2] [--fixed 清单REF]
  plans
  plan-show <计划名称|ID>
  run <计划名称|ID> [--kinds K] [--rule-keys K]    手动触发运行
  runs [--active] [--plan 计划] [--purpose COMPARE|METADATA_PREVIEW|MANUAL] [--limit N] [--page P]
  run-show [运行ID|latest] [--wait]
  run-action <运行ID|latest> pause|restart|resume|cancel [--reason R] [--preview-only] [--yes]
                                       pause 等在途查询结束后暂停；restart/resume 继续同一轮；cancel 永久取消
  checks [运行ID|latest] [--kind COUNT|METRIC|SAMPLE|DEEP] [--table db.t] [--limit N] [--page P]
         [--coverage PENDING|COMPLETED_EQUAL|COMPLETED_DIFFERENT|INCONCLUSIVE|ERROR|UNSUPPORTED|BLOCKED|EXCLUDED]
                                       运行的检查项列表（每行 = 表 × 分区范围 × 规则）

6. 周期
  schedule-create NAME --plan 计划 (--cron "分 时 日 月 周" | --interval 秒 | --continuous [--rest 秒])
                  [--tz Asia/Shanghai] [--start ISO时间] [--end ISO时间] [--overlap SKIP|COALESCE]
                  [--misfire SKIP|LATEST] [--cancel-on-end] [--kinds K] [--disabled] [--preview-only] [--yes]
                                       先预览触发时间，确认后再创建
  schedules
  schedule-action <周期名称|ID> enable|disable|archive|restart [--reason R]

7. 差异
  diffs [--state NEW|PERSISTENT|RESOLVED] [--kind K] [--layer L] [--table 表名] [--asset ID] [--limit N] [--page P]
  diff-show <comparison_id> [--show-values] [--type T] [--limit N] [--page P]

其他
  get /路径                            原样调用 GET 并输出 JSON，例如: $SELF get /workers
  post /路径 'JSON'                    原样调用 POST

环境变量: SRDQ_API（API 地址）SRDQ_POLL（--wait 轮询秒数，默认 60）SRDQ_TZ（显示时区，默认 Asia/Shanghai）
EOF
}

main() {
    local cmd=${1:-help}
    if (($#)); then shift; fi
    case $cmd in
        status) cmd_status "$@" ;;
        import-assets) cmd_import_assets "$@" ;;
        assets) cmd_assets "$@" ;;
        discover) cmd_discover "$@" ;;
        discover-status) cmd_discover_status "$@" ;;
        manifests) cmd_manifests "$@" ;;
        manifest-show) cmd_manifest_show "$@" ;;
        manifest-confirm) cmd_manifest_decide confirm "$@" ;;
        manifest-reject) cmd_manifest_decide reject "$@" ;;
        import-rules) cmd_import_rules "$@" ;;
        approve) cmd_approve "$@" ;;
        rules) cmd_rules "$@" ;;
        plan-create) cmd_plan_create "$@" ;;
        plans) cmd_plans "$@" ;;
        plan-show) cmd_plan_show "$@" ;;
        run) cmd_run "$@" ;;
        runs) cmd_runs "$@" ;;
        run-show) cmd_run_show "$@" ;;
        run-action) cmd_run_action "$@" ;;
        checks) cmd_checks "$@" ;;
        schedule-create) cmd_schedule_create "$@" ;;
        schedules) cmd_schedules "$@" ;;
        schedule-action) cmd_schedule_action "$@" ;;
        diffs) cmd_diffs "$@" ;;
        diff-show) cmd_diff_show "$@" ;;
        get) cmd_raw_get "$@" ;;
        post) cmd_raw_post "$@" ;;
        help | -h | --help) usage ;;
        *) usage >&2; exit 2 ;;
    esac
}

main "$@"
