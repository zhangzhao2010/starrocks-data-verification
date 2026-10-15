# -*- coding: utf-8 -*-
"""手动核对单表：双端按主键键值窗口分批拉取归并，输出全部差异行明细。

分批模式：有时间列则批=时间窗（步长默认按 COUNT 自适应，单窗≈--batch 行），每窗两端
无序拉全列、本地按键排序归并——集群侧只做范围扫描零排序；无时间列退化为键值窗 keyset。
输出三类差异：
  SRC_ONLY / TGT_ONLY  —— 仅一端存在的行（键+整行摘要）
  VALUE                —— 两端都有但某列值不同（每差异列一行，含两端完整值）
值豁免（算通过：不写 CSV、不计差异，尾部单独打印豁免行数）：
  数值尾零  77600.0 vs 77600.0000（Decimal 数值相等即豁免，覆盖 decimal 标度差/double 形态差）
  时间微秒  02:27:20.123456 vs 02:27:20（截到秒相等即豁免，同步链路微秒截断）
  JSON 格式  {"a":1, "b":2} vs {"a":1,"b":2}（parse 后深度语义相等才豁免：键间空格、键序、
            数字写法 4.0E7 vs 40000000、反斜杠u 转义差；字符串值内容/键集/顺序不同不豁免）

用法（与 sr_page_scan.py 同目录）：
  python sr_manual_diff.py --table ods.ods_workflow_core_ACT_HI_COMMENT            # 归并键自动解析
  python sr_manual_diff.py --table ods.ods_funds_manager_tb_funds_receive_order --batch 10000 --out fundsrecv
归并键（--pk 默认 auto）：自动解析 SHOW CREATE TABLE，先 PRIMARY KEY、无则 UNIQUE KEY；
  联合键整体作一组键列，CSV 键字段为逗号拼接值（csv 模块自动加引号）；也可 --pk "k1,k2" 显式指定。
断点续跑（从上一批边界键继续，CSV 追加）：
  python sr_manual_diff.py --table ... --out fundsrecv --from-key <上一批最大键，联合键逗号隔开>
时间分批（默认 auto：自动取第一个 datetime 列，按 MIN/MAX+COUNT 自适应步长切窗，批=时间窗）：
  python sr_manual_diff.py --table ods.ods_funds_manager_tb_funds_receive_order --out fundsrecv
  --time-step-days 7 固定步长；--time-col off 改纯键分批；--time-col <列名> 显式指定
结果: {out}_manualdiff.csv + 终端统计
"""
import argparse
import csv
import datetime
import decimal
import io
import json
import re
import sys
import time

from sr_page_scan import load_config, connect, q


def lit(v):
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return "'%s'" % s


def lex_gt(keys, vals):
    """字典序 key > vals：k1>v1 OR (k1=v1 AND k2>v2) OR ...；键列非空时 NOT(>) 即 <="""
    ors = []
    for i in range(len(keys)):
        ands = ["`%s` = %s" % (keys[j], lit(vals[j])) for j in range(i)]
        ands.append("`%s` > %s" % (keys[i], lit(vals[i])))
        ors.append("(" + " AND ".join(ands) + ")")
    return "(" + " OR ".join(ors) + ")"


def detect_keys(conn, db, tbl):
    """从 SHOW CREATE TABLE 解析归并键：先 PRIMARY KEY，无则 UNIQUE KEY。
    返回 (来源, [键列...])；DUPLICATE KEY 表返回 (None, None)。
    注意：表模型键在 information_schema.statistics / COLUMN_KEY 里查不到，必须解析 DDL。"""
    row = q(conn, "SHOW CREATE TABLE `%s`.`%s`" % (db, tbl))[0]
    ddl = row.get("Create Table") or list(row.values())[-1]
    m = re.search(r"PRIMARY\s+KEY\s*\(([^)]+)\)", ddl, re.I)
    src = "PRIMARY KEY"
    if not m:
        m = re.search(r"UNIQUE\s+KEY\s*\(([^)]+)\)", ddl, re.I)
        src = "UNIQUE KEY"
    if not m:
        return None, None
    return src, [c.strip().strip("`").strip() for c in m.group(1).split(",")]


def key_of(r, keys):
    return tuple(r[k] for k in keys)


def fetch_range(conn, db, tbl, cols, keys, lo, hi, limit=None, order=True, partition=None, extra=None):
    """拉取键值窗口 (lo, hi]；lo=None 首批无下界，hi=None 无上限。
    keys 为有序键列（单列或联合键），lo/hi 为等长值元组。
    order=False 时不带 ORDER BY（集群侧免排序，由调用方本地排序）。
    partition 指定后用 PARTITION(...) 物理裁剪，扫描量锁死在单分区内。
    extra 为附加 WHERE 条件（如排序键首列的月窗口），与键窗口 AND 叠加。"""
    sel = ", ".join("`%s`" % c for c in cols)
    from_ = "`%s`.`%s`" % (db, tbl)
    if partition:
        from_ += " PARTITION(%s)" % partition
    conds = []
    if lo is not None and hi is not None:
        conds.append("%s AND NOT %s" % (lex_gt(keys, lo), lex_gt(keys, hi)))
    elif lo is not None:
        conds.append(lex_gt(keys, lo))
    if extra:
        conds.append("(%s)" % extra)
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    sql = "SELECT %s FROM %s %s" % (sel, from_, where)
    if order:
        sql += " ORDER BY " + ", ".join("`%s`" % k for k in keys)
    if limit:
        sql += " LIMIT %d" % limit
    return q(conn, sql)


def row_str(r, cols):
    return " | ".join("%s=%s" % (c, to_str(r[c])) for c in cols)


def to_str(v):
    if v is None:
        return "<NULL>"
    return str(v)


def _as_dec(s):
    try:
        return decimal.Decimal(s)
    except Exception:
        return None


def _as_sec(s):
    try:
        return datetime.datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _deep_eq(a, b):
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_deep_eq(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_deep_eq(x, y) for x, y in zip(a, b))
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    return a == b


def _json_eq(sa, sb):
    """""""""JSON 形态字符串：parse 后深度语义相等才豁免（空格/键序/数字写法/unicode转义差），
    字符串值内容、键集、数组顺序不同不豁免（防把真差异当格式差放走）。"""
    try:
        ja, jb = json.loads(sa), json.loads(sb)
    except Exception:
        return False
    return _deep_eq(ja, jb)


def val_eq(va, vb):
    """""""""字符串严格相等之外的三类豁免：数值尾零、时间微秒截断、JSON 序列化格式差。"""""""""
    sa, sb = to_str(va), to_str(vb)
    if sa == sb:
        return True
    da, dbv = _as_dec(sa), _as_dec(sb)
    if da is not None and dbv is not None and da == dbv:
        return True
    ta, tv = _as_sec(sa), _as_sec(sb)
    if ta is not None and tv is not None and ta == tv:
        return True
    if sa[:1] in ("{", "[") and sb[:1] in ("{", "["):
        return _json_eq(sa, sb)
    return False


def key_str(kt):
    """键值元组拼成 CSV 单字段（联合键逗号隔开，csv 模块自动加引号）"""
    return ",".join(to_str(v) for v in kt)


def _dt_floor(v):
    """把 date/datetime/字符串统一对齐到当天 00:00:00。"""
    if isinstance(v, datetime.datetime):
        d = v
    elif isinstance(v, datetime.date):
        d = datetime.datetime(v.year, v.month, v.day)
    else:
        d = datetime.datetime.strptime(str(v)[:19], "%Y-%m-%d %H:%M:%S")
    return d.replace(hour=0, minute=0, second=0, microsecond=0)


def main():
    # nohup 重定向下 stdout 默认块缓冲，日志会攒到几 KB 才落盘，先改成行缓冲实时刷
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="work/config.yaml")
    ap.add_argument("--table", required=True, help="db.table")
    ap.add_argument("--pk", default="auto",
                    help="归并键：auto(默认)=解析 SHOW CREATE TABLE，先 PRIMARY KEY 无则 UNIQUE KEY；"
                         "或显式列名，联合键逗号隔开如 sharding_time,id；必须唯一")
    ap.add_argument("--out", default="manual")
    ap.add_argument("--batch", type=int, default=10000, help="每批行数，默认 1 万")
    ap.add_argument("--sleep", type=float, default=5.0, help="批间隔秒数，默认 5，0 关闭")
    ap.add_argument("--partition", default=None, help="只跑指定分区（如 p202610），扫描量锁死在单分区内")
    ap.add_argument("--where", dest="extra_where", default=None,
                    help="附加范围条件，如 sharding_time>='2026-09-01' AND sharding_time<'2026-10-01'（排序键首列范围=穷人版分区裁剪）")
    ap.add_argument("--time-col", default="auto",
                    help="时间分段列：auto=自动取共同列中第一个 datetime 列（默认），off=不分段，或显式列名")
    ap.add_argument("--time-step-days", type=int, default=0,
                    help="时间批步长（天）：0(默认)=按COUNT自适应，单窗≈--batch行；>0 固定天数")
    ap.add_argument("--from-key", default=None,
                    help="断点续跑起始键（不含该键本身），CSV 以追加模式写入")
    a = ap.parse_args()

    db, tbl = a.table.split(".", 1)
    sc = load_config(a.config, "source")[0]
    tc = load_config(a.config, "target")[0]
    sconn, tconn = connect(sc, 7200), connect(tc, 7200)
    q(sconn, "SET query_timeout=7200")
    q(tconn, "SET query_timeout=7200")

    def cols_of(conn):
        rows = q(conn, "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM columns "
                       "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' ORDER BY ORDINAL_POSITION"
                 % (db, tbl))
        return {r["COLUMN_NAME"]: (r["DATA_TYPE"], r["IS_NULLABLE"]) for r in rows}

    scols, tcols = cols_of(sconn), cols_of(tconn)
    common = [c for c in scols if c in tcols]
    print("列: src=%d tgt=%d common=%d" % (len(scols), len(tcols), len(common)))
    # ---- 归并键解析：auto=主键→唯一键（联合键整体一组）；显式则逗号拆分 ----
    if a.pk.strip().lower() == "auto":
        key_src, keys = detect_keys(sconn, db, tbl)
        if not keys:
            raise SystemExit("未解析到 PRIMARY KEY / UNIQUE KEY（DUPLICATE KEY 表无唯一约束），"
                             "请用 --pk 显式指定唯一列，联合键逗号隔开")
        print("归并键: 自动解析 %s(%s)" % (key_src, ",".join(keys)))
    else:
        keys = [c.strip() for c in a.pk.split(",") if c.strip()]
        print("归并键: 手动指定 (%s)" % ",".join(keys))
    bad_key = [k for k in keys if k not in common]
    if bad_key:
        raise SystemExit("键列 %s 不在共同列中" % bad_key)
    null_keys = [k for k in keys if scols[k][1].upper() == "YES"]
    if null_keys:
        print("警告: 键列 %s 可空，NULL 键的行不参与窗口归并，结果会漏这些行" % null_keys)
    if len(scols) != len(tcols):
        only_s = sorted(set(scols) - set(tcols))
        only_t = sorted(set(tcols) - set(scols))
        print("  仅源端列: %s | 仅目标端列: %s（不参与比对）" % (only_s, only_t))

    print("分批归并: 单批目标 %d 行，批间隔 %ss（有时间列按时间窗，无则键值窗）%s%s" % (
        a.batch, a.sleep,
        (" 分区=" + a.partition) if a.partition else "",
        (" where=" + a.extra_where) if a.extra_where else ""))
    # ---- 时间分段：默认自动取共同列中第一个 datetime 列，按其 MIN/MAX 切段 ----
    # 列可空时追加 NULL 兜底段，防止 NULL 行被所有时间窗排除而漏比对
    tcol = a.time_col
    tcol_nullable = False
    if tcol == "auto":
        tcol = None
        for c in common:
            if scols[c][0].lower() in ("datetime", "date", "timestamp"):
                tcol = c
                tcol_nullable = (scols[c][1].upper() == "YES")
                break
    elif tcol.lower() in ("off", "none", ""):
        tcol = None
    if tcol and tcol not in common:
        raise SystemExit("时间分段列 %s 不在共同列中" % tcol)
    if tcol:
        tcol_nullable = tcol_nullable or (scols[tcol][1].upper() == "YES")
    scopes = [(None, None, False)]
    if tcol:
        mm_sql = "SELECT MIN(`%s`) mn, MAX(`%s`) mx FROM `%s`.`%s`" % (tcol, tcol, db, tbl)
        cnt_sql = "SELECT COUNT(*) c FROM `%s`.`%s`" % (db, tbl)
        if a.extra_where:
            mm_sql += " WHERE (%s)" % a.extra_where
            cnt_sql += " WHERE (%s)" % a.extra_where
        r = q(sconn, mm_sql)[0]
        if r["mn"] is not None:
            cur, end = _dt_floor(r["mn"]), _dt_floor(r["mx"])
            step = a.time_step_days
            if step <= 0:
                # 自适应步长：COUNT 估算行/天，使单窗行数≈--batch（自适应下限 1 天）
                cnt = q(sconn, cnt_sql)[0]["c"]
                span_days = max(1, (end - cur).days + 1)
                rpd = max(1.0, float(cnt) / span_days)
                step = max(1, int(round(a.batch / rpd)))
                if rpd > 5 * a.batch:
                    print("警告: 平均单日约 %d 行远超单批目标 %d 行，尖峰日单窗内存会放大，"
                          "必要时 --time-col off 改纯键分批" % (int(rpd), a.batch))
            scopes = []
            while cur <= end:
                nxt = cur + datetime.timedelta(days=step)
                scopes.append((cur, nxt, False))
                cur = nxt
            if tcol_nullable:
                scopes.append((None, None, True))
            print("时间分批: 列=%s 共 %d 批（步长 %d 天%s）" % (
                tcol, len(scopes), step, "，含NULL兜底" if tcol_nullable else ""))
        else:
            print("时间分批: %s 全空或全 NULL，单批全表" % tcol)
    if tcol and a.from_key:
        print("提示: 时间批模式下 --from-key 不生效，续跑用 --where \"%s >= '断点日期'\" 收窄" % tcol)
    n_val = n_src = n_tgt = n_same = n_exempt = n_dup = 0
    tot_s = tot_t = bno = 0
    col_diff_stat = {}
    lo0 = None
    if a.from_key:
        lo0 = tuple(a.from_key.split(","))
        if len(lo0) != len(keys):
            raise SystemExit("--from-key 需为 %d 段键值（逗号隔开），对应键列 %s"
                             % (len(keys), ",".join(keys)))
    def merge_rows(sa, tb):
        """两端同窗全量本地按键排序后线性归并，差异写 CSV；返回 (S仅, T仅, 值差) 增量。"""
        nonlocal n_val, n_src, n_tgt, n_same, n_exempt, n_dup
        sa = list(sa)
        tb = list(tb)
        sa.sort(key=lambda r: key_of(r, keys))
        tb.sort(key=lambda r: key_of(r, keys))
        ka = [key_of(r, keys) for r in sa]
        kt = [key_of(r, keys) for r in tb]
        seen = {}
        for k in ka:
            seen[k] = seen.get(k, 0) + 1
        dups = dict((k, n) for k, n in seen.items() if n > 1)
        if dups:
            n_dup += len(dups)
            k0 = sorted(dups)[0]
            print("警告: 本窗源端归并键不唯一（重复 %d 个键，如 %s 有 %d 行），同键行配对可能错位，"
                  "结果不可信，需换更完整键重跑（累计 %d）" % (len(dups), key_str(k0), dups[k0], n_dup))
        b0 = (n_src, n_tgt, n_val)
        i = j = 0
        while i < len(sa) or j < len(tb):
            if j >= len(tb) or (i < len(sa) and ka[i] < kt[j]):
                r = sa[i]; i += 1; n_src += 1
                w.writerow(["SRC_ONLY", key_str(key_of(r, keys)), "*", row_str(r, common), ""])
            elif i >= len(sa) or ka[i] > kt[j]:
                r = tb[j]; j += 1; n_tgt += 1
                w.writerow(["TGT_ONLY", key_str(key_of(r, keys)), "*", "", row_str(r, common)])
            else:
                kk = ka[i]
                ra, rb = sa[i], tb[j]; i += 1; j += 1
                bad, exempt = [], False
                for c in common:
                    if to_str(ra[c]) != to_str(rb[c]):
                        if val_eq(ra[c], rb[c]):
                            exempt = True
                        else:
                            bad.append(c)
                if bad:
                    n_val += 1
                    for c in bad:
                        col_diff_stat[c] = col_diff_stat.get(c, 0) + 1
                        w.writerow(["VALUE", key_str(kk), c, to_str(ra[c]), to_str(rb[c])])
                elif exempt:
                    n_exempt += 1
                else:
                    n_same += 1
        return n_src - b0[0], n_tgt - b0[1], n_val - b0[2]

    mode = "a" if a.from_key else "w"
    with io.open(a.out + "_manualdiff.csv", mode, newline="", encoding="utf-8") as fout:
        w = csv.writer(fout)
        if not a.from_key:
            w.writerow(["diff_type", ",".join(keys), "column", "src_v", "tgt_v"])
        for (s0, s1, null_seg) in scopes:
            extra = a.extra_where
            if null_seg:
                seg = "`%s` IS NULL" % tcol
                extra = ("(%s) AND (%s)" % (a.extra_where, seg)) if a.extra_where else seg
                head = "[%s IS NULL 兜底段]" % tcol
            elif s0 is not None:
                seg = "`%s` >= '%s' AND `%s` < '%s'" % (tcol, s0, tcol, s1)
                extra = ("(%s) AND (%s)" % (a.extra_where, seg)) if a.extra_where else seg
                head = "[%s, %s)" % (s0, s1)
            else:
                head = "[全表]"
            if tcol:
                # 时间批：整窗两端各拉一次（时间范围扫描，集群零排序），本地按键排序归并
                print("批 %d %s: 拉取两端..." % (bno + 1, head))
                sa = fetch_range(sconn, db, tbl, common, keys, None, None, order=False,
                                 partition=a.partition, extra=extra)
                tb = fetch_range(tconn, db, tbl, common, keys, None, None, order=False,
                                 partition=a.partition, extra=extra)
                ds, dt2, dv = merge_rows(sa, tb)
                tot_s += len(sa); tot_t += len(tb)
                bno += 1
                print("批 %d %s: src=%d tgt=%d 差异(S=%d/T=%d/V=%d) | 累计 src=%d tgt=%d"
                      % (bno, head, len(sa), len(tb), ds, dt2, dv, tot_s, tot_t))
                if a.sleep > 0:
                    time.sleep(a.sleep)
                continue
            lo = lo0
            while True:
                # 无时间列时兜底：键值窗 keyset，源端先拉键列探出本批窗口 (lo, hi]
                kp = fetch_range(sconn, db, tbl, keys, keys, lo, None, a.batch, partition=a.partition, extra=extra)
                if not kp:
                    # 源端已拉完：目标端剩余行分批清尾，全部记 TGT_ONLY
                    tb = fetch_range(tconn, db, tbl, common, keys, lo, None, a.batch, partition=a.partition, extra=extra)
                    if not tb:
                        break
                    for r in tb:
                        n_tgt += 1
                        w.writerow(["TGT_ONLY", key_str(key_of(r, keys)), "*", "", row_str(r, common)])
                    tot_t += len(tb)
                    lo = key_of(tb[-1], keys)
                    print("清尾: hi=%s 目标端剩余 +%d（累计 tgt=%d）" % (key_str(lo), len(tb), tot_t))
                    continue
                hi = key_of(kp[-1], keys)
                # 两端按同一窗口无序拉全列，本地排序（集群侧不做全列排序）
                sa = fetch_range(sconn, db, tbl, common, keys, lo, hi, order=False, partition=a.partition, extra=extra)
                tb = fetch_range(tconn, db, tbl, common, keys, lo, hi, order=False, partition=a.partition, extra=extra)
                tot_s += len(sa); tot_t += len(tb)
                ds, dt2, dv = merge_rows(sa, tb)
                lo = hi
                bno += 1
                print("批 %d: hi=%s 本批 src=%d tgt=%d 差异(S=%d/T=%d/V=%d) | 累计 src=%d tgt=%d"
                      % (bno, key_str(hi), len(sa), len(tb), ds, dt2, dv, tot_s, tot_t))
                if a.sleep > 0:
                    time.sleep(a.sleep)

    print("=" * 60)
    print("src=%d tgt=%d，共 %d 批" % (tot_s, tot_t, bno))
    print("共同行一致: %d" % n_same)
    print("豁免通过(数值尾零/微秒截断/JSON格式): %d" % n_exempt)
    if n_dup:
        print("!! 归并键重复警告累计 %d 处，同键行配对可能错位，以上结果不可信，需换更完整键重跑" % n_dup)
    print("同键值不同行: %d" % n_val)
    print("仅源端有: %d" % n_src)
    print("仅目标端有: %d" % n_tgt)
    if col_diff_stat:
        print("值差异按列分布:")
        for c, n in sorted(col_diff_stat.items(), key=lambda x: -x[1]):
            print("  %-20s %d 行" % (c, n))
    print("明细 -> %s_manualdiff.csv" % a.out)


if __name__ == "__main__":
    main()
