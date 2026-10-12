"""Side-result comparison (doc 03 §14): verdicts, bounded difference lists, no raw values for CONTENT_HASH."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterator

from ..common import canonical as c
from .common import Ctx
from .intake import material_files

MAX_VALUE_CHARS = 1000
ROW_HASH_COLUMN = "<row_hash>"


class CompareError(Exception):
    pass


@dataclass
class Diffs:
    cap: int
    items: list[dict] = field(default_factory=list)
    count: int = 0

    def add(self, type_: str, **fields: Any) -> None:
        self.count += 1
        if len(self.items) < self.cap:
            for k in ("source_value", "target_value"):
                v = fields.get(k)
                if isinstance(v, str) and len(v) > MAX_VALUE_CHARS:
                    fields[k] = v[:MAX_VALUE_CHARS]
                    fields.setdefault("detail", {})[f"{k}_truncated"] = True
            fields.setdefault("detail", {})
            self.items.append({"type": type_, **fields})


@dataclass
class Outcome:
    verdict: str
    reason: str | None
    summary: dict
    diffs: Diffs
    source_ids: list
    target_ids: list
    source_read: tuple
    target_read: tuple


def _pairs(results: list[dict], source_cluster_id: str, target_cluster_id: str) -> dict[str, dict[str, dict]]:
    out: dict[str, dict[str, dict]] = {}
    for r in results:
        role = "source" if r["cluster_id"] == source_cluster_id else \
            "target" if r["cluster_id"] == target_cluster_id else None
        if role is None:
            raise CompareError(f"result belongs to unexpected cluster {r['cluster_id']}")
        out.setdefault(str(r["node_id"]), {})[role] = r
    for nid, pair in out.items():
        if set(pair) != {"source", "target"}:
            raise CompareError(f"member {nid} lacks an accepted result on both clusters")
    return out


def _read_window(rs: list[dict]) -> tuple:
    return min(r["read_started"] for r in rs), max(r["read_finished"] for r in rs)


def compare(ctx: Ctx, rc: dict, results: list[dict], load_groups: Callable[[Any], dict] | None = None) -> Outcome:
    """Compare the ACCEPTED cluster results of one completed observation."""
    xs = rc["execution_snapshot"]
    source_cluster_id, target_cluster_id = xs["source_cluster_id"], xs["target_cluster_id"]
    pairs = _pairs(results, source_cluster_id, target_cluster_id)
    src = [p["source"] for p in pairs.values()]
    tgt = [p["target"] for p in pairs.values()]
    diffs = Diffs(int(ctx.cfg["retention"]["max_difference_rows"]))
    summary = {"pair_id": xs["pair_id"], "source_cluster_id": source_cluster_id,
               "target_cluster_id": target_cluster_id}
    out = Outcome("OBSERVED_EQUAL", None, summary, diffs, [r["id"] for r in src], [r["id"] for r in tgt],
                  _read_window(src), _read_window(tgt))
    skew = _skew(ctx, pairs)
    out.summary["timing"] = skew
    if skew["violation"]:
        out.verdict, out.reason = "INCONCLUSIVE", skew["violation"]
        return out
    kind = rc["check_kind"]
    d = rc["rule_snapshot"]["definition"]
    cols = rc["execution_snapshot"].get("columns") or {}
    if kind == "COUNT":
        _count(pairs, out)
    elif kind == "METRIC":
        if load_groups is None:
            raise CompareError("METRIC comparison needs a group loader")
        _metric(d, pairs, out, load_groups)
    elif kind == "SAMPLE":
        _sample(ctx, d, cols, pairs, out)
    else:
        _deep(ctx, d, cols, rc["expected_evidence"], pairs, out)
    return out


def error_outcome(ctx: Ctx, rc: dict, results: list[dict], message: str) -> Outcome:
    xs = rc["execution_snapshot"]
    src = [r for r in results if r["cluster_id"] == xs["source_cluster_id"]]
    tgt = [r for r in results if r["cluster_id"] == xs["target_cluster_id"]]
    if not src or not tgt:
        raise CompareError("cannot record a comparison without results from both clusters")
    summary = {"error": message[:2000], "pair_id": xs["pair_id"],
               "source_cluster_id": xs["source_cluster_id"], "target_cluster_id": xs["target_cluster_id"]}
    return Outcome("ERROR", "COMPARE_FAILED", summary, Diffs(int(ctx.cfg["retention"]["max_difference_rows"])),
                   [r["id"] for r in src], [r["id"] for r in tgt], _read_window(src), _read_window(tgt))


def _skew(ctx: Ctx, pairs: dict) -> dict:
    cmp = ctx.cfg["comparison"]
    max_skew = float(cmp["max_start_skew_seconds"])
    deadline = float(cmp["observation_deadline_seconds"])
    worst_skew = worst_span = 0.0
    violation = None
    for p in pairs.values():
        s, t = p["source"], p["target"]
        sk = abs((s["read_started"] - t["read_started"]).total_seconds())
        span = (max(s["read_finished"], t["read_finished"]) - min(s["read_started"], t["read_started"])).total_seconds()
        worst_skew, worst_span = max(worst_skew, sk), max(worst_span, span)
        if violation is None and sk > max_skew:
            violation = "START_SKEW"
        if violation is None and span > deadline:
            violation = "OBSERVATION_DEADLINE"
    return {"max_start_skew_seconds": round(worst_skew, 3), "max_span_seconds": round(worst_span, 3),
            "skew_limit_seconds": max_skew, "deadline_seconds": deadline, "violation": violation}


def _finish(out: Outcome, unsupported: str | None = None, inconclusive: str | None = None) -> None:
    if out.diffs.count:
        out.verdict = "OBSERVED_DIFFERENT"
    elif unsupported:
        out.verdict, out.reason = "UNSUPPORTED", unsupported
    elif inconclusive:
        out.verdict, out.reason = "INCONCLUSIVE", inconclusive
    else:
        out.verdict = "OBSERVED_EQUAL"


# ---- COUNT ---------------------------------------------------------------------------------

def _count(pairs: dict, out: Outcome) -> None:
    (p,) = pairs.values()
    s = int(p["source"]["payload"]["row_count"])
    t = int(p["target"]["payload"]["row_count"])
    out.summary.update(source_rows=str(s), target_rows=str(t))
    if s != t:
        rel = None if s == 0 else str((Decimal(abs(t - s)) / Decimal(s)).quantize(Decimal("0.000001")))
        out.diffs.add("COUNT_DELTA", source_value=str(s), target_value=str(t),
                      detail={"signed_delta": str(t - s), "abs_delta": str(abs(t - s)), "relative_delta": rel})
    _finish(out)


# ---- METRIC --------------------------------------------------------------------------------

def load_groups(db, side_result_id) -> dict[bytes, dict]:
    groups: dict[bytes, dict] = {}
    by_id: dict = {}
    for g in db.query("SELECT * FROM dq_metric_group WHERE side_result_id=%s ORDER BY ordinal", (side_result_id,)):
        key = bytes(g["canonical_group_key"])
        groups[key] = {"values": g["group_values"], "rows": int(g["group_rows"]), "nn": g["non_null_guards"], "m": {}}
        by_id[g["id"]] = groups[key]
    for v in db.query("SELECT * FROM dq_metric_value WHERE side_result_id=%s", (side_result_id,)):
        by_id[v["group_id"]]["m"][v["metric_id"]] = {"type": v["value_type"], "state": v["value_state"],
                                                     "text": v["value_text"], "scale": v["scale"]}
    return groups


def _metric_equal(m: dict, a: dict, b: dict) -> tuple[bool, bool]:
    """(equal, non_finite_seen) for one metric value pair."""
    if m.get("compare") == "TOLERANCE":
        bad = ("NAN", "POS_INF", "NEG_INF")
        if a["state"] in bad or b["state"] in bad:
            return True, True
        if a["state"] == "NULL" or b["state"] == "NULL":
            return a["state"] == b["state"], False
        return c.floats_close(float(a["text"]), float(b["text"]), float(m["rtol"]), float(m["atol"])), False
    if a["state"] in ("NAN", "POS_INF", "NEG_INF") or b["state"] in ("NAN", "POS_INF", "NEG_INF"):
        return True, True
    return (a["state"], a["text"]) == (b["state"], b["text"]), False


def _delta(m: dict, a: dict, b: dict) -> dict:
    if a["state"] != "FINITE" or b["state"] != "FINITE":
        return {"source_state": a["state"], "target_state": b["state"]}
    if m.get("value_type") in ("FLOAT32", "FLOAT64"):
        return {"signed_delta": c.float_text(float(b["text"]) - float(a["text"]))}
    return {"signed_delta": format(Decimal(b["text"]) - Decimal(a["text"]), "f")}


def _metric(d: dict, pairs: dict, out: Outcome, loader: Callable[[Any], dict]) -> None:
    (p,) = pairs.values()
    sg, tg = loader(p["source"]["id"]), loader(p["target"]["id"])
    metrics = d["metrics"]
    sums = [m["id"] for m in metrics if m["function"] == "SUM"]
    non_finite = False
    common = 0
    for key in sorted(set(sg) | set(tg)):
        a, b = sg.get(key), tg.get(key)
        if b is None:
            out.diffs.add("GROUP_SOURCE_ONLY", group_key=key, group_values=a["values"],
                          detail={"source_rows": str(a["rows"])})
            continue
        if a is None:
            out.diffs.add("GROUP_TARGET_ONLY", group_key=key, group_values=b["values"],
                          detail={"target_rows": str(b["rows"])})
            continue
        common += 1
        for mid in sums:
            if a["nn"].get(mid) != b["nn"].get(mid):
                out.diffs.add("GUARD_DIFF", group_key=key, group_values=a["values"], metric_id=mid,
                              source_value=a["nn"].get(mid), target_value=b["nn"].get(mid),
                              detail={"guard": "NON_NULL_COUNT"})
        for m in metrics:
            va, vb = a["m"][m["id"]], b["m"][m["id"]]
            eq, nf = _metric_equal(m, va, vb)
            non_finite = non_finite or nf
            if not eq:
                out.diffs.add("METRIC_VALUE_DIFF", group_key=key, group_values=a["values"], metric_id=m["id"],
                              source_value=va["text"], target_value=vb["text"],
                              detail={"function": m["function"], "compare": m.get("compare") or "EXACT",
                                      **_delta(m, va, vb)})
    out.summary.update(source_groups=len(sg), target_groups=len(tg), common_groups=common)
    _finish(out, unsupported="FLOAT_NON_FINITE" if non_finite else None)


# ---- row material ---------------------------------------------------------------------------

def _lines(paths: list[Path]) -> Iterator[dict]:
    for path in paths:
        with open(path, "rb") as f:
            for raw in f:
                if raw.strip():
                    yield json.loads(raw)


def _merge(a: Iterator[dict], b: Iterator[dict], key: str) -> Iterator[tuple[dict | None, dict | None]]:
    x, y = next(a, None), next(b, None)
    while x is not None or y is not None:
        if y is None or (x is not None and x[key] < y[key]):
            yield x, None
            x = next(a, None)
        elif x is None or y[key] < x[key]:
            yield None, y
            y = next(b, None)
        else:
            yield x, y
            x, y = next(a, None), next(b, None)


class Rows:
    """Display helpers for one projection."""

    def __init__(self, d: dict, cols: dict):
        proj = d["projection"]
        self.exact = proj["exact_columns"]
        self.tolerant = proj.get("tolerant_columns") or []
        self.keys = proj.get("key_columns") or []
        self.cols = cols
        tol = d.get("float_tolerance") or {}
        self.rtol = float(tol.get("rtol") or 0)
        self.atol = float(tol.get("atol") or 0)

    def show(self, name: str, val_hex: str) -> str | None:
        (_tag, payload), = c.decode_vals(bytes.fromhex(val_hex), 0, 1)[0]
        col = self.cols[name]
        return c.payload_display(col["type"], payload, scale=col.get("scale"))

    def key_values(self, key_hex: str) -> list:
        vals = c.decode_key(bytes.fromhex(key_hex))
        return [c.payload_display(self.cols[n]["type"], p, scale=self.cols[n].get("scale"))
                for n, (_t, p) in zip(self.keys, vals)]

    def tolerant_equal(self, a: str | None, b: str | None) -> tuple[bool, bool]:
        if a is None or b is None:
            return a is None and b is None, False
        fa, fb = float(a), float(b)
        if c.float_state(fa) != "FINITE" or c.float_state(fb) != "FINITE":
            return True, True
        return c.floats_close(fa, fb, self.rtol, self.atol), False


def _keyed_rows(rows: Rows, sl: Iterator[dict], tl: Iterator[dict], out: Outcome, prefix: str, *,
                with_values: bool, extra: dict) -> tuple[int, int, int, bool]:
    """Merge-join keyed material. Returns (source_rows, target_rows, matched, non_finite)."""
    ns = nt = matched = 0
    non_finite = False
    for x, y in _merge(sl, tl, "k"):
        k = (x or y)["k"]
        if y is None:
            ns += 1
            out.diffs.add(f"{prefix}_KEY_SOURCE_ONLY", key_hex=k, key_values=rows.key_values(k), detail=dict(extra))
            continue
        if x is None:
            nt += 1
            out.diffs.add(f"{prefix}_KEY_TARGET_ONLY", key_hex=k, key_values=rows.key_values(k), detail=dict(extra))
            continue
        ns += 1
        nt += 1
        matched += 1
        kv = rows.key_values(k)
        if x["h"] != y["h"]:
            if with_values:
                for name, va, vb in zip(rows.exact, x["v"], y["v"]):
                    if va != vb:
                        out.diffs.add(f"{prefix}_FIELD_DIFF", key_hex=k, key_values=kv, column_name=name,
                                      source_value=rows.show(name, va), target_value=rows.show(name, vb),
                                      detail=dict(extra))
            else:
                out.diffs.add(f"{prefix}_FIELD_DIFF", key_hex=k, key_values=kv, column_name=ROW_HASH_COLUMN,
                              source_value=x["h"], target_value=y["h"], detail=dict(extra))
        for name in rows.tolerant:
            a, b = x["t"][name], y["t"][name]
            eq, nf = rows.tolerant_equal(a, b)
            non_finite = non_finite or nf
            if not eq:
                if with_values:
                    out.diffs.add(f"{prefix}_FIELD_DIFF", key_hex=k, key_values=kv, column_name=name,
                                  source_value=a, target_value=b,
                                  detail={**extra, "compare": "TOLERANCE", "rtol": rows.rtol, "atol": rows.atol})
                else:
                    out.diffs.add(f"{prefix}_FIELD_DIFF", key_hex=k, key_values=kv, column_name=name,
                                  detail={**extra, "compare": "TOLERANCE", "values_withheld": True})
    return ns, nt, matched, non_finite


# ---- SAMPLE --------------------------------------------------------------------------------

def _sample(ctx: Ctx, d: dict, cols: dict, pairs: dict, out: Outcome) -> None:
    (p,) = pairs.values()
    s, t = p["source"], p["target"]
    rows = Rows(d, cols)
    sp, tp = s["payload"], t["payload"]
    ns, nt = int(sp["sample_rows"]), int(tp["sample_rows"])
    fast = (not rows.tolerant and sp["sample_row_digest"] == tp["sample_row_digest"]
            and sp["sample_key_digest"] == tp["sample_key_digest"] and ns == nt)
    non_finite = False
    matched = ns if fast else 0
    if not fast:
        _ns, _nt, matched, non_finite = _keyed_rows(
            rows, _lines(material_files(ctx, s["id"], sp)), _lines(material_files(ctx, t["id"], tp)), out,
            "SAMPLE", with_values=True, extra={})
    out.summary.update(source_rows=str(ns), target_rows=str(nt), matched_keys=str(matched), fast_path=fast)
    min_rows = int(d.get("min_sample_rows_per_side") or ctx.cfg["checks"]["sample"]["min_sample_rows_per_side"])
    _finish(out, unsupported="FLOAT_NON_FINITE" if non_finite else None,
            inconclusive="INSUFFICIENT_SAMPLE" if min(ns, nt) < max(1, min_rows) else None)


# ---- DEEP ----------------------------------------------------------------------------------

def _deep(ctx: Ctx, d: dict, cols: dict, evidence: str, pairs: dict, out: Outcome) -> None:
    rows = Rows(d, cols)
    with_values = evidence == "EXACT"
    keyless = not rows.keys
    non_finite = False
    members_equal = 0
    tot_s = tot_t = 0
    for nid in sorted(pairs):
        s, t = pairs[nid]["source"], pairs[nid]["target"]
        sp, tp = s["payload"], t["payload"]
        ns, nt = int(sp["row_count"]), int(tp["row_count"])
        tot_s, tot_t = tot_s + ns, tot_t + nt
        extra = {"node_id": nid}
        before = out.diffs.count
        if ns != nt:
            out.diffs.add("DEEP_ROW_COUNT_DIFF", source_value=str(ns), target_value=str(nt),
                          detail={**extra, "signed_delta": str(nt - ns)})
        if sp["digest"] == tp["digest"] and ns == nt and (keyless or not rows.tolerant):
            members_equal += 1
            continue
        sl = _lines(material_files(ctx, s["id"], sp))
        tl = _lines(material_files(ctx, t["id"], tp))
        if keyless:
            for x, y in _merge(sl, tl, "h"):
                cs, ct = (x or {}).get("n", 0), (y or {}).get("n", 0)
                if cs != ct:
                    det = {**extra, "row_hash": (x or y)["h"], "source_count": cs, "target_count": ct}
                    if with_values:
                        det["values"] = {n: rows.show(n, v) for n, v in zip(rows.exact, (x or y)["v"])}
                    out.diffs.add("DEEP_DIGEST_DIFF", detail=det)
        else:
            _a, _b, _m, nf = _keyed_rows(rows, sl, tl, out, "DEEP", with_values=with_values, extra=extra)
            non_finite = non_finite or nf
        if out.diffs.count == before:
            members_equal += 1
    out.summary.update(members=len(pairs), members_equal=members_equal, source_rows=str(tot_s),
                       target_rows=str(tot_t), keyless=keyless, evidence=evidence)
    _finish(out, unsupported="FLOAT_NON_FINITE" if non_finite else None)
