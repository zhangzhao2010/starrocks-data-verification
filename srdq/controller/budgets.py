"""RUN/NODE budget reservation, cumulative usage accounting, and settlement (doc 03 §13.5)."""
from __future__ import annotations

from typing import Any


def _needed(transfer_required: bool) -> tuple[str, ...]:
    return ("RESULT_BYTES", "TRANSFER_BYTES") if transfer_required else ("RESULT_BYTES",)


def reserve(cur, run_id, node_id, transfer_required: bool, cap: int) -> tuple[str, list[dict]]:
    """Returns ("OK", entries) | ("WAIT", []) | ("EXCEEDED", [])."""
    cur.execute("""SELECT * FROM dq_budget WHERE run_id=%s AND (level='RUN' OR node_id=%s) AND resource = ANY(%s)
                   ORDER BY level DESC, resource FOR UPDATE""",
                (run_id, node_id, list(_needed(transfer_required))))
    rows = cur.fetchall()
    for b in rows:
        if b["budget_state"] != "OPEN":
            return "EXCEEDED", []
        if int(b["consumed"]) + int(b["reserved"]) + cap > int(b["limit_amount"]):
            return ("WAIT" if int(b["reserved"]) > 0 else "EXCEEDED"), []
    entries = []
    for b in rows:
        cur.execute("UPDATE dq_budget SET reserved = reserved + %s, updated_at=now() WHERE id=%s", (cap, b["id"]))
        entries.append({"budget_id": str(b["id"]), "resource": b["resource"], "level": b["level"], "amount": cap,
                        "used": 0})
    return "OK", entries


def consume(cur, entries: list[dict], resource: str, delta: int) -> list[dict]:
    """Account `delta` more units of `resource`; returns budgets that just became EXCEEDED."""
    exceeded = []
    if delta <= 0:
        return exceeded
    for e in entries:
        if e["resource"] != resource:
            continue
        from_res = max(0, min(delta, e["amount"] - e["used"]))
        e["used"] += delta
        cur.execute("""UPDATE dq_budget SET reserved = GREATEST(reserved - %s, 0), consumed = consumed + %s,
                       overrun_amount = GREATEST(consumed + %s - limit_amount, 0),
                       budget_state = CASE WHEN consumed + %s > limit_amount THEN 'EXCEEDED' ELSE budget_state END,
                       updated_at = now() WHERE id=%s
                       RETURNING id, run_id, node_id, level, resource, budget_state, consumed, limit_amount""",
                    (from_res, delta, delta, delta, e["budget_id"]))
        b = cur.fetchone()
        if b["budget_state"] == "EXCEEDED" and int(b["consumed"]) - delta <= int(b["limit_amount"]):
            exceeded.append(b)
    return exceeded


def settle(cur, entries: list[dict]) -> None:
    """Release what is left of the reservations (query is known to have ended)."""
    for e in entries:
        remaining = max(0, e["amount"] - e["used"])
        if remaining:
            cur.execute("UPDATE dq_budget SET reserved = GREATEST(reserved - %s, 0), updated_at=now() WHERE id=%s",
                        (remaining, e["budget_id"]))
        e["amount"] = min(e["amount"], e["used"])


def progress_entries(progress: dict | None) -> list[dict]:
    return list((progress or {}).get("reservations") or [])


def with_entries(progress: dict | None, entries: list[dict], **extra: Any) -> dict:
    p = dict(progress or {})
    p["reservations"] = entries
    p.update(extra)
    return p
