"""Execution window arithmetic (local timezone, may wrap midnight) with manual close/extend overrides."""
from __future__ import annotations

import datetime as _dt
from zoneinfo import ZoneInfo


def _minutes(hhmm: str) -> int:
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def window_bounds(cfg, key: str, overrides: dict | None = None) -> tuple[_dt.datetime, _dt.datetime, _dt.datetime]:
    """(open, drain, close) in UTC for the window that opens on local date `key`."""
    w = cfg["execution_window"]
    tz = ZoneInfo(w["timezone"])
    o, d, c = _minutes(w["open_at"]), _minutes(w["drain_at"]), _minutes(w["close_at"])
    day = _dt.date.fromisoformat(key)
    open_local = _dt.datetime(day.year, day.month, day.day, o // 60, o % 60, tzinfo=tz)
    open_at = open_local.astimezone(_dt.timezone.utc)
    drain_at = open_at + _dt.timedelta(minutes=(d - o) % 1440)
    close_at = open_at + _dt.timedelta(minutes=(c - o) % 1440 or 1440)
    ov = (overrides or {}).get(key) or {}
    if ov.get("extend_until"):
        ext = _dt.datetime.fromisoformat(ov["extend_until"])
        if ext > close_at:
            drain_at = drain_at + (ext - close_at)
            close_at = ext
    if ov.get("close_now_at"):
        cn = _dt.datetime.fromisoformat(ov["close_now_at"])
        close_at = min(close_at, cn)
        drain_at = min(drain_at, close_at)
    return open_at, drain_at, close_at


def local_date(cfg, now: _dt.datetime) -> _dt.date:
    return now.astimezone(ZoneInfo(cfg["execution_window"]["timezone"])).date()


def window_state(cfg, now: _dt.datetime, overrides: dict | None = None) -> dict:
    today = local_date(cfg, now)
    last_closed = None
    for delta in (0, 1, 2):
        key = (today - _dt.timedelta(days=delta)).isoformat()
        o, d, c = window_bounds(cfg, key, overrides)
        if o <= now < d:
            return {"state": "OPEN", "key": key, "open_at": o, "drain_at": d, "close_at": c}
        if d <= now < c:
            return {"state": "DRAINING", "key": key, "open_at": o, "drain_at": d, "close_at": c}
        if c <= now and last_closed is None:
            last_closed = {"state": "CLOSED", "key": key, "open_at": o, "drain_at": d, "close_at": c}
    return last_closed or {"state": "CLOSED", "key": None, "open_at": None, "drain_at": None, "close_at": None}


def next_open(cfg, now: _dt.datetime, overrides: dict | None = None) -> _dt.datetime:
    today = local_date(cfg, now)
    for delta in (0, 1, 2):
        o, _d, _c = window_bounds(cfg, (today + _dt.timedelta(days=delta)).isoformat(), overrides)
        if o > now:
            return o
    raise AssertionError("no future window")  # pragma: no cover
