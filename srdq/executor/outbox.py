"""Per-identity local outbox (SQLite): final results awaiting acknowledgement and a query journal."""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path

STATES = ("PENDING_ACK", "ACKED", "STALE_REJECTED", "QUARANTINED")

_DDL = """
CREATE TABLE IF NOT EXISTS result_outbox (
    attempt_id TEXT PRIMARY KEY,
    envelope TEXT NOT NULL,
    material_dir TEXT NULL,
    state TEXT NOT NULL,
    receipt TEXT NULL,
    last_error TEXT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS query_journal (
    attempt_id TEXT PRIMARY KEY,
    query_ref TEXT NOT NULL,
    started_at REAL NOT NULL,
    finished_at REAL NULL
);
"""


class Outbox:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None, timeout=30)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_DDL)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def size_bytes(self) -> int:
        total = 0
        for suffix in ("", "-wal", "-shm"):
            p = Path(str(self.path) + suffix)
            if p.exists():
                total += p.stat().st_size
        return total

    # ---- results -------------------------------------------------------------
    def put(self, attempt_id: str, envelope: dict, material_dir: str | None) -> None:
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO result_outbox (attempt_id, envelope, material_dir, state, created_at, updated_at) "
                "VALUES (?, ?, ?, 'PENDING_ACK', ?, ?) ON CONFLICT(attempt_id) DO NOTHING",
                (attempt_id, json.dumps(envelope, sort_keys=True), material_dir, now, now))

    def mark(self, attempt_id: str, state: str, receipt: dict | None = None, error: str | None = None) -> None:
        assert state in STATES
        with self._lock:
            self._conn.execute("UPDATE result_outbox SET state=?, receipt=?, last_error=?, updated_at=? "
                               "WHERE attempt_id=?",
                               (state, json.dumps(receipt) if receipt else None, error, time.time(), attempt_id))

    def pending(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute("SELECT attempt_id, envelope, material_dir FROM result_outbox "
                                      "WHERE state='PENDING_ACK' ORDER BY created_at").fetchall()
        return [{"attempt_id": a, "envelope": json.loads(e), "material_dir": m} for a, e, m in rows]

    def get(self, attempt_id: str) -> dict | None:
        with self._lock:
            r = self._conn.execute("SELECT state, receipt, last_error FROM result_outbox WHERE attempt_id=?",
                                   (attempt_id,)).fetchone()
        if r is None:
            return None
        return {"state": r[0], "receipt": json.loads(r[1]) if r[1] else None, "last_error": r[2]}

    # ---- query journal -------------------------------------------------------
    def journal_start(self, attempt_id: str, query_ref: dict) -> None:
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO query_journal (attempt_id, query_ref, started_at) "
                               "VALUES (?, ?, ?)", (attempt_id, json.dumps(query_ref), time.time()))

    def journal_finish(self, attempt_id: str) -> None:
        with self._lock:
            self._conn.execute("UPDATE query_journal SET finished_at=? WHERE attempt_id=?", (time.time(), attempt_id))

    def journal_ref(self, attempt_id: str) -> dict | None:
        with self._lock:
            r = self._conn.execute("SELECT query_ref FROM query_journal WHERE attempt_id=?", (attempt_id,)).fetchone()
        return json.loads(r[0]) if r else None
