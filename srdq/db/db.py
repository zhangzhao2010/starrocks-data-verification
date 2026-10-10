"""PostgreSQL access: small connection pool, explicit transactions, numbered migrations."""
from __future__ import annotations

import hashlib
import json
import queue
import re
import threading
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

MIGRATIONS_DIR = Path(__file__).parent / "migrations"
_MIG_RE = re.compile(r"(\d{4})_[a-z0-9_]+\.sql")


class MigrationError(RuntimeError):
    pass


def new_id() -> uuid.UUID:
    return uuid.uuid4()


def jb(obj: Any) -> Jsonb:
    return Jsonb(obj)


class Database:
    def __init__(self, dsn: str, max_connections: int = 8):
        self.dsn = dsn
        self._pool: queue.LifoQueue[psycopg.Connection] = queue.LifoQueue()
        self._sem = threading.BoundedSemaphore(max_connections)

    def _connect(self) -> psycopg.Connection:
        conn = psycopg.connect(self.dsn, row_factory=dict_row, autocommit=False)
        with conn.cursor() as cur:
            cur.execute("SET search_path = srdq, public")
            cur.execute("SET TIME ZONE 'UTC'")
        conn.commit()
        return conn

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        self._sem.acquire()
        conn = None
        try:
            try:
                conn = self._pool.get_nowait()
                if conn.closed or conn.broken:
                    conn = self._connect()
            except queue.Empty:
                conn = self._connect()
            yield conn
        finally:
            if conn is not None:
                if conn.closed or conn.broken:
                    pass
                else:
                    try:
                        conn.rollback()
                        self._pool.put(conn)
                    except Exception:
                        conn.close()
            self._sem.release()

    @contextmanager
    def tx(self) -> Iterator[psycopg.Cursor]:
        """One short transaction. Commits on success, rolls back on any exception."""
        with self.connection() as conn:
            try:
                with conn.cursor() as cur:
                    yield cur
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def query(self, sql: str, params: Any = None) -> list[dict]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall() if cur.description else []
            conn.rollback()
            return rows

    def one(self, sql: str, params: Any = None) -> dict | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: Any = None) -> int:
        with self.tx() as cur:
            cur.execute(sql, params)
            return cur.rowcount

    def close(self) -> None:
        while True:
            try:
                self._pool.get_nowait().close()
            except queue.Empty:
                break


def _migrations() -> list[tuple[int, str, str]]:
    out = []
    for p in sorted(MIGRATIONS_DIR.glob("*.sql")):
        m = _MIG_RE.fullmatch(p.name)
        if not m:
            raise MigrationError(f"bad migration file name {p.name}")
        out.append((int(m.group(1)), p.name, p.read_text(encoding="utf-8")))
    versions = [v for v, _, _ in out]
    if versions != list(range(1, len(versions) + 1)):
        raise MigrationError("migration versions must be contiguous from 0001")
    return out


def migrate(dsn: str) -> list[str]:
    """Apply pending migrations; refuses if the database is ahead of this release or a file changed."""
    applied_now = []
    with psycopg.connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(727274)")
            cur.execute("CREATE SCHEMA IF NOT EXISTS srdq_meta")
            cur.execute("""CREATE TABLE IF NOT EXISTS srdq_meta.migration (
                version int PRIMARY KEY, name text NOT NULL, digest text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT now())""")
            cur.execute("SELECT version, name, digest FROM srdq_meta.migration ORDER BY version")
            done = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
            known = _migrations()
            if done and max(done) > len(known):
                raise MigrationError(f"database schema version {max(done)} is newer than this release; "
                                     "refusing to downgrade")
            for version, name, sql in known:
                digest = hashlib.sha256(sql.encode()).hexdigest()
                if version in done:
                    if done[version][1] != digest:
                        raise MigrationError(f"applied migration {name} differs from the shipped file")
                    continue
                cur.execute(sql)
                cur.execute("SET search_path = public")
                cur.execute("INSERT INTO srdq_meta.migration (version, name, digest) VALUES (%s, %s, %s)",
                            (version, name, digest))
                applied_now.append(name)
        conn.commit()
    return applied_now


def schema_version(db: Database) -> int:
    r = db.one("SELECT max(version) AS v FROM srdq_meta.migration")
    return int(r["v"] or 0)


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)
