"""SQLite-backed StarRocks simulator for tests and local demos. Not a production adapter.

Each simulated cluster is a directory: `meta.sqlite` holds table models, columns, partitions,
settings and injected faults; every SR database is a separate SQLite file ATTACHed under its
own name so that compiled `` `db`.`tbl` `` references work unchanged.  SQL functions used by the
compiler are overridden with StarRocks semantics (byte length, hex of numbers, sha2, conv...).
"""
from __future__ import annotations

import hashlib
import itertools
import json
import sqlite3
import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterator

from ...common.dsl import check_ident
from .base import AdapterError, ColumnMeta, TableMeta

_IDS = itertools.count(1)
_RUNNING: dict[int, "_Running"] = {}
_LOCK = threading.Lock()

_STORAGE = {
    "tinyint": "INTEGER", "smallint": "INTEGER", "int": "INTEGER", "bigint": "INTEGER", "largeint": "INTEGER",
    "boolean": "INTEGER", "float": "REAL", "double": "REAL",
}


class _Running:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.running = False
        self.cancelled = False


def _hex(x: Any) -> str | None:
    if x is None:
        return None
    if isinstance(x, bool):
        x = int(x)
    if isinstance(x, int):
        return format(x if x >= 0 else x & 0xFFFFFFFFFFFFFFFF, "X")
    if isinstance(x, float):
        return format(int(x), "X")
    if isinstance(x, bytes):
        return x.hex().upper()
    return str(x).encode("utf-8").hex().upper()


def _length(x: Any) -> int | None:
    if x is None:
        return None
    if isinstance(x, bytes):
        return len(x)
    return len(str(x).encode("utf-8"))


def _lpad(s: Any, n: int, pad: str) -> str | None:
    if s is None:
        return None
    s = str(s)
    if len(s) >= n:
        return s[:n]
    fill = (pad * n)[: n - len(s)]
    return fill + s


def _concat(*args: Any) -> str | None:
    if any(a is None for a in args):
        return None
    return "".join(a.decode() if isinstance(a, bytes) else str(a) for a in args)


def _sha2(s: Any, bits: int) -> str | None:
    if s is None:
        return None
    if bits != 256:
        raise ValueError("sim sha2 supports 256 only")
    data = s if isinstance(s, bytes) else str(s).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _conv(s: Any, frm: int, to: int) -> str | None:
    if s is None:
        return None
    if to != 10:
        raise ValueError("sim conv supports base 10 output only")
    return str(int(str(s), int(frm)))


def _mod(a: Any, b: Any) -> int | None:
    if a is None or b is None:
        return None
    a, b = int(a), int(b)
    r = abs(a) % abs(b)
    return -r if a < 0 else r


class _Sum:
    def __init__(self):
        self.acc: Any = None

    def step(self, v: Any) -> None:
        if v is None:
            return
        if isinstance(v, str):
            v = Decimal(v)
        self.acc = v if self.acc is None else self.acc + v

    def finalize(self) -> Any:
        if self.acc is None:
            return None
        if isinstance(self.acc, Decimal):
            return format(self.acc, "f")
        if isinstance(self.acc, int) and not -(2**63) <= self.acc < 2**63:
            return str(self.acc)
        return self.acc


def _install_functions(conn: sqlite3.Connection) -> None:
    conn.create_function("hex", 1, _hex, deterministic=True)
    conn.create_function("length", 1, _length, deterministic=True)
    conn.create_function("lpad", 3, _lpad, deterministic=True)
    conn.create_function("concat", -1, _concat, deterministic=True)
    conn.create_function("sha2", 2, _sha2, deterministic=True)
    conn.create_function("conv", 3, _conv, deterministic=True)
    conn.create_function("mod", 2, _mod, deterministic=True)
    conn.create_aggregate("sum", 1, _Sum)


_META_DDL = """
CREATE TABLE IF NOT EXISTS sim_databases (name TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS sim_tables (
    db TEXT, tbl TEXT, model TEXT, keys TEXT, partition TEXT, status TEXT DEFAULT 'PRESENT',
    PRIMARY KEY (db, tbl));
CREATE TABLE IF NOT EXISTS sim_columns (
    db TEXT, tbl TEXT, name TEXT, data_type TEXT, column_type TEXT, nullable INTEGER, ordinal INTEGER,
    PRIMARY KEY (db, tbl, name));
CREATE TABLE IF NOT EXISTS sim_settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS sim_faults (db TEXT, tbl TEXT, message TEXT, code INTEGER, remaining INTEGER);
"""


class SimCluster:
    """Builder/inspector for one simulated SR cluster directory."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        with self._meta() as m:
            m.executescript(_META_DDL)

    def _meta(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path / "meta.sqlite", timeout=30, isolation_level=None)
        return _Closing(conn)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path / "meta.sqlite", timeout=30, check_same_thread=False, isolation_level=None)
        for (name,) in conn.execute("SELECT name FROM sim_databases").fetchall():
            conn.execute("ATTACH DATABASE ? AS " + _q(name), (str(self.path / f"{name}.sqlite"),))
        _install_functions(conn)
        return conn

    # ---- building ----------------------------------------------------------
    def create_table(self, db: str, tbl: str, columns: list[tuple], *, model: str = "DUP_KEYS",
                     keys: list[str] | None = None, partition: dict | None = None) -> None:
        """columns: (name, sr_type[, nullable=True])."""
        check_ident(db), check_ident(tbl)
        with self._meta() as m:
            m.execute("INSERT OR IGNORE INTO sim_databases VALUES (?)", (db,))
            m.execute("DELETE FROM sim_columns WHERE db=? AND tbl=?", (db, tbl))
            m.execute("INSERT OR REPLACE INTO sim_tables (db, tbl, model, keys, partition, status) "
                      "VALUES (?,?,?,?,?, 'PRESENT')",
                      (db, tbl, model, json.dumps(keys or []), json.dumps(partition or {"kind": "NONE"})))
            for i, col in enumerate(columns):
                name, sr_type = col[0], col[1]
                nullable = col[2] if len(col) > 2 else True
                base = sr_type.lower().split("(")[0]
                m.execute("INSERT INTO sim_columns VALUES (?,?,?,?,?,?,?)",
                          (db, tbl, name, base, sr_type.lower(), 1 if nullable else 0, i + 1))
        conn = sqlite3.connect(self.path / f"{db}.sqlite", isolation_level=None)
        try:
            cols = ", ".join(f"{_q(c[0])} {_STORAGE.get(c[1].lower().split('(')[0], 'TEXT')}" for c in columns)
            conn.execute(f"DROP TABLE IF EXISTS {_q(tbl)}")
            conn.execute(f"CREATE TABLE {_q(tbl)} ({cols})")
        finally:
            conn.close()

    def drop_table(self, db: str, tbl: str) -> None:
        with self._meta() as m:
            m.execute("DELETE FROM sim_tables WHERE db=? AND tbl=?", (db, tbl))
            m.execute("DELETE FROM sim_columns WHERE db=? AND tbl=?", (db, tbl))
        conn = sqlite3.connect(self.path / f"{db}.sqlite", isolation_level=None)
        try:
            conn.execute(f"DROP TABLE IF EXISTS {_q(tbl)}")
        finally:
            conn.close()

    def set_partition(self, db: str, tbl: str, partition: dict) -> None:
        with self._meta() as m:
            m.execute("UPDATE sim_tables SET partition=? WHERE db=? AND tbl=?", (json.dumps(partition), db, tbl))

    def set_table_status(self, db: str, tbl: str, status: str) -> None:
        with self._meta() as m:
            m.execute("UPDATE sim_tables SET status=? WHERE db=? AND tbl=?", (status, db, tbl))

    def insert(self, db: str, tbl: str, rows: list[dict]) -> None:
        if not rows:
            return
        conn = sqlite3.connect(self.path / f"{db}.sqlite", isolation_level=None)
        try:
            cols = list(rows[0])
            sql = (f"INSERT INTO {_q(tbl)} ({', '.join(_q(c) for c in cols)}) "
                   f"VALUES ({', '.join('?' * len(cols))})")
            conn.execute("BEGIN")
            conn.executemany(sql, [tuple(_store(r[c]) for c in cols) for r in rows])
            conn.execute("COMMIT")
        finally:
            conn.close()

    def execute(self, db: str, sql: str, params: tuple = ()) -> None:
        conn = sqlite3.connect(self.path / f"{db}.sqlite", isolation_level=None)
        try:
            conn.execute(sql, params)
        finally:
            conn.close()

    def set_setting(self, key: str, value: Any) -> None:
        with self._meta() as m:
            if value is None:
                m.execute("DELETE FROM sim_settings WHERE key=?", (key,))
            else:
                m.execute("INSERT OR REPLACE INTO sim_settings VALUES (?, ?)", (key, str(value)))

    def setting(self, key: str, default: Any = None) -> Any:
        with self._meta() as m:
            r = m.execute("SELECT value FROM sim_settings WHERE key=?", (key,)).fetchone()
        return default if r is None else r[0]

    def add_fault(self, db: str, tbl: str, message: str, code: int = 1064, remaining: int = -1) -> None:
        with self._meta() as m:
            m.execute("INSERT INTO sim_faults VALUES (?,?,?,?,?)", (db, tbl, message, code, remaining))

    def clear_faults(self) -> None:
        with self._meta() as m:
            m.execute("DELETE FROM sim_faults")


class _Closing:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def __enter__(self) -> sqlite3.Connection:
        return self.conn

    def __exit__(self, *exc) -> None:
        self.conn.close()


def _q(name: str) -> str:
    check_ident(name)
    return "`" + name + "`"


def _store(v: Any) -> Any:
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, bool):
        return int(v)
    if hasattr(v, "isoformat"):
        return v.isoformat(sep=" ") if hasattr(v, "hour") else v.isoformat()
    return v


class SimSession:
    def __init__(self, adapter: "SimAdapter"):
        self.adapter = adapter
        self.id = next(_IDS)
        self.conn = adapter.cluster.connect()
        self.state = _Running(self.conn)
        self.timeout = None
        slow = float(adapter.cluster.setting("slow_ms_per_kop", "0") or 0)

        def progress() -> int:
            if self.state.cancelled:
                return 1
            if self.timeout is not None and time.monotonic() > self.timeout:
                return 1
            if slow:
                time.sleep(slow / 1000.0)
            return 0

        self.conn.set_progress_handler(progress, 1000)
        with _LOCK:
            _RUNNING[self.id] = self.state

    def query_ref(self) -> dict:
        return {"host": "sim", "port": 0, "connection_id": self.id, "cluster": str(self.adapter.cluster.path)}

    def set_limits(self, query_timeout_seconds: int, mem_limit_bytes: int | None) -> None:
        self._timeout_seconds = query_timeout_seconds

    def stream(self, sql: str, params: list, batch_rows: int) -> Iterator[list[tuple]]:
        self.adapter._check_faults(sql)
        self.state.running = True
        self.timeout = time.monotonic() + getattr(self, "_timeout_seconds", 3600)
        try:
            cur = self.conn.execute(sql.replace("%s", "?"), [_store(p) for p in params])
            while True:
                if self.state.cancelled:
                    raise AdapterError("query cancelled", error_class="CANCELLED")
                batch = cur.fetchmany(batch_rows)
                if not batch:
                    break
                yield batch
                delay = float(self.adapter.cluster.setting("slow_ms_per_batch", "0") or 0)
                if delay:
                    end = time.monotonic() + delay / 1000.0
                    while time.monotonic() < end and not self.state.cancelled:
                        time.sleep(0.01)
        except sqlite3.OperationalError as e:
            if self.state.cancelled:
                raise AdapterError("query cancelled", error_class="CANCELLED") from e
            if self.timeout is not None and time.monotonic() > self.timeout:
                raise AdapterError("query timeout exceeded", error_class="TIMEOUT") from e
            raise AdapterError(str(e), error_class="QUERY_ERROR") from e
        finally:
            self.state.running = False

    def close(self) -> None:
        self.state.running = False
        try:
            self.conn.close()
        except Exception:
            pass
        with _LOCK:
            _RUNNING.pop(self.id, None)


class SimAdapter:
    """Adapter facade over a SimCluster, mirroring the StarRocks adapter API."""

    def __init__(self, side: str, path: str | Path):
        self.side = side
        self.cluster = SimCluster(path)

    def connect(self) -> SimSession:
        if self.cluster.setting("unreachable") == "1":
            raise AdapterError("Can't connect to sim server (connection refused)", error_class="NETWORK", code="2003")
        return SimSession(self)

    def _check_faults(self, sql: str) -> None:
        with self.cluster._meta() as m:
            for rowid, db, tbl, message, code, remaining in m.execute(
                    "SELECT rowid, db, tbl, message, code, remaining FROM sim_faults").fetchall():
                if f"`{db}`.`{tbl}`" in sql and remaining != 0:
                    if remaining > 0:
                        m.execute("UPDATE sim_faults SET remaining = remaining - 1 WHERE rowid=?", (rowid,))
                    raise AdapterError(message, error_class="QUERY_ERROR", code=str(code))

    def cancel(self, query_ref: dict) -> None:
        with _LOCK:
            st = _RUNNING.get(int(query_ref.get("connection_id", -1)))
        if st is not None:
            st.cancelled = True
            try:
                st.conn.interrupt()
            except Exception:
                pass

    def confirm_terminated(self, query_ref: dict) -> bool:
        if self.cluster.setting("cancel_unconfirmable") == "1":
            return False
        with _LOCK:
            st = _RUNNING.get(int(query_ref.get("connection_id", -1)))
        return st is None or not st.running

    def table_meta(self, db: str, table: str) -> TableMeta:
        if self.cluster.setting("unreachable") == "1":
            return TableMeta(status="QUERY_FAILED", message="connection refused")
        with self.cluster._meta() as m:
            r = m.execute("SELECT model, keys, partition, status FROM sim_tables WHERE db=? AND tbl=?",
                          (db, table)).fetchone()
            if r is None:
                return TableMeta(status="ABSENT_CONFIRMED")
            if r[3] != "PRESENT":
                return TableMeta(status=r[3], message=f"simulated {r[3]}")
            cols = [ColumnMeta(name=c[0], data_type=c[1], column_type=c[2], nullable=bool(c[3]), ordinal=c[4])
                    for c in m.execute("SELECT name, data_type, column_type, nullable, ordinal FROM sim_columns "
                                       "WHERE db=? AND tbl=? ORDER BY ordinal", (db, table)).fetchall()]
        part = json.loads(r[2])
        return TableMeta(status="PRESENT", model=r[0], key_columns=json.loads(r[1]), columns=cols,
                         partition=_normalize_partition(part))

    def version(self) -> str:
        return self.cluster.setting("version", "3.3.0-sim")

    def scalar_row(self, sql: str, params: list) -> tuple:
        s = self.connect()
        try:
            rows = [r for b in s.stream(sql, params, 10) for r in b]
        finally:
            s.close()
        if len(rows) != 1:
            raise AdapterError(f"expected exactly one row, got {len(rows)}", error_class="CONTRACT_VIOLATION")
        return rows[0]

    def signals(self) -> dict | None:
        raw = self.cluster.setting("signals")
        return json.loads(raw) if raw else None


def _normalize_partition(part: dict) -> dict:
    kind = part.get("kind", "NONE")
    if kind == "NONE":
        return {"kind": "NONE", "columns": [], "partitions": []}
    col = part.get("column") or (part.get("columns") or [None])[0]
    return {"kind": kind, "columns": [col], "partitions": part.get("partitions", [])}
