"""StarRocks adapter over PyMySQL (MySQL protocol, streaming SSCursor, TLS with certificate verification)."""
from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path
from typing import Any, Iterator

from .base import AdapterError, ColumnMeta, TableMeta, classify_error

_RANGE_KEYS_RE = re.compile(r"keys:\s*\[([^\]]*)\]")
_LIST_VALUE_RE = re.compile(r"'((?:[^'\\]|\\.)*)'|(-?\d+(?:\.\d+)?)")
# Newer SR versions (e.g. 3.5) report long model names in information_schema.tables_config.
_MODEL_ALIASES = {"PRIMARY_KEYS": "PRI_KEYS", "UNIQUE_KEYS": "UNQ_KEYS", "AGGREGATE_KEYS": "AGG_KEYS",
                  "DUPLICATE_KEYS": "DUP_KEYS"}


def normalize_model(raw: Any) -> str:
    model = str(raw).strip().upper()
    return _MODEL_ALIASES.get(model, model)


class CredentialError(RuntimeError):
    pass


def load_credentials(path: str | Path, *, require_mode_0600: bool = True) -> dict:
    """Credentials live in a per-side file readable only by the side's OS user; never logged."""
    p = Path(path)
    st = p.stat()
    if require_mode_0600:
        if stat.S_IMODE(st.st_mode) & 0o077:
            raise CredentialError(f"credential file {p} must not be accessible by group/other (mode 0600)")
        if hasattr(os, "getuid") and st.st_uid != os.getuid():
            raise CredentialError(f"credential file {p} must be owned by the executor's OS user")
    data = json.loads(p.read_text(encoding="utf-8"))
    for k in ("host", "port", "user", "password"):
        if k not in data:
            raise CredentialError(f"credential file {p} lacks {k!r}")
    return data


def _sr_error(e: Exception, cancelled: bool, patterns: list[str]) -> AdapterError:
    code = None
    msg = str(e)
    if getattr(e, "args", None) and isinstance(e.args[0], int):
        code = e.args[0]
        msg = str(e.args[1]) if len(e.args) > 1 else msg
    cls = classify_error(msg, code=code, cancelled=cancelled, patterns=patterns)
    return AdapterError(msg[:2000], error_class=cls, code=None if code is None else str(code))


class StarRocksSession:
    def __init__(self, adapter: "StarRocksAdapter"):
        import pymysql
        import pymysql.cursors

        self.adapter = adapter
        self._pymysql = pymysql
        try:
            self.conn = adapter._connect_raw(cursorclass=pymysql.cursors.SSCursor)
            with self.conn.cursor() as cur:
                cur.execute("SELECT CONNECTION_ID()")
                self.connection_id = int(cur.fetchone()[0])
        except pymysql.MySQLError as e:
            raise _sr_error(e, False, adapter.patterns) from e
        self.cancelled = False

    def query_ref(self) -> dict:
        return {"host": self.adapter.creds["host"], "port": int(self.adapter.creds["port"]),
                "connection_id": self.connection_id}

    def set_limits(self, query_timeout_seconds: int, mem_limit_bytes: int | None) -> None:
        try:
            with self.conn.cursor() as cur:
                cur.execute(f"SET query_timeout = {int(query_timeout_seconds)}")
                if mem_limit_bytes:
                    cur.execute(f"SET query_mem_limit = {int(mem_limit_bytes)}")
        except self._pymysql.MySQLError as e:
            raise _sr_error(e, False, self.adapter.patterns) from e

    def stream(self, sql: str, params: list, batch_rows: int) -> Iterator[list[tuple]]:
        cur = self.conn.cursor()
        try:
            cur.execute(sql, params or None)
            while True:
                batch = cur.fetchmany(batch_rows)
                if not batch:
                    break
                yield list(batch)
        except self._pymysql.MySQLError as e:
            raise _sr_error(e, self.cancelled, self.adapter.patterns) from e
        finally:
            try:
                cur.close()
            except Exception:
                pass

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


class StarRocksAdapter:
    """One logical side. KILL QUERY goes to the FE recorded in query_ref; confirmation uses SHOW PROCESSLIST
    on the same FE (behind an SLB this is only reliable when the SLB pins sessions to one FE)."""

    def __init__(self, side: str, credentials_file: str | Path, *, storage_patterns: list[str] | None = None,
                 connect_timeout: int = 10, read_timeout: int = 60, require_mode_0600: bool = True):
        self.side = side
        self.creds = load_credentials(credentials_file, require_mode_0600=require_mode_0600)
        self.patterns = storage_patterns or []
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout

    def _ssl(self) -> dict | None:
        if not self.creds.get("tls", True):
            return None
        ssl: dict[str, Any] = {"check_hostname": bool(self.creds.get("tls_verify_hostname", True))}
        if self.creds.get("ssl_ca"):
            ssl["ca"] = self.creds["ssl_ca"]
        return ssl

    def _connect_raw(self, cursorclass=None, host: str | None = None, port: int | None = None):
        import pymysql

        kw: dict[str, Any] = dict(host=host or self.creds["host"], port=int(port or self.creds["port"]),
                                  user=self.creds["user"], password=self.creds["password"],
                                  connect_timeout=self.connect_timeout, read_timeout=self.read_timeout,
                                  charset="utf8mb4", autocommit=True)
        ssl = self._ssl()
        if ssl is not None:
            kw["ssl"] = ssl
        if cursorclass is not None:
            kw["cursorclass"] = cursorclass
        return pymysql.connect(**kw)

    def _admin_query(self, sql: str, params: list | None = None, host=None, port=None) -> list[tuple]:
        import pymysql

        try:
            conn = self._connect_raw(host=host, port=port)
        except pymysql.MySQLError as e:
            raise _sr_error(e, False, self.patterns) from e
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params or None)
                return list(cur.fetchall()) if cur.description else []
        except pymysql.MySQLError as e:
            raise _sr_error(e, False, self.patterns) from e
        finally:
            conn.close()

    def connect(self) -> StarRocksSession:
        return StarRocksSession(self)

    def cancel(self, query_ref: dict) -> None:
        cid = int(query_ref["connection_id"])
        self._admin_query(f"KILL QUERY {cid}", host=query_ref.get("host"), port=query_ref.get("port"))

    def confirm_terminated(self, query_ref: dict) -> bool:
        cid = int(query_ref["connection_id"])
        try:
            rows = self._admin_query("SHOW PROCESSLIST", host=query_ref.get("host"), port=query_ref.get("port"))
        except AdapterError:
            return False
        for r in rows:
            if int(r[0]) == cid:
                command = str(r[4] if len(r) > 4 else "").lower()
                return command in ("sleep", "")
        return True

    def table_meta(self, db: str, table: str) -> TableMeta:
        try:
            exists = self._admin_query("SELECT TABLE_TYPE FROM information_schema.tables "
                                       "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s", [db, table])
            if not exists:
                return TableMeta(status="ABSENT_CONFIRMED")
            cols = [ColumnMeta(name=r[0], data_type=str(r[1]).lower(), column_type=str(r[2]).lower(),
                               nullable=str(r[3]).upper() == "YES", ordinal=int(r[4]))
                    for r in self._admin_query(
                        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, ORDINAL_POSITION "
                        "FROM information_schema.columns WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s "
                        "ORDER BY ORDINAL_POSITION", [db, table])]
            cfg = self._admin_query("SELECT TABLE_MODEL, PRIMARY_KEY, PARTITION_KEY FROM "
                                    "information_schema.tables_config WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                                    [db, table])
            model, keys = None, []
            if cfg:
                model = normalize_model(cfg[0][0])
                keys = [k.strip().strip("`") for k in str(cfg[0][1] or "").split(",") if k.strip()]
            partition = self._partitions(db, table)
        except AdapterError as e:
            status = "ACCESS_DENIED" if e.error_class == "ACCESS_DENIED" else "QUERY_FAILED"
            return TableMeta(status=status, message=str(e)[:500])
        return TableMeta(status="PRESENT", model=model, key_columns=keys, columns=cols, partition=partition)

    def _partitions(self, db: str, table: str) -> dict:
        import pymysql

        try:
            conn = self._connect_raw(cursorclass=pymysql.cursors.DictCursor)
        except pymysql.MySQLError as e:
            raise _sr_error(e, False, self.patterns) from e
        try:
            with conn.cursor() as cur:
                cur.execute(f"SHOW PARTITIONS FROM `{db}`.`{table}`")
                rows = cur.fetchall()
        except pymysql.MySQLError as e:
            raise _sr_error(e, False, self.patterns) from e
        finally:
            conn.close()
        return parse_show_partitions(rows)

    def version(self) -> str:
        rows = self._admin_query("SELECT current_version()")
        return str(rows[0][0]) if rows else "unknown"

    def scalar_row(self, sql: str, params: list) -> tuple:
        rows = self._admin_query(sql, params)
        if len(rows) != 1:
            raise AdapterError(f"expected exactly one row, got {len(rows)}", error_class="CONTRACT_VIOLATION")
        return tuple(rows[0])

    def signals(self) -> dict | None:
        try:
            rows = self._admin_query("SHOW PROCESSLIST")
        except AdapterError:
            return None
        running = sum(1 for r in rows if len(r) > 4 and str(r[4]).lower() == "query")
        return {"running_queries": running, "pending_queries": None, "visible_scope": "OWN_ACCOUNT"}


def parse_show_partitions(rows: list[dict]) -> dict:
    """Normalize SHOW PARTITIONS output. Unrecognized layouts become UNSUPPORTED (manifest falls back to ALL)."""
    if not rows:
        return {"kind": "NONE", "columns": [], "partitions": []}
    first = rows[0]
    pkey = str(first.get("PartitionKey") or "").strip()
    if not pkey or len(rows) == 1 and str(first.get("PartitionName")) == first.get("TableName"):
        return {"kind": "NONE", "columns": [], "partitions": []}
    cols = [c.strip().strip("`") for c in pkey.split(",") if c.strip()]
    if len(cols) != 1:
        return {"kind": "UNSUPPORTED", "columns": cols, "partitions": [], "reason": "multi-column partition key"}
    parts = []
    if first.get("Range") and str(first.get("Range")).strip() not in ("", "-"):
        for r in rows:
            keys = _RANGE_KEYS_RE.findall(str(r.get("Range")))
            if len(keys) != 2:
                return {"kind": "UNSUPPORTED", "columns": cols, "partitions": [], "reason": "unparsed range"}
            lo, hi = (k.strip() for k in keys)
            parts.append({"name": r.get("PartitionName"), "lower": _bound(lo), "upper": _bound(hi)})
        return {"kind": "RANGE", "columns": cols, "partitions": parts}
    lst = first.get("List") or first.get("ListValues")
    if lst:
        for r in rows:
            raw = str(r.get("List") or r.get("ListValues") or "")
            vals = [a if a else b for a, b in _LIST_VALUE_RE.findall(raw)]
            parts.append({"name": r.get("PartitionName"), "values": vals})
        return {"kind": "LIST", "columns": cols, "partitions": parts}
    return {"kind": "UNSUPPORTED", "columns": cols, "partitions": [], "reason": "unknown partition layout"}


def _bound(text: str) -> str | None:
    t = text.strip()
    if t.upper() in ("MINVALUE", "MAXVALUE", "-9223372036854775808", "0000-01-01", "0000-01-01 00:00:00",
                     "9999-12-31 23:59:59", "MAX_VALUE", "MIN_VALUE"):
        return None
    return t
