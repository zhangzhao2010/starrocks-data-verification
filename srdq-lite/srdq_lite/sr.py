"""StarRocks connections over PyMySQL."""
from __future__ import annotations

import threading
from typing import Any, Iterator

import pymysql
import pymysql.cursors

from .config import Cluster, RunSettings

# Analysis/permission errors repeat deterministically, so retrying them only wastes time.
NON_RETRYABLE_CODES = {1044, 1045, 1054, 1064, 1142, 1146, 1227}


class SRError(Exception):
    def __init__(self, message: str, code: int | None = None, retryable: bool = True):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


def _wrap(e: Exception) -> SRError:
    code = e.args[0] if e.args and isinstance(e.args[0], int) else None
    msg = str(e.args[1]) if code is not None and len(e.args) > 1 else str(e)
    prefix = f"[{code}] " if code is not None else ""
    return SRError((prefix + msg)[:2000], code, code not in NON_RETRYABLE_CODES)


def qi(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def qtable(db: str, table: str) -> str:
    return f"{qi(db)}.{qi(table)}"


class Connection:
    def __init__(self, cluster: Cluster, run: RunSettings, session: dict[str, Any]):
        kw: dict[str, Any] = dict(host=cluster.host, port=cluster.port, user=cluster.user, password=cluster.password,
                                  connect_timeout=run.connect_timeout_s, read_timeout=run.query_timeout_s + 60,
                                  write_timeout=run.query_timeout_s + 60, charset="utf8mb4", autocommit=True)
        if cluster.ssl:
            kw["ssl"] = {"ca": cluster.ssl_ca} if cluster.ssl_ca else {"check_hostname": False}
        try:
            self.conn = pymysql.connect(**kw)
            self.connection_id = self.conn.thread_id()
            with self.conn.cursor() as cur:
                cur.execute(f"SET query_timeout = {int(run.query_timeout_s)}")
                for k, v in session.items():
                    cur.execute(f"SET {k} = {self._literal(v)}")
        except (pymysql.MySQLError, OSError) as e:
            raise _wrap(e) from e

    def _literal(self, v: Any) -> str:
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return self.conn.escape(str(v))

    def query(self, sql: str, params: list | None = None) -> list[tuple]:
        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, params)
                return list(cur.fetchall()) if cur.description else []
        except (pymysql.MySQLError, OSError) as e:
            raise _wrap(e) from e

    def query_dicts(self, sql: str, params: list | None = None) -> list[dict]:
        try:
            with self.conn.cursor(pymysql.cursors.DictCursor) as cur:
                cur.execute(sql, params)
                return list(cur.fetchall()) if cur.description else []
        except (pymysql.MySQLError, OSError) as e:
            raise _wrap(e) from e

    def stream(self, sql: str, batch: int = 5000) -> Iterator[tuple]:
        try:
            with self.conn.cursor(pymysql.cursors.SSCursor) as cur:
                cur.execute(sql)
                while True:
                    rows = cur.fetchmany(batch)
                    if not rows:
                        return
                    yield from rows
        except (pymysql.MySQLError, OSError) as e:
            raise _wrap(e) from e

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass


class ConnPool:
    """One lazily created connection per worker thread."""

    def __init__(self, cluster: Cluster, run: RunSettings, session: dict[str, Any]):
        self.cluster, self.run, self.session = cluster, run, session
        self._local = threading.local()
        self._lock = threading.Lock()
        self._all: set[Connection] = set()

    def get(self) -> Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = Connection(self.cluster, self.run, self.session)
            self._local.conn = c
            with self._lock:
                self._all.add(c)
        return c

    def discard(self) -> None:
        c = getattr(self._local, "conn", None)
        self._local.conn = None
        if c is not None:
            with self._lock:
                self._all.discard(c)
            c.close()

    def kill(self, connection_id: int) -> None:
        try:
            c = Connection(self.cluster, self.run, {})
            try:
                c.query(f"KILL QUERY {int(connection_id)}")
            finally:
                c.close()
        except SRError:
            pass

    def close_all(self) -> None:
        with self._lock:
            conns, self._all = list(self._all), set()
        for c in conns:
            c.close()
