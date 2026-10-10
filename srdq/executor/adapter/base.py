"""SR adapter interface shared by the real StarRocks adapter and the SQLite simulator."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator, Protocol


class AdapterError(Exception):
    def __init__(self, message: str, *, error_class: str = "QUERY_ERROR", code: str | None = None):
        super().__init__(message)
        self.error_class = error_class
        self.code = code


@dataclass
class ColumnMeta:
    name: str
    data_type: str
    column_type: str
    nullable: bool
    ordinal: int


@dataclass
class TableMeta:
    status: str  # PRESENT / ABSENT_CONFIRMED / ACCESS_DENIED / UNKNOWN / QUERY_FAILED
    model: str | None = None  # DUP_KEYS / AGG_KEYS / UNQ_KEYS / PRI_KEYS
    key_columns: list[str] = field(default_factory=list)
    columns: list[ColumnMeta] = field(default_factory=list)
    partition: dict | None = None  # {"kind": NONE|RANGE|LIST|UNSUPPORTED, "columns": [...], "partitions": [...]}
    message: str | None = None

    def to_json(self) -> dict:
        return {
            "status": self.status,
            "model": self.model,
            "key_columns": self.key_columns,
            "columns": [c.__dict__ for c in self.columns],
            "partition": self.partition,
            "message": self.message,
        }


class Session(Protocol):
    def query_ref(self) -> dict: ...
    def set_limits(self, query_timeout_seconds: int, mem_limit_bytes: int | None) -> None: ...
    def stream(self, sql: str, params: list, batch_rows: int) -> Iterator[list[tuple]]: ...
    def close(self) -> None: ...


class Adapter(Protocol):
    side: str

    def connect(self) -> Session: ...
    def cancel(self, query_ref: dict) -> None: ...
    def confirm_terminated(self, query_ref: dict) -> bool: ...
    def table_meta(self, db: str, table: str) -> TableMeta: ...
    def version(self) -> str: ...
    def scalar_row(self, sql: str, params: list) -> tuple: ...
    def signals(self) -> dict | None: ...


def classify_error(message: str, *, code: int | None, cancelled: bool, patterns: list[str]) -> str:
    if cancelled:
        return "CANCELLED"
    msg = (message or "").lower()
    if code in (2003, 2006, 2013, 2055) or "lost connection" in msg or "connection refused" in msg:
        return "NETWORK"
    if code in (1044, 1045, 1142, 1143, 1227) or "access denied" in msg:
        return "ACCESS_DENIED"
    if any(p.lower() in msg for p in patterns):
        return "STORAGE_SUSPECT"
    if "timeout" in msg or "timed out" in msg:
        return "TIMEOUT"
    return "QUERY_ERROR"
