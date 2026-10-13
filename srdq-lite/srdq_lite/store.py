"""File helpers: crash-tolerant jsonl, atomic writes, gzip result files, run lock."""
from __future__ import annotations

import csv
import fcntl
import gzip
import io
import json
import os
import threading
from pathlib import Path
from typing import Any, Iterable, Iterator


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str, separators=(",", ":"))


def read_jsonl(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    lines = p.read_bytes().split(b"\n")
    out = []
    for i, raw in enumerate(lines):
        if not raw.strip():
            continue
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError:
            if i == len(lines) - 1:
                break  # torn last line left by a crash
            raise ValueError(f"{p}: corrupt line {i + 1}")
    return out


def _repair_tail(path: Path) -> None:
    if not path.exists():
        return
    with open(path, "rb+") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        if size == 0:
            return
        f.seek(size - 1)
        if f.read(1) == b"\n":
            return
        pos = size
        while pos > 0:
            start = max(0, pos - 65536)
            f.seek(start)
            idx = f.read(pos - start).rfind(b"\n")
            if idx >= 0:
                f.truncate(start + idx + 1)
                return
            pos = start
        f.truncate(0)


class JsonlAppender:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        _repair_tail(self.path)
        self._f = open(self.path, "a", encoding="utf-8")
        self._lock = threading.Lock()

    def write(self, obj: Any) -> None:
        line = dumps(obj) + "\n"
        with self._lock:
            self._f.write(line)
            self._f.flush()

    def close(self) -> None:
        with self._lock:
            if not self._f.closed:
                self._f.flush()
                os.fsync(self._f.fileno())
                self._f.close()

    def __enter__(self) -> "JsonlAppender":
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def _atomic_write(path: Path, writer) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        writer(f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def write_json(path: str | Path, obj: Any) -> None:
    _atomic_write(Path(path), lambda f: json.dump(obj, f, ensure_ascii=False, indent=2, default=str))


def write_jsonl(path: str | Path, rows: Iterable[Any]) -> None:
    def w(f: io.TextIOBase) -> None:
        for r in rows:
            f.write(dumps(r) + "\n")
    _atomic_write(Path(path), w)


def write_csv(path: str | Path, header: list[str], rows: Iterable[dict]) -> None:
    def w(f: io.TextIOBase) -> None:
        f.write("\ufeff")  # BOM so Excel opens UTF-8 correctly
        cw = csv.DictWriter(f, fieldnames=header, extrasaction="ignore")
        cw.writeheader()
        for r in rows:
            cw.writerow({k: _csv_cell(r.get(k)) for k in header})
    _atomic_write(Path(path), w)


def _csv_cell(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return dumps(v)
    return "" if v is None else v


def read_json(path: str | Path) -> Any:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def iter_gz_rows(path: str | Path) -> Iterator[list]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


class RunLock:
    """Exclusive lock on an output directory so two processes never share it."""

    def __init__(self, out_dir: Path):
        self.path = out_dir / "run.lock"
        self._f = None

    def __enter__(self) -> "RunLock":
        self._f = open(self.path, "a+")
        try:
            fcntl.flock(self._f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._f.close()
            raise RuntimeError(f"{self.path.parent} is being used by another srdq_lite process")
        self._f.seek(0)
        self._f.truncate()
        self._f.write(str(os.getpid()))
        self._f.flush()
        return self

    def __exit__(self, *exc) -> None:
        if self._f:
            fcntl.flock(self._f, fcntl.LOCK_UN)
            self._f.close()
