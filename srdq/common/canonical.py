"""SRDQ-C1 canonical encoding, SRDQ-SAMPLE-1 sampling, SRDQ-D1 digest, deterministic JSON."""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import re
import struct
from decimal import Context, Decimal, InvalidOperation
from typing import Any, Iterable

KEY_MAGIC = b"SRDQ-KEY-1\0"
ROW_MAGIC = b"SRDQ-ROW-1\0"
SHARD_MAGIC = b"SRDQ-SHARD-1\0"
SAMPLE_PREFIX = b"SRDQ-SAMPLE-1|"
EMPTY_GROUP_KEY = KEY_MAGIC + struct.pack(">I", 0)

TAG_BOOLEAN = 1
TAG_INTEGER = 2
TAG_DECIMAL = 3
TAG_STRING = 4
TAG_DATE = 5
TAG_DATETIME = 6
TAG_BINARY = 7
TAG_FLOAT32 = 8
TAG_FLOAT64 = 9

LOGICAL_TAGS = {
    "BOOLEAN": TAG_BOOLEAN,
    "INTEGER": TAG_INTEGER,
    "DECIMAL": TAG_DECIMAL,
    "STRING": TAG_STRING,
    "DATE": TAG_DATE,
    "DATETIME": TAG_DATETIME,
    "BINARY": TAG_BINARY,
    "FLOAT32": TAG_FLOAT32,
    "FLOAT64": TAG_FLOAT64,
}

SEED_RE = re.compile(r"[0-9a-f]{32}")
_INT_RE = re.compile(r"-?(0|[1-9][0-9]*)")
_DEC_RE = re.compile(r"[+-]?([0-9]+)(\.[0-9]*)?|[+-]?\.[0-9]+")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_EXACT = Context(prec=400, Emax=999999, Emin=-999999)


class CanonicalError(ValueError):
    """Value cannot be encoded losslessly under the frozen contract."""


def sha256(data: bytes) -> bytes:
    return hashlib.sha256(data).digest()


def val(tag: int, payload: bytes | None) -> bytes:
    if payload is None:
        return struct.pack(">HBQ", tag, 0, 0)
    return struct.pack(">HBQ", tag, 1, len(payload)) + payload


def decode_vals(data: bytes, offset: int, count: int) -> tuple[list[tuple[int, bytes | None]], int]:
    out = []
    for _ in range(count):
        if offset + 11 > len(data):
            raise CanonicalError("truncated VAL")
        tag, present, length = struct.unpack_from(">HBQ", data, offset)
        offset += 11
        if present not in (0, 1) or (present == 0 and length != 0):
            raise CanonicalError("invalid VAL header")
        if offset + length > len(data):
            raise CanonicalError("truncated VAL payload")
        out.append((tag, data[offset:offset + length] if present else None))
        offset += length
    return out, offset


def key_bytes(vals: Iterable[bytes]) -> bytes:
    vals = list(vals)
    return KEY_MAGIC + struct.pack(">I", len(vals)) + b"".join(vals)


def row_bytes(vals: Iterable[bytes]) -> bytes:
    vals = list(vals)
    return ROW_MAGIC + struct.pack(">I", len(vals)) + b"".join(vals)


def decode_key(data: bytes) -> list[tuple[int, bytes | None]]:
    if not data.startswith(KEY_MAGIC) or len(data) < len(KEY_MAGIC) + 4:
        raise CanonicalError("not a C1 KEY")
    (n,) = struct.unpack_from(">I", data, len(KEY_MAGIC))
    vals, end = decode_vals(data, len(KEY_MAGIC) + 4, n)
    if end != len(data):
        raise CanonicalError("trailing bytes in KEY")
    return vals


def canonical_int_text(value: Any) -> str:
    if isinstance(value, bool):
        raise CanonicalError("boolean is not an integer")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, Decimal):
        if value != value.to_integral_value():
            raise CanonicalError("non-integral value for INTEGER")
        return str(int(value))
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("ascii")
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("+"):
            text = text[1:]
        neg = text.startswith("-")
        digits = text[1:] if neg else text
        if not digits.isdigit():
            raise CanonicalError(f"invalid integer text {value!r}")
        n = int(digits)
        return str(-n if neg else n)
    raise CanonicalError(f"unsupported integer value type {type(value).__name__}")


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise CanonicalError("boolean is not a decimal")
    if isinstance(value, Decimal):
        d = value
    elif isinstance(value, int):
        d = Decimal(value)
    elif isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("ascii")
        d = _to_decimal(value)
    elif isinstance(value, str):
        text = value.strip()
        if not _DEC_RE.fullmatch(text):
            raise CanonicalError(f"invalid decimal text {value!r}")
        try:
            d = Decimal(text)
        except InvalidOperation as e:  # pragma: no cover - regex guards
            raise CanonicalError(str(e)) from e
    else:
        raise CanonicalError(f"unsupported decimal value type {type(value).__name__} (float is never exact)")
    if not d.is_finite():
        raise CanonicalError("non-finite decimal")
    return d


def decimal_scaled_text(value: Any, scale: int) -> str:
    """Exact integer ASCII of value * 10^scale; refuses rounding."""
    if scale is None or scale < 0:
        raise CanonicalError("DECIMAL requires a non-negative frozen scale")
    d = _to_decimal(value)
    scaled = d.scaleb(scale, _EXACT)
    if scaled != scaled.to_integral_value():
        raise CanonicalError(f"decimal {d} does not fit scale {scale} without rounding")
    return str(int(scaled))


def decimal_text(value: Any, scale: int) -> str:
    """Canonical exact decimal string with exactly `scale` fractional digits."""
    n = int(decimal_scaled_text(value, scale))
    if scale == 0:
        return str(n)
    sign = "-" if n < 0 else ""
    digits = str(abs(n)).rjust(scale + 1, "0")
    return f"{sign}{digits[:-scale]}.{digits[-scale:]}"


def _date_text(value: Any) -> str:
    if isinstance(value, _dt.datetime):
        raise CanonicalError("DATETIME value for DATE column")
    if isinstance(value, _dt.date):
        return f"{value.year:04d}-{value.month:02d}-{value.day:02d}"
    if isinstance(value, str) and _DATE_RE.fullmatch(value):
        _dt.date.fromisoformat(value)
        return value
    raise CanonicalError(f"invalid DATE value {value!r}")


def _datetime_text(value: Any, fsp: int) -> str:
    if isinstance(value, str):
        try:
            value = _dt.datetime.fromisoformat(value.replace(" ", "T"))
        except ValueError as e:
            raise CanonicalError(f"invalid DATETIME text {value!r}") from e
    if not isinstance(value, _dt.datetime):
        raise CanonicalError(f"invalid DATETIME value {value!r}")
    if value.tzinfo is not None:
        raise CanonicalError("DATETIME must be zone-less (wall clock semantics frozen in contract)")
    if not 0 <= fsp <= 6:
        raise CanonicalError("fsp must be 0..6")
    base = (f"{value.year:04d}-{value.month:02d}-{value.day:02d}T"
            f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}")
    micro = f"{value.microsecond:06d}"
    if fsp == 0:
        if value.microsecond:
            raise CanonicalError("fractional seconds present but contract fsp=0")
        return base
    if micro[fsp:].strip("0"):
        raise CanonicalError(f"fractional seconds exceed contract fsp={fsp}")
    return f"{base}.{micro[:fsp]}"


def payload_for(logical_type: str, value: Any, *, scale: int | None = None, fsp: int = 6) -> bytes | None:
    """Encode one non-key value to its C1 payload (None for SQL NULL)."""
    if value is None:
        return None
    if logical_type == "BOOLEAN":
        if isinstance(value, bool):
            return b"1" if value else b"0"
        if isinstance(value, int) and value in (0, 1):
            return b"1" if value else b"0"
        if isinstance(value, str) and value in ("0", "1"):
            return value.encode()
        raise CanonicalError(f"invalid BOOLEAN value {value!r}")
    if logical_type == "INTEGER":
        return canonical_int_text(value).encode("ascii")
    if logical_type == "DECIMAL":
        return decimal_scaled_text(value, scale).encode("ascii")
    if logical_type == "STRING":
        if isinstance(value, str):
            return value.encode("utf-8")
        if isinstance(value, (bytes, bytearray)):
            b = bytes(value)
            b.decode("utf-8")
            return b
        raise CanonicalError(f"invalid STRING value type {type(value).__name__}")
    if logical_type == "DATE":
        return _date_text(value).encode("ascii")
    if logical_type == "DATETIME":
        return _datetime_text(value, fsp).encode("ascii")
    if logical_type == "BINARY":
        if isinstance(value, (bytes, bytearray, memoryview)):
            return bytes(value)
        raise CanonicalError("invalid BINARY value")
    if logical_type in ("FLOAT32", "FLOAT64"):
        raise CanonicalError("FLOAT columns require FLOAT_EXACT approval; use a tolerance policy instead")
    raise CanonicalError(f"UNSUPPORTED logical type {logical_type}")


def encode_val(logical_type: str, value: Any, *, scale: int | None = None, fsp: int = 6) -> bytes:
    tag = LOGICAL_TAGS.get(logical_type)
    if tag is None:
        raise CanonicalError(f"UNSUPPORTED logical type {logical_type}")
    return val(tag, payload_for(logical_type, value, scale=scale, fsp=fsp))


def display_text(logical_type: str, value: Any, *, scale: int | None = None, fsp: int = 6) -> str | None:
    """Human readable canonical text (used for group values and difference locations)."""
    if value is None:
        return None
    if logical_type == "DECIMAL":
        return decimal_text(value, scale)
    if logical_type == "BINARY":
        return bytes(value).hex()
    if logical_type in ("FLOAT32", "FLOAT64"):
        return float_text(value)
    return payload_for(logical_type, value, scale=scale, fsp=fsp).decode("utf-8")


def payload_display(logical_type: str, payload: bytes | None, *, scale: int | None = None) -> str | None:
    """display_text computed from an already encoded C1 payload."""
    if payload is None:
        return None
    if logical_type == "DECIMAL":
        s = int(scale or 0)
        return decimal_text(Decimal(int(payload.decode("ascii"))).scaleb(-s, _EXACT), s)
    if logical_type == "BINARY":
        return payload.hex()
    return payload.decode("utf-8")


def float_text(value: Any) -> str:
    f = float(value)
    if f != f:
        return "NaN"
    if f in (float("inf"), float("-inf")):
        return "Infinity" if f > 0 else "-Infinity"
    return repr(f)


def float_state(value: Any) -> str:
    if value is None:
        return "NULL"
    f = float(value)
    if f != f:
        return "NAN"
    if f == float("inf"):
        return "POS_INF"
    if f == float("-inf"):
        return "NEG_INF"
    return "FINITE"


def floats_close(a: float, b: float, rtol: float, atol: float) -> bool:
    return abs(a - b) <= max(atol, rtol * max(abs(a), abs(b)))


# ---- S1 ---------------------------------------------------------------------

def check_seed(seed_hex: str) -> str:
    if not isinstance(seed_hex, str) or SEED_RE.fullmatch(seed_hex) is None:
        raise CanonicalError("seed must be 32 lowercase hex characters")
    return seed_hex


def s1_digest(key: bytes, seed_hex: str) -> bytes:
    check_seed(seed_hex)
    wire = SAMPLE_PREFIX + seed_hex.encode("ascii") + b"|" + key.hex().encode("ascii")
    return sha256(wire)


def s1_bucket(key: bytes, seed_hex: str, bucket_count: int = 1000) -> int:
    d = s1_digest(key, seed_hex)
    return int.from_bytes(d[:8], "big", signed=False) % bucket_count


def s1_selected(key: bytes, seed_hex: str, bucket_count: int = 1000, selected_bucket: int = 0) -> bool:
    return s1_bucket(key, seed_hex, bucket_count) == selected_bucket


def single_key(logical_type: str, value: Any) -> bytes:
    if value is None:
        raise CanonicalError("NULL key violates key contract")
    if logical_type not in ("INTEGER", "STRING"):
        raise CanonicalError(f"key type {logical_type} not in builtin key contracts")
    return key_bytes([encode_val(logical_type, value)])


# ---- D1 ---------------------------------------------------------------------

class D1:
    """Streaming SRDQ-D1 digest over ordered 32-byte row hashes."""

    def __init__(self, context32: bytes):
        if len(context32) != 32:
            raise CanonicalError("context must be 32 bytes")
        self._h = hashlib.sha256(SHARD_MAGIC + context32)
        self.n = 0

    def add(self, row_hash: bytes) -> None:
        if len(row_hash) != 32:
            raise CanonicalError("row hash must be 32 bytes")
        self._h.update(row_hash)
        self.n += 1

    def finish(self) -> bytes:
        h = self._h.copy()
        h.update(b"END1" + struct.pack(">Q", self.n))
        return h.digest()


def d1(context32: bytes, row_hashes: Iterable[bytes]) -> bytes:
    d = D1(context32)
    for rh in row_hashes:
        d.add(rh)
    return d.finish()


# ---- deterministic JSON -----------------------------------------------------

def _check_json(obj: Any) -> None:
    if isinstance(obj, float):
        raise CanonicalError("floats are not allowed in canonical JSON; use exact strings")
    if isinstance(obj, dict):
        for k, v in obj.items():
            if not isinstance(k, str):
                raise CanonicalError("canonical JSON keys must be strings")
            _check_json(v)
    elif isinstance(obj, (list, tuple)):
        for v in obj:
            _check_json(v)
    elif isinstance(obj, int) and not isinstance(obj, bool) and abs(obj) > 2**53:
        raise CanonicalError("large integers must be strings in canonical JSON")


def canonical_json(obj: Any) -> bytes:
    _check_json(obj)
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def json_digest(obj: Any) -> bytes:
    return sha256(canonical_json(obj))


def key_projection_context(member_context32: bytes) -> bytes:
    return json_digest({"base_context": member_context32.hex(), "projection": "SAMPLE_KEYS"})
