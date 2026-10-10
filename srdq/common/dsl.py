"""Typed scope predicates and the restricted filter DSL (no raw SQL)."""
from __future__ import annotations

import datetime as _dt
import re
from decimal import Decimal
from typing import Any

from . import canonical as c

MAX_FILTER_DEPTH = 8
MAX_FILTER_NODES = 64
MAX_IN_VALUES = 1000

COMPARE_OPS = {"EQ": "=", "NE": "<>", "LT": "<", "LE": "<=", "GT": ">", "GE": ">="}
LOGIC_OPS = ("AND", "OR")
NULL_OPS = ("IS_NULL", "IS_NOT_NULL")

IDENT_RE = re.compile(r"[A-Za-z0-9_$\u0080-\uffff]{1,128}")

# SR data type -> logical type used by C1.  Anything not listed is UNSUPPORTED.
_SR_TYPES = {
    "tinyint": "INTEGER", "smallint": "INTEGER", "int": "INTEGER", "integer": "INTEGER",
    "bigint": "INTEGER", "largeint": "INTEGER",
    "boolean": "BOOLEAN", "bool": "BOOLEAN",
    "decimal": "DECIMAL", "decimalv2": "DECIMAL", "decimal32": "DECIMAL", "decimal64": "DECIMAL",
    "decimal128": "DECIMAL", "decimalv3": "DECIMAL", "numeric": "DECIMAL",
    "char": "STRING", "varchar": "STRING", "string": "STRING", "text": "STRING",
    "date": "DATE", "datetime": "DATETIME", "timestamp": "DATETIME",
    "float": "FLOAT32", "double": "FLOAT64", "real": "FLOAT64",
    "binary": "BINARY", "varbinary": "BINARY", "blob": "BINARY",
}

_SCALE_RE = re.compile(r"\(\s*\d+\s*,\s*(\d+)\s*\)")


class DslError(ValueError):
    pass


def check_ident(name: str) -> str:
    if not isinstance(name, str) or IDENT_RE.fullmatch(name) is None:
        raise DslError(f"invalid identifier {name!r}")
    return name


def logical_type(data_type: str, column_type: str | None = None) -> tuple[str, int | None]:
    """Map an SR column type to (logical_type, scale). Unknown types map to ('UNSUPPORTED', None)."""
    base = (data_type or "").strip().lower()
    base = base.split("(")[0].strip()
    lt = _SR_TYPES.get(base)
    if lt is None:
        return "UNSUPPORTED", None
    scale = None
    if lt == "DECIMAL":
        m = _SCALE_RE.search(column_type or data_type or "")
        scale = int(m.group(1)) if m else 0
    return lt, scale


def is_char_type(data_type: str) -> bool:
    return (data_type or "").strip().lower().split("(")[0] == "char"


def is_float(lt: str) -> bool:
    return lt in ("FLOAT32", "FLOAT64")


# ---- typed values ------------------------------------------------------------

def typed_value(lt: str, value: Any, scale: int | None = None) -> Any:
    """Convert a DSL/boundary literal (string or int) to a bound SQL parameter of the column type."""
    if value is None:
        raise DslError("NULL literal is not allowed; use IS_NULL")
    try:
        if lt == "INTEGER":
            return int(c.canonical_int_text(value))
        if lt == "DECIMAL":
            d = Decimal(str(value))
            if not d.is_finite():
                raise DslError("non-finite decimal literal")
            return str(d)
        if lt == "STRING":
            if not isinstance(value, str):
                raise DslError("STRING literal must be a string")
            return value
        if lt == "DATE":
            return _dt.date.fromisoformat(str(value)).isoformat()
        if lt == "DATETIME":
            return _dt.datetime.fromisoformat(str(value).replace("T", " ")).isoformat(sep=" ")
        if lt == "BOOLEAN":
            if value in (True, "true", "1", 1):
                return 1
            if value in (False, "false", "0", 0):
                return 0
            raise DslError("invalid BOOLEAN literal")
    except (ValueError, c.CanonicalError) as e:
        raise DslError(f"invalid {lt} literal {value!r}: {e}") from e
    raise DslError(f"literals of type {lt} are not supported in predicates")


def sort_key(lt: str, value: Any):
    """Total order for boundary values of the same logical type (None = unbounded handled by caller)."""
    if lt == "INTEGER":
        return int(c.canonical_int_text(value))
    if lt == "DECIMAL":
        return Decimal(str(value))
    if lt == "DATE":
        return _dt.date.fromisoformat(str(value))
    if lt == "DATETIME":
        return _dt.datetime.fromisoformat(str(value).replace("T", " "))
    if lt == "STRING":
        return str(value).encode("utf-8")
    raise DslError(f"type {lt} not orderable for range scopes")


# ---- filter DSL --------------------------------------------------------------

def validate_filter(node: Any, columns: dict[str, dict] | None = None) -> dict:
    """Validate a filter DSL tree; `columns` maps name -> {'type': lt, 'scale': s} when known."""
    counter = [0]

    def walk(n: Any, depth: int) -> None:
        counter[0] += 1
        if counter[0] > MAX_FILTER_NODES:
            raise DslError("filter has too many nodes")
        if depth > MAX_FILTER_DEPTH:
            raise DslError("filter is too deep")
        if not isinstance(n, dict) or "op" not in n:
            raise DslError("filter node must be an object with 'op'")
        op = n["op"]
        allowed: set[str]
        if op == "TRUE":
            allowed = {"op"}
        elif op in LOGIC_OPS:
            allowed = {"op", "args"}
            args = n.get("args")
            if not isinstance(args, list) or not args:
                raise DslError(f"{op} needs a non-empty args list")
            for a in args:
                walk(a, depth + 1)
        elif op in COMPARE_OPS:
            allowed = {"op", "column", "value"}
            _check_col(n.get("column"), columns, n.get("value"))
        elif op == "IN":
            allowed = {"op", "column", "values"}
            vals = n.get("values")
            if not isinstance(vals, list) or not vals or len(vals) > MAX_IN_VALUES:
                raise DslError(f"IN needs 1..{MAX_IN_VALUES} values")
            for v in vals:
                _check_col(n.get("column"), columns, v)
        elif op in NULL_OPS:
            allowed = {"op", "column"}
            _check_col(n.get("column"), columns, None, null_op=True)
        else:
            raise DslError(f"unsupported filter op {op!r}")
        extra = set(n) - allowed
        if extra:
            raise DslError(f"unknown filter fields {sorted(extra)}")

    walk(node, 1)
    return node


def _check_col(name, columns, value, null_op=False):
    check_ident(name)
    if columns is not None:
        if name not in columns:
            raise DslError(f"filter column {name!r} not found")
        if not null_op:
            col = columns[name]
            typed_value(col["type"], value, col.get("scale"))
    elif not null_op and (value is None or isinstance(value, float)):
        raise DslError("filter literal must be a string/integer (never NULL or float)")


def filter_columns(node: dict) -> set[str]:
    out: set[str] = set()
    if node.get("op") in LOGIC_OPS:
        for a in node["args"]:
            out |= filter_columns(a)
    elif "column" in node:
        out.add(node["column"])
    return out


def compile_filter(node: dict, columns: dict[str, dict], quote) -> tuple[str, list]:
    op = node["op"]
    if op == "TRUE":
        return "1 = 1", []
    if op in LOGIC_OPS:
        parts, params = [], []
        for a in node["args"]:
            s, p = compile_filter(a, columns, quote)
            parts.append(f"({s})")
            params += p
        return f" {op} ".join(parts), params
    col = columns[node["column"]]
    q = quote(node["column"])
    if op in COMPARE_OPS:
        return f"{q} {COMPARE_OPS[op]} %s", [typed_value(col["type"], node["value"], col.get("scale"))]
    if op == "IN":
        vals = [typed_value(col["type"], v, col.get("scale")) for v in node["values"]]
        return f"{q} IN ({', '.join(['%s'] * len(vals))})", vals
    if op == "IS_NULL":
        return f"{q} IS NULL", []
    if op == "IS_NOT_NULL":
        return f"{q} IS NOT NULL", []
    raise DslError(op)  # pragma: no cover


# ---- scope predicates --------------------------------------------------------

SCOPE_KINDS = ("ALL", "RANGE", "LIST", "IS_NULL", "KEY_RANGE", "HASH_BUCKET")


def scope_all() -> dict:
    return {"kind": "ALL"}


def scope_range(column: str, lt: str, lower: Any, upper: Any) -> dict:
    return {"kind": "RANGE", "column": column, "type": lt,
            "lower": None if lower is None else str(lower), "upper": None if upper is None else str(upper)}


def scope_list(column: str, lt: str, values: list) -> dict:
    vals = sorted({str(v) for v in values}, key=lambda v: sort_key(lt, v) if lt != "BOOLEAN" else v)
    return {"kind": "LIST", "column": column, "type": lt, "values": vals}


def scope_null(column: str) -> dict:
    return {"kind": "IS_NULL", "column": column}


def scope_key(pred: dict | list) -> str:
    return c.canonical_json(pred).decode("utf-8")


def scope_digest(pred: dict | list) -> bytes:
    return c.json_digest(pred)


def scope_label(pred: dict | list) -> str:
    if isinstance(pred, list):
        return " AND ".join(scope_label(p) for p in pred) or "ALL"
    k = pred["kind"]
    if k == "ALL":
        return "ALL"
    if k in ("RANGE", "KEY_RANGE"):
        lo = "-inf" if pred.get("lower") is None else pred["lower"]
        hi = "+inf" if pred.get("upper") is None else pred["upper"]
        return f"{pred['column']} in [{lo}, {hi})"
    if k == "LIST":
        return f"{pred['column']} in {{{', '.join(pred['values'])}}}"
    if k == "IS_NULL":
        return f"{pred['column']} IS NULL"
    if k == "HASH_BUCKET":
        return f"hash bucket {pred['bucket']}/{pred['buckets']}"
    return k


def compile_scope(pred: dict | list, quote) -> tuple[str, list]:
    atoms = pred if isinstance(pred, list) else [pred]
    parts, params = [], []
    for a in atoms:
        k = a["kind"]
        if k in ("ALL", "HASH_BUCKET"):
            continue
        q = quote(check_ident(a["column"]))
        if k in ("RANGE", "KEY_RANGE"):
            if a.get("lower") is not None:
                parts.append(f"{q} >= %s")
                params.append(typed_value(a["type"], a["lower"]))
            if a.get("upper") is not None:
                parts.append(f"{q} < %s")
                params.append(typed_value(a["type"], a["upper"]))
            if a.get("lower") is None and a.get("upper") is None:
                parts.append(f"{q} IS NOT NULL")
        elif k == "LIST":
            vals = [typed_value(a["type"], v) for v in a["values"]]
            parts.append(f"{q} IN ({', '.join(['%s'] * len(vals))})")
            params += vals
        elif k == "IS_NULL":
            parts.append(f"{q} IS NULL")
        else:
            raise DslError(f"unsupported scope kind {k}")
    return (" AND ".join(parts) if parts else "1 = 1"), params
