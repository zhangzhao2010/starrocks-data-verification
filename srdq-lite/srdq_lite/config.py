"""Load and validate config.yaml and tables.jsonl."""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import yaml

RULE_NAMES = ("ROW_COUNT", "METRIC", "SAMPLE")
METRIC_FUNCS = ("SUM", "MIN", "MAX", "COUNT", "COUNT_NULL", "COUNT_DISTINCT")
_SESSION_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Cluster:
    side: str
    host: str
    port: int
    user: str
    password: str = field(repr=False)
    ssl: bool = False
    ssl_ca: str | None = None

    def describe(self) -> dict:
        return {"host": self.host, "port": self.port, "user": self.user, "ssl": self.ssl}


@dataclass(frozen=True)
class RunSettings:
    concurrency: int = 4
    connect_timeout_s: int = 10
    query_timeout_s: int = 600
    retries: int = 2
    max_partitions_per_sql: int = 100
    max_rows_per_sql: int = 2_000_000_000
    sample_partitions_per_sql: int = 10
    max_diffs_per_rule: int = 10_000


@dataclass(frozen=True)
class Config:
    source: Cluster
    target: Cluster
    run: RunSettings
    session: dict[str, Any]

    def fingerprint(self) -> dict:
        return {"source": self.source.describe(), "target": self.target.describe(),
                "run": asdict(self.run), "session": self.session}


@dataclass(frozen=True)
class Rule:
    index: int
    name: str
    config: dict

    @property
    def key(self) -> str:
        return f"{self.index}:{self.name}"


@dataclass(frozen=True)
class TableSpec:
    line: int
    source_db: str
    source_table: str
    target_db: str
    target_table: str
    rules: tuple[Rule, ...]
    column_mapping: dict[str, str]

    @property
    def id(self) -> str:
        return f"{self.source_db}.{self.source_table}=>{self.target_db}.{self.target_table}"

    def to_dict(self) -> dict:
        return {"source_db": self.source_db, "source_table": self.source_table, "target_db": self.target_db,
                "target_table": self.target_table, "column_mapping": self.column_mapping,
                "rules": [{"rule_name": r.name, "config": r.config} for r in self.rules]}


def canonical_hash(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False, default=str).encode()).hexdigest()


# ---------------------------------------------------------------- config.yaml

def load_config(path: str | Path) -> Config:
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as e:
        raise ConfigError(f"cannot read config {path}: {e}") from e
    if not isinstance(data, dict):
        raise ConfigError("config root must be a mapping")
    _no_unknown(data, {"source", "target", "run", "session"}, "config")
    session = data.get("session") or {}
    if not isinstance(session, dict):
        raise ConfigError("session must be a mapping")
    for k, v in session.items():
        if not _SESSION_NAME_RE.match(str(k)):
            raise ConfigError(f"invalid session variable name {k!r}")
        if not isinstance(v, (str, int, float, bool)):
            raise ConfigError(f"session variable {k} must be a scalar")
    return Config(source=_cluster("source", data.get("source")), target=_cluster("target", data.get("target")),
                  run=_run_settings(data.get("run") or {}), session=dict(session))


def _cluster(side: str, raw: Any) -> Cluster:
    if not isinstance(raw, dict):
        raise ConfigError(f"{side} must be a mapping")
    _no_unknown(raw, {"host", "port", "user", "password", "password_env", "ssl", "ssl_ca"}, side)
    for k in ("host", "user"):
        if not raw.get(k):
            raise ConfigError(f"{side}.{k} is required")
    if raw.get("password_env"):
        env = str(raw["password_env"])
        if env not in os.environ:
            raise ConfigError(f"{side}.password_env: environment variable {env} is not set")
        password = os.environ[env]
    elif "password" in raw:
        password = str(raw["password"] if raw["password"] is not None else "")
    else:
        raise ConfigError(f"{side}: set password or password_env")
    return Cluster(side=side, host=str(raw["host"]), port=_pos_int(raw.get("port", 9030), f"{side}.port"),
                   user=str(raw["user"]), password=password, ssl=bool(raw.get("ssl", False)),
                   ssl_ca=raw.get("ssl_ca"))


def _run_settings(raw: Any) -> RunSettings:
    if not isinstance(raw, dict):
        raise ConfigError("run must be a mapping")
    defaults = RunSettings()
    _no_unknown(raw, set(asdict(defaults)), "run")
    vals = {k: _pos_int(raw.get(k, v), f"run.{k}", allow_zero=(k == "retries")) for k, v in asdict(defaults).items()}
    return RunSettings(**vals)


# ---------------------------------------------------------------- tables.jsonl

def load_tables(path: str | Path) -> list[TableSpec]:
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except OSError as e:
        raise ConfigError(f"cannot read tables file {path}: {e}") from e
    specs: list[TableSpec] = []
    seen: dict[str, int] = {}
    for no, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as e:
            raise ConfigError(f"{path}:{no}: invalid JSON: {e}") from e
        try:
            spec = _table_spec(no, raw)
        except ConfigError as e:
            raise ConfigError(f"{path}:{no}: {e}") from e
        if spec.id in seen:
            raise ConfigError(f"{path}:{no}: duplicate table pair {spec.id} (first at line {seen[spec.id]})")
        seen[spec.id] = no
        specs.append(spec)
    if not specs:
        raise ConfigError(f"{path}: no tables")
    return specs


def _table_spec(no: int, raw: Any) -> TableSpec:
    if not isinstance(raw, dict):
        raise ConfigError("each line must be a JSON object")
    _no_unknown(raw, {"source_db", "source_table", "target_db", "target_table", "rules", "rule",
                      "column_mapping"}, "table")
    for k in ("source_db", "source_table", "target_db", "target_table"):
        if not isinstance(raw.get(k), str) or not raw[k]:
            raise ConfigError(f"{k} is required")
    rules_raw = raw.get("rules", raw.get("rule"))
    if isinstance(rules_raw, dict):
        rules_raw = [rules_raw]
    if not isinstance(rules_raw, list) or not rules_raw:
        raise ConfigError("rules must be a non-empty list")
    mapping = raw.get("column_mapping") or {}
    if not isinstance(mapping, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in mapping.items()):
        raise ConfigError("column_mapping must map source column names to target column names")
    rules = tuple(_rule(i, r) for i, r in enumerate(rules_raw))
    return TableSpec(line=no, source_db=raw["source_db"], source_table=raw["source_table"],
                     target_db=raw["target_db"], target_table=raw["target_table"], rules=rules,
                     column_mapping={k.lower(): v for k, v in mapping.items()})


def _rule(index: int, raw: Any) -> Rule:
    if not isinstance(raw, dict):
        raise ConfigError(f"rule #{index} must be an object")
    _no_unknown(raw, {"rule_name", "config"}, f"rule #{index}")
    name = str(raw.get("rule_name", "")).upper()
    if name not in RULE_NAMES:
        raise ConfigError(f"rule #{index}: rule_name must be one of {', '.join(RULE_NAMES)}")
    cfg = raw.get("config") or {}
    if not isinstance(cfg, dict):
        raise ConfigError(f"rule #{index}: config must be an object")
    where = f"rule #{index} {name}"
    if name == "ROW_COUNT":
        _no_unknown(cfg, {"filter"}, where)
        norm = {"filter": _filter(cfg.get("filter"), where)}
    elif name == "METRIC":
        norm = _metric_config(cfg, where)
    else:
        norm = _sample_config(cfg, where)
    return Rule(index=index, name=name, config=norm)


def _metric_config(cfg: dict, where: str) -> dict:
    _no_unknown(cfg, {"metrics", "group_by", "filter", "max_groups"}, where)
    metrics = cfg.get("metrics") or []
    if not isinstance(metrics, list):
        raise ConfigError(f"{where}: metrics must be a list")
    group_by = _str_list(cfg.get("group_by") or [], f"{where}.group_by")
    if not metrics and not group_by:
        raise ConfigError(f"{where}: metrics or group_by is required")
    out = []
    for m in metrics:
        if not isinstance(m, dict):
            raise ConfigError(f"{where}: each metric must be an object")
        _no_unknown(m, {"column", "func", "tolerance"}, f"{where} metric")
        func = str(m.get("func", "")).upper()
        if func not in METRIC_FUNCS:
            raise ConfigError(f"{where}: func must be one of {', '.join(METRIC_FUNCS)}")
        if not isinstance(m.get("column"), str) or not m["column"]:
            raise ConfigError(f"{where}: metric column is required")
        tol = m.get("tolerance")
        if tol is not None and (not isinstance(tol, (int, float)) or isinstance(tol, bool) or tol < 0):
            raise ConfigError(f"{where}: tolerance must be a non-negative number")
        out.append({"column": m["column"], "func": func, "tolerance": tol})
    return {"metrics": out, "group_by": group_by, "filter": _filter(cfg.get("filter"), where),
            "max_groups": _pos_int(cfg.get("max_groups", 100_000), f"{where}.max_groups")}


def _sample_config(cfg: dict, where: str) -> dict:
    _no_unknown(cfg, {"key", "columns", "exclude_columns", "ratio", "max_rows", "filter"}, where)
    key = cfg.get("key")
    if key is not None:
        key = [key] if isinstance(key, str) else _str_list(key, f"{where}.key")
        if not key:
            raise ConfigError(f"{where}: key must not be empty")
    columns = cfg.get("columns", "*")
    if columns != "*":
        columns = _str_list(columns, f"{where}.columns")
    ratio = cfg.get("ratio", 0.001)
    if not isinstance(ratio, (int, float)) or isinstance(ratio, bool) or not 0 < ratio <= 1:
        raise ConfigError(f"{where}: ratio must be in (0, 1]")
    return {"key": key, "columns": columns, "exclude_columns": _str_list(cfg.get("exclude_columns") or [],
                                                                          f"{where}.exclude_columns"),
            "ratio": float(ratio), "max_rows": _pos_int(cfg.get("max_rows", 10_000), f"{where}.max_rows"),
            "filter": _filter(cfg.get("filter"), where)}


def _filter(raw: Any, where: str) -> dict | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, str):
        return {"source": raw, "target": raw}
    if isinstance(raw, dict) and set(raw) <= {"source", "target"} and all(isinstance(v, str) for v in raw.values()):
        return {"source": raw.get("source") or None, "target": raw.get("target") or None}
    raise ConfigError(f"{where}: filter must be a string or {{\"source\": ..., \"target\": ...}}")


def _str_list(raw: Any, where: str) -> list[str]:
    if not isinstance(raw, list) or not all(isinstance(x, str) and x for x in raw):
        raise ConfigError(f"{where} must be a list of names")
    return list(raw)


def _pos_int(raw: Any, where: str, allow_zero: bool = False) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0 or (raw == 0 and not allow_zero):
        raise ConfigError(f"{where} must be a {'non-negative' if allow_zero else 'positive'} integer")
    return raw


def _no_unknown(raw: dict, allowed: set[str], where: str) -> None:
    extra = set(raw) - allowed
    if extra:
        raise ConfigError(f"{where}: unknown field(s) {', '.join(sorted(extra))}")
