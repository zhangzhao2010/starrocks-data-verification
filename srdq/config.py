"""Strict configuration loading for fixed multi-cluster verification pairs."""
from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml

SPEC_VERSION = "srdq-config-v1.4"
PROTOCOL_VERSION = "srdq-v1.4"
PLACEHOLDER_RE = re.compile(r"^<.*>$")
ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
POOL_KINDS = ("COUNT", "METRIC", "SAMPLE", "METADATA", "DEEP")

DEFAULTS: dict[str, Any] = {
    "spec_version": SPEC_VERSION,
    "run_enabled": False,
    "read_mode": "ONLINE_ONLY",
    "temporal_assurance": "NONE",
    "lineage_required": False,
    "production_job_monitoring": False,
    "physical_clusters": [
        {
            "id": "huawei",
            "name": "Huawei",
            "cloud": "HUAWEI",
            "connection_ref": "sr-huawei-ro",
            "credential_file": "/etc/srdq/secrets/huawei.json",
            "os_user": "srdq-hw",
            "concurrency": {"COUNT": 40, "METRIC": 40, "SAMPLE": 40, "METADATA": 2, "DEEP": 2},
        },
        {
            "id": "aliyun",
            "name": "Aliyun",
            "cloud": "ALIYUN",
            "connection_ref": "sr-aliyun-ro",
            "credential_file": "/etc/srdq/secrets/aliyun.json",
            "os_user": "srdq-al",
            "concurrency": {"COUNT": 40, "METRIC": 40, "SAMPLE": 40, "METADATA": 2, "DEEP": 2},
        },
    ],
    "cluster_pairs": [
        {"id": "legacy", "name": "Legacy", "source_cluster_id": "huawei", "target_cluster_id": "aliyun"},
    ],
    "deployment": {
        "mode": "CENTRAL",
        "controller_location": "ALIYUN",
        "side_isolation": "SEPARATE_PROCESS_AND_IDENTITY",
        "worker_on_huawei_required": False,
    },
    "scheduler": {
        "strategy": "KIND_THEN_PRIORITY_THEN_LAYER",
        "kind_order": ["COUNT", "METRIC", "SAMPLE", "DEEP"],
        "layer_order": ["ODS", "DWD", "DWS", "ADS", "DIM", "OTHER"],
        "default_priority": 50,
        "implicit_aging": False,
        "same_asset_cross_run": "SERIALIZE",
    },
    "execution_window": {
        "timezone": "Asia/Shanghai",
        "open_at": "20:00",
        "drain_at": "07:30",
        "close_at": "08:00",
        "on_close": "CANCEL_AND_CONFIRM",
        "predict_overrun": True,
        "predict_factor": 1.5,
    },
    "load_guard": {
        "adaptive_concurrency": False,
        "min_concurrency": 1,
        "timeout_rate_threshold": 0.05,
        "slowdown_factor_threshold": 3.0,
        "hard_stop_consecutive_timeouts": 5,
        "window_queries": 20,
        "recover_after_seconds": 600,
        "sr_signals": {
            "enabled": True,
            "poll_seconds": 60,
            "running_queries_threshold": None,
            "pending_queries_threshold": None,
        },
    },
    "scope": {
        "input_format": "asset-list-v1",
        "rule_template_format": "csv-utf8",
        "union_of_both_sides": True,
        "new_scope_policy": "REQUIRE_CONFIRM",
        "unknown_inventory_blocks_full_coverage": True,
        "null_scope": "explicit",
        "outside_scope": "explicit",
    },
    "checks": {
        "baseline": "COUNT",
        "optional": ["METRIC", "SAMPLE"],
        "deep_default_enabled": False,
        "rule_version": "check-rule-v1.3",
        "default_check_selector": {"kinds": ["COUNT"]},
        "count": {"exact": True, "business_filter_allowed": False},
        "metric": {
            "compute_location": "STARROCKS",
            "max_groups_per_side": 10000,
            "max_group_key_bytes": 512,
            "max_result_bytes_per_side": 67108864,
            "sum_non_null_guard": True,
        },
        "sample": {
            "protocol": "SRDQ-SAMPLE-1",
            "bucket_count": 1000,
            "selected_bucket": 0,
            "filter_location": "STARROCKS",
            "fingerprint_location": "EXECUTOR",
            "builtin_key_contract": ["NON_NULL_UNIQUE_INTEGER", "NON_NULL_UNIQUE_STRING"],
            "uniqueness_evidence": "TABLE_MODEL_ONLY",
            "max_key_bytes": 256,
            "default_seed_hex": "00000000000000000000000000000000",
            "min_sample_rows_per_side": 1,
            "max_sample_rows_per_side": 2000000,
            "max_result_bytes_per_side": 2147483648,
            "empty_sample_policy": "INCONCLUSIVE",
            "fallback_full_fetch": False,
        },
    },
    "comparison": {
        "canonical": "SRDQ-C1",
        "digest": "SRDQ-D1",
        "context_schema": "SRDQ-OBS-2",
        "max_start_skew_seconds": 300,
        "observation_deadline_seconds": 1800,
        "retry_reads": "BOTH_SIDES_NEW_OBSERVATION",
        "float_default": "REQUIRE_POLICY",
        "non_finite_default": "UNSUPPORTED",
        "complex_type_default": "UNSUPPORTED",
    },
    "control": {
        "default_effect": "CURRENT_ONLY",
        "pause_mode": "DRAIN_STARTED_OBSERVATIONS",
        "archive_mode": "LOGICAL_ONLY",
        "regenerate_mode": "NEW_RUN",
    },
    "execution": {
        "lease_seconds": 120,
        "heartbeat_seconds": 30,
        "query_timeout_seconds": 300,
        "read_timeout_seconds": 60,
        "max_retries": 2,
        "retry_backoff_seconds": 60,
        "cancel_pending_alert_seconds": 600,
        "executor_memory_bytes": 536870912,
        "batch_rows": 5000,
        "batch_bytes": 8388608,
        "max_value_bytes": 16777216,
        "max_row_bytes": 33554432,
        "local_material_quota_bytes": 21474836480,
        "local_outbox_quota_bytes": 1073741824,
        "require_pushdown_capability": True,
        "require_verified_eof": True,
    },
    "transfer": {
        "global_approval_ref": "<待登记：客户书面审批依据>",
        "globally_approved_classes": ["SUMMARY_ONLY", "AGGREGATES_APPROVED", "SAMPLE_VALUES_APPROVED"],
        "detail_requires_per_request_confirm": True,
        "central_sample_required_class": "SAMPLE_VALUES_APPROVED",
        "metric_required_class": "AGGREGATES_APPROVED",
        "max_cross_cloud_bytes_per_run": 10737418240,
        "max_result_bytes_per_run": 21474836480,
        "reset_budget_on_retry": False,
    },
    "diagnosis": {
        "source_modes": ["LIVE_REREAD"],
        "default_check": "SAME_AS_ORIGIN",
        "deep_requires_explicit_approval": True,
        "display_sample_limit": 100,
        "max_refinement_depth": 8,
        "max_leaf_count": 2048,
        "max_seconds_per_scope": 1800,
        "max_read_bytes_per_scope": 10737418240,
        "max_transfer_bytes_per_run": 1073741824,
    },
    "retention": {
        "raw_value_days": 7,
        "purge_outside_window_only": True,
        "max_difference_rows": 1000,
        "backup_retention": "RAW_VALUE_DAYS_PLUS_ONE_FULL_CYCLE",
    },
    "difference": {
        "auto_recheck": False,
        "triage_records": False,
        "trend_basis": ["OBSERVED_EQUAL", "OBSERVED_DIFFERENT"],
    },
    "error_classification": {
        "storage_suspect_patterns": ["tablet", "replica", "checksum", "corrupt", "segment", "no queryable replica"],
        "storage_suspect_auto_pause": False,
    },
    "report": {
        "daily": True,
        "trigger": "AFTER_WINDOW_CLOSED",
        "grace_minutes": 15,
        "dir": "/var/lib/srdq/reports",
        "formats": ["MARKDOWN", "CSV_UTF8_BOM"],
        "top_n_in_markdown": 100,
    },
    "notification": {"channels": ["DASHBOARD", "DAILY_REPORT"]},
    "credentials": {
        "controller_pg": {"file": "/etc/srdq/secrets/pg.json", "os_user": "srdq-ctl", "mode": "0600"},
    },
    "schedule_defaults": {
        "timezone": "Asia/Shanghai",
        "overlap": "SKIP",
        "misfire": "SKIP",
        "continuous_rest_seconds": 300,
        "cancel_on_end": False,
    },
    "web": {
        "bind_address": "0.0.0.0",
        "port": 8470,
        "authorization": "NONE",
        "worker_auth": "CLUSTER_BOUND_TOKEN",
        "max_page_size": 200,
        "operation_preview_required": True,
        "default_masked_values": True,
    },
    "controller": {
        "worker_api_bind": "127.0.0.1",
        "worker_api_port": 8471,
        "tick_seconds": 2,
        "data_dir": "/var/lib/srdq/controller",
    },
    "executor": {
        "controller_url": "http://127.0.0.1:8471",
        "data_dir": "/var/lib/srdq/executor",
        "poll_seconds": 0.2,
    },
}

# Keys whose value is a free-form mapping (no strict sub-key checking beyond type).
_NULLABLE_NUMBERS = {
    ("load_guard", "sr_signals", "running_queries_threshold"),
    ("load_guard", "sr_signals", "pending_queries_threshold"),
}

_FIXED = {
    ("read_mode",): "ONLINE_ONLY",
    ("temporal_assurance",): "NONE",
    ("lineage_required",): False,
    ("production_job_monitoring",): False,
    ("deployment", "side_isolation"): "SEPARATE_PROCESS_AND_IDENTITY",
    ("scheduler", "strategy"): "KIND_THEN_PRIORITY_THEN_LAYER",
    ("scheduler", "kind_order"): ["COUNT", "METRIC", "SAMPLE", "DEEP"],
    ("scheduler", "layer_order"): ["ODS", "DWD", "DWS", "ADS", "DIM", "OTHER"],
    ("scheduler", "implicit_aging"): False,
    ("scheduler", "same_asset_cross_run"): "SERIALIZE",
    ("execution_window", "on_close"): "CANCEL_AND_CONFIRM",
    ("scope", "input_format"): "asset-list-v1",
    ("scope", "rule_template_format"): "csv-utf8",
    ("scope", "union_of_both_sides"): True,
    ("checks", "baseline"): "COUNT",
    ("checks", "rule_version"): "check-rule-v1.3",
    ("checks", "count", "exact"): True,
    ("checks", "count", "business_filter_allowed"): False,
    ("checks", "metric", "compute_location"): "STARROCKS",
    ("checks", "sample", "protocol"): "SRDQ-SAMPLE-1",
    ("checks", "sample", "bucket_count"): 1000,
    ("checks", "sample", "selected_bucket"): 0,
    ("checks", "sample", "filter_location"): "STARROCKS",
    ("checks", "sample", "fingerprint_location"): "EXECUTOR",
    ("checks", "sample", "uniqueness_evidence"): "TABLE_MODEL_ONLY",
    ("checks", "sample", "empty_sample_policy"): "INCONCLUSIVE",
    ("checks", "sample", "fallback_full_fetch"): False,
    ("comparison", "canonical"): "SRDQ-C1",
    ("comparison", "digest"): "SRDQ-D1",
    ("comparison", "context_schema"): "SRDQ-OBS-2",
    ("comparison", "retry_reads"): "BOTH_SIDES_NEW_OBSERVATION",
    ("comparison", "float_default"): "REQUIRE_POLICY",
    ("comparison", "non_finite_default"): "UNSUPPORTED",
    ("comparison", "complex_type_default"): "UNSUPPORTED",
    ("control", "pause_mode"): "DRAIN_STARTED_OBSERVATIONS",
    ("control", "archive_mode"): "LOGICAL_ONLY",
    ("execution", "require_pushdown_capability"): True,
    ("execution", "require_verified_eof"): True,
    ("transfer", "detail_requires_per_request_confirm"): True,
    ("transfer", "reset_budget_on_retry"): False,
    ("diagnosis", "source_modes"): ["LIVE_REREAD"],
    ("diagnosis", "deep_requires_explicit_approval"): True,
    ("difference", "auto_recheck"): False,
    ("difference", "triage_records"): False,
    ("difference", "trend_basis"): ["OBSERVED_EQUAL", "OBSERVED_DIFFERENT"],
    ("error_classification", "storage_suspect_auto_pause"): False,
    ("report", "trigger"): "AFTER_WINDOW_CLOSED",
    ("web", "authorization"): "NONE",
    ("web", "worker_auth"): "CLUSTER_BOUND_TOKEN",
}


class ConfigError(ValueError):
    pass


class Config(dict):
    """Nested dict with attribute access (read-only by convention)."""

    def __getattr__(self, name: str) -> Any:
        try:
            v = self[name]
        except KeyError as e:
            raise AttributeError(name) from e
        return Config(v) if isinstance(v, dict) and not isinstance(v, Config) else v

    def get_path(self, *path: str) -> Any:
        v: Any = self
        for p in path:
            v = v[p]
        return v

    @property
    def transfer_approved(self) -> bool:
        ref = self["transfer"]["global_approval_ref"]
        return bool(ref) and not PLACEHOLDER_RE.match(ref.strip())

    @property
    def cluster_ids(self) -> tuple[str, ...]:
        return tuple(c["id"] for c in self["physical_clusters"])

    def cluster(self, cluster_id: str) -> dict:
        return next(c for c in self["physical_clusters"] if c["id"] == cluster_id)

    def pair(self, pair_id: str) -> dict:
        return next(p for p in self["cluster_pairs"] if p["id"] == pair_id)

    def pair_for_cluster(self, cluster_id: str) -> dict:
        return next(p for p in self["cluster_pairs"]
                    if cluster_id in (p["source_cluster_id"], p["target_cluster_id"]))

    def concurrency(self, cluster_id: str, kind: str) -> int:
        return int(self.cluster(cluster_id)["concurrency"][kind])


def _merge(default: Any, override: Any, path: tuple[str, ...]) -> Any:
    if isinstance(default, dict):
        if not isinstance(override, dict):
            raise ConfigError(f"{'.'.join(path)} must be a mapping")
        unknown = set(override) - set(default)
        if unknown:
            raise ConfigError(f"unknown config keys at {'.'.join(path) or '<root>'}: {sorted(unknown)}")
        out = copy.deepcopy(default)
        for k, v in override.items():
            out[k] = _merge(default[k], v, path + (k,))
        return out
    if path in _NULLABLE_NUMBERS:
        if override is not None and (isinstance(override, bool) or not isinstance(override, (int, float))):
            raise ConfigError(f"{'.'.join(path)} must be a number or null")
        return override
    if isinstance(default, bool):
        if not isinstance(override, bool):
            raise ConfigError(f"{'.'.join(path)} must be a boolean")
    elif isinstance(default, int):
        if isinstance(override, bool) or not isinstance(override, int):
            raise ConfigError(f"{'.'.join(path)} must be an integer")
        if override < 0:
            raise ConfigError(f"{'.'.join(path)} must be non-negative")
    elif isinstance(default, float):
        if isinstance(override, bool) or not isinstance(override, (int, float)):
            raise ConfigError(f"{'.'.join(path)} must be a number")
        override = float(override)
    elif isinstance(default, str):
        if not isinstance(override, str):
            raise ConfigError(f"{'.'.join(path)} must be a string")
    elif isinstance(default, list):
        if not isinstance(override, list):
            raise ConfigError(f"{'.'.join(path)} must be a list")
    return override


def _hhmm(s: str, name: str) -> int:
    m = re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", s)
    if not m:
        raise ConfigError(f"{name} must be HH:MM")
    return int(m.group(1)) * 60 + int(m.group(2))


def validate(cfg: dict) -> None:
    if cfg["spec_version"] != SPEC_VERSION:
        raise ConfigError(f"spec_version must be {SPEC_VERSION} (older versions are rejected)")
    clusters = cfg["physical_clusters"]
    pairs = cfg["cluster_pairs"]
    if not isinstance(clusters, list) or not clusters:
        raise ConfigError("physical_clusters must be a non-empty list")
    required_cluster = {"id", "name", "cloud", "connection_ref", "credential_file", "os_user", "concurrency"}
    cluster_ids: list[str] = []
    for i, cluster in enumerate(clusters):
        if not isinstance(cluster, dict) or set(cluster) != required_cluster:
            raise ConfigError(f"physical_clusters[{i}] must contain exactly {sorted(required_cluster)}")
        cid = cluster["id"]
        if not isinstance(cid, str) or not ID_RE.fullmatch(cid):
            raise ConfigError(f"physical_clusters[{i}].id is invalid")
        cluster_ids.append(cid)
        for key in ("name", "cloud", "connection_ref", "credential_file", "os_user"):
            if not isinstance(cluster[key], str) or not cluster[key].strip():
                raise ConfigError(f"physical_clusters[{i}].{key} must be a non-empty string")
        concurrency = cluster["concurrency"]
        if not isinstance(concurrency, dict) or set(concurrency) != set(POOL_KINDS):
            raise ConfigError(f"physical_clusters[{i}].concurrency must contain exactly {list(POOL_KINDS)}")
        for kind, limit in concurrency.items():
            if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
                raise ConfigError(f"physical_clusters[{i}].concurrency.{kind} must be a positive integer")
    if len(cluster_ids) != len(set(cluster_ids)):
        raise ConfigError("physical cluster ids must be unique")
    if not isinstance(pairs, list) or not pairs:
        raise ConfigError("cluster_pairs must be a non-empty list")
    required_pair = {"id", "name", "source_cluster_id", "target_cluster_id"}
    pair_ids: list[str] = []
    assigned: list[str] = []
    for i, pair in enumerate(pairs):
        if not isinstance(pair, dict) or set(pair) != required_pair:
            raise ConfigError(f"cluster_pairs[{i}] must contain exactly {sorted(required_pair)}")
        pid = pair["id"]
        if not isinstance(pid, str) or not ID_RE.fullmatch(pid):
            raise ConfigError(f"cluster_pairs[{i}].id is invalid")
        if not isinstance(pair["name"], str) or not pair["name"].strip():
            raise ConfigError(f"cluster_pairs[{i}].name must be a non-empty string")
        source, target = pair["source_cluster_id"], pair["target_cluster_id"]
        if source == target or source not in cluster_ids or target not in cluster_ids:
            raise ConfigError(f"cluster_pairs[{i}] must reference two different physical clusters")
        pair_ids.append(pid)
        assigned.extend((source, target))
    if len(pair_ids) != len(set(pair_ids)):
        raise ConfigError("cluster pair ids must be unique")
    if len(assigned) != len(set(assigned)):
        raise ConfigError("a physical cluster can belong to only one cluster pair")
    if set(assigned) != set(cluster_ids):
        raise ConfigError("every physical cluster must belong to exactly one cluster pair")
    for path, want in _FIXED.items():
        v: Any = cfg
        for p in path:
            v = v[p]
        if v != want:
            raise ConfigError(f"{'.'.join(path)} must be {want!r} in v1.4, got {v!r}")
    if cfg["deployment"]["mode"] not in ("CENTRAL", "LOCAL"):
        raise ConfigError("deployment.mode must be CENTRAL or LOCAL")
    for tzpath in (("execution_window", "timezone"), ("schedule_defaults", "timezone")):
        try:
            ZoneInfo(cfg[tzpath[0]][tzpath[1]])
        except Exception as e:
            raise ConfigError(f"{'.'.join(tzpath)}: invalid IANA timezone") from e
    w = cfg["execution_window"]
    o, d, c = (_hhmm(w["open_at"], "open_at"), _hhmm(w["drain_at"], "drain_at"), _hhmm(w["close_at"], "close_at"))
    span = lambda a, b: (b - a) % 1440
    if not (0 < span(o, d) <= span(o, c) and span(o, c) > 0):
        raise ConfigError("execution_window must satisfy open_at < drain_at <= close_at (may wrap midnight)")
    if w["predict_factor"] < 1.0:
        raise ConfigError("execution_window.predict_factor must be >= 1")
    if not 0 <= cfg["scheduler"]["default_priority"] <= 100:
        raise ConfigError("scheduler.default_priority must be 0..100")
    if cfg["scope"]["new_scope_policy"] not in ("REQUIRE_CONFIRM", "AUTO_ADD_COMPATIBLE"):
        raise ConfigError("scope.new_scope_policy must be REQUIRE_CONFIRM or AUTO_ADD_COMPATIBLE")
    sd = cfg["checks"]["sample"]["default_seed_hex"]
    if not re.fullmatch(r"[0-9a-f]{32}", sd):
        raise ConfigError("checks.sample.default_seed_hex must be 32 lowercase hex characters")
    if not set(cfg["checks"]["sample"]["builtin_key_contract"]) <= {"NON_NULL_UNIQUE_INTEGER", "NON_NULL_UNIQUE_STRING"}:
        raise ConfigError("checks.sample.builtin_key_contract has unknown contract")
    kinds = cfg["checks"]["default_check_selector"].get("kinds", [])
    if not kinds or not set(kinds) <= {"COUNT", "METRIC", "SAMPLE"}:
        raise ConfigError("checks.default_check_selector.kinds must be a subset of COUNT/METRIC/SAMPLE")
    if set(cfg["checks"]["default_check_selector"]) - {"kinds", "rule_keys"}:
        raise ConfigError("checks.default_check_selector accepts kinds/rule_keys only")
    if not set(cfg["notification"]["channels"]) <= {"DASHBOARD", "DAILY_REPORT"}:
        raise ConfigError("notification.channels: only DASHBOARD and DAILY_REPORT are allowed (no outbound messages)")
    if not set(cfg["report"]["formats"]) <= {"MARKDOWN", "CSV_UTF8_BOM"}:
        raise ConfigError("report.formats: only MARKDOWN and CSV_UTF8_BOM")
    if cfg["controller"]["worker_api_bind"] not in ("127.0.0.1", "::1", "localhost") \
            and cfg["deployment"]["mode"] == "CENTRAL":
        raise ConfigError("controller.worker_api_bind must be a loopback address in CENTRAL mode")
    if not set(cfg["transfer"]["globally_approved_classes"]) <= {"SUMMARY_ONLY", "AGGREGATES_APPROVED",
                                                               "SAMPLE_VALUES_APPROVED"}:
        raise ConfigError("transfer.globally_approved_classes cannot include DETAIL_APPROVED")
    if cfg["credentials"]["controller_pg"]["mode"] != "0600":
        raise ConfigError("credentials.controller_pg.mode must be 0600")
    if cfg["execution"]["heartbeat_seconds"] * 2 > cfg["execution"]["lease_seconds"]:
        raise ConfigError("execution.heartbeat_seconds must be at most half of lease_seconds")
    for k in ("lease_seconds", "heartbeat_seconds", "batch_rows"):
        if cfg["execution"][k] <= 0:
            raise ConfigError(f"execution.{k} must be positive")
    if not cfg["error_classification"]["storage_suspect_patterns"]:
        raise ConfigError("error_classification.storage_suspect_patterns must not be empty")


def from_dict(data: dict | None) -> Config:
    cfg = _merge(DEFAULTS, data or {}, ())
    validate(cfg)
    return Config(cfg)


def load(path: str | Path) -> Config:
    text = Path(path).read_text(encoding="utf-8")
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise ConfigError("config root must be a mapping")
    return from_dict(data)
