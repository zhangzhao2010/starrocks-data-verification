"""Shared controller plumbing: service context, errors, clock, audit, incidents, the global control row."""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..common import canonical as c
from ..config import PROTOCOL_VERSION, Config
from ..db.db import Database, jb, new_id

KIND_RANK = {"COUNT": 0, "METRIC": 1, "SAMPLE": 2, "DEEP": 3}
LAYER_RANK = {"ODS": 0, "DWD": 1, "DWS": 2, "ADS": 3, "DIM": 4, "OTHER": 5}
TRANSFER_CLASS = {"COUNT": "SUMMARY_ONLY", "METRIC": "AGGREGATES_APPROVED", "SAMPLE": "SAMPLE_VALUES_APPROVED",
                  "DEEP": "DETAIL_APPROVED"}
LIVE_ATTEMPT = ("LEASED", "RUNNING", "CANCEL_REQUESTED", "CANCEL_PENDING")
COVERAGE_OF_VERDICT = {"OBSERVED_EQUAL": "COMPLETED_EQUAL", "OBSERVED_DIFFERENT": "COMPLETED_DIFFERENT",
                       "INCONCLUSIVE": "INCONCLUSIVE", "ERROR": "ERROR", "UNSUPPORTED": "UNSUPPORTED"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, detail: Any = None):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.detail = detail

    def body(self) -> dict:
        out = {"code": self.code, "message": self.message}
        if self.detail is not None:
            out["detail"] = self.detail
        return out


class Clock:
    """Wall clock used for windows, schedules and reports. Leases always use the PG clock."""

    def now(self) -> _dt.datetime:
        return _dt.datetime.now(_dt.timezone.utc)


class FixedClock(Clock):
    def __init__(self, at: _dt.datetime):
        self._at = at
        self._lock = threading.Lock()

    def now(self) -> _dt.datetime:
        with self._lock:
            return self._at

    def set(self, at: _dt.datetime) -> None:
        with self._lock:
            self._at = at

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._at = self._at + _dt.timedelta(seconds=seconds)


@dataclass
class Ctx:
    cfg: Config
    db: Database
    clock: Clock = field(default_factory=Clock)
    data_dir: Path | None = None

    def __post_init__(self):
        if self.data_dir is None:
            self.data_dir = Path(self.cfg["controller"]["data_dir"])
        self.data_dir = Path(self.data_dir)

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def material_dir(self) -> Path:
        return self.data_dir / "material"

    def now(self) -> _dt.datetime:
        return self.clock.now()


def hexd(b: bytes | memoryview | None) -> str | None:
    return None if b is None else bytes(b).hex()


def digest_json(obj: Any) -> bytes:
    """sha256 over deterministic JSON that tolerates floats (for preview/idempotency digests, not protocol)."""
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                                     default=str).encode("utf-8")).digest()


def as_uuid(v: Any, name: str = "id") -> uuid.UUID:
    try:
        return v if isinstance(v, uuid.UUID) else uuid.UUID(str(v))
    except (ValueError, TypeError, AttributeError) as e:
        raise ApiError(422, "INVALID_ID", f"{name} is not a UUID") from e


def audit(cur, event_type: str, object_type: str, object_id: Any = None, payload: dict | None = None,
          reason: str | None = None, actor: str = "local-user") -> None:
    cur.execute("INSERT INTO dq_outbox (event_type, object_type, object_id, payload, reason, actor) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (event_type, object_type, None if object_id is None else str(object_id), jb(payload or {}),
                 reason, actor))


def incident(cur, kind: str, dedup_key: str, message: str, *, cluster_id: str | None = None,
             severity: str = "WARN", detail: dict | None = None, **fields: Any) -> None:
    cols = ["run_id", "run_check_id", "asset_id", "rule_key", "check_kind", "scope_key", "error_code"]
    vals = [fields.get(k) for k in cols]
    cur.execute(
        f"""INSERT INTO dq_incident (id, kind, dedup_key, cluster_id, severity, message, detail, {', '.join(cols)})
            VALUES (%s, %s, %s, %s, %s, %s, %s, {', '.join(['%s'] * len(cols))})
            ON CONFLICT (kind, dedup_key) WHERE state = 'OPEN'
            DO UPDATE SET occurrences = dq_incident.occurrences + 1, last_seen = now(),
                          message = EXCLUDED.message, detail = EXCLUDED.detail""",
        [new_id(), kind, dedup_key, cluster_id, severity, message[:2000], jb(detail or {})] + vals)


def close_incident(cur, kind: str, dedup_key: str) -> None:
    cur.execute("UPDATE dq_incident SET state='CLOSED', last_seen=now() WHERE kind=%s AND dedup_key=%s "
                "AND state='OPEN'", (kind, dedup_key))


def ensure_control(ctx: Ctx) -> dict:
    """Seed global control, physical clusters and fixed verification pairs. Idempotent."""
    cfg = ctx.cfg
    roles = {p["source_cluster_id"]: "SOURCE" for p in cfg["cluster_pairs"]}
    roles.update({p["target_cluster_id"]: "TARGET" for p in cfg["cluster_pairs"]})
    with ctx.db.tx() as cur:
        cur.execute("INSERT INTO dq_control_state (id, control_generation, transfer_approval_ref) "
                    "VALUES (1, %s, %s) ON CONFLICT (id) DO NOTHING",
                    (new_id(), cfg["transfer"]["global_approval_ref"] if cfg.transfer_approved else None))
        cur.execute("SELECT * FROM dq_control_state WHERE id=1 FOR UPDATE")
        ctl = cur.fetchone()
        ref = cfg["transfer"]["global_approval_ref"] if cfg.transfer_approved else None
        if ctl["transfer_approval_ref"] != ref:
            cur.execute("UPDATE dq_control_state SET transfer_approval_ref=%s, revision=revision+1, updated_at=now() "
                        "WHERE id=1", (ref,))
            audit(cur, "TRANSFER_APPROVAL_CHANGED", "control", 1, {"approval_ref": ref,
                                                                   "previous": ctl["transfer_approval_ref"]})
        for cluster in cfg["physical_clusters"]:
            cluster_id = cluster["id"]
            cur.execute("""INSERT INTO dq_source (id, cluster_id, name, cloud, role, connection_ref)
                           VALUES (%s, %s, %s, %s, %s, %s)
                           ON CONFLICT (cluster_id) DO UPDATE SET name=EXCLUDED.name, cloud=EXCLUDED.cloud,
                           role=EXCLUDED.role, connection_ref=EXCLUDED.connection_ref, updated_at=now()""",
                        (new_id(), cluster_id, cluster["name"], cluster["cloud"], roles[cluster_id],
                         cluster["connection_ref"]))
        configured_pairs = {p["id"] for p in cfg["cluster_pairs"]}
        cur.execute("SELECT id FROM dq_cluster_pair FOR UPDATE")
        stale = {r["id"] for r in cur.fetchall()} - configured_pairs
        if stale:
            cur.execute("SELECT pair_id, count(*) AS n FROM dq_asset WHERE pair_id = ANY(%s) GROUP BY pair_id",
                        (list(stale),))
            used = {r["pair_id"]: int(r["n"]) for r in cur.fetchall()}
            if used:
                raise ApiError(409, "STALE_CLUSTER_PAIRS",
                               f"unconfigured cluster pairs still own assets: {used}; migrate those assets first")
            cur.execute("DELETE FROM dq_cluster_pair WHERE id = ANY(%s)", (list(stale),))
        for pair in cfg["cluster_pairs"]:
            cur.execute("""INSERT INTO dq_cluster_pair (id, name, source_cluster_id, target_cluster_id)
                           VALUES (%s, %s, %s, %s)
                           ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name,
                           source_cluster_id=EXCLUDED.source_cluster_id,
                           target_cluster_id=EXCLUDED.target_cluster_id, updated_at=now()""",
                        (pair["id"], pair["name"], pair["source_cluster_id"], pair["target_cluster_id"]))
        cur.execute("SELECT * FROM dq_control_state WHERE id=1")
        return cur.fetchone()


def lock_control(cur) -> dict:
    cur.execute("SELECT * FROM dq_control_state WHERE id=1 FOR UPDATE")
    row = cur.fetchone()
    if row is None:
        raise ApiError(503, "NOT_INITIALIZED", "control state is not initialized")
    return row


def control(ctx: Ctx) -> dict:
    row = ctx.db.one("SELECT * FROM dq_control_state WHERE id=1")
    if row is None:
        raise ApiError(503, "NOT_INITIALIZED", "control state is not initialized")
    return row


def pg_now(cur) -> _dt.datetime:
    cur.execute("SELECT now() AS n")
    return cur.fetchone()["n"]


def transfer_allowed(cfg: Config, transfer_class: str, detail_ref: str | None = None) -> bool:
    if not cfg.transfer_approved:
        return False
    if transfer_class == "DETAIL_APPROVED":
        return bool(detail_ref)
    return transfer_class in cfg["transfer"]["globally_approved_classes"]


def rule_definition_digest(check_kind: str, definition: dict, execution_policy: str, transfer_class: str) -> bytes:
    return c.json_digest({"format_version": "check-rule-v1.3", "check_kind": check_kind, "definition": definition,
                          "execution_policy": execution_policy, "transfer_class": transfer_class})


def protocol_ok(v: Any) -> None:
    if v != PROTOCOL_VERSION:
        raise ApiError(422, "PROTOCOL_VERSION", f"protocol_version must be {PROTOCOL_VERSION}")


def iso(t: _dt.datetime | None) -> str | None:
    return None if t is None else t.isoformat()


def parse_ts(s: Any, name: str = "timestamp") -> _dt.datetime:
    if isinstance(s, _dt.datetime):
        return s if s.tzinfo else s.replace(tzinfo=_dt.timezone.utc)
    try:
        t = _dt.datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except ValueError as e:
        raise ApiError(422, "INVALID_TIMESTAMP", f"{name} is not an ISO timestamp") from e
    if t.tzinfo is None:
        raise ApiError(422, "INVALID_TIMESTAMP", f"{name} must carry a timezone offset")
    return t
