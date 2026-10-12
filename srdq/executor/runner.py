"""Executor main loop: register, claim, run, upload, report; heartbeat thread handles leases and cancels."""
from __future__ import annotations

import logging
import shutil
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from ..common import canonical as c
from . import probe, tasks
from .adapter.base import AdapterError
from .client import PROTOCOL_VERSION, ApiRejected, WorkerClient
from .encoder import EncodeError
from .outbox import Outbox

log = logging.getLogger("srdq.executor")

REPROBE_SECONDS = 300


@dataclass
class Active:
    item: dict
    cancel: threading.Event = field(default_factory=threading.Event)
    query_ref: dict | None = None
    usage_bytes: int = 0
    usage_seq: int = 0
    lease_lost: bool = False
    cancel_reason: str | None = None

    @property
    def attempt_id(self) -> str:
        return self.item["attempt_id"]


def dir_size(path: Path) -> int:
    total = 0
    if path.exists():
        for p in path.rglob("*"):
            if p.is_file():
                total += p.stat().st_size
    return total


class Executor:
    CLAIM_KINDS = ("COUNT", "METRIC", "SAMPLE", "METADATA", "DEEP")

    def __init__(self, cfg, cluster_id: str, adapter, client: WorkerClient, data_dir: str | Path):
        self.cfg = cfg
        self.cluster_id = cluster_id
        self.adapter = adapter
        self.client = client
        self.data_dir = Path(data_dir)
        self.material_dir = self.data_dir / "material"
        self.material_dir.mkdir(parents=True, exist_ok=True)
        self.outbox = Outbox(self.data_dir / "outbox.sqlite")
        ex = cfg["execution"]
        self.limits = {kind: cfg.concurrency(cluster_id, kind) for kind in self.CLAIM_KINDS}
        self.poll_seconds = float(cfg["executor"]["poll_seconds"])
        self.heartbeat_seconds = float(ex["heartbeat_seconds"])
        self.lease_seconds = float(ex["lease_seconds"])
        self.material_quota = int(ex["local_material_quota_bytes"])
        self.outbox_quota = int(ex["local_outbox_quota_bytes"])
        sig = cfg["load_guard"]["sr_signals"]
        self.signals_enabled = bool(sig["enabled"])
        self.signals_poll = float(sig["poll_seconds"])
        self.boot_id = str(uuid.uuid4())
        self.registration_id: str | None = None
        self.generation: str | None = None
        self.capabilities: dict = {}
        self.active: dict[str, Active] = {}
        self.unconfirmed: dict[str, dict] = {}
        self._lock = threading.Lock()
        self._register_lock = threading.Lock()
        self._flush_lock = threading.Lock()
        self._stop = threading.Event()
        self._pools = {kind: ThreadPoolExecutor(max_workers=self.limits[kind],
                                               thread_name_prefix=f"srdq-{cluster_id}-{kind.lower()}")
                       for kind in self.CLAIM_KINDS}
        self._futures: dict[str, object] = {}
        self._threads: list[threading.Thread] = []
        self._last_hb_ok = time.monotonic()
        self._last_signals = 0.0
        self._pending_signals: dict | None = None
        self._probed_at = 0.0

    # ---- lifecycle -------------------------------------------------------------
    def probe_capabilities(self) -> dict:
        caps = {"adapter": type(self.adapter).__name__, "compiler_release": probe.COMPILER_RELEASE,
                "protocol_version": PROTOCOL_VERSION, "claim_kinds": list(self.CLAIM_KINDS),
                "kind_limits": self.limits}
        try:
            caps["sr_version"] = self.adapter.version()
        except AdapterError as e:
            caps["sr_version"] = None
            caps["version_error"] = {"error_class": e.error_class, "message": str(e)[:300]}
        caps["s1"] = probe.run_probe(self.adapter)
        caps["session_limits"] = ["query_timeout", "query_mem_limit"]
        caps["signals"] = self.signals_enabled
        self._probed_at = time.monotonic()
        return caps

    def register(self, expected: tuple[str | None, str | None] | None = None) -> bool:
        with self._register_lock:
            if expected is not None and (self.registration_id, self.generation) != expected:
                return False
            self.capabilities = self.probe_capabilities()
            resp = self.client.register(self.boot_id, self.cluster_id, self.capabilities)
            self.registration_id = resp["registration_id"]
            self.generation = resp["control_generation"]
            self.lease_seconds = float(resp.get("lease_seconds", self.lease_seconds))
            self.heartbeat_seconds = float(resp.get("heartbeat_seconds", self.heartbeat_seconds))
            self._last_hb_ok = time.monotonic()
            for pending in resp.get("pending", []):
                ref = pending.get("query_ref") or self.outbox.journal_ref(pending["attempt_id"])
                with self._lock:
                    if pending["attempt_id"] not in self.active:
                        self.unconfirmed[pending["attempt_id"]] = {"query_ref": ref, "cancel_sent": False}
            log.info("registered cluster=%s registration=%s s1=%s", self.cluster_id, self.registration_id,
                     self.capabilities["s1"]["status"])
            return True

    def start(self) -> None:
        self.register()
        self.flush_outbox()
        for kind in self.CLAIM_KINDS:
            t = threading.Thread(target=self._claim_loop, args=(kind,),
                                 name=f"srdq-{self.cluster_id}-claim-{kind.lower()}", daemon=True)
            t.start()
            self._threads.append(t)
        heartbeat = threading.Thread(target=self._heartbeat_loop,
                                     name=f"srdq-{self.cluster_id}-heartbeat", daemon=True)
        heartbeat.start()
        self._threads.append(heartbeat)

    def request_stop(self) -> None:
        self._stop.set()

    def stop(self, timeout: float = 30) -> None:
        self._stop.set()
        self._cancel_active("EXECUTOR_STOP")
        for t in self._threads:
            t.join(timeout)
        self._cancel_active("EXECUTOR_STOP")
        for pool in self._pools.values():
            pool.shutdown(wait=True)
        self.outbox.close()
        self.client.close()

    def _cancel_active(self, reason: str) -> None:
        with self._lock:
            acts = list(self.active.values())
        for active in acts:
            active.cancel_reason = active.cancel_reason or reason
            self._request_cancel(active)

    def run_forever(self) -> None:
        delay = 1.0
        while not self._stop.is_set():
            try:
                self.start()
                break
            except httpx.TransportError as e:
                log.warning("controller unreachable (%s); retrying in %.0fs", e, delay)
                self._stop.wait(delay)
                delay = min(delay * 2, 60.0)
        if self._stop.is_set() and not self._threads:
            self.stop()
            return
        try:
            while not self._stop.is_set():
                self._stop.wait(1)
        finally:
            self.stop()

    # ---- claim loop --------------------------------------------------------------
    def _quota_ok(self) -> bool:
        return dir_size(self.material_dir) < self.material_quota and self.outbox.size_bytes() < self.outbox_quota

    def _claim_loop(self, claim_kind: str) -> None:
        while not self._stop.is_set():
            expected = (self.registration_id, self.generation)
            try:
                self.claim_once(claim_kind)
            except ApiRejected as e:
                if e.code in ("REGISTRATION_STALE", "GENERATION_MISMATCH", "UNKNOWN_REGISTRATION"):
                    self._reregister(expected)
                else:
                    log.debug("%s claim rejected: %s", claim_kind, e)
            except Exception as e:  # noqa: BLE001 - keep the loop alive across transient errors
                log.warning("%s claim loop error: %s", claim_kind, e)
            self._stop.wait(self.poll_seconds)

    def _reregister(self, expected: tuple[str | None, str | None]) -> None:
        try:
            self.register(expected)
        except Exception as e:  # noqa: BLE001
            log.warning("re-registration failed: %s", e)

    def claim_once(self, claim_kind: str | None = None) -> int:
        if claim_kind is None:
            return sum(self.claim_once(kind) for kind in self.CLAIM_KINDS)
        if claim_kind not in self.CLAIM_KINDS:
            raise ValueError(f"unknown claim kind {claim_kind}")
        if self.registration_id is None:
            self.register()
        if self.capabilities.get("s1", {}).get("status") == "PROBE_ERROR" \
                and time.monotonic() - self._probed_at > REPROBE_SECONDS:
            self.register((self.registration_id, self.generation))
        self.flush_outbox()
        with self._lock:
            active_for_kind = sum(1 for active in self.active.values() if self._active_kind(active) == claim_kind)
            free = self.limits[claim_kind] - active_for_kind
        if free <= 0 or not self._quota_ok():
            return 0
        resp = self.client.claim(self.registration_id, self.generation, free, claim_kind)
        items = resp.get("items", [])
        for item in items:
            active = Active(item)
            with self._lock:
                self.active[active.attempt_id] = active
            future = self._pools[claim_kind].submit(self._execute, active)
            with self._lock:
                self._futures[active.attempt_id] = future
        return len(items)

    @staticmethod
    def _active_kind(active: Active) -> str:
        return "METADATA" if active.item["kind"] == "METADATA" else active.item["check_kind"]

    # ---- execution -----------------------------------------------------------------
    def _execute(self, a: Active) -> None:
        aid = a.attempt_id
        item = a.item
        spec = item["spec"]
        workdir = self.material_dir / aid
        try:
            if a.cancel.is_set():
                raise tasks.Cancelled()
            if spec["kind"] == "METADATA":
                meta = tasks.run_metadata(spec, self.adapter)
                envelope = self._envelope(item, "METADATA", [], metadata=meta, bytes_read=0)
                self.outbox.put(aid, envelope, None)
            else:
                def on_ref(ref: dict) -> None:
                    self.outbox.journal_start(aid, ref)
                    a.query_ref = ref

                def on_usage(n: int) -> None:
                    a.usage_bytes = n

                outcome = tasks.run_check(spec, self.adapter, workdir, cancel=a.cancel, on_query_ref=on_ref,
                                          usage=on_usage)
                if a.cancel.is_set():
                    raise tasks.Cancelled()
                self._upload(aid, outcome)
                members = [{"node_id": m.node_id, "context_digest_hex": m.context_digest_hex,
                            "evidence": m.evidence, "read_complete": True, "column_complete": True,
                            "read_started": m.read_started, "read_finished": m.read_finished, "payload": m.payload}
                           for m in outcome.members]
                envelope = self._envelope(item, "CHECK", members, bytes_read=outcome.bytes_read)
                self.outbox.put(aid, envelope, str(workdir))
            self.outbox.journal_finish(aid)
            self._send(aid)
        except tasks.Cancelled:
            self._finish_cancel(a)
        except AdapterError as e:
            if a.cancel.is_set() or e.error_class == "CANCELLED":
                self._finish_cancel(a)
            else:
                self._fail(a, e.error_class, e.code, str(e), terminated=True)
        except EncodeError as e:
            self._fail(a, e.error_class, None, str(e), terminated=self._stop_server_query(a))
        except Exception as e:  # noqa: BLE001 - any local fault becomes a reported failure
            log.exception("attempt %s failed locally", aid)
            self._fail(a, "INTERNAL", None, f"{type(e).__name__}: {e}",
                       terminated=self._stop_server_query(a))
        finally:
            with self._lock:
                self.active.pop(aid, None)
                self._futures.pop(aid, None)
            if self.outbox.get(aid) is None or self.outbox.get(aid)["state"] != "PENDING_ACK":
                shutil.rmtree(workdir, ignore_errors=True)

    def _envelope(self, item: dict, result_kind: str, members: list[dict], *, metadata: dict | None = None,
                  bytes_read: int = 0) -> dict:
        spec = item["spec"]
        return {
            "protocol_version": PROTOCOL_VERSION,
            "control_generation": self.generation,
            "registration_id": self.registration_id,
            "work_item_id": item["work_item_id"],
            "attempt_id": item["attempt_id"],
            "lease_generation": item["lease_generation"],
            "observation_id": item.get("observation_id"),
            "run_check_id": item.get("run_check_id"),
            "scan_unit_id": item.get("scan_unit_id"),
            "rule_id": item.get("rule_id"),
            "cluster_id": self.cluster_id,
            "result_kind": result_kind,
            "check_kind": item.get("check_kind"),
            "members": members,
            "metadata": metadata,
            "query_ref": self.outbox.journal_ref(item["attempt_id"]),
            "usage": {"RESULT_BYTES": str(sum(int(m["payload"].get("total_bytes", 0)) for m in members))},
            "bytes_read": str(bytes_read),
            "template_digest": c.json_digest({"compiler": probe.COMPILER_RELEASE, "kind": spec.get("check_kind"),
                                              "rule_digest": spec.get("rule_digest_hex")}).hex(),
        }

    def _upload(self, aid: str, outcome) -> None:
        for m in outcome.members:
            for ch in m.payload.get("chunks_manifest", []):
                path = Path(m.chunk_dir) / f"chunk_{int(ch['chunk_no']):05d}.jsonl"
                self.client.upload_chunk(aid, m.node_id, int(ch["chunk_no"]), path, ch["sha256"])

    def flush_outbox(self) -> None:
        if not self._flush_lock.acquire(blocking=False):
            return
        try:
            for pending in self.outbox.pending():
                self._send(pending["attempt_id"], pending)
        finally:
            self._flush_lock.release()

    def _send(self, aid: str, pending: dict | None = None) -> None:
        if pending is None:
            pending = next((p for p in self.outbox.pending() if p["attempt_id"] == aid), None)
            if pending is None:
                return
        try:
            receipt = self.client.result(pending["envelope"])
            self.outbox.mark(aid, "ACKED", receipt)
        except ApiRejected as e:
            if e.status == 409:
                self.outbox.mark(aid, "STALE_REJECTED", error=f"{e.code}: {e.message}")
            elif 400 <= e.status < 500 and e.status not in (408, 429):
                self.outbox.mark(aid, "QUARANTINED", error=f"{e.code}: {e.message}")
            else:
                log.warning("result for %s not acknowledged yet: %s", aid, e)
                return
        except Exception as e:  # noqa: BLE001 - network: keep PENDING_ACK for resend
            log.warning("result for %s not delivered yet: %s", aid, e)
            return
        if pending.get("material_dir"):
            shutil.rmtree(pending["material_dir"], ignore_errors=True)

    def _fail(self, a: Active, error_class: str, code, message: str, *, terminated: bool) -> None:
        body = {"registration_id": self.registration_id, "attempt_id": a.attempt_id,
                "lease_generation": a.item["lease_generation"], "error_class": error_class,
                "error_code": code, "message": message[:2000], "terminated": terminated,
                "usage": {"RESULT_BYTES": "0", "READ_BYTES": str(a.usage_bytes)}}
        if not terminated:
            with self._lock:
                self.unconfirmed[a.attempt_id] = {"query_ref": a.query_ref, "cancel_sent": True}
        try:
            self.client.failure(body)
        except Exception as e:  # noqa: BLE001 - controller will reap the lease
            log.warning("failure report for %s not delivered: %s", a.attempt_id, e)

    def _stop_server_query(self, a: Active) -> bool:
        if a.query_ref is None:
            return True
        try:
            self.adapter.cancel(a.query_ref)
        except AdapterError:
            pass
        return self._confirm(a.query_ref)

    def _confirm(self, query_ref: dict | None, tries: int = 5) -> bool:
        if query_ref is None:
            return True
        for i in range(tries):
            try:
                if self.adapter.confirm_terminated(query_ref):
                    return True
            except AdapterError:
                pass
            time.sleep(min(0.2 * (i + 1), 1.0))
        return False

    def _request_cancel(self, a: Active) -> None:
        a.cancel.set()
        if a.query_ref is not None:
            try:
                self.adapter.cancel(a.query_ref)
            except AdapterError as e:
                log.warning("cancel of %s failed: %s", a.attempt_id, e)

    def _finish_cancel(self, a: Active) -> None:
        confirmed = self._confirm(a.query_ref)
        if not confirmed:
            with self._lock:
                self.unconfirmed[a.attempt_id] = {"query_ref": a.query_ref, "cancel_sent": True}
            return
        self._post_cancelled(a.attempt_id, a.query_ref)

    def _post_cancelled(self, aid: str, query_ref: dict | None) -> bool:
        try:
            self.client.cancelled({"registration_id": self.registration_id, "attempt_id": aid, "confirmed": True,
                                   "query_ref": query_ref})
            return True
        except ApiRejected as e:
            return e.status in (404, 409, 422)
        except Exception as e:  # noqa: BLE001
            log.warning("cancellation report for %s not delivered: %s", aid, e)
            return False

    # ---- heartbeat ------------------------------------------------------------------
    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.heartbeat_once()
            except Exception as e:  # noqa: BLE001
                log.warning("heartbeat error: %s", e)
            self._stop.wait(self.heartbeat_seconds)

    def heartbeat_once(self) -> None:
        if self.registration_id is None:
            return
        now = time.monotonic()
        signals = None
        if self.signals_enabled and now - self._last_signals >= self.signals_poll:
            self._last_signals = now
            try:
                signals = self.adapter.signals()
            except AdapterError as e:
                signals = {"error_class": e.error_class}
        with self._lock:
            acts = list(self.active.values())
        attempts = []
        for a in acts:
            a.usage_seq += 1
            attempts.append({"attempt_id": a.attempt_id, "lease_generation": a.item["lease_generation"],
                             "query_ref": a.query_ref, "usage": {"READ_BYTES": str(a.usage_bytes)},
                             "usage_seq": a.usage_seq})
        batches = [attempts[i:i + 256] for i in range(0, len(attempts), 256)] or [[]]
        expected = (self.registration_id, self.generation)
        try:
            for index, batch in enumerate(batches):
                resp = self.client.heartbeat(expected[0], expected[1], batch, signals if index == 0 else None)
                self._last_hb_ok = time.monotonic()
                self._apply_directives(resp.get("directives") or {})
        except ApiRejected as e:
            if e.code in ("REGISTRATION_STALE", "GENERATION_MISMATCH", "UNKNOWN_REGISTRATION"):
                for active in acts:
                    active.lease_lost = True
                    self._request_cancel(active)
                self._reregister(expected)
            return
        except Exception:  # noqa: BLE001 - controller unreachable
            if time.monotonic() - self._last_hb_ok > self.lease_seconds:
                for active in acts:
                    active.lease_lost = True
                    active.cancel_reason = "HEARTBEAT_LOST"
                    self._request_cancel(active)
            self._retry_unconfirmed()
            return
        self._retry_unconfirmed()

    def _apply_directives(self, directives: dict[str, str]) -> None:
        for aid, directive in directives.items():
            with self._lock:
                active = self.active.get(aid)
            if active is None or directive not in ("CANCEL", "LEASE_LOST"):
                continue
            active.lease_lost = directive == "LEASE_LOST"
            active.cancel_reason = directive
            self._request_cancel(active)

    def _retry_unconfirmed(self) -> None:
        with self._lock:
            items = list(self.unconfirmed.items())
        for aid, st in items:
            ref = st.get("query_ref")
            if ref is not None and not st.get("cancel_sent"):
                try:
                    self.adapter.cancel(ref)
                except AdapterError:
                    pass
                st["cancel_sent"] = True
            if self._confirm(ref, tries=1) and self._post_cancelled(aid, ref):
                with self._lock:
                    self.unconfirmed.pop(aid, None)
