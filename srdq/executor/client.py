"""HTTP client for the controller's worker API (side-bound bearer token)."""
from __future__ import annotations

from pathlib import Path

import httpx

PROTOCOL_VERSION = "srdq-v1.3"


class ApiRejected(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message


class WorkerClient:
    def __init__(self, base_url: str, token: str, *, timeout: float = 30.0, transport=None):
        self._http = httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout, transport=transport,
                                  headers={"Authorization": f"Bearer {token}"})

    def close(self) -> None:
        self._http.close()

    def _post(self, path: str, body: dict) -> dict:
        body = {"protocol_version": PROTOCOL_VERSION, **body}
        r = self._http.post(path, json=body)
        return self._handle(r)

    @staticmethod
    def _handle(r: httpx.Response) -> dict:
        if r.status_code >= 400:
            try:
                d = r.json()
                code, msg = d.get("code", "ERROR"), d.get("message", r.text)
            except Exception:
                code, msg = "ERROR", r.text
            raise ApiRejected(r.status_code, code, msg)
        return r.json()

    def register(self, boot_id: str, side: str, capabilities: dict) -> dict:
        return self._post("/v1/worker/registrations", {"boot_id": boot_id, "side": side, "capabilities": capabilities})

    def claim(self, registration_id: str, control_generation: str, max_items: int) -> dict:
        return self._post("/v1/worker/claims", {"registration_id": registration_id,
                                                "control_generation": control_generation, "max_items": max_items})

    def heartbeat(self, registration_id: str, control_generation: str, attempts: list[dict],
                  signals: dict | None = None) -> dict:
        return self._post("/v1/worker/heartbeats", {"registration_id": registration_id,
                                                    "control_generation": control_generation,
                                                    "attempts": attempts, "signals": signals})

    def upload_chunk(self, attempt_id: str, node_id: str, chunk_no: int, path: Path, sha256: str) -> dict:
        with open(path, "rb") as f:
            data = f.read()
        r = self._http.put(f"/v1/worker/result-chunks/{attempt_id}/{node_id}/{chunk_no}", content=data,
                           headers={"X-Content-SHA256": sha256, "Content-Type": "application/octet-stream"})
        return self._handle(r)

    def result(self, envelope: dict) -> dict:
        return self._post("/v1/worker/results", envelope)

    def failure(self, body: dict) -> dict:
        return self._post("/v1/worker/failures", body)

    def cancelled(self, body: dict) -> dict:
        return self._post("/v1/worker/cancellations", body)
