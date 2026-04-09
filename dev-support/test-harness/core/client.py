"""HTTP client with latency tracking for Atlas REST API."""

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests


@dataclass
class ApiResponse:
    status_code: int
    body: Any
    headers: Dict[str, str]
    latency_ms: float
    request_method: str
    request_path: str

    @property
    def ok(self):
        return 200 <= self.status_code < 300

    def json(self):
        return self.body


class AtlasClient:
    """HTTP client wrapping requests with auth, latency tracking, and 401 retry."""

    def __init__(self, api_base, admin_base, auth_provider, timeout=30, request_logger=None):
        self.api_base = api_base.rstrip("/")
        self.admin_base = admin_base.rstrip("/")
        self.auth = auth_provider
        self.timeout = timeout
        self.latency_log: List[Dict] = []
        self.request_logger = request_logger

    def get(self, path, params=None, admin=False, timeout=None, retries=None) -> ApiResponse:
        return self._do("GET", path, params=params, admin=admin, timeout=timeout, retries=retries)

    def post(self, path, json_data=None, params=None, admin=False, timeout=None, retries=None) -> ApiResponse:
        return self._do("POST", path, json_data=json_data, params=params, admin=admin, timeout=timeout, retries=retries)

    def put(self, path, json_data=None, params=None, admin=False, timeout=None, retries=None) -> ApiResponse:
        return self._do("PUT", path, json_data=json_data, params=params, admin=admin, timeout=timeout, retries=retries)

    def delete(self, path, json_data=None, params=None, admin=False, timeout=None, retries=None) -> ApiResponse:
        return self._do("DELETE", path, json_data=json_data, params=params, admin=admin, timeout=timeout, retries=retries)

    def _do(self, method, path, json_data=None, params=None, admin=False, timeout=None, retries=None) -> ApiResponse:
        base = self.admin_base if admin else self.api_base
        url = f"{base}{path}"
        effective_timeout = timeout if timeout is not None else self.timeout

        headers = self.auth.get_headers()
        auth_obj = self.auth.get_requests_auth()

        # Retry config: caller can override with retries=N (0 = no retries, 1 attempt only)
        if retries is not None:
            max_attempts = retries + 1  # retries=0 means 1 attempt, retries=2 means 3 attempts
        else:
            max_attempts = 3 if method in ("POST", "PUT", "DELETE") else (2 if method == "GET" else 1)
        last_api_resp = None

        for attempt in range(max_attempts):
            # Escalate timeout on retries: 1x, 2x, 3x
            attempt_timeout = effective_timeout * (attempt + 1) if attempt > 0 else effective_timeout

            start = time.perf_counter()
            try:
                resp = requests.request(
                    method, url,
                    headers=headers,
                    auth=auth_obj,
                    json=json_data,
                    params=params,
                    timeout=attempt_timeout,
                )
            except requests.exceptions.Timeout:
                latency_ms = (time.perf_counter() - start) * 1000
                last_api_resp = ApiResponse(
                    status_code=408,
                    body={"error": "Request timed out"},
                    headers={},
                    latency_ms=latency_ms,
                    request_method=method,
                    request_path=path,
                )
                self.latency_log.append({
                    "method": method, "path": path,
                    "status": 408, "latency_ms": latency_ms,
                })
                if self.request_logger:
                    self.request_logger.log(method, path, params, json_data, 408, last_api_resp.body, latency_ms)
                # Don't retry POST/PUT/DELETE on timeout — server may still
                # be processing (non-idempotent). Only retry GET on timeout.
                if method == "GET" and attempt < max_attempts - 1:
                    print(f"  [client] {method} {path} timed out ({attempt_timeout}s), "
                          f"retrying ({attempt+1}/{max_attempts})...")
                    time.sleep(2 * (attempt + 1))
                    continue
                if method != "GET":
                    print(f"  [client] {method} {path} timed out ({attempt_timeout}s), "
                          f"not retrying (server may still be processing)")
                return last_api_resp

            # 401 retry
            if resp.status_code == 401 and self.auth.handle_401():
                headers = self.auth.get_headers()
                resp = requests.request(
                    method, url,
                    headers=headers,
                    auth=auth_obj,
                    json=json_data,
                    params=params,
                    timeout=attempt_timeout,
                )

            latency_ms = (time.perf_counter() - start) * 1000

            # Parse body
            try:
                body = resp.json()
            except (ValueError, requests.exceptions.JSONDecodeError):
                body = resp.text

            api_resp = ApiResponse(
                status_code=resp.status_code,
                body=body,
                headers=dict(resp.headers),
                latency_ms=latency_ms,
                request_method=method,
                request_path=path,
            )

            self.latency_log.append({
                "method": method,
                "path": path,
                "status": resp.status_code,
                "latency_ms": latency_ms,
            })

            if self.request_logger:
                self.request_logger.log(method, path, params, json_data, resp.status_code, body, latency_ms)

            # Retry on transient errors: 429 (rate limit), 500, 502, 503
            if resp.status_code in (429, 500, 502, 503) and attempt < max_attempts - 1:
                # Rate limit: use Retry-After header or default 10s; server errors: 5s * attempt
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 10))
                    wait = min(retry_after, 30)
                else:
                    wait = 5 * (attempt + 1)
                print(f"  [client] {method} {path} returned {resp.status_code}, "
                      f"waiting {wait}s then retrying ({attempt+1}/{max_attempts})...")
                time.sleep(wait)
                last_api_resp = api_resp
                continue

            return api_resp

        return last_api_resp
