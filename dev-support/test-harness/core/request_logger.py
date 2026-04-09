"""JSONL trace logger for API requests and responses."""

import json
import os
from datetime import datetime


class RequestLogger:
    """Logs every API call as a JSONL line to a timestamped trace file."""

    def __init__(self, file_path):
        self._file = open(file_path, "a", encoding="utf-8")
        self._suite = None
        self._test = None

    def set_context(self, suite, test):
        self._suite = suite
        self._test = test

    def log(self, method, path, params, request_body, status, response_body, latency_ms):
        entry = {
            "ts": datetime.now().isoformat(timespec="milliseconds"),
            "suite": self._suite,
            "test": self._test,
            "method": method,
            "path": path,
            "request_params": params,
            "request_body": request_body,
            "status": status,
            "response_body": response_body,
            "latency_ms": round(latency_ms, 1),
        }
        line = json.dumps(entry, default=str, ensure_ascii=False)
        self._file.write(line + "\n")
        self._file.flush()

    def close(self):
        if self._file and not self._file.closed:
            self._file.flush()
            self._file.close()

    @staticmethod
    def generate_filename():
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"api_trace_{ts}.jsonl"
