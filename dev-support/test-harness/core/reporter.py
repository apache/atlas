"""Console + JSON reporting with latency statistics."""

import json
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


# ANSI colors
_GREEN = "\033[92m"
_RED = "\033[91m"
_YELLOW = "\033[93m"
_CYAN = "\033[96m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


@dataclass
class TestResult:
    suite: str
    test_name: str
    status: str                 # PASS, FAIL, SKIP, ERROR
    latency_ms: float = 0.0
    error_message: str = ""
    error_type: str = ""
    api_calls: List[Dict] = field(default_factory=list)


class Reporter:
    """Collects test results and produces console + JSON output."""

    def __init__(self, verbose=False, output_file=None, tenant=None, html_file=None):
        self.verbose = verbose
        self.output_file = output_file
        self.html_file = html_file
        self.tenant = tenant
        self.results: List[TestResult] = []
        self.start_time = time.time()
        self._current_suite = None
        self._lock = threading.Lock()

    def suite_start(self, suite_name, description=""):
        self._current_suite = suite_name
        print(f"\n{_BOLD}{_CYAN}--- {suite_name} ---{_RESET}", flush=True)
        if description and self.verbose:
            print(f"    {description}", flush=True)

    def suite_end(self, suite_name):
        results = [r for r in self.results if r.suite == suite_name]
        passed = sum(1 for r in results if r.status == "PASS")
        failed = sum(1 for r in results if r.status == "FAIL")
        errors = sum(1 for r in results if r.status == "ERROR")
        skipped = sum(1 for r in results if r.status == "SKIP")
        total = len(results)
        color = _GREEN if failed == 0 and errors == 0 else _RED
        print(
            f"    {color}{passed}/{total} passed"
            f"{f', {failed} failed' if failed else ''}"
            f"{f', {errors} errors' if errors else ''}"
            f"{f', {skipped} skipped' if skipped else ''}"
            f"{_RESET}",
            flush=True,
        )

    def record(self, result: TestResult):
        with self._lock:
            self.results.append(result)
        self._print_result(result)

    def replay_results(self, results):
        """Print previously collected results (for parallel/deferred output)."""
        for result in results:
            with self._lock:
                self.results.append(result)
            self._print_result(result)

    def _print_result(self, result):
        """Print a single test result to console."""
        status = result.status
        if status == "PASS":
            icon = f"{_GREEN}PASS{_RESET}"
        elif status == "FAIL":
            icon = f"{_RED}FAIL{_RESET}"
        elif status == "ERROR":
            icon = f"{_RED}ERROR{_RESET}"
        else:
            icon = f"{_YELLOW}SKIP{_RESET}"

        latency_str = f" ({result.latency_ms:.0f}ms)" if result.latency_ms > 0 else ""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"  [{ts}] [{icon}] {result.suite}::{result.test_name}{latency_str}", flush=True)

        if result.error_message and (status in ("FAIL", "ERROR") or self.verbose):
            for line in result.error_message.split("\n")[:5]:
                print(f"         {_RED}{line}{_RESET}", flush=True)

    def print_summary(self):
        elapsed = time.time() - self.start_time
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == "PASS")
        failed = sum(1 for r in self.results if r.status == "FAIL")
        errors = sum(1 for r in self.results if r.status == "ERROR")
        skipped = sum(1 for r in self.results if r.status == "SKIP")

        print(f"\n{'=' * 60}")
        color = _GREEN if failed == 0 and errors == 0 else _RED
        print(f"{_BOLD}TEST SUMMARY{_RESET}")
        if self.tenant:
            print(f"  Tenant:  {self.tenant}")
        print(f"  Total:   {total}")
        print(f"  {_GREEN}Passed:  {passed}{_RESET}")
        if failed:
            print(f"  {_RED}Failed:  {failed}{_RESET}")
        if errors:
            print(f"  {_RED}Errors:  {errors}{_RESET}")
        if skipped:
            print(f"  {_YELLOW}Skipped: {skipped}{_RESET}")
        print(f"  Time:    {elapsed:.1f}s")

        # List failures
        failures = [r for r in self.results if r.status in ("FAIL", "ERROR")]
        if failures:
            tenant_label = f" [{self.tenant}]" if self.tenant else ""
            print(f"\n{_RED}{_BOLD}Failures{tenant_label}:{_RESET}")
            for r in failures:
                print(f"  - {r.suite}::{r.test_name}: {r.error_message[:120]}")

        print(f"\n  Result:  {color}{_BOLD}{'PASS' if not failures else 'FAIL'}{_RESET}")
        print(f"{'=' * 60}")

    def _compute_latency_stats(self, latencies):
        if not latencies:
            return {}
        s = sorted(latencies)
        n = len(s)
        return {
            "count": n,
            "min_ms": round(s[0], 1),
            "max_ms": round(s[-1], 1),
            "avg_ms": round(statistics.mean(s), 1),
            "p50_ms": round(s[n // 2], 1),
            "p95_ms": round(s[int(n * 0.95)], 1) if n >= 20 else round(s[-1], 1),
            "p99_ms": round(s[int(n * 0.99)], 1) if n >= 100 else round(s[-1], 1),
        }

    def _build_report_data(self, client_latency_log=None):
        """Build the report dict used by both JSON and HTML reporters."""
        elapsed = time.time() - self.start_time
        total = len(self.results)
        passed = sum(1 for r in self.results if r.status == "PASS")
        failed = sum(1 for r in self.results if r.status == "FAIL")
        errors = sum(1 for r in self.results if r.status == "ERROR")
        skipped = sum(1 for r in self.results if r.status == "SKIP")

        # Per-suite results
        suites = {}
        for r in self.results:
            if r.suite not in suites:
                suites[r.suite] = {"tests": [], "passed": 0, "failed": 0, "errors": 0, "skipped": 0}
            suites[r.suite]["tests"].append({
                "name": r.test_name,
                "status": r.status,
                "latency_ms": round(r.latency_ms, 1),
                "error": r.error_message if r.status in ("FAIL", "ERROR") else None,
            })
            suites[r.suite][r.status.lower()] = suites[r.suite].get(r.status.lower(), 0) + 1

        # Per-endpoint latency stats
        endpoint_stats = {}
        if client_latency_log:
            by_endpoint = {}
            for entry in client_latency_log:
                key = f"{entry['method']} {entry['path']}"
                by_endpoint.setdefault(key, []).append(entry["latency_ms"])
            for key, latencies in by_endpoint.items():
                endpoint_stats[key] = self._compute_latency_stats(latencies)

        return {
            "timestamp": datetime.now().isoformat(),
            "duration_s": round(elapsed, 1),
            "summary": {
                "total": total,
                "passed": passed,
                "failed": failed,
                "errors": errors,
                "skipped": skipped,
                "pass_rate": f"{passed / total * 100:.1f}%" if total else "N/A",
            },
            "suites": suites,
            "endpoint_latency": endpoint_stats,
        }

    def write_json_report(self, client_latency_log=None):
        if not self.output_file:
            return
        report = self._build_report_data(client_latency_log)
        with open(self.output_file, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\nJSON report written to: {self.output_file}", flush=True)

    def write_html_report(self, client_latency_log=None):
        if not self.html_file:
            return
        from core.html_reporter import generate_html_report
        report = self._build_report_data(client_latency_log)
        generate_html_report(report, self.html_file, tenant=self.tenant)
        print(f"HTML report written to: {self.html_file}", flush=True)

    @property
    def all_passed(self):
        return all(r.status in ("PASS", "SKIP") for r in self.results)
