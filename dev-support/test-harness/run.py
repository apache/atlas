#!/usr/bin/env python3
"""
Atlas Metastore REST API Test Harness

Exercises ~220 REST API endpoints as a black box. Self-contained:
creates test data, runs tests, cleans up.

Usage:
  python3 run.py                              # local dev, all suites
  python3 run.py --tenant staging             # staging, all suites
  python3 run.py --suite admin entity_crud    # specific suites
  python3 run.py --tag smoke                  # smoke tests only
  python3 run.py -v -o report.json            # verbose + JSON report
"""

import os
import shutil
import signal
import sys
import time

# Clear __pycache__ to avoid running stale bytecode after edits
_harness_root = os.path.dirname(os.path.abspath(__file__))
for _dirpath, _dirnames, _ in os.walk(_harness_root):
    if "__pycache__" in _dirnames:
        shutil.rmtree(os.path.join(_dirpath, "__pycache__"), ignore_errors=True)
        _dirnames.remove("__pycache__")

# Ensure the harness root is on sys.path
sys.path.insert(0, _harness_root)

from core.config import parse_args
from core.auth import build_auth_provider
from core.client import AtlasClient
from core.context import TestContext
from core.reporter import Reporter
from core import runner


def main():
    config = parse_args()

    print(f"Atlas API Test Harness")
    print(f"  Environment: {config.env}")
    print(f"  API base:    {config.api_base}")
    print(f"  Admin base:  {config.admin_base}")
    if config.suites:
        print(f"  Suites:      {config.suites}")
    if config.tags:
        print(f"  Tags:        {config.tags}")
    if config.exclude_tags:
        print(f"  Exclude:     {config.exclude_tags}")
    if config.parallel > 1:
        print(f"  Parallel:    {config.parallel} concurrent suites")
    print()

    # Build auth
    auth = build_auth_provider(config)

    # Trace logger (optional)
    request_logger = None
    if config.trace_log:
        from core.request_logger import RequestLogger
        trace_path = config.trace_log
        if trace_path == "auto":
            trace_path = RequestLogger.generate_filename()
        request_logger = RequestLogger(trace_path)
        print(f"  Trace log:   {os.path.abspath(trace_path)}")

    # Build client
    client = AtlasClient(
        api_base=config.api_base,
        admin_base=config.admin_base,
        auth_provider=auth,
        timeout=config.timeout,
        request_logger=request_logger,
    )

    # Shared context
    ctx = TestContext()
    ctx.set("config", config)
    ctx.set("es_sync_wait", config.es_sync_wait)
    if request_logger:
        ctx.set("request_logger", request_logger)

    # Kafka verifier (optional)
    if config.no_kafka:
        print("  Kafka:       disabled (--no-kafka)")
    elif config.kafka_bootstrap_servers:
        from core.kafka_helpers import KafkaVerifier
        kafka = KafkaVerifier(config.kafka_bootstrap_servers)
        ctx.set("kafka_verifier", kafka)

    # Reporter
    reporter = Reporter(verbose=config.verbose, output_file=config.output_file,
                        html_file=config.html_report, tenant=config.tenant)

    # Signal handler for graceful cleanup
    def handle_signal(signum, frame):
        print(f"\nInterrupted (signal {signum}). Running cleanup...")
        if not config.skip_cleanup:
            errors = ctx.run_cleanup(client=client)
            if errors:
                print(f"  Cleanup errors: {len(errors)}")
        reporter.print_summary()
        reporter.write_json_report(client.latency_log)
        reporter.write_html_report(client.latency_log)
        sys.exit(1)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Preflight: verify Atlas is reachable
    try:
        resp = client.get("/status", admin=True)
        if resp.status_code == 200:
            body = resp.body
            status = body.get("Status", "UNKNOWN") if isinstance(body, dict) else "UNKNOWN"
            print(f"  Atlas status: {status}")
        else:
            print(f"  WARNING: Atlas status endpoint returned {resp.status_code}")
    except Exception as e:
        print(f"  ERROR: Cannot reach Atlas at {config.atlas_url}: {e}")
        sys.exit(1)

    # Cleanup mode — delete artifacts from previous runs, then exit
    if config.cleanup:
        from core.cleanup import run_cleanup_all
        run_cleanup_all(client, verbose=config.verbose)
        sys.exit(0)

    # Run tests
    try:
        runner.run(client, ctx, reporter, config)
    except Exception as e:
        print(f"\nFatal error during test run: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Print results first
        reporter.print_summary()

        # Cleanup
        if not config.skip_cleanup:
            print("\nRunning cleanup...")
            errors = ctx.run_cleanup(client=client)
            if errors:
                print(f"  Cleanup completed with {len(errors)} errors")
                if config.verbose:
                    for err in errors:
                        print(f"    - {err}")
            else:
                print("  Cleanup complete")
        else:
            print("\nSkipping cleanup (--skip-cleanup)")

        # Close Kafka verifier
        kafka_verifier = ctx.get("kafka_verifier")
        if kafka_verifier:
            kafka_verifier.close()

        # Close trace logger
        if request_logger:
            request_logger.close()

        # Write reports
        reporter.write_json_report(client.latency_log)
        reporter.write_html_report(client.latency_log)

    sys.exit(0 if reporter.all_passed else 1)


if __name__ == "__main__":
    main()
