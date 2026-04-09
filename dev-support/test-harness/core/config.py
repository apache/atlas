"""Configuration management for the test harness."""

import argparse
import os
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class HarnessConfig:
    env: str = "local"                          # "local" or "staging"
    tenant: Optional[str] = None                # e.g. "staging"
    atlas_url: str = "http://localhost:21000"
    api_base: str = "http://localhost:21000/api/atlas/v2"
    admin_base: str = "http://localhost:21000/api/atlas/admin"
    user: str = "admin"
    password: str = "admin"
    creds_file: str = os.environ.get("TOKEN_FILE", os.path.expanduser("~/creds.yaml"))
    suites: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    exclude_tags: List[str] = field(default_factory=list)
    skip_cleanup: bool = False
    cleanup: bool = False
    timeout: int = 30
    es_sync_wait: int = 5
    verbose: bool = False
    output_file: Optional[str] = None
    trace_log: Optional[str] = None
    kafka_bootstrap_servers: Optional[str] = None
    no_kafka: bool = False
    parallel: int = 1                               # max concurrent suites (1 = sequential)
    html_report: Optional[str] = None                  # path for HTML report output


def parse_args() -> HarnessConfig:
    parser = argparse.ArgumentParser(
        description="Atlas Metastore REST API Test Harness",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Local dev (all suites)
  python3 run.py

  # Staging (all suites)
  python3 run.py --tenant staging

  # Specific suites
  python3 run.py --suite admin entity_crud glossary

  # Smoke tests only
  python3 run.py --tag smoke

  # Skip destructive/slow tests
  python3 run.py --exclude-tag slow destructive

  # Leave test data for debugging
  python3 run.py --skip-cleanup

  # Verbose with JSON report
  python3 run.py -v -o report.json
""",
    )
    parser.add_argument("--tenant", default=None,
                        help="Tenant name for OAuth2 (e.g., 'staging')")
    parser.add_argument("--url", default=None,
                        help="Atlas base URL override")
    parser.add_argument("--user", default="admin",
                        help="Basic auth username (local dev, default: admin)")
    parser.add_argument("--password", default="admin",
                        help="Basic auth password (local dev, default: admin)")
    parser.add_argument("--creds-file",
                        default=os.environ.get("TOKEN_FILE", os.path.expanduser("~/creds.yaml")),
                        help="Path to creds.yaml for OAuth2")
    parser.add_argument("--suite", nargs="+", default=[], dest="suites",
                        help="Run only these suites (space-separated names)")
    parser.add_argument("--tag", nargs="+", default=[], dest="tags",
                        help="Run only tests with these tags")
    parser.add_argument("--exclude-tag", nargs="+", default=[], dest="exclude_tags",
                        help="Skip tests with these tags")
    parser.add_argument("--skip-cleanup", action="store_true",
                        help="Leave test data after run")
    parser.add_argument("--cleanup", action="store_true",
                        help="Delete all test-harness artifacts from previous runs (no tests run)")
    parser.add_argument("--timeout", type=int, default=30,
                        help="HTTP request timeout in seconds (default: 30)")
    parser.add_argument("--es-sync-wait", type=int, default=5,
                        help="Seconds to wait for ES sync after writes (default: 5)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Verbose output")
    parser.add_argument("-o", "--output", default=None, dest="output_file",
                        help="Write JSON report to this file")
    parser.add_argument("--trace-log", nargs="?", const="auto", default=None,
                        help="Log full request/response JSON to a JSONL file (default: auto-generated timestamped name)")
    parser.add_argument("--kafka-bootstrap-servers", default=None,
                        help="Kafka bootstrap servers for notification verification (e.g. 'localhost:9092')")
    parser.add_argument("--no-kafka", action="store_true",
                        help="Disable Kafka notification verification entirely")
    parser.add_argument("-P", "--parallel", type=int, default=1,
                        help="Max concurrent suites to run in parallel (default: 1 = sequential)")
    parser.add_argument("--html", default=None, dest="html_report",
                        help="Write HTML report to this file")

    args = parser.parse_args()

    cfg = HarnessConfig()
    cfg.verbose = args.verbose
    cfg.output_file = args.output_file
    cfg.suites = args.suites
    cfg.tags = args.tags
    cfg.exclude_tags = args.exclude_tags
    cfg.skip_cleanup = args.skip_cleanup
    cfg.cleanup = args.cleanup
    cfg.timeout = args.timeout
    cfg.es_sync_wait = args.es_sync_wait
    cfg.creds_file = args.creds_file
    cfg.user = args.user
    cfg.password = args.password
    cfg.trace_log = args.trace_log
    cfg.kafka_bootstrap_servers = args.kafka_bootstrap_servers
    cfg.no_kafka = args.no_kafka
    cfg.parallel = max(1, args.parallel)
    cfg.html_report = args.html_report

    if args.tenant:
        cfg.env = "staging"
        cfg.tenant = args.tenant
        cfg.atlas_url = args.url or f"https://{args.tenant}.atlan.com"
        cfg.api_base = f"{cfg.atlas_url}/api/meta"
        cfg.admin_base = f"{cfg.atlas_url}/api/meta/admin"
    else:
        cfg.env = "local"
        cfg.atlas_url = args.url or "http://localhost:21000"
        cfg.api_base = f"{cfg.atlas_url}/api/atlas/v2"
        cfg.admin_base = f"{cfg.atlas_url}/api/atlas/admin"

    return cfg
