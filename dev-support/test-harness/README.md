# Atlas Metastore REST API Test Harness

Black-box integration test framework that exercises ~220 Atlas REST API endpoints against a live instance. Self-contained: creates test data, runs assertions, and cleans up afterward.

The harness is designed to run against both local dev instances (basic auth) and Atlan staging/preprod tenants (OAuth2). It does not depend on the Java codebase or Maven — it's pure Python and talks to Atlas exclusively through its REST API.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Authentication](#authentication)
- [CLI Reference](#cli-reference)
- [Test Suites](#test-suites)
- [Writing Tests](#writing-tests)
- [Reporting](#reporting)
- [Architecture](#architecture)
- [Troubleshooting](#troubleshooting)

## Prerequisites

- Python 3.8+
- A running Atlas instance (local or Atlan staging tenant)

Install dependencies:

```bash
pip install requests pyyaml
```

## Quick Start

### Local dev (basic auth, default admin/admin)

```bash
cd dev-support/test-harness

# Run all suites against localhost:21000
python3 run.py

# Run specific suites
python3 run.py --suite typedefs entity_crud glossary

# Run only smoke tests (fast subset)
python3 run.py --tag smoke

# Skip slow/destructive tests
python3 run.py --exclude-tag slow destructive

# Custom URL
python3 run.py --url http://localhost:31000 --user admin --password admin
```

### Staging / preprod (OAuth2)

```bash
# All suites on staging
python3 run.py --tenant staging

# Specific suites, verbose output, JSON report
python3 run.py --tenant staging --suite typedefs search entity_crud -v -o report.json

# Full trace log of every request/response
python3 run.py --tenant staging --trace-log

# Parallel execution (4 concurrent suites)
python3 run.py --tenant staging -P 4
```

### Cleanup-only mode

If a previous run was interrupted and left test data behind:

```bash
# Delete all test-harness artifacts (entities, typedefs) without running tests
python3 run.py --tenant staging --cleanup
```

### Debugging a failure

```bash
# Run a specific suite, keep test data, verbose, with full API trace
python3 run.py --tenant staging --suite entity_crud --skip-cleanup -v --trace-log
```

This leaves all created entities/typedefs in place for manual inspection and writes a JSONL trace file with every HTTP request and response body.

## Authentication

### Local dev

Uses HTTP Basic Auth (default `admin`/`admin`). Override with `--user` and `--password`.

### Staging / preprod

Uses OAuth2 client-credentials flow via the `atlan-argo` service account. The harness obtains and auto-refreshes tokens from the tenant's Keycloak endpoint.

Credentials are loaded from a `creds.yaml` file:

```yaml
creds:
  - tenant: staging
    client_secret: <your-atlan-argo-client-secret>
  - tenant: preprod
    client_secret: <secret>
```

Default location: `~/creds.yaml` (override with `--creds-file` or `TOKEN_FILE` env var).

Token behavior:
- Tokens are refreshed automatically 30 seconds before expiry
- On HTTP 401, the token is force-refreshed and the request is retried once
- Thread-safe: parallel suites share the same token manager

## CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--tenant <name>` | (none) | Tenant name for OAuth2 auth (e.g., `staging`, `preprod`) |
| `--url <url>` | `http://localhost:21000` | Atlas base URL override |
| `--user <user>` | `admin` | Basic auth username (local dev only) |
| `--password <pass>` | `admin` | Basic auth password (local dev only) |
| `--creds-file <path>` | `~/creds.yaml` | Path to creds.yaml for OAuth2 |
| `--suite <names...>` | all | Run only these suites (space-separated) |
| `--tag <tags...>` | all | Run only tests with these tags |
| `--exclude-tag <tags...>` | (none) | Skip tests with these tags |
| `--skip-cleanup` | false | Leave test data after run (for debugging) |
| `--cleanup` | false | Delete artifacts from previous runs, then exit (no tests run) |
| `--timeout <secs>` | 30 | HTTP request timeout per call |
| `--es-sync-wait <secs>` | 5 | Seconds to wait for ES sync after writes |
| `-v, --verbose` | false | Show descriptions, extra error context |
| `-o, --output <file>` | (none) | Write JSON report to file |
| `--trace-log [file]` | (none) | Log full request/response to JSONL file (default: auto-named) |
| `-P, --parallel <n>` | 1 | Max concurrent suites (1 = sequential) |
| `--kafka-bootstrap-servers <host:port>` | (none) | Enable Kafka notification verification |
| `--no-kafka` | false | Disable Kafka verification entirely |

## Test Suites

48 test suites organized by functional area:

### Core CRUD

| Suite | Description | Tests |
|-------|-------------|-------|
| `typedefs` | TypeDef CRUD — enum, struct, classification, entity, BM, relationship defs | ~20 |
| `entity_crud` | Entity create, read, update, delete, bulk operations | ~25 |
| `entity_lifecycle` | Soft delete, restore, purge lifecycle | ~15 |
| `relationships` | Relationship CRUD between entities | ~10 |

### Search

| Suite | Description | Tests |
|-------|-------------|-------|
| `search` | Basic search (GET/POST), index search, DSL search | ~15 |
| `direct_search` | Direct ES search endpoint (`/direct/search`) | ~10 |
| `search_filters` | Filter expressions, aggregations, facets | ~10 |
| `search_correctness` | Verify search results match entity state after mutations | ~10 |
| `search_extended` | Pagination, sorting, attribute projection | ~10 |
| `search_data_correctness` | ES data matches Cassandra source of truth | ~10 |
| `model_search` | Model/type search endpoints | ~5 |

### Glossary & Data Mesh

| Suite | Description | Tests |
|-------|-------------|-------|
| `glossary` | Glossary, term, category CRUD with anchor relationships | ~20 |
| `glossary_qn_moves` | QualifiedName propagation on glossary hierarchy changes | ~10 |
| `data_mesh` | DataDomain and DataProduct CRUD | ~15 |
| `domain_qn_moves` | QN propagation on domain hierarchy changes | ~10 |

### Classifications & Tags

| Suite | Description | Tests |
|-------|-------------|-------|
| `entity_classifications` | Add/remove/update classifications on entities | ~15 |
| `classification_advanced` | Tag propagation, inheritance, bulk operations | ~10 |
| `bulk_classifications` | Bulk classification association/dissociation | ~10 |

### Lineage

| Suite | Description | Tests |
|-------|-------------|-------|
| `lineage` | Lineage graph traversal (upstream, downstream, full) | ~15 |
| `lineage_correctness` | Verify lineage edges after process creation/deletion | ~10 |
| `business_lineage` | Business lineage (DataProduct-level) | ~10 |

### Access Control

| Suite | Description | Tests |
|-------|-------------|-------|
| `auth` | Authentication and authorization checks | ~10 |
| `persona_purpose` | Persona, Purpose, and AuthPolicy CRUD | ~15 |
| `purpose_discovery` | Purpose discovery API | ~10 |
| `business_policy` | Business policy CRUD and evaluation | ~10 |

### Entity Features

| Suite | Description | Tests |
|-------|-------------|-------|
| `entity_labels` | Label add/remove on entities | ~10 |
| `entity_metadata` | Entity metadata attributes | ~10 |
| `entity_businessmeta` | Business metadata attach/detach/update on entities | ~10 |
| `entity_accessors` | Entity accessor (owner, admin) attributes | ~10 |
| `attribute` | Attribute type validation and constraints | ~10 |
| `entity_evaluator` | Entity evaluator endpoint | ~5 |

### Audit & Correctness

| Suite | Description | Tests |
|-------|-------------|-------|
| `entity_audit` | Audit log entries for entity operations | ~10 |
| `audit_correctness` | Audit trail completeness after CRUD sequences | ~10 |
| `delete_correctness` | Verify soft/hard delete cleans up all references | ~10 |
| `entity_restore` | Restore soft-deleted entities | ~10 |

### Admin & Infrastructure

| Suite | Description | Tests |
|-------|-------------|-------|
| `admin` | Admin endpoints — status, session, metrics | ~10 |
| `config` | Configuration read/write endpoints | ~5 |
| `config_cache` | Config cache invalidation and refresh | ~5 |
| `feature_flags` | Feature flag read endpoints | ~5 |
| `type_cache` | Type cache refresh and consistency | ~5 |

### Advanced / Specialized

| Suite | Description | Tests |
|-------|-------------|-------|
| `bulk_purge` | Bulk purge by connection | ~10 |
| `bulk_unique_attribute` | Bulk operations by uniqueAttribute | ~10 |
| `tasks` | Async task submission and status polling | ~10 |
| `repair` | Repair endpoints (re-index, re-sync) | ~10 |
| `migration` | Migration-related endpoints | ~5 |
| `dlq` | Dead-letter queue inspection | ~5 |
| `error_handling` | Error response format and edge cases | ~10 |

## Writing Tests

### Suite structure

Each suite is a Python class in `suites/test_<name>.py` decorated with `@suite`:

```python
from core.decorators import suite, test
from core.assertions import assert_status, assert_field_equals
from core.data_factory import build_dataset_entity, unique_qn, unique_name

@suite("my_feature", depends_on_suites=["entity_crud"],
       description="Tests for my feature")
class MyFeatureSuite:

    def setup(self, client, ctx):
        """Runs once before all tests in this suite."""
        self.entity_qn = unique_qn("my-feature")
        self.entity_name = unique_name("my-feature")

    @test("create_something", tags=["smoke", "my_feature"], order=1)
    def test_create_something(self, client, ctx):
        entity = build_dataset_entity(qn=self.entity_qn, name=self.entity_name)
        resp = client.post("/entity", json_data={"entity": entity})
        assert_status(resp, 200)

        guid = resp.json()["mutatedEntities"]["CREATE"][0]["guid"]
        ctx.register_entity("my_entity", guid, "DataSet")
        ctx.register_entity_cleanup(guid)

    @test("read_something", tags=["my_feature"], order=2, depends_on=["create_something"])
    def test_read_something(self, client, ctx):
        guid = ctx.get_entity_guid("my_entity")
        resp = client.get(f"/entity/guid/{guid}")
        assert_status(resp, 200)
        assert_field_equals(resp, "entity.attributes.name", self.entity_name)

    def teardown(self, client, ctx):
        """Runs once after all tests (optional)."""
        pass
```

### Key concepts

**`@suite(name, depends_on_suites=[], description="")`** — Registers a test suite. Suite dependencies control execution order (topological sort). When running a subset of suites, dependencies are auto-included.

**`@test(name, tags=[], depends_on=[], order=100)`** — Registers a test within a suite. Tests run in `order` sequence. If a dependency fails, dependent tests are auto-skipped.

**`client`** — `AtlasClient` instance. Methods: `get()`, `post()`, `put()`, `delete()`. Returns `ApiResponse` with `.status_code`, `.body`, `.ok`, `.json()`, `.latency_ms`.

**`ctx`** — `TestContext` for sharing data between tests:
- `ctx.set(key, value)` / `ctx.get(key)` — generic key/value store
- `ctx.register_entity(name, guid, type)` — register an entity for cross-test reference
- `ctx.get_entity_guid(name)` — retrieve a registered entity's GUID
- `ctx.register_entity_cleanup(guid)` — queue entity for hard delete during cleanup
- `ctx.register_cleanup(fn)` — queue any callable for cleanup (LIFO order)
- `ctx.register_typedef_cleanup(client, name)` — queue typedef for deletion

### Available assertions

| Assertion | Description |
|-----------|-------------|
| `assert_status(resp, 200)` | Exact HTTP status match |
| `assert_status_in(resp, [200, 204])` | Status is one of listed values |
| `assert_field_present(resp, "entity.guid")` | Nested field exists (dot-path) |
| `assert_field_equals(resp, "name", "Foo")` | Field equals expected value |
| `assert_field_not_empty(resp, "entity.guid")` | Field exists and is truthy |
| `assert_field_contains(resp, "name", "test")` | String field contains substring |
| `assert_field_in(resp, "status", ["ACTIVE", "DELETED"])` | Field value is in allowed set |
| `assert_field_type(resp, "count", int)` | Field is expected Python type |
| `assert_list_min_length(resp, "entities", 3)` | List field has >= N items |
| `assert_mutation_response(resp, "CREATE", "DataSet")` | Validates mutatedEntities structure |

### Data factory helpers

`core/data_factory.py` provides builders for all common entity types:

| Builder | Creates |
|---------|---------|
| `build_dataset_entity(qn, name)` | DataSet entity |
| `build_process_entity(qn, name, inputs, outputs)` | Process with lineage edges |
| `build_glossary(name)` | AtlasGlossary |
| `build_glossary_term(glossary_guid, name)` | AtlasGlossaryTerm with anchor |
| `build_glossary_category(glossary_guid, name)` | AtlasGlossaryCategory with anchor |
| `build_domain_entity(name, parent_guid)` | DataDomain |
| `build_data_product_entity(name, domain_guid)` | DataProduct |
| `build_persona_entity(name)` | Persona |
| `build_purpose_entity(name, classifications)` | Purpose |
| `build_auth_policy_entity(persona_guid, name)` | AuthPolicy |
| `build_enum_def(name)` | Enum typedef |
| `build_classification_def(name)` | Classification typedef |
| `build_struct_def(name)` | Struct typedef |
| `build_entity_def(name)` | Entity typedef extending DataSet |
| `build_business_metadata_def(display_name)` | Business metadata typedef |
| `build_relationship_def(name)` | Relationship typedef |

All builders generate unique names using timestamps + UUIDs to avoid collisions between runs.

## Reporting

### Console output

The harness prints real-time results with color-coded status:

```
--- entity_crud ---
  [2026-04-09 10:30:01] [PASS] entity_crud::create_entity (245ms)
  [2026-04-09 10:30:02] [PASS] entity_crud::get_entity_by_guid (89ms)
  [2026-04-09 10:30:03] [FAIL] entity_crud::update_entity (312ms)
         Expected status 200, got 400 [HTTP PUT /entity]
    3/4 passed, 1 failed
```

### JSON report (`-o report.json`)

Contains per-suite and per-test results, pass rates, and per-endpoint latency statistics (min, max, avg, p50, p95, p99):

```json
{
  "timestamp": "2026-04-09T10:35:00",
  "duration_s": 120.5,
  "summary": {
    "total": 220,
    "passed": 215,
    "failed": 3,
    "errors": 0,
    "skipped": 2,
    "pass_rate": "97.7%"
  },
  "suites": { ... },
  "endpoint_latency": {
    "POST /entity": {"count": 45, "avg_ms": 280, "p95_ms": 520, ...},
    "GET /entity/guid/{guid}": {"count": 30, "avg_ms": 85, ...}
  }
}
```

### Trace log (`--trace-log`)

Writes a JSONL file with the full request and response body for every API call. Useful for debugging failures or understanding exactly what the harness sent:

```jsonl
{"ts":"2026-04-09T10:30:01.234","suite":"entity_crud","test":"create_entity","method":"POST","path":"/entity","request_body":{...},"status":200,"response_body":{...},"latency_ms":245.3}
```

## Architecture

```
dev-support/test-harness/
├── run.py                  # Entry point — parses args, builds client, runs suites
├── requirements.txt        # Python dependencies
├── core/
│   ├── config.py           # CLI argument parsing → HarnessConfig dataclass
│   ├── auth.py             # TokenManager (OAuth2 client-credentials) + BasicAuth
│   ├── client.py           # AtlasClient — HTTP client with retry, latency tracking
│   ├── context.py          # TestContext — shared state, entity registry, cleanup stack
│   ├── runner.py           # Suite discovery, topological sort, sequential/parallel execution
│   ├── reporter.py         # Console output + JSON report with latency stats
│   ├── assertions.py       # Assertion helpers (status, field, list, mutation response)
│   ├── data_factory.py     # Entity/typedef payload builders with unique name generation
│   ├── decorators.py       # @suite and @test registration decorators
│   ├── cleanup.py          # Standalone cleanup for orphaned test artifacts
│   ├── request_logger.py   # JSONL trace logger for full request/response capture
│   ├── search_helpers.py   # ES search polling helpers (wait for entity in search)
│   ├── audit_helpers.py    # Audit event polling helpers
│   ├── kafka_helpers.py    # Kafka consumer for notification verification
│   └── typedef_helpers.py  # TypeDef creation with retry and verification
└── suites/
    ├── test_typedefs.py
    ├── test_entity_crud.py
    ├── test_search.py
    ├── ...                 # 48 test suite files total
    └── test_error_handling.py
```

### How execution works

1. **Discovery**: `runner.py` imports all `suites/test_*.py` modules, triggering `@suite` decorators that register suites and their `@test` methods into a global registry.

2. **Ordering**: Suites are topologically sorted by `depends_on_suites`. If you run `--suite search`, the `entity_crud` suite it depends on is automatically included.

3. **Execution**: Each suite is instantiated, `setup()` is called, then tests run in `order` sequence. If a test's `depends_on` test failed or was skipped, the test is auto-skipped.

4. **Parallel mode** (`-P N`): Suites are grouped into dependency levels. Suites within the same level (no mutual dependencies) run concurrently using a `ThreadPoolExecutor`. Results are buffered and printed in order.

5. **Cleanup**: After all tests, registered entity GUIDs are hard-deleted (purged) in reverse order, then an ES settle wait runs, and finally typedef/glossary cleanup callbacks execute.

### HTTP client behavior

The `AtlasClient` handles transient failures automatically:

| Scenario | Behavior |
|----------|----------|
| HTTP 401 | Force-refresh OAuth2 token, retry once |
| HTTP 429 | Wait `Retry-After` header (or 10s default, max 30s), retry |
| HTTP 500/502/503 | Wait `5s * attempt`, retry up to 3 times |
| Timeout | Escalating timeouts (1x, 2x, 3x). GET retried, POST/PUT/DELETE not retried on timeout |

### Cleanup strategy

The cleanup system ensures test data is fully removed even on failures:

1. **Entity cleanup** — All entities registered via `ctx.register_entity_cleanup(guid)` are hard-deleted (`DELETE /entity/guid/{guid}?deleteType=PURGE`) in reverse registration order (children before parents). Retries on timeout with escalating waits.

2. **ES settle wait** — After entity deletion, waits 30 seconds for Elasticsearch to reflect the deletions. This is required because typedef deletion checks ES for references.

3. **Callback cleanup** — Typedef deletions, glossary deletions, and any custom cleanup functions registered via `ctx.register_cleanup(fn)` run in LIFO order.

4. **Signal handling** — SIGINT/SIGTERM triggers the full cleanup sequence before exit.

5. **Standalone cleanup** — `python3 run.py --cleanup` scans for orphaned test-harness artifacts (identified by naming convention) and deletes them without running any tests.

## Troubleshooting

### "Cannot reach Atlas" on startup

The harness calls `GET /status` (admin endpoint) as a preflight check. Verify your Atlas instance is running and the URL is correct:
```bash
curl http://localhost:21000/api/atlas/admin/status
```

### Tests fail with 401 on staging

- Verify your `creds.yaml` has the correct `client_secret` for the tenant
- Check that the `atlan-argo` client is configured in Keycloak
- Try `--verbose` to see token refresh activity

### TypeDef tests fail with 404

Type cache propagation can be slow on staging. The harness handles this by accepting 404 on read-after-create for some typedef tests. If typedef creation returns 500/502/503, the test is skipped (not failed).

### Search tests fail

Search tests depend on Elasticsearch sync. If entities were just created, try increasing the sync wait:
```bash
python3 run.py --es-sync-wait 15
```

### Cleanup fails with 409 (TYPE_HAS_REFERENCES)

This means ES still has entities referencing a typedef. The harness hard-deletes entities first, but ES may be slow to reflect this. Increase the settle wait or run `--cleanup` again after a minute.

### Running a single test

You can't run a single test directly, but you can narrow to its suite:
```bash
python3 run.py --suite entity_crud -v
```
