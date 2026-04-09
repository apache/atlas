---
description: Investigate a test harness failure to classify it as a real bug, timing issue, environment issue, or test logic error
allowed-tools: [Bash, Read, Grep, Glob, Task]
argument-hint: "[suite_name::test_name | suite_name | --report path/to/report.json]"
---

# Triage Test Failure

Investigate one or more test harness failures. Read the test source, examine the server code, check observability data, and classify each failure with an actionable recommendation.

$ARGUMENTS

## Phase 1: Resolve Input

**Argument formats:**
- `suite_name::test_name` — triage a single specific test (e.g., `glossary::create_term`)
- `suite_name` — triage all failures in that suite from the latest report
- `--report path/to/file.json` — read a specific report file and triage all failures
- Empty — read the default report and triage all failures

**Suite to file mapping:** Suite `X` lives at:
`dev-support/test-harness/suites/test_X.py`

**Default report path:**
`dev-support/test-harness/report.json`

If the argument contains `::`, split on `::` to get suite_name and test_name.
If no failures are found for the specified test/suite, inform the user and stop.

## Phase 2: Gather Failure Context

Read the report JSON and extract for each target failure:
- `status` (FAIL or ERROR)
- `error` (full error message string)
- `latency_ms` (helps detect timeout issues)

Parse the error message to extract structured fields:
- **HTTP status:** from `[HTTP NNN METHOD /path]` pattern
- **Expected vs actual:** from `expected=X, actual=Y` or `Expected X, got Y` pattern
- **Atlas error code:** from `ATLAS-NNN-NN-NNN` pattern
- **Exception type:** from `ExceptionType: message` pattern
- **API path:** the endpoint being called
- **Response body:** from `Response: {...}` pattern

## Phase 3: Read the Test Source

Read the test file for the failing suite:
```
dev-support/test-harness/suites/test_<suite_name>.py
```

Find the test method by searching for `@test("<test_name>"` or the method whose decorator has matching name.

From the test, identify:
1. **Purpose:** What is this test validating? (read docstring and code)
2. **API calls:** Which endpoints does it call? (look for `client.get/post/put/delete`)
3. **Assertions:** What does it assert? (look for `assert_status`, `assert_field_equals`, `assert_status_in`, etc.)
4. **Dependencies:** Does it depend on other tests? (`depends_on` in decorator)
5. **Timing sensitivity:** Does it use `time.sleep`, polling, or retry loops?
6. **Tags:** smoke, crud, search, negative, etc.

## Phase 4: Parallel Investigation

Launch 2-3 Task agents IN PARALLEL (single message, multiple Task calls) with `subagent_type: Explore`:

### Agent 1 — Server-Side Code Investigation
Given the API endpoint from the test (e.g., `POST /entity`, `DELETE /types/typedef/name/{name}`):
- Find the JAX-RS resource class in `webapp/src/main/java/org/apache/atlas/web/rest/`
- Find the service/store method it delegates to in `repository/src/main/java/org/apache/atlas/`
- If an Atlas error code was found (e.g., `ATLAS-500-00-001`), search for where it is defined and thrown
- If a Java exception was in the error (NPE, ClassCastException), search for where it could originate
- Report file paths with line numbers

### Agent 2 — Error Pattern and History
- Search git log for recent changes to files related to the endpoint/entity type
- Look for similar error messages in other test files in `dev-support/test-harness/suites/`
- Check if this test has known issues documented (search for test_name in comments)
- If the error involves a specific entity type, check if other tests for the same operation also failed

### Agent 3 — Observability Check (only if tenant is known from report.json)
Use the atlas-observability MCP tools:
- `mcp__atlas-observability__get_health_metrics` for the tenant
- `mcp__atlas-observability__get_emerging_errors` for the tenant with timeRange '24h'
- If test failure involved slow responses (latency > 5000ms), call `mcp__atlas-observability__analyze_slowness`
- Report any anomalies that correlate with the test failure

## Phase 5: Classify the Failure

Use this decision tree:

```
1. Is latency_ms > 30000 or HTTP status 408?
   → Check if test has polling/wait logic
     → If test already polls with sufficient timeout: ENVIRONMENT ISSUE
     → If test has no/short wait: TEST TIMING ISSUE

2. Does error contain a Java server exception (NPE, ClassCast, IllegalArgument)?
   → Is the exception in a code path that should validate input?
     → If input was intentionally invalid (negative test): TEST LOGIC ERROR
     → Otherwise: REAL BUG

3. Is the error a status code mismatch (expected 4xx, got 5xx)?
   → If error message shows server crash (NPE, unexpected state): REAL BUG
   → If server returns valid error but different status code: TEST LOGIC ERROR

4. Is the error "entity not found" or "field not found" after a create/update?
   → TEST TIMING ISSUE (ES eventual consistency)

5. Did observability show elevated errors or degraded health?
   → ENVIRONMENT ISSUE

6. Does the test assertion use exact match where a range would be correct?
   → TEST LOGIC ERROR

7. Default → REAL BUG (needs further manual investigation)
```

**Classification definitions:**

| Classification | Meaning | Owner |
|---|---|---|
| **Real Bug** | Server behavior is incorrect per API contract | Server team |
| **Test Timing Issue** | Test doesn't wait long enough for async/ES operations | Test author |
| **Environment Issue** | Tenant-specific problem (capacity, health, network) | Infra team |
| **Test Logic Error** | Test assertion is wrong or too strict | Test author |

## Phase 6: Produce Triage Report

### For a single test:

```markdown
## Triage: suite_name::test_name

### Classification: **[Real Bug | Test Timing Issue | Environment Issue | Test Logic Error]**

### Test Summary
[2-3 sentences describing what the test does]

### Failure Details
| Field | Value |
|-------|-------|
| Suite | suite_name |
| Test | test_name |
| Status | FAIL / ERROR |
| Latency | Xms |
| API Call | METHOD /path |
| Expected | <expected behavior/status> |
| Actual | <actual behavior/status> |

### Analysis
[Detailed explanation referencing both test source and server source]

### Evidence
- **Test file:** `dev-support/test-harness/suites/test_X.py` (lines N-M)
- **Server file:** `repository/src/main/java/.../X.java` (line N) — if applicable
- **Observability:** [findings from MCP, if any]
- **Git history:** [recent relevant changes, if any]

### Recommendation
| Aspect | Detail |
|--------|--------|
| **Action** | [Specific fix description] |
| **Priority** | High / Medium / Low |
| **Owner** | Test author / Server team / Infra team |

### Suggested Fix
[Concrete recommendation based on classification]
```

### For batch mode (multiple failures):

Present a summary table first, then individual triage reports:

```markdown
## Triage Summary (N failures)

| # | Test | Classification | Priority | Quick Recommendation |
|---|------|---------------|----------|---------------------|
| 1 | suite::test | Timing Issue | Low | Increase wait time |
| 2 | suite::test | Real Bug | High | NPE in server code |

### Breakdown by Classification
- Real Bugs: N (need server fix)
- Timing Issues: N (need test adjustment)
- Environment Issues: N (transient, re-run)
- Test Logic Errors: N (need test fix)
```

Then individual triage reports for each failure.

## Reference: Suite Names

All registered suites: admin, attribute, audit_correctness, auth, bulk_classifications, bulk_purge, bulk_unique_attribute, business_lineage, business_policy, classification_advanced, config, config_cache, data_mesh, delete_correctness, direct_search, dlq, entity_accessors, entity_audit, entity_businessmeta, entity_classifications, entity_crud, entity_evaluator, entity_labels, entity_lifecycle, entity_metadata, entity_restore, error_handling, feature_flags, glossary, lineage, lineage_correctness, migration, model_search, persona_purpose, purpose_discovery, relationships, repair, search, search_correctness, search_data_correctness, search_extended, search_filters, tasks, type_cache, typedefs
