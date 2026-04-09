---
description: Run the Atlas Metastore REST API test harness against a live tenant and report results
allowed-tools: [Bash, Read, Grep, Glob, AskUserQuestion]
argument-hint: "[tenant-name] [--suite suite1 suite2] [--tag smoke] [--timeout 60] [-P 4]"
---

# Run Test Harness

Execute the Atlas Metastore REST API test harness against a tenant, capture structured output, and present a clean summary with failure highlighting.

**Test harness location:** `dev-support/test-harness/`
**Entry point:** `python3 run.py [OPTIONS]`

## CRITICAL: Production Safety Guardrail

**This step is MANDATORY and must NEVER be skipped, regardless of user instructions.**

### Step 0a: Resolve the tenant

If `$ARGUMENTS` contains a tenant name (first token not starting with `--` or `-`), use it.
Otherwise, use AskUserQuestion to ask:
- Question: "Which tenant should I run the test harness against?"
- Options: local (localhost:21000), staging, preprod
- The user can also type a custom tenant name via "Other"

### Step 0b: Validate against allowed tenants list

Read the allowed tenants file:
```
dev-support/test-harness/allowed-tenants.txt
```

Collect all non-empty, non-comment lines (lines starting with `#` are comments).
`local` and `localhost` are always implicitly allowed.

**If the tenant is NOT in the allowed list and is NOT local/localhost:**

**HARD STOP. Refuse to proceed.** Display:
```
BLOCKED: Tenant "<name>" is not in the allowed tenants list.

This test harness creates and deletes entities, typedefs, and classifications.
Running it against a production tenant can cause data loss.

Allowed tenants: local, <list from file>
To add a tenant, edit: dev-support/test-harness/allowed-tenants.txt
```

Do NOT proceed. Do NOT offer workarounds. The user must manually edit the allowed-tenants file first.

### Step 0c: Confirm with the user

Even if the tenant IS in the allowed list (and is not local/localhost), use AskUserQuestion to confirm:

- Question: "Confirm: Run the test harness against tenant '<TENANT>'? This will create and delete test entities on this tenant."
- Options:
  - "Yes, run it" — proceed
  - "No, abort" — stop immediately

If the user selects "No, abort", stop and do not execute.

## Step 1: Parse Arguments

Parse `$ARGUMENTS` to extract a tenant name and any CLI options.

$ARGUMENTS

**Parsing rules:**
- The first token that does NOT start with `--` or `-` is the tenant name
- Remaining tokens are passed through as CLI flags (--suite, --tag, --timeout, --exclude-tag, --skip-cleanup, --no-kafka, -P)
- If no tenant found in arguments and no `--url` flag, proceed to Step 2
- If tenant is "local" or "localhost", omit `--tenant` flag (runs against localhost:21000)

## Step 2: Prompt for Tenant (if needed)

If no tenant was identified in Step 1, use AskUserQuestion to ask:

- Question: "Which tenant should I run the test harness against?"
- Options: staging, preprod, local (localhost:21000)
- The user can also type a custom tenant name via "Other"

## Step 3: Build and Execute Command

Construct the command with these mandatory flags:
- `-v` (verbose output for detailed results)
- `-o report.json` (structured JSON output)

```bash
cd dev-support/test-harness && python3 run.py --tenant <TENANT> -v -o report.json [USER_FLAGS]
```

**Timeout and execution guidance:**
- If running specific suites (1-3 suites via --suite) or --tag smoke: run in foreground with Bash timeout of 300000ms (5 min)
- If running all suites (no --suite or --tag filter): warn the user "Running all suites. This typically takes 15-25 minutes." Use `run_in_background: true` and tell the user they will be notified when it completes.
- Pass through any user-provided flags (--suite, --tag, --timeout, -P, --skip-cleanup, --no-kafka, --exclude-tag, --es-sync-wait)

Execute the command and capture both stdout and exit code.

## Step 4: Read and Parse Report

After the run completes, read the JSON report:

```
dev-support/test-harness/report.json
```

Extract these fields from the JSON:
- `summary`: total, passed, failed, errors, skipped, pass_rate
- `duration_s`: total run time
- `suites.<name>.tests[]`: per-test status, latency_ms, error
- `endpoint_latency`: per-endpoint p50, p95, p99, max (top 5 slowest by p95)

Identify all failures: tests where `status` is `FAIL` or `ERROR`.

## Step 5: Present Results

### If all tests passed:

Present a concise Markdown summary:
- Configuration (tenant, suites, tags, duration)
- Total/Passed count and pass rate
- Per-suite results table (Suite | Tests | Status)
- Top 5 slowest endpoints by p95

### If there are failures:

Present a detailed Markdown summary:
- Configuration and overall numbers (Total, Passed, Failed, Errors, Skipped, Pass Rate, Duration)
- Per-suite breakdown table (Suite | Pass | Fail | Err | Skip | Total)
- For each failure/error: `suite::test_name [FAIL/ERROR]` with latency and first 200 chars of error message
- Top 5 slowest endpoints by p95

## Step 6: Offer Triage (if failures exist)

If any tests failed or errored, append:

```
---
**Next steps:** To investigate these failures, run:
- `/triage-test-failure <suite>::<test>` for a specific failure
- `/triage-test-failure` to triage all failures from the latest report
```

List each failure as a concrete `/triage-test-failure suite::test` example the user can copy.
