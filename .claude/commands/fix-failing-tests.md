---
description: Analyze CI test failures, identify root cause, and generate fixes for broken tests
allowed-tools: [Bash, Read, Grep, Glob, Edit, Write, Task]
argument-hint: "[PR-number | workflow-run-id]"
---

# Fix Failing Tests

Analyze test failures from CI (GitHub Actions) or local runs, determine whether failures are caused by code changes or are pre-existing, and generate fixes.

## Input Resolution

Determine what to analyze based on `$ARGUMENTS`:

1. **PR number** (e.g., `#123` or `123`): Get test failures from the PR's CI checks
   ```bash
   gh pr checks <number> --repo atlanhq/atlas-metastore
   ```
2. **Workflow run ID**: Get test results from a specific Actions run
   ```bash
   gh run view <run-id> --repo atlanhq/atlas-metastore
   ```
3. **No argument**: Look at the latest CI run on the current branch
   ```bash
   gh run list --branch $(git branch --show-current) --repo atlanhq/atlas-metastore --limit 3
   ```

## Phase 0: Extract Linear Ticket Context

### 0a: Find ticket IDs
```bash
# From PR title and description
gh pr view <number> --repo atlanhq/atlas-metastore --json title,body,headRefName 2>/dev/null

# From branch name
git branch --show-current | grep -oiE '(ms|metastore)-[0-9]+'
```

### 0b: Fetch ticket details (if ID found)
```bash
linear issue view <TICKET_ID> 2>/dev/null
```

Use ticket context to understand:
- Was the behavior change intentional? (ticket says "change X to Y" → test failure is expected)
- What's the expected new behavior? (helps write correct test assertions)
- Are there known edge cases from the ticket that relate to the failure?

## Phase 1: Parallel Research — Gather Failure Context

Launch **multiple Explore agents in parallel** using the Task tool:

```
Launch these agents IN PARALLEL (single message, multiple Task calls):
```

**Agent 1 — CI Log Analysis:**
Fetch the CI run status, find failed jobs, download logs, and parse failure details: which test classes failed, which methods, error messages, stack traces, assertion details (expected vs actual).
```bash
gh pr checks <number> --repo atlanhq/atlas-metastore 2>&1
gh run view <run-id> --repo atlanhq/atlas-metastore --log-failed 2>&1
gh run download <run-id> --repo atlanhq/atlas-metastore --name "integration-test-reports-*" --dir /tmp/test-results 2>/dev/null
```

**Agent 2 — Test Code Deep Read:**
For each failed test class, find and read the full test file. Understand what each failed test method is testing, what it asserts, and what setup it requires. Read the test's `@BeforeMethod`/`@BeforeClass` to understand preconditions.

**Agent 3 — PR Diff Analysis:**
Get the full PR diff. For each changed file, understand what was modified. Map changed code to the failed tests — is the failure in a method that was directly changed? Or in a downstream caller?
```bash
gh pr diff <number> --repo atlanhq/atlas-metastore
```

**Agent 4 — Master Baseline Check:**
Check if the failed tests were passing on master before this PR. This distinguishes regressions from pre-existing failures.
```bash
gh run list --workflow "Integration Tests" --branch master --repo atlanhq/atlas-metastore --limit 5 --json conclusion,databaseId
```

Wait for all agents to complete before proceeding.

## Phase 2: Correlate Failures with Changes

For each failed test, determine the failure category:

| Category | How to Detect | Action |
|---|---|---|
| **Direct regression** | Failed test calls a method that was changed in the PR | Fix the code or update the test |
| **Indirect regression** | Failed test uses an entity type/operation affected by the change | Fix the code or add handling |
| **Test assumption broken** | Test hardcodes a value/behavior that intentionally changed (check ticket) | Update the test assertions |
| **Flaky test** | Test fails intermittently, not related to changes | Skip or mark as known-flaky |
| **Infrastructure issue** | Timeout, connection refused, container startup failure | Not a code issue |
| **Compilation error** | Test won't compile due to changed method signatures | Update test to match new signatures |

## Phase 3: Generate Fixes

### For direct/indirect regressions (code bug):
1. Read the changed code carefully
2. Identify the bug
3. Suggest a code fix with explanation
4. Don't modify tests — the tests are correct, the code is wrong

### For broken test assumptions (intentional behavior change):
1. Read the test and understand what it asserts
2. Determine the new expected behavior (use ticket context)
3. Update the test assertions to match the new behavior
4. Add comments explaining why the assertion changed

### For compilation errors (signature changes):
1. Find all test references to the changed method
2. Update test code to match new signatures
3. Update mock setups (`when(...).thenReturn(...)`)

### For flaky tests:
1. Identify the flaky pattern
2. Add retry logic or stabilization
3. Or recommend marking as `@Disabled("Flaky — tracked in MS-XXX")`

## Phase 4: Apply Fixes

1. Edit the test file using the Edit tool
2. Verify compilation:
   ```bash
   JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
     /opt/homebrew/bin/mvn compile -pl <module> -am -DskipTests -Drat.skip=true 2>&1 | tail -20
   ```
3. If fix is to source code (not test), clearly mark it as a source fix

## Phase 5: Report

```markdown
## Test Failure Analysis

### CI Run
- **PR:** #<number> — <title>
- **Branch:** <branch>
- **Workflow run:** <run-id>
- **Overall status:** <conclusion>

### Ticket Context (if found)
- **Ticket:** MS-XXX — <title>
- **Intentional behavior changes:** [from ticket]

### Failures Found

#### 1. <TestClass>.<testMethod>

| Field | Value |
|---|---|
| **Category** | Direct regression / Broken assumption / Flaky / Infra |
| **Error** | `<error message>` |
| **Root cause** | <explanation> |
| **Related change** | `<file>:<line>` — <what changed> |
| **Ticket context** | Intentional change per MS-XXX / Not related |

**Fix applied:**
```java
// Before
assertEquals(expected, actual);

// After
assertEquals(newExpected, actual); // Updated for new behavior from MS-XXX
```

### Summary

| Test | Category | Fix | Status |
|------|----------|-----|--------|
| TestClass.method1 | Broken assumption | Updated assertion | Fixed |
| TestClass.method2 | Flaky | Added retry | Fixed |
| TestClass.method3 | Code regression | Suggested code fix | Needs review |

### Recommendations
- [ ] Review source code fixes before merging
- [ ] Re-run CI to verify fixes
- [ ] Consider adding unit tests for the changed code paths
```
