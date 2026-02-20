---
description: Identify and run regression tests affected by code changes, report pass/fail results
allowed-tools: [Bash, Read, Grep, Glob, Task]
argument-hint: "[branch-name | PR-number | file-or-directory-path]"
---

# Run Regression Suite

Automatically identify which integration and unit tests are affected by code changes, execute them, and report results with pass/fail analysis.

## Input Resolution

Determine what to analyze based on `$ARGUMENTS`:

1. **PR number** (e.g., `#123` or `123`): Get changed files from the PR
   ```bash
   gh pr diff <number> --repo atlanhq/atlas-metastore --name-only
   ```
2. **Branch name** (e.g., `feature/my-branch`): Get changed files on that branch
   ```bash
   git fetch origin <branch> 2>/dev/null
   git diff --name-only $(git merge-base master origin/<branch>)..origin/<branch>
   ```
3. **File or directory path**: Scope to those specific files
4. **No argument**: Use the current branch diff against master
   ```bash
   git diff --name-only $(git merge-base HEAD master)..HEAD
   ```

**How to detect the argument type:**
- Starts with `#` or is purely numeric → PR number
- Contains `/` but doesn't exist as a file path on disk → branch name
- Exists as a file or directory on disk → file path
- Matches a known git branch (`git branch -a | grep <arg>`) → branch name

If no changes are found, inform the user and stop.

## Phase 0: Extract Linear Ticket Context

Linear tickets may describe expected behaviors useful for interpreting test results.

### 0a: Find ticket IDs
```bash
# From PR title and description
gh pr view <number> --repo atlanhq/atlas-metastore --json title,body,headRefName 2>/dev/null

# From branch name
git branch --show-current | grep -oiE '(ms|metastore)-[0-9]+'

# From commit messages
git log --oneline $(git merge-base HEAD master)..HEAD | grep -oiE '(ms|metastore)-[0-9]+'
```

### 0b: Fetch ticket details (if ID found)
```bash
linear issue view <TICKET_ID> 2>/dev/null
```

Use ticket context to:
- Understand if test failures are expected (intentional behavior change)
- Determine if "Expected failure" tests need updated assertions based on ticket description
- Cross-reference acceptance criteria with test results

## Phase 1: Parallel Research — Map Changes to Tests

Launch **multiple Explore agents in parallel** using the Task tool:

```
Launch these agents IN PARALLEL (single message, multiple Task calls):
```

**Agent 1 — Integration Test Mapping:**
Read `.claude/test-coverage-map.json`. For each changed file, match against `pathMappings` to find required integration tests. Check if any changed file is in the high-impact list (EntityGraphMapper, PreProcessorUtils, AtlasEntityStoreV2, AtlasTypeRegistry, RequestContext, EntityGraphRetriever) — if so, flag `RUN_FULL_SUITE=true`.

**Agent 2 — Unit Test Discovery:**
For each changed source file (e.g., `repository/src/main/java/.../Foo.java`), search for corresponding test files (`repository/src/test/java/.../FooTest.java`). List all found unit test classes. Also check for test files with alternate naming patterns like `*Tests.java`.

**Agent 3 — Change Impact Analysis:**
Read the full diff for each changed file. Identify which methods are new, modified, or deleted. For modified methods, search for all callers in the codebase. Classify changes as: logic change, signature change, new feature, bug fix, refactor.

Wait for all agents to complete before proceeding.

## Phase 2: Pre-flight Check

Before running tests, verify the environment:

```bash
# Check Java version
java -version 2>&1

# Check Maven
/opt/homebrew/bin/mvn --version 2>&1 | head -3

# Check Docker (required for integration tests with Testcontainers)
docker info > /dev/null 2>&1 && echo "Docker: OK" || echo "Docker: NOT RUNNING"
```

If Docker is not running, warn the user that integration tests require Docker and ask if they want to proceed with unit tests only.

## Phase 3: Build Dependencies

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn install -pl repository -am -DskipTests -Drat.skip=true 2>&1 | tail -20
```

If integration tests are in scope:
```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn install -pl webapp -am -DskipTests -Drat.skip=true 2>&1 | tail -20
```

If build fails, report the failure and stop.

## Phase 4: Run Unit Tests

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn test -pl repository \
  -Dtest="TestClass1,TestClass2" \
  -Drat.skip=true \
  -Dsurefire.failIfNoSpecifiedTests=false 2>&1
```

## Phase 5: Run Integration Tests

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn test -pl webapp \
  -Dtest="TestClass1,TestClass2,TestClass3" \
  -Drat.skip=true \
  -Dsurefire.failIfNoSpecifiedTests=false 2>&1
```

**For full suite** (if high-impact files changed):
```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn test -pl webapp \
  -Dtest="EntityCrudIntegrationTest,GlossaryIntegrationTest,GlossaryExtendedIntegrationTest,SQLAssetHierarchyIntegrationTest,ClassificationIntegrationTest,LineageIntegrationTest,SearchIntegrationTest,RelationshipIntegrationTest,BusinessMetadataIntegrationTest,DataMeshIntegrationTest,PersonaPurposeIntegrationTest,LabelIntegrationTest,TypeDefIntegrationTest" \
  -Drat.skip=true \
  -Dsurefire.failIfNoSpecifiedTests=false 2>&1
```

Use a timeout of 600000ms (10 minutes).

## Phase 6: Parse Results

Parse Maven Surefire output for:
- `Tests run: X, Failures: Y, Errors: Z, Skipped: W`
- Failed test details and stack traces
- Test timing

## Phase 7: Analyze Failures

For each failed test, use the Linear ticket context (if available) to classify:
- **New regression** — test was passing on master, now fails due to changes
- **Pre-existing failure** — test was already failing (flaky or broken on master)
- **Expected failure** — test needs updating because behavior intentionally changed (check ticket description for intentional changes)

```bash
# Check if test passed on master recently
gh run list --workflow "Integration Tests" --branch master --repo atlanhq/atlas-metastore --limit 5 --json conclusion,databaseId
```

## Phase 8: Report Results

```markdown
## Regression Test Results

### Ticket Context (if found)
- **Ticket:** MS-XXX — <title>
- **Expected behavior changes:** [from ticket description]

### Summary
- **Changes analyzed:** N files
- **Unit tests run:** X (Y passed, Z failed)
- **Integration tests run:** X (Y passed, Z failed)
- **Overall status:** PASS / FAIL
- **Full suite triggered:** Yes/No (reason)

### Change-to-Test Mapping

| Changed File | Mapped Tests | Status |
|-------------|-------------|--------|
| `path/to/File.java` | TestClass1, TestClass2 | PASS / FAIL |

### Unit Test Results

| Test Class | Tests | Passed | Failed | Time |
|-----------|-------|--------|--------|------|
| FooTest | 5 | 5 | 0 | 1.2s |

### Integration Test Results

| Test Class | Tests | Passed | Failed | Time |
|-----------|-------|--------|--------|------|
| EntityCrudIntegrationTest | 12 | 12 | 0 | 45s |

### Failed Tests Detail

#### [REGRESSION] testMethodName — TestClass
- **Error:** AssertionError: expected X but got Y
- **Stack trace:** (first 10 lines)
- **Related change:** `File.java:123`
- **Suggested fix:** [description]

#### [EXPECTED] testMethodName — TestClass
- **Error:** ...
- **Note:** Behavior intentionally changed per ticket MS-XXX. Test assertions need updating.

#### [PRE-EXISTING] testOtherMethod — TestClass
- **Error:** TimeoutException
- **Note:** Flaky on master — not caused by current changes

### Coverage Gaps

| Changed Code | Test Coverage |
|-------------|--------------|
| `NewPreProcessor.java` | NO existing tests |

### Verdict

**ALL TESTS PASS** / **REGRESSION DETECTED** / **PRE-EXISTING FAILURES ONLY**

### Run Commands (for manual re-run)

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn test -pl repository \
  -Dtest="FailedTest1,FailedTest2" \
  -Drat.skip=true
```
```
