---
description: Run the full AI-driven QA pipeline — code quality, security, architecture, QA review, unit test generation, and regression testing
allowed-tools: [Bash, Read, Grep, Glob, Edit, Write, Task]
argument-hint: "[PR-number | branch-name]"
---

# AI-Driven QA Pipeline

Orchestrate the full QA cycle for a code change. Runs multiple review and testing skills in parallel, collects results, and produces a unified merge recommendation.

## Pipeline Skills

| # | Skill | Purpose | Phase |
|---|-------|---------|-------|
| 1 | `/review-code` | Null safety, error handling, anti-patterns | Parallel Review |
| 2 | `/review-security` | Injection, authorization, sensitive data | Parallel Review |
| 3 | `/review-architecture` | Layer violations, coupling, breaking changes | Parallel Review |
| 4 | `/qa-review` | Edge cases, regression risks, data integrity | Parallel Review |
| 5 | `/generate-unit-tests` | Generate TestNG + Mockito tests for changed code | Test Generation |
| 6 | `/run-regression` | Run affected unit + integration tests | Test Execution |

## Input Resolution

Determine what to analyze based on `$ARGUMENTS`:

1. **PR number** (e.g., `#123` or `123`):
   ```bash
   gh pr view <number> --repo atlanhq/atlas-metastore --json title,headRefName,body,state
   gh pr diff <number> --repo atlanhq/atlas-metastore --name-only
   ```
2. **Branch name** (e.g., `feature/my-branch`):
   ```bash
   git fetch origin <branch> 2>/dev/null
   git diff --name-only $(git merge-base master origin/<branch>)..origin/<branch>
   ```
3. **No argument**: Use current branch diff against master
   ```bash
   git diff --name-only $(git merge-base HEAD master)..HEAD
   ```

**Detection logic:**
- Starts with `#` or is purely numeric → PR number
- Contains `/` but doesn't exist on disk → branch name
- No argument → current branch

If no changes are found, inform the user and stop.

Store the resolved input (PR number, branch, or file list) as `$TARGET` for passing to each skill agent.

## Phase 0: Context Gathering

Before launching skill agents, gather shared context that all skills need:

```bash
# 1. Changed files
CHANGED_FILES=$(gh pr diff <number> --repo atlanhq/atlas-metastore --name-only 2>/dev/null || \
  git diff --name-only $(git merge-base HEAD master)..HEAD)

# 2. Change stats
FILE_COUNT=$(echo "$CHANGED_FILES" | wc -l | tr -d ' ')
JAVA_FILES=$(echo "$CHANGED_FILES" | grep '\.java$' | wc -l | tr -d ' ')

# 3. PR metadata (if PR number)
PR_TITLE=$(gh pr view <number> --repo atlanhq/atlas-metastore --json title --jq .title 2>/dev/null)
BRANCH=$(gh pr view <number> --repo atlanhq/atlas-metastore --json headRefName --jq .headRefName 2>/dev/null || git branch --show-current)

# 4. Linear ticket (if found)
TICKET_ID=$(echo "$PR_TITLE $BRANCH" | grep -oiE '(ms|metastore)-[0-9]+' | head -1)
```

Report the pipeline kickoff:

```markdown
## QA Pipeline Started

- **Target:** PR #<number> / branch <name>
- **Title:** <PR title>
- **Files changed:** N (M Java files)
- **Ticket:** MS-XXX (if found)
- **Skills to run:** 6
```

## Phase 1: Parallel Reviews

Launch **4 review agents in parallel** using the Task tool. Each agent runs one complete skill.

```
Launch ALL 4 agents IN PARALLEL (single message, multiple Task calls):
```

**Agent 1 — Code Quality Review:**
Run the `/review-code` skill on `$TARGET`. Follow the full skill instructions from `.claude/commands/review-code.md`. Return the complete findings table with severity levels (CRITICAL/WARNING/INFO), file:line references, and fix suggestions.

**Agent 2 — Security Review:**
Run the `/review-security` skill on `$TARGET`. Follow the full skill instructions from `.claude/commands/review-security.md`. Return all security findings with severity, vulnerability type, and concrete fix code.

**Agent 3 — Architecture Review:**
Run the `/review-architecture` skill on `$TARGET`. Follow the full skill instructions from `.claude/commands/review-architecture.md`. Return architecture findings with impact assessment (breaking/non-breaking) and refactoring suggestions.

**Agent 4 — QA Engineer Review:**
Run the `/review-qa` skill on `$TARGET`. Follow the full skill instructions from `.claude/commands/qa-review.md`. Return edge cases, regression risks, data integrity concerns, backward compatibility issues, deployment risks, and test scenarios.

**IMPORTANT for each agent:** Read the full skill file (`.claude/commands/<skill>.md`) first, then execute all its phases against `$TARGET`. Return the complete structured output as defined in the skill.

Wait for all 4 agents to complete.

## Phase 2: Collect Review Results

Parse results from all 4 agents. Build a findings summary:

```
For each agent result, extract:
- Number of CRITICAL findings
- Number of WARNING findings
- Number of INFO findings
- Any BLOCK recommendations
```

If **any agent found CRITICAL issues**, flag the pipeline as having blockers.

## Phase 3: Unit Test Generation

Launch 1 agent for unit test generation:

**Agent 5 — Generate Unit Tests:**
Run the `/generate-unit-tests` skill on `$TARGET`. Follow `.claude/commands/generate-unit-tests.md`. Generate TestNG + Mockito unit tests for all changed/new code. Write the test files to the correct locations. Verify they compile. Return the list of generated test files and compilation status.

Wait for completion.

## Phase 4: Regression Testing

Check if the environment supports running tests:

```bash
# Check Java
/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home/bin/java -version 2>&1

# Check Maven
/opt/homebrew/bin/mvn --version 2>&1 | head -1

# Check Maven settings (needed for private deps)
test -f ~/.m2/settings.xml && echo "OK" || echo "MISSING"

# Check Docker (needed for integration tests)
docker info > /dev/null 2>&1 && echo "OK" || echo "NOT RUNNING"
```

**If environment is ready** (Java 17 + Maven + settings.xml):

**Agent 6 — Run Regression Tests:**
Run the `/run-regression` skill on `$TARGET`. Follow `.claude/commands/run-regression.md`. Build dependencies, run mapped unit tests and integration tests (if Docker available), parse results, classify any failures.

**If environment is NOT ready**, skip and note in the report:
```
Regression tests SKIPPED — missing: [settings.xml / Docker / Java 17]
```

## Phase 5: Unified QA Report

Compile all results into a single report:

```markdown
## QA Pipeline Report

### Overview
- **Target:** PR #<number> — <title>
- **Branch:** <branch>
- **Ticket:** MS-XXX (if found)
- **Files changed:** N (M Java files)
- **Pipeline duration:** ~Xm

### Findings Summary

| Skill | Critical | Warning | Info | Status |
|-------|----------|---------|------|--------|
| Code Quality | 0 | 2 | 1 | PASS |
| Security | 1 | 0 | 0 | BLOCK |
| Architecture | 0 | 1 | 0 | PASS |
| QA Review | 0 | 3 | 2 | CAUTION |
| **Total** | **1** | **6** | **3** | |

### Critical Findings (must fix before merge)

#### [SECURITY] <finding title>
- **File:** `path/to/File.java:123`
- **Issue:** <description>
- **Fix:** <suggestion>

#### [CODE] <finding title>
...

### Warnings (should fix)

| # | Skill | Finding | File | Suggestion |
|---|-------|---------|------|------------|
| 1 | Code Quality | Missing null check | File.java:45 | Add null guard |
| 2 | Architecture | Layer violation | Rest.java:78 | Move to service |

### Unit Tests

| Status | Detail |
|--------|--------|
| Generated | N new test files, M test methods |
| Compilation | PASS / FAIL |
| Files | `path/to/NewTest.java` |

### Regression Tests

| Metric | Result |
|--------|--------|
| Unit tests run | X (Y passed, Z failed) |
| Integration tests run | X (Y passed, Z failed) |
| New regressions | N |
| Pre-existing failures | N |
| Status | PASS / FAIL / SKIPPED |

### QA Risk Assessment

| Risk | Severity | Area | Mitigation |
|------|----------|------|------------|
| <from qa-review> | HIGH/MED/LOW | <area> | <action> |

### Test Scenarios (manual verification needed)

| # | Scenario | Expected | Risk |
|---|----------|----------|------|
| <from qa-review> | ... | ... | HIGH |

---

## Merge Recommendation

### **APPROVE** / **APPROVE WITH CONDITIONS** / **REQUEST CHANGES**

**Reasoning:** <1-3 sentences summarizing the overall quality assessment>

**Conditions (if applicable):**
- [ ] Fix N critical findings
- [ ] Address security vulnerability in <file>
- [ ] Verify test scenario #N manually
- [ ] Re-run regression after fixing <test>

### Skill Details

<collapsed sections with full output from each skill>
```

## Merge Recommendation Logic

Determine the final recommendation based on all skill results:

| Condition | Recommendation |
|-----------|---------------|
| 0 critical findings, 0 test failures, no blockers from any skill | **APPROVE** |
| 0 critical findings, minor warnings, regression tests pass or skipped | **APPROVE WITH CONDITIONS** |
| Any critical security finding | **REQUEST CHANGES** |
| Any new regression (test was passing on master, now fails) | **REQUEST CHANGES** |
| Critical code quality issue (NPE risk, data corruption) | **REQUEST CHANGES** |
| Architecture breaking change without migration plan | **REQUEST CHANGES** |
| QA review recommends BLOCK | **REQUEST CHANGES** |
| Only warnings + info findings | **APPROVE WITH CONDITIONS** |

## Error Handling

- If a skill agent fails or times out, note it in the report and continue with other skills
- Never block the entire pipeline because one skill failed
- Report partial results with a note about which skills could not complete
- If the input is invalid (no changes, PR not found), stop early with a clear message
