---
name: review-pr
description: >
  Review pull request code for quality, security, performance, and Atlas-specific patterns.
  Posts a tracked review comment with issue resolution history.
user-invocable: true
allowed-tools: [Skill, Read, Glob, Grep, Bash]
argument-hint: "[PR-number]"
---

# PR Code Review for Atlas Metastore

Review code changes for the Atlas Metastore repository — a Java 17 / Maven project that implements a metadata catalog built on JanusGraph, Cassandra, Elasticsearch, and Kafka.

## Phase 1: Identify Changes to Review

### 1.1 Get diff statistics

```bash
PR_NUMBER=$ARGUMENTS
if [ -z "$PR_NUMBER" ]; then
  PR_NUMBER=$(gh pr view --json number -q .number 2>/dev/null)
fi

# Get statistics for auto-approve decision
gh pr diff $PR_NUMBER --stat 2>/dev/null || git diff main --stat

# List changed files
gh pr diff $PR_NUMBER --name-only 2>/dev/null || git diff main...HEAD --name-only

# Get the full diff
gh pr diff $PR_NUMBER 2>/dev/null || git diff main...HEAD
```

### 1.2 Categorize changes

Focus areas by file location:

**Repository module (`repository/`)**
- Entity preprocessors (`preprocessor/`) — QN generation, entity lifecycle
- Store layer (`store/graph/v2/`) — JanusGraph CRUD, transactions
- Services (`glossary/`, `tasks/`) — Business logic
- Utilities (`util/`) — Shared helpers

**REST API (`webapp/`)**
- Endpoint definitions, authorization, request handling

**API Models (`intg/`)**
- Entity models, client libraries, REST contracts

**Infrastructure**
- Helm charts (`helm/`) — Deployment configs
- CI/CD (`.github/workflows/`) — Pipeline changes
- Config (`addons/`) — TypeDefs, policies, ES mappings

## Phase 2: Review Focus Areas

### 2a: Java Code Quality
- Thread safety (Atlas is multi-threaded)
- Null checks — all `getAttribute` / `getVertex` calls must be guarded
- Resource cleanup — try-with-resources for streams, graph transactions
- Consistent use of existing patterns (PreProcessor pattern, store pattern)
- `CollectionUtils.isEmpty` / `StringUtils.isEmpty` instead of `.size() == 0`

### 2b: Graph Database Concerns
- JanusGraph transaction handling (commit/rollback)
- `@GraphTransaction` annotation on store-layer mutations
- No external calls (HTTP, Kafka) inside graph transactions
- Vertex/Edge property access patterns
- Index usage and query efficiency

### 2c: Security
- Authorization checks in REST endpoints
- Input validation (especially qualifiedName, GUID parameters)
- No secrets or credentials in code
- Log sanitization for user-controlled strings

### 2d: Performance
- N+1 query patterns in graph traversals
- Unnecessary object creation in hot paths
- Elasticsearch query efficiency
- Proper use of caching
- REST endpoints must have `@Timed` + `AtlasPerfTracer`
- Heavy operations should use `RequestContext` metric recording

### 2e: Error Handling
- `AtlasBaseException` with specific error codes (not generic `Exception`)
- No empty catch blocks
- Preprocessors throw, never catch and suppress
- REST layer handles and returns appropriate HTTP status

### 2f: Backward Compatibility
- REST API contract changes
- TypeDef changes that could break existing entities
- Kafka message format changes

### 2g: Helm / CI Changes (if applicable)
- Chart version bumps
- Values.yaml schema changes
- Workflow correctness

## Phase 3: Critical Issues (Block Auto-Approve)

Issues that BLOCK auto-approval — mark as **Critical**:

- NullPointerException risks (unguarded getAttribute/getVertex)
- Security vulnerabilities (missing auth checks, injection risks)
- Resource leaks (unclosed streams, transactions)
- Race conditions (mutable static fields, unsynchronized shared state)
- Data corruption risks (missing @GraphTransaction, wrong error propagation)
- Breaking API contract changes without migration

## Phase 4: Review Comment Format

When posting a review comment, use this format:

```markdown
<!-- CLAUDE_REVIEW -->
<!-- REVIEW_DATA
{
  "review_id": "PR-{number}-{timestamp}",
  "commit": "{head_sha}",
  "timestamp": "{ISO_timestamp}",
  "issues": [
    {
      "id": "issue-1",
      "file": "repository/src/main/java/.../SomeFile.java",
      "line": 42,
      "severity": "Critical",
      "description": "Unguarded getAttribute call may throw NPE",
      "status": "pending"
    }
  ]
}
-->

## Code Review - PR #{number}

### Review Summary

{1-2 sentence summary of changes and overall assessment}

### Issue Tracking

| Status | Count |
|--------|-------|
| Resolved | {resolved_count} |
| Pending | {pending_count} |
| New | {new_count} |

### Resolved Issues

{list with strikethrough, or "None"}

### Pending Issues

#### Critical (must fix)

{critical issues with file:line references, or "None"}

#### Warning (should fix)

{warning issues, or "None"}

### New Issues Found

{new issues not in previous review, or "None"}

### Suggestions

{optional improvements — only if genuinely useful}
```

## Phase 5: Updating Existing Comments

When the workflow detects a previous review comment (containing `<!-- CLAUDE_REVIEW -->`):

### 1. Find the existing comment ID

```bash
PR_NUMBER=$(gh pr view --json number -q .number)
COMMENT_ID=$(gh api repos/{owner}/{repo}/issues/${PR_NUMBER}/comments \
  --jq '.[] | select(.body | contains("<!-- CLAUDE_REVIEW -->")) | .id' | head -1)
```

### 2. Parse previous issues from REVIEW_DATA JSON

Extract the JSON between `<!-- REVIEW_DATA` and `-->` markers.

### 3. Check if each previous issue is fixed

For each previous issue:
- Get current diff: `git diff main -- {file}`
- Check if the problematic pattern still exists at/near the line
- If the pattern is gone or line significantly changed -> mark as "resolved"
- If the pattern still exists -> keep as "pending"

### 4. Run a fresh review to find new issues

Compare new findings against previous issues to identify what's new.

### 5. Update the comment

```bash
gh api repos/{owner}/{repo}/issues/comments/${COMMENT_ID} \
  --method PATCH \
  -f body="$(cat <<'EOF'
{updated review comment}
EOF
)"
```

## Phase 6: Review Decision

After completing the code review, decide whether to **APPROVE** or **COMMENT** (defer to human).

### Auto-Approve Criteria

**AUTO-APPROVE only when ALL of these apply:**

| Criteria | Threshold |
|----------|-----------|
| Files changed | <=5 |
| Lines changed | <=200 (check `git diff --stat`) |
| Critical issues | None |
| Warning issues | None |
| PR type | Documentation, config, minor fix |

**NEVER AUTO-APPROVE when ANY of these apply:**

| Condition | Rationale |
|-----------|-----------|
| >5 files changed | Scope too large for automated review |
| >200 lines changed | Impact too significant |
| Changes to preprocessors | Core entity lifecycle logic |
| Changes to EntityGraphMapper | High-impact core class |
| Changes to store layer | Data integrity risk |
| Changes to REST endpoints | API contract changes |
| Changes to Helm charts | Infrastructure changes |
| Changes to CI workflows | Pipeline changes |
| Any critical or warning issues | Quality gate not met |

## Phase 7: Submitting the Review

### Post the review comment first (with tracking markers)

```bash
PR_NUMBER=$(gh pr view --json number -q .number)
gh pr comment ${PR_NUMBER} --body "$(cat <<'EOF'
{formatted review comment from Phase 4}
EOF
)"
```

### If approving

```bash
gh pr review ${PR_NUMBER} --approve --body "$(cat <<'EOF'
## Approved

**Auto-approval criteria met:**
- Files changed: X (<=5)
- Lines changed: X (<=200)
- No infrastructure changes
- No critical/warning issues

See review comment for details.
EOF
)"
```

### If deferring to human

```bash
gh pr review ${PR_NUMBER} --comment --body "$(cat <<'EOF'
## Code Review Complete

This PR requires human review due to:
- [List specific reasons]

See review comment for detailed findings.
EOF
)"
```

## Review Rules — IMPORTANT

- ONLY leave inline comments for: critical bugs, security vulnerabilities, unexpected runtime issues (NPE, resource leaks, race conditions), or concrete improvement suggestions with code examples
- Every inline comment MUST reference the specific code line and explain WHY it's a problem
- NEVER leave positive/affirmation comments like "Good change" or "Nice refactoring" — these are noise
- NEVER comment on trivial style, formatting, or naming unless it introduces a real bug
- If you have a suggestion, include a concrete code snippet showing the improvement
- If the PR is clean with no real issues, just post a short summary — ZERO inline comments
- Fewer high-quality comments >> many low-value comments

## Safety Rules

1. **When in doubt, use COMMENT** (defer to human)
2. **Never approve PRs with critical or warning issues**
3. **Developer always merges** — Claude only approves, never merges
4. **Verify thresholds with `git diff --stat`** before approving
5. **No approval for core business logic changes** (preprocessors, store layer, EntityGraphMapper)

$ARGUMENTS
