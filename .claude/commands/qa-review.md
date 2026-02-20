---
description: QA engineer review — edge cases, regression risks, data integrity, and merge recommendation
allowed-tools: [Bash, Read, Grep, Glob, Task]
argument-hint: "[branch-name | PR-number | file-or-directory-path]"
---

# QA Engineer Review

Think like a senior QA engineer reviewing code changes. Focus on what could go wrong in production — edge cases, regressions, data integrity issues, deployment risks, and backward compatibility. Provide a merge recommendation.

## Input Resolution

Determine what to review based on `$ARGUMENTS`:

1. **PR number** (e.g., `#123` or `123`): Fetch the PR diff
   ```bash
   gh pr diff <number> --repo atlanhq/atlas-metastore
   ```
2. **Branch name** (e.g., `feature/my-branch`): Diff that branch against master
   ```bash
   git rev-parse --verify <branch> 2>/dev/null || git rev-parse --verify origin/<branch> 2>/dev/null
   git fetch origin <branch> 2>/dev/null
   git diff $(git merge-base master origin/<branch>)..origin/<branch>
   ```
3. **File or directory path**: Review only those files
   ```bash
   git diff $(git merge-base HEAD master)..HEAD -- <path>
   ```
4. **No argument**: Review the current branch diff against master
   ```bash
   git diff --name-only $(git merge-base HEAD master)..HEAD -- '*.java' '*.json' '*.xml' '*.properties'
   ```

**How to detect the argument type:**
- Starts with `#` or is purely numeric → PR number
- Contains `/` but doesn't exist as a file path on disk → branch name
- Exists as a file or directory on disk → file path
- Matches a known git branch (`git branch -a | grep <arg>`) → branch name

If no changes are found, inform the user and stop.

## Phase 0: Extract Linear Ticket Context

Extract Linear ticket IDs to get acceptance criteria, bug details, and expected behavior that inform QA scenarios.

### 0a: Find ticket IDs
Search for Linear ticket IDs (pattern: `MS-\d+`, `METASTORE-\d+`, or similar) in:
```bash
# From PR title and description
gh pr view <number> --repo atlanhq/atlas-metastore --json title,body,headRefName 2>/dev/null

# From branch name (e.g., ms-579-fix-bigquery-error)
git branch --show-current | grep -oiE '(ms|metastore)-[0-9]+'

# From commit messages
git log --oneline $(git merge-base HEAD master)..HEAD | grep -oiE '(ms|metastore)-[0-9]+'
```

### 0b: Fetch ticket details
If a ticket ID is found, fetch its details from Linear:
```bash
# Try Linear CLI if available
linear issue view <TICKET_ID> 2>/dev/null

# Or use Linear API via curl
curl -s -X POST "https://api.linear.app/graphql" \
  -H "Content-Type: application/json" \
  -H "Authorization: <LINEAR_API_KEY>" \
  -d '{"query": "{ issueSearch(filter: { identifier: { eq: \"<TICKET_ID>\" } }) { nodes { title description state { name } priority labels { nodes { name } } comments { nodes { body } } } } }"}'
```

If Linear API key is not configured or fetch fails, note it and continue — ticket context is supplementary, not required.

### 0c: Extract QA-relevant context from ticket
From the ticket, extract:
- **Acceptance criteria** → become test scenarios
- **Bug description** → informs regression test cases
- **Expected behavior** → validate against code changes
- **Related tickets** → broader impact awareness
- **Labels** (e.g., "bug", "feature", "regression") → risk classification

Include this context in the review output as a "Ticket Context" section.

## Phase 1: Parallel Research

Get the list of changed files first, then launch **multiple Explore agents in parallel** using the Task tool:

```
Launch these agents IN PARALLEL (single message, multiple Task calls):
```

**Agent 1 — Change Understanding:**
Read the full diff for all changed files. Read commit messages. Classify the change type (New Feature / Bug Fix / Refactor / Config Change / TypeDef Change). Identify the blast radius — which entity types, APIs, and user-facing behaviors are affected. If a Linear ticket was found, compare the change against acceptance criteria.

**Agent 2 — Blast Radius & Caller Analysis:**
For each changed method, search the entire codebase for all callers. Check if any changed file is in the high-impact list (EntityGraphMapper, PreProcessorUtils, AtlasEntityStoreV2, AtlasTypeRegistry, RequestContext, AtlasRelationshipStoreV2, EntityGraphRetriever). Count how many other files depend on the changed code.

**Agent 3 — Test Coverage Gap Analysis:**
Read `.claude/test-coverage-map.json`. For each changed file, find the mapped integration tests and check if they exist. For each changed method, search for unit tests that cover it. Identify methods with NO test coverage at all.

**Agent 4 — Backward Compatibility Check:**
Check for: changed REST endpoint signatures, removed/renamed response fields, modified TypeDefs (attribute removals, type changes), changed QualifiedName patterns, changed Kafka message formats, changed error codes. For each finding, grep for external consumers.

Wait for all agents to complete before proceeding.

## Phase 2: Edge Case Analysis

For each changed method/logic path, identify edge cases:

### 2a: Null and empty inputs
- What happens if an attribute value is `null`?
- What happens if a list attribute is empty `[]`?
- What happens if a string attribute is `""` (empty string)?
- What happens if a required relationship (e.g., glossary anchor) doesn't exist?

### 2b: Boundary conditions
- Maximum length strings in `name` or `qualifiedName` attributes
- Unicode / special characters in entity names
- Very large collections (1000+ items in a bulk operation)
- Deeply nested hierarchies (categories 10+ levels deep)
- Circular references in relationships

### 2c: Concurrent operations
- Two users creating entities with the same `qualifiedName` simultaneously
- Update and delete of the same entity racing
- Bulk operations partially failing
- `RequestContext` thread-local correctness in async/parallel code paths

### 2d: Type edge cases
- Entity types with no preprocessor defined
- Extra attributes not in the typedef schema
- Relationship attributes pointing to soft-deleted entities
- Mixed entity types in a single bulk request

## Phase 3: Regression Risk Assessment

### 3a: Shared code changes
If the change touches high-impact files, flag HIGH regression risk:

| File | Impact |
|------|--------|
| `EntityGraphMapper.java` | ALL entity create/update operations |
| `PreProcessorUtils.java` | ALL preprocessors |
| `AtlasEntityStoreV2.java` | ALL entity store operations |
| `AtlasTypeRegistry.java` | ALL type resolution |
| `RequestContext.java` | ALL request processing |
| `AtlasRelationshipStoreV2.java` | ALL relationship operations |
| `EntityGraphRetriever.java` | ALL entity reads |

### 3b: Changed defaults or constants
Flag changes to: default attribute values, error codes, constants, feature flag defaults, configuration property defaults.

### 3c: Changed method signatures
Flag: parameter changes, return type changes, exception declaration changes, null contract changes.

### 3d: Behavioral changes
Look for: changed operation ordering, changed condition logic, added/removed side effects, changed transaction scope.

## Phase 4: Data Integrity Analysis

### 4a: Partial failure states
For create/update operations — are orphaned vertices/edges cleaned up? Are related entities left consistent?

### 4b: Cascade consistency
Does deleting a glossary clean up terms? Does updating a domain QN cascade to children?

### 4c: Index consistency
Are ES index updates guaranteed after graph mutations?

### 4d: Idempotency
Can the same request be safely retried?

## Phase 5: Backward Compatibility

### 5a: API compatibility
Changed response shapes, new required fields, changed error codes.

### 5b: TypeDef compatibility
New required attributes on existing types, changed attribute types, removed attributes.

### 5c: Wire format compatibility
Kafka message format changes, notification payload changes, cache key format changes.

## Phase 6: Deployment Risk Assessment

Flag: new feature flags without defaults, database schema changes, ES mapping changes, new required config properties, bootstrap data changes.

### Rollback safety
Can this change be safely rolled back? Does it create irreversible state?

## Phase 7: Test Scenario Generation

Based on findings (and Linear ticket acceptance criteria if available), generate specific test scenarios:

```markdown
### Manual Test Scenarios

| # | Scenario | Steps | Expected Result | Risk Level | Source |
|---|----------|-------|-----------------|------------|--------|
| 1 | [name] | 1. Do X 2. Do Y | Z happens | HIGH/MED/LOW | Edge case / Ticket AC / Regression |
```

## Phase 8: Merge Recommendation

## Output Format

```markdown
## QA Review

### Ticket Context (if found)
- **Ticket:** MS-XXX — <title>
- **Type:** Bug Fix / Feature / etc.
- **Acceptance Criteria:**
  - [ ] AC1 — covered by code change: Yes/No
  - [ ] AC2 — covered by code change: Yes/No
- **Notes from ticket:** [relevant context]

### Change Summary
- **Type:** New Feature / Bug Fix / Refactor / Config Change
- **Blast Radius:** [which entity types/APIs/behaviors affected]
- **Files Changed:** N

### Edge Cases Found

#### [HIGH] <edge case title>
- **Scenario:** What triggers this
- **Impact:** What goes wrong
- **Suggestion:** How to handle it

#### [MEDIUM] <edge case title>
...

### Regression Risks

| Risk | Severity | Affected Area | Mitigation |
|------|----------|---------------|------------|
| [description] | HIGH/MED/LOW | [area] | [what to do] |

### Data Integrity Concerns
[List any partial failure, cascade, or index consistency issues]

### Backward Compatibility
[List any API, TypeDef, or wire format breaking changes]

### Deployment Notes
[Any deployment-specific concerns, rollback plan, required config]

### Test Scenarios

| # | Scenario | Steps | Expected | Risk | Source |
|---|----------|-------|----------|------|--------|
| 1 | ... | ... | ... | HIGH | Ticket AC / Edge case |

### Merge Recommendation

**SAFE TO MERGE** / **MERGE WITH CAUTION** / **BLOCK — NEEDS CHANGES**

Reasoning: [1-3 sentences explaining the recommendation]

Conditions (if CAUTION):
- [ ] [Condition that must be met before merge]
- [ ] [Another condition]
```
