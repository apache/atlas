---
description: Analyze code changes and suggest integration tests, informed by Linear ticket context
allowed-tools: [Bash, Read, Grep, Glob, Task]
argument-hint: "[branch-name | PR-number | file-or-directory-path]"
---

# Suggest Integration Tests for Code Changes

Analyze code changes, pull context from Linear tickets when available, and recommend which integration tests should cover these changes. Identify coverage gaps and suggest specific test cases to add.

## Input Resolution

Determine what to analyze based on `$ARGUMENTS`:

1. **PR number** (e.g., `#123` or `123`): Get changed files from PR
   ```bash
   gh pr diff <number> --repo atlanhq/atlas-metastore --name-only
   ```
2. **Branch name** (e.g., `feature/my-branch`): Get changed files on that branch
   ```bash
   git fetch origin <branch> 2>/dev/null
   git diff --name-only $(git merge-base master origin/<branch>)..origin/<branch>
   ```
3. **File or directory path**: Scope to those specific files
4. **No argument**: Use current branch diff against master
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

Linear tickets often contain acceptance criteria, bug repro steps, and expected behaviors that should drive test scenarios.

### 0a: Find ticket IDs
Search for Linear ticket IDs (pattern: `MS-\d+`, `METASTORE-\d+`) in:
```bash
# From PR title and description
gh pr view <number> --repo atlanhq/atlas-metastore --json title,body,headRefName 2>/dev/null

# From branch name
git branch --show-current | grep -oiE '(ms|metastore)-[0-9]+'

# From commit messages
git log --oneline $(git merge-base HEAD master)..HEAD | grep -oiE '(ms|metastore)-[0-9]+'
```

### 0b: Fetch ticket details
If found, fetch from Linear:
```bash
linear issue view <TICKET_ID> 2>/dev/null

# Or via API
curl -s -X POST "https://api.linear.app/graphql" \
  -H "Content-Type: application/json" \
  -H "Authorization: <LINEAR_API_KEY>" \
  -d '{"query": "{ issueSearch(filter: { identifier: { eq: \"<TICKET_ID>\" } }) { nodes { title description state { name } priority labels { nodes { name } } } } }"}'
```

### 0c: Extract test-relevant context
From the ticket, extract:
- **Acceptance criteria** → each AC should have at least one test
- **Bug repro steps** → convert into a regression test scenario
- **Expected behavior** → assertion targets for tests
- **Edge cases mentioned** → additional test scenarios beyond the happy path

## Phase 1: Parallel Research

Launch **multiple Explore agents in parallel** using the Task tool:

```
Launch these agents IN PARALLEL (single message, multiple Task calls):
```

**Agent 1 — Diff Analysis & Endpoint Mapping:**
Read all diffs. For each changed file, map to affected REST API endpoints using the file-to-endpoint mapping. Identify which entity types are affected. Read `.claude/test-coverage-map.json` for official mappings.

**Agent 2 — Existing Test Coverage Scan:**
For each mapped integration test class, read the test file and list all existing test methods. Identify what scenarios are already covered (CRUD, error cases, edge cases, etc.) and what gaps exist for the specific code changes.

**Agent 3 — Convention & Pattern Discovery:**
Read 2-3 existing integration test classes to understand the test writing patterns (setup, assertion style, ordering, shared state). Find the base test class and understand the test infrastructure (client setup, cleanup).

**Agent 4 — Linear Ticket Test Scenarios** (only if ticket ID found):
Analyze the Linear ticket description, acceptance criteria, and comments. Generate specific test scenarios that validate each acceptance criterion. Cross-reference with the code changes to ensure coverage.

Wait for all agents to complete before proceeding.

## Phase 2: Map Changes to Test Classes

### Code Path to Test Mapping

| Changed Path Pattern | Required Integration Tests |
|---------------------|---------------------------|
| `**/glossary/**` | GlossaryIntegrationTest, GlossaryExtendedIntegrationTest |
| `**/preprocessor/glossary/**` | GlossaryIntegrationTest, GlossaryExtendedIntegrationTest |
| `**/preprocessor/datamesh/**` | DataMeshIntegrationTest |
| `**/preprocessor/accesscontrol/**` | PersonaPurposeIntegrationTest |
| `**/preprocessor/sql/**` | SQLAssetHierarchyIntegrationTest |
| `**/EntityGraphMapper.java` | EntityCrudIntegrationTest |
| `**/AtlasRelationshipStoreV2.java` | RelationshipIntegrationTest |
| `**/classification/**` or `**/tag/**` | ClassificationIntegrationTest |
| `**/lineage/**` | LineageIntegrationTest |
| `**/discovery/**` or `**/search/**` | SearchIntegrationTest |
| `**/AtlasTypeDefGraphStoreV2.java` | TypeDefIntegrationTest |
| `**/businessmetadata/**` | BusinessMetadataIntegrationTest |
| `**/label/**` | LabelIntegrationTest |

## Phase 3: Analyze Each Changed File

For each changed file:
1. Read the diff to understand what changed
2. Identify the functionality affected (new methods, modified logic, error handling)
3. Check if existing tests cover the change

## Phase 4: Generate Recommendations

Output a structured report:

```markdown
## Integration Test Recommendations

### Ticket Context (if found)
- **Ticket:** MS-XXX — <title>
- **Acceptance Criteria → Test Mapping:**
  | AC | Test Scenario | Existing Coverage | Status |
  |----|--------------|-------------------|--------|
  | AC1: [description] | testXxx | GlossaryIntegrationTest.testCreate | Covered |
  | AC2: [description] | testYyy | None | NEEDS TEST |

### Changed Files Analysis

| File | Change Type | Recommended Tests |
|------|-------------|-------------------|
| path/to/file.java | Modified | TestClass1, TestClass2 |

### Existing Test Coverage

| Test Class | Covers Changes | Gaps Identified |
|------------|----------------|-----------------|
| EntityCrudIntegrationTest | Partial | Missing: bulk update with null values |

### Recommended New Test Cases

#### 1. [Test Class Name]

**New test method:** `testDescriptiveName`
**Source:** Ticket AC / Code change / Edge case

**Purpose:** [What this test verifies]

**Suggested implementation:**
```java
@Test
@Order(N)
void testDescriptiveName() throws AtlasServiceException {
    // Setup
    // Action
    // Assert
}
```

### Coverage Summary

- **Well covered:** [List of changes with good test coverage]
- **Partial coverage:** [List of changes with some coverage]
- **No coverage:** [List of changes that need new tests]

### Priority

1. **Must have:** [Critical tests — from ticket ACs and regression risks]
2. **Should have:** [Important but not blocking]
3. **Nice to have:** [Future improvements]
```

## Test Writing Guidelines

Follow existing patterns:

### Entity CRUD Test Pattern
```java
@Test
@Order(1)
void testCreateEntity() throws AtlasServiceException {
    AtlasEntity entity = new AtlasEntity("Table");
    entity.setAttribute("qualifiedName", "test://integration/table/" + System.currentTimeMillis());
    entity.setAttribute("name", "test-table");
    EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
    assertNotNull(response.getFirstEntityCreated());
    createdGuid = response.getFirstEntityCreated().getGuid();
}
```

### Error Case Test Pattern
```java
@Test
@Order(99)
void testCreateEntityMissingRequired() {
    AtlasEntity entity = new AtlasEntity("Table");
    entity.setAttribute("name", "missing-qn-table");
    assertThrows(AtlasServiceException.class, () -> {
        atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
    });
}
```

## Running Suggested Tests Locally

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn test -pl webapp \
  -Dtest="TestClass1,TestClass2" \
  -Drat.skip=true -Dsurefire.failIfNoSpecifiedTests=false
```
