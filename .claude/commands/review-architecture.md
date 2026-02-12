---
description: Review code changes for architecture violations, layer boundaries, and structural quality
allowed-tools: [Bash, Read, Grep, Glob, Task]
argument-hint: "[branch-name | PR-number | file-or-directory-path]"
---

# Architecture Review

Review code changes for structural quality — layer violations, preprocessor pattern compliance, coupling issues, breaking changes, and naming conventions in the Atlas Metastore codebase.

## Input Resolution

Determine what to review based on `$ARGUMENTS`:

1. **PR number** (e.g., `#123` or `123`): Fetch the PR diff
   ```bash
   gh pr diff <number> --repo atlanhq/atlas-metastore
   ```
2. **Branch name** (e.g., `feature/my-branch`): Diff that branch against master
   ```bash
   git fetch origin <branch> 2>/dev/null
   git diff $(git merge-base master origin/<branch>)..origin/<branch>
   ```
3. **File or directory path**: Review only those files
   ```bash
   git diff $(git merge-base HEAD master)..HEAD -- <path>
   ```
4. **No argument**: Review the current branch diff against master
   ```bash
   git diff --name-only $(git merge-base HEAD master)..HEAD -- '*.java' '*.json'
   ```

**How to detect the argument type:**
- Starts with `#` or is purely numeric → PR number
- Contains `/` but doesn't exist as a file path on disk → branch name
- Exists as a file or directory on disk → file path
- Matches a known git branch (`git branch -a | grep <arg>`) → branch name

If no changes are found, inform the user and stop.

## Phase 1: Parallel Research

Get the list of changed files first, then launch **multiple Explore agents in parallel** using the Task tool:

```
Launch these agents IN PARALLEL (single message, multiple Task calls):
```

**Agent 1 — Layer Classification & Boundary Check:**
Read all changed files. Classify each into its architectural layer (REST, Service, Store, Preprocessor, Model, TypeDef, Config). For each file, check if it imports from or calls into layers it shouldn't (e.g., REST importing store classes, preprocessors calling REST utilities). Read `.claude/code-quality-rules.json` for layer rules.

**Agent 2 — Coupling & Dependency Analysis:**
Check all new `import` statements in the diff. Find cross-module dependencies (repository→webapp is wrong, intg→repository is wrong). For each changed class, count constructor parameters and check for god-class smell (>7 deps). Search for any circular package dependencies introduced.

**Agent 3 — Breaking Change Detection:**
Check for changes to: TypeDefs in `addons/models/`, REST endpoint signatures in `webapp/`, public method signatures in store/service classes, QualifiedName generation patterns. For each changed public method, grep for all callers to see if they're updated in the same diff.

**Agent 4 — Preprocessor & Convention Compliance:**
For any new/changed preprocessor files, read the full file and check: implements PreProcessor interface, handles CREATE/UPDATE/DELETE in switch, has proper metric recording, QN generation follows CLAUDE.md patterns. Also check naming conventions for all new classes/methods.

Wait for all agents to complete before proceeding.

## Phase 2: Check Layer Boundary Violations

### 2a: REST layer violations
The REST layer should ONLY handle:
- Input validation (`Servlets.validateQueryParamLength()`)
- Authorization (`AtlasAuthorizationUtils.verifyAccess()`)
- Performance tracing (`AtlasPerfTracer`)
- Delegation to service/store layer

**Flag** if REST layer code:
- Contains business logic (conditional entity manipulation, attribute enrichment)
- Directly accesses graph vertices/edges
- Calls preprocessor methods directly
- Contains entity creation/transformation logic

### 2b: Preprocessor layer violations
Preprocessors should ONLY handle:
- Attribute validation and enrichment
- QualifiedName generation
- Relationship setup
- Pre-mutation checks

**Flag** if preprocessor code:
- Makes HTTP/REST calls
- Sends Kafka notifications directly
- Reads from Elasticsearch directly
- Manages graph transactions
- Accesses `RequestContext.get().getUser()` for authorization

### 2c: Store layer violations
Stores should ONLY handle graph CRUD operations, vertex/edge management, transaction management.

**Flag** if store code:
- Contains business validation logic
- Formats HTTP responses
- Contains entity-type-specific logic without delegation to preprocessor

### 2d: Service layer violations
Services should orchestrate stores and preprocessors, not duplicate their work.

## Phase 3: Preprocessor Pattern Compliance

For new or modified preprocessor files, verify the standard pattern:

### Required structure
```java
public class XxxPreProcessor implements PreProcessor {
    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        AtlasEntity entity = (AtlasEntity) entityStruct;
        switch (operation) {
            case CREATE:
                processCreate(entity, vertex, operation);
                break;
            case UPDATE:
                processUpdate(entity, vertex, operation);
                break;
        }
    }
}
```

**Flag** if:
- Missing `CREATE` or `UPDATE` case handling
- Not implementing the `PreProcessor` interface
- Missing metric recording in heavy methods
- QualifiedName generation doesn't follow CLAUDE.md patterns

### New entity types
If a new entity type is added:
- **MUST** have a corresponding preprocessor class
- **MUST** be registered in the preprocessor registry
- **MUST** have QualifiedName generation logic
- **SHOULD** have integration test coverage

## Phase 4: Coupling Analysis

### 4a: Cross-module dependencies
**Flag:**
- `repository` module importing from `webapp` (wrong direction)
- `intg` module importing from `repository` (wrong direction)
- `common` importing from any specific module
- Circular dependencies between packages

### 4b: New class dependencies
For new/modified classes, check constructor parameters. Flag if:
- More than 7 constructor parameters (god class smell)
- Injecting concrete classes instead of interfaces
- Injecting services from unrelated domains

### 4c: Utility sprawl
**Flag** new utility classes when `PreProcessorUtils` or existing utils already have similar methods.

## Phase 5: Breaking Change Detection

### 5a: TypeDef changes
- **BREAKING:** Removing/renaming attributes, changing attribute types, changing relationship cardinality
- **SAFE:** Adding new optional attributes, adding new entity types

### 5b: REST API changes
- **BREAKING:** Removing endpoints, changing URL paths, removing response fields, adding required params
- **SAFE:** Adding optional params, adding new response fields, adding new endpoints

### 5c: QualifiedName pattern changes
**CRITICAL breaking change** if the QN generation pattern is modified for an existing entity type.

### 5d: Internal API changes
If public method signatures change, check all callers are updated in the same PR.

## Phase 6: Transaction Boundary Review

### 6a: @GraphTransaction placement
- Store-layer mutation methods MUST have `@GraphTransaction`
- Preprocessors should NOT have `@GraphTransaction`

### 6b: Transaction scope
**Flag** if a `@GraphTransaction` method makes external HTTP calls, sends Kafka notifications, or calls another `@GraphTransaction` method.

## Phase 7: Naming Convention Review

### 7a: Class naming
- Preprocessors: `{EntityType}PreProcessor.java`
- REST: `{Domain}REST.java`
- Services: `{Domain}Service.java`
- Stores: `Atlas{Domain}Store{Version}.java`
- Tests: `{ClassName}Test.java` (unit) or `{Domain}IntegrationTest.java` (integration)

### 7b: Constant naming
- Entity type names: PascalCase
- Attribute names: camelCase
- Config keys: SCREAMING_SNAKE_CASE
- Error codes: SCREAMING_SNAKE_CASE

## Output Format

```markdown
## Architecture Review

### Summary
- Files reviewed: N
- Layers touched: [REST, Service, Store, Preprocessor, ...]
- Breaking changes: Yes/No
- Layer violations: N found

### Findings

#### [CRITICAL] <title>
- **File:** `path/to/file.java:lineNumber`
- **Category:** Layer Violation / Breaking Change / Coupling / Naming / Transaction
- **Issue:** Description
- **Impact:** Breaking / Non-breaking
- **Fix:** Suggested refactoring

### Architecture Checklist
- [ ] Layer boundaries respected
- [ ] Preprocessor pattern followed
- [ ] No new cross-module coupling violations
- [ ] No breaking TypeDef or API changes
- [ ] Transaction boundaries correct
- [ ] Naming conventions followed
- [ ] New entity types have preprocessors and tests

### Impact Assessment
- **Breaking changes:** [list or "None"]
- **Migration required:** Yes/No
- **Downstream impact:** [which other services/clients are affected]
```
