---
description: Review code changes for quality, null safety, error handling, and metastore-specific patterns
allowed-tools: [Bash, Read, Grep, Glob, Task]
argument-hint: "[branch-name | PR-number | file-or-directory-path]"
---

# Code Quality Review

Review code changes for quality issues specific to the Atlas Metastore codebase. Catches null safety problems, error handling mistakes, missing performance instrumentation, and anti-patterns.

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
   git diff --name-only $(git merge-base HEAD master)..HEAD -- '*.java'
   ```

**How to detect the argument type:**
- Starts with `#` or is purely numeric → PR number
- Contains `/` but doesn't exist as a file path on disk → branch name
- Exists as a file or directory on disk → file path
- Matches a known git branch (`git branch -a | grep <arg>`) → branch name

If no changes are found, inform the user and stop.

## Phase 1: Parallel Research

Get the list of changed files first, then launch **multiple Explore agents in parallel** using the Task tool to gather all context simultaneously:

```
Launch these agents IN PARALLEL (single message, multiple Task calls):
```

**Agent 1 — Diff Analysis & Classification:**
Read the full diff for every changed file. Classify each file by layer (REST, Service, Store, Preprocessor, Util). Identify all new/modified methods and their signatures.

**Agent 2 — Caller & Impact Analysis:**
For each changed file, search the codebase for all callers of modified methods. Determine blast radius — how many other files depend on the changed code. Check if any changed file is in the high-impact list (EntityGraphMapper, PreProcessorUtils, AtlasEntityStoreV2, RequestContext, EntityGraphRetriever).

**Agent 3 — Pattern & Convention Check:**
For each changed file, find similar existing files in the same package to understand established patterns. Check if the changed code follows the same conventions (error handling, logging, null checks) as its neighbors.

Wait for all agents to complete before proceeding.

## Phase 2: Check Null Safety

Scan for these patterns in changed code:

### 2a: Unguarded getAttribute / getVertex
```java
// BAD: No null check
String value = entity.getAttribute("key").toString();
AtlasVertex vertex = context.getVertex(guid);
vertex.getProperty("name");

// GOOD: Null-safe
Object value = entity.getAttribute("key");
if (value != null) { ... }

AtlasVertex vertex = context.getVertex(guid);
if (vertex == null) {
    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
}
```

### 2b: Map/Collection access without containsKey/isEmpty
```java
// BAD
map.get(key).doSomething();
list.get(0);

// GOOD
Object val = map.get(key);
if (val != null) { val.doSomething(); }
if (CollectionUtils.isNotEmpty(list)) { list.get(0); }
```

### 2c: Missing hasAttribute before getAttribute in preprocessors
In preprocessor code, attributes may not exist. Check for:
```java
// BAD: getAttribute without checking existence
String qn = (String) entity.getAttribute(QUALIFIED_NAME);

// GOOD: Check first or handle null
if (entity.hasAttribute(QUALIFIED_NAME)) {
    String qn = (String) entity.getAttribute(QUALIFIED_NAME);
}
```

## Phase 3: Check Error Handling

### 3a: Exception handling patterns
Flag these issues:
- **Catching generic Exception** instead of `AtlasBaseException` — almost always wrong
- **Empty catch blocks** — swallowing errors silently
- **Catching and re-throwing without context** — losing stack trace information
- **Missing AtlasErrorCode** — throwing `AtlasBaseException` without a specific error code
- **Wrong error code** — using `BAD_REQUEST` for server errors, etc.

```java
// BAD: Generic catch
try { ... } catch (Exception e) { LOG.error("failed", e); }

// BAD: Empty catch
try { ... } catch (AtlasBaseException e) { }

// GOOD: Specific catch with proper error code
try { ... } catch (AtlasBaseException e) {
    LOG.error("Failed to process entity {}: {}", guid, e.getMessage());
    throw e;  // Re-throw to caller
}
```

### 3b: Error propagation
- Preprocessors should throw `AtlasBaseException`, never catch and suppress
- REST layer should handle exceptions and return appropriate HTTP status
- Service layer should propagate store-layer exceptions

## Phase 4: Check Performance Instrumentation

### 4a: REST endpoints
Every REST endpoint method should have:
- `@Timed` annotation
- `AtlasPerfTracer` in try-finally block

```java
// REQUIRED pattern in REST methods
@Timed
public Response doSomething(...) throws AtlasBaseException {
    AtlasPerfTracer perf = null;
    try {
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ClassName.methodName");
        }
        // ... logic ...
    } finally {
        AtlasPerfTracer.log(perf);
    }
}
```

### 4b: Preprocessor / Service methods
Heavy operations should use `RequestContext` metric recording:
```java
AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("methodName");
try {
    // ... logic ...
} finally {
    RequestContext.get().endMetricRecord(metric);
}
```

## Phase 5: Check Logging Standards

Flag these issues:
- **LOG.info for debug-level messages** — entry/exit/detail logs should be DEBUG
- **Missing log sanitization** — user-controlled strings in log messages without sanitizing newlines
  ```java
  // BAD: log injection risk
  LOG.info("Processing: " + userInput);
  // GOOD: parameterized + sanitized
  LOG.debug("Processing: {}", sanitize(userInput));
  ```
- **Logging sensitive data** — passwords, tokens, API keys in log statements
- **Missing debug logging** — preprocessor methods should log entry at DEBUG level

## Phase 6: Check Collection / String Safety

Flag direct size checks instead of utility methods:
```java
// BAD
if (list.size() == 0) { ... }
if (str.equals("")) { ... }
if (str.length() == 0) { ... }

// GOOD
if (CollectionUtils.isEmpty(list)) { ... }
if (StringUtils.isEmpty(str)) { ... }
```

## Phase 7: Check Graph Transaction Safety

For store-layer methods that mutate data:
- Must have `@GraphTransaction` annotation
- Must not perform external calls (HTTP, Kafka) inside a graph transaction
- Must not hold transactions open across async operations

## Phase 8: Check Anti-Patterns

Flag these common issues:
- **Mutable static fields** — thread safety risk in a multi-threaded server
- **Hardcoded strings** — entity type names, attribute names should use constants
- **System.out / System.err** — must use SLF4J Logger
- **Thread.sleep in production code** — indicates polling instead of event-driven design
- **new HashMap/ArrayList without initial capacity** — for known-size collections, pre-size
- **String concatenation in loops** — use StringBuilder
- **Unclosed resources** — streams, connections without try-with-resources

## Output Format

Structure your review as:

```markdown
## Code Quality Review

### Summary
- Files reviewed: N
- Total findings: N (X critical, Y warnings, Z info)

### Findings

#### [CRITICAL] <title>
- **File:** `path/to/file.java:lineNumber`
- **Issue:** Description of the problem
- **Risk:** What could go wrong
- **Fix:**
  ```java
  // suggested fix
  ```

#### [WARNING] <title>
...

#### [INFO] <title>
...

### Checklist
- [ ] Null safety: All getAttribute/getVertex calls guarded
- [ ] Error handling: AtlasBaseException with proper error codes
- [ ] Performance: REST endpoints have @Timed and PerfTracer
- [ ] Logging: Proper levels, no sensitive data, sanitized inputs
- [ ] Collections: Using utility methods for empty/null checks
- [ ] Transactions: Store mutations have @GraphTransaction
- [ ] No anti-patterns detected
```

If no issues are found, confirm what was checked and state the code looks good.
