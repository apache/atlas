---
description: Review code changes for security vulnerabilities, injection risks, and authorization gaps
allowed-tools: [Bash, Read, Grep, Glob, Task]
argument-hint: "[branch-name | PR-number | file-or-directory-path]"
---

# Security Review

Review code changes for security vulnerabilities specific to the Atlas Metastore codebase. Checks for injection risks, missing authorization, sensitive data exposure, and OWASP Top 10 issues.

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

Get the list of changed files first, then launch **multiple Explore agents in parallel** using the Task tool:

```
Launch these agents IN PARALLEL (single message, multiple Task calls):
```

**Agent 1 — Input Flow Tracing:**
For each changed file, trace how user input flows into the code. Start from REST endpoints, follow through service/store layers to graph queries. Identify every point where external data enters the system and where it's used in queries, logs, or responses.

**Agent 2 — Authorization Audit:**
For each changed REST endpoint or store method, find the `verifyAccess()` call (or lack thereof). Check which `AtlasPrivilege` is used. Search for any code paths that bypass authorization.

**Agent 3 — Sensitive Data & Dependency Scan:**
Search changed code for logging statements with user data, error messages that leak internals, and check any `pom.xml` changes for new dependencies. Also check for deserialization patterns.

Wait for all agents to complete before proceeding.

## Phase 2: Input Validation

### 2a: REST parameter validation
Every REST endpoint that accepts user input MUST validate it:

```java
// REQUIRED: Validate path/query params
Servlets.validateQueryParamLength("guid", guid);
```

**Flag:** Any new REST method parameter that is not validated before use.

### 2b: User input in queries
Search for user-controlled values flowing into:
- **Graph traversals** — JanusGraph Gremlin queries built with string concatenation
- **Elasticsearch queries** — DSL queries with unescaped user input
- **Cassandra queries** — CQL queries with string interpolation

```java
// BAD: Direct interpolation in graph query
String query = "g.V().has('name', '" + userInput + "')";  // CRITICAL: injection

// GOOD: Use parameterized builders
SearchParams params = new SearchParams();
params.setQuery(sanitizedInput);
```

### 2c: Type coercion safety
Flag unsafe casts of user-provided data:
```java
// BAD: ClassCastException risk from user input
String value = (String) entity.getAttribute(userProvidedKey);

// GOOD: Safe type checking
Object value = entity.getAttribute(key);
if (value instanceof String) { ... }
```

## Phase 3: Authorization Checks

### 3a: Missing authorization
Every REST endpoint that reads, creates, updates, or deletes entities MUST call:
```java
AtlasAuthorizationUtils.verifyAccess(
    new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, ...)
);
```

**Flag:** New REST endpoints or modified operations without `verifyAccess()` calls.

### 3b: Correct privilege levels
Verify the right `AtlasPrivilege` is used:
- `ENTITY_READ` for GET operations
- `ENTITY_CREATE` for POST/PUT that creates
- `ENTITY_UPDATE` for POST/PUT that modifies
- `ENTITY_DELETE` for DELETE operations
- `TYPE_READ`, `TYPE_CREATE`, `TYPE_UPDATE`, `TYPE_DELETE` for typedef operations

**Flag:** Operations using a weaker privilege than required.

### 3c: Authorization bypass
Check for code paths that skip authorization:
- Conditional auth checks (`if (authEnabled) { verifyAccess(...) }`)
- Internal methods callable from REST without auth
- Batch operations that auth-check the batch but not individual items

## Phase 4: Injection Prevention

### 4a: Server-Side Include (SSI) injection
The codebase has a known SSI pattern check. Verify it's applied to all user-provided text fields:
```java
Pattern SSI_TAG_PATTERN = Pattern.compile("<!--#\\s*\\w+.*-->");
if (SSI_TAG_PATTERN.matcher(message).find()) {
    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "SSI tags not allowed");
}
```

**Flag:** New string attributes stored from user input without SSI check.

### 4b: HTML/Script injection
User-provided attribute values stored and later rendered:
- Entity names, descriptions, announcements
- Glossary term definitions
- Custom metadata values

**Flag:** Storing raw HTML/script content without sanitization.

### 4c: URL validation
For attributes that store URLs (announcements, links):
```java
// Required: Validate URLs to prevent SSRF
// Check protocol (only http/https allowed)
// Check for internal network addresses (127.0.0.1, 10.x.x.x, 192.168.x.x, etc.)
```

## Phase 5: Sensitive Data Exposure

### 5a: Logging sensitive data
Search changed code for logging statements that might expose:
- API keys, tokens, passwords
- User credentials
- Connection strings with credentials
- Personal identifiable information (PII)

### 5b: Error message exposure
Exception messages returned to users should not reveal:
- Internal class names or stack traces
- Database schema details
- File system paths
- Internal IP addresses or hostnames

### 5c: Log injection
User-controlled strings in log messages must be sanitized:
```java
// BAD: Newline injection allows fake log entries
LOG.info("Processing entity: " + qualifiedName);

// GOOD: Parameterized logging + sanitization
LOG.info("Processing entity: {}", qualifiedName.replaceAll("[\r\n]", "_"));
```

## Phase 6: Deserialization Safety

### 6a: JSON deserialization
Check for unsafe deserialization of user-provided JSON:
- Using `ObjectMapper` with `enableDefaultTyping()` enabled
- Deserializing to `Object` type without constraints
- Missing `@JsonIgnoreProperties(ignoreUnknown = true)` on DTOs

### 6b: Entity attribute types
Check that entity attributes from REST input are validated against the typedef schema.

## Phase 7: Dependency Review

If `pom.xml` files are changed:
- Check for new dependencies with known CVEs
- Check for version downgrades of security-critical libraries
- Check for removal of security libraries
- Flag dependencies with overly broad scope

## Phase 8: Concurrency and Race Conditions

### 8a: Thread-local safety
`RequestContext` is thread-local. Flag:
- Passing `RequestContext` data to new threads without copying
- Accessing `RequestContext` in async callbacks
- Missing `RequestContext.clear()` in error paths

### 8b: Shared mutable state
Flag:
- Static mutable fields accessed from request handlers
- Collections shared between threads without synchronization
- Cache operations without proper atomicity

## Output Format

```markdown
## Security Review

### Summary
- Files reviewed: N
- Total findings: N (X critical, Y high, Z medium, W low)
- Blocking issues: Yes/No

### Findings

#### [CRITICAL] <title>
- **File:** `path/to/file.java:lineNumber`
- **Category:** Injection / Auth Bypass / Data Exposure / etc.
- **Issue:** Description of the vulnerability
- **Impact:** What an attacker could do
- **Fix:**
  ```java
  // concrete fix
  ```

#### [HIGH] <title>
...

### Security Checklist
- [ ] Input validation: All REST params validated
- [ ] Authorization: verifyAccess() on all operations
- [ ] Injection: No string-concatenated queries, SSI checked
- [ ] Data exposure: No secrets in logs, safe error messages
- [ ] Deserialization: Safe JSON handling
- [ ] Dependencies: No known vulnerable additions
- [ ] Concurrency: Thread-local safety maintained

### Recommendation
PASS / PASS WITH NOTES / FAIL (with blocking issues listed)
```
