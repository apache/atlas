---
description: Generate unit tests for changed/new code using TestNG + Mockito conventions
allowed-tools: [Bash, Read, Grep, Glob, Edit, Write, Task]
argument-hint: "[branch-name | PR-number | file-or-directory-path]"
---

# Generate Unit Tests

Analyze changed or specified code and generate unit tests following the atlas-metastore TestNG + Mockito conventions. Produces ready-to-use test files.

## Input Resolution

Determine what to generate tests for based on `$ARGUMENTS`:

1. **PR number** (e.g., `#123` or `123`): Get changed files from the PR
   ```bash
   gh pr diff <number> --repo atlanhq/atlas-metastore --name-only | grep '\.java$' | grep -v 'src/test/'
   ```
2. **Branch name** (e.g., `feature/my-branch`): Get changed files on that branch
   ```bash
   git fetch origin <branch> 2>/dev/null
   git diff --name-only $(git merge-base master origin/<branch>)..origin/<branch> -- '*.java' | grep -v 'src/test/'
   ```
3. **File or directory path**: Generate tests for those specific files
4. **No argument**: Find changed Java files on the current branch
   ```bash
   git diff --name-only $(git merge-base HEAD master)..HEAD -- '*.java' | grep -v 'src/test/'
   ```

**How to detect the argument type:**
- Starts with `#` or is purely numeric → PR number
- Contains `/` but doesn't exist as a file path on disk → branch name
- Exists as a file or directory on disk → file path
- Matches a known git branch (`git branch -a | grep <arg>`) → branch name

If no source files are found, inform the user and stop.

## Phase 0: Extract Linear Ticket Context

Linear tickets often contain acceptance criteria and expected behaviors that should drive test scenarios.

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

### 0c: Extract test-relevant context
- **Acceptance criteria** → each AC becomes at least one test method
- **Bug repro steps** → convert into regression test with specific setup/assertions
- **Edge cases in description** → additional test scenarios
- **Expected behavior** → assertion values

## Phase 1: Parallel Research

Launch **multiple Explore agents in parallel** using the Task tool:

```
Launch these agents IN PARALLEL (single message, multiple Task calls):
```

**Agent 1 — Source Code Deep Read:**
Read every changed source file completely (not just the diff). Understand the full class structure: constructors, fields, all public/package-private methods, inheritance hierarchy, and interfaces implemented. Identify all dependencies that need mocking.

**Agent 2 — Existing Test Discovery:**
For each changed source file, search for existing test files: `*/<ClassName>Test.java`, `*/<ClassName>*Test.java`. Read any existing tests fully to understand current coverage, style, and conventions. List all untested methods.

**Agent 3 — Convention Discovery:**
Find 2-3 similar test files in the same package or a sibling package. Read them to learn: how mocks are set up, what helper methods exist, how RequestContext is managed, how AtlasEntity objects are constructed for tests. Identify reusable patterns.

**Agent 4 — Diff & Ticket Analysis:**
Read the git diff for each file to understand exactly what's new/changed. If a Linear ticket was found, map acceptance criteria to specific methods that need testing. Identify which methods are new (need full coverage) vs modified (need regression tests).

Wait for all agents to complete before proceeding.

## Phase 2: Determine Test Scenarios

For each testable method, determine scenarios:

### Happy path
- Valid input produces expected output
- All attributes present and correct
- Standard CREATE and UPDATE operations

### Error cases
- Null required parameters → `AtlasBaseException`
- Invalid attribute values → `AtlasBaseException` with specific error code
- Missing entity/vertex → `AtlasBaseException(INSTANCE_GUID_NOT_FOUND)`

### Edge cases
- Empty collections/strings
- Null optional attributes
- Entity with minimal attributes (only required ones)
- Special characters in names

### From Linear ticket (if available)
- Each acceptance criterion → at least one test
- Bug repro steps → regression test
- Mentioned edge cases → additional test methods

## Phase 3: Generate Test Code

### File placement
Mirror the source file's package under `repository/src/test/java/`:
```
Source:  repository/src/main/java/org/apache/atlas/.../Foo.java
Test:    repository/src/test/java/org/apache/atlas/.../FooTest.java
```

### Test class template

Follow this exact pattern (based on existing tests like `AssetPreProcessorTest.java`):

```java
package org.apache.atlas.repository.store.graph.v2.preprocessor.glossary;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.RequestContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class GlossaryPreProcessorTest {

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private EntityGraphRetriever entityRetriever;

    @Mock
    private EntityMutationContext context;

    @Mock
    private AtlasVertex vertex;

    private GlossaryPreProcessor preProcessor;
    private AutoCloseable closeable;

    @BeforeMethod
    public void setup() {
        closeable = MockitoAnnotations.openMocks(this);
        preProcessor = new GlossaryPreProcessor(/* inject mocks */);
        RequestContext.clear();
        RequestContext.get();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        RequestContext.clear();
        if (closeable != null) {
            closeable.close();
        }
    }

    @Test
    public void testMethodName_happyPath() throws AtlasBaseException {
        // Setup
        // Execute
        // Verify
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testMethodName_nullInput() throws AtlasBaseException {
        preProcessor.processAttributes(null, context, EntityMutations.EntityOperation.CREATE);
    }
}
```

### Conventions (MUST follow)

1. **Framework:** TestNG (NOT JUnit 5) — use `org.testng.annotations.*` and `org.testng.Assert.*`
2. **Mocking:** Mockito with `@Mock` annotations and `MockitoAnnotations.openMocks(this)`
3. **Setup/Teardown:**
   - `@BeforeMethod` (not `@Before`) to initialize mocks and clear RequestContext
   - `@AfterMethod` (not `@After`) to close mocks and clear RequestContext
4. **Assertions:** Use `org.testng.Assert.*` (not JUnit)
   - `assertEquals(actual, expected)` — TestNG order is (actual, expected)
   - `assertNotNull(value)`
   - `assertTrue(condition)`
5. **Test naming:** `testMethodName_scenario` (e.g., `testProcessCreate_nullQualifiedName`)
6. **No `@Order` annotation** — unit tests must be independent
7. **RequestContext:** Always `RequestContext.clear()` in both `@BeforeMethod` and `@AfterMethod`

## Phase 4: Write Test Files

1. **If adding to existing test file:** Use the Edit tool to add new test methods
2. **If creating new test file:** Use the Write tool

After writing, verify compilation:
```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home /opt/homebrew/bin/mvn compile -pl repository -am -DskipTests -Drat.skip=true 2>&1 | tail -30
```

If compilation fails, fix the issues and retry.

## Phase 5: Summary

```markdown
## Unit Tests Generated

### Ticket Context (if found)
- **Ticket:** MS-XXX — <title>
- **ACs covered by tests:** X/Y

### Files Modified/Created
| File | Status | Tests Added |
|------|--------|-------------|
| `path/to/TestFile.java` | Created/Modified | N tests |

### Test Coverage
| Source Method | Test Methods | Scenarios Covered | Source |
|--------------|-------------|-------------------|--------|
| `processCreate()` | `testProcessCreate_happyPath`, `testProcessCreate_nullQN` | Happy path, null input | Code change |
| `validateInput()` | `testValidateInput_emptyName` | AC: "name must not be empty" | Ticket MS-XXX |

### Compilation Status
- [ ] All tests compile successfully

### How to Run
```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn test -pl repository \
  -Dtest=<TestClassName> \
  -Drat.skip=true
```
```
