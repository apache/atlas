---
description: Analyze code changes and suggest integration tests
allowed-tools: [Bash, Read, Grep, Glob]
---

# Suggest Integration Tests for Code Changes

Analyze the current git diff and recommend which integration tests should cover these changes, identify coverage gaps, and suggest specific test cases to add.

## Phase 1: Get Changed Files

```bash
git diff --name-only $(git merge-base HEAD master)..HEAD
```

## Phase 2: Map Changes to Test Classes

Use this mapping to identify relevant tests:

### Code Path to Test Mapping

| Changed Path Pattern | Required Integration Tests | Test Class Location |
|---------------------|---------------------------|---------------------|
| `**/glossary/**` | GlossaryIntegrationTest, GlossaryExtendedIntegrationTest | `webapp/src/test/java/.../integration/` |
| `**/preprocessor/glossary/**` | GlossaryIntegrationTest, GlossaryExtendedIntegrationTest | |
| `**/preprocessor/datamesh/**` | DataMeshIntegrationTest | |
| `**/preprocessor/accesscontrol/**` | PersonaPurposeIntegrationTest | |
| `**/preprocessor/sql/**` | SQLAssetHierarchyIntegrationTest | |
| `**/EntityGraphMapper.java` | EntityCrudIntegrationTest | |
| `**/AtlasRelationshipStoreV2.java` | RelationshipIntegrationTest | |
| `**/classification/**` or `**/tag/**` | ClassificationIntegrationTest | |
| `**/lineage/**` | LineageIntegrationTest | |
| `**/discovery/**` or `**/search/**` | SearchIntegrationTest | |
| `**/AtlasTypeDefGraphStoreV2.java` | TypeDefIntegrationTest | |
| `**/businessmetadata/**` | BusinessMetadataIntegrationTest | |
| `**/label/**` | LabelIntegrationTest | |
| `intg/src/main/java/**/model/**` | EntityCrudIntegrationTest (for model changes) | |
| `webapp/src/main/java/**/rest/**` | Depends on endpoint - check REST resource name | |

### Entity Type to Test Mapping

| Entity Type | Primary Test | Secondary Tests |
|-------------|--------------|-----------------|
| AtlasGlossary, AtlasGlossaryTerm, AtlasGlossaryCategory | GlossaryIntegrationTest | GlossaryExtendedIntegrationTest |
| Table, Column, Schema, Database | EntityCrudIntegrationTest | SQLAssetHierarchyIntegrationTest |
| DataDomain, DataProduct | DataMeshIntegrationTest | |
| Persona, Purpose, AuthPolicy | PersonaPurposeIntegrationTest | |
| Process (lineage) | LineageIntegrationTest | |
| Any entity with classifications | ClassificationIntegrationTest | |
| Any entity with labels | LabelIntegrationTest | |
| Any entity with business metadata | BusinessMetadataIntegrationTest | |
| Relationships | RelationshipIntegrationTest | |

## Phase 3: Analyze Each Changed File

For each changed file:

1. **Read the diff** to understand what changed:
   ```bash
   git diff $(git merge-base HEAD master)..HEAD -- <file_path>
   ```

2. **Identify the functionality affected**:
   - New methods added?
   - Existing logic modified?
   - Error handling changed?
   - New entity types or attributes?

3. **Check if existing tests cover the change**:
   ```bash
   # Search for test methods that might cover this functionality
   grep -r "test.*<MethodName>" webapp/src/test/java/org/apache/atlas/web/integration/
   ```

## Phase 4: Check Existing Test Coverage

For each identified test class, read the test to understand current coverage:

```bash
# List all test methods in a test class
grep -E "@Test|void test" webapp/src/test/java/org/apache/atlas/web/integration/<TestClass>.java
```

## Phase 5: Generate Recommendations

Output a structured report:

```markdown
## Integration Test Recommendations

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

1. **Must have:** [Critical tests for the PR]
2. **Should have:** [Important but not blocking]
3. **Nice to have:** [Future improvements]
```

## Phase 6: Provide Actionable Next Steps

1. If tests need to be added, provide the exact file to modify
2. Include code snippets that can be directly added
3. Reference similar existing tests as patterns to follow

## Test Writing Guidelines

When suggesting new tests, follow these patterns from the existing codebase:

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
    createdGuid = response.getFirstEntityCreated().getGuid(); // Store for later tests
}
```

### Glossary Test Pattern
```java
@Test
@Order(1)
void testCreateGlossary() throws AtlasServiceException {
    AtlasGlossary glossary = new AtlasGlossary();
    glossary.setName("Test Glossary " + System.currentTimeMillis());
    glossary.setShortDescription("Integration test glossary");

    createdGlossary = atlasClient.createGlossary(glossary);

    assertNotNull(createdGlossary.getGuid());
}
```

### Error Case Test Pattern
```java
@Test
@Order(99)
void testCreateEntityMissingRequired() {
    AtlasEntity entity = new AtlasEntity("Table");
    entity.setAttribute("name", "missing-qn-table");
    // Deliberately missing qualifiedName

    assertThrows(AtlasServiceException.class, () -> {
        atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
    });
}
```

## Running Suggested Tests Locally

After identifying tests, run them locally:

```bash
# Single test class
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn test -pl webapp \
  -Dtest=<TestClassName> \
  -Drat.skip=true -Dsurefire.failIfNoSpecifiedTests=false

# Multiple test classes
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn test -pl webapp \
  -Dtest="TestClass1,TestClass2" \
  -Drat.skip=true -Dsurefire.failIfNoSpecifiedTests=false
```
