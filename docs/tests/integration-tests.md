# Atlas In-Process Integration Tests

## Overview

Integration tests run a **full Atlas server in-process** (no Docker image build) with infrastructure services provided by **testcontainers**. Tests use `AtlasClientV2` with basic auth to call real REST APIs against a real graph database.

This gives us true end-to-end coverage — from HTTP request through Spring/Jersey, preprocessors, graph storage (Cassandra), indexing (Elasticsearch), and back — without the overhead of building and deploying a Docker image.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  JUnit 5 Test (e.g. EntityCrudIntegrationTest)          │
│    └── AtlasClientV2 (HTTP, basic auth)                 │
├─────────────────────────────────────────────────────────┤
│  In-Process Atlas Server (Jetty)                        │
│    ├── REST API (Jersey/Spring)                         │
│    ├── Preprocessors, Store, Graph                      │
│    └── Bootstrap type definitions                       │
├─────────────────────────────────────────────────────────┤
│  Testcontainers (singleton, shared across test classes)  │
│    ├── Cassandra 2.1        (graph storage)             │
│    ├── Elasticsearch 7.16.2 (index)                     │
│    ├── Redis 6.2.14         (cache)                     │
│    ├── Kafka (cp-kafka 7.4.0)                           │
│    └── ZooKeeper 3.8                                    │
└─────────────────────────────────────────────────────────┘
```

## Project Structure

```
webapp/src/test/java/org/apache/atlas/web/integration/
├── AtlasInProcessBaseIT.java              # Base class — starts containers + Atlas server
├── InProcessAtlasServer.java              # Jetty server wrapper
├── GlossaryIntegrationTest.java           # Glossary CRUD tests (11 tests)
├── EntityCrudIntegrationTest.java         # Entity CRUD tests (12 tests)
└── ...

webapp/src/test/resources/deploy/          # Test deployment directory (atlas.home)
├── conf/                                  # Config templates
├── elasticsearch/                         # ES settings + mappings
└── models/                                # Bootstrap type definitions
    └── 0000-Area0/
        ├── 0010-base_model.json           # Core types (Asset, Glossary, etc.)
        └── patches/
            ├── 002-base_model_add_service_type.json
            ├── 006-base_model_add_atlas_operation_attributes.json
            └── 007_all_custom_models.json # Extended types (Table, Column, etc.)
```

## Base Class: AtlasInProcessBaseIT

All integration tests extend `AtlasInProcessBaseIT`, which handles:

1. **Container lifecycle** — Singleton containers started once, shared across all test classes via `static` fields and a `containersStarted` flag. Containers are stopped by JVM shutdown hook (not per-class).

2. **Configuration generation** — Creates a temp directory with `atlas-application.properties` pointing to the testcontainer ports. Key settings:
   - `SoftDeleteHandlerV1` (soft delete — entities get `status=DELETED`, not removed)
   - `NoopEntityAuditRepository` (no HBase dependency)
   - File-based authentication (`admin/admin`)
   - Index recovery disabled (avoids shutdown hang)
   - Authorizer set to `none`

3. **ES template setup** — Pushes the `atlan-template` index template to Elasticsearch before Atlas starts (required for JanusGraph ES indexing).

4. **Server startup + readiness** — Starts Jetty on a random free port, polls `/api/atlas/admin/status` until HTTP 200 (up to 5 minutes).

5. **Client creation** — Provides a ready-to-use `atlasClient` (`AtlasClientV2`) with basic auth.

### What subclasses get

```java
public abstract class AtlasInProcessBaseIT {
    protected static AtlasClientV2 atlasClient;  // Ready after @BeforeAll

    protected int getAtlasPort();                 // Dynamic port
    protected String getAtlasBaseUrl();            // http://localhost:{port}
}
```

## Writing a New Test Class

### 1. Extend the base class

```java
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MyIntegrationTest extends AtlasInProcessBaseIT {
    // atlasClient is available after @BeforeAll
}
```

### 2. Use `@Order` for test sequencing

Tests within a class share state (instance fields) because of `PER_CLASS` lifecycle. Use `@Order` to control execution order when tests depend on each other (e.g., create before read before delete).

```java
@Test @Order(1)
void testCreate() throws AtlasServiceException { ... }

@Test @Order(2)
void testRead() throws AtlasServiceException { ... }
```

### 3. Use `AtlasClientV2` for all operations

All tests go through the REST API client — no direct graph/store access. This ensures we test the full stack.

```java
// Entity CRUD
EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
AtlasEntityWithExtInfo result = atlasClient.getEntityByGuid(guid);
atlasClient.updateEntity(new AtlasEntityWithExtInfo(entity));
atlasClient.deleteEntityByGuid(guid);

// Glossary CRUD
AtlasGlossary glossary = atlasClient.createGlossary(glossary);
AtlasGlossaryTerm term = atlasClient.createGlossaryTerm(term);
```

### 4. Assert with JUnit 5

```java
import static org.junit.jupiter.api.Assertions.*;

assertNotNull(response.getFirstEntityCreated());
assertEquals(AtlasEntity.Status.ACTIVE, entity.getStatus());
assertThrows(AtlasServiceException.class, () -> atlasClient.getEntityByGuid(bogusGuid));
```

## Available Bootstrap Types

The test deploy directory loads types from `0010-base_model.json` and the `007_all_custom_models.json` patch. Available types include:

| Type | Source | Notes |
|------|--------|-------|
| `Referenceable`, `Asset`, `DataSet`, `Process` | base_model | Core supertypes |
| `AtlasGlossary`, `AtlasGlossaryTerm`, `AtlasGlossaryCategory` | base_model | Glossary types |
| `DataDomain`, `DataProduct` | base_model | Data mesh |
| `Persona`, `Purpose`, `AuthPolicy` | base_model | Access control |
| `Table`, `Column`, `Schema`, `Database` | 007_all_custom_models | Common entity types |
| `Connection` | 007_all_custom_models | Connector framework |
| `Collection`, `Folder`, `Query` | base_model | Query types |

### Adding more types

If your test needs types not currently loaded, you have two options:

1. **Add a model file** to `webapp/src/test/resources/deploy/models/` — top-level JSON files are loaded after folders. Place a `minimal.json` or a trimmed subset here. No main-source changes required.

2. **Add a patch file** to `webapp/src/test/resources/deploy/models/0000-Area0/patches/` — name it alphabetically after existing patches (e.g., `008-my-types.json`).

The model loader skips types that are already registered (same name + version), so conflicts between files are safe.

## Existing Test Suites

### GlossaryIntegrationTest (11 tests)

End-to-end glossary lifecycle: create glossary, terms, categories, update, list, delete.

| # | Test | What it covers |
|---|------|---------------|
| 1 | `testCreateGlossary` | Create glossary, verify GUID assigned |
| 2 | `testGetGlossary` | Fetch by GUID, verify attributes |
| 3 | `testCreateGlossaryTerm` | Create term with anchor to glossary |
| 4 | `testCreateGlossaryCategory` | Create category with anchor |
| 5 | `testUpdateGlossary` | Update description |
| 6 | `testGetGlossaryTerms` | List terms in glossary |
| 7 | `testGetGlossaryCategories` | List categories in glossary |
| 8 | `testUpdateGlossaryTerm` | Update term description |
| 9 | `testDeleteGlossaryTerm` | Delete term, verify removed from list |
| 10 | `testDeleteGlossaryCategory` | Delete category |
| 11 | `testDeleteGlossary` | Delete glossary, verify 404 on re-fetch |

**Gotcha:** When updating glossary objects, remove `lexicographicalSortOrder` from attribute maps to avoid "Duplicate Lexorank" errors. See `removeLexorank()` helper.

### EntityCrudIntegrationTest (12 tests)

General-purpose entity CRUD using `Table` type.

| # | Test | What it covers |
|---|------|---------------|
| 1 | `testCreateSingleEntity` | Create Table, verify GUID |
| 2 | `testGetEntityByGuid` | Fetch by GUID, verify all attributes + ACTIVE status |
| 3 | `testGetEntityByUniqueAttribute` | Fetch by qualifiedName |
| 4 | `testGetEntityHeaderByGuid` | Lightweight header fetch |
| 5 | `testUpdateEntity` | Full update (change description, add displayName) |
| 6 | `testPartialUpdateByGuid` | Partial update (name only), verify others unchanged |
| 7 | `testCreateMultipleEntities` | Bulk create 3 entities |
| 8 | `testGetEntitiesByGuids` | Bulk fetch by GUIDs |
| 9 | `testDeleteSingleEntity` | Soft delete, verify `status=DELETED` still fetchable |
| 10 | `testDeleteMultipleEntities` | Bulk soft delete |
| 11 | `testCreateEntityMissingRequired` | Error: missing qualifiedName throws AtlasServiceException |
| 12 | `testGetNonExistentEntity` | Error: bogus GUID throws AtlasServiceException |

**Gotcha:** `partialUpdateEntityByGuid` sends the attribute value as the raw HTTP body. A plain Java String is not auto-quoted by Jersey, so string values must be wrapped in JSON quotes:

```java
// Wrong — server receives: renamed-table (invalid JSON)
atlasClient.partialUpdateEntityByGuid(guid, "renamed-table", "name");

// Correct — server receives: "renamed-table" (valid JSON string)
atlasClient.partialUpdateEntityByGuid(guid, "\"renamed-table\"", "name");
```

## Running Tests

### Prerequisites

Build the webapp module first (needed once, or after source changes):

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn install -pl webapp -am -DskipTests -Drat.skip=true
```

### Run a specific test class

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn test -pl webapp \
  -Dtest=EntityCrudIntegrationTest \
  -Drat.skip=true -Dsurefire.failIfNoSpecifiedTests=false
```

### Run all integration tests

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  /opt/homebrew/bin/mvn test -pl webapp \
  -Dtest="GlossaryIntegrationTest,EntityCrudIntegrationTest" \
  -Drat.skip=true -Dsurefire.failIfNoSpecifiedTests=false
```

### Run from IntelliJ

Tests can also be run directly from IntelliJ IDEA — just right-click the test class and run. Make sure:
- JDK 17 (Zulu) is configured
- Docker Desktop is running (for testcontainers)

## Performance

| Phase | Time |
|-------|------|
| Container startup (first test class) | ~20-30s |
| Atlas server startup + bootstrap | ~40-50s |
| Individual test execution | <1s each |
| Total for 12 entity tests | ~75s |
| Total for 11 glossary tests | ~75s |

Containers are shared across test classes, so running multiple suites in one Maven invocation only pays the startup cost once.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| In-process server | Jetty via `InProcessAtlasServer` | No Docker image build, fast iteration, debuggable |
| Container strategy | Singleton (static, shared) | One startup cost regardless of test class count |
| Auth | File-based (`admin/admin`) | Simplest setup, no Keycloak dependency |
| Delete handler | `SoftDeleteHandlerV1` | Matches production config, allows verifying soft-delete behavior |
| Authorizer | `none` | Tests focus on CRUD logic, not authorization |
| Entity audit | `NoopEntityAuditRepository` | No HBase dependency needed |
| Index recovery | Disabled | Avoids monitor thread hang on shutdown |
| QualifiedName pattern | `test://integration/{type}/{name}/{timestamp}` | Unique across runs, clearly identifies test data |
| Test lifecycle | `PER_CLASS` with `@Order` | Allows state sharing between ordered test methods |

## Troubleshooting

### "Could not find deploy directory with models/"
The test resolves `atlas.home` from either `$PWD/src/test/resources/deploy` or `$PWD/webapp/src/test/resources/deploy`. Make sure you're running from the project root or the webapp module.

### Container startup timeout
Default timeout is 3 minutes for Cassandra. If on a slow machine/network (first pull), increase `withStartupTimeout()` in `AtlasInProcessBaseIT`.

### "Type not found" for Table/Column/etc.
Check that `007_all_custom_models.json` exists in `webapp/src/test/resources/deploy/models/0000-Area0/patches/`. This patch file adds the extended types like Table.

### Atlas doesn't become ready within 5 minutes
Check container logs. Common issues:
- Cassandra keyspace creation failure (port mismatch)
- ES template not applied (check `initElasticsearchTemplate()`)
- Missing `users-credentials.properties`

### Partial update returns 400 Bad Request
The `partialUpdateEntityByGuid` API sends the value as a raw JSON body. String values must be JSON-quoted (see gotcha above).

### "Duplicate Lexorank" on glossary update
Remove `lexicographicalSortOrder` from the glossary object's attribute maps before updating.
