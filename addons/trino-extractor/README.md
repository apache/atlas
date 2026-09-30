# Trino Extractor — integration tests

## Live integration test (`TrinoExtractorIT`)

`TrinoExtractorIT` runs `./bin/run-trino-extractor.sh` from a **standalone tarball** (same `lib/` layout as distro), not the Maven test classpath. It skips automatically when no live stack is configured.

**CI / default `mvn verify`:** integration tests are **skipped** (`skipITs=true`). Enable them with the `trino-extractor-it` profile below.

**Run against Atlas + Trino lab** (e.g. ranger-docker Trino on `:8080`, Atlas on `:21000`):

```bash
export ATLAS_REST_URL=http://localhost:21000
export TRINO_JDBC_URL=jdbc:trino://localhost:8080/
export ATLAS_USERNAME=admin
export ATLAS_PASSWORD=atlasR0cks!
export TRINO_EXTRACTOR_SCHEMA=hr
export TRINO_EXTRACTOR_TABLE=trino_pii_lab
export TRINO_EXTRACTOR_COLUMN=ssn

mvn -pl addons/trino-extractor -Ptrino-extractor-it verify
```

The `-Ptrino-extractor-it` profile builds the tarball in `pre-integration-test`, then runs `TrinoExtractorIT`. Optional: `TRINO_EXTRACTOR_TARBALL=/path/to/apache-atlas-*-trino-extractor.tar.gz` to reuse a pre-built distro artifact.

### Test coverage map

| Original testcase | Test class | Method | Default run |
|-------------------|------------|--------|-------------|
| Invalid arguments | `TrinoExtractorIT` | `testInvalidArguments` | Live (Atlas up) |
| Invalid cron expression | `TrinoExtractorIT` | `testInvalidCronExpression` | Live |
| Valid catalog run | `TrinoExtractorIT` | `testValidRegisteredCatalogRun` | Live |
| Instance creation | `TrinoExtractorIT` | `testInstanceCreation` | Live |
| Catalog creation | `TrinoExtractorIT` | `testCatalogCreation` | Live |
| Schema creation | `TrinoExtractorIT` | `testSchemaCreation` | Live |
| Table creation | `TrinoExtractorIT` | `testTableCreation` | Live |
| Without cron expression | `TrinoExtractorIT` | `testWithoutCronExpression` | Live |
| Unregistered catalog via CLI `-c` | `TrinoExtractorIT` | `testUnregisteredCatalogViaCommandLine` | Live |
| Hook entity linked | `TrinoExtractorIT` | `testHookEntityLinkedToTrinoColumn` | Skip unless `TRINO_IT_HOOK_ENABLED=1` |
| Tag propagated | `TrinoExtractorIT` | `testTagPropagatedToTrinoColumn` | Skip unless `TRINO_IT_TAG_PROPAGATION=1` |
| Deleted table / schema / catalog | `TrinoExtractorIT` | `testDeleted*` | Skip unless `TRINO_IT_DELETE_SYNC=1` |
| Rename catalog / schema (stale cleanup) | `TrinoExtractorIT` | `testRename*` | Skip unless `TRINO_IT_DELETE_SYNC=1` |
| Deleted column | `TrinoExtractorIT` | `testDeletedColumnNotSupportedYet` | Skip (not implemented in extractor) |
| Cron overlap guard | `TrinoExtractorTest` | `testMetadataJobDisallowConcurrentExecution` | Unit (`mvn test`) |
| Cron validation | `TrinoExtractorTest` | `testInvalidCronExpressionRejected` | Unit |
| Tarball Jersey classpath | `TrinoExtractorIT` | `testTarballJerseyClasspath` | Live |

### Unit tests (no Atlas/Trino required)

```bash
mvn -pl addons/trino-extractor test -Dtest=TrinoExtractorTest
```

No live services are needed — only JDK and Maven.

---

## Components required to run tests

What must be up depends on which tests you run.

### Unit tests only

| Required | Not required |
|----------|----------------|
| JDK + Maven | Atlas, Trino, Kafka, Hive, Ranger |

```bash
mvn -pl addons/trino-extractor test -Dtest=TrinoExtractorTest
```

### Default integration tests (`TrinoExtractorIT`)

Set `ATLAS_REST_URL` and run:

```bash
mvn -pl addons/trino-extractor -Ptrino-extractor-it verify
```

#### Must be running

| Component | Role | Default |
|-----------|------|---------|
| **Atlas server** | REST API — create/update/assert `trino_*` entities | `http://localhost:21000` |
| **Atlas backend store** | Postgres or HBase (started with Atlas) | — |
| **Trino coordinator** | JDBC metadata source for the extractor | `jdbc:trino://localhost:8080/` |
| **Hive metastore** | Backing store for the Trino `hive` catalog | e.g. `ranger-hive` in docker lab |
| **Trino `hive` catalog** | Catalog the extractor queries | `hive` |
| **Schema / table / column** | Objects imported into Atlas | `hr.trino_pii_lab.ssn` |

```text
Trino Extractor IT  --REST-->  Atlas (:21000)
        |
        +--JDBC-->  Trino (:8080)  -->  hive catalog  -->  Hive metastore
```

#### Minimum environment variables

```bash
export ATLAS_REST_URL=http://localhost:21000
export TRINO_JDBC_URL=jdbc:trino://localhost:8080/
export ATLAS_USERNAME=admin
export ATLAS_PASSWORD=atlasR0cks!

# Optional — defaults match ranger-docker lab
export TRINO_EXTRACTOR_CATALOG=hive
export TRINO_EXTRACTOR_SCHEMA=hr
export TRINO_EXTRACTOR_TABLE=trino_pii_lab
export TRINO_EXTRACTOR_COLUMN=ssn
export ATLAS_TRINO_NAMESPACE=dev
```

#### Not required for default ITs

| Component | Why not |
|-----------|---------|
| **Kafka** | Extractor uses Trino JDBC + Atlas REST only |
| **Hive hook** | Default tests only create `trino_*` entities |
| **Ranger** | Not used by extractor ITs |
| **TagSync** | Only for optional tag propagation test |
| **Pre-built distro tarball** | Maven profile `trino-extractor-it` builds the IT tarball |

If `ATLAS_REST_URL` is unset or Atlas is unreachable, `TrinoExtractorIT` **skips** (suite passes without live tests).

### Optional integration tests (extra flags)

| Flag | Extra components | What it verifies |
|------|------------------|------------------|
| `TRINO_IT_HOOK_ENABLED=1` | HiveServer2 with **Atlas Hive hook**; `hive_column` in Atlas (default namespace `cm`) | `trino_column` links to `hive_column` |
| `TRINO_IT_TAG_PROPAGATION=1` | Hook linkage + Atlas classification API | PII tag propagates to `trino_column` |
| `TRINO_IT_DELETE_SYNC=1` | Atlas only (stale entities seeded via REST) | Stale `trino_table` / `trino_schema` / `trino_catalog` removal |

TagSync and Ranger are **not** required even for tag propagation ITs (Atlas REST only).

### Test tier matrix

| Tier | Atlas | Trino | Hive MS + table | Hive hook | Kafka | Ranger | TagSync |
|------|-------|-------|-----------------|-----------|-------|--------|---------|
| Unit (`TrinoExtractorTest`) | — | — | — | — | — | — | — |
| Default IT | Yes | Yes | Yes (`hr.trino_pii_lab`) | — | — | — | — |
| Hook IT | Yes | Yes | Yes | Yes | —* | — | — |
| Tag propagation IT | Yes | Yes | Yes | Yes | —* | — | — |
| Delete sync IT | Yes | Yes** | — | — | — | — | — |

\* Kafka only if your Hive hook setup uses it; not required by the extractor itself.  
\*\* Trino still needed for extractor JDBC; stale entities are seeded via Atlas REST.

### Recommended docker lab (default ITs)

Atlas coexist + ranger-docker stack:

| Service | Port | Purpose |
|---------|------|---------|
| `atlas` | 21000 | Atlas REST + graph store |
| Atlas Postgres (or HBase) | — | Atlas persistence |
| `ranger-trino` | 8080 | Trino JDBC |
| `ranger-hive` | — | Hive metastore for `hive` catalog |

Create the test table if missing:

```sql
CREATE TABLE hr.trino_pii_lab (id INT, ssn STRING, address STRING);
```

Verify Trino sees it:

```bash
docker exec ranger-trino trino --user admin \
  --execute "SELECT ssn FROM hive.hr.trino_pii_lab LIMIT 1"
```

### Pre-flight checks

```bash
# Atlas up
curl -u admin:atlasR0cks! http://localhost:21000/api/atlas/v2/types/trino_column

# Trino up
docker exec ranger-trino trino --user admin --execute "SHOW SCHEMAS FROM hive"

# Table exists
docker exec ranger-trino trino --user admin \
  --execute "DESCRIBE hive.hr.trino_pii_lab"
```

For full Atlas + Ranger + TagSync E2E (beyond these ITs), see `dev-support/atlas-docker/README-TRINO-ATLAS-RANGER-E2E.md`.
