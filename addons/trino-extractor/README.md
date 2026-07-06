# Trino Extractor — integration tests

## Live integration test (`TrinoExtractorIT`)

`TrinoExtractorIT` runs `./bin/run-trino-extractor.sh` from a **standalone tarball** (same `lib/` layout as distro), not the Maven test classpath. It skips automatically when no live stack is configured.

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
