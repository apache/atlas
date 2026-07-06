/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.trino.cli;

import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

/**
 * Live integration tests for the standalone Trino extractor tarball against Atlas + Trino.
 * <p>
 * Enable with {@code mvn -pl addons/trino-extractor -Ptrino-extractor-it verify} and set
 * {@code ATLAS_REST_URL}. Optional flags: {@code TRINO_IT_HOOK_ENABLED=1},
 * {@code TRINO_IT_TAG_PROPAGATION=1}.
 */
public class TrinoExtractorIT {
    private static TrinoExtractorITSupport.LiveStackConfig config;
    private static Path workDir;

    @BeforeClass
    public static void setUpClass() throws Exception {
        config  = TrinoExtractorITSupport.loadLiveStackConfig();
        workDir = TrinoExtractorITSupport.prepareExtractorWorkDir(config);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        TrinoExtractorITSupport.deleteWorkDir(workDir);
        workDir = null;
    }

    @BeforeMethod
    public void resetExtractorProperties() throws Exception {
        TrinoExtractorITSupport.rewriteProperties(workDir, config, null);
    }

    @Test(priority = 1)
    public void testTarballJerseyClasspath() throws Exception {
        TrinoExtractorITSupport.assertTarballJerseyClasspath(workDir);
    }

    @Test(priority = 2)
    public void testInvalidArguments() throws Exception {
        int exitCode = TrinoExtractorITSupport.runExtractorJava(workDir, config, "-c", config.catalog, "unexpected-arg");
        assertNotEquals(exitCode, 0, "unrecognized Java argument should fail");
    }

    @Test(priority = 2)
    public void testInvalidCronExpression() throws Exception {
        long startMs = System.currentTimeMillis();
        int exitCode = TrinoExtractorITSupport.runExtractorJava(workDir, config, "--cronExpression", "not-a-valid-cron", "-c", config.catalog, "-s", config.schema, "-t", config.table);
        long elapsedSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startMs);

        assertNotEquals(exitCode, 0, "invalid cron expression should fail");
        assertTrue(elapsedSec < 120, "invalid cron should fail quickly, took " + elapsedSec + "s");
    }

    @Test(priority = 3)
    public void testWithoutCronExpression() throws Exception {
        assertEquals(runSingleTableExtract(), 0);
    }

    @Test(priority = 3)
    public void testTableCreation() throws Exception {
        assertEquals(runSingleTableExtract(), 0);

        String columnQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_column", config.catalog, config.schema, config.table, config.column);
        TrinoExtractorITSupport.assertEntityExists(config, "trino_column", columnQn);
    }

    @Test(priority = 3, dependsOnMethods = "testTableCreation")
    public void testSchemaCreation() throws Exception {
        String schemaQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_schema", config.catalog, config.schema);
        TrinoExtractorITSupport.assertEntityExists(config, "trino_schema", schemaQn);
    }

    @Test(priority = 3, dependsOnMethods = "testTableCreation")
    public void testCatalogCreation() throws Exception {
        String catalogQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_catalog", config.catalog);
        TrinoExtractorITSupport.assertEntityExists(config, "trino_catalog", catalogQn);
    }

    @Test(priority = 3, dependsOnMethods = "testTableCreation")
    public void testInstanceCreation() throws Exception {
        TrinoExtractorITSupport.assertEntityExists(config, "trino_instance", config.trinoNamespace);
    }

    @Test(priority = 4)
    public void testValidRegisteredCatalogRun() throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("atlas.trino.catalogs.registered", config.catalog);
        overrides.remove("atlas.trino.extractor.catalog");
        overrides.remove("atlas.trino.extractor.schema");
        overrides.remove("atlas.trino.extractor.table");

        TrinoExtractorITSupport.rewriteProperties(workDir, config, overrides);

        int exitCode = TrinoExtractorITSupport.runExtractorScript(workDir, config, "-c", config.catalog, "-s", config.schema, "-t", config.table);
        assertEquals(exitCode, 0);
    }

    @Test(priority = 4)
    public void testUnregisteredCatalogViaCommandLine() throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("atlas.trino.catalogs.registered", "not_registered_in_trino");
        TrinoExtractorITSupport.rewriteProperties(workDir, config, overrides);

        int exitCode = TrinoExtractorITSupport.runExtractorScript(workDir, config, "-c", config.catalog, "-s", config.schema, "-t", config.table);
        assertEquals(exitCode, 0, "CLI -c should work even when catalog is not in catalogs.registered");
    }

    @Test(priority = 5)
    public void testDeletedTable() throws Exception {
        if (!TrinoExtractorITSupport.isEnabled("TRINO_IT_DELETE_SYNC")) {
            throw new SkipException("Set TRINO_IT_DELETE_SYNC=1 to verify stale trino_table removal (REST-seeded stale entities are not removed in current runs)");
        }

        assertEquals(runSingleTableExtract(), 0);

        String staleTableName = "stale_trino_table_it_" + System.currentTimeMillis();
        TrinoExtractorITSupport.seedStaleTrinoTable(config, staleTableName);

        int exitCode = TrinoExtractorITSupport.runExtractorScript(workDir, config, "-c", config.catalog, "-s", config.schema);
        assertEquals(exitCode, 0);

        String staleTableQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_table", config.catalog, config.schema, staleTableName);
        TrinoExtractorITSupport.assertEntityAbsent(config, "trino_table", staleTableQn);
    }

    @Test(priority = 5)
    public void testDeletedSchema() throws Exception {
        if (!TrinoExtractorITSupport.isEnabled("TRINO_IT_DELETE_SYNC")) {
            throw new SkipException("Set TRINO_IT_DELETE_SYNC=1 to verify stale trino_schema removal");
        }

        assertEquals(runSingleTableExtract(), 0);

        String staleSchemaName = "stale_trino_schema_it_" + System.currentTimeMillis();
        TrinoExtractorITSupport.seedStaleTrinoSchema(config, staleSchemaName);

        int exitCode = TrinoExtractorITSupport.runExtractorScript(workDir, config, "-c", config.catalog);
        assertEquals(exitCode, 0);

        String staleSchemaQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_schema", config.catalog, staleSchemaName);
        TrinoExtractorITSupport.assertEntityAbsent(config, "trino_schema", staleSchemaQn);
    }

    @Test(priority = 5)
    public void testDeletedCatalog() throws Exception {
        if (!TrinoExtractorITSupport.isEnabled("TRINO_IT_DELETE_SYNC")) {
            throw new SkipException("Set TRINO_IT_DELETE_SYNC=1 to verify stale trino_catalog removal");
        }

        assertEquals(runSingleTableExtract(), 0);

        String staleCatalogName = "stale_trino_catalog_it_" + System.currentTimeMillis();
        TrinoExtractorITSupport.seedStaleTrinoCatalog(config, staleCatalogName);

        Map<String, String> overrides = new HashMap<>();
        overrides.put("atlas.trino.catalogs.registered", config.catalog);
        TrinoExtractorITSupport.rewriteProperties(workDir, config, overrides);

        int exitCode = TrinoExtractorITSupport.runExtractorScript(workDir, config);
        assertEquals(exitCode, 0);

        String staleCatalogQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_catalog", staleCatalogName);
        TrinoExtractorITSupport.assertEntityAbsent(config, "trino_catalog", staleCatalogQn);
    }

    @Test(priority = 6)
    public void testRenameSchemaCreatesNewEntityAndDropsStale() throws Exception {
        if (!TrinoExtractorITSupport.isEnabled("TRINO_IT_DELETE_SYNC")) {
            throw new SkipException("Set TRINO_IT_DELETE_SYNC=1 to verify stale trino_schema cleanup after Trino rename");
        }

        assertEquals(runSingleTableExtract(), 0);

        String staleSchemaName = "stale_trino_schema_it_" + System.currentTimeMillis();
        TrinoExtractorITSupport.seedStaleTrinoSchema(config, staleSchemaName);

        int exitCode = TrinoExtractorITSupport.runExtractorScript(workDir, config, "-c", config.catalog);
        assertEquals(exitCode, 0);

        String staleSchemaQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_schema", config.catalog, staleSchemaName);
        TrinoExtractorITSupport.assertEntityAbsent(config, "trino_schema", staleSchemaQn);

        String liveSchemaQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_schema", config.catalog, config.schema);
        TrinoExtractorITSupport.assertEntityExists(config, "trino_schema", liveSchemaQn);
    }

    @Test(priority = 6)
    public void testRenameCatalogCreatesNewEntityAndDropsStale() throws Exception {
        if (!TrinoExtractorITSupport.isEnabled("TRINO_IT_DELETE_SYNC")) {
            throw new SkipException("Set TRINO_IT_DELETE_SYNC=1 to verify stale trino_catalog cleanup after Trino rename");
        }

        assertEquals(runSingleTableExtract(), 0);

        String staleCatalogName = "stale_trino_catalog_it_" + System.currentTimeMillis();
        TrinoExtractorITSupport.seedStaleTrinoCatalog(config, staleCatalogName);

        Map<String, String> overrides = new HashMap<>();
        overrides.put("atlas.trino.catalogs.registered", config.catalog);
        TrinoExtractorITSupport.rewriteProperties(workDir, config, overrides);

        int exitCode = TrinoExtractorITSupport.runExtractorScript(workDir, config);
        assertEquals(exitCode, 0);

        String staleCatalogQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_catalog", staleCatalogName);
        TrinoExtractorITSupport.assertEntityAbsent(config, "trino_catalog", staleCatalogQn);

        String liveCatalogQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_catalog", config.catalog);
        TrinoExtractorITSupport.assertEntityExists(config, "trino_catalog", liveCatalogQn);
    }

    @Test(priority = 7)
    public void testHookEntityLinkedToTrinoColumn() throws Exception {
        if (!TrinoExtractorITSupport.isEnabled("TRINO_IT_HOOK_ENABLED")) {
            throw new SkipException("Set TRINO_IT_HOOK_ENABLED=1 when Hive hook metadata exists in Atlas (namespace " + config.hiveNamespace + ")");
        }

        runHookEnabledExtract();

        String columnQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_column", config.catalog, config.schema, config.table, config.column);
        String body = TrinoExtractorITSupport.getEntityBody(config, "trino_column", columnQn);

        assertTrue(body != null && body.contains("hive_column"), "trino_column should link to hive_column when hook is enabled");
    }

    @Test(priority = 7)
    public void testTagPropagatedToTrinoColumn() throws Exception {
        if (!TrinoExtractorITSupport.isEnabled("TRINO_IT_TAG_PROPAGATION")) {
            throw new SkipException("Set TRINO_IT_TAG_PROPAGATION=1 when hive_column PII tag propagation is configured");
        }

        runHookEnabledExtract();

        String hiveColumnQn = config.schema + "." + config.table + "." + config.column + "@" + config.hiveNamespace;
        String hiveGuid = TrinoExtractorITSupport.getEntityGuid(config, "hive_column", hiveColumnQn);

        if (hiveGuid == null) {
            throw new SkipException("hive_column not found for tag propagation test: " + hiveColumnQn);
        }

        classifyEntity(config, hiveGuid, "PII");

        assertEquals(runSingleTableExtract(), 0);

        String trinoColumnQn = TrinoExtractorITSupport.trinoQualifiedName(config, "trino_column", config.catalog, config.schema, config.table, config.column);
        String body = TrinoExtractorITSupport.getEntityBody(config, "trino_column", trinoColumnQn);

        assertTrue(body != null && body.contains("PII"), "PII tag should propagate to trino_column when hook link exists");
    }

    @Test(priority = 8)
    public void testDeletedColumnNotSupportedYet() {
        throw new SkipException("Extractor does not delete orphan trino_column entities when a column is dropped in Trino");
    }

    @Test(priority = 8)
    public void testCronDoesNotOverlapConcurrentRuns() {
        throw new SkipException("Concurrent cron scheduling is guarded by @DisallowConcurrentExecution; covered in TrinoExtractorTest");
    }

    private static int runSingleTableExtract() throws Exception {
        return TrinoExtractorITSupport.runExtractorScript(workDir, config, "-c", config.catalog, "-s", config.schema, "-t", config.table);
    }

    private void runHookEnabledExtract() throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put("atlas.trino.catalog.hook.enabled." + config.catalog, "true");
        overrides.put("atlas.trino.catalog.hook.enabled." + config.catalog + ".namespace", config.hiveNamespace);
        TrinoExtractorITSupport.rewriteProperties(workDir, config, overrides);
        assertEquals(runSingleTableExtract(), 0);
    }

    private static void classifyEntity(TrinoExtractorITSupport.LiveStackConfig cfg, String guid, String classification) throws IOException {
        String payload = "[{\"typeName\":\"" + classification + "\"}]";
        HttpURLConnection connection = openClassificationsPost(cfg, guid, payload);

        try {
            int status = connection.getResponseCode();
            assertTrue(status >= 200 && status < 300, "classification POST failed: HTTP " + status);
        } finally {
            connection.disconnect();
        }
    }

    private static HttpURLConnection openClassificationsPost(TrinoExtractorITSupport.LiveStackConfig cfg, String guid, String payload) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL(cfg.atlasRestUrl + "api/atlas/v2/entity/guid/" + guid + "/classifications").openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        String credentials = cfg.atlasUsername + ":" + cfg.atlasPassword;
        String encoded = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        connection.setRequestProperty("Authorization", "Basic " + encoded);
        connection.getOutputStream().write(payload.getBytes(StandardCharsets.UTF_8));
        return connection;
    }
}
