/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.impexp;

import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.ITestContext;
import org.testng.annotations.Test;
import org.testng.annotations.Guice;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;

import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import static org.apache.atlas.graph.GraphSandboxUtil.useLocalSolr;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.*;
import static org.testng.Assert.*;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class RelationshipAttributesExtractorTest {

    private static final String EXPORT_FULL = "full";
    private static final String EXPORT_CONNECTED = "connected";
    private static final String QUALIFIED_NAME_DB = "db_test_1@02052019";
    private static final String QUALIFIED_NAME_TABLE_LINEAGE = "db_test_1.test_tbl_ctas_2@02052019";
    private static final String QUALIFIED_NAME_TABLE_NON_LINEAGE = "db_test_1.test_tbl_1@02052019";

    private static final String GUID_DB = "f0b72ab4-7452-4e42-ac74-2aee7728cce4";
    private static final String GUID_TABLE_1 = "4d5adf00-2c9b-4877-ad23-c41fd7319150";
    private static final String GUID_TABLE_2 = "8d0b834c-61ce-42d8-8f66-6fa51c36bccb";
    private static final String GUID_TABLE_CTAS_2 = "eaec545b-3ac7-4e1b-a497-bd4a2b6434a2";
    private static final String GUID_HIVE_PROCESS = "bd3138b2-f29e-4226-b859-de25eaa1c18b";

    @Inject
    private ImportService importService;

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private ExportService exportService;

    @BeforeClass
    public void setup() throws IOException, AtlasBaseException {
        loadBaseModel();
        loadHiveModel();
    }

    @BeforeTest
    public void setupTest() {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);
    }

    @AfterClass
    public void clear() throws Exception {
        AtlasGraphProvider.cleanup();

        if (useLocalSolr()) {
            LocalSolrRunner.stop();
        }
    }

    @DataProvider(name = "hiveDb")
    public static Object[][] getData(ITestContext context) throws IOException, AtlasBaseException {
        return getZipSource("hive_db_lineage.zip");
    }

    @Test(dataProvider = "hiveDb")
    public void importHiveDb(InputStream inputStream) throws AtlasBaseException, IOException {
        runImportWithNoParameters(importService, inputStream);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportDBFull() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveDb(QUALIFIED_NAME_DB, EXPORT_FULL, false));
        verifyDBFull(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportDBFullSkipLineageFull() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveDb(QUALIFIED_NAME_DB, EXPORT_FULL, true));
        verifyDBFullSkipLineageFull(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableWithLineageFull() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_LINEAGE, EXPORT_FULL, false));
        verifyTableWithLineageFull(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableWithLineageSkipLineageFull() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_LINEAGE, EXPORT_FULL, true));
        verifyTableWithLineageSkipLineageFull(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableWithoutLineageFull() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_NON_LINEAGE, EXPORT_FULL, false));
        verifyTableWithoutLineageFull(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableWithoutLineageSkipLineageFull() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_NON_LINEAGE, EXPORT_FULL, true));
        verifyTableWithoutLineageSkipLineageFull(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportDBConn() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveDb(QUALIFIED_NAME_DB, EXPORT_CONNECTED, false));
        verifyDBConn(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportDBSkipLineageConn() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveDb(QUALIFIED_NAME_DB, EXPORT_CONNECTED, true));
        verifyDBSkipLineageConn(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableWithLineageConn() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_LINEAGE, EXPORT_CONNECTED, false));
        verifyTableWithLineageConn(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableWithLineageSkipLineageConn() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_LINEAGE, EXPORT_CONNECTED, true));
        verifyTableWithLineageSkipLineageConn(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableWithoutLineageConn() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_NON_LINEAGE, EXPORT_CONNECTED, false));
        verifyTableWithoutLineageConn(source);
    }

    @Test(dependsOnMethods = "importHiveDb")
    public void exportTableWithoutLineageSkipLineageConn() throws Exception {
        ZipSource source = runExport(getExportRequestForHiveTable(QUALIFIED_NAME_TABLE_NON_LINEAGE, EXPORT_CONNECTED, true));
        verifyTableWithoutLineageSkipLineageConn(source);
    }

    private void loadHiveModel() throws IOException, AtlasBaseException {
        loadModelFromJson("1000-Hadoop/1030-hive_model.json", typeDefStore, typeRegistry);
    }

    private void loadBaseModel() throws IOException, AtlasBaseException {
        loadModelFromJson("0000-Area0/0010-base_model.json", typeDefStore, typeRegistry);
    }

    private AtlasExportRequest getExportRequestForHiveDb(String hiveDbName, String fetchType, boolean skipLineage) {
        AtlasExportRequest request = new AtlasExportRequest();

        List<AtlasObjectId> itemsToExport = new ArrayList<>();
        itemsToExport.add(new AtlasObjectId("hive_db", "qualifiedName", hiveDbName));
        request.setItemsToExport(itemsToExport);
        request.setOptions(getOptionsMap(fetchType, skipLineage));

        return request;
    }

    private AtlasExportRequest getExportRequestForHiveTable(String hiveTableName, String fetchType, boolean skipLineage) {
        AtlasExportRequest request = new AtlasExportRequest();

        List<AtlasObjectId> itemsToExport = new ArrayList<>();
        itemsToExport.add(new AtlasObjectId("hive_table", "qualifiedName", hiveTableName));
        request.setItemsToExport(itemsToExport);
        request.setOptions(getOptionsMap(fetchType, skipLineage));

        return request;
    }

    private Map<String, Object> getOptionsMap(String fetchType, boolean skipLineage){
        Map<String, Object> optionsMap = new HashMap<>();
        optionsMap.put("fetchType", fetchType.isEmpty() ? "full" : fetchType );
        optionsMap.put("skipLineage", skipLineage);

        return optionsMap;
    }

    private ZipSource runExport(AtlasExportRequest request) throws AtlasBaseException, IOException {
        final String requestingIP = "1.0.0.0";
        final String hostName = "localhost";
        final String userName = "admin";

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipSink zipSink = new ZipSink(baos);
        AtlasExportResult result = exportService.run(zipSink, request, userName, hostName, requestingIP);

        zipSink.close();

        ByteArrayInputStream bis = new ByteArrayInputStream(baos.toByteArray());
        ZipSource zipSource = new ZipSource(bis);
        return zipSource;
    }

    private void verifyDBFull(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 5);

        assertTrue(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_1, GUID_TABLE_2, GUID_TABLE_CTAS_2, GUID_HIVE_PROCESS);
    }

    private void verifyDBFullSkipLineageFull(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 4);

        assertFalse(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_1, GUID_TABLE_2, GUID_TABLE_CTAS_2);
    }

    private void verifyTableWithLineageFull(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 5);

        assertTrue(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_1, GUID_TABLE_2, GUID_TABLE_CTAS_2, GUID_HIVE_PROCESS);
    }

    private void verifyTableWithLineageSkipLineageFull(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 4);

        assertFalse(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_1, GUID_TABLE_2, GUID_TABLE_CTAS_2);
    }

    private void verifyTableWithoutLineageFull(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 5);

        assertTrue(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_1, GUID_TABLE_2, GUID_TABLE_CTAS_2,GUID_HIVE_PROCESS);
    }

    private void verifyTableWithoutLineageSkipLineageFull(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 4);

        assertFalse(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_1, GUID_TABLE_2, GUID_TABLE_CTAS_2);
    }


    private void verifyDBConn(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 5);

        assertTrue(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_1, GUID_TABLE_2, GUID_TABLE_CTAS_2, GUID_HIVE_PROCESS);
    }

    private void verifyDBSkipLineageConn(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 4);

        assertFalse(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_1, GUID_TABLE_2, GUID_TABLE_CTAS_2);
    }

    private void verifyTableWithLineageConn(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 4);

        assertTrue(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_2, GUID_TABLE_CTAS_2, GUID_HIVE_PROCESS);
    }

    private void verifyTableWithLineageSkipLineageConn(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(),2);

        assertFalse(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_CTAS_2);;
    }

    private void verifyTableWithoutLineageConn(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 2);

        assertFalse(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_1);
    }

    private void verifyTableWithoutLineageSkipLineageConn(ZipSource zipSource) {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 2);;

        assertFalse(zipSource.getCreationOrder().contains(GUID_HIVE_PROCESS));
        verifyExpectedEntities(getFileNames(zipSource), GUID_DB, GUID_TABLE_1);
    }

    private void verifyExpectedEntities(List<String> fileNames, String... guids){
        assertEquals(fileNames.size(), guids.length);
        for (String guid : guids) {
            assertTrue(fileNames.contains(guid.toLowerCase()));
        }
    }

    private List<String> getFileNames(ZipSource zipSource){
        List<String> ret = new ArrayList<>();
        assertTrue(zipSource.hasNext());

        while (zipSource.hasNext()){
            AtlasEntity atlasEntity = zipSource.next();
            assertNotNull(atlasEntity);
            ret.add(atlasEntity.getGuid());
        }
        return ret;
    }
}
