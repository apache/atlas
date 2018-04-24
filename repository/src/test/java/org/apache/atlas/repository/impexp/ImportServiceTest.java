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
package org.apache.atlas.repository.impexp;

import com.google.inject.Inject;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.graph.GraphSandboxUtil.useLocalSolr;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ImportServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(ImportServiceTest.class);
    private static final int DEFAULT_LIMIT = 25;
    private final ImportService importService;

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private EntityDiscoveryService discoveryService;

    @Inject
    public ImportServiceTest(ImportService importService) {
        this.importService = importService;
    }

    @BeforeTest
    public void setupTest() {
        RequestContextV1.clear();
        RequestContextV1.get().setUser(TestUtilsV2.TEST_USER, null);
    }

    @AfterClass
    public void clear() throws Exception {
        AtlasGraphProvider.cleanup();

        if (useLocalSolr()) {
            LocalSolrRunner.stop();
        }
    }

    @DataProvider(name = "sales")
    public static Object[][] getDataFromQuickStart_v1_Sales(ITestContext context) throws IOException {
        return getZipSource("sales-v1-full.zip");
    }

    @Test(dataProvider = "sales")
    public void importDB1(ZipSource zipSource) throws AtlasBaseException, IOException {
        loadBaseModel();
        runAndVerifyQuickStart_v1_Import(importService, zipSource);
    }

    @DataProvider(name = "reporting")
    public static Object[][] getDataFromReporting() throws IOException {
        return getZipSource("reporting-v1-full.zip");
    }

    @Test(dataProvider = "reporting")
    public void importDB2(ZipSource zipSource) throws AtlasBaseException, IOException {
        loadBaseModel();
        runAndVerifyQuickStart_v1_Import(importService, zipSource);
    }

    @DataProvider(name = "logging")
    public static Object[][] getDataFromLogging(ITestContext context) throws IOException {
        return getZipSource("logging-v1-full.zip");
    }

    @Test(dataProvider = "logging")
    public void importDB3(ZipSource zipSource) throws AtlasBaseException, IOException {
        loadBaseModel();
        runAndVerifyQuickStart_v1_Import(importService, zipSource);
    }

    @DataProvider(name = "salesNewTypeAttrs")
    public static Object[][] getDataFromSalesNewTypeAttrs(ITestContext context) throws IOException {
        return getZipSource("salesNewTypeAttrs.zip");
    }

    @Test(dataProvider = "salesNewTypeAttrs", dependsOnMethods = "importDB1")
    public void importDB4(ZipSource zipSource) throws AtlasBaseException, IOException {
        loadBaseModel();
        runImportWithParameters(importService, getDefaultImportRequest(), zipSource);
    }

    @DataProvider(name = "salesNewTypeAttrs-next")
    public static Object[][] getDataFromSalesNewTypeAttrsNext(ITestContext context) throws IOException {
        return getZipSource("salesNewTypeAttrs-next.zip");
    }

    @Test(dataProvider = "salesNewTypeAttrs-next", dependsOnMethods = "importDB4")
    public void importDB5(ZipSource zipSource) throws AtlasBaseException, IOException {
        final String newEnumDefName = "database_action";

        assertNotNull(typeDefStore.getEnumDefByName(newEnumDefName));

        AtlasImportRequest request = getDefaultImportRequest();
        Map<String, String> options = new HashMap<>();
        options.put("updateTypeDefinition", "false");
        request.setOptions(options);

        runImportWithParameters(importService, request, zipSource);
        assertNotNull(typeDefStore.getEnumDefByName(newEnumDefName));
        assertEquals(typeDefStore.getEnumDefByName(newEnumDefName).getElementDefs().size(), 4);
    }

    @Test(dataProvider = "salesNewTypeAttrs-next", dependsOnMethods = "importDB4")
    public void importDB6(ZipSource zipSource) throws AtlasBaseException, IOException {
        final String newEnumDefName = "database_action";

        assertNotNull(typeDefStore.getEnumDefByName(newEnumDefName));

        AtlasImportRequest request = getDefaultImportRequest();
        Map<String, String> options = new HashMap<>();
        options.put("updateTypeDefinition", "true");
        request.setOptions(options);

        runImportWithParameters(importService, request, zipSource);
        assertNotNull(typeDefStore.getEnumDefByName(newEnumDefName));
        assertEquals(typeDefStore.getEnumDefByName(newEnumDefName).getElementDefs().size(), 8);
    }

    @DataProvider(name = "ctas")
    public static Object[][] getDataFromCtas(ITestContext context) throws IOException {
        return getZipSource("ctas.zip");
    }

    @Test(dataProvider = "ctas")
    public void importCTAS(ZipSource zipSource) throws IOException, AtlasBaseException {
        loadBaseModel();
        loadHiveModel();

        runImportWithNoParameters(importService, zipSource);
    }

    @DataProvider(name = "stocks-glossary")
    public static Object[][] getDataFromGlossary(ITestContext context) throws IOException {
        return getZipSource("stocks-glossary.zip");
    }

    @Test(dataProvider = "stocks-glossary")
    public void importGlossary(ZipSource zipSource) throws IOException, AtlasBaseException {
        loadBaseModel();
        loadGlossary();
        runImportWithNoParameters(importService, zipSource);

        assertEntityCount("__AtlasGlossary", "40c80052-3129-4f7c-8f2f-391677935416", 1);
        assertEntityCount("__AtlasGlossaryTerm", "e93ac426-de04-4d54-a7c9-d76c1e96369b", 1);
        assertEntityCount("__AtlasGlossaryTerm", "93ad3bf6-23dc-4e3f-b70e-f8fad6438203", 1);
        assertEntityCount("__AtlasGlossaryTerm", "105533b6-c125-4a87-bed5-cdf67fb68c39", 1);
    }

    private List<AtlasEntityHeader> getEntitiesFromDB(String query, String guid) throws AtlasBaseException {
        String q = StringUtils.isEmpty(guid) ? query : String.format("%s where __guid = '%s'", query, guid);
        return discoveryService.searchUsingDslQuery(q, DEFAULT_LIMIT, 0).getEntities();
    }

    @DataProvider(name = "hdfs_path1")
    public static Object[][] getDataFromHdfsPath1(ITestContext context) throws IOException {
        return getZipSource("hdfs_path1.zip");
    }

    @Test(dataProvider = "hdfs_path1", expectedExceptions = AtlasBaseException.class)
    public void importHdfs_path1(ZipSource zipSource) throws IOException, AtlasBaseException {
        loadBaseModel();
        loadFsModel();
        loadModelFromResourcesJson("tag1.json", typeDefStore, typeRegistry);

        try {
            runImportWithNoParameters(importService, zipSource);
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_IMPORT_ATTRIBUTE_TYPE_CHANGED);
            AtlasClassificationType tag1 = typeRegistry.getClassificationTypeByName("tag1");
            assertNotNull(tag1);
            assertEquals(tag1.getAllAttributes().size(), 2);
            throw e;
        }
    }

    @DataProvider(name = "relationshipLineage")
    public static Object[][] getImportWithRelationships(ITestContext context) throws IOException {
        return getZipSource("rel-lineage.zip");
    }

    @Test(dataProvider = "relationshipLineage")
    public void importDB8(ZipSource zipSource) throws AtlasBaseException, IOException {
        loadBaseModel();
        loadHiveModel();
        AtlasImportRequest request = getDefaultImportRequest();
        runImportWithParameters(importService, request, zipSource);
    }

    @DataProvider(name = "relationship")
    public static Object[][] getImportWithRelationshipsWithLineage(ITestContext context) throws IOException {
        return getZipSource("stocks-rel-2.zip");
    }

    @Test(dataProvider = "relationship")
    public void importDB7(ZipSource zipSource) throws AtlasBaseException, IOException {
        loadBaseModel();
        loadHiveModel();
        AtlasImportRequest request = getDefaultImportRequest();
        runImportWithParameters(importService, request, zipSource);

        assertEntityCount("hive_db", "d7dc0848-fbba-4d63-9264-a460798361f5", 1);
        assertEntityCount("hive_table", "2fb31eaa-4bb2-4eb8-b333-a888ba7c84fe", 1);
        assertEntityCount("hive_column", "13422f0c-9265-4960-91a9-290ffd83b7f1",1);
        assertEntityCount("hive_column", "c1ae870f-ce0c-44ae-832f-ff77035b1f7e",1);
        assertEntityCount("hive_column", "b84baab3-0664-4f13-82f1-e81d043db02f",1);
        assertEntityCount("hive_column", "53ea1991-6ca8-44f2-a75e-61b8d4866fc8",1);
        assertEntityCount("hive_column", "a973c04c-aa42-49f4-877c-66fbe6754fb5",1);
        assertEntityCount("hive_column", "a4550803-f18e-4072-a1e8-1201e6022a58",1);
        assertEntityCount("hive_column", "6c4f196a-4046-493b-8c3a-2b1a9ef255a2",1);
    }

    private void assertEntityCount(String entityType, String guid, int expectedCount) throws AtlasBaseException {
        assertEquals(getEntitiesFromDB(entityType, guid).size(), expectedCount);
    }

    @Test
    public void importServiceProcessesIOException() {
        ImportService importService = new ImportService(typeDefStore, typeRegistry, null);
        AtlasImportRequest req = mock(AtlasImportRequest.class);

        Answer<Map> answer = invocationOnMock -> {
            throw new IOException("file is read only");
        };

        when(req.getFileName()).thenReturn("some-file.zip");
        when(req.getOptions()).thenAnswer(answer);

        try {
            importService.run(req, "a", "b", "c");
        }
        catch (AtlasBaseException ex) {
            assertEquals(ex.getAtlasErrorCode().getErrorCode(), AtlasErrorCode.INVALID_PARAMETERS.getErrorCode());
        }
    }

    private void loadFsModel() throws IOException, AtlasBaseException {
        loadModelFromJson("1000-Hadoop/1020-fs_model.json", typeDefStore, typeRegistry);
    }

    private void loadHiveModel() throws IOException, AtlasBaseException {
        loadModelFromJson("1000-Hadoop/1030-hive_model.json", typeDefStore, typeRegistry);
    }

    private void loadBaseModel() throws IOException, AtlasBaseException {
        loadModelFromJson("0000-Area0/0010-base_model.json", typeDefStore, typeRegistry);
    }

    private void loadGlossary() throws IOException, AtlasBaseException {
        loadModelFromJson("0000-Area0/0011-glossary_model.json", typeDefStore, typeRegistry);
    }
}
