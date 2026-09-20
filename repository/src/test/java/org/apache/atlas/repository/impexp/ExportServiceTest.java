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

import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class ExportServiceTest extends AtlasTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(ExportServiceTest.class);

    @Inject
    AtlasTypeRegistry typeRegistry;
    @Inject
    ExportService exportService;
    @Inject
    private AtlasTypeDefStore typeDefStore;
    @Inject
    private ExportImportAuditService auditService;

    @Inject
    private AtlasEntityStoreV2 entityStore;

    @Inject
    private ImportService importService;

    private static final String HIVE_TABLE_TYPE_NAME = "hive_table";

    private static final String TABLE_1_GUID = "9f9e5088-3ace-4dd7-ae3e-41a93875e264";
    private static final String TABLE_2_GUID = "49dc552e-835a-4b88-b752-220e03c6df36";
    private static final String TABLE_3_GUID = "8b2f7e53-ac4b-4c6b-8159-8b6b62805d1a";
    private static final String TABLE_4_GUID = "e5c2edd4-df48-4646-8f22-0fbdce235496";
    private static final String TABLE_5_GUID = "1f6cb442-e7c4-4521-827a-49d567240e74";
    private static final String TABLE_6_GUID = "edc9facc-2e76-4dbd-830d-ad7644541451";

    private static final String TABLE_1_QUALIFIED_NAME = "hivedb01.hivetable01@primary";
    private static final String TABLE_2_QUALIFIED_NAME = "hivedb01.hivetable02@primary";
    private static final String TABLE_3_QUALIFIED_NAME = "hivedb01.hivetable03@primary";
    private static final String TABLE_4_QUALIFIED_NAME = "hivedb01.hivetable04@primary";
    private static final String TABLE_5_QUALIFIED_NAME = "hivedb01.hivetable05@primary";
    private static final String TABLE_6_QUALIFIED_NAME = "hivedb01.hivetable06@primary";

    @BeforeTest
    public void setupTest() throws IOException, AtlasBaseException {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);
        basicSetup(typeDefStore, typeRegistry);
    }

    @BeforeClass
    public void setupSampleData() throws Exception {
        super.initialize();

        AtlasTypesDef sampleTypes   = TestUtilsV2.defineDeptEmployeeTypes();
        AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(sampleTypes, typeRegistry);

        if (!typesToCreate.isEmpty()) {
            typeDefStore.createTypesDef(typesToCreate);
        }

        AtlasEntity.AtlasEntitiesWithExtInfo deptEg2      = TestUtilsV2.createDeptEg2();
        AtlasEntityStream                    entityStream = new AtlasEntityStream(deptEg2);
        EntityMutationResponse               emr          = entityStore.createOrUpdate(entityStream, false);
        assertNotNull(emr);
        assertNotNull(emr.getCreatedEntities());
        assertFalse(emr.getCreatedEntities().isEmpty());
    }

    @AfterClass
    public void clear() throws Exception {
        Thread.sleep(1000);
        assertExportImportAuditEntry(auditService);
        AtlasGraphProvider.cleanup();

        super.cleanup();
    }

    @Test
    public void exportType() throws AtlasBaseException {
        String requestingIP = "1.0.0.0";
        String hostName     = "root";

        AtlasExportRequest    request = getRequestForFullFetch();
        ByteArrayOutputStream baos    = new ByteArrayOutputStream();
        ZipSink               zipSink = new ZipSink(baos);
        AtlasExportResult     result  = exportService.run(zipSink, request, "admin", hostName, requestingIP);

        assertNotNull(exportService);
        assertEquals(result.getHostName(), hostName);
        assertEquals(result.getClientIpAddress(), requestingIP);
        assertEquals(request, result.getRequest());
        assertNotNull(result.getSourceClusterName());
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void requestingEntityNotFound_NoData() throws AtlasBaseException, IOException {
        String requestingIP = "1.0.0.0";
        String hostName     = "root";

        ByteArrayOutputStream baos    = new ByteArrayOutputStream();
        ZipSink               zipSink = new ZipSink(baos);
        AtlasExportResult result = exportService.run(
                zipSink, getRequestForFullFetch(), "admin", hostName, requestingIP);

        assertNull(result.getData());

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        new ZipSource(bais);
    }

    @Test
    public void requestingEntityFoundDefaultFetch_ContainsData() throws Exception {
        ZipSource source = runExportWithParameters(
                getRequestForDept(false, "", false, ""));
        verifyExportForHrData(source);
    }

    @Test
    public void requestingHrEntityWithMatchTypeContains_ContainsData() throws Exception {
        ZipSource source = runExportWithParameters(
                getRequestForDept(false, "", true, "CONTAINS"));
        verifyExportForHrData(source);
    }

    @Test
    public void requestingHrEntityWithMatchTypeEndsWith_ContainsData() throws Exception {
        ZipSource source = runExportWithParameters(
                getRequestForDept(false, "", true, "ENDSWITH"));
        verifyExportForHrData(source);
    }

    @Test
    public void requestingDeptEntityFoundFullFetch_ContainsData() throws Exception {
        ZipSource source = runExportWithParameters(
                getRequestForDept(true, "FULL", false, ""));
        verifyExportForHrData(source);
    }

    @Test
    public void requestingDeptEntityFoundConnectedFetch_ContainsData() throws Exception {
        ZipSource source = runExportWithParameters(
                getRequestForDept(true, "CONNECTED", false, ""));
        verifyExportForHrDataForConnected(source);
    }

    @Test
    public void requestingEmployeeEntityFoundConnectedFetch_ContainsData() throws Exception {
        ZipSource zipSource = runExportWithParameters(getRequestForEmployee());
        verifyExportForEmployeeData(zipSource);
    }

    @Test
    public void verifyOverallStatus() {
        assertEquals(exportService.getOverallOperationStatus(), AtlasExportResult.OperationStatus.FAIL);

        assertEquals(exportService.getOverallOperationStatus(AtlasExportResult.OperationStatus.SUCCESS), AtlasExportResult.OperationStatus.SUCCESS);

        assertEquals(exportService.getOverallOperationStatus(
                AtlasExportResult.OperationStatus.SUCCESS,
                AtlasExportResult.OperationStatus.SUCCESS,
                AtlasExportResult.OperationStatus.SUCCESS), AtlasExportResult.OperationStatus.SUCCESS);

        assertEquals(exportService.getOverallOperationStatus(
                AtlasExportResult.OperationStatus.FAIL,
                AtlasExportResult.OperationStatus.PARTIAL_SUCCESS,
                AtlasExportResult.OperationStatus.SUCCESS), AtlasExportResult.OperationStatus.PARTIAL_SUCCESS);

        assertEquals(exportService.getOverallOperationStatus(
                AtlasExportResult.OperationStatus.FAIL,
                AtlasExportResult.OperationStatus.FAIL,
                AtlasExportResult.OperationStatus.PARTIAL_SUCCESS), AtlasExportResult.OperationStatus.PARTIAL_SUCCESS);

        assertEquals(exportService.getOverallOperationStatus(
                AtlasExportResult.OperationStatus.FAIL,
                AtlasExportResult.OperationStatus.FAIL,
                AtlasExportResult.OperationStatus.FAIL), AtlasExportResult.OperationStatus.FAIL);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void requestingExportOfNonExistentEntity_ReturnsFailure() throws Exception {
        AtlasExportRequest request = getRequestForEmployee();
        tamperEmployeeRequest(request);
        runExportWithParameters(request);
    }

    @Test
    public void requestForTypeFull() {
        AtlasExportRequest req = getRequestForTypeFull("Department,Employee");

        assertNotNull(req);
        assertEquals(req.getItemsToExport().size(), 1);
        assertEquals(req.getOptions().get(AtlasExportRequest.OPTION_ATTR_MATCH_TYPE), "forType");
    }

    @Test
    public void verifyTypeFull() throws AtlasBaseException, IOException {
        ZipSource zipSource = runExportWithParameters(getRequestForTypeFull("Department,Employee,Manager"));
        verifyExportForFullEmployeeData(zipSource);
    }

    @DataProvider(name = "ctashivetables")
    public static Object[][] importCtasData(ITestContext context) throws IOException, AtlasBaseException {
        return ZipFileResourceTestUtils.getZipSource("ctas_hive_tables.zip");
    }

    @Test(dataProvider = "ctashivetables")
    public void testExportConnectedHiveTables(InputStream inputStream) throws AtlasBaseException, IOException {
        AtlasImportResult result = ZipFileResourceTestUtils.runImportWithNoParameters(importService, inputStream);
        assertEquals(result.getOperationStatus(), AtlasImportResult.OperationStatus.SUCCESS);

        AtlasExportRequest request = getRequestForConnected(HIVE_TABLE_TYPE_NAME, TABLE_1_QUALIFIED_NAME, TABLE_2_QUALIFIED_NAME);
        ZipSource zipSource = runExportWithParameters(request);
        List<String> guidList = zipSource.getCreationOrder();

        assertTrue(guidList.contains(TABLE_1_GUID));
        assertTrue(guidList.contains(TABLE_2_GUID));
        assertTrue(guidList.contains(TABLE_3_GUID));
        assertFalse(guidList.contains(TABLE_4_GUID));
        assertFalse(guidList.contains(TABLE_5_GUID));
        assertFalse(guidList.contains(TABLE_6_GUID));

        request = getRequestForConnected(HIVE_TABLE_TYPE_NAME, TABLE_1_QUALIFIED_NAME, TABLE_6_QUALIFIED_NAME);
        zipSource = runExportWithParameters(request);
        guidList = zipSource.getCreationOrder();

        assertTrue(guidList.contains(TABLE_1_GUID));
        assertTrue(guidList.contains(TABLE_2_GUID));
        assertFalse(guidList.contains(TABLE_3_GUID));
        assertFalse(guidList.contains(TABLE_4_GUID));
        assertTrue(guidList.contains(TABLE_5_GUID));
        assertTrue(guidList.contains(TABLE_6_GUID));

        request = getRequestForConnected(HIVE_TABLE_TYPE_NAME, TABLE_1_QUALIFIED_NAME, TABLE_4_QUALIFIED_NAME);
        zipSource = runExportWithParameters(request);
        guidList = zipSource.getCreationOrder();

        assertTrue(guidList.contains(TABLE_1_GUID));
        assertTrue(guidList.contains(TABLE_2_GUID));
        assertTrue(guidList.contains(TABLE_3_GUID));
        assertTrue(guidList.contains(TABLE_4_GUID));
        assertTrue(guidList.contains(TABLE_5_GUID));
        assertFalse(guidList.contains(TABLE_6_GUID));
    }

    private AtlasExportRequest getRequestForConnected(String typeName, String qualifiedName1, String qualifiedName2) {
        AtlasExportRequest request = new AtlasExportRequest();
        List<AtlasObjectId> itemsToExport = new ArrayList<>();
        itemsToExport.add(new AtlasObjectId(typeName, "qualifiedName", qualifiedName1));
        itemsToExport.add(new AtlasObjectId(typeName, "qualifiedName", qualifiedName2));
        request.setItemsToExport(itemsToExport);
        setOptionsMap(request, true, AtlasExportRequest.FETCH_TYPE_CONNECTED, false, "");
        return request;
    }

    private AtlasExportRequest getRequestForFullFetch() {
        AtlasExportRequest request = new AtlasExportRequest();

        List<AtlasObjectId> itemsToExport = new ArrayList<>();
        itemsToExport.add(new AtlasObjectId("hive_db", "qualifiedName", "default@cl1"));
        request.setItemsToExport(itemsToExport);

        return request;
    }

    private AtlasExportRequest getRequestForDept(boolean addFetchType, String fetchTypeValue, boolean addMatchType, String matchTypeValue) {
        AtlasExportRequest request = new AtlasExportRequest();

        List<AtlasObjectId> itemsToExport = new ArrayList<>();
        itemsToExport.add(new AtlasObjectId("Department", "name", "hr"));
        request.setItemsToExport(itemsToExport);

        setOptionsMap(request, addFetchType, fetchTypeValue, addMatchType, matchTypeValue);
        return request;
    }

    private AtlasExportRequest getRequestForEmployee() {
        AtlasExportRequest request = new AtlasExportRequest();

        List<AtlasObjectId> itemsToExport = new ArrayList<>();
        itemsToExport.add(new AtlasObjectId("Employee", "name", "Max"));
        request.setItemsToExport(itemsToExport);

        setOptionsMap(request, true, "CONNECTED", false, "");
        return request;
    }

    private void setOptionsMap(AtlasExportRequest request,
            boolean addFetchType, String fetchTypeValue, boolean addMatchType, String matchTypeValue) {
        Map<String, Object> optionsMap = null;
        if (addFetchType) {
            if (optionsMap == null) {
                optionsMap = new HashMap<>();
            }

            optionsMap.put("fetchType", fetchTypeValue);
            request.setOptions(optionsMap);
        }

        if (addMatchType) {
            if (optionsMap == null) {
                optionsMap = new HashMap<>();
            }

            optionsMap.put("matchType", matchTypeValue);
        }

        if (optionsMap != null) {
            request.setOptions(optionsMap);
        }
    }

    private ZipSource runExportWithParameters(AtlasExportRequest request) throws AtlasBaseException, IOException {
        final String requestingIP = "1.0.0.0";
        final String hostName     = "localhost";
        final String userName     = "admin";

        ByteArrayOutputStream baos    = new ByteArrayOutputStream();
        ZipSink               zipSink = new ZipSink(baos);
        AtlasExportResult     result  = exportService.run(zipSink, request, userName, hostName, requestingIP);

        zipSink.close();

        ByteArrayInputStream bis = new ByteArrayInputStream(baos.toByteArray());

        return new ZipSource(bis);
    }

    private AtlasExportRequest getRequestForTypeFull(String type) {
        String jsonRequest = "{ \"itemsToExport\": [ { \"typeName\": \"%s\" } ], \"options\": {  \"fetchType\": \"FULL\", \"matchType\": \"forType\"} }";
        return AtlasType.fromJson(String.format(jsonRequest, type), AtlasExportRequest.class);
    }

    private void tamperEmployeeRequest(AtlasExportRequest request) {
        AtlasObjectId objectId = request.getItemsToExport().get(0);
        objectId.getUniqueAttributes().remove("name");
        objectId.getUniqueAttributes().put("qualifiedName", "XXX@121");
    }

    private void verifyExportForEmployeeData(ZipSource zipSource) throws AtlasBaseException {
        final List<String> expectedEntityTypes = Arrays.asList("Manager", "Employee", "Department");

        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 2);
        assertTrue(zipSource.hasNext());

        while (zipSource.hasNext()) {
            AtlasEntity entity = zipSource.next();

            assertNotNull(entity);
            assertEquals(entity.getStatus(), AtlasEntity.Status.ACTIVE);
            assertTrue(expectedEntityTypes.contains(entity.getTypeName()));
        }

        verifyTypeDefs(zipSource);
    }

    private void verifyExportForFullEmployeeData(ZipSource zipSource) throws AtlasBaseException {
        final List<String> expectedEntityTypes = Arrays.asList("Manager", "Employee", "Department");

        assertNotNull(zipSource.getCreationOrder());
        assertTrue(zipSource.hasNext());

        while (zipSource.hasNext()) {
            AtlasEntity entity = zipSource.next();

            assertNotNull(entity);
            assertEquals(entity.getStatus(), AtlasEntity.Status.ACTIVE);
            assertTrue(expectedEntityTypes.contains(entity.getTypeName()));
        }

        verifyTypeDefs(zipSource);
    }

    private void verifyExportForHrData(ZipSource zipSource) throws AtlasBaseException {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 1);
        assertTrue(zipSource.hasNext());

        AtlasEntity entity = zipSource.next();

        assertNotNull(entity);
        assertEquals(entity.getTypeName(), "Department");
        assertEquals(entity.getStatus(), AtlasEntity.Status.ACTIVE);
        verifyTypeDefs(zipSource);
    }

    private void verifyExportForHrDataForConnected(ZipSource zipSource) throws AtlasBaseException {
        assertNotNull(zipSource.getCreationOrder());
        assertEquals(zipSource.getCreationOrder().size(), 1);
        assertTrue(zipSource.hasNext());

        AtlasEntity entity = zipSource.next();

        assertNotNull(entity);
        assertEquals(entity.getTypeName(), "Department");
        assertEquals(entity.getStatus(), AtlasEntity.Status.ACTIVE);
        verifyTypeDefs(zipSource);
    }

    private void verifyTypeDefs(ZipSource zipSource) throws AtlasBaseException {
        assertEquals(zipSource.getTypesDef().getEnumDefs().size(), 1);
        assertEquals(zipSource.getTypesDef().getClassificationDefs().size(), 0);
        assertEquals(zipSource.getTypesDef().getStructDefs().size(), 1);
        assertEquals(zipSource.getTypesDef().getEntityDefs().size(), 4);
    }
}
