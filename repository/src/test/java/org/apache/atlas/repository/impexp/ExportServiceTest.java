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


import com.google.inject.Inject;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityChangeNotifier;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStoreV1;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerV1;
import org.apache.atlas.repository.store.graph.v1.SoftDeleteHandlerV1;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;
import scala.actors.threadpool.Arrays;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

@Guice(modules = RepositoryMetadataModule.class)
public class ExportServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(ExportServiceTest.class);

    @Inject
    AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    ExportService exportService;
    private DeleteHandlerV1 deleteHandler = mock(SoftDeleteHandlerV1.class);;
    private AtlasEntityChangeNotifier mockChangeNotifier = mock(AtlasEntityChangeNotifier.class);
    private AtlasEntityStoreV1 entityStore;

    @BeforeClass
    public void setupSampleData() throws AtlasBaseException {
        entityStore = new AtlasEntityStoreV1(deleteHandler, typeRegistry, mockChangeNotifier);;

        AtlasTypesDef sampleTypes = TestUtilsV2.defineDeptEmployeeTypes();
        AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(sampleTypes, typeRegistry);

        if (!typesToCreate.isEmpty()) {
            typeDefStore.createTypesDef(typesToCreate);
        }

        AtlasEntity.AtlasEntitiesWithExtInfo  hrDept = TestUtilsV2.createDeptEg2();

        AtlasEntityStream entityStream = new AtlasEntityStream(hrDept);
        entityStore.createOrUpdate(entityStream, false);
        LOG.debug("==> setupSampleData: ", AtlasEntity.dumpObjects(hrDept.getEntities(), null).toString());
    }

    @BeforeTest
    public void setupExportService () throws AtlasBaseException {
        exportService = new ExportService(typeRegistry);
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
        if(addFetchType) {
            if(optionsMap == null) {
                optionsMap = new HashMap<>();
            }

            optionsMap.put("fetchType", fetchTypeValue);
            request.setOptions(optionsMap);
        }

        if(addMatchType) {
            if(optionsMap == null) {
                optionsMap = new HashMap<>();
            }

            optionsMap.put("matchType", matchTypeValue);
        }

        if(optionsMap != null) {
            request.setOptions(optionsMap);
        }
    }

    private ZipSource runExportWithParameters(AtlasExportRequest request) throws AtlasBaseException, IOException {
        final String requestingIP = "1.0.0.0";
        final String hostName = "localhost";
        final String userName = "admin";

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipSink zipSink = new ZipSink(baos);
        AtlasExportResult result = exportService.run(zipSink, request, userName, hostName, requestingIP);
        Assert.assertEquals(result.getOperationStatus(), AtlasExportResult.OperationStatus.SUCCESS);

        zipSink.close();

        ByteArrayInputStream bis = new ByteArrayInputStream(baos.toByteArray());
        ZipSource zipSource = new ZipSource(bis);
        return zipSource;
    }

    @Test
    public void exportType_Succeeds() throws AtlasBaseException, FileNotFoundException {
        String requestingIP = "1.0.0.0";
        String hostName = "root";

        AtlasExportRequest request = getRequestForFullFetch();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipSink zipSink = new ZipSink(baos);
        AtlasExportResult result = exportService.run(zipSink, request, "admin", hostName, requestingIP);

        Assert.assertNotNull(exportService);
        Assert.assertEquals(result.getHostName(), hostName);
        Assert.assertEquals(result.getClientIpAddress(), requestingIP);
        Assert.assertEquals(request, result.getRequest());
    }

    @Test
    public void requestingEntityNotFound_NoData() throws AtlasBaseException, IOException {
        String requestingIP = "1.0.0.0";
        String hostName = "root";

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ZipSink zipSink = new ZipSink(baos);
        AtlasExportResult result = exportService.run(
                zipSink, getRequestForFullFetch(), "admin", hostName, requestingIP);

        Assert.assertNull(result.getData());

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ZipSource zipSource = new ZipSource(bais);

        Assert.assertNotNull(exportService);
        Assert.assertNotNull(zipSource.getCreationOrder());
        Assert.assertFalse(zipSource.hasNext());
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

    private void verifyExportForEmployeeData(ZipSource zipSource) throws AtlasBaseException {
        final List<String> expectedEntityTypes = Arrays.asList(new String[]{"Manager", "Employee", "Department"});

        Assert.assertNotNull(zipSource.getCreationOrder());
        Assert.assertEquals(zipSource.getCreationOrder().size(), 2);
        Assert.assertTrue(zipSource.hasNext());

        while (zipSource.hasNext()) {
            AtlasEntity entity = zipSource.next();

            Assert.assertNotNull(entity);
            Assert.assertEquals(entity.getStatus(), AtlasEntity.Status.ACTIVE);
            Assert.assertTrue(expectedEntityTypes.contains(entity.getTypeName()));
        }

        verifyTypeDefs(zipSource);
    }

    private void verifyExportForHrData(ZipSource zipSource) throws IOException, AtlasBaseException {
        Assert.assertNotNull(zipSource.getCreationOrder());
        Assert.assertTrue(zipSource.getCreationOrder().size() == 1);
        Assert.assertTrue(zipSource.hasNext());

        AtlasEntity entity = zipSource.next();

        Assert.assertNotNull(entity);
        Assert.assertTrue(entity.getTypeName().equals("Department"));
        Assert.assertEquals(entity.getStatus(), AtlasEntity.Status.ACTIVE);
        verifyTypeDefs(zipSource);
    }

    private void verifyExportForHrDataForConnected(ZipSource zipSource) throws IOException, AtlasBaseException {
        Assert.assertNotNull(zipSource.getCreationOrder());
        Assert.assertTrue(zipSource.getCreationOrder().size() == 2);
        Assert.assertTrue(zipSource.hasNext());

        AtlasEntity entity = zipSource.next();

        Assert.assertNotNull(entity);
        Assert.assertTrue(entity.getTypeName().equals("Department"));
        Assert.assertEquals(entity.getStatus(), AtlasEntity.Status.ACTIVE);
        verifyTypeDefs(zipSource);
    }

    private void verifyTypeDefs(ZipSource zipSource) throws AtlasBaseException {
        Assert.assertEquals(zipSource.getTypesDef().getEnumDefs().size(), 1);
        Assert.assertEquals(zipSource.getTypesDef().getClassificationDefs().size(), 0);
        Assert.assertEquals(zipSource.getTypesDef().getStructDefs().size(), 1);
        Assert.assertEquals(zipSource.getTypesDef().getEntityDefs().size(), 4);
    }
}
