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

package org.apache.atlas.examples;

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class QuickStartV2Test {
    @Mock
    private AtlasClientV2 mockAtlasClientV2;

    @Mock
    private EntityMutationResponse mockEntityMutationResponse;

    @Mock
    private AtlasEntityHeader mockEntityHeader;

    @Mock
    private AtlasEntityWithExtInfo mockEntityWithExtInfo;

    @Mock
    private AtlasSearchResult mockSearchResult;

    @Mock
    private AtlasLineageInfo mockLineageInfo;

    @Mock
    private AtlasTypesDef mockTypesDef;

    private QuickStartV2 quickStartV2;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Create QuickStartV2 instance with mocked dependencies using reflection
        quickStartV2 = createQuickStartV2WithMockedClient();
    }

    private QuickStartV2 createQuickStartV2WithMockedClient() throws Exception {
        // Use reflection to create QuickStartV2 instance and inject mocked AtlasClientV2
        QuickStartV2 instance = new QuickStartV2(new String[] {"http://localhost:21000"});

        // Use reflection to set the private AtlasClientV2 field
        Field clientField = QuickStartV2.class.getDeclaredField("atlasClientV2");
        clientField.setAccessible(true);
        clientField.set(instance, mockAtlasClientV2);

        return instance;
    }

    @Test
    public void testCreateTypes_Success() throws Exception {
        // Setup
        when(mockAtlasClientV2.createAtlasTypeDefs(any(AtlasTypesDef.class))).thenReturn(null);

        // Use reflection to call private method
        Method createTypesMethod = QuickStartV2.class.getDeclaredMethod("createTypes");
        createTypesMethod.setAccessible(true);

        // Execute
        createTypesMethod.invoke(quickStartV2);

        // Verify
        verify(mockAtlasClientV2).createAtlasTypeDefs(any(AtlasTypesDef.class));
    }

    @Test
    public void testCreateTypes_WithException() throws Exception {
        // Setup
        when(mockAtlasClientV2.createAtlasTypeDefs(any(AtlasTypesDef.class)))
                .thenThrow(new RuntimeException("Create types failed"));

        // Use reflection to call private method
        Method createTypesMethod = QuickStartV2.class.getDeclaredMethod("createTypes");
        createTypesMethod.setAccessible(true);

        // Execute & Verify
        Exception exception = expectThrows(Exception.class, () -> {
            createTypesMethod.invoke(quickStartV2);
        });

        assertTrue(exception.getCause() instanceof RuntimeException);
        assertEquals(exception.getCause().getMessage(), "Create types failed");
    }

    @Test
    public void testCreateTypeDefinitions_Success() throws Exception {
        // Use reflection to call private method
        Method createTypeDefinitionsMethod = QuickStartV2.class.getDeclaredMethod("createTypeDefinitions");
        createTypeDefinitionsMethod.setAccessible(true);

        AtlasTypesDef result = (AtlasTypesDef) createTypeDefinitionsMethod.invoke(quickStartV2);

        // Verify
        assertNotNull(result);
        assertNotNull(result.getEntityDefs());
        assertNotNull(result.getRelationshipDefs());
        assertNotNull(result.getClassificationDefs());
        assertNotNull(result.getBusinessMetadataDefs());
    }

    @Test
    public void testCreateEntities_Success() throws Exception {
        // Setup
        when(mockAtlasClientV2.createEntity(any(AtlasEntityWithExtInfo.class)))
                .thenReturn(mockEntityMutationResponse);
        when(mockEntityMutationResponse.getEntitiesByOperation(EntityOperation.CREATE))
                .thenReturn(Collections.singletonList(mockEntityHeader));
        when(mockEntityHeader.getGuid()).thenReturn("test-guid-123");
        when(mockAtlasClientV2.getEntityByGuid("test-guid-123"))
                .thenReturn(mockEntityWithExtInfo);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mock(AtlasEntity.class));

        // Use reflection to call private method
        Method createEntitiesMethod = QuickStartV2.class.getDeclaredMethod("createEntities");
        createEntitiesMethod.setAccessible(true);

        // Execute
        createEntitiesMethod.invoke(quickStartV2);

        // Verify
        verify(mockAtlasClientV2, atLeastOnce()).createEntity(any(AtlasEntityWithExtInfo.class));
    }

    @Test
    public void testCreateEntities_WithException() throws Exception {
        // Setup
        when(mockAtlasClientV2.createEntity(any(AtlasEntityWithExtInfo.class)))
                .thenThrow(new RuntimeException("Entity creation failed"));

        // Use reflection to call private method
        Method createEntitiesMethod = QuickStartV2.class.getDeclaredMethod("createEntities");
        createEntitiesMethod.setAccessible(true);

        // Execute & Verify
        Exception exception = expectThrows(Exception.class, () -> {
            createEntitiesMethod.invoke(quickStartV2);
        });

        assertTrue(exception.getCause() instanceof RuntimeException);
        assertEquals(exception.getCause().getMessage(), "Entity creation failed");
    }

    @Test
    public void testCreateStorageDescriptor_Success() throws Exception {
        // Use reflection to call private method
        Method createStorageDescriptorMethod = QuickStartV2.class.getDeclaredMethod("createStorageDescriptor",
                String.class, String.class, String.class, boolean.class);
        createStorageDescriptorMethod.setAccessible(true);

        // Execute
        AtlasEntity result = (AtlasEntity) createStorageDescriptorMethod.invoke(quickStartV2,
                "hdfs://test/location", "TextInputFormat", "TextOutputFormat", true);

        // Verify
        assertNotNull(result);
        assertEquals(result.getTypeName(), "StorageDesc");
        assertEquals(result.getAttribute("location"), "hdfs://test/location");
        assertEquals(result.getAttribute("inputFormat"), "TextInputFormat");
        assertEquals(result.getAttribute("outputFormat"), "TextOutputFormat");
        assertEquals(result.getAttribute("compressed"), true);
    }

    @Test
    public void testCreateColumn_Success() throws Exception {
        // Use reflection to call private method
        Method createColumnMethod = QuickStartV2.class.getDeclaredMethod("createColumn",
                String.class, String.class, String.class, String.class, String.class, String[].class);
        createColumnMethod.setAccessible(true);

        // Execute
        AtlasEntity result = (AtlasEntity) createColumnMethod.invoke(
                quickStartV2,
                "test_db", "test_table", "test_column", "string", "test comment", new String[] {"PII"});

        // Verify
        assertNotNull(result);
        assertEquals(result.getTypeName(), "Column");
        assertEquals(result.getAttribute("name"), "test_column");
        assertEquals(result.getAttribute("dataType"), "string");
        assertEquals(result.getAttribute("comment"), "test comment");
        assertNotNull(result.getClassifications());
        assertEquals(result.getClassifications().size(), 1);
        assertEquals(result.getClassifications().get(0).getTypeName(), "PII");
    }

    @Test
    public void testCreateView_Success() throws Exception {
        // Setup
        AtlasEntity mockDatabase = mock(AtlasEntity.class);
        when(mockDatabase.getGuid()).thenReturn("db-guid-123");

        AtlasEntity mockInputTable = mock(AtlasEntity.class);
        when(mockInputTable.getGuid()).thenReturn("table-guid-123");

        when(mockAtlasClientV2.createEntity(any(AtlasEntityWithExtInfo.class)))
                .thenReturn(mockEntityMutationResponse);
        when(mockEntityMutationResponse.getEntitiesByOperation(EntityOperation.CREATE))
                .thenReturn(Collections.singletonList(mockEntityHeader));
        when(mockEntityHeader.getGuid()).thenReturn("view-guid-123");
        when(mockAtlasClientV2.getEntityByGuid("view-guid-123"))
                .thenReturn(mockEntityWithExtInfo);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mock(AtlasEntity.class));

        // Use reflection to call private method
        Method createViewMethod = QuickStartV2.class.getDeclaredMethod("createView",
                String.class, AtlasEntity.class, List.class, String[].class);
        createViewMethod.setAccessible(true);

        // Execute
        AtlasEntity result = (AtlasEntity) createViewMethod.invoke(
                quickStartV2,
                "test_view",
                mockDatabase,
                Collections.singletonList(mockInputTable),
                new String[] {"Dimension", "JdbcAccess"});

        // Verify
        assertNotNull(result);
        verify(mockAtlasClientV2).createEntity(any(AtlasEntityWithExtInfo.class));
    }

    @Test
    public void testCreateInstance_WithEntity_Success() throws Exception {
        // Setup
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        when(mockAtlasClientV2.createEntity(any(AtlasEntityWithExtInfo.class)))
                .thenReturn(mockEntityMutationResponse);
        when(mockEntityMutationResponse.getEntitiesByOperation(EntityOperation.CREATE))
                .thenReturn(Collections.singletonList(mockEntityHeader));
        when(mockEntityHeader.getGuid()).thenReturn("entity-guid-123");
        when(mockAtlasClientV2.getEntityByGuid("entity-guid-123"))
                .thenReturn(mockEntityWithExtInfo);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockEntity);

        // Use reflection to call private method
        Method createInstanceMethod = QuickStartV2.class.getDeclaredMethod("createInstance", AtlasEntity.class);
        createInstanceMethod.setAccessible(true);

        // Execute
        AtlasEntity result = (AtlasEntity) createInstanceMethod.invoke(quickStartV2, mockEntity);

        // Verify
        assertNotNull(result);
        verify(mockAtlasClientV2).createEntity(any(AtlasEntityWithExtInfo.class));
        verify(mockAtlasClientV2).getEntityByGuid("entity-guid-123");
    }

    @Test
    public void testCreateInstance_WithEntityWithExtInfo_Success() throws Exception {
        // Setup
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        AtlasEntityWithExtInfo mockEntityWithExtInfo = mock(AtlasEntityWithExtInfo.class);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockEntity);

        when(mockAtlasClientV2.createEntity(any(AtlasEntityWithExtInfo.class)))
                .thenReturn(mockEntityMutationResponse);
        when(mockEntityMutationResponse.getEntitiesByOperation(EntityOperation.CREATE))
                .thenReturn(Collections.singletonList(mockEntityHeader));
        when(mockEntityHeader.getGuid()).thenReturn("entity-guid-123");
        when(mockAtlasClientV2.getEntityByGuid("entity-guid-123"))
                .thenReturn(mockEntityWithExtInfo);

        // Use reflection to call private method
        Method createInstanceMethod = QuickStartV2.class.getDeclaredMethod("createInstance", AtlasEntityWithExtInfo.class);
        createInstanceMethod.setAccessible(true);

        // Execute
        AtlasEntity result = (AtlasEntity) createInstanceMethod.invoke(quickStartV2, mockEntityWithExtInfo);

        // Verify
        assertNotNull(result);
        verify(mockAtlasClientV2).createEntity(any(AtlasEntityWithExtInfo.class));
        verify(mockAtlasClientV2).getEntityByGuid("entity-guid-123");
    }

    @Test
    public void testCreateInstance_WithEmptyResponse() throws Exception {
        // Setup
        AtlasEntity mockEntity = mock(AtlasEntity.class);
        when(mockAtlasClientV2.createEntity(any(AtlasEntityWithExtInfo.class)))
                .thenReturn(mockEntityMutationResponse);
        when(mockEntityMutationResponse.getEntitiesByOperation(EntityOperation.CREATE))
                .thenReturn(Collections.emptyList());

        // Use reflection to call private method
        Method createInstanceMethod = QuickStartV2.class.getDeclaredMethod("createInstance", AtlasEntity.class);
        createInstanceMethod.setAccessible(true);

        // Execute
        AtlasEntity result = (AtlasEntity) createInstanceMethod.invoke(quickStartV2, mockEntity);

        // Verify
        assertNull(result);
        verify(mockAtlasClientV2).createEntity(any(AtlasEntityWithExtInfo.class));
    }

    @Test
    public void testToAtlasClassifications_WithClassifications() throws Exception {
        // Use reflection to call private method
        Method toAtlasClassificationsMethod = QuickStartV2.class.getDeclaredMethod("toAtlasClassifications", String[].class);
        toAtlasClassificationsMethod.setAccessible(true);

        // Execute
        List<AtlasClassification> result = (List<AtlasClassification>) toAtlasClassificationsMethod.invoke(quickStartV2,
                (Object) new String[] {"PII", "Fact"});

        // Verify
        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertEquals(result.get(0).getTypeName(), "PII");
        assertEquals(result.get(1).getTypeName(), "Fact");
    }

    @Test
    public void testToAtlasClassifications_WithEmptyArray() throws Exception {
        // Use reflection to call private method
        Method toAtlasClassificationsMethod = QuickStartV2.class.getDeclaredMethod("toAtlasClassifications", String[].class);
        toAtlasClassificationsMethod.setAccessible(true);

        // Execute
        List<AtlasClassification> result = (List<AtlasClassification>) toAtlasClassificationsMethod.invoke(quickStartV2,
                (Object) new String[] {});

        // Verify
        assertNotNull(result);
        assertEquals(result.size(), 0);
    }

    @Test
    public void testVerifyTypesCreated_Success() throws Exception {
        // Setup
        when(mockAtlasClientV2.getAllTypeDefs(any(SearchFilter.class)))
                .thenReturn(mockTypesDef);

        // Use reflection to call private method
        Method verifyTypesCreatedMethod = QuickStartV2.class.getDeclaredMethod("verifyTypesCreated");
        verifyTypesCreatedMethod.setAccessible(true);

        // Execute
        verifyTypesCreatedMethod.invoke(quickStartV2);

        // Verify
        verify(mockAtlasClientV2, atLeastOnce()).getAllTypeDefs(any(SearchFilter.class));
    }

    @Test
    public void testSearch_Success() throws Exception {
        // Setup
        when(mockAtlasClientV2.dslSearchWithParams(anyString(), anyInt(), anyInt()))
                .thenReturn(mockSearchResult);
        when(mockSearchResult.getEntities()).thenReturn(Collections.singletonList(mockEntityHeader));
        when(mockSearchResult.getFullTextResult()).thenReturn(Collections.emptyList());
        when(mockSearchResult.getAttributes()).thenReturn(null);

        // Use reflection to call private method
        Method searchMethod = QuickStartV2.class.getDeclaredMethod("search");
        searchMethod.setAccessible(true);

        // Execute
        searchMethod.invoke(quickStartV2);

        // Verify
        verify(mockAtlasClientV2, atLeastOnce()).dslSearchWithParams(anyString(), anyInt(), anyInt());
    }

    @Test
    public void testSearch_WithException() throws Exception {
        // Setup
        when(mockAtlasClientV2.dslSearchWithParams(anyString(), anyInt(), anyInt()))
                .thenThrow(new RuntimeException("Search failed"));

        // Use reflection to call private method
        Method searchMethod = QuickStartV2.class.getDeclaredMethod("search");
        searchMethod.setAccessible(true);

        // Execute
        searchMethod.invoke(quickStartV2);

        // Verify - should not throw exception, just log error
        verify(mockAtlasClientV2, atLeastOnce()).dslSearchWithParams(anyString(), anyInt(), anyInt());
    }

    @Test
    public void testGetDSLQueries_Success() throws Exception {
        // Use reflection to call private method
        Method getDSLQueriesMethod = QuickStartV2.class.getDeclaredMethod("getDSLQueries");
        getDSLQueriesMethod.setAccessible(true);

        // Execute
        String[] result = (String[]) getDSLQueriesMethod.invoke(quickStartV2);

        // Verify
        assertNotNull(result);
        assertTrue(result.length > 0);
        assertTrue(Arrays.asList(result).contains("from DB"));
        assertTrue(Arrays.asList(result).contains("Table"));
    }

    @Test
    public void testLineage_Success() throws Exception {
        // Setup
        AtlasEntity mockEntity = mock(AtlasEntity.class);

        when(mockAtlasClientV2.getEntityByAttribute(anyString(), anyMap()))
                .thenReturn(mockEntityWithExtInfo);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getGuid()).thenReturn("table-guid-123");

        when(mockAtlasClientV2.getLineageInfo(anyString(), any(LineageDirection.class), anyInt()))
                .thenReturn(mockLineageInfo);
        when(mockLineageInfo.getRelations()).thenReturn(Collections.emptySet());
        when(mockLineageInfo.getGuidEntityMap()).thenReturn(Collections.emptyMap());

        // Use reflection to call private method
        Method lineageMethod = QuickStartV2.class.getDeclaredMethod("lineage");
        lineageMethod.setAccessible(true);

        try {
            // Execute
            lineageMethod.invoke(quickStartV2);
        } catch (InvocationTargetException e) {
            // unwrap actual exception for debugging
            throw (Exception) e.getCause();
        }

        // Verify interactions
        verify(mockAtlasClientV2).getEntityByAttribute(anyString(), anyMap());
        verify(mockAtlasClientV2).getLineageInfo(anyString(), any(LineageDirection.class), anyInt());
    }

    @Test
    public void testLineage_WithException() throws Exception {
        // Setup
        when(mockAtlasClientV2.getEntityByAttribute(anyString(), anyMap()))
                .thenThrow(new RuntimeException("Lineage failed"));

        // Use reflection to call private method
        Method lineageMethod = QuickStartV2.class.getDeclaredMethod("lineage");
        lineageMethod.setAccessible(true);

        // Execute & Verify
        Exception exception = expectThrows(Exception.class, () -> {
            lineageMethod.invoke(quickStartV2);
        });

        assertTrue(exception.getCause() instanceof RuntimeException);
        assertEquals(exception.getCause().getMessage(), "Lineage failed");
    }

    @Test
    public void testGetTableId_Success() throws Exception {
        // Setup
        AtlasEntity mockTableEntity = mock(AtlasEntity.class);
        when(mockTableEntity.getGuid()).thenReturn("table-guid-123");
        when(mockAtlasClientV2.getEntityByAttribute(anyString(), anyMap()))
                .thenReturn(mockEntityWithExtInfo);
        when(mockEntityWithExtInfo.getEntity()).thenReturn(mockTableEntity);

        // Use reflection to call private method
        Method getTableIdMethod = QuickStartV2.class.getDeclaredMethod("getTableId", String.class);
        getTableIdMethod.setAccessible(true);

        // Execute
        String result = (String) getTableIdMethod.invoke(quickStartV2, "test_table");

        // Verify
        assertEquals(result, "table-guid-123");
        verify(mockAtlasClientV2).getEntityByAttribute(anyString(), anyMap());
    }

    @Test
    public void testCloseConnection_Success() throws Exception {
        // Use reflection to call private method
        Method closeConnectionMethod = QuickStartV2.class.getDeclaredMethod("closeConnection");
        closeConnectionMethod.setAccessible(true);

        // Execute
        closeConnectionMethod.invoke(quickStartV2);

        // Verify
        verify(mockAtlasClientV2).close();
    }

    @Test
    public void testConstructor_WithUrls() throws Exception {
        // Test constructor with URLs only
        QuickStartV2 instance = new QuickStartV2(new String[] {"http://localhost:21000"});
        assertNotNull(instance);
    }

    @Test
    public void testConstructor_WithUrlsAndAuth() throws Exception {
        // Test constructor with URLs and authentication
        QuickStartV2 instance = new QuickStartV2(new String[] {"http://localhost:21000"},
                new String[] {"admin", "admin"});
        assertNotNull(instance);
    }

    @Test
    public void testMain_WithArgs() throws Exception {
        // Use reflection to call static method
        Method mainMethod = QuickStartV2.class.getDeclaredMethod("main", String[].class);
        mainMethod.setAccessible(true);

        assertNotNull(mainMethod);
    }

    @Test
    public void testRunQuickstart_WithArgs() throws Exception {
        // Use reflection to call static method
        Method runQuickstartMethod = QuickStartV2.class.getDeclaredMethod("runQuickstart",
                String[].class, String[].class);
        runQuickstartMethod.setAccessible(true);

        // This will fail due to missing Atlas server, but we can test the method exists
        assertNotNull(runQuickstartMethod);
    }

    @Test
    public void testGetServerUrl_WithArgs() throws Exception {
        // Test getServerUrl method with arguments
        String[] args = {"http://localhost:21000,http://localhost:21001"};

        // Use reflection to call static method
        Method getServerUrlMethod = QuickStartV2.class.getDeclaredMethod("getServerUrl", String[].class);
        getServerUrlMethod.setAccessible(true);

        String[] result = (String[]) getServerUrlMethod.invoke(null, (Object) args);

        // Verify
        assertNotNull(result);
        assertEquals(result.length, 2);
        assertEquals(result[0], "http://localhost:21000");
        assertEquals(result[1], "http://localhost:21001");
    }

    @Test
    public void testConstants() {
        assertEquals(QuickStartV2.SALES_DB, "Sales");
        assertEquals(QuickStartV2.REPORTING_DB, "Reporting");
        assertEquals(QuickStartV2.LOGGING_DB, "Logging");
        assertEquals(QuickStartV2.SALES_FACT_TABLE, "sales_fact");
        assertEquals(QuickStartV2.PRODUCT_DIM_TABLE, "product_dim");
        assertEquals(QuickStartV2.CUSTOMER_DIM_TABLE, "customer_dim");
        assertEquals(QuickStartV2.TIME_DIM_TABLE, "time_dim");
        assertEquals(QuickStartV2.DATABASE_TYPE, "DB");
        assertEquals(QuickStartV2.TABLE_TYPE, "Table");
        assertEquals(QuickStartV2.COLUMN_TYPE, "Column");
        assertEquals(QuickStartV2.VIEW_TYPE, "View");
        assertEquals(QuickStartV2.LOAD_PROCESS_TYPE, "LoadProcess");
        assertEquals(QuickStartV2.LOAD_PROCESS_EXECUTION_TYPE, "LoadProcessExecution");
        assertEquals(QuickStartV2.STORAGE_DESC_TYPE, "StorageDesc");
        assertEquals(QuickStartV2.DIMENSION_CLASSIFICATION, "Dimension");
        assertEquals(QuickStartV2.FACT_CLASSIFICATION, "Fact");
        assertEquals(QuickStartV2.PII_CLASSIFICATION, "PII");
        assertEquals(QuickStartV2.METRIC_CLASSIFICATION, "Metric");
        assertEquals(QuickStartV2.ETL_CLASSIFICATION, "ETL");
        assertEquals(QuickStartV2.JDBC_CLASSIFICATION, "JdbcAccess");
        assertEquals(QuickStartV2.LOGDATA_CLASSIFICATION, "Log Data");
        assertEquals(QuickStartV2.VERSION_1, "1.0");
        assertEquals(QuickStartV2.MANAGED_TABLE, "Managed");
        assertEquals(QuickStartV2.EXTERNAL_TABLE, "External");
        assertEquals(QuickStartV2.CLUSTER_SUFFIX, "@cl1");
    }
}
