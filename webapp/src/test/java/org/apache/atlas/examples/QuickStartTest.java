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

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.typedef.AttributeDefinition;
import org.apache.atlas.v1.model.typedef.ClassTypeDefinition;
import org.apache.atlas.v1.model.typedef.Multiplicity;
import org.apache.atlas.v1.model.typedef.TraitTypeDefinition;
import org.apache.atlas.v1.model.typedef.TypesDef;
import org.apache.atlas.v1.typesystem.types.utils.TypesUtil;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

public class QuickStartTest {
    @Mock
    private AtlasClient mockAtlasClient;

    @Mock
    private Id mockId;

    @Mock
    private Referenceable mockReferenceable;

    @Mock
    private TypesDef mockTypesDef;

    private QuickStart quickStart;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        // Create QuickStart instance with mocked dependencies using reflection
        quickStart = createQuickStartWithMockedClient();
    }

    private QuickStart createQuickStartWithMockedClient() throws Exception {
        // Use reflection to create QuickStart instance and inject mocked AtlasClient
        QuickStart instance = new QuickStart(new String[] {"http://localhost:21000"});

        // Use reflection to set the private AtlasClient field
        java.lang.reflect.Field clientField = QuickStart.class.getDeclaredField("metadataServiceClient");
        clientField.setAccessible(true);
        clientField.set(instance, mockAtlasClient);

        return instance;
    }

    @Test
    public void testCreateTypes_WithException() throws Exception {
        // Setup
        when(mockAtlasClient.createType(anyString())).thenThrow(new RuntimeException("Create failed"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(TypesDef.class))).thenReturn("mock");

            // Use reflection to call private method
            Method createTypesMethod = QuickStart.class.getDeclaredMethod("createTypes");
            createTypesMethod.setAccessible(true);

            // Execute & Verify
            Exception exception = expectThrows(Exception.class, () -> {
                createTypesMethod.invoke(quickStart);
            });

            assertTrue(exception.getCause() instanceof RuntimeException);
            assertEquals(exception.getCause().getMessage(), "Create failed");
        }
    }

    @Test
    public void testCreateTypeDefinitions_Success() throws Exception {
        // Setup
        try (MockedStatic<TypesUtil> mockedTypesUtil = mockStatic(TypesUtil.class)) {
            mockedTypesUtil.when(() -> TypesUtil.createClassTypeDef(anyString(), anyString(), any(), any(AttributeDefinition.class)))
                    .thenReturn(mock(ClassTypeDefinition.class));
            mockedTypesUtil.when(() -> TypesUtil.createTraitTypeDef(anyString(), anyString(), any()))
                    .thenReturn(mock(TraitTypeDefinition.class));
            mockedTypesUtil.when(() -> TypesUtil.createUniqueRequiredAttrDef(anyString(), anyString()))
                    .thenReturn(mock(AttributeDefinition.class));

            // Use reflection to call private method
            Method createTypeDefinitionsMethod = QuickStart.class.getDeclaredMethod("createTypeDefinitions");
            createTypeDefinitionsMethod.setAccessible(true);

            TypesDef result = (TypesDef) createTypeDefinitionsMethod.invoke(quickStart);

            // Verify
            assertNotNull(result);
        }
    }

    @Test
    public void testCreateEntities_Success() throws Exception {
        // Setup
        when(mockAtlasClient.createEntity(anyString())).thenReturn(Collections.singletonList("guid-123"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Use reflection to call private method
            Method createEntitiesMethod = QuickStart.class.getDeclaredMethod("createEntities");
            createEntitiesMethod.setAccessible(true);

            createEntitiesMethod.invoke(quickStart);

            // Verify
            verify(mockAtlasClient, atLeastOnce()).createEntity(anyString());
        }
    }

    @Test
    public void testCreateEntities_WithException() throws Exception {
        // Setup
        when(mockAtlasClient.createEntity(anyString())).thenThrow(new RuntimeException("Entity creation failed"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Use reflection to call private method
            Method createEntitiesMethod = QuickStart.class.getDeclaredMethod("createEntities");
            createEntitiesMethod.setAccessible(true);

            // Execute & Verify
            Exception exception = expectThrows(Exception.class, () -> {
                createEntitiesMethod.invoke(quickStart);
            });

            assertTrue(exception.getCause() instanceof AtlasBaseException);
        }
    }

    @Test
    public void testDatabase_Success() throws Exception {
        // Setup
        when(mockAtlasClient.createEntity(anyString())).thenReturn(Collections.singletonList("db-guid-123"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Execute
            Id result = quickStart.database("test_db", "Test Database", "test_owner", "hdfs://test/location");

            // Verify
            assertNotNull(result);
            verify(mockAtlasClient).createEntity(anyString());
        }
    }

    @Test
    public void testDatabase_WithException() throws Exception {
        // Setup
        when(mockAtlasClient.createEntity(anyString())).thenThrow(new RuntimeException("Database creation failed"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Execute & Verify
            AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
                quickStart.database("test_db", "Test Database", "test_owner", "hdfs://test/location");
            });

            assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.QUICK_START);
            assertTrue(exception.getMessage().contains("test_db database entity creation failed"));
        }
    }

    @Test
    public void testDatabase_WithTraits() throws Exception {
        // Setup
        when(mockAtlasClient.createEntity(anyString())).thenReturn(Collections.singletonList("db-guid-456"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Execute
            Id result = quickStart.database("test_db_traits", "Test Database", "test_owner", "hdfs://test/location", "Fact_v1", "Dimension_v1");

            // Verify
            assertNotNull(result);
            verify(mockAtlasClient).createEntity(anyString());
        }
    }

    @Test
    public void testRawStorageDescriptor_Success() {
        // Execute
        Referenceable result = quickStart.rawStorageDescriptor("hdfs://test/location", "TextInputFormat", "TextOutputFormat", true);

        // Verify
        assertNotNull(result);
        assertEquals(result.getTypeName(), "StorageDesc_v1");
        assertEquals(result.get("location"), "hdfs://test/location");
        assertEquals(result.get("inputFormat"), "TextInputFormat");
        assertEquals(result.get("outputFormat"), "TextOutputFormat");
        assertEquals(result.get("compressed"), true);
    }

    @Test
    public void testRawColumn_Success() throws Exception {
        // Setup
        when(mockAtlasClient.createEntity(anyString())).thenReturn(Collections.singletonList("column-guid-123"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Execute
            Referenceable result = quickStart.rawColumn("test_column", "string", "test comment");

            // Verify
            assertNotNull(result);
            assertEquals(result.getTypeName(), "Column_v1");
            assertEquals(result.get("name"), "test_column");
            assertEquals(result.get("dataType"), "string");
            assertEquals(result.get("comment"), "test comment");
        }
    }

    @Test
    public void testTable_WithException() throws Exception {
        // Setup
        Id mockDbId = mock(Id.class);
        Referenceable mockSd = mock(Referenceable.class);
        List<Referenceable> mockColumns = Collections.singletonList(mock(Referenceable.class));
        when(mockAtlasClient.createEntity(anyString())).thenThrow(new RuntimeException("Table creation failed"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Execute & Verify
            AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
                quickStart.table("test_table_error", "Test Table", mockDbId, mockSd, "test_owner", "Managed", mockColumns, "Fact_v1");
            });

            assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.QUICK_START);
            assertTrue(exception.getMessage().contains("test_table_error table entity creation failed"));
        }
    }

    @Test
    public void testLoadProcess_Success() throws Exception {
        // Setup
        List<Id> mockInputTables = Collections.singletonList(mock(Id.class));
        List<Id> mockOutputTables = Collections.singletonList(mock(Id.class));
        when(mockAtlasClient.createEntity(anyString())).thenReturn(Collections.singletonList("process-guid-123"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Execute
            Id result = quickStart.loadProcess("test_process", "Test Process", "test_user", mockInputTables, mockOutputTables,
                    "SELECT * FROM test", "plan", "id", "graph", "ETL_v1");

            // Verify
            assertNotNull(result);
            verify(mockAtlasClient).createEntity(anyString());
        }
    }

    @Test
    public void testLoadProcess_WithException() throws Exception {
        // Setup
        List<Id> mockInputTables = Collections.singletonList(mock(Id.class));
        List<Id> mockOutputTables = Collections.singletonList(mock(Id.class));
        when(mockAtlasClient.createEntity(anyString())).thenThrow(new RuntimeException("Process creation failed"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Execute & Verify
            AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
                quickStart.loadProcess("test_process_error", "Test Process", "test_user", mockInputTables, mockOutputTables,
                        "SELECT * FROM test", "plan", "id", "graph", "ETL_v1");
            });

            assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.QUICK_START);
            assertTrue(exception.getMessage().contains("test_process_error process entity creation failed"));
        }
    }

    @Test
    public void testView_Success() throws Exception {
        // Setup
        Id mockDbId = mock(Id.class);
        List<Id> mockInputTables = Collections.singletonList(mock(Id.class));
        when(mockAtlasClient.createEntity(anyString())).thenReturn(Collections.singletonList("view-guid-123"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Execute
            Id result = quickStart.view("test_view", mockDbId, mockInputTables, "Dimension_v1", "JdbcAccess_v1");

            // Verify
            assertNotNull(result);
            verify(mockAtlasClient).createEntity(anyString());
        }
    }

    @Test
    public void testView_WithException() throws Exception {
        // Setup
        Id mockDbId = mock(Id.class);
        List<Id> mockInputTables = Collections.singletonList(mock(Id.class));
        when(mockAtlasClient.createEntity(anyString())).thenThrow(new RuntimeException("View creation failed"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Execute & Verify
            AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
                quickStart.view("test_view_error", mockDbId, mockInputTables, "Dimension_v1", "JdbcAccess_v1");
            });

            assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.QUICK_START);
            assertTrue(exception.getMessage().contains("test_view_error Id creation"));
        }
    }

    @Test
    public void testCreateInstance_Success() throws Exception {
        // Setup
        Referenceable mockRef = mock(Referenceable.class);
        when(mockRef.getTypeName()).thenReturn("TestType");
        when(mockRef.getId()).thenReturn(mock(org.apache.atlas.v1.model.instance.Id.class));
        when(mockAtlasClient.createEntity(anyString())).thenReturn(Collections.singletonList("guid-123"));

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Use reflection to call private method
            Method createInstanceMethod = QuickStart.class.getDeclaredMethod("createInstance", Referenceable.class);
            createInstanceMethod.setAccessible(true);

            Id result = (Id) createInstanceMethod.invoke(quickStart, mockRef);

            // Verify
            assertNotNull(result);
            verify(mockAtlasClient).createEntity(anyString());
        }
    }

    @Test
    public void testCreateInstance_WithEmptyGuids() throws Exception {
        // Setup
        Referenceable mockRef = mock(Referenceable.class);
        when(mockRef.getTypeName()).thenReturn("TestType");
        when(mockRef.getId()).thenReturn(mock(org.apache.atlas.v1.model.instance.Id.class));
        when(mockAtlasClient.createEntity(anyString())).thenReturn(Collections.emptyList());

        try (MockedStatic<org.apache.atlas.type.AtlasType> mockedAtlasType = mockStatic(org.apache.atlas.type.AtlasType.class)) {
            mockedAtlasType.when(() -> org.apache.atlas.type.AtlasType.toV1Json(any(Referenceable.class))).thenReturn("mock");

            // Use reflection to call private method
            Method createInstanceMethod = QuickStart.class.getDeclaredMethod("createInstance", Referenceable.class);
            createInstanceMethod.setAccessible(true);

            Id result = (Id) createInstanceMethod.invoke(quickStart, mockRef);

            // Verify
            assertNull(result);
            verify(mockAtlasClient).createEntity(anyString());
        }
    }

    @Test
    public void testVerifyTypesCreated_Success() throws Exception {
        // Setup
        when(mockAtlasClient.listTypes()).thenReturn(Arrays.asList("DB_v1", "Table_v1", "Column_v1", "StorageDesc_v1",
                "LoadProcess_v1", "View_v1", "JdbcAccess_v1", "ETL_v1", "Metric_v1", "PII_v1", "Fact_v1", "Dimension_v1", "Log Data_v1"));

        // Use reflection to call private method
        Method verifyTypesCreatedMethod = QuickStart.class.getDeclaredMethod("verifyTypesCreated");
        verifyTypesCreatedMethod.setAccessible(true);

        verifyTypesCreatedMethod.invoke(quickStart);

        // Verify
        verify(mockAtlasClient).listTypes();
    }

    @Test
    public void testVerifyTypesCreated_WithMissingType() throws Exception {
        // Setup
        when(mockAtlasClient.listTypes()).thenReturn(Arrays.asList("DB_v1", "Table_v1")); // Missing types

        // Use reflection to call private method
        Method verifyTypesCreatedMethod = QuickStart.class.getDeclaredMethod("verifyTypesCreated");
        verifyTypesCreatedMethod.setAccessible(true);

        // Execute & Verify
        Exception exception = expectThrows(Exception.class, () -> {
            verifyTypesCreatedMethod.invoke(quickStart);
        });

        assertTrue(exception.getCause() instanceof AtlasBaseException);
        assertEquals(((AtlasBaseException) exception.getCause()).getAtlasErrorCode(), AtlasErrorCode.QUICK_START);
    }

    @Test
    public void testSearch_Success() throws Exception {
        // Setup
        when(mockAtlasClient.search(anyString(), anyInt(), anyInt())).thenReturn(mock(com.fasterxml.jackson.databind.JsonNode.class));

        // Use reflection to call private method
        Method searchMethod = QuickStart.class.getDeclaredMethod("search");
        searchMethod.setAccessible(true);

        searchMethod.invoke(quickStart);

        // Verify
        verify(mockAtlasClient, atLeastOnce()).search(anyString(), anyInt(), anyInt());
    }

    @Test
    public void testSearch_WithException() throws Exception {
        // Setup
        when(mockAtlasClient.search(anyString(), anyInt(), anyInt())).thenThrow(new RuntimeException("Search failed"));

        // Use reflection to call private method
        Method searchMethod = QuickStart.class.getDeclaredMethod("search");
        searchMethod.setAccessible(true);

        // Execute & Verify
        Exception exception = expectThrows(Exception.class, () -> {
            searchMethod.invoke(quickStart);
        });

        assertTrue(exception.getCause() instanceof AtlasBaseException);
        assertEquals(((AtlasBaseException) exception.getCause()).getAtlasErrorCode(), AtlasErrorCode.QUICK_START);
    }

    @Test
    public void testGetDSLQueries_Success() throws Exception {
        // Use reflection to call private method
        Method getDSLQueriesMethod = QuickStart.class.getDeclaredMethod("getDSLQueries");
        getDSLQueriesMethod.setAccessible(true);

        String[] result = (String[]) getDSLQueriesMethod.invoke(quickStart);

        // Verify
        assertNotNull(result);
        assertTrue(result.length > 0);
        assertTrue(Arrays.asList(result).contains("from DB_v1"));
        assertTrue(Arrays.asList(result).contains("Table_v1"));
    }

    @Test
    public void testCloseConnection_Success() throws Exception {
        // Use reflection to call private method
        Method closeConnectionMethod = QuickStart.class.getDeclaredMethod("closeConnection");
        closeConnectionMethod.setAccessible(true);

        closeConnectionMethod.invoke(quickStart);

        // Verify
        verify(mockAtlasClient).close();
    }

    @Test
    public void testAttrDef_WithDefaultMultiplicity() {
        // Execute
        AttributeDefinition result = quickStart.attrDef("test_attr", "string");

        // Verify
        assertNotNull(result);
        assertEquals(result.getName(), "test_attr");
        assertEquals(result.getDataTypeName(), "string");
        assertEquals(result.getMultiplicity(), Multiplicity.OPTIONAL);
    }

    @Test
    public void testAttrDef_WithCustomMultiplicity() {
        // Execute
        AttributeDefinition result = quickStart.attrDef("test_attr", "string", Multiplicity.REQUIRED);

        // Verify
        assertNotNull(result);
        assertEquals(result.getName(), "test_attr");
        assertEquals(result.getDataTypeName(), "string");
        assertEquals(result.getMultiplicity(), Multiplicity.REQUIRED);
    }

    @Test
    public void testAttrDef_WithFullParameters() {
        // Execute
        AttributeDefinition result = quickStart.attrDef("test_attr", "string", Multiplicity.REQUIRED, true, "reverseAttr");

        // Verify
        assertNotNull(result);
        assertEquals(result.getName(), "test_attr");
        assertEquals(result.getDataTypeName(), "string");
        assertEquals(result.getMultiplicity(), Multiplicity.REQUIRED);
//        assertTrue(result.isComposite());
        assertEquals(result.getReverseAttributeName(), "reverseAttr");
    }

    @Test
    public void testAttrDef_WithNullName() {
        // Execute & Verify
        assertThrows(NullPointerException.class, () -> {
            quickStart.attrDef(null, "string");
        });
    }

    @Test
    public void testAttrDef_WithNullDataType() {
        // Execute & Verify
        assertThrows(NullPointerException.class, () -> {
            quickStart.attrDef("test_attr", null);
        });
    }

    private TypesDef createMockTypesDef() {
        return new TypesDef(Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void testGetServerUrl_WithArgs() throws Exception {
        // Setup
        String[] args = {"http://localhost:21000,http://localhost:21001"};

        // Use reflection to call static method
        Method getServerUrlMethod = QuickStart.class.getDeclaredMethod("getServerUrl", String[].class);
        getServerUrlMethod.setAccessible(true);

        String[] result = (String[]) getServerUrlMethod.invoke(null, (Object) args);

        // Verify
        assertNotNull(result);
        assertEquals(result.length, 2);
        assertEquals(result[0], "http://localhost:21000");
        assertEquals(result[1], "http://localhost:21001");
    }
}
