package org.apache.atlas.impala.hook.events;

/** Licensed to the Apache Software Foundation (ASF) under one
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
 **/

import org.apache.atlas.impala.hook.AtlasImpalaHookContext;
import org.apache.atlas.impala.model.ImpalaDataType;
import org.apache.atlas.impala.model.ImpalaNode;
import org.apache.atlas.impala.model.ImpalaOperationType;
import org.apache.atlas.impala.model.ImpalaQuery;
import org.apache.atlas.impala.model.ImpalaVertexType;
import org.apache.atlas.impala.model.LineageVertex;
import org.apache.atlas.impala.model.LineageVertexMetadata;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.atlas.impala.hook.events.BaseImpalaEvent.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

public class BaseImpalaEventTest {
    private static final Logger LOG = LoggerFactory.getLogger(BaseImpalaEventTest.class);

    @Mock
    private AtlasImpalaHookContext mockContext;

    @Mock
    private ImpalaQuery mockQuery;

    private static final String CLUSTER_NAME = "test_cluster";
    private static final String DB_NAME = "test_database";
    private static final String TABLE_NAME = "test_table";
    private static final String COLUMN_NAME = "test_column";
    private static final String USER_NAME = "test_user";
    private static final String HOST_NAME = "test_host";
    private static final String QUERY_TEXT = "SELECT * FROM test_table";
    private static final String QUERY_ID = "test_query_123";
    private static final long TIMESTAMP = 1554750072L;
    private static final long END_TIME = 1554750554L;

    private static class TestBaseImpalaEvent extends BaseImpalaEvent {
        public TestBaseImpalaEvent(AtlasImpalaHookContext context) {
            super(context);
        }

        @Override
        public List<HookNotification> getNotificationMessages() throws Exception {
            return Collections.emptyList();
        }
    }

    private TestBaseImpalaEvent baseImpalaEvent;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(mockContext.getUserName()).thenReturn(USER_NAME);
        when(mockContext.getHostName()).thenReturn(HOST_NAME);
        when(mockContext.getMetadataNamespace()).thenReturn(CLUSTER_NAME);
        when(mockContext.getQueryStr()).thenReturn(QUERY_TEXT);
        when(mockContext.getImpalaOperationType()).thenReturn(ImpalaOperationType.QUERY);
        when(mockContext.getLineageQuery()).thenReturn(mockQuery);

        when(mockQuery.getQueryId()).thenReturn(QUERY_ID);
        when(mockQuery.getTimestamp()).thenReturn(TIMESTAMP);
        when(mockQuery.getEndTime()).thenReturn(END_TIME);

        setupQualifiedNameMocks();

        baseImpalaEvent = new TestBaseImpalaEvent(mockContext);
    }

    private void setupQualifiedNameMocks() {
        when(mockContext.getQualifiedNameForDb(anyString())).thenAnswer(invocation -> {
            String dbName = invocation.getArgument(0);
            return dbName.toLowerCase()  +"@"+  CLUSTER_NAME;
        });

        when(mockContext.getQualifiedNameForTable(anyString())).thenAnswer(invocation -> {
            String tableQualifiedName = invocation.getArgument(0);
            return tableQualifiedName.toLowerCase()  +"@"+  CLUSTER_NAME;
        });

        when(mockContext.getQualifiedNameForColumn(any(LineageVertex.class))).thenAnswer(invocation -> {
            LineageVertex vertex = invocation.getArgument(0);
            return vertex.getVertexId().toLowerCase()  +"@"+  CLUSTER_NAME;
        });

        when(mockContext.getDatabaseNameFromTable(anyString())).thenAnswer(invocation -> {
            String tableName = invocation.getArgument(0);
            if (tableName.contains(".")) {
                return tableName.split("\\.")[0];
            }
            return DB_NAME;
        });

        when(mockContext.getColumnNameOnly(anyString())).thenAnswer(invocation -> {
            String columnName = invocation.getArgument(0);
            if (columnName.contains(".")) {
                String[] parts = columnName.split("\\.");
                return parts[parts.length - 1];
            }
            return columnName;
        });

        when(mockContext.getTableNameFromColumn(anyString())).thenAnswer(invocation -> {
            String columnName = invocation.getArgument(0);
            if (columnName.contains(".") && columnName.split("\\.").length >= 2) {
                String[] parts = columnName.split("\\.");
                return parts[0]  +"."+  parts[1];
            }
            return DB_NAME  +"."+  TABLE_NAME;
        });
    }

    @Test
    public void testGetUserName() {
        assertEquals(baseImpalaEvent.getUserName(), USER_NAME);
    }

    @Test
    public void testGetQualifiedNameWithImpalaNode() throws Exception {
        // Create a test ImpalaNode with database vertex
        LineageVertex dbVertex = createDatabaseVertex(DB_NAME);
        ImpalaNode dbNode = new ImpalaNode(dbVertex);

        String qualifiedName = baseImpalaEvent.getQualifiedName(dbNode);

        assertNotNull(qualifiedName);
        assertEquals(qualifiedName, DB_NAME.toLowerCase()  +"@"+  CLUSTER_NAME);
    }

    @Test
    public void testGetQualifiedNameWithLineageVertex() throws Exception {
        // Test database vertex
        LineageVertex dbVertex = createDatabaseVertex(DB_NAME);
        String dbQualifiedName = baseImpalaEvent.getQualifiedName(dbVertex);
        assertEquals(dbQualifiedName, DB_NAME.toLowerCase()  +"@"+  CLUSTER_NAME);

        // Test table vertex
        LineageVertex tableVertex = createTableVertex(DB_NAME+"."+TABLE_NAME);
        String tableQualifiedName = baseImpalaEvent.getQualifiedName(tableVertex);
        assertEquals(tableQualifiedName, DB_NAME+"."+TABLE_NAME.toLowerCase()  +"@"+  CLUSTER_NAME);

        // Test column vertex
        LineageVertex columnVertex = createColumnVertex(COLUMN_NAME, TABLE_NAME);
        String columnQualifiedName = baseImpalaEvent.getQualifiedName(columnVertex);
        System.out.println("columnQualifiedName : "+columnQualifiedName);
        assertEquals(columnQualifiedName, DB_NAME+"."+TABLE_NAME+"."+COLUMN_NAME.toLowerCase()  +"@"+  CLUSTER_NAME);
    }

    @Test
    public void testGetQualifiedNameWithNullVertex() {
        try {
            baseImpalaEvent.getQualifiedName((LineageVertex) null);
            fail("Expected IllegalArgumentException for null vertex");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "node is null");
        }
    }

    @Test
    public void testGetQualifiedNameWithNullVertexType() {
        LineageVertex vertex = new LineageVertex();
        vertex.setVertexId("test");
        vertex.setVertexType(null);

        String qualifiedName = baseImpalaEvent.getQualifiedName(vertex);
        assertNull(qualifiedName);
    }

    @Test
    public void testGetQualifiedNameWithNullVertexId() {
        LineageVertex vertex = new LineageVertex();
        vertex.setVertexType(ImpalaVertexType.DATABASE);
        vertex.setVertexId(null);

        String qualifiedName = baseImpalaEvent.getQualifiedName(vertex);
        assertNull(qualifiedName);
    }

    @Test
    public void testGetTableNameFromVertex() {
        // Test with column vertex that has metadata
        LineageVertex columnVertex = createColumnVertex(COLUMN_NAME, TABLE_NAME);
        LineageVertexMetadata metadata = new LineageVertexMetadata();
        metadata.setTableName(TABLE_NAME);
        columnVertex.setMetadata(metadata);

        String tableName = baseImpalaEvent.getTableNameFromVertex(columnVertex);
        assertEquals(tableName, TABLE_NAME);

        // Test with non-column vertex
        LineageVertex dbVertex = createDatabaseVertex(DB_NAME);
        String tableNameFromDb = baseImpalaEvent.getTableNameFromVertex(dbVertex);
        assertEquals(tableNameFromDb, DB_NAME  +"."+  TABLE_NAME);
    }

    @Test
    public void testGetTableNameFromColumn() {
        String columnName = DB_NAME  +"."+  TABLE_NAME  +"."+  COLUMN_NAME;
        String tableName = baseImpalaEvent.getTableNameFromColumn(columnName);
        assertEquals(tableName, DB_NAME  +"."+  TABLE_NAME);
    }

    @Test
    public void testToDbEntityWithString() throws Exception {

        when(mockContext.getEntity(anyString())).thenReturn(null);

        AtlasEntity dbEntity = baseImpalaEvent.toDbEntity(DB_NAME);

        assertNotNull(dbEntity);
        assertEquals(dbEntity.getTypeName(), HIVE_TYPE_DB);
        assertEquals(dbEntity.getAttribute(ATTRIBUTE_NAME), DB_NAME.toLowerCase());
        assertEquals(dbEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME), DB_NAME.toLowerCase()  +"@"+  CLUSTER_NAME);
        assertEquals(dbEntity.getAttribute(ATTRIBUTE_CLUSTER_NAME), CLUSTER_NAME);
        assertNull(dbEntity.getGuid());
    }


    @Test
    public void testGetColumnEntities() throws Exception {

        LineageVertex tableVertex = createTableVertex(DB_NAME+"."+TABLE_NAME);
        tableVertex.setId(1L);
        tableVertex.setCreateTime(TIMESTAMP);
        ImpalaNode tableNode = new ImpalaNode(tableVertex);
        tableNode.addChild(tableVertex);
        AtlasEntity entity = new AtlasEntity(HIVE_TYPE_COLUMN);
        entity.setGuid("test-guid");
        entity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, "test.qualified.name");

        AtlasObjectId objectId = BaseImpalaEvent.getObjectId(entity);

        when(mockContext.getEntity(anyString())).thenReturn(null);
        when(mockContext.getUserName()).thenReturn(USER_NAME);

        List<AtlasEntity> entityList = baseImpalaEvent.getColumnEntities(objectId ,tableNode);

        assertNotNull(entityList.get(0));
        assertEquals(entityList.get(0).getTypeName(), HIVE_TYPE_COLUMN);
        assertEquals(entityList.get(0).getAttribute(ATTRIBUTE_TABLE),objectId);
    }

    @Test
    public void testToTableEntity() throws Exception {

        LineageVertex tableVertex = createTableVertex(DB_NAME+"."+TABLE_NAME);
        tableVertex.setId(1L);
        tableVertex.setCreateTime(TIMESTAMP);
        ImpalaNode tableNode = new ImpalaNode(tableVertex);
        tableNode.addChild(tableVertex);
        AtlasEntity entity = new AtlasEntity(HIVE_TYPE_TABLE);
        entity.setGuid("test-guid");

        AtlasObjectId objectId = BaseImpalaEvent.getObjectId(entity);

        when(mockContext.getEntity(anyString())).thenReturn(null);
        when(mockContext.getUserName()).thenReturn(USER_NAME);

        AtlasEntity atlasEntity = baseImpalaEvent.toTableEntity(objectId ,tableNode,null);


        assertEquals(atlasEntity.getTypeName(),HIVE_TYPE_TABLE);
        assertEquals(atlasEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME),DB_NAME+"."+TABLE_NAME+"@"+CLUSTER_NAME);
    }



    @Test
    public void testToDbEntityWithCachedEntity() throws Exception {
        // Setup entity cache to return existing entity
        AtlasEntity cachedEntity = new AtlasEntity(HIVE_TYPE_DB);
        cachedEntity.setAttribute(ATTRIBUTE_NAME, DB_NAME);
        when(mockContext.getEntity(anyString())).thenReturn(cachedEntity);

        AtlasEntity dbEntity = baseImpalaEvent.toDbEntity(DB_NAME);

        assertNotNull(dbEntity);
        assertEquals(dbEntity, cachedEntity);
    }


    @Test
    public void testToTableEntityWithNullNode() {
        try {
            baseImpalaEvent.toTableEntity((ImpalaNode) null, (AtlasEntitiesWithExtInfo) null);
            fail("Expected IllegalArgumentException for null table node");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains("table is null"));
        }
    }

    @Test
    public void testGetObjectId() {
        AtlasEntity entity = new AtlasEntity(HIVE_TYPE_TABLE);
        entity.setGuid("test-guid");
        entity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, "test.qualified.name");

        AtlasObjectId objectId = BaseImpalaEvent.getObjectId(entity);

        assertNotNull(objectId);
        assertEquals(objectId.getGuid(), "test-guid");
        assertEquals(objectId.getTypeName(), HIVE_TYPE_TABLE);
        assertEquals(objectId.getUniqueAttributes().get(ATTRIBUTE_QUALIFIED_NAME), "test.qualified.name");
    }

    @Test
    public void testGetObjectIds() {
        List<AtlasEntity> entities = new ArrayList<>();

        AtlasEntity entity1 = new AtlasEntity(HIVE_TYPE_TABLE);
        entity1.setGuid("test-guid-1");
        entity1.setAttribute(ATTRIBUTE_QUALIFIED_NAME, "test.table1");
        entities.add(entity1);

        AtlasEntity entity2 = new AtlasEntity(HIVE_TYPE_COLUMN);
        entity2.setGuid("test-guid-2");
        entity2.setAttribute(ATTRIBUTE_QUALIFIED_NAME, "test.column1");
        entities.add(entity2);

        List<AtlasObjectId> objectIds = BaseImpalaEvent.getObjectIds(entities);

        assertNotNull(objectIds);
        assertEquals(objectIds.size(), 2);
        assertEquals(objectIds.get(0).getGuid(), "test-guid-1");
        assertEquals(objectIds.get(1).getGuid(), "test-guid-2");
    }

    @Test
    public void testGetObjectIdsWithEmptyList() {
        List<AtlasObjectId> objectIds = BaseImpalaEvent.getObjectIds(Collections.emptyList());
        assertNotNull(objectIds);
        assertTrue(objectIds.isEmpty());
    }

    @Test
    public void testGetTableCreateTimeWithNode() {
        LineageVertex tableVertex = createTableVertex(DB_NAME+"."+TABLE_NAME);
        tableVertex.setCreateTime(TIMESTAMP);
        ImpalaNode tableNode = new ImpalaNode(tableVertex);

        long createTime = BaseImpalaEvent.getTableCreateTime(tableNode);
        assertEquals(createTime, TIMESTAMP * MILLIS_CONVERT_FACTOR);
    }

    @Test
    public void testGetTableCreateTimeWithVertex() {
        LineageVertex tableVertex = createTableVertex(DB_NAME+"."+TABLE_NAME);
        tableVertex.setCreateTime(TIMESTAMP);

        long createTime = BaseImpalaEvent.getTableCreateTime(tableVertex);
        assertEquals(createTime, TIMESTAMP * MILLIS_CONVERT_FACTOR);
    }

    @Test
    public void testGetTableCreateTimeWithNullTime() {
        LineageVertex tableVertex = createTableVertex(DB_NAME+"."+TABLE_NAME);
        tableVertex.setCreateTime(null);

        long createTime = BaseImpalaEvent.getTableCreateTime(tableVertex);
        assertTrue(createTime > 0);
    }

    @Test
    public void testGetImpalaProcessEntity() throws Exception {
        List<AtlasEntity> inputs = createMockEntities("input_table");
        List<AtlasEntity> outputs = createMockEntities("output_table");

        AtlasEntity processEntity = baseImpalaEvent.getImpalaProcessEntity(inputs, outputs);

        assertNotNull(processEntity);
        assertEquals(processEntity.getTypeName(), ImpalaDataType.IMPALA_PROCESS.getName());
        assertNotNull(processEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
        assertNotNull(processEntity.getAttribute(ATTRIBUTE_INPUTS));
        assertNotNull(processEntity.getAttribute(ATTRIBUTE_OUTPUTS));
        assertEquals(processEntity.getAttribute(ATTRIBUTE_OPERATION_TYPE), ImpalaOperationType.QUERY);
        assertEquals(processEntity.getAttribute(ATTRIBUTE_START_TIME), TIMESTAMP * MILLIS_CONVERT_FACTOR);
        assertEquals(processEntity.getAttribute(ATTRIBUTE_END_TIME), END_TIME * MILLIS_CONVERT_FACTOR);
        assertEquals(processEntity.getAttribute(ATTRIBUTE_USER_NAME), EMPTY_ATTRIBUTE_VALUE);
        assertEquals(processEntity.getAttribute(ATTRIBUTE_QUERY_TEXT), EMPTY_ATTRIBUTE_VALUE);
    }

    @Test
    public void testGetImpalaProcessExecutionEntity() throws Exception {
        List<AtlasEntity> inputs = createMockEntities("input_table");
        List<AtlasEntity> outputs = createMockEntities("output_table");
        AtlasEntity processEntity = baseImpalaEvent.getImpalaProcessEntity(inputs, outputs);

        AtlasEntity executionEntity = baseImpalaEvent.getImpalaProcessExecutionEntity(processEntity);

        assertNotNull(executionEntity);
        assertEquals(executionEntity.getTypeName(), ImpalaDataType.IMPALA_PROCESS_EXECUTION.getName());
        assertNotNull(executionEntity.getAttribute(ATTRIBUTE_QUALIFIED_NAME));
        assertEquals(executionEntity.getAttribute(ATTRIBUTE_START_TIME), TIMESTAMP * MILLIS_CONVERT_FACTOR);
        assertEquals(executionEntity.getAttribute(ATTRIBUTE_END_TIME), END_TIME * MILLIS_CONVERT_FACTOR);
        assertEquals(executionEntity.getAttribute(ATTRIBUTE_USER_NAME), USER_NAME);
        assertEquals(executionEntity.getAttribute(ATTRIBUTE_QUERY_TEXT), QUERY_TEXT.toLowerCase().trim());
        assertEquals(executionEntity.getAttribute(ATTRIBUTE_QUERY_ID), QUERY_ID);
        assertEquals(executionEntity.getAttribute(ATTRIBUTE_HOSTNAME), HOST_NAME);
    }

    @Test
    public void testCreateTableNode() {
        Long createTime = TIMESTAMP;
        ImpalaNode tableNode = baseImpalaEvent.createTableNode(TABLE_NAME, createTime);

        assertNotNull(tableNode);
        assertEquals(tableNode.getOwnVertex().getVertexType(), ImpalaVertexType.TABLE);
        assertEquals(tableNode.getOwnVertex().getVertexId(), TABLE_NAME);
        assertEquals(tableNode.getOwnVertex().getCreateTime(), createTime);
    }

    @Test
    public void testCreateHiveDDLEntityWithDb() {
        AtlasEntity dbEntity = new AtlasEntity(HIVE_TYPE_DB);
        dbEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, "test.db.qualified.name");

        AtlasEntity ddlEntity = baseImpalaEvent.createHiveDDLEntity(dbEntity);

        assertNotNull(ddlEntity);
        assertEquals(ddlEntity.getTypeName(), ImpalaDataType.HIVE_DB_DDL.getName());
        assertEquals(ddlEntity.getAttribute(ATTRIBUTE_SERVICE_TYPE), "impala");
        assertEquals(ddlEntity.getAttribute(ATTRIBUTE_EXEC_TIME), TIMESTAMP * MILLIS_CONVERT_FACTOR);
        assertEquals(ddlEntity.getAttribute(ATTRIBUTE_QUERY_TEXT), QUERY_TEXT);
        assertEquals(ddlEntity.getAttribute(ATTRIBUTE_USER_NAME), USER_NAME);
        assertNotNull(ddlEntity.getAttribute(ATTRIBUTE_DB));
    }

    @Test
    public void testCreateHiveDDLEntityWithTable() {
        AtlasEntity tableEntity = new AtlasEntity(HIVE_TYPE_TABLE);
        tableEntity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, "test.table.qualified.name");

        AtlasEntity ddlEntity = baseImpalaEvent.createHiveDDLEntity(tableEntity);

        assertNotNull(ddlEntity);
        assertEquals(ddlEntity.getTypeName(), ImpalaDataType.HIVE_TABLE_DDL.getName());
        assertEquals(ddlEntity.getAttribute(ATTRIBUTE_SERVICE_TYPE), "impala");
        assertEquals(ddlEntity.getAttribute(ATTRIBUTE_EXEC_TIME), TIMESTAMP * MILLIS_CONVERT_FACTOR);
        assertEquals(ddlEntity.getAttribute(ATTRIBUTE_QUERY_TEXT), QUERY_TEXT);
        assertEquals(ddlEntity.getAttribute(ATTRIBUTE_USER_NAME), USER_NAME);
        assertNotNull(ddlEntity.getAttribute(ATTRIBUTE_TABLE));
    }

    @Test
    public void testIsDdlOperation() {

        when(mockContext.getImpalaOperationType()).thenReturn(ImpalaOperationType.CREATEVIEW);
        assertTrue(baseImpalaEvent.isDdlOperation());

        when(mockContext.getImpalaOperationType()).thenReturn(ImpalaOperationType.ALTERVIEW_AS);
        assertTrue(baseImpalaEvent.isDdlOperation());

        when(mockContext.getImpalaOperationType()).thenReturn(ImpalaOperationType.CREATETABLE_AS_SELECT);
        assertTrue(baseImpalaEvent.isDdlOperation());

        when(mockContext.getImpalaOperationType()).thenReturn(ImpalaOperationType.QUERY);
        assertFalse(baseImpalaEvent.isDdlOperation());
    }

    @Test
    public void testGetCreateTimeInVertex() {

        LineageVertex vertex = createDatabaseVertex(DB_NAME);
        vertex.setCreateTime(TIMESTAMP);

        Long createTime = baseImpalaEvent.getCreateTimeInVertex(vertex);
        assertEquals(createTime, Long.valueOf(TIMESTAMP));

        Long createTimeNull = baseImpalaEvent.getCreateTimeInVertex(null);
        assertTrue(createTimeNull > 0); // Should return current time in seconds

        LineageVertex columnVertex = createColumnVertex(COLUMN_NAME, TABLE_NAME);
        columnVertex.setCreateTime(null);
        LineageVertexMetadata metadata = new LineageVertexMetadata();
        metadata.setTableCreateTime(TIMESTAMP);
        columnVertex.setMetadata(metadata);

        Long metadataCreateTime = baseImpalaEvent.getCreateTimeInVertex(columnVertex);
        assertEquals(metadataCreateTime, Long.valueOf(TIMESTAMP));
    }

    @Test(dataProvider = "qualifiedNameDataProvider")
    public void testGetQualifiedNameForProcess(ImpalaOperationType operationType,
                                               List<AtlasEntity> inputs,
                                               List<AtlasEntity> outputs,
                                               boolean shouldThrowException) throws Exception {
        when(mockContext.getImpalaOperationType()).thenReturn(operationType);

        if (shouldThrowException) {
            try {
                baseImpalaEvent.getQualifiedName(inputs, outputs);
                fail("Expected IllegalArgumentException");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("unexpected operation type"));
            }
        } else {
            String qualifiedName = baseImpalaEvent.getQualifiedName(inputs, outputs);
            assertNotNull(qualifiedName);
            if (operationType == ImpalaOperationType.CREATEVIEW ||
                    operationType == ImpalaOperationType.CREATETABLE_AS_SELECT ||
                    operationType == ImpalaOperationType.ALTERVIEW_AS) {
                assertTrue(qualifiedName.contains("@"));
            } else {
                assertTrue(qualifiedName.contains("->"));
            }
        }
    }

    @DataProvider(name = "qualifiedNameDataProvider")
    public Object[][] qualifiedNameDataProvider() {
        List<AtlasEntity> inputs = createMockEntities("input_table");
        List<AtlasEntity> outputs = createMockEntities("output_table");

        return new Object[][] {
                {ImpalaOperationType.CREATEVIEW, inputs, outputs, false},
                {ImpalaOperationType.CREATETABLE_AS_SELECT, inputs, outputs, false},
                {ImpalaOperationType.ALTERVIEW_AS, inputs, outputs, false},
                {ImpalaOperationType.QUERY, inputs, outputs, false},
                {ImpalaOperationType.QUERY_WITH_CLAUSE, inputs, outputs, false},
                {ImpalaOperationType.INSERT, inputs, outputs, true}  // Should throw exception
        };
    }

    // Helper methods for creating test data

    private LineageVertex createDatabaseVertex(String dbName) {
        LineageVertex vertex = new LineageVertex();
        vertex.setVertexType(ImpalaVertexType.DATABASE);
        vertex.setVertexId(dbName);
        vertex.setCreateTime(TIMESTAMP);
        return vertex;
    }

    private LineageVertex createTableVertex(String tableName) {
        LineageVertex vertex = new LineageVertex();
        vertex.setVertexType(ImpalaVertexType.TABLE);
        vertex.setVertexId(tableName);
        vertex.setCreateTime(TIMESTAMP);
        return vertex;
    }

    private LineageVertex createColumnVertex(String columnName, String tableName) {
        LineageVertex vertex = new LineageVertex();
        vertex.setVertexType(ImpalaVertexType.COLUMN);
        vertex.setVertexId(DB_NAME+"."+tableName+"."+columnName);
        vertex.setCreateTime(TIMESTAMP);

        LineageVertexMetadata metadata = new LineageVertexMetadata();
        metadata.setTableName(tableName);
        metadata.setTableCreateTime(TIMESTAMP);
        vertex.setMetadata(metadata);

        return vertex;
    }

    private List<AtlasEntity> createMockEntities(String tableBaseName) {
        List<AtlasEntity> entities = new ArrayList<>();

        AtlasEntity entity = new AtlasEntity(HIVE_TYPE_TABLE);
        entity.setGuid("test-guid-" + tableBaseName);
        entity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, tableBaseName  +"@"+  CLUSTER_NAME);
        entity.setAttribute(ATTRIBUTE_NAME, tableBaseName);
        entity.setAttribute(ATTRIBUTE_CREATE_TIME, TIMESTAMP * MILLIS_CONVERT_FACTOR);

        entities.add(entity);
        return entities;
    }
}
