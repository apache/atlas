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
package org.apache.atlas.impala.hook.events;

import org.apache.atlas.impala.hook.AtlasImpalaHookContext;
import org.apache.atlas.impala.hook.ImpalaLineageHook;
import org.apache.atlas.impala.model.ImpalaDependencyType;
import org.apache.atlas.impala.model.ImpalaOperationType;
import org.apache.atlas.impala.model.ImpalaQuery;
import org.apache.atlas.impala.model.ImpalaVertexType;
import org.apache.atlas.impala.model.LineageEdge;
import org.apache.atlas.impala.model.LineageVertex;
import org.apache.atlas.impala.model.LineageVertexMetadata;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.notification.HookNotification;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.impala.hook.events.BaseImpalaEvent.ATTRIBUTE_NAME;
import static org.apache.atlas.impala.hook.events.BaseImpalaEvent.ATTRIBUTE_QUALIFIED_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;
import static org.testng.AssertJUnit.assertTrue;

public class CreateImpalaProcessTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateImpalaProcessTest.class);

    @Mock
    private ImpalaLineageHook mockHook;

    @Mock
    private AtlasImpalaHookContext mockContext;

    @Mock
    private ImpalaLineageHook impalaLineageHook;

    @Mock
    private ImpalaOperationType impalaOperationType;

    @Mock
    private ImpalaQuery impalaQuery;

    @Mock
    private AtlasEntity mockOutputEntity;

    @Mock
    private LineageVertexMetadata mockMetadata;

    @Mock
    private LineageEdge mockLineageEdge;
    @Mock
    private ImpalaQuery mockLineageQuery;

    @Mock
    private AtlasEntity mockInputEntity;


    private CreateImpalaProcess createImpalaProcess;

    private static final String TEST_CLUSTER_NAME = "test_cluster";
    private static final String TEST_DB_NAME = "test_db";
    private static final String CLUSTER_NAME = "testcluster";
    private static final String DB_NAME = "testdb";
    private static final String TABLE_NAME_SOURCE = "source_table";
    private static final String TABLE_NAME_TARGET = "target_table";
    private static final String COLUMN_NAME_ID = "id";
    private static final String COLUMN_NAME_NAME = "name";
    private static final String USER_NAME = "testuser";
    private static final String HOST_NAME = "testhost";
    private static final String QUERY_TEXT = "CREATE VIEW target_table AS SELECT id, name FROM source_db.source_table";
    private static final String QUERY_ID = "test_query_id_123";
    private static final long TIMESTAMP = 1554750072L;
    private static final long END_TIME = 1554750554L;

    private static final String TEST_TABLE_NAME = "test_table";
    private static final String TEST_COLUMN_NAME = "test_column";
    private static final String TEST_QUALIFIED_NAME = "test_db.test_table@test_cluster";
    private static final String TEST_QUERY_TEXT = "SELECT * FROM test_table";
    private static final String TEST_USER_NAME = "test_user";
    private static final long TEST_TIMESTAMP = 1234567890L;
    private static final long TEST_END_TIME = 1234567900L;
    private static final long TEST_VERTEX_ID = 123L;


    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(mockHook.getMetadataNamespace()).thenReturn(CLUSTER_NAME);
        when(mockHook.getHostName()).thenReturn(HOST_NAME);
        when(mockHook.isConvertHdfsPathToLowerCase()).thenReturn(false);

        when(mockContext.getUserName()).thenReturn(USER_NAME);
        when(mockContext.getHostName()).thenReturn(HOST_NAME);
        when(mockContext.getMetadataNamespace()).thenReturn(CLUSTER_NAME);
        when(mockContext.getQueryStr()).thenReturn(QUERY_TEXT);
        when(mockContext.getImpalaOperationType()).thenReturn(ImpalaOperationType.CREATEVIEW);

        createImpalaProcess = new CreateImpalaProcess(mockContext);
    }


    @Test
    public void testGetNotificationMessagesWithNoEntities() throws Exception {

        ImpalaQuery emptyQuery = new ImpalaQuery();
        emptyQuery.setVertices(new ArrayList<>());
        emptyQuery.setEdges(new ArrayList<>());
        emptyQuery.setQueryText(QUERY_TEXT);
        emptyQuery.setQueryId(QUERY_ID);
        emptyQuery.setUser(USER_NAME);
        emptyQuery.setTimestamp(TIMESTAMP);
        emptyQuery.setEndTime(END_TIME);

        when(mockContext.getLineageQuery()).thenReturn(emptyQuery);

        CreateImpalaProcess process = new CreateImpalaProcess(mockContext);
        List<HookNotification> notifications = process.getNotificationMessages();

        assertNull(notifications);
    }

    @Test
    public void testGetNotificationMessagesWithEntities() throws Exception {

        String sourceTableFullName = "testdb.source_table";
        String targetTableFullName = "testdb.target_table";
        ImpalaQuery query = createTestQuery();
        when(mockContext.getLineageQuery()).thenReturn(query);

        AtlasEntity mockInputEntity = createMockTableEntity(TABLE_NAME_SOURCE);
        AtlasEntity mockOutputEntity = createMockTableEntity(TABLE_NAME_TARGET);

        when(mockContext.getQualifiedNameForTable(sourceTableFullName)).thenReturn(TEST_DB_NAME+"."+TABLE_NAME_SOURCE+"@"+TEST_CLUSTER_NAME);
        when(mockContext.getDatabaseNameFromTable(sourceTableFullName)).thenReturn(TEST_DB_NAME);
        when(mockContext.getQualifiedNameForTable(targetTableFullName)).thenReturn(TEST_DB_NAME+"."+TABLE_NAME_TARGET+"@"+TEST_CLUSTER_NAME);
        when(mockContext.getDatabaseNameFromTable(targetTableFullName)).thenReturn(TEST_DB_NAME);
        when(mockContext.getImpalaOperationType()).thenReturn(ImpalaOperationType.QUERY);
        when(mockContext.getEntity(getQualifiedTableName(TABLE_NAME_SOURCE)))
                .thenReturn(mockInputEntity);
        when(mockContext.getEntity(getQualifiedTableName(TABLE_NAME_TARGET)))
                .thenReturn(mockOutputEntity);


        CreateImpalaProcess process = new CreateImpalaProcess(mockContext);
        AtlasEntitiesWithExtInfo entities = process.getEntities();
        assertEquals(entities.getEntities().size(),2);
        assertEquals(entities.getEntities().get(0).getTypeName(),"impala_process");
        assertEquals(entities.getEntities().get(1).getTypeName(),"impala_process_execution");
        assertEquals(entities.getEntities().get(0).getAttribute(ATTRIBUTE_QUALIFIED_NAME),"QUERY:"+TEST_DB_NAME+"."+TABLE_NAME_SOURCE+"@"+TEST_CLUSTER_NAME+":"+"1554750072000->:"+TEST_DB_NAME+"."+TABLE_NAME_TARGET+"@"+TEST_CLUSTER_NAME+":1554750072000");
    }


    @Test
    public void testGetEntitiesWithValidData() throws Exception {

        String sourceTableFullName = "testdb.source_table";
        String targetTableFullName = "testdb.target_table";
        ImpalaQuery query = createTestQuery();
        when(mockContext.getLineageQuery()).thenReturn(query);

        AtlasEntity mockInputEntity = createMockTableEntity(TABLE_NAME_SOURCE);
        AtlasEntity mockOutputEntity = createMockTableEntity(TABLE_NAME_TARGET);

        when(mockContext.getQualifiedNameForTable(sourceTableFullName)).thenReturn(TEST_DB_NAME+"."+TABLE_NAME_SOURCE+"@"+TEST_CLUSTER_NAME);
        when(mockContext.getDatabaseNameFromTable(sourceTableFullName)).thenReturn(TEST_DB_NAME);
        when(mockContext.getQualifiedNameForTable(targetTableFullName)).thenReturn(TEST_DB_NAME+"."+TABLE_NAME_TARGET+"@"+TEST_CLUSTER_NAME);
        when(mockContext.getDatabaseNameFromTable(targetTableFullName)).thenReturn(TEST_DB_NAME);
        when(mockContext.getImpalaOperationType()).thenReturn(ImpalaOperationType.QUERY);
        when(mockContext.getEntity(getQualifiedTableName(TABLE_NAME_SOURCE)))
                .thenReturn(mockInputEntity);
        when(mockContext.getEntity(getQualifiedTableName(TABLE_NAME_TARGET)))
                .thenReturn(mockOutputEntity);

        CreateImpalaProcess process = new CreateImpalaProcess(mockContext);
        List<HookNotification>  hookMsgList = process.getNotificationMessages();

        assertEquals(hookMsgList.get(0).getType().ordinal(),HookNotification.HookNotificationType.ENTITY_CREATE_V2.ordinal());
    }

    @Test
    public void testGetEntitiesWithEmptyInputsAndOutputs() throws Exception {
        ImpalaQuery query = new ImpalaQuery();
        query.setVertices(new ArrayList<>());
        query.setEdges(new ArrayList<>());
        query.setQueryText(QUERY_TEXT);
        query.setQueryId(QUERY_ID);
        query.setUser(USER_NAME);
        query.setTimestamp(TIMESTAMP);
        query.setEndTime(END_TIME);

        when(mockContext.getLineageQuery()).thenReturn(query);

        CreateImpalaProcess process = new CreateImpalaProcess(mockContext);
        AtlasEntitiesWithExtInfo entities = process.getEntities();

        assertNull(entities);
    }

    @Test
    public void testUserName() {
        assertEquals(createImpalaProcess.getUserName(), USER_NAME);
    }

    private ImpalaQuery createTestQuery() {
        ImpalaQuery query = new ImpalaQuery();
        query.setQueryText(QUERY_TEXT);
        query.setQueryId(QUERY_ID);
        query.setUser(USER_NAME);
        query.setTimestamp(TIMESTAMP);
        query.setEndTime(END_TIME);

        List<LineageVertex> vertices = new ArrayList<>();


        LineageVertex sourceCol1 = createColumnVertex(0L, DB_NAME  +"."+  TABLE_NAME_SOURCE  +"."+  COLUMN_NAME_ID,
                TABLE_NAME_SOURCE, COLUMN_NAME_ID);
        LineageVertex sourceCol2 = createColumnVertex(1L, DB_NAME  +"."+  TABLE_NAME_SOURCE  +"." + COLUMN_NAME_NAME,
                TABLE_NAME_SOURCE, COLUMN_NAME_NAME);


        LineageVertex targetCol1 = createColumnVertex(2L, DB_NAME  +"."+  TABLE_NAME_TARGET  +"."+  COLUMN_NAME_ID,
                TABLE_NAME_TARGET, COLUMN_NAME_ID);
        LineageVertex targetCol2 = createColumnVertex(3L, DB_NAME  +"."+  TABLE_NAME_TARGET  +"."  +COLUMN_NAME_NAME,
                TABLE_NAME_TARGET, COLUMN_NAME_NAME);

        vertices.add(sourceCol1);
        vertices.add(sourceCol2);
        vertices.add(targetCol1);
        vertices.add(targetCol2);
        query.setVertices(vertices);

        List<LineageEdge> edges = new ArrayList<>();

        LineageEdge edge1 = new LineageEdge();
        edge1.setSources(Arrays.asList(0L));
        edge1.setTargets(Arrays.asList(2L));
        edge1.setEdgeType(ImpalaDependencyType.PROJECTION);
        edges.add(edge1);

        LineageEdge edge2 = new LineageEdge();
        edge2.setSources(Arrays.asList(1L));
        edge2.setTargets(Arrays.asList(3L));
        edge2.setEdgeType(ImpalaDependencyType.PROJECTION);
        edges.add(edge2);

        query.setEdges(edges);

        return query;
    }


    private LineageVertex createColumnVertex(Long id, String vertexId, String tableName, String columnName) {
        LineageVertex vertex = new LineageVertex();
        vertex.setId(id);
        vertex.setVertexId(vertexId);
        vertex.setVertexType(ImpalaVertexType.COLUMN);

        LineageVertexMetadata metadata = new LineageVertexMetadata();
        metadata.setTableName(DB_NAME  +"."+  tableName);
        metadata.setTableCreateTime(TIMESTAMP);
        vertex.setMetadata(metadata);

        return vertex;
    }


    private AtlasEntity createMockTableEntity(String tableName) {
        AtlasEntity entity = new AtlasEntity(BaseImpalaEvent.HIVE_TYPE_TABLE);
        entity.setAttribute(ATTRIBUTE_NAME, tableName);
        entity.setAttribute(ATTRIBUTE_QUALIFIED_NAME, getQualifiedTableName(tableName));
        return entity;
    }


    private String getQualifiedTableName(String tableName) {
        return (DB_NAME  +"."+  tableName  +"@"+  CLUSTER_NAME).toLowerCase();
    }

    @Test
    public void testCreateImpalaProcessWithColumnLineage() throws Exception {

        ImpalaQuery query = createTestQuery();
        when(mockContext.getLineageQuery()).thenReturn(query);

        AtlasEntity mockInputCol1 = new AtlasEntity(BaseImpalaEvent.HIVE_TYPE_COLUMN);
        mockInputCol1.setAttribute(ATTRIBUTE_NAME, COLUMN_NAME_ID);
        AtlasEntity mockInputCol2 = new AtlasEntity(BaseImpalaEvent.HIVE_TYPE_COLUMN);
        mockInputCol2.setAttribute(ATTRIBUTE_NAME, COLUMN_NAME_NAME);

        AtlasEntity mockOutputCol1 = new AtlasEntity(BaseImpalaEvent.HIVE_TYPE_COLUMN);
        mockOutputCol1.setAttribute(ATTRIBUTE_NAME, COLUMN_NAME_ID);
        AtlasEntity mockOutputCol2 = new AtlasEntity(BaseImpalaEvent.HIVE_TYPE_COLUMN);
        mockOutputCol2.setAttribute(ATTRIBUTE_NAME, COLUMN_NAME_NAME);

        when(mockContext.getEntity(DB_NAME  +"."+  TABLE_NAME_SOURCE  +"."+  COLUMN_NAME_ID))
                .thenReturn(mockInputCol1);
        when(mockContext.getEntity(DB_NAME  +"."+  TABLE_NAME_SOURCE  +"."+  COLUMN_NAME_NAME))
                .thenReturn(mockInputCol2);
        when(mockContext.getEntity(DB_NAME  +"."+  TABLE_NAME_TARGET  +"."+  COLUMN_NAME_ID))
                .thenReturn(mockOutputCol1);
        when(mockContext.getEntity(DB_NAME  +"."+  TABLE_NAME_TARGET  +"."+  COLUMN_NAME_NAME))
                .thenReturn(mockOutputCol2);

        CreateImpalaProcess process = new CreateImpalaProcess(mockContext);

        AtlasEntitiesWithExtInfo entities = process.getEntities();

    }

    @Test
    public void testProcessWithPredicateEdgeType() throws Exception {
        ImpalaQuery query = createTestQuery();

        for (LineageEdge edge : query.getEdges()) {
            edge.setEdgeType(ImpalaDependencyType.PREDICATE);
        }

        when(mockContext.getLineageQuery()).thenReturn(query);

        CreateImpalaProcess process = new CreateImpalaProcess(mockContext);

        AtlasEntitiesWithExtInfo entities = process.getEntities();

    }

    @Test
    public void testProcessWithMixedEdgeTypes() throws Exception {
        ImpalaQuery query = createTestQuery();

        LineageEdge predicateEdge = new LineageEdge();
        predicateEdge.setSources(Arrays.asList(0L));
        predicateEdge.setTargets(Arrays.asList(2L));
        predicateEdge.setEdgeType(ImpalaDependencyType.PREDICATE);
        query.getEdges().add(predicateEdge);

        when(mockContext.getLineageQuery()).thenReturn(query);

        CreateImpalaProcess process = new CreateImpalaProcess(mockContext);
        AtlasEntitiesWithExtInfo entities = process.getEntities();

        LOG.info("Mixed edge types test completed");
    }

    @Test
    public void testProcessWithNullVertexMetadata() throws Exception {
        ImpalaQuery query = createTestQuery();

        query.getVertices().get(0).setMetadata(null);

        when(mockContext.getLineageQuery()).thenReturn(query);

        CreateImpalaProcess process = new CreateImpalaProcess(mockContext);

        AtlasEntitiesWithExtInfo entities = process.getEntities();

    }

    private void setupBasicMocks() {
        when(mockContext.getLineageQuery()).thenReturn(mockLineageQuery);
        when(mockContext.getUserName()).thenReturn(TEST_USER_NAME);
        when(mockContext.getImpalaOperationType()).thenReturn(ImpalaOperationType.QUERY);
        when(mockLineageQuery.getQueryText()).thenReturn(TEST_QUERY_TEXT);
        when(mockLineageQuery.getTimestamp()).thenReturn(TEST_TIMESTAMP);
        when(mockLineageQuery.getEndTime()).thenReturn(TEST_END_TIME);
    }

    private void setupOutputVertices() throws Exception {
        java.lang.reflect.Field verticesMapField = BaseImpalaEvent.class.getDeclaredField("verticesMap");
        verticesMapField.setAccessible(true);
        Map<Long, LineageVertex> verticesMap = (Map<Long, LineageVertex>) verticesMapField.get(createImpalaProcess);

        LineageVertex outputVertex1 = mock(LineageVertex.class);
        LineageVertex inputVertex = mock(LineageVertex.class);

        when(outputVertex1.getVertexId()).thenReturn("output_column1");
        when(outputVertex1.getVertexType()).thenReturn(ImpalaVertexType.COLUMN);
        when(outputVertex1.getMetadata()).thenReturn(mockMetadata);

        when(inputVertex.getVertexId()).thenReturn("input_column");
        when(inputVertex.getVertexType()).thenReturn(ImpalaVertexType.COLUMN);
        when(inputVertex.getMetadata()).thenReturn(mockMetadata);

        when(mockMetadata.getTableName()).thenReturn(TEST_TABLE_NAME);

        verticesMap.put(TEST_VERTEX_ID, outputVertex1);
        verticesMap.put(TEST_VERTEX_ID + 2, inputVertex);
    }

    private AtlasEntity createMockProcessEntity() {
        AtlasEntity process = mock(AtlasEntity.class);
        when(process.getAttribute("qualifiedName")).thenReturn(TEST_DB_NAME+"."+TEST_TABLE_NAME+"."+TEST_COLUMN_NAME+"@"+TEST_CLUSTER_NAME);
        when(process.getAttribute("name")).thenReturn("test_process");
        return process;
    }

    @Test
    public void testProcessColumnLineageWithOutputColumn() throws Exception {

        String outputColumnName = "output_column";
        setupBasicMocks();

        List<LineageEdge> edges = Arrays.asList(mockLineageEdge);
        when(mockLineageQuery.getEdges()).thenReturn(edges);
        when(mockLineageEdge.getEdgeType()).thenReturn(ImpalaDependencyType.PROJECTION);
        when(mockLineageEdge.getTargets()).thenReturn(Arrays.asList(TEST_VERTEX_ID));
        when(mockContext.getQualifiedNameForColumn(any(LineageVertex.class))).thenReturn(TEST_DB_NAME+"."+TEST_TABLE_NAME+"."+TEST_COLUMN_NAME+"@"+TEST_CLUSTER_NAME);
        when(mockLineageEdge.getSources()).thenReturn(Arrays.asList(TEST_VERTEX_ID));

        setupOutputVertices();

        when(mockContext.getEntity(anyString())).thenReturn(mockOutputEntity, mockOutputEntity, mockInputEntity);
        when(mockOutputEntity.getAttribute("name")).thenReturn(outputColumnName);

        AtlasEntitiesWithExtInfo entities = new AtlasEntitiesWithExtInfo();
        AtlasEntity process = createMockProcessEntity();

        Method processColumnLineageMethod = CreateImpalaProcess.class.getDeclaredMethod("processColumnLineage", AtlasEntity.class, AtlasEntitiesWithExtInfo.class);
        processColumnLineageMethod.setAccessible(true);

        processColumnLineageMethod.invoke(createImpalaProcess, process, entities);

        assertTrue(entities.getEntities().size() > 0);
        assertEquals(entities.getEntities().get(0).getTypeName(),"impala_column_lineage");
        assertEquals(entities.getEntities().get(0).getAttribute(ATTRIBUTE_QUALIFIED_NAME),TEST_DB_NAME+"."+TEST_TABLE_NAME+"."+TEST_COLUMN_NAME+"@"+TEST_CLUSTER_NAME+":"+outputColumnName);
    }


}