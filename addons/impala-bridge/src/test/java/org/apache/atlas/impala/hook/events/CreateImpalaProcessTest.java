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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.atlas.impala.hook.events.BaseImpalaEvent.ATTRIBUTE_NAME;
import static org.apache.atlas.impala.hook.events.BaseImpalaEvent.ATTRIBUTE_QUALIFIED_NAME;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

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


    private CreateImpalaProcess createImpalaProcess;

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
    public void testGetEntitiesWithValidData() throws Exception {
        ImpalaQuery query = createTestQuery();
        when(mockContext.getLineageQuery()).thenReturn(query);

        AtlasEntity mockInputEntity = createMockTableEntity(TABLE_NAME_SOURCE);
        AtlasEntity mockOutputEntity = createMockTableEntity(TABLE_NAME_TARGET);

        when(mockContext.getEntity(getQualifiedTableName(TABLE_NAME_SOURCE)))
                .thenReturn(mockInputEntity);
        when(mockContext.getEntity(getQualifiedTableName(TABLE_NAME_TARGET)))
                .thenReturn(mockOutputEntity);

        CreateImpalaProcess process = new CreateImpalaProcess(mockContext);
        AtlasEntitiesWithExtInfo entities = process.getEntities();

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
        edge1.setSources(Arrays.asList(0L)); // source id column
        edge1.setTargets(Arrays.asList(2L)); // target id column
        edge1.setEdgeType(ImpalaDependencyType.PROJECTION);
        edges.add(edge1);

        LineageEdge edge2 = new LineageEdge();
        edge2.setSources(Arrays.asList(1L)); // source name column
        edge2.setTargets(Arrays.asList(3L)); // target name column
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

        // This should handle predicate edges appropriately (ignore them for lineage)
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
}