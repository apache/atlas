/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasRelationship.AtlasRelationshipWithExtInfo;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AtlasRelationshipStoreV2ReadAuthTest {
    private static final String RELATIONSHIP_GUID     = "relationship-guid-1";
    private static final String END1_GUID             = "end1-guid";
    private static final String END2_GUID             = "end2-guid";
    private static final String END1_QUALIFIED_NAME   = "sales_fact@cl1";
    private static final String END2_QUALIFIED_NAME   = "Sales.sales_fact.sales@cl1";

    @Mock
    private AtlasGraph graph;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private DeleteHandlerDelegate deleteDelegate;

    @Mock
    private IAtlasEntityChangeNotifier entityChangeNotifier;

    @Mock
    private GraphHelper graphHelper;

    @Mock
    private EntityGraphRetriever entityRetriever;

    @Mock
    private AtlasEdge edge;

    @Mock
    private AtlasVertex end1Vertex;

    @Mock
    private AtlasVertex end2Vertex;

    private AtlasRelationshipStoreV2 relationshipStore;

    private AutoCloseable mocks;

    @BeforeMethod
    public void setUp() throws Exception {
        mocks = MockitoAnnotations.openMocks(this);

        relationshipStore = new AtlasRelationshipStoreV2(graph, typeRegistry, deleteDelegate, entityChangeNotifier);

        setField("graphHelper", graphHelper);
        setField("entityRetriever", entityRetriever);

        when(graphHelper.getEdgeForGUID(RELATIONSHIP_GUID)).thenReturn(edge);
        when(edge.getOutVertex()).thenReturn(end1Vertex);
        when(edge.getInVertex()).thenReturn(end2Vertex);

        when(entityRetriever.toAtlasEntityHeaderWithClassifications(end1Vertex)).thenReturn(tableHeader(END1_QUALIFIED_NAME));
        when(entityRetriever.toAtlasEntityHeaderWithClassifications(end2Vertex)).thenReturn(columnHeader(END2_QUALIFIED_NAME));
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    public void testGetById_VerifiesEntityReadOnBothEnds() throws Exception {
        AtlasRelationship expectedRelationship = new AtlasRelationship("hive_table_columns");

        when(entityRetriever.mapEdgeToAtlasRelationship(edge)).thenReturn(expectedRelationship);

        try (MockedStatic<AtlasAuthorizationUtils> authUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            authUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasEntityAccessRequest.class), any(), any()))
                    .thenAnswer(invocation -> null);

            AtlasRelationship actualRelationship = relationshipStore.getById(RELATIONSHIP_GUID);

            assertEquals(actualRelationship, expectedRelationship);
            authUtils.verify(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasEntityAccessRequest.class), any(), any()), times(2));
        }
    }

    @Test
    public void testGetExtInfoById_VerifiesEntityReadOnBothEnds() throws Exception {
        AtlasRelationshipWithExtInfo expectedRelationship = new AtlasRelationshipWithExtInfo(new AtlasRelationship("hive_table_columns"));

        when(entityRetriever.mapEdgeToAtlasRelationshipWithExtInfo(edge)).thenReturn(expectedRelationship);

        try (MockedStatic<AtlasAuthorizationUtils> authUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            authUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasEntityAccessRequest.class), any(), any()))
                    .thenAnswer(invocation -> null);

            AtlasRelationshipWithExtInfo actualRelationship = relationshipStore.getExtInfoById(RELATIONSHIP_GUID);

            assertEquals(actualRelationship, expectedRelationship);
            authUtils.verify(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasEntityAccessRequest.class), any(), any()), times(2));
        }
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testGetById_DeniesWhenEnd1EntityReadFails() throws Exception {
        when(entityRetriever.mapEdgeToAtlasRelationship(edge)).thenReturn(new AtlasRelationship("hive_table_columns"));

        try (MockedStatic<AtlasAuthorizationUtils> authUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            authUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasEntityAccessRequest.class), eq("read relationship: end1 guid="), eq(END1_GUID)))
                    .thenThrow(new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, END1_GUID, AtlasPrivilege.ENTITY_READ.getType()));

            relationshipStore.getById(RELATIONSHIP_GUID);
        }
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testGetById_DeniesWhenEnd2EntityReadFails() throws Exception {
        when(entityRetriever.mapEdgeToAtlasRelationship(edge)).thenReturn(new AtlasRelationship("hive_table_columns"));

        try (MockedStatic<AtlasAuthorizationUtils> authUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            authUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasEntityAccessRequest.class), eq("read relationship: end1 guid="), eq(END1_GUID)))
                    .thenAnswer(invocation -> null);
            authUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasEntityAccessRequest.class), eq("read relationship: end2 guid="), eq(END2_GUID)))
                    .thenThrow(new AtlasBaseException(AtlasErrorCode.UNAUTHORIZED_ACCESS, END2_GUID, AtlasPrivilege.ENTITY_READ.getType()));

            relationshipStore.getById(RELATIONSHIP_GUID);
        }
    }

    @Test
    public void testGetById_PassesEndQualifiedNamesToEntityReadCheck() throws Exception {
        when(entityRetriever.mapEdgeToAtlasRelationship(edge)).thenReturn(new AtlasRelationship("Table_Columns"));

        ArgumentCaptor<AtlasEntityAccessRequest> requestCaptor = ArgumentCaptor.forClass(AtlasEntityAccessRequest.class);

        try (MockedStatic<AtlasAuthorizationUtils> authUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            authUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(requestCaptor.capture(), any(), any()))
                    .thenAnswer(invocation -> null);

            relationshipStore.getById(RELATIONSHIP_GUID);

            List<AtlasEntityAccessRequest> requests = requestCaptor.getAllValues();

            assertEquals(requests.size(), 2);
            assertEquals(requests.get(0).getEntityId(), END1_QUALIFIED_NAME);
            assertEquals(requests.get(1).getEntityId(), END2_QUALIFIED_NAME);
            assertEquals(requests.get(0).getAction(), AtlasPrivilege.ENTITY_READ);
            assertEquals(requests.get(1).getAction(), AtlasPrivilege.ENTITY_READ);
        }
    }

    @Test
    public void testGetById_AllowsWhenBothEndQualifiedNamesPermitted() throws Exception {
        when(entityRetriever.mapEdgeToAtlasRelationship(edge)).thenReturn(new AtlasRelationship("Table_Columns"));

        try (MockedStatic<AtlasAuthorizationUtils> authUtils = mockStatic(AtlasAuthorizationUtils.class)) {
            authUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasEntityAccessRequest.class), any(), any()))
                    .thenAnswer(invocation -> {
                        AtlasEntityAccessRequest request = invocation.getArgument(0);
                        String                   entityId  = request.getEntityId();

                        assertTrue(entityId.equals(END1_QUALIFIED_NAME) || entityId.equals(END2_QUALIFIED_NAME),
                                "unexpected entityId for relationship read: " + entityId);

                        return null;
                    });

            AtlasRelationship actualRelationship = relationshipStore.getById(RELATIONSHIP_GUID);

            assertEquals(actualRelationship.getTypeName(), "Table_Columns");
            authUtils.verify(() -> AtlasAuthorizationUtils.verifyAccess(any(AtlasEntityAccessRequest.class), any(), any()), times(2));
        }
    }

    private AtlasEntityHeader tableHeader(String qualifiedName) {
        return createEntityHeader("Table", END1_GUID, qualifiedName);
    }

    private AtlasEntityHeader columnHeader(String qualifiedName) {
        return createEntityHeader("Column", END2_GUID, qualifiedName);
    }

    private AtlasEntityHeader createEntityHeader(String typeName, String guid, String qualifiedName) {
        Map<String, Object> attributes = new HashMap<>();

        attributes.put("qualifiedName", qualifiedName);

        return new AtlasEntityHeader(typeName, guid, attributes);
    }

    private void setField(String fieldName, Object value) throws Exception {
        Field field = AtlasRelationshipStoreV2.class.getDeclaredField(fieldName);

        field.setAccessible(true);
        field.set(relationshipStore, value);
    }
}
