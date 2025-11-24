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

package org.apache.atlas.web.util;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.instance.Struct;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class LineageUtilsTest {
    private AtlasTypeRegistry typeRegistry;
    private AtlasLineageInfo lineageInfo;
    private AtlasEntityHeader entityHeader;
    private AtlasEntityType entityType;

    @BeforeMethod
    public void setUp() {
        typeRegistry = mock(AtlasTypeRegistry.class);
        lineageInfo = mock(AtlasLineageInfo.class);
        entityHeader = mock(AtlasEntityHeader.class);
        entityType = mock(AtlasEntityType.class);
    }

    @Test
    public void testToLineageStruct_InputDirection() throws AtlasBaseException {
        // Arrange
        Map<String, AtlasEntityHeader> guidEntityMap = new HashMap<>();
        String guid = "test-guid";
        guidEntityMap.put(guid, entityHeader);

        Set<AtlasLineageInfo.LineageRelation> relations = new HashSet<>();
        AtlasLineageInfo.LineageRelation relation = new AtlasLineageInfo.LineageRelation();
        relation.setFromEntityId("from-guid");
        relation.setToEntityId(guid);
        relations.add(relation);

        when(lineageInfo.getGuidEntityMap()).thenReturn(guidEntityMap);
        when(lineageInfo.getRelations()).thenReturn(relations);
        when(lineageInfo.getLineageDirection()).thenReturn(AtlasLineageInfo.LineageDirection.INPUT);
        when(entityHeader.getTypeName()).thenReturn("DataSetType");
        when(entityHeader.getStatus()).thenReturn(AtlasEntity.Status.ACTIVE);
        when(entityHeader.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME)).thenReturn("qualifiedName");
        when(entityHeader.getDisplayText()).thenReturn("displayName");
        when(typeRegistry.getType("DataSetType")).thenReturn(entityType);
        when(entityType.getAllSuperTypes()).thenReturn(Collections.singleton("DataSet"));

        // Act
        Struct result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

        // Assert
        assertNotNull(result);
        assertTrue(result.getTypeName().startsWith(Constants.TEMP_STRUCT_NAME_PREFIX));
        Map<String, Struct> vertices = (Map<String, Struct>) result.get("vertices");
        Map<String, List<String>> edges = (Map<String, List<String>>) result.get("edges");

        assertNotNull(vertices);
        assertEquals(vertices.size(), 1);
        Struct vertex = vertices.get(guid);
        assertNotNull(vertex);
        assertEquals(vertex.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), "qualifiedName");
        assertEquals(vertex.get(AtlasClient.NAME), "displayName");

        Struct vertexId = (Struct) vertex.get("vertexId");
        assertNotNull(vertexId);
        assertEquals(vertexId.getTypeName(), "__IdType");
        assertEquals(vertexId.get(Constants.ATTRIBUTE_NAME_GUID), guid);
        assertEquals(vertexId.get(Constants.ATTRIBUTE_NAME_STATE), "ACTIVE");
        assertEquals(vertexId.get(Constants.ATTRIBUTE_NAME_TYPENAME), "DataSetType");

        assertNotNull(edges);
        assertEquals(edges.size(), 1);
        assertTrue(edges.containsKey(guid));
        assertEquals(edges.get(guid).size(), 1);
        assertEquals(edges.get(guid).get(0), "from-guid");
    }

    @Test
    public void testToLineageStruct_OutputDirection() throws AtlasBaseException {
        // Arrange
        Map<String, AtlasEntityHeader> guidEntityMap = new HashMap<>();
        String guid = "test-guid";
        guidEntityMap.put(guid, entityHeader);

        Set<AtlasLineageInfo.LineageRelation> relations = new HashSet<>();
        AtlasLineageInfo.LineageRelation relation = new AtlasLineageInfo.LineageRelation();
        relation.setFromEntityId(guid);
        relation.setToEntityId("to-guid");
        relations.add(relation);

        when(lineageInfo.getGuidEntityMap()).thenReturn(guidEntityMap);
        when(lineageInfo.getRelations()).thenReturn(relations);
        when(lineageInfo.getLineageDirection()).thenReturn(AtlasLineageInfo.LineageDirection.OUTPUT);
        when(entityHeader.getTypeName()).thenReturn("DataSetType");
        when(entityHeader.getStatus()).thenReturn(AtlasEntity.Status.ACTIVE);
        when(entityHeader.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME)).thenReturn("qualifiedName");
        when(entityHeader.getDisplayText()).thenReturn("displayName");
        when(typeRegistry.getType("DataSetType")).thenReturn(entityType);
        when(entityType.getAllSuperTypes()).thenReturn(Collections.singleton("DataSet"));

        // Act
        Struct result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

        // Assert
        assertNotNull(result);
        assertTrue(result.getTypeName().startsWith(Constants.TEMP_STRUCT_NAME_PREFIX));
        Map<String, Struct> vertices = (Map<String, Struct>) result.get("vertices");
        Map<String, List<String>> edges = (Map<String, List<String>>) result.get("edges");

        assertNotNull(vertices);
        assertEquals(vertices.size(), 1);
        Struct vertex = vertices.get(guid);
        assertNotNull(vertex);
        assertEquals(vertex.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), "qualifiedName");
        assertEquals(vertex.get(AtlasClient.NAME), "displayName");

        Struct vertexId = (Struct) vertex.get("vertexId");
        assertNotNull(vertexId);
        assertEquals(vertexId.getTypeName(), "__IdType");
        assertEquals(vertexId.get(Constants.ATTRIBUTE_NAME_GUID), guid);
        assertEquals(vertexId.get(Constants.ATTRIBUTE_NAME_STATE), "ACTIVE");
        assertEquals(vertexId.get(Constants.ATTRIBUTE_NAME_TYPENAME), "DataSetType");

        assertNotNull(edges);
        assertEquals(edges.size(), 1);
        assertTrue(edges.containsKey(guid));
        assertEquals(edges.get(guid).size(), 1);
        assertEquals(edges.get(guid).get(0), "to-guid");
    }

    @Test
    public void testToLineageStruct_NullLineageInfo() throws AtlasBaseException {
        // Act
        Struct result = LineageUtils.toLineageStruct(null, typeRegistry);

        // Assert
        assertNotNull(result);
        assertTrue(result.getTypeName().startsWith(Constants.TEMP_STRUCT_NAME_PREFIX));
        assertNull(result.get("vertices"));
        assertNull(result.get("edges"));
    }

    @Test
    public void testToLineageStruct_EmptyLineageInfo() throws AtlasBaseException {
        // Arrange
        when(lineageInfo.getGuidEntityMap()).thenReturn(new HashMap<>());
        when(lineageInfo.getRelations()).thenReturn(new HashSet<>());
        when(lineageInfo.getLineageDirection()).thenReturn(AtlasLineageInfo.LineageDirection.INPUT);

        // Act
        Struct result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

        // Assert
        assertNotNull(result);
        assertTrue(result.getTypeName().startsWith(Constants.TEMP_STRUCT_NAME_PREFIX));
        Map<String, Struct> vertices = (Map<String, Struct>) result.get("vertices");
        Map<String, List<String>> edges = (Map<String, List<String>>) result.get("edges");
        assertTrue(vertices.isEmpty());
        assertTrue(edges.isEmpty());
    }

    @Test
    public void testToLineageStruct_NonDataSetEntity() throws AtlasBaseException {
        // Arrange - Entity that is not a dataset
        Map<String, AtlasEntityHeader> guidEntityMap = new HashMap<>();
        String guid = "test-guid";
        guidEntityMap.put(guid, entityHeader);

        when(lineageInfo.getGuidEntityMap()).thenReturn(guidEntityMap);
        when(lineageInfo.getRelations()).thenReturn(new HashSet<>());
        when(lineageInfo.getLineageDirection()).thenReturn(AtlasLineageInfo.LineageDirection.INPUT);
        when(entityHeader.getTypeName()).thenReturn("NonDataSetType");
        when(typeRegistry.getType("NonDataSetType")).thenReturn(entityType);
        when(entityType.getAllSuperTypes()).thenReturn(Collections.emptySet());

        // Act
        Struct result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

        // Assert
        assertNotNull(result);
        Map<String, Struct> vertices = (Map<String, Struct>) result.get("vertices");
        assertTrue(vertices.isEmpty()); // Should have no vertices since entity is not a dataset
    }

    @Test
    public void testToLineageStruct_EntityWithoutQualifiedName() throws AtlasBaseException {
        // Arrange - Entity without qualifiedName attribute
        Map<String, AtlasEntityHeader> guidEntityMap = new HashMap<>();
        String guid = "test-guid";
        guidEntityMap.put(guid, entityHeader);

        Set<AtlasLineageInfo.LineageRelation> relations = new HashSet<>();
        AtlasLineageInfo.LineageRelation relation = new AtlasLineageInfo.LineageRelation();
        relation.setFromEntityId("from-guid");
        relation.setToEntityId(guid);
        relations.add(relation);

        when(lineageInfo.getGuidEntityMap()).thenReturn(guidEntityMap);
        when(lineageInfo.getRelations()).thenReturn(relations);
        when(lineageInfo.getLineageDirection()).thenReturn(AtlasLineageInfo.LineageDirection.INPUT);
        when(entityHeader.getTypeName()).thenReturn("DataSetType");
        when(entityHeader.getStatus()).thenReturn(AtlasEntity.Status.ACTIVE);
        when(entityHeader.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME)).thenReturn(null); // No qualifiedName
        when(entityHeader.getDisplayText()).thenReturn("displayName");
        when(typeRegistry.getType("DataSetType")).thenReturn(entityType);
        when(entityType.getAllSuperTypes()).thenReturn(Collections.singleton("DataSet"));

        // Act
        Struct result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

        // Assert
        assertNotNull(result);
        Map<String, Struct> vertices = (Map<String, Struct>) result.get("vertices");
        assertFalse(vertices.isEmpty());
        Struct vertex = vertices.get(guid);
        // Should use displayText as fallback when qualifiedName is null
        assertEquals(vertex.get(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME), "displayName");
    }

    @Test
    public void testToLineageStruct_DeletedEntity() throws AtlasBaseException {
        // Arrange - Deleted entity
        Map<String, AtlasEntityHeader> guidEntityMap = new HashMap<>();
        String guid = "test-guid";
        guidEntityMap.put(guid, entityHeader);

        Set<AtlasLineageInfo.LineageRelation> relations = new HashSet<>();
        AtlasLineageInfo.LineageRelation relation = new AtlasLineageInfo.LineageRelation();
        relation.setFromEntityId("from-guid");
        relation.setToEntityId(guid);
        relations.add(relation);

        when(lineageInfo.getGuidEntityMap()).thenReturn(guidEntityMap);
        when(lineageInfo.getRelations()).thenReturn(relations);
        when(lineageInfo.getLineageDirection()).thenReturn(AtlasLineageInfo.LineageDirection.INPUT);
        when(entityHeader.getTypeName()).thenReturn("DataSetType");
        when(entityHeader.getStatus()).thenReturn(AtlasEntity.Status.DELETED); // Deleted status
        when(entityHeader.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME)).thenReturn("qualifiedName");
        when(entityHeader.getDisplayText()).thenReturn("displayName");
        when(typeRegistry.getType("DataSetType")).thenReturn(entityType);
        when(entityType.getAllSuperTypes()).thenReturn(Collections.singleton("DataSet"));

        // Act
        Struct result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

        // Assert
        assertNotNull(result);
        Map<String, Struct> vertices = (Map<String, Struct>) result.get("vertices");
        assertFalse(vertices.isEmpty());
        Struct vertex = vertices.get(guid);
        Struct vertexId = (Struct) vertex.get("vertexId");
        assertEquals(vertexId.get(Constants.ATTRIBUTE_NAME_STATE), "DELETED"); // Should show DELETED status
    }

    @Test
    public void testToLineageStruct_NullDirection() throws AtlasBaseException {
        // Arrange - Null lineage direction
        Map<String, AtlasEntityHeader> guidEntityMap = new HashMap<>();
        String guid = "test-guid";
        guidEntityMap.put(guid, entityHeader);

        Set<AtlasLineageInfo.LineageRelation> relations = new HashSet<>();
        AtlasLineageInfo.LineageRelation relation = new AtlasLineageInfo.LineageRelation();
        relation.setFromEntityId("from-guid");
        relation.setToEntityId(guid);
        relations.add(relation);

        when(lineageInfo.getGuidEntityMap()).thenReturn(guidEntityMap);
        when(lineageInfo.getRelations()).thenReturn(relations);
        when(lineageInfo.getLineageDirection()).thenReturn(null); // Null direction
        when(entityHeader.getTypeName()).thenReturn("DataSetType");
        when(entityHeader.getStatus()).thenReturn(AtlasEntity.Status.ACTIVE);
        when(entityHeader.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME)).thenReturn("qualifiedName");
        when(entityHeader.getDisplayText()).thenReturn("displayName");
        when(typeRegistry.getType("DataSetType")).thenReturn(entityType);
        when(entityType.getAllSuperTypes()).thenReturn(Collections.singleton("DataSet"));

        // Act
        Struct result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

        // Assert
        assertNotNull(result);
        Map<String, Struct> vertices = (Map<String, Struct>) result.get("vertices");
        assertFalse(vertices.isEmpty());
        // With null direction, no edges should be created
        Map<String, List<String>> edges = (Map<String, List<String>>) result.get("edges");
        assertTrue(edges.isEmpty());
    }

    @Test
    public void testToLineageStruct_EmptyRelations() throws AtlasBaseException {
        // Arrange - Empty relations
        Map<String, AtlasEntityHeader> guidEntityMap = new HashMap<>();
        String guid = "test-guid";
        guidEntityMap.put(guid, entityHeader);

        when(lineageInfo.getGuidEntityMap()).thenReturn(guidEntityMap);
        when(lineageInfo.getRelations()).thenReturn(new HashSet<>()); // Empty relations
        when(lineageInfo.getLineageDirection()).thenReturn(AtlasLineageInfo.LineageDirection.INPUT);
        when(entityHeader.getTypeName()).thenReturn("DataSetType");
        when(entityHeader.getStatus()).thenReturn(AtlasEntity.Status.ACTIVE);
        when(entityHeader.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME)).thenReturn("qualifiedName");
        when(entityHeader.getDisplayText()).thenReturn("displayName");
        when(typeRegistry.getType("DataSetType")).thenReturn(entityType);
        when(entityType.getAllSuperTypes()).thenReturn(Collections.singleton("DataSet"));

        // Act
        Struct result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

        // Assert
        assertNotNull(result);
        Map<String, Struct> vertices = (Map<String, Struct>) result.get("vertices");
        assertFalse(vertices.isEmpty());
        Map<String, List<String>> edges = (Map<String, List<String>>) result.get("edges");
        assertTrue(edges.isEmpty()); // Should have no edges with empty relations
    }

    @Test
    public void testToLineageStruct_MultipleEntities() throws AtlasBaseException {
        // Arrange - Multiple entities
        Map<String, AtlasEntityHeader> guidEntityMap = new HashMap<>();
        String guid1 = "test-guid-1";
        String guid2 = "test-guid-2";

        AtlasEntityHeader entityHeader2 = mock(AtlasEntityHeader.class);
        guidEntityMap.put(guid1, entityHeader);
        guidEntityMap.put(guid2, entityHeader2);

        Set<AtlasLineageInfo.LineageRelation> relations = new HashSet<>();
        AtlasLineageInfo.LineageRelation relation = new AtlasLineageInfo.LineageRelation();
        relation.setFromEntityId(guid1);
        relation.setToEntityId(guid2);
        relations.add(relation);

        when(lineageInfo.getGuidEntityMap()).thenReturn(guidEntityMap);
        when(lineageInfo.getRelations()).thenReturn(relations);
        when(lineageInfo.getLineageDirection()).thenReturn(AtlasLineageInfo.LineageDirection.OUTPUT);

        // Setup first entity
        when(entityHeader.getTypeName()).thenReturn("DataSetType");
        when(entityHeader.getStatus()).thenReturn(AtlasEntity.Status.ACTIVE);
        when(entityHeader.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME)).thenReturn("qualifiedName1");
        when(entityHeader.getDisplayText()).thenReturn("displayName1");

        // Setup second entity
        when(entityHeader2.getTypeName()).thenReturn("DataSetType");
        when(entityHeader2.getStatus()).thenReturn(AtlasEntity.Status.ACTIVE);
        when(entityHeader2.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME)).thenReturn("qualifiedName2");
        when(entityHeader2.getDisplayText()).thenReturn("displayName2");

        when(typeRegistry.getType("DataSetType")).thenReturn(entityType);
        when(entityType.getAllSuperTypes()).thenReturn(Collections.singleton("DataSet"));

        // Act
        Struct result = LineageUtils.toLineageStruct(lineageInfo, typeRegistry);

        // Assert
        assertNotNull(result);
        Map<String, Struct> vertices = (Map<String, Struct>) result.get("vertices");
        assertEquals(vertices.size(), 2);
        assertTrue(vertices.containsKey(guid1));
        assertTrue(vertices.containsKey(guid2));

        Map<String, List<String>> edges = (Map<String, List<String>>) result.get("edges");
        assertEquals(edges.size(), 1);
        assertTrue(edges.containsKey(guid1));
        assertEquals(edges.get(guid1).size(), 1);
        assertEquals(edges.get(guid1).get(0), guid2);
    }

    @Test
    public void testIsDataSet_True() throws AtlasBaseException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // Arrange
        when(typeRegistry.getType("DataSetType")).thenReturn(entityType);
        when(entityType.getAllSuperTypes()).thenReturn(Collections.singleton("DataSet"));

        Method isDataSetMethod = LineageUtils.class.getDeclaredMethod(
                "isDataSet", String.class, AtlasTypeRegistry.class);
        isDataSetMethod.setAccessible(true);

        // Act
        boolean result = (boolean) isDataSetMethod.invoke(null, "DataSetType", typeRegistry);

        // Assert
        assertTrue(result);
    }

    @Test
    public void testIsDataSet_False() throws AtlasBaseException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // Arrange
        when(typeRegistry.getType("NonDataSetType")).thenReturn(entityType);
        when(entityType.getAllSuperTypes()).thenReturn(Collections.emptySet());

        Method isDataSetMethod = LineageUtils.class.getDeclaredMethod(
                "isDataSet", String.class, AtlasTypeRegistry.class);
        isDataSetMethod.setAccessible(true);

        // Act
        boolean result = (boolean) isDataSetMethod.invoke(null, "NonDataSetType", typeRegistry);

        // Assert
        assertFalse(result);
    }

    @Test
    public void testIsDataSet_NonEntityType() throws AtlasBaseException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        // Arrange - Return a type that is not AtlasEntityType
        AtlasType nonEntityType = mock(AtlasType.class);
        when(typeRegistry.getType("NonEntityType")).thenReturn(nonEntityType);

        Method isDataSetMethod = LineageUtils.class.getDeclaredMethod(
                "isDataSet", String.class, AtlasTypeRegistry.class);
        isDataSetMethod.setAccessible(true);

        // Act
        boolean result = (boolean) isDataSetMethod.invoke(null, "NonDataSetType", typeRegistry);

        // Assert
        assertFalse(result);
    }
}
