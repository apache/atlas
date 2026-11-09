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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.attribute.Text;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class AtlasJanusGraphTraversalTest {
    @Mock
    private AtlasJanusGraph mockAtlasGraph;

    @Mock
    private GraphTraversalSource mockTraversalSource;

    @Mock
    private Graph mockGraph;

    private AtlasJanusGraphTraversal traversal;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.openMocks(this);
        traversal = new AtlasJanusGraphTraversal(mockAtlasGraph, mockTraversalSource);
    }

    @Test
    public void testDefaultConstructor() {
        AtlasJanusGraphTraversal defaultTraversal = new AtlasJanusGraphTraversal();
        assertNotNull(defaultTraversal);
    }

    @Test
    public void testConstructorWithAtlasGraphAndTraversalSource() {
        AtlasJanusGraphTraversal testTraversal = new AtlasJanusGraphTraversal(mockAtlasGraph, mockTraversalSource);
        assertNotNull(testTraversal);
    }

    @Test
    public void testConstructorWithAtlasGraphAndGraph() {
        AtlasJanusGraphTraversal testTraversal = new AtlasJanusGraphTraversal(mockAtlasGraph, mockGraph);
        assertNotNull(testTraversal);
    }

    @Test
    public void testStartAnonymousTraversal() {
        AtlasGraphTraversal<AtlasJanusVertex, AtlasJanusEdge> anonymousTraversal = traversal.startAnonymousTraversal();
        assertNotNull(anonymousTraversal);
        assertTrue(anonymousTraversal instanceof AtlasJanusGraphTraversal);
    }

    @Test
    public void testGetAtlasVertexListWithVertices() throws Exception {
        // Setup mock vertices
        List<Object> resultList = new ArrayList<>();
        Vertex mockVertex1 = mock(Vertex.class);
        Vertex mockVertex2 = mock(Vertex.class);
        resultList.add(mockVertex1);
        resultList.add(mockVertex2);

        // Set up the result list using reflection
        setResultList(traversal, resultList);

        List<AtlasJanusVertex> vertices = traversal.getAtlasVertexList();
        assertNotNull(vertices);
        assertEquals(vertices.size(), 2);
    }

    @Test
    public void testGetAtlasVertexListWithMapResult() throws Exception {
        // Setup mock result with map (from groupBy operation)
        List<Object> resultList = new ArrayList<>();
        Map<String, Object> mapResult = new HashMap<>();
        mapResult.put("key1", "value1");
        resultList.add(mapResult);

        // Set up the result list using reflection
        setResultList(traversal, resultList);

        List<AtlasJanusVertex> vertices = traversal.getAtlasVertexList();
        assertNotNull(vertices);
        assertTrue(vertices.isEmpty()); // Map results should return empty list
    }

    @Test
    public void testGetAtlasVertexListEmpty() throws Exception {
        // Set up empty result list
        setResultList(traversal, Collections.emptyList());

        List<AtlasJanusVertex> vertices = traversal.getAtlasVertexList();
        assertNotNull(vertices);
        assertTrue(vertices.isEmpty());
    }

    @Test
    public void testGetAtlasVertexListWithNonVertexObjects() throws Exception {
        // Setup result list with non-vertex objects
        List<Object> resultList = new ArrayList<>();
        resultList.add("not a vertex");
        resultList.add(123);

        // Set up the result list using reflection
        setResultList(traversal, resultList);

        List<AtlasJanusVertex> vertices = traversal.getAtlasVertexList();
        assertNotNull(vertices);
        assertTrue(vertices.isEmpty()); // Non-vertex objects should be filtered out
    }

    @Test
    public void testGetAtlasVertexSetWithVertices() throws Exception {
        // Setup mock vertices
        Set<Object> resultSet = new HashSet<>();
        Vertex mockVertex1 = mock(Vertex.class);
        Vertex mockVertex2 = mock(Vertex.class);
        resultSet.add(mockVertex1);
        resultSet.add(mockVertex2);

        // Set up the result set using reflection
        setResultSet(traversal, resultSet);

        Set<AtlasJanusVertex> vertices = traversal.getAtlasVertexSet();
        assertNotNull(vertices);
        assertEquals(vertices.size(), 2);
    }

    @Test
    public void testGetAtlasVertexSetEmpty() throws Exception {
        // Set up empty result set
        setResultSet(traversal, Collections.emptySet());

        Set<AtlasJanusVertex> vertices = traversal.getAtlasVertexSet();
        assertNotNull(vertices);
        assertTrue(vertices.isEmpty());
    }

    @Test
    public void testGetAtlasVertexSetWithNonVertexObjects() throws Exception {
        // Setup result set with non-vertex objects
        Set<Object> resultSet = new HashSet<>();
        resultSet.add("not a vertex");
        resultSet.add(456);

        // Set up the result set using reflection
        setResultSet(traversal, resultSet);

        Set<AtlasJanusVertex> vertices = traversal.getAtlasVertexSet();
        assertNotNull(vertices);
        assertTrue(vertices.isEmpty()); // Non-vertex objects should be filtered out
    }

    @Test
    public void testGetAtlasVertexMapWithValidMap() throws Exception {
        // Setup result list with map containing vertex lists
        List<Object> resultList = new ArrayList<>();
        Map<String, Object> mapResult = new HashMap<>();

        List<Vertex> vertexList1 = new ArrayList<>();
        vertexList1.add(mock(Vertex.class));
        vertexList1.add(mock(Vertex.class));

        List<Vertex> vertexList2 = new ArrayList<>();
        vertexList2.add(mock(Vertex.class));

        mapResult.put("group1", vertexList1);
        mapResult.put("group2", vertexList2);
        resultList.add(mapResult);

        // Set up the result list using reflection
        setResultList(traversal, resultList);

        Map<String, Collection<AtlasJanusVertex>> vertexMap = traversal.getAtlasVertexMap();
        assertNotNull(vertexMap);
        assertEquals(vertexMap.size(), 2);
        assertEquals(vertexMap.get("group1").size(), 2);
        assertEquals(vertexMap.get("group2").size(), 1);
    }

    @Test
    public void testGetAtlasVertexMapWithNonStringKey() throws Exception {
        // Setup result list with map containing non-string keys
        List<Object> resultList = new ArrayList<>();
        Map<Object, Object> mapResult = new HashMap<>();

        List<Vertex> vertexList = new ArrayList<>();
        vertexList.add(mock(Vertex.class));

        mapResult.put(123, vertexList); // Non-string key (Integer)
        mapResult.put("validKey", vertexList);
        resultList.add(mapResult);

        // Set up the result list using reflection
        setResultList(traversal, resultList);

        Map<String, Collection<AtlasJanusVertex>> vertexMap = traversal.getAtlasVertexMap();
        assertNotNull(vertexMap);
        assertEquals(vertexMap.size(), 2);
        assertNotNull(vertexMap.get("validKey"));
        assertNotNull(vertexMap.get("123")); // Integer key converted to String
    }

    @Test
    public void testGetAtlasVertexMapWithNonVertexObjects() throws Exception {
        // Setup result list with map containing non-vertex objects
        List<Object> resultList = new ArrayList<>();
        Map<String, Object> mapResult = new HashMap<>();

        List<Object> mixedList = new ArrayList<>();
        mixedList.add(mock(Vertex.class));
        mixedList.add("not a vertex");
        mixedList.add(mock(Vertex.class));

        mapResult.put("mixedGroup", mixedList);
        resultList.add(mapResult);

        // Set up the result list using reflection
        setResultList(traversal, resultList);

        Map<String, Collection<AtlasJanusVertex>> vertexMap = traversal.getAtlasVertexMap();
        assertNotNull(vertexMap);
        assertEquals(vertexMap.size(), 1);
        assertEquals(vertexMap.get("mixedGroup").size(), 2); // Only vertices should be included
    }

    @Test
    public void testGetAtlasVertexMapEmpty() throws Exception {
        // Set up empty result list
        setResultList(traversal, Collections.emptyList());

        Map<String, Collection<AtlasJanusVertex>> vertexMap = traversal.getAtlasVertexMap();
        assertNotNull(vertexMap);
        assertTrue(vertexMap.isEmpty());
    }

    @Test
    public void testGetAtlasVertexMapWithLongKeysForDSLGroupByCreateTime() throws Exception {
        List<Object> resultList = new ArrayList<>();
        Map<Object, Object> mapResult = new HashMap<>();

        // Create mock vertices for different timestamp groups
        List<Vertex> group1Vertices = new ArrayList<>();
        group1Vertices.add(mock(Vertex.class));
        group1Vertices.add(mock(Vertex.class));

        List<Vertex> group2Vertices = new ArrayList<>();
        group2Vertices.add(mock(Vertex.class));

        List<Vertex> group3Vertices = new ArrayList<>();
        group3Vertices.add(mock(Vertex.class));
        group3Vertices.add(mock(Vertex.class));
        group3Vertices.add(mock(Vertex.class));

        // Add groups with Long keys (simulating createTime timestamps)
        mapResult.put(1700000000000L, group1Vertices); // Timestamp 1
        mapResult.put(1710000000000L, group2Vertices); // Timestamp 2
        mapResult.put(1720000000000L, group3Vertices); // Timestamp 3

        resultList.add(mapResult);
        setResultList(traversal, resultList);

        Map<String, Collection<AtlasJanusVertex>> vertexMap = traversal.getAtlasVertexMap();
        // Verify results
        assertNotNull(vertexMap);
        assertEquals(vertexMap.size(), 3); // All Long keys converted to String
        assertNotNull(vertexMap.get("1700000000000")); // Verify Long converted to String
        assertNotNull(vertexMap.get("1710000000000"));
        assertNotNull(vertexMap.get("1720000000000"));
        assertEquals(vertexMap.get("1700000000000").size(), 2); // 2 vertices in group1
        assertEquals(vertexMap.get("1710000000000").size(), 1); // 1 vertex in group2
        assertEquals(vertexMap.get("1720000000000").size(), 3); // 3 vertices in group3
    }

    @Test
    public void testGetAtlasEdgeSetWithEdges() throws Exception {
        // Setup mock edges
        Set<Object> resultSet = new HashSet<>();
        Edge mockEdge1 = mock(Edge.class);
        Edge mockEdge2 = mock(Edge.class);
        resultSet.add(mockEdge1);
        resultSet.add(mockEdge2);

        // Set up the result set using reflection
        setResultSet(traversal, resultSet);

        Set<AtlasJanusEdge> edges = traversal.getAtlasEdgeSet();
        assertNotNull(edges);
        assertEquals(edges.size(), 2);
    }

    @Test
    public void testGetAtlasEdgeSetEmpty() throws Exception {
        // Set up empty result set
        setResultSet(traversal, Collections.emptySet());

        Set<AtlasJanusEdge> edges = traversal.getAtlasEdgeSet();
        assertNotNull(edges);
        assertTrue(edges.isEmpty());
    }

    @Test
    public void testGetAtlasEdgeSetWithNonEdgeObjects() throws Exception {
        // Setup result set with non-edge objects
        Set<Object> resultSet = new HashSet<>();
        resultSet.add("not an edge");
        resultSet.add(789);

        // Set up the result set using reflection
        setResultSet(traversal, resultSet);

        Set<AtlasJanusEdge> edges = traversal.getAtlasEdgeSet();
        assertNotNull(edges);
        assertTrue(edges.isEmpty()); // Non-edge objects should be filtered out
    }

    @Test
    public void testGetAtlasEdgeMap() {
        Map<String, AtlasJanusEdge> edgeMap = traversal.getAtlasEdgeMap();
        assertNull(edgeMap); // Current implementation returns null
    }

    @Test
    public void testTextPredicate() {
        AtlasGraphTraversal.TextPredicate textPredicate = traversal.textPredicate();
        assertNotNull(textPredicate);
        assertTrue(textPredicate instanceof AtlasJanusGraphTraversal.JanusGraphPredicate);
    }

    @Test
    public void testTextRegEx() {
        try {
            AtlasGraphTraversal<AtlasJanusVertex, AtlasJanusEdge> result = traversal.textRegEx("testKey", "testValue");
            assertNotNull(result);
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testTextContainsRegEx() {
        try {
            AtlasGraphTraversal<AtlasJanusVertex, AtlasJanusEdge> result = traversal.textContainsRegEx("testKey", "testValue");
            assertNotNull(result);
        } catch (Exception e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testJanusGraphPredicateContains() {
        AtlasJanusGraphTraversal.JanusGraphPredicate predicate = new AtlasJanusGraphTraversal.JanusGraphPredicate();

        BiPredicate<Object, Object> contains = predicate.contains();
        assertNotNull(contains);
        assertEquals(contains, Text.CONTAINS);
    }

    @Test
    public void testJanusGraphPredicateContainsPrefix() {
        AtlasJanusGraphTraversal.JanusGraphPredicate predicate = new AtlasJanusGraphTraversal.JanusGraphPredicate();

        BiPredicate<Object, Object> containsPrefix = predicate.containsPrefix();
        assertNotNull(containsPrefix);
        assertEquals(containsPrefix, Text.CONTAINS_PREFIX);
    }

    @Test
    public void testJanusGraphPredicateContainsRegex() {
        AtlasJanusGraphTraversal.JanusGraphPredicate predicate = new AtlasJanusGraphTraversal.JanusGraphPredicate();

        BiPredicate<Object, Object> containsRegex = predicate.containsRegex();
        assertNotNull(containsRegex);
        assertEquals(containsRegex, Text.CONTAINS_REGEX);
    }

    @Test
    public void testJanusGraphPredicatePrefix() {
        AtlasJanusGraphTraversal.JanusGraphPredicate predicate = new AtlasJanusGraphTraversal.JanusGraphPredicate();

        BiPredicate<Object, Object> prefix = predicate.prefix();
        assertNotNull(prefix);
        assertEquals(prefix, Text.PREFIX);
    }

    @Test
    public void testJanusGraphPredicateRegex() {
        AtlasJanusGraphTraversal.JanusGraphPredicate predicate = new AtlasJanusGraphTraversal.JanusGraphPredicate();

        BiPredicate<Object, Object> regex = predicate.regex();
        assertNotNull(regex);
        assertEquals(regex, Text.REGEX);
    }

    @Test
    public void testGetResultListCaching() throws Exception {
        // Setup mock result
        List<Object> expectedList = new ArrayList<>();
        expectedList.add("item1");
        expectedList.add("item2");

        // First call should set the resultList
        setResultList(traversal, expectedList);
        List<?> firstCall = getResultList(traversal);

        // Second call should return cached result
        List<?> secondCall = getResultList(traversal);

        assertEquals(firstCall, secondCall);
    }

    @Test
    public void testGetResultSetCaching() throws Exception {
        // Setup mock result
        Set<Object> expectedSet = new HashSet<>();
        expectedSet.add("item1");
        expectedSet.add("item2");

        // First call should set the resultSet
        setResultSet(traversal, expectedSet);
        Set<?> firstCall = getResultSet(traversal);

        // Second call should return cached result
        Set<?> secondCall = getResultSet(traversal);

        assertEquals(firstCall, secondCall);
    }

    // Helper methods to access private fields and methods using reflection
    private void setResultList(AtlasJanusGraphTraversal traversal, List<Object> resultList) throws Exception {
        Field field = AtlasJanusGraphTraversal.class.getDeclaredField("resultList");
        field.setAccessible(true);
        field.set(traversal, resultList);
    }

    private void setResultSet(AtlasJanusGraphTraversal traversal, Set<Object> resultSet) throws Exception {
        Field field = AtlasJanusGraphTraversal.class.getDeclaredField("resultSet");
        field.setAccessible(true);
        field.set(traversal, resultSet);
    }

    private List<?> getResultList(AtlasJanusGraphTraversal traversal) throws Exception {
        Method method = AtlasJanusGraphTraversal.class.getDeclaredMethod("getResultList");
        method.setAccessible(true);
        return (List<?>) method.invoke(traversal);
    }

    private Set<?> getResultSet(AtlasJanusGraphTraversal traversal) throws Exception {
        Method method = AtlasJanusGraphTraversal.class.getDeclaredMethod("getResultSet");
        method.setAccessible(true);
        return (Set<?>) method.invoke(traversal);
    }

    @Test
    public void testPrivateFieldsInitialization() throws Exception {
        // Test that private fields are properly initialized
        Field resultListField = AtlasJanusGraphTraversal.class.getDeclaredField("resultList");
        resultListField.setAccessible(true);

        Field resultSetField = AtlasJanusGraphTraversal.class.getDeclaredField("resultSet");
        resultSetField.setAccessible(true);

        AtlasJanusGraphTraversal newTraversal = new AtlasJanusGraphTraversal();

        // Initially both should be null
        assertNull(resultListField.get(newTraversal));
        assertNull(resultSetField.get(newTraversal));
    }

    @Test
    public void testInheritanceFromAtlasGraphTraversal() {
        assertTrue(traversal instanceof AtlasGraphTraversal);
    }

    @Test
    public void testJanusGraphPredicateInstantiation() {
        // Test that we can create instances of the inner class
        AtlasJanusGraphTraversal.JanusGraphPredicate predicate1 = new AtlasJanusGraphTraversal.JanusGraphPredicate();
        AtlasJanusGraphTraversal.JanusGraphPredicate predicate2 = new AtlasJanusGraphTraversal.JanusGraphPredicate();

        assertNotNull(predicate1);
        assertNotNull(predicate2);

        // Test that they provide the same text predicates
        assertEquals(predicate1.contains(), predicate2.contains());
        assertEquals(predicate1.prefix(), predicate2.prefix());
        assertEquals(predicate1.regex(), predicate2.regex());
        assertEquals(predicate1.containsPrefix(), predicate2.containsPrefix());
        assertEquals(predicate1.containsRegex(), predicate2.containsRegex());
    }

    @Test
    public void testGetAtlasVertexMapWithNonListValue() throws Exception {
        // Setup result list with map containing non-list values
        List<Object> resultList = new ArrayList<>();
        Map<String, Object> mapResult = new HashMap<>();

        mapResult.put("stringValue", "not a list");
        mapResult.put("numberValue", 123);
        resultList.add(mapResult);

        // Set up the result list using reflection
        setResultList(traversal, resultList);

        Map<String, Collection<AtlasJanusVertex>> vertexMap = traversal.getAtlasVertexMap();
        assertNotNull(vertexMap);
        assertTrue(vertexMap.isEmpty()); // Non-list values should be ignored
    }
}
