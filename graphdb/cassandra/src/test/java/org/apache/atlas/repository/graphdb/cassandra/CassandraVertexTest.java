package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraVertex.
 * Covers: constructors, vertex label, edge retrieval, addProperty (Set), addListProperty, query().
 */
public class CassandraVertexTest {

    private CassandraGraph mockGraph;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(CassandraGraph.class);
    }

    // ======================== Constructor Tests ========================

    @Test
    public void testConstructorWithIdOnly() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        assertEquals(v.getIdString(), "v1");
        assertTrue(v.isNew());
        assertNull(v.getVertexLabel());
    }

    @Test
    public void testConstructorWithProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put("name", "test");
        CassandraVertex v = new CassandraVertex("v1", props, mockGraph);
        assertEquals(v.getProperty("name", String.class), "test");
        assertFalse(v.isNew());
    }

    @Test
    public void testConstructorWithLabel() {
        Map<String, Object> props = new HashMap<>();
        CassandraVertex v = new CassandraVertex("v1", "asset", props, mockGraph);
        assertEquals(v.getVertexLabel(), "asset");
    }

    // ======================== Vertex Label Tests ========================

    @Test
    public void testSetVertexLabel() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        v.setVertexLabel("typeSystem");
        assertEquals(v.getVertexLabel(), "typeSystem");
    }

    // ======================== GetV Test ========================

    @Test
    public void testGetVReturnsSelf() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        assertSame(v.getV(), v);
    }

    // ======================== Edge Retrieval Tests ========================

    @Test
    public void testGetEdgesDelegatesToGraph() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        List<AtlasEdge<CassandraVertex, CassandraEdge>> edges = new ArrayList<>();
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "knows")).thenReturn(edges);

        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result = v.getEdges(AtlasEdgeDirection.OUT, "knows");
        assertSame(result, edges);
        verify(mockGraph).getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "knows");
    }

    @Test
    public void testGetEdgesNoLabel() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        List<AtlasEdge<CassandraVertex, CassandraEdge>> edges = new ArrayList<>();
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.BOTH, null)).thenReturn(edges);

        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result = v.getEdges(AtlasEdgeDirection.BOTH);
        assertSame(result, edges);
    }

    @Test
    public void testGetEdgesMultipleLabels() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        CassandraEdge edge1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge edge2 = new CassandraEdge("e2", "v1", "v3", "likes", mockGraph);

        List<AtlasEdge<CassandraVertex, CassandraEdge>> knowsEdges = Collections.singletonList(edge1);
        List<AtlasEdge<CassandraVertex, CassandraEdge>> likesEdges = Collections.singletonList(edge2);

        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "knows")).thenReturn(knowsEdges);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "likes")).thenReturn(likesEdges);

        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result =
                v.getEdges(AtlasEdgeDirection.OUT, new String[]{"knows", "likes"});
        List<AtlasEdge<CassandraVertex, CassandraEdge>> resultList = new ArrayList<>();
        result.forEach(resultList::add);
        assertEquals(resultList.size(), 2);
    }

    @Test
    public void testGetEdgesEmptyLabelsArray() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        List<AtlasEdge<CassandraVertex, CassandraEdge>> allEdges = new ArrayList<>();
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, null)).thenReturn(allEdges);

        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result =
                v.getEdges(AtlasEdgeDirection.OUT, new String[]{});
        assertSame(result, allEdges);
    }

    @Test
    public void testGetEdgesNullLabelsArray() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        List<AtlasEdge<CassandraVertex, CassandraEdge>> allEdges = new ArrayList<>();
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, null)).thenReturn(allEdges);

        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result =
                v.getEdges(AtlasEdgeDirection.OUT, (String[]) null);
        assertSame(result, allEdges);
    }

    // ======================== GetInEdges With Exclusion ========================

    @Test
    public void testGetInEdgesExcludesLabels() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        CassandraEdge e1 = new CassandraEdge("e1", "v2", "v1", "keep", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v3", "v1", "exclude", mockGraph);
        CassandraEdge e3 = new CassandraEdge("e3", "v4", "v1", "keep", mockGraph);

        List<AtlasEdge<CassandraVertex, CassandraEdge>> allIn = Arrays.asList(e1, e2, e3);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.IN, null)).thenReturn(allIn);

        Set<CassandraEdge> result = v.getInEdges(new String[]{"exclude"});
        assertEquals(result.size(), 2);
        assertTrue(result.contains(e1));
        assertTrue(result.contains(e3));
        assertFalse(result.contains(e2));
    }

    @Test
    public void testGetInEdgesNoExclusions() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        CassandraEdge e1 = new CassandraEdge("e1", "v2", "v1", "label1", mockGraph);

        List<AtlasEdge<CassandraVertex, CassandraEdge>> allIn = Collections.singletonList(e1);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.IN, null)).thenReturn(allIn);

        Set<CassandraEdge> result = v.getInEdges(null);
        assertEquals(result.size(), 1);
    }

    // ======================== Edge Count / Has Edges ========================

    @Test
    public void testGetEdgesCount() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v1", "v3", "knows", mockGraph);
        List<AtlasEdge<CassandraVertex, CassandraEdge>> edges = Arrays.asList(e1, e2);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "knows")).thenReturn(edges);

        assertEquals(v.getEdgesCount(AtlasEdgeDirection.OUT, "knows"), 2);
    }

    @Test
    public void testGetEdgesCountZero() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "knows"))
                .thenReturn(Collections.emptyList());
        assertEquals(v.getEdgesCount(AtlasEdgeDirection.OUT, "knows"), 0);
    }

    @Test
    public void testHasEdgesTrue() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "knows"))
                .thenReturn(Collections.singletonList(e1));
        assertTrue(v.hasEdges(AtlasEdgeDirection.OUT, "knows"));
    }

    @Test
    public void testHasEdgesFalse() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "knows"))
                .thenReturn(Collections.emptyList());
        assertFalse(v.hasEdges(AtlasEdgeDirection.OUT, "knows"));
    }

    // ======================== addProperty (Set Behavior) ========================

    @Test
    public void testAddPropertyCreatesSet() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        v.addProperty("tags", "a");
        Object value = v.getProperties().get("tags");
        assertTrue(value instanceof Set);
        assertEquals(((Set<?>) value).size(), 1);
    }

    @Test
    public void testAddPropertyAccumulatesInSet() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        v.addProperty("tags", "a");
        v.addProperty("tags", "b");
        v.addProperty("tags", "c");
        Set<String> result = v.getMultiValuedSetProperty("tags", String.class);
        assertEquals(result.size(), 3);
        assertTrue(result.contains("a"));
        assertTrue(result.contains("b"));
        assertTrue(result.contains("c"));
    }

    @Test
    public void testAddPropertyConvertsExistingSingleToSet() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        v.setProperty("tags", "existing"); // single value
        v.addProperty("tags", "new");
        Object value = v.getProperties().get("tags");
        assertTrue(value instanceof Set);
        Set<?> set = (Set<?>) value;
        assertEquals(set.size(), 2);
        assertTrue(set.contains("existing"));
        assertTrue(set.contains("new"));
    }

    @Test
    public void testAddPropertyDeduplicates() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        v.addProperty("tags", "a");
        v.addProperty("tags", "a");
        Set<String> result = v.getMultiValuedSetProperty("tags", String.class);
        assertEquals(result.size(), 1);
    }

    // ======================== addListProperty (List Behavior) ========================

    @Test
    public void testAddListPropertyCreatesList() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        v.addListProperty("items", "a");
        Object value = v.getProperties().get("items");
        assertTrue(value instanceof List);
        assertEquals(((List<?>) value).size(), 1);
    }

    @Test
    public void testAddListPropertyAccumulatesInList() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        v.addListProperty("items", "a");
        v.addListProperty("items", "b");
        v.addListProperty("items", "c");
        List<String> result = v.getMultiValuedProperty("items", String.class);
        assertEquals(result.size(), 3);
    }

    @Test
    public void testAddListPropertyAllowsDuplicates() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        v.addListProperty("items", "a");
        v.addListProperty("items", "a");
        List<String> result = v.getMultiValuedProperty("items", String.class);
        assertEquals(result.size(), 2);
    }

    @Test
    public void testAddListPropertyConvertsExistingSingleToList() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        v.setProperty("items", "existing");
        v.addListProperty("items", "new");
        Object value = v.getProperties().get("items");
        assertTrue(value instanceof List);
        List<?> list = (List<?>) value;
        assertEquals(list.size(), 2);
    }

    // ======================== toString ========================

    @Test
    public void testToString() {
        CassandraVertex v = new CassandraVertex("v1", "myLabel", new HashMap<>(), mockGraph);
        String str = v.toString();
        assertTrue(str.contains("v1"));
        assertTrue(str.contains("myLabel"));
    }

    // ======================== Property Name Normalization ========================

    @Test
    public void testNormalize_typeQualifiedStripped() {
        // Type-qualified names like "Asset.name" should be stripped to just the attribute name
        assertEquals(VertexRepository.normalizePropertyName("Referenceable.qualifiedName"),
                "qualifiedName");
        assertEquals(VertexRepository.normalizePropertyName("Asset.name"), "name");
        assertEquals(VertexRepository.normalizePropertyName("Asset.description"), "description");
        assertEquals(VertexRepository.normalizePropertyName("Asset.connectorName"), "connectorName");
    }

    @Test
    public void testNormalize_doubleUnderscoreNeverNormalized() {
        // ALL properties starting with "__" are Atlas internal and must be preserved exactly.
        // This includes the __type. prefix used by Atlas's TypeDef system.
        assertEquals(VertexRepository.normalizePropertyName("__guid"), "__guid");
        assertEquals(VertexRepository.normalizePropertyName("__typeName"), "__typeName");
        assertEquals(VertexRepository.normalizePropertyName("__state"), "__state");
        assertEquals(VertexRepository.normalizePropertyName("__type"), "__type");
        assertEquals(VertexRepository.normalizePropertyName("__type_name"), "__type_name");
        assertEquals(VertexRepository.normalizePropertyName("__createdBy"), "__createdBy");
        assertEquals(VertexRepository.normalizePropertyName("__superTypeNames"), "__superTypeNames");
        assertEquals(VertexRepository.normalizePropertyName("__qualifiedNameHierarchy"),
                "__qualifiedNameHierarchy");
        // TypeDef metadata properties — __type. prefix must be preserved
        assertEquals(VertexRepository.normalizePropertyName("__type.atlas_operation"),
                "__type.atlas_operation");
        assertEquals(VertexRepository.normalizePropertyName("__type.atlas_operation.CREATE"),
                "__type.atlas_operation.CREATE");
        assertEquals(VertexRepository.normalizePropertyName("__type.Asset.certificateUpdatedAt"),
                "__type.Asset.certificateUpdatedAt");
    }

    @Test
    public void testNormalize_plainNamesKeptAsIs() {
        assertEquals(VertexRepository.normalizePropertyName("qualifiedName"), "qualifiedName");
        assertEquals(VertexRepository.normalizePropertyName("mongoDBCollectionIsCapped"),
                "mongoDBCollectionIsCapped");
        assertEquals(VertexRepository.normalizePropertyName("documentDBCollectionTotalIndexSize"),
                "documentDBCollectionTotalIndexSize");
    }

    @Test
    public void testNormalize_nullReturnsNull() {
        assertNull(VertexRepository.normalizePropertyName(null));
    }
}
