package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraGraphQuery.
 * Covers: fluent API, predicate matching (has/in/operator), paging, child queries, or-queries,
 *         index routing (GUID lookup, QN+Type lookup, TypeDef lookup).
 */
public class CassandraGraphQueryTest {

    private CassandraGraph mockGraph;
    private IndexRepository mockIndexRepo;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(CassandraGraph.class);
        mockIndexRepo = mock(IndexRepository.class);
        when(mockGraph.getIndexRepository()).thenReturn(mockIndexRepo);
        // Return empty vertex cache by default
        when(mockGraph.getVertices()).thenReturn(Collections.emptyList());
    }

    // ======================== Fluent API ========================

    @Test
    public void testHasReturnsSelf() {
        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        assertSame(query.has("key", "value"), query);
    }

    @Test
    public void testInReturnsSelf() {
        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        assertSame(query.in("key", Arrays.asList("a", "b")), query);
    }

    @Test
    public void testOrderByReturnsSelf() {
        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        assertSame(query.orderBy("name", AtlasGraphQuery.SortOrder.ASC), query);
    }

    @Test
    public void testCreateChildQuery() {
        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        AtlasGraphQuery<CassandraVertex, CassandraEdge> child = query.createChildQuery();
        assertNotNull(child);
        assertTrue(child.isChildQuery());
        assertFalse(query.isChildQuery());
    }

    // ======================== GUID Index Lookup ========================

    @Test
    public void testGuidLookupHitsIndex() {
        CassandraVertex vertex = new CassandraVertex("v1", new HashMap<>(), mockGraph);
        vertex.setProperty("__guid", "guid-123");

        when(mockIndexRepo.lookupVertex("__guid_idx", "guid-123")).thenReturn("v1");
        when(mockGraph.getVertex("v1")).thenReturn(vertex);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result = query.has("__guid", "guid-123").vertices();
        List<AtlasVertex<CassandraVertex, CassandraEdge>> list = toList(result);
        assertEquals(list.size(), 1);
        assertEquals(((CassandraVertex) list.get(0)).getIdString(), "v1");
    }

    @Test
    public void testGuidLookupMissReturnsEmpty() {
        when(mockIndexRepo.lookupVertex("__guid_idx", "nonexistent")).thenReturn(null);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result = query.has("__guid", "nonexistent").vertices();
        assertTrue(toList(result).isEmpty());
    }

    @Test
    public void testGuidLookupWithPropertyPrefix() {
        CassandraVertex vertex = new CassandraVertex("v1", new HashMap<>(), mockGraph);
        // The predicate key is "Property.__guid", so the vertex must also store it under that key
        // for matchesAllPredicates to pass (index routing strips prefix, but predicate matching is literal)
        vertex.setProperty("Property.__guid", "guid-456");

        when(mockIndexRepo.lookupVertex("__guid_idx", "guid-456")).thenReturn("v1");
        when(mockGraph.getVertex("v1")).thenReturn(vertex);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result = query.has("Property.__guid", "guid-456").vertices();
        assertEquals(toList(result).size(), 1);
    }

    @Test
    public void testGuidIndexHitButVertexMissing() {
        when(mockIndexRepo.lookupVertex("__guid_idx", "guid-orphan")).thenReturn("v-deleted");
        when(mockGraph.getVertex("v-deleted")).thenReturn(null);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result = query.has("__guid", "guid-orphan").vertices();
        assertTrue(toList(result).isEmpty());
    }

    // ======================== QualifiedName + TypeName Composite Lookup ========================

    @Test
    public void testQnTypeLookup() {
        CassandraVertex vertex = new CassandraVertex("v1", new HashMap<>(), mockGraph);
        vertex.setProperty("qualifiedName", "default/table/users");
        vertex.setProperty("__typeName", "Table");

        when(mockIndexRepo.lookupVertex("qn_type_idx", "default/table/users:Table")).thenReturn("v1");
        when(mockGraph.getVertex("v1")).thenReturn(vertex);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.has("qualifiedName", "default/table/users").has("__typeName", "Table").vertices();
        assertEquals(toList(result).size(), 1);
    }

    @Test
    public void testQnTypeLookupMiss() {
        when(mockIndexRepo.lookupVertex("qn_type_idx", "missing:Table")).thenReturn(null);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.has("qualifiedName", "missing").has("__typeName", "Table").vertices();
        assertTrue(toList(result).isEmpty());
    }

    // ======================== TypeDef Lookup (VERTEX_TYPE + TYPENAME) ========================

    @Test
    public void testTypeDefLookup() {
        CassandraVertex vertex = new CassandraVertex("td1", new HashMap<>(), mockGraph);
        vertex.setProperty("__type", "typeSystem");
        vertex.setProperty("__type_name", "Table");

        when(mockIndexRepo.lookupVertex("type_typename_idx", "typeSystem:Table")).thenReturn("td1");
        when(mockGraph.getVertex("td1")).thenReturn(vertex);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.has("__type", "typeSystem").has("__type_name", "Table").vertices();
        assertEquals(toList(result).size(), 1);
    }

    // ======================== Property Index Lookup (1:N) ========================

    @Test
    public void testTypeCategoryLookup() {
        CassandraVertex v1 = new CassandraVertex("td1", new HashMap<>(), mockGraph);
        v1.setProperty("__type", "typeSystem");
        v1.setProperty("__type_category", "ENTITY");
        CassandraVertex v2 = new CassandraVertex("td2", new HashMap<>(), mockGraph);
        v2.setProperty("__type", "typeSystem");
        v2.setProperty("__type_category", "ENTITY");

        when(mockIndexRepo.lookupVertices("type_category_idx", "typeSystem:ENTITY"))
                .thenReturn(Arrays.asList("td1", "td2"));
        when(mockGraph.getVertex("td1")).thenReturn(v1);
        when(mockGraph.getVertex("td2")).thenReturn(v2);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.has("__type", "typeSystem").has("__type_category", "ENTITY").vertices();
        assertEquals(toList(result).size(), 2);
    }

    // ======================== Predicate Matching ========================

    @Test
    public void testHasPredicateMatchesExactValue() {
        CassandraVertex v1 = new CassandraVertex("v1", new HashMap<>(), mockGraph);
        v1.setProperty("__guid", "guid-1");
        v1.setProperty("__state", "ACTIVE");

        when(mockIndexRepo.lookupVertex("__guid_idx", "guid-1")).thenReturn("v1");
        when(mockGraph.getVertex("v1")).thenReturn(v1);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        // Query with __guid AND __state - index resolves by GUID, then __state predicate filters
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.has("__guid", "guid-1").has("__state", "ACTIVE").vertices();
        assertEquals(toList(result).size(), 1);
    }

    @Test
    public void testHasPredicateFiltersMismatch() {
        CassandraVertex v1 = new CassandraVertex("v1", new HashMap<>(), mockGraph);
        v1.setProperty("__guid", "guid-1");
        v1.setProperty("__state", "DELETED");

        when(mockIndexRepo.lookupVertex("__guid_idx", "guid-1")).thenReturn("v1");
        when(mockGraph.getVertex("v1")).thenReturn(v1);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.has("__guid", "guid-1").has("__state", "ACTIVE").vertices();
        assertTrue(toList(result).isEmpty()); // __state doesn't match
    }

    // ======================== addConditionsFrom ========================

    @Test
    public void testAddConditionsFrom() {
        CassandraGraphQuery source = new CassandraGraphQuery(mockGraph);
        source.has("__guid", "guid-1");

        CassandraGraphQuery target = new CassandraGraphQuery(mockGraph);
        target.addConditionsFrom(source);

        // Target should now have the GUID predicate
        when(mockIndexRepo.lookupVertex("__guid_idx", "guid-1")).thenReturn("v1");
        CassandraVertex v1 = new CassandraVertex("v1", new HashMap<>(), mockGraph);
        v1.setProperty("__guid", "guid-1");
        when(mockGraph.getVertex("v1")).thenReturn(v1);

        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result = target.vertices();
        assertEquals(toList(result).size(), 1);
    }

    // ======================== Or Queries ========================

    @Test
    @SuppressWarnings("unchecked")
    public void testOrQueries() {
        CassandraVertex v1 = new CassandraVertex("v1", new HashMap<>(), mockGraph);
        v1.setProperty("__guid", "guid-1");
        v1.setProperty("color", "red");

        // When orQueries is non-empty, index lookup is skipped; fallback uses getVertices(key, value)
        when(mockGraph.getVertices("__guid", "guid-1"))
                .thenReturn((Iterable) Collections.singletonList(v1));

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        CassandraGraphQuery child1 = (CassandraGraphQuery) query.createChildQuery();
        child1.has("color", "red");
        CassandraGraphQuery child2 = (CassandraGraphQuery) query.createChildQuery();
        child2.has("color", "blue");

        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.has("__guid", "guid-1").or(Arrays.asList(child1, child2)).vertices();
        assertEquals(toList(result).size(), 1); // matches "red" child
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testOrQueriesNoMatch() {
        CassandraVertex v1 = new CassandraVertex("v1", new HashMap<>(), mockGraph);
        v1.setProperty("__guid", "guid-1");
        v1.setProperty("color", "green");

        // When orQueries is non-empty, index lookup is skipped; fallback uses getVertices(key, value)
        when(mockGraph.getVertices("__guid", "guid-1"))
                .thenReturn((Iterable) Collections.singletonList(v1));

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        CassandraGraphQuery child1 = (CassandraGraphQuery) query.createChildQuery();
        child1.has("color", "red");
        CassandraGraphQuery child2 = (CassandraGraphQuery) query.createChildQuery();
        child2.has("color", "blue");

        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.has("__guid", "guid-1").or(Arrays.asList(child1, child2)).vertices();
        assertTrue(toList(result).isEmpty()); // "green" matches neither
    }

    // ======================== Paging ========================

    @Test
    public void testVerticesWithLimit() {
        CassandraVertex v1 = new CassandraVertex("td1", new HashMap<>(), mockGraph);
        v1.setProperty("__type", "typeSystem");
        v1.setProperty("__type_category", "ENTITY");
        CassandraVertex v2 = new CassandraVertex("td2", new HashMap<>(), mockGraph);
        v2.setProperty("__type", "typeSystem");
        v2.setProperty("__type_category", "ENTITY");
        CassandraVertex v3 = new CassandraVertex("td3", new HashMap<>(), mockGraph);
        v3.setProperty("__type", "typeSystem");
        v3.setProperty("__type_category", "ENTITY");

        when(mockIndexRepo.lookupVertices("type_category_idx", "typeSystem:ENTITY"))
                .thenReturn(Arrays.asList("td1", "td2", "td3"));
        when(mockGraph.getVertex("td1")).thenReturn(v1);
        when(mockGraph.getVertex("td2")).thenReturn(v2);
        when(mockGraph.getVertex("td3")).thenReturn(v3);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.has("__type", "typeSystem").has("__type_category", "ENTITY").vertices(2);
        assertEquals(toList(result).size(), 2);
    }

    @Test
    public void testVerticesWithOffsetAndLimit() {
        CassandraVertex v1 = new CassandraVertex("td1", new HashMap<>(), mockGraph);
        v1.setProperty("__type", "typeSystem");
        v1.setProperty("__type_category", "ENTITY");
        CassandraVertex v2 = new CassandraVertex("td2", new HashMap<>(), mockGraph);
        v2.setProperty("__type", "typeSystem");
        v2.setProperty("__type_category", "ENTITY");
        CassandraVertex v3 = new CassandraVertex("td3", new HashMap<>(), mockGraph);
        v3.setProperty("__type", "typeSystem");
        v3.setProperty("__type_category", "ENTITY");

        when(mockIndexRepo.lookupVertices("type_category_idx", "typeSystem:ENTITY"))
                .thenReturn(Arrays.asList("td1", "td2", "td3"));
        when(mockGraph.getVertex("td1")).thenReturn(v1);
        when(mockGraph.getVertex("td2")).thenReturn(v2);
        when(mockGraph.getVertex("td3")).thenReturn(v3);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.has("__type", "typeSystem").has("__type_category", "ENTITY").vertices(1, 1);
        assertEquals(toList(result).size(), 1);
    }

    // ======================== Vertex IDs ========================

    @Test
    public void testVertexIds() {
        CassandraVertex vertex = new CassandraVertex("v1", new HashMap<>(), mockGraph);
        vertex.setProperty("__guid", "guid-1");

        when(mockIndexRepo.lookupVertex("__guid_idx", "guid-1")).thenReturn("v1");
        when(mockGraph.getVertex("v1")).thenReturn(vertex);

        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<Object> result = query.has("__guid", "guid-1").vertexIds();
        List<Object> ids = toList(result);
        assertEquals(ids.size(), 1);
        assertEquals(ids.get(0), "v1");
    }

    // ======================== Edges Query ========================

    @Test
    public void testEdgesQueryReturnsEmpty() {
        // Edge queries via graph query are not supported (use vertex.getEdges() instead)
        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result = query.edges();
        assertTrue(toList(result).isEmpty());
    }

    // ======================== No Predicates ========================

    @Test
    public void testNoPredicatesReturnsEmpty() {
        CassandraGraphQuery query = new CassandraGraphQuery(mockGraph);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result = query.vertices();
        assertTrue(toList(result).isEmpty());
    }

    // ======================== Helper ========================

    private <T> List<T> toList(Iterable<T> iterable) {
        List<T> list = new ArrayList<>();
        iterable.forEach(list::add);
        return list;
    }
}
