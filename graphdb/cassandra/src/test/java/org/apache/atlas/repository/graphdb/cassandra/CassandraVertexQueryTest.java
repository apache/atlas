package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraVertexQuery.
 * Covers: fluent API, direction/label filtering, has predicates, adjacent vertices, count, limit.
 */
public class CassandraVertexQueryTest {

    private CassandraGraph mockGraph;
    private CassandraVertex sourceVertex;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(CassandraGraph.class);
        sourceVertex = new CassandraVertex("v1", mockGraph);
    }

    // ======================== Fluent API ========================

    @Test
    public void testFluentApiReturnsThis() {
        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        assertSame(query.direction(AtlasEdgeDirection.OUT), query);
        assertSame(query.label("knows"), query);
        assertSame(query.has("state", "ACTIVE"), query);
    }

    // ======================== Edges By Direction ========================

    @Test
    public void testEdgesOutDirection() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, (String) null))
                .thenReturn(Collections.singletonList(e1));

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result = query.direction(AtlasEdgeDirection.OUT).edges();
        List<AtlasEdge<CassandraVertex, CassandraEdge>> list = toList(result);
        assertEquals(list.size(), 1);
    }

    @Test
    public void testEdgesWithLabel() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.BOTH, "knows"))
                .thenReturn(Collections.singletonList(e1));

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result = query.label("knows").edges();
        assertEquals(toList(result).size(), 1);
    }

    @Test
    public void testEdgesWithMultipleLabels() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v1", "v3", "likes", mockGraph);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.BOTH, "knows"))
                .thenReturn(Collections.singletonList(e1));
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.BOTH, "likes"))
                .thenReturn(Collections.singletonList(e2));

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result = query.label("knows", "likes").edges();
        assertEquals(toList(result).size(), 2);
    }

    // ======================== Has Predicate Filtering ========================

    @Test
    public void testEdgesFilteredByHasPredicate() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        e1.setProperty("state", "ACTIVE");
        CassandraEdge e2 = new CassandraEdge("e2", "v1", "v3", "knows", mockGraph);
        e2.setProperty("state", "DELETED");

        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "knows"))
                .thenReturn(Arrays.asList(e1, e2));

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result =
                query.direction(AtlasEdgeDirection.OUT).label("knows").has("state", "ACTIVE").edges();
        List<AtlasEdge<CassandraVertex, CassandraEdge>> list = toList(result);
        assertEquals(list.size(), 1);
        assertEquals(list.get(0).getProperty("state", String.class), "ACTIVE");
    }

    @Test
    public void testEdgesWithMultipleHasPredicates() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        e1.setProperty("state", "ACTIVE");
        e1.setProperty("weight", 5);
        CassandraEdge e2 = new CassandraEdge("e2", "v1", "v3", "knows", mockGraph);
        e2.setProperty("state", "ACTIVE");
        e2.setProperty("weight", 10);

        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, (String) null))
                .thenReturn(Arrays.asList(e1, e2));

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result =
                query.direction(AtlasEdgeDirection.OUT).has("state", "ACTIVE").has("weight", 5).edges();
        assertEquals(toList(result).size(), 1);
    }

    // ======================== Edge Limit ========================

    @Test
    public void testEdgesWithLimit() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v1", "v3", "knows", mockGraph);
        CassandraEdge e3 = new CassandraEdge("e3", "v1", "v4", "knows", mockGraph);

        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, (String) null))
                .thenReturn(Arrays.asList(e1, e2, e3));

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result =
                query.direction(AtlasEdgeDirection.OUT).edges(2);
        assertEquals(toList(result).size(), 2);
    }

    // ======================== Adjacent Vertices ========================

    @Test
    public void testAdjacentVerticesOutDirection() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraVertex v2 = new CassandraVertex("v2", mockGraph);

        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, (String) null))
                .thenReturn(Collections.singletonList(e1));
        when(mockGraph.getVertex("v2")).thenReturn(v2);

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.direction(AtlasEdgeDirection.OUT).vertices();
        List<AtlasVertex<CassandraVertex, CassandraEdge>> list = toList(result);
        assertEquals(list.size(), 1);
        assertEquals(((CassandraVertex) list.get(0)).getIdString(), "v2");
    }

    @Test
    public void testAdjacentVerticesInDirection() {
        CassandraEdge e1 = new CassandraEdge("e1", "v2", "v1", "knows", mockGraph);
        CassandraVertex v2 = new CassandraVertex("v2", mockGraph);

        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.IN, (String) null))
                .thenReturn(Collections.singletonList(e1));
        when(mockGraph.getVertex("v2")).thenReturn(v2);

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.direction(AtlasEdgeDirection.IN).vertices();
        List<AtlasVertex<CassandraVertex, CassandraEdge>> list = toList(result);
        assertEquals(list.size(), 1);
        assertEquals(((CassandraVertex) list.get(0)).getIdString(), "v2");
    }

    @Test
    public void testAdjacentVerticesBothDirection() {
        CassandraEdge eOut = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge eIn = new CassandraEdge("e2", "v3", "v1", "knows", mockGraph);
        CassandraVertex v2 = new CassandraVertex("v2", mockGraph);
        CassandraVertex v3 = new CassandraVertex("v3", mockGraph);

        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.BOTH, (String) null))
                .thenReturn(Arrays.asList(eOut, eIn));
        when(mockGraph.getVertex("v2")).thenReturn(v2);
        when(mockGraph.getVertex("v3")).thenReturn(v3);

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result = query.vertices();
        assertEquals(toList(result).size(), 2);
    }

    @Test
    public void testAdjacentVerticesWithLimit() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v1", "v3", "knows", mockGraph);
        CassandraVertex v2 = new CassandraVertex("v2", mockGraph);
        CassandraVertex v3 = new CassandraVertex("v3", mockGraph);

        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, (String) null))
                .thenReturn(Arrays.asList(e1, e2));
        when(mockGraph.getVertex("v2")).thenReturn(v2);
        when(mockGraph.getVertex("v3")).thenReturn(v3);

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> result =
                query.direction(AtlasEdgeDirection.OUT).vertices(1);
        assertEquals(toList(result).size(), 1);
    }

    // ======================== Count ========================

    @Test
    public void testCount() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v1", "v3", "knows", mockGraph);
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, (String) null))
                .thenReturn(Arrays.asList(e1, e2));

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        assertEquals(query.direction(AtlasEdgeDirection.OUT).count(), 2);
    }

    @Test
    public void testCountZero() {
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, (String) null))
                .thenReturn(Collections.emptyList());

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        assertEquals(query.direction(AtlasEdgeDirection.OUT).count(), 0);
    }

    // ======================== Empty Results ========================

    @Test
    public void testNoEdgesReturnsEmpty() {
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.BOTH, (String) null))
                .thenReturn(Collections.emptyList());

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> result = query.edges();
        assertTrue(toList(result).isEmpty());
    }

    @Test
    public void testNoAdjacentVerticesReturnsEmpty() {
        when(mockGraph.getEdgesForVertex("v1", AtlasEdgeDirection.BOTH, (String) null))
                .thenReturn(Collections.emptyList());

        CassandraVertexQuery query = new CassandraVertexQuery(mockGraph, sourceVertex);
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
