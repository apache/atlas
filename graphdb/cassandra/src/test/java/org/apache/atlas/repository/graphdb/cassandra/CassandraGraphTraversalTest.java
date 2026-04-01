package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraphTraversal;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.BiPredicate;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraGraphTraversal.
 * Covers: V() start step, E() start step, result accessors, text predicates, anonymous traversal.
 */
public class CassandraGraphTraversalTest {

    private CassandraGraph mockGraph;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(CassandraGraph.class);
    }

    // ======================== V() Start Step ========================

    @Test
    public void testVWithVertexIds() {
        CassandraVertex v1 = new CassandraVertex("v1", mockGraph);
        CassandraVertex v2 = new CassandraVertex("v2", mockGraph);
        when(mockGraph.getVertex("v1")).thenReturn(v1);
        when(mockGraph.getVertex("v2")).thenReturn(v2);

        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, "v1", "v2");
        assertEquals(t.getAtlasVertexList().size(), 2);
    }

    @Test
    public void testVWithNoIds() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph);
        assertTrue(t.getAtlasVertexList().isEmpty());
    }

    @Test
    public void testVSkipsMissingVertices() {
        when(mockGraph.getVertex("v1")).thenReturn(null);
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, "v1");
        assertTrue(t.getAtlasVertexList().isEmpty());
    }

    @Test
    public void testVWithNullIds() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, (Object[]) null);
        assertTrue(t.getAtlasVertexList().isEmpty());
    }

    // ======================== E() Start Step ========================

    @Test
    public void testEWithEdgeIds() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        when(mockGraph.getEdge("e1")).thenReturn(e1);

        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, true, new Object[]{"e1"});
        assertEquals(t.getAtlasEdgeSet().size(), 1);
    }

    @Test
    public void testEWithNoIds() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, true, new Object[0]);
        assertTrue(t.getAtlasEdgeSet().isEmpty());
    }

    @Test
    public void testESkipsMissingEdges() {
        when(mockGraph.getEdge("e1")).thenReturn(null);
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, true, new Object[]{"e1"});
        assertTrue(t.getAtlasEdgeSet().isEmpty());
    }

    @Test
    public void testEWithNullIds() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, true, (Object[]) null);
        assertTrue(t.getAtlasEdgeSet().isEmpty());
    }

    // ======================== Result Accessors ========================

    @Test
    public void testGetAtlasVertexSet() {
        CassandraVertex v1 = new CassandraVertex("v1", mockGraph);
        when(mockGraph.getVertex("v1")).thenReturn(v1);

        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, "v1");
        Set<AtlasVertex> set = t.getAtlasVertexSet();
        assertEquals(set.size(), 1);
    }

    @Test
    public void testGetAtlasVertexMap() {
        CassandraVertex v1 = new CassandraVertex("v1", mockGraph);
        when(mockGraph.getVertex("v1")).thenReturn(v1);

        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, "v1");
        Map<String, Collection<AtlasVertex>> map = t.getAtlasVertexMap();
        assertFalse(map.isEmpty());
        assertTrue(map.containsKey("v1"));
    }

    @Test
    public void testGetAtlasEdgeMap() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        when(mockGraph.getEdge("e1")).thenReturn(e1);

        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, true, new Object[]{"e1"});
        Map<String, AtlasEdge> map = t.getAtlasEdgeMap();
        assertEquals(map.size(), 1);
        assertTrue(map.containsKey("e1"));
    }

    // ======================== Anonymous Traversal ========================

    @Test
    public void testStartAnonymousTraversal() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, new Object[0]);
        AtlasGraphTraversal anon = t.startAnonymousTraversal();
        assertNotNull(anon);
        assertTrue(anon instanceof CassandraGraphTraversal);
    }

    // ======================== Text Predicates ========================

    @Test
    public void testTextPredicateNotNull() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, new Object[0]);
        assertNotNull(t.textPredicate());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTextPredicateContains() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, new Object[0]);
        BiPredicate pred = t.textPredicate().contains();
        assertTrue(pred.test("Hello World", "world"));
        assertFalse(pred.test("Hello", "world"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTextPredicateContainsPrefix() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, new Object[0]);
        BiPredicate pred = t.textPredicate().containsPrefix();
        assertTrue(pred.test("Hello World", "hello"));
        assertFalse(pred.test("Hello World", "world"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTextPredicateContainsRegex() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, new Object[0]);
        BiPredicate pred = t.textPredicate().containsRegex();
        assertTrue(pred.test("Hello123", "Hello\\d+"));
        assertFalse(pred.test("Hello", "\\d+"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTextPredicatePrefix() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, new Object[0]);
        BiPredicate pred = t.textPredicate().prefix();
        assertTrue(pred.test("Hello World", "Hello"));
        assertFalse(pred.test("Hello World", "hello")); // case-sensitive
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTextPredicateRegex() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, new Object[0]);
        BiPredicate pred = t.textPredicate().regex();
        assertTrue(pred.test("abc123", "abc\\d+"));
        assertFalse(pred.test("abc", "\\d+"));
    }

    // ======================== Fluent no-ops ========================

    @Test
    public void testTextRegExReturnsSelf() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, new Object[0]);
        assertSame(t.textRegEx("key", ".*"), t);
    }

    @Test
    public void testTextContainsRegExReturnsSelf() {
        CassandraGraphTraversal t = new CassandraGraphTraversal(mockGraph, new Object[0]);
        assertSame(t.textContainsRegEx("value", ""), t);
    }
}
