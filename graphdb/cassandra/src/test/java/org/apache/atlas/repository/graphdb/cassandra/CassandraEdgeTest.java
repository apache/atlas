package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraEdge.
 * Covers: constructors, vertex resolution, label, properties, state.
 */
public class CassandraEdgeTest {

    private CassandraGraph mockGraph;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(CassandraGraph.class);
    }

    // ======================== Constructor Tests ========================

    @Test
    public void testConstructorBasic() {
        CassandraEdge edge = new CassandraEdge("e1", "out1", "in1", "knows", mockGraph);
        assertEquals(edge.getIdString(), "e1");
        assertEquals(edge.getOutVertexId(), "out1");
        assertEquals(edge.getInVertexId(), "in1");
        assertEquals(edge.getLabel(), "knows");
        assertTrue(edge.isNew());
    }

    @Test
    public void testConstructorWithProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put("weight", 0.5);
        CassandraEdge edge = new CassandraEdge("e1", "out1", "in1", "knows", props, mockGraph);
        assertEquals(edge.getProperty("weight", Double.class), 0.5);
        assertFalse(edge.isNew()); // properties constructor = loaded from DB
    }

    // ======================== Vertex Resolution Tests ========================

    @Test
    public void testGetInVertex() {
        CassandraVertex inVertex = new CassandraVertex("in1", mockGraph);
        when(mockGraph.getVertex("in1")).thenReturn(inVertex);

        CassandraEdge edge = new CassandraEdge("e1", "out1", "in1", "knows", mockGraph);
        AtlasVertex<CassandraVertex, CassandraEdge> result = edge.getInVertex();
        assertSame(result, inVertex);
        verify(mockGraph).getVertex("in1");
    }

    @Test
    public void testGetOutVertex() {
        CassandraVertex outVertex = new CassandraVertex("out1", mockGraph);
        when(mockGraph.getVertex("out1")).thenReturn(outVertex);

        CassandraEdge edge = new CassandraEdge("e1", "out1", "in1", "knows", mockGraph);
        AtlasVertex<CassandraVertex, CassandraEdge> result = edge.getOutVertex();
        assertSame(result, outVertex);
        verify(mockGraph).getVertex("out1");
    }

    @Test
    public void testGetInVertexReturnsNullWhenNotFound() {
        when(mockGraph.getVertex("in1")).thenReturn(null);
        CassandraEdge edge = new CassandraEdge("e1", "out1", "in1", "knows", mockGraph);
        assertNull(edge.getInVertex());
    }

    // ======================== Label Tests ========================

    @Test
    public void testGetLabel() {
        CassandraEdge edge = new CassandraEdge("e1", "out1", "in1", "__Process.inputs", mockGraph);
        assertEquals(edge.getLabel(), "__Process.inputs");
    }

    // ======================== GetE Test ========================

    @Test
    public void testGetEReturnsSelf() {
        CassandraEdge edge = new CassandraEdge("e1", "out1", "in1", "knows", mockGraph);
        assertSame(edge.getE(), edge);
    }

    // ======================== Property Tests (inherited from CassandraElement) ========================

    @Test
    public void testEdgeProperties() {
        CassandraEdge edge = new CassandraEdge("e1", "out1", "in1", "knows", mockGraph);
        edge.setProperty("state", "ACTIVE");
        edge.setProperty("created_at", 1700000000L);
        assertEquals(edge.getProperty("state", String.class), "ACTIVE");
        assertEquals(edge.getProperty("created_at", Long.class), Long.valueOf(1700000000L));
    }

    // ======================== State Tests ========================

    @Test
    public void testEdgeMarkDeleted() {
        CassandraEdge edge = new CassandraEdge("e1", "out1", "in1", "knows", mockGraph);
        edge.markDeleted();
        assertTrue(edge.isDeleted());
        assertFalse(edge.exists());
    }

    // ======================== Equals / HashCode ========================

    @Test
    public void testEdgeEqualsById() {
        CassandraEdge e1 = new CassandraEdge("e1", "out1", "in1", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e1", "out2", "in2", "likes", mockGraph);
        assertEquals(e1, e2); // same ID
    }

    @Test
    public void testEdgeNotEqualsDifferentId() {
        CassandraEdge e1 = new CassandraEdge("e1", "out1", "in1", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "out1", "in1", "knows", mockGraph);
        assertNotEquals(e1, e2);
    }

    @Test
    public void testEdgeNotEqualsVertex() {
        CassandraEdge edge = new CassandraEdge("id1", "out", "in", "label", mockGraph);
        CassandraVertex vertex = new CassandraVertex("id1", mockGraph);
        assertNotEquals(edge, vertex);
    }

    // ======================== toString ========================

    @Test
    public void testEdgeToString() {
        CassandraEdge edge = new CassandraEdge("e1", "out1", "in1", "knows", mockGraph);
        String str = edge.toString();
        assertTrue(str.contains("e1"));
        assertTrue(str.contains("knows"));
        assertTrue(str.contains("out1"));
        assertTrue(str.contains("in1"));
    }
}
