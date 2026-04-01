package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

/**
 * Tests for TransactionBuffer.
 * Covers: vertex lifecycle, edge lifecycle, edge filtering, buffer state management.
 */
public class TransactionBufferTest {

    private TransactionBuffer buffer;
    private CassandraGraph mockGraph;

    @BeforeMethod
    public void setUp() {
        buffer = new TransactionBuffer();
        mockGraph = mock(CassandraGraph.class);
    }

    // ======================== Initial State ========================

    @Test
    public void testNewBufferIsEmpty() {
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.getNewVertices().isEmpty());
        assertTrue(buffer.getDirtyVertices().isEmpty());
        assertTrue(buffer.getRemovedVertices().isEmpty());
        assertTrue(buffer.getNewEdges().isEmpty());
        assertTrue(buffer.getRemovedEdges().isEmpty());
    }

    // ======================== Vertex Lifecycle ========================

    @Test
    public void testAddVertex() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        buffer.addVertex(v);
        assertFalse(buffer.isEmpty());
        assertEquals(buffer.getNewVertices().size(), 1);
        assertEquals(buffer.getNewVertices().get(0).getIdString(), "v1");
    }

    @Test
    public void testAddMultipleVertices() {
        buffer.addVertex(new CassandraVertex("v1", mockGraph));
        buffer.addVertex(new CassandraVertex("v2", mockGraph));
        buffer.addVertex(new CassandraVertex("v3", mockGraph));
        assertEquals(buffer.getNewVertices().size(), 3);
    }

    @Test
    public void testAddDuplicateVertexOverwrites() {
        CassandraVertex v1a = new CassandraVertex("v1", mockGraph);
        CassandraVertex v1b = new CassandraVertex("v1", mockGraph);
        v1b.setProperty("key", "updated");
        buffer.addVertex(v1a);
        buffer.addVertex(v1b);
        assertEquals(buffer.getNewVertices().size(), 1);
        assertEquals(buffer.getNewVertices().get(0).getProperty("key", String.class), "updated");
    }

    @Test
    public void testMarkVertexDirty() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        buffer.markVertexDirty(v);
        assertEquals(buffer.getDirtyVertices().size(), 1);
    }

    @Test
    public void testMarkVertexDirtySkipsNewVertices() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        buffer.addVertex(v);
        buffer.markVertexDirty(v); // should be ignored since it's already in newVertices
        assertEquals(buffer.getDirtyVertices().size(), 0);
        assertEquals(buffer.getNewVertices().size(), 1);
    }

    @Test
    public void testRemoveVertex() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        buffer.addVertex(v);
        buffer.removeVertex(v);
        assertTrue(buffer.getNewVertices().isEmpty()); // removed from new
        assertEquals(buffer.getRemovedVertices().size(), 1);
    }

    @Test
    public void testRemoveVertexAlsoRemovesDirty() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        buffer.markVertexDirty(v);
        buffer.removeVertex(v);
        assertTrue(buffer.getDirtyVertices().isEmpty());
        assertEquals(buffer.getRemovedVertices().size(), 1);
    }

    // ======================== Edge Lifecycle ========================

    @Test
    public void testAddEdge() {
        CassandraEdge e = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        buffer.addEdge(e);
        assertFalse(buffer.isEmpty());
        assertEquals(buffer.getNewEdges().size(), 1);
    }

    @Test
    public void testRemoveEdge() {
        CassandraEdge e = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        buffer.addEdge(e);
        buffer.removeEdge(e);
        assertTrue(buffer.getNewEdges().isEmpty());
        assertEquals(buffer.getRemovedEdges().size(), 1);
    }

    @Test
    public void testIsEdgeRemoved() {
        CassandraEdge e = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        assertFalse(buffer.isEdgeRemoved("e1"));
        buffer.removeEdge(e);
        assertTrue(buffer.isEdgeRemoved("e1"));
    }

    // ======================== Edge Filtering by Direction ========================

    @Test
    public void testGetEdgesForVertexOutDirection() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v2", "v1", "knows", mockGraph); // incoming to v1
        buffer.addEdge(e1);
        buffer.addEdge(e2);

        List<CassandraEdge> result = buffer.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, null);
        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getIdString(), "e1");
    }

    @Test
    public void testGetEdgesForVertexInDirection() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v2", "v1", "knows", mockGraph);
        buffer.addEdge(e1);
        buffer.addEdge(e2);

        List<CassandraEdge> result = buffer.getEdgesForVertex("v1", AtlasEdgeDirection.IN, null);
        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getIdString(), "e2");
    }

    @Test
    public void testGetEdgesForVertexBothDirection() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v3", "v1", "likes", mockGraph);
        CassandraEdge e3 = new CassandraEdge("e3", "v4", "v5", "other", mockGraph); // not related to v1
        buffer.addEdge(e1);
        buffer.addEdge(e2);
        buffer.addEdge(e3);

        List<CassandraEdge> result = buffer.getEdgesForVertex("v1", AtlasEdgeDirection.BOTH, null);
        assertEquals(result.size(), 2);
    }

    // ======================== Edge Filtering by Label ========================

    @Test
    public void testGetEdgesForVertexByLabel() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v1", "v3", "likes", mockGraph);
        buffer.addEdge(e1);
        buffer.addEdge(e2);

        List<CassandraEdge> result = buffer.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "knows");
        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getLabel(), "knows");
    }

    @Test
    public void testGetEdgesForVertexByLabelNoMatch() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        buffer.addEdge(e1);

        List<CassandraEdge> result = buffer.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "nonexistent");
        assertTrue(result.isEmpty());
    }

    // ======================== Edge Filtering Skips Removed ========================

    @Test
    public void testGetEdgesForVertexSkipsRemovedEdges() {
        CassandraEdge e1 = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        CassandraEdge e2 = new CassandraEdge("e2", "v1", "v3", "knows", mockGraph);
        buffer.addEdge(e1);
        buffer.addEdge(e2);
        buffer.removeEdge(e1);

        List<CassandraEdge> result = buffer.getEdgesForVertex("v1", AtlasEdgeDirection.OUT, "knows");
        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getIdString(), "e2");
    }

    // ======================== Clear ========================

    @Test
    public void testClear() {
        buffer.addVertex(new CassandraVertex("v1", mockGraph));
        buffer.markVertexDirty(new CassandraVertex("v2", mockGraph));
        buffer.addEdge(new CassandraEdge("e1", "v1", "v2", "knows", mockGraph));
        assertFalse(buffer.isEmpty());

        buffer.clear();
        assertTrue(buffer.isEmpty());
        assertTrue(buffer.getNewVertices().isEmpty());
        assertTrue(buffer.getDirtyVertices().isEmpty());
        assertTrue(buffer.getRemovedVertices().isEmpty());
        assertTrue(buffer.getNewEdges().isEmpty());
        assertTrue(buffer.getRemovedEdges().isEmpty());
    }

    // ======================== IsEmpty ========================

    @Test
    public void testIsEmptyWithNewVertex() {
        buffer.addVertex(new CassandraVertex("v1", mockGraph));
        assertFalse(buffer.isEmpty());
    }

    @Test
    public void testIsEmptyWithDirtyVertex() {
        buffer.markVertexDirty(new CassandraVertex("v1", mockGraph));
        assertFalse(buffer.isEmpty());
    }

    @Test
    public void testIsEmptyWithRemovedVertex() {
        CassandraVertex v = new CassandraVertex("v1", mockGraph);
        buffer.removeVertex(v);
        assertFalse(buffer.isEmpty());
    }

    @Test
    public void testIsEmptyWithNewEdge() {
        buffer.addEdge(new CassandraEdge("e1", "v1", "v2", "knows", mockGraph));
        assertFalse(buffer.isEmpty());
    }

    @Test
    public void testIsEmptyWithRemovedEdge() {
        CassandraEdge e = new CassandraEdge("e1", "v1", "v2", "knows", mockGraph);
        buffer.removeEdge(e);
        assertFalse(buffer.isEmpty());
    }

    // ======================== Return Copies Not References ========================

    @Test
    public void testGetNewVerticesReturnsNewList() {
        buffer.addVertex(new CassandraVertex("v1", mockGraph));
        List<CassandraVertex> list1 = buffer.getNewVertices();
        List<CassandraVertex> list2 = buffer.getNewVertices();
        assertNotSame(list1, list2);
        assertEquals(list1.size(), list2.size());
    }

    @Test
    public void testGetNewEdgesReturnsNewList() {
        buffer.addEdge(new CassandraEdge("e1", "v1", "v2", "knows", mockGraph));
        List<CassandraEdge> list1 = buffer.getNewEdges();
        List<CassandraEdge> list2 = buffer.getNewEdges();
        assertNotSame(list1, list2);
    }
}
