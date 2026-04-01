package org.apache.atlas.repository.graphdb.migrator;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class DecodedEdgeTest {

    @Test
    public void testEdgeConstruction() {
        DecodedEdge edge = new DecodedEdge(100L, 1L, 2L, "__Process.inputs");

        assertEquals(edge.getJgRelationId(), 100L);
        assertEquals(edge.getOutVertexJgId(), 1L);
        assertEquals(edge.getInVertexJgId(), 2L);
        assertEquals(edge.getLabel(), "__Process.inputs");
    }

    @Test
    public void testEdgeIdConversion() {
        DecodedEdge edge = new DecodedEdge(999L, 1L, 2L, "label");
        assertEquals(edge.getEdgeId(), "999");
    }

    @Test
    public void testVertexIdConversions() {
        DecodedEdge edge = new DecodedEdge(100L, 12345L, 67890L, "label");
        assertEquals(edge.getOutVertexId(), "12345");
        assertEquals(edge.getInVertexId(), "67890");
    }

    @Test
    public void testLargeIds() {
        long largeId = Long.MAX_VALUE - 1;
        DecodedEdge edge = new DecodedEdge(largeId, largeId - 1, largeId - 2, "label");
        assertEquals(edge.getEdgeId(), String.valueOf(largeId));
        assertEquals(edge.getOutVertexId(), String.valueOf(largeId - 1));
        assertEquals(edge.getInVertexId(), String.valueOf(largeId - 2));
    }

    @Test
    public void testEmptyProperties() {
        DecodedEdge edge = new DecodedEdge(1L, 2L, 3L, "label");
        assertTrue(edge.getProperties().isEmpty());
    }

    @Test
    public void testAddProperties() {
        DecodedEdge edge = new DecodedEdge(1L, 2L, 3L, "label");
        edge.addProperty("__state", "ACTIVE");
        edge.addProperty("__typeName", "Process");
        edge.addProperty("_r__guid", "edge-guid-123");

        assertEquals(edge.getProperties().size(), 3);
        assertEquals(edge.getProperties().get("__state"), "ACTIVE");
        assertEquals(edge.getProperties().get("__typeName"), "Process");
        assertEquals(edge.getProperties().get("_r__guid"), "edge-guid-123");
    }

    @Test
    public void testPropertyOverwrite() {
        DecodedEdge edge = new DecodedEdge(1L, 2L, 3L, "label");
        edge.addProperty("key", "value1");
        edge.addProperty("key", "value2");
        // Unlike DecodedVertex, DecodedEdge uses simple put (single-valued)
        assertEquals(edge.getProperties().get("key"), "value2");
    }

    @Test
    public void testToString() {
        DecodedEdge edge = new DecodedEdge(100L, 1L, 2L, "__Process.inputs");
        edge.addProperty("__state", "ACTIVE");

        String str = edge.toString();
        assertTrue(str.contains("100"));
        assertTrue(str.contains("1"));
        assertTrue(str.contains("2"));
        assertTrue(str.contains("__Process.inputs"));
        assertTrue(str.contains("props=1"));
    }

    @Test
    public void testEdgeLabelPreserved() {
        String[] labels = {
            "__Process.inputs", "__Process.outputs",
            "__AtlasGlossaryTerm.anchor", "__AtlasGlossaryCategory.parentCategory",
            "__Asset.meanings", "classifiedAs"
        };

        for (String label : labels) {
            DecodedEdge edge = new DecodedEdge(1L, 2L, 3L, label);
            assertEquals(edge.getLabel(), label);
        }
    }

    @Test
    public void testPropertiesMapIsLinkedHashMap() {
        DecodedEdge edge = new DecodedEdge(1L, 2L, 3L, "label");
        edge.addProperty("z", 1);
        edge.addProperty("a", 2);
        edge.addProperty("m", 3);

        Object[] keys = edge.getProperties().keySet().toArray();
        assertEquals(keys[0], "z");
        assertEquals(keys[1], "a");
        assertEquals(keys[2], "m");
    }
}
