package org.apache.atlas.repository.graphdb.migrator;

import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

public class DecodedVertexTest {

    @Test
    public void testVertexIdConversion() {
        DecodedVertex vertex = new DecodedVertex(12345L);
        assertEquals(vertex.getJgVertexId(), 12345L);
        assertEquals(vertex.getVertexId(), "12345");
    }

    @Test
    public void testVertexIdNegative() {
        DecodedVertex vertex = new DecodedVertex(-999L);
        assertEquals(vertex.getVertexId(), "-999");
    }

    @Test
    public void testVertexIdZero() {
        DecodedVertex vertex = new DecodedVertex(0L);
        assertEquals(vertex.getVertexId(), "0");
    }

    @Test
    public void testVertexIdMaxLong() {
        DecodedVertex vertex = new DecodedVertex(Long.MAX_VALUE);
        assertEquals(vertex.getVertexId(), String.valueOf(Long.MAX_VALUE));
    }

    @Test
    public void testAddSingleProperty() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("name", "test-entity");
        assertEquals(vertex.getProperties().get("name"), "test-entity");
    }

    @Test
    public void testAddMultipleDistinctProperties() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("name", "entity1");
        vertex.addProperty("age", 42);
        vertex.addProperty("active", true);

        assertEquals(vertex.getProperties().size(), 3);
        assertEquals(vertex.getProperties().get("name"), "entity1");
        assertEquals(vertex.getProperties().get("age"), 42);
        assertEquals(vertex.getProperties().get("active"), true);
    }

    @Test
    public void testMultiValuePropertyConvertedToList() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("tags", "tag1");
        vertex.addProperty("tags", "tag2");

        Object tags = vertex.getProperties().get("tags");
        assertTrue(tags instanceof List);
        List<?> tagList = (List<?>) tags;
        assertEquals(tagList.size(), 2);
        assertEquals(tagList.get(0), "tag1");
        assertEquals(tagList.get(1), "tag2");
    }

    @Test
    public void testMultiValuePropertyThreeValues() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("labels", "a");
        vertex.addProperty("labels", "b");
        vertex.addProperty("labels", "c");

        Object labels = vertex.getProperties().get("labels");
        assertTrue(labels instanceof List);
        List<?> labelList = (List<?>) labels;
        assertEquals(labelList.size(), 3);
        assertEquals(labelList.get(0), "a");
        assertEquals(labelList.get(1), "b");
        assertEquals(labelList.get(2), "c");
    }

    @Test
    public void testConvenienceAccessorGuid() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("__guid", "abc-123-def");
        assertEquals(vertex.getGuid(), "abc-123-def");
    }

    @Test
    public void testConvenienceAccessorGuidNull() {
        DecodedVertex vertex = new DecodedVertex(1L);
        assertNull(vertex.getGuid());
    }

    @Test
    public void testConvenienceAccessorTypeName() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("__typeName", "Table");
        assertEquals(vertex.getTypeName(), "Table");
    }

    @Test
    public void testConvenienceAccessorState() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("__state", "ACTIVE");
        assertEquals(vertex.getState(), "ACTIVE");
    }

    @Test
    public void testConvenienceAccessorVertexLabel() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("__type", "asset");
        assertEquals(vertex.getVertexLabel(), "asset");
    }

    @Test
    public void testAddOutEdge() {
        DecodedVertex vertex = new DecodedVertex(1L);
        DecodedEdge edge = new DecodedEdge(100L, 1L, 2L, "__Process.inputs");

        vertex.addOutEdge(edge);

        assertEquals(vertex.getOutEdges().size(), 1);
        assertSame(vertex.getOutEdges().get(0), edge);
    }

    @Test
    public void testAddMultipleOutEdges() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addOutEdge(new DecodedEdge(100L, 1L, 2L, "__Process.inputs"));
        vertex.addOutEdge(new DecodedEdge(101L, 1L, 3L, "__Process.outputs"));
        vertex.addOutEdge(new DecodedEdge(102L, 1L, 4L, "__AtlasGlossaryTerm.anchor"));

        assertEquals(vertex.getOutEdges().size(), 3);
    }

    @Test
    public void testEmptyVertexPropertiesAndEdges() {
        DecodedVertex vertex = new DecodedVertex(1L);
        assertTrue(vertex.getProperties().isEmpty());
        assertTrue(vertex.getOutEdges().isEmpty());
    }

    @Test
    public void testToString() {
        DecodedVertex vertex = new DecodedVertex(12345L);
        vertex.addProperty("__typeName", "Table");
        vertex.addProperty("__guid", "guid-123");
        vertex.addProperty("name", "my_table");
        vertex.addOutEdge(new DecodedEdge(1L, 12345L, 2L, "edge_label"));

        String str = vertex.toString();
        assertTrue(str.contains("12345"));
        assertTrue(str.contains("Table"));
        assertTrue(str.contains("guid-123"));
        assertTrue(str.contains("props=3"));
        assertTrue(str.contains("edges=1"));
    }

    @Test
    public void testPropertyNullValue() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("nullProp", null);
        // null is stored as a value
        assertTrue(vertex.getProperties().containsKey("nullProp"));
    }

    @Test
    public void testGuidFromNonStringValue() {
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("__guid", 12345);
        assertEquals(vertex.getGuid(), "12345");
    }

    @Test
    public void testPropertiesMapIsLinkedHashMap() {
        // Verify insertion order is preserved
        DecodedVertex vertex = new DecodedVertex(1L);
        vertex.addProperty("c", 3);
        vertex.addProperty("a", 1);
        vertex.addProperty("b", 2);

        Object[] keys = vertex.getProperties().keySet().toArray();
        assertEquals(keys[0], "c");
        assertEquals(keys[1], "a");
        assertEquals(keys[2], "b");
    }
}
