package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.Assert.*;

/**
 * Tests for CassandraGraphIndex.
 * Covers: index type flags, field keys, uniqueness.
 */
public class CassandraGraphIndexTest {

    // ======================== Constructor / Flag Tests ========================

    @Test
    public void testVertexCompositeIndex() {
        CassandraGraphIndex index = new CassandraGraphIndex(
                "guid_idx", false, true, false, true, true);
        assertEquals(index.getName(), "guid_idx");
        assertFalse(index.isMixedIndex());
        assertTrue(index.isCompositeIndex());
        assertFalse(index.isEdgeIndex());
        assertTrue(index.isVertexIndex());
        assertTrue(index.isUnique());
    }

    @Test
    public void testVertexMixedIndex() {
        CassandraGraphIndex index = new CassandraGraphIndex(
                "vertex_index", true, false, false, true, false);
        assertTrue(index.isMixedIndex());
        assertFalse(index.isCompositeIndex());
        assertTrue(index.isVertexIndex());
        assertFalse(index.isEdgeIndex());
        assertFalse(index.isUnique());
    }

    @Test
    public void testEdgeCompositeIndex() {
        CassandraGraphIndex index = new CassandraGraphIndex(
                "edge_state_idx", false, true, true, false, false);
        assertTrue(index.isEdgeIndex());
        assertFalse(index.isVertexIndex());
        assertTrue(index.isCompositeIndex());
    }

    @Test
    public void testEdgeMixedIndex() {
        CassandraGraphIndex index = new CassandraGraphIndex(
                "edge_index", true, false, true, false, false);
        assertTrue(index.isMixedIndex());
        assertTrue(index.isEdgeIndex());
    }

    // ======================== Field Key Tests ========================

    @Test
    public void testFieldKeysEmptyByDefault() {
        CassandraGraphIndex index = new CassandraGraphIndex(
                "test", false, true, false, true, false);
        assertNotNull(index.getFieldKeys());
        assertTrue(index.getFieldKeys().isEmpty());
    }

    @Test
    public void testAddFieldKey() {
        CassandraGraphIndex index = new CassandraGraphIndex(
                "test", false, true, false, true, false);
        CassandraPropertyKey key = new CassandraPropertyKey("__guid", String.class, AtlasCardinality.SINGLE);
        index.addFieldKey(key);
        assertEquals(index.getFieldKeys().size(), 1);
        assertTrue(index.getFieldKeys().contains(key));
    }

    @Test
    public void testAddMultipleFieldKeys() {
        CassandraGraphIndex index = new CassandraGraphIndex(
                "qn_type_idx", false, true, false, true, true);
        CassandraPropertyKey key1 = new CassandraPropertyKey("qualifiedName", String.class, AtlasCardinality.SINGLE);
        CassandraPropertyKey key2 = new CassandraPropertyKey("__typeName", String.class, AtlasCardinality.SINGLE);
        index.addFieldKey(key1);
        index.addFieldKey(key2);
        assertEquals(index.getFieldKeys().size(), 2);
    }

    @Test
    public void testAddDuplicateFieldKey() {
        CassandraGraphIndex index = new CassandraGraphIndex(
                "test", false, true, false, true, false);
        CassandraPropertyKey key = new CassandraPropertyKey("__guid", String.class, AtlasCardinality.SINGLE);
        index.addFieldKey(key);
        index.addFieldKey(key);
        assertEquals(index.getFieldKeys().size(), 1); // Set deduplicates
    }

    // ======================== Name Tests ========================

    @Test
    public void testGetName() {
        CassandraGraphIndex index = new CassandraGraphIndex(
                "my_custom_index", false, true, false, true, false);
        assertEquals(index.getName(), "my_custom_index");
    }

    // ======================== Real-world Index Patterns ========================

    @Test
    public void testGuidCompositeIndex() {
        // Simulates the __guid_idx used in Atlas
        CassandraGraphIndex index = new CassandraGraphIndex(
                "__guid_idx", false, true, false, true, true);
        CassandraPropertyKey guidKey = new CassandraPropertyKey("__guid", String.class, AtlasCardinality.SINGLE);
        index.addFieldKey(guidKey);

        assertTrue(index.isUnique());
        assertTrue(index.isCompositeIndex());
        assertTrue(index.isVertexIndex());
        assertEquals(index.getFieldKeys().size(), 1);
    }

    @Test
    public void testQualifiedNameTypeNameCompositeIndex() {
        // Simulates qn_type_idx
        CassandraGraphIndex index = new CassandraGraphIndex(
                "qn_type_idx", false, true, false, true, true);
        index.addFieldKey(new CassandraPropertyKey("qualifiedName", String.class, AtlasCardinality.SINGLE));
        index.addFieldKey(new CassandraPropertyKey("__typeName", String.class, AtlasCardinality.SINGLE));

        assertTrue(index.isUnique());
        assertEquals(index.getFieldKeys().size(), 2);
    }

    @Test
    public void testVertexMixedSearchIndex() {
        // Simulates the ES-backed vertex_index used for full-text search
        CassandraGraphIndex index = new CassandraGraphIndex(
                "vertex_index", true, false, false, true, false);
        index.addFieldKey(new CassandraPropertyKey("__typeName", String.class, AtlasCardinality.SINGLE));
        index.addFieldKey(new CassandraPropertyKey("__guid", String.class, AtlasCardinality.SINGLE));
        index.addFieldKey(new CassandraPropertyKey("qualifiedName", String.class, AtlasCardinality.SINGLE));
        index.addFieldKey(new CassandraPropertyKey("__state", String.class, AtlasCardinality.SINGLE));

        assertTrue(index.isMixedIndex());
        assertFalse(index.isUnique()); // mixed indexes are not unique
        assertEquals(index.getFieldKeys().size(), 4);
    }
}
