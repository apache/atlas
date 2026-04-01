package org.apache.atlas.repository.graphdb.cassandra;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Tests for IndexRepository.IndexEntry data class.
 * (Full IndexRepository CRUD is tested via CassandraGraph integration tests
 * since it requires a real CqlSession with prepared statements.)
 */
public class IndexRepositoryTest {

    @Test
    public void testIndexEntryConstructor() {
        IndexRepository.IndexEntry entry = new IndexRepository.IndexEntry("__guid_idx", "guid-123", "v1");
        assertEquals(entry.indexName, "__guid_idx");
        assertEquals(entry.indexValue, "guid-123");
        assertEquals(entry.vertexId, "v1");
    }

    @Test
    public void testIndexEntryWithCompositeKey() {
        IndexRepository.IndexEntry entry = new IndexRepository.IndexEntry(
                "qn_type_idx", "default/table/users:Table", "v2");
        assertEquals(entry.indexName, "qn_type_idx");
        assertEquals(entry.indexValue, "default/table/users:Table");
        assertEquals(entry.vertexId, "v2");
    }

    @Test
    public void testIndexEntryWithNullValues() {
        IndexRepository.IndexEntry entry = new IndexRepository.IndexEntry(null, null, null);
        assertNull(entry.indexName);
        assertNull(entry.indexValue);
        assertNull(entry.vertexId);
    }

    @Test
    public void testIndexEntryFieldsArePublicFinal() {
        IndexRepository.IndexEntry entry = new IndexRepository.IndexEntry("idx", "val", "vid");
        // Fields are public and accessible
        assertNotNull(entry.indexName);
        assertNotNull(entry.indexValue);
        assertNotNull(entry.vertexId);
    }

    @Test
    public void testIndexEntryForPropertyIndex() {
        IndexRepository.IndexEntry entry = new IndexRepository.IndexEntry(
                "type_category_idx", "typeSystem:ENTITY", "td1");
        assertEquals(entry.indexName, "type_category_idx");
        assertTrue(entry.indexValue.contains(":"));
    }
}
