package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraGraphManagement.
 * Covers: property key CRUD, edge label CRUD, composite/mixed index creation, schema queries.
 */
public class CassandraGraphManagementTest {

    private CassandraGraph mockGraph;
    private CassandraGraphManagement management;

    private Map<String, CassandraPropertyKey> propertyKeysMap;
    private Map<String, CassandraEdgeLabel> edgeLabelsMap;
    private Map<String, CassandraGraphIndex> graphIndexesMap;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(CassandraGraph.class);
        propertyKeysMap = new LinkedHashMap<>();
        edgeLabelsMap = new LinkedHashMap<>();
        graphIndexesMap = new LinkedHashMap<>();

        when(mockGraph.getPropertyKeysMap()).thenReturn(propertyKeysMap);
        when(mockGraph.getEdgeLabelsMap()).thenReturn(edgeLabelsMap);
        when(mockGraph.getGraphIndexesMap()).thenReturn(graphIndexesMap);

        management = new CassandraGraphManagement(mockGraph);
    }

    // ======================== Property Key Tests ========================

    @Test
    public void testMakePropertyKey() {
        AtlasPropertyKey key = management.makePropertyKey("__guid", String.class, AtlasCardinality.SINGLE);
        assertNotNull(key);
        assertEquals(key.getName(), "__guid");
        assertTrue(propertyKeysMap.containsKey("__guid"));
    }

    @Test
    public void testMakePropertyKeySetCardinality() {
        AtlasPropertyKey key = management.makePropertyKey("tags", String.class, AtlasCardinality.SET);
        assertEquals(key.getCardinality(), AtlasCardinality.SET);
        verify(mockGraph).addMultiProperty("tags");
    }

    @Test
    public void testMakePropertyKeyListCardinality() {
        AtlasPropertyKey key = management.makePropertyKey("meanings", String.class, AtlasCardinality.LIST);
        verify(mockGraph).addMultiProperty("meanings");
    }

    @Test
    public void testMakePropertyKeySingleCardinalityDoesNotAddMulti() {
        management.makePropertyKey("name", String.class, AtlasCardinality.SINGLE);
        verify(mockGraph, never()).addMultiProperty(anyString());
    }

    @Test
    public void testContainsPropertyKeyTrue() {
        management.makePropertyKey("__guid", String.class, AtlasCardinality.SINGLE);
        assertTrue(management.containsPropertyKey("__guid"));
    }

    @Test
    public void testContainsPropertyKeyFalse() {
        assertFalse(management.containsPropertyKey("nonexistent"));
    }

    @Test
    public void testGetPropertyKey() {
        management.makePropertyKey("__guid", String.class, AtlasCardinality.SINGLE);
        AtlasPropertyKey key = management.getPropertyKey("__guid");
        assertNotNull(key);
        assertEquals(key.getName(), "__guid");
    }

    @Test
    public void testGetPropertyKeyMissing() {
        assertNull(management.getPropertyKey("nonexistent"));
    }

    @Test
    public void testDeletePropertyKey() {
        management.makePropertyKey("toDelete", String.class, AtlasCardinality.SINGLE);
        assertTrue(management.containsPropertyKey("toDelete"));
        management.deletePropertyKey("toDelete");
        assertFalse(management.containsPropertyKey("toDelete"));
    }

    // ======================== Edge Label Tests ========================

    @Test
    public void testMakeEdgeLabel() {
        AtlasEdgeLabel label = management.makeEdgeLabel("__Process.inputs");
        assertNotNull(label);
        assertEquals(label.getName(), "__Process.inputs");
        assertTrue(edgeLabelsMap.containsKey("__Process.inputs"));
    }

    @Test
    public void testGetEdgeLabel() {
        management.makeEdgeLabel("knows");
        AtlasEdgeLabel label = management.getEdgeLabel("knows");
        assertNotNull(label);
        assertEquals(label.getName(), "knows");
    }

    @Test
    public void testGetEdgeLabelMissing() {
        assertNull(management.getEdgeLabel("nonexistent"));
    }

    // ======================== Vertex Composite Index Tests ========================

    @Test
    public void testCreateVertexCompositeIndex() {
        AtlasPropertyKey guidKey = management.makePropertyKey("__guid", String.class, AtlasCardinality.SINGLE);
        List<AtlasPropertyKey> keys = Collections.singletonList(guidKey);

        management.createVertexCompositeIndex("guid_idx", true, keys);

        AtlasGraphIndex index = management.getGraphIndex("guid_idx");
        assertNotNull(index);
        assertTrue(index.isCompositeIndex());
        assertFalse(index.isMixedIndex());
        assertTrue(index.isVertexIndex());
        assertFalse(index.isEdgeIndex());
        assertTrue(index.isUnique());
        assertEquals(index.getFieldKeys().size(), 1);
    }

    @Test
    public void testCreateVertexCompositeIndexMultipleKeys() {
        AtlasPropertyKey key1 = management.makePropertyKey("qualifiedName", String.class, AtlasCardinality.SINGLE);
        AtlasPropertyKey key2 = management.makePropertyKey("__typeName", String.class, AtlasCardinality.SINGLE);
        List<AtlasPropertyKey> keys = Arrays.asList(key1, key2);

        management.createVertexCompositeIndex("qn_type_idx", true, keys);

        AtlasGraphIndex index = management.getGraphIndex("qn_type_idx");
        assertEquals(index.getFieldKeys().size(), 2);
    }

    @Test
    public void testCreateVertexCompositeIndexNonUnique() {
        AtlasPropertyKey key = management.makePropertyKey("__typeName", String.class, AtlasCardinality.SINGLE);
        management.createVertexCompositeIndex("type_idx", false, Collections.singletonList(key));

        AtlasGraphIndex index = management.getGraphIndex("type_idx");
        assertFalse(index.isUnique());
    }

    // ======================== Edge Composite Index Tests ========================

    @Test
    public void testCreateEdgeCompositeIndex() {
        AtlasPropertyKey stateKey = management.makePropertyKey("__state", String.class, AtlasCardinality.SINGLE);
        management.createEdgeCompositeIndex("edge_state_idx", false, Collections.singletonList(stateKey));

        AtlasGraphIndex index = management.getGraphIndex("edge_state_idx");
        assertNotNull(index);
        assertTrue(index.isCompositeIndex());
        assertTrue(index.isEdgeIndex());
        assertFalse(index.isVertexIndex());
    }

    // ======================== Mixed Index Tests ========================

    @Test
    public void testCreateVertexMixedIndex() {
        AtlasPropertyKey key = management.makePropertyKey("__typeName", String.class, AtlasCardinality.SINGLE);

        // ensureESIndexExists will fail gracefully since no ES is running
        management.createVertexMixedIndex("vertex_index", "search", Collections.singletonList(key));

        AtlasGraphIndex index = management.getGraphIndex("vertex_index");
        assertNotNull(index);
        assertTrue(index.isMixedIndex());
        assertFalse(index.isCompositeIndex());
        assertTrue(index.isVertexIndex());
        assertFalse(index.isUnique());
    }

    @Test
    public void testCreateEdgeMixedIndex() {
        AtlasPropertyKey key = management.makePropertyKey("__state", String.class, AtlasCardinality.SINGLE);
        management.createEdgeMixedIndex("edge_index", "search", Collections.singletonList(key));

        AtlasGraphIndex index = management.getGraphIndex("edge_index");
        assertNotNull(index);
        assertTrue(index.isMixedIndex());
        assertTrue(index.isEdgeIndex());
    }

    @Test
    public void testCreateFullTextMixedIndex() {
        AtlasPropertyKey key = management.makePropertyKey("description", String.class, AtlasCardinality.SINGLE);
        management.createFullTextMixedIndex("fulltext_index", "search", Collections.singletonList(key));

        AtlasGraphIndex index = management.getGraphIndex("fulltext_index");
        assertNotNull(index);
        assertTrue(index.isMixedIndex());
        assertTrue(index.isVertexIndex());
    }

    // ======================== AddMixedIndex Tests ========================

    @Test
    public void testAddMixedIndex() {
        AtlasPropertyKey typeKey = management.makePropertyKey("__typeName", String.class, AtlasCardinality.SINGLE);
        management.createVertexMixedIndex("vertex_index", "search", Collections.singletonList(typeKey));

        AtlasPropertyKey newKey = management.makePropertyKey("description", String.class, AtlasCardinality.SINGLE);
        String fieldName = management.addMixedIndex("vertex_index", newKey, true);

        assertEquals(fieldName, "description"); // direct name, no JanusGraph encoding
        AtlasGraphIndex index = management.getGraphIndex("vertex_index");
        assertEquals(index.getFieldKeys().size(), 2);
    }

    @Test
    public void testAddMixedIndexToNonexistent() {
        AtlasPropertyKey key = management.makePropertyKey("name", String.class, AtlasCardinality.SINGLE);
        String fieldName = management.addMixedIndex("nonexistent", key, true);
        assertEquals(fieldName, "name"); // still returns field name even if index doesn't exist
    }

    @Test
    public void testAddMixedIndexWithConfig() {
        AtlasPropertyKey key = management.makePropertyKey("name", String.class, AtlasCardinality.SINGLE);
        management.createVertexMixedIndex("vertex_index", "search", Collections.singletonList(key));

        AtlasPropertyKey newKey = management.makePropertyKey("desc", String.class, AtlasCardinality.SINGLE);
        String fieldName = management.addMixedIndex("vertex_index", newKey, true,
                new HashMap<>(), new HashMap<>());
        assertEquals(fieldName, "desc");
    }

    // ======================== GetIndexFieldName Tests ========================

    @Test
    public void testGetIndexFieldName() {
        AtlasPropertyKey key = new CassandraPropertyKey("__guid", String.class, AtlasCardinality.SINGLE);
        String fieldName = management.getIndexFieldName("vertex_index", key, false);
        assertEquals(fieldName, "__guid"); // no JanusGraph encoding
    }

    @Test
    public void testGetIndexFieldNameStringField() {
        AtlasPropertyKey key = new CassandraPropertyKey("qualifiedName", String.class, AtlasCardinality.SINGLE);
        String fieldName = management.getIndexFieldName("vertex_index", key, true);
        assertEquals(fieldName, "qualifiedName");
    }

    // ======================== Edge Index Exist Tests ========================

    @Test
    public void testEdgeIndexExistTrue() {
        management.createEdgeCompositeIndex("my_edge_idx", false, Collections.emptyList());
        assertTrue(management.edgeIndexExist("some_label", "my_edge_idx"));
    }

    @Test
    public void testEdgeIndexExistFalse() {
        assertFalse(management.edgeIndexExist("some_label", "nonexistent"));
    }

    // ======================== No-op Methods ========================

    @Test
    public void testRollbackNoOp() {
        management.rollback(); // should not throw
    }

    @Test
    public void testCommitNoOp() {
        management.commit(); // should not throw
    }

    @Test
    public void testUpdateUniqueIndexesNoOp() {
        management.updateUniqueIndexesForConsistencyLock(); // should not throw
    }

    @Test
    public void testUpdateSchemaStatusNoOp() {
        management.updateSchemaStatus(); // should not throw
    }

    @Test
    public void testStartIndexRecoveryReturnsNull() {
        assertNull(management.startIndexRecovery(System.currentTimeMillis()));
    }

    @Test
    public void testStopIndexRecoveryNoOp() {
        management.stopIndexRecovery(null); // should not throw
    }

    @Test
    public void testPrintIndexRecoveryStatsNoOp() {
        management.printIndexRecoveryStats(null); // should not throw
    }

    @Test
    public void testReindexNoOp() throws Exception {
        management.reindex("vertex_index", Collections.emptyList()); // should not throw
    }

    @Test
    public void testCreateEdgeIndexNoOp() {
        management.createEdgeIndex("label", "indexName", AtlasEdgeDirection.OUT, Collections.emptyList());
        // should not throw, just logs
    }
}
