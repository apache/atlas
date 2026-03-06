package org.apache.atlas.repository.graph;

import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for GraphHelper classification name methods (V1/legacy vertex property path).
 *
 * Note: getTraitNamesV2() queries Cassandra via TagDAOCassandraImpl and is covered by
 * integration tests (ClassificationBatchFetchIntegrationTest) rather than unit tests,
 * since TagDAOCassandraImpl's static initializer requires a Cassandra connection.
 */
class GraphHelperClassificationNamesTest {

    @Test
    void testGetClassificationNamesFromVertex() {
        AtlasVertex vertex = mockVertexWithProperty(CLASSIFICATION_NAMES_KEY, "|PII|Confidential|Internal|");

        List<String> names = GraphHelper.getClassificationNamesFromVertex(vertex);

        assertEquals(3, names.size());
        assertEquals("PII", names.get(0));
        assertEquals("Confidential", names.get(1));
        assertEquals("Internal", names.get(2));
    }

    @Test
    void testGetClassificationNamesFromVertexNull() {
        AtlasVertex vertex = mockVertexWithProperty(CLASSIFICATION_NAMES_KEY, null);

        List<String> names = GraphHelper.getClassificationNamesFromVertex(vertex);

        assertNotNull(names);
        assertTrue(names.isEmpty());
    }

    @Test
    void testGetClassificationNamesFromVertexEmpty() {
        AtlasVertex vertex = mockVertexWithProperty(CLASSIFICATION_NAMES_KEY, "||");

        List<String> names = GraphHelper.getClassificationNamesFromVertex(vertex);

        assertNotNull(names);
        assertTrue(names.isEmpty());
    }

    @Test
    void testGetPropagatedClassificationNamesFromVertex() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, String.class)).thenReturn("|Sensitive|PII|");

        List<String> names = GraphHelper.getPropagatedClassificationNamesFromVertex(vertex);

        assertEquals(2, names.size());
        assertEquals("Sensitive", names.get(0));
        assertEquals("PII", names.get(1));
    }

    @Test
    void testGetPropagatedClassificationNamesFromVertexNull() {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, String.class)).thenReturn(null);

        List<String> names = GraphHelper.getPropagatedClassificationNamesFromVertex(vertex);

        assertNotNull(names);
        assertTrue(names.isEmpty());
    }

    private AtlasVertex mockVertexWithProperty(String propertyKey, String value) {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(vertex.getProperty(propertyKey, String.class)).thenReturn(value);
        return vertex;
    }
}
