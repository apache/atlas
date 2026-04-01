package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Tests for CassandraIndexQuery.
 * Tests construction and null-client fallback paths.
 * (ES integration tests require a running ES instance and are separate.)
 */
public class CassandraIndexQueryTest {

    private CassandraGraph mockGraph;

    @BeforeMethod
    public void setUp() {
        mockGraph = mock(CassandraGraph.class);
    }

    // ======================== ES doc ID decoding (base-36) ========================

    @Test
    public void testDecodeDocIdBase36() {
        // JanusGraph LongEncoding uses base-36 (0-9, a-z)
        // "48" in base-36 = 4*36 + 8 = 152
        assertEquals(CassandraIndexQuery.decodeDocId("48"), "152");
    }

    @Test
    public void testDecodeDocIdSmallNumbers() {
        // Single digit base-36: '0'=0, '1'=1, '9'=9, 'a'=10, 'z'=35
        assertEquals(CassandraIndexQuery.decodeDocId("0"), "0");
        assertEquals(CassandraIndexQuery.decodeDocId("1"), "1");
        assertEquals(CassandraIndexQuery.decodeDocId("9"), "9");
        assertEquals(CassandraIndexQuery.decodeDocId("a"), "10");
        assertEquals(CassandraIndexQuery.decodeDocId("z"), "35");
    }

    @Test
    public void testDecodeDocIdMultiDigit() {
        // "10" in base-36 = 1*36 + 0 = 36
        assertEquals(CassandraIndexQuery.decodeDocId("10"), "36");
        // "11" in base-36 = 1*36 + 1 = 37
        assertEquals(CassandraIndexQuery.decodeDocId("11"), "37");
        // "zz" in base-36 = 35*36 + 35 = 1295
        assertEquals(CassandraIndexQuery.decodeDocId("zz"), "1295");
        // "100" in base-36 = 1*1296 + 0 + 0 = 1296
        assertEquals(CassandraIndexQuery.decodeDocId("100"), "1296");
    }

    @Test
    public void testDecodeDocIdUppercasePassesThrough() {
        // JanusGraph base-36 only uses lowercase — uppercase chars pass through as-is
        assertEquals(CassandraIndexQuery.decodeDocId("A"), "A");
        assertEquals(CassandraIndexQuery.decodeDocId("Z"), "Z");
        assertEquals(CassandraIndexQuery.decodeDocId("5A"), "5A");
    }

    @Test
    public void testDecodeDocIdUuidPassesThrough() {
        // UUID strings contain hyphens — should pass through unchanged
        String uuid = "550e8400-e29b-41d4-a716-446655440000";
        assertEquals(CassandraIndexQuery.decodeDocId(uuid), uuid);
    }

    @Test
    public void testDecodeDocIdNullAndEmpty() {
        assertNull(CassandraIndexQuery.decodeDocId(null));
        assertEquals(CassandraIndexQuery.decodeDocId(""), "");
    }

    @Test
    public void testDecodeDocIdInvalidCharPassesThrough() {
        // Characters not in base-36 (special chars, uppercase) cause pass-through
        assertEquals(CassandraIndexQuery.decodeDocId("abc!def"), "abc!def");
    }

    @Test
    public void testDecodeDocIdRealWorldExamples() {
        // JanusGraph vertex IDs are typically large numbers
        // e.g., vertex 4096 → "348" in base-36 (3*1296 + 4*36 + 8 = 3888 + 144 + 8 = 4040)
        // Actually: let's verify specific values
        // vertex 256: 256 / 36 = 7 remainder 4 → "74"
        assertEquals(CassandraIndexQuery.decodeDocId("74"), "256");
        // vertex 1000: 1000 / 36 = 27 rem 28 → 27='r', 28='s' → "rs"
        assertEquals(CassandraIndexQuery.decodeDocId("rs"), "1000");
    }

    // ======================== Construction ========================

    @Test
    public void testConstructWithQueryString() {
        // ES clients will be null (AtlasElasticsearchDatabase not initialized in test)
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{\"query\":{\"match_all\":{}}}", 0);
        assertNotNull(query);
    }

    @Test
    public void testConstructWithOffset() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 10);
        assertNotNull(query);
    }

    @Test
    public void testConstructWithNullSourceBuilder() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index",
                (org.elasticsearch.search.builder.SearchSourceBuilder) null);
        assertNotNull(query);
    }

    // ======================== Vertices with null ES client ========================

    @Test
    public void testVerticesReturnsEmptyWhenNoESClient() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index",
                (org.elasticsearch.search.builder.SearchSourceBuilder) null);
        Iterator<AtlasIndexQuery.Result<CassandraVertex, CassandraEdge>> result = query.vertices();
        assertFalse(result.hasNext());
    }

    @Test
    public void testVerticesWithOffsetAndLimitReturnsEmpty() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index",
                (org.elasticsearch.search.builder.SearchSourceBuilder) null);
        Iterator<AtlasIndexQuery.Result<CassandraVertex, CassandraEdge>> result = query.vertices(0, 10);
        assertFalse(result.hasNext());
    }

    @Test
    public void testVerticesWithSortReturnsEmpty() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 0);
        Iterator<AtlasIndexQuery.Result<CassandraVertex, CassandraEdge>> result =
                query.vertices(0, 10, "name", org.apache.tinkerpop.gremlin.process.traversal.Order.asc);
        assertFalse(result.hasNext());
    }

    // ======================== vertexTotals ========================

    @Test
    public void testVertexTotalsDefaultMinusOne() {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 0);
        assertEquals(query.vertexTotals().longValue(), -1L);
    }

    // ======================== directIndexQuery when ES unreachable ========================
    // AtlasElasticsearchDatabase initializes a real REST client from atlas-application.properties,
    // so lowLevelRestClient is non-null. Calls fail with AtlasBaseException since no ES is running.

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testDirectIndexQueryThrowsWhenESUnreachable() throws Exception {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 0);
        query.directIndexQuery("{\"query\":{\"match_all\":{}}}");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testDirectEsIndexQueryThrowsWhenESUnreachable() throws Exception {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 0);
        query.directEsIndexQuery("{\"query\":{\"match_all\":{}}}");
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testCountIndexQueryThrowsWhenESUnreachable() throws Exception {
        CassandraIndexQuery query = new CassandraIndexQuery(mockGraph, "vertex_index", "{}", 0);
        query.countIndexQuery("{\"query\":{\"match_all\":{}}}");
    }
}
