/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.notification.task.AtlasDistributedTaskNotificationSender;
import org.apache.atlas.repository.Constants;
import org.janusgraph.util.encoding.LongEncoding;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.service.redis.RedisService;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.http.HttpEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class BulkPurgeServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TEST_CONNECTION_QN = "default/snowflake/1234567890";

    static {
        try {
            PropertiesConfiguration config = new PropertiesConfiguration();
            config.setProperty("atlas.graph.storage.hostname", "localhost");
            config.setProperty("atlas.graph.index.search.hostname", "localhost:9200");
            ApplicationProperties.set(config);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize test configuration", e);
        }
    }

    @Mock private AtlasGraph       mockGraph;
    @Mock private RedisService     mockRedisService;
    @Mock private EntityAuditRepository mockAuditRepository;
    @Mock private AtlasDistributedTaskNotificationSender mockTaskNotificationSender;
    @Mock private AtlasVertex      mockConnectionVertex;
    @Mock private RestClient       mockEsClient;

    private BulkPurgeService bulkPurgeService;
    private MockedStatic<AtlasGraphUtilsV2> mockedGraphUtils;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        RequestContext.clear();
        RequestContext.get();

        bulkPurgeService = new BulkPurgeService(
                mockGraph, mockRedisService, Set.of(mockAuditRepository), mockTaskNotificationSender);

        // Inject the mock ES client directly — avoids thread-scoped MockedStatic issues
        bulkPurgeService.setEsClient(mockEsClient);

        // AtlasGraphUtilsV2 is only called from the test thread (in submitPurge validation)
        mockedGraphUtils = mockStatic(AtlasGraphUtilsV2.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (mockedGraphUtils != null) mockedGraphUtils.close();
        RequestContext.clear();
        // Give async tasks a moment to finish to avoid thread leaks
        Thread.sleep(100);
    }

    // ======================== Validation Tests ========================

    @Test
    void testBulkPurgeByConnection_nullConnectionQN_throwsException() {
        assertThrows(AtlasBaseException.class,
                () -> bulkPurgeService.bulkPurgeByConnection(null, "admin", false));
    }

    @Test
    void testBulkPurgeByConnection_emptyConnectionQN_throwsException() {
        assertThrows(AtlasBaseException.class,
                () -> bulkPurgeService.bulkPurgeByConnection("", "admin", false));
    }

    @Test
    void testBulkPurgeByConnection_connectionNotFound_throwsException() {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(null);

        AtlasBaseException ex = assertThrows(AtlasBaseException.class,
                () -> bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false));
        assertTrue(ex.getMessage().contains("Connection not found"));
    }

    @Test
    void testBulkPurgeByQualifiedName_nullPrefix_throwsException() {
        assertThrows(AtlasBaseException.class,
                () -> bulkPurgeService.bulkPurgeByQualifiedName(null, "admin"));
    }

    @Test
    void testBulkPurgeByQualifiedName_shortPrefix_throwsException() {
        assertThrows(AtlasBaseException.class,
                () -> bulkPurgeService.bulkPurgeByQualifiedName("short", "admin"));

        AtlasBaseException ex = assertThrows(AtlasBaseException.class,
                () -> bulkPurgeService.bulkPurgeByQualifiedName("123456789", "admin"));
        assertTrue(ex.getMessage().contains("at least 10 characters"));
    }

    // ======================== Submission Tests ========================

    @Test
    void testBulkPurgeByConnection_validInput_returnsRequestId() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        assertNotNull(requestId);
        assertFalse(requestId.isEmpty());
        // UUID format check
        assertEquals(36, requestId.length());
    }

    @Test
    void testBulkPurgeByQualifiedName_validInput_returnsRequestId() throws Exception {
        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");

        String requestId = bulkPurgeService.bulkPurgeByQualifiedName("default/snowflake/1234567890", "admin");

        assertNotNull(requestId);
        assertEquals(36, requestId.length());
    }

    @Test
    void testBulkPurgeByConnection_alreadyRunning_throwsException() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        // Simulate an active purge with recent heartbeat
        String runningStatus = MAPPER.writeValueAsString(Map.of(
                "status", "RUNNING",
                "requestId", "existing-request-id",
                "lastHeartbeat", System.currentTimeMillis()
        ));
        when(mockRedisService.getValue("bulk_purge:" + TEST_CONNECTION_QN)).thenReturn(runningStatus);

        AtlasBaseException ex = assertThrows(AtlasBaseException.class,
                () -> bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false));
        assertTrue(ex.getMessage().contains("already in progress"));
    }

    @Test
    void testBulkPurgeByConnection_staleRunning_allowsResubmit() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        // Simulate a stale purge (heartbeat > 5 minutes ago)
        String staleStatus = MAPPER.writeValueAsString(Map.of(
                "status", "RUNNING",
                "requestId", "stale-request-id",
                "lastHeartbeat", System.currentTimeMillis() - (6 * 60 * 1000)
        ));
        when(mockRedisService.getValue("bulk_purge:" + TEST_CONNECTION_QN)).thenReturn(staleStatus);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");

        // Should NOT throw — stale lock should be overridden
        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);
        assertNotNull(requestId);
    }

    // ======================== Status Tests ========================

    @Test
    void testGetStatus_notFound_returnsNull() {
        when(mockRedisService.getValue(anyString())).thenReturn(null);

        Map<String, Object> status = bulkPurgeService.getStatus("nonexistent-id");
        assertNull(status);
    }

    @Test
    void testGetStatus_fromRedis_returnsMap() throws Exception {
        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("requestId", "test-id");
        expected.put("status", "COMPLETED");
        expected.put("deletedCount", 1000);

        when(mockRedisService.getValue("bulk_purge_request:test-id"))
                .thenReturn(MAPPER.writeValueAsString(expected));

        Map<String, Object> status = bulkPurgeService.getStatus("test-id");
        assertNotNull(status);
        assertEquals("test-id", status.get("requestId"));
        assertEquals("COMPLETED", status.get("status"));
    }

    // ======================== Cancel Tests ========================

    @Test
    void testCancelPurge_noActivePurge_returnsFalse() {
        assertFalse(bulkPurgeService.cancelPurge("nonexistent-id"));
    }

    @Test
    void testCancelPurge_activePurge_returnsTrue() throws Exception {
        // Submit a purge to create an active context
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // Set up ES mock to make purge take a while (large count, slow scroll)
        setupFullEsMock(100000, Collections.emptyList());

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Cancel immediately — purge is in activePurges synchronously after submit
        boolean cancelled = bulkPurgeService.cancelPurge(requestId);
        assertTrue(cancelled);
    }

    // ======================== Batch Processing Tests ========================

    @Test
    void testProcessBatch_deletesVerticesAndEdges() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // ES _id is base-36 encoded; graph.getVertex() expects the decoded long as a string
        String esId1 = LongEncoding.encode(1001L);
        String esId2 = LongEncoding.encode(1002L);
        setupFullEsMock(2, Arrays.asList(esId1, esId2));

        // Mock graph operations — use decoded long vertex IDs
        AtlasVertex v1 = mock(AtlasVertex.class);
        AtlasVertex v2 = mock(AtlasVertex.class);
        AtlasEdge edge1 = mock(AtlasEdge.class);

        when(mockGraph.getVertex("1001")).thenReturn(v1);
        when(mockGraph.getVertex("1002")).thenReturn(v2);
        when(v1.getEdges(AtlasEdgeDirection.BOTH)).thenReturn(Collections.singletonList(edge1));
        when(v1.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class))).thenReturn(Collections.emptyList());
        when(v2.getEdges(AtlasEdgeDirection.BOTH)).thenReturn(Collections.emptyList());
        when(v2.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class))).thenReturn(Collections.emptyList());

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify graph operations occurred (using timeout for async)
        verify(mockGraph, timeout(5000).atLeastOnce()).getVertex("1001");
        verify(mockGraph, timeout(5000).atLeastOnce()).getVertex("1002");
        verify(mockGraph, timeout(5000).atLeastOnce()).removeEdge(edge1);
        verify(mockGraph, timeout(5000).atLeastOnce()).removeVertex(v1);
        verify(mockGraph, timeout(5000).atLeastOnce()).removeVertex(v2);
        verify(mockGraph, timeout(5000).atLeastOnce()).commit();
    }

    @Test
    void testProcessBatch_nullVertex_skippedGracefully() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        String esId = LongEncoding.encode(9999L);
        setupFullEsMock(1, Arrays.asList(esId));

        // Vertex already deleted — returns null
        when(mockGraph.getVertex("9999")).thenReturn(null);

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify commit was called (batch completes even with null vertices)
        verify(mockGraph, timeout(5000).atLeastOnce()).commit();
        // Verify no removeVertex was called (vertex was null)
        verify(mockGraph, never()).removeVertex(any());
    }

    @Test
    void testProcessBatch_lockFailure_setsFailed() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(false);

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // The status should be stored in Redis with FAILED (using timeout for async)
        verify(mockRedisService, timeout(5000).atLeastOnce()).putValue(
                argThat(key -> key.startsWith("bulk_purge:")),
                argThat(json -> json.contains("FAILED")),
                anyInt());
    }

    // ======================== Lineage Collection Tests ========================

    @Test
    void testCollectExternalLineageVertices_crossConnection() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        String esId = LongEncoding.encode(5001L);
        setupFullEsMock(1, Arrays.asList(esId));

        // Create a Process vertex with cross-connection lineage edges
        AtlasVertex processVertex = mock(AtlasVertex.class);
        AtlasVertex externalVertex = mock(AtlasVertex.class);
        AtlasEdge lineageEdge = mock(AtlasEdge.class);

        when(mockGraph.getVertex("5001")).thenReturn(processVertex);
        when(processVertex.getId()).thenReturn("5001");
        when(processVertex.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class)).thenReturn(true);
        when(processVertex.getEdges(AtlasEdgeDirection.BOTH)).thenReturn(Collections.singletonList(lineageEdge));

        // Lineage edge pointing to an external connection vertex
        when(processVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.singletonList(lineageEdge));
        when(lineageEdge.getInVertex()).thenReturn(processVertex);
        when(lineageEdge.getOutVertex()).thenReturn(externalVertex);
        when(externalVertex.getId()).thenReturn("external-vertex-id");
        when(externalVertex.getProperty(Constants.CONNECTION_QUALIFIED_NAME, String.class))
                .thenReturn("other/connection/9999");
        when(externalVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class))
                .thenReturn("Table");

        // For lineage repair: graph.getVertex for external vertex
        when(mockGraph.getVertex("external-vertex-id")).thenReturn(externalVertex);
        // External vertex has no remaining active lineage edges (the process was purged)
        when(externalVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify lineage was repaired synchronously: __hasLineage set to false on external vertex.
        // setEncodedProperty ultimately calls element.setProperty(), so verify on the mock vertex.
        verify(externalVertex, timeout(5000).atLeastOnce())
                .setProperty(eq(org.apache.atlas.type.Constants.HAS_LINEAGE), eq(false));
    }

    // ======================== Audit Event Tests ========================

    @Test
    void testWriteSummaryAuditEvent_afterCompletion() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        setupFullEsMock(0, Collections.emptyList()); // No entities — fast path

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify audit event was written (using timeout for async)
        verify(mockAuditRepository, timeout(5000).atLeastOnce()).putEventsV2(any(EntityAuditEventV2.class));
    }

    // ======================== Worker Count Auto-Scaling Tests ========================

    @Test
    void testWorkerCountAutoScaling() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // 5000 entities → should auto-scale to 1 worker (< 10000)
        setupFullEsMock(5000, Collections.emptyList()); // empty scroll to finish quickly

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify workerCount=1 was set in Redis status (using timeout for async)
        verify(mockRedisService, timeout(5000).atLeastOnce()).putValue(
                argThat(key -> key.startsWith("bulk_purge:")),
                argThat(json -> json.contains("\"workerCount\":1")),
                anyInt());
    }

    // ======================== PurgeContext Tests ========================

    @Test
    void testPurgeContext_toStatusMap_containsAllFields() {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-123", "default/snowflake/1234567890", "CONNECTION", "admin",
                "{\"query\":{\"prefix\":{\"__qualifiedNameHierarchy\":\"default/snowflake/1234567890/\"}}}", true);
        ctx.status = "RUNNING";
        ctx.totalDiscovered = 5000;
        ctx.workerCount = 4;
        ctx.totalDeleted.set(2500);
        ctx.totalFailed.set(10);
        ctx.completedBatches.set(25);
        ctx.lastProcessedBatchIndex = 24;
        ctx.lastHeartbeat = System.currentTimeMillis();

        Map<String, Object> statusMap = ctx.toStatusMap();

        assertEquals("req-123", statusMap.get("requestId"));
        assertEquals("default/snowflake/1234567890", statusMap.get("purgeKey"));
        assertEquals("CONNECTION", statusMap.get("purgeMode"));
        assertEquals("RUNNING", statusMap.get("status"));
        assertEquals("admin", statusMap.get("submittedBy"));
        assertEquals(5000L, statusMap.get("totalDiscovered"));
        assertEquals(2500, statusMap.get("deletedCount"));
        assertEquals(10, statusMap.get("failedCount"));
        assertEquals(25, statusMap.get("completedBatches"));
        assertEquals(24, statusMap.get("lastProcessedBatchIndex"));
        assertEquals(4, statusMap.get("workerCount"));
        assertEquals(true, statusMap.get("deleteConnection"));
        assertEquals(false, statusMap.get("connectionDeleted"));
        assertFalse(statusMap.containsKey("error"));
    }

    @Test
    void testPurgeContext_toStatusMap_includesErrorWhenSet() {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-456", "key", "CONNECTION", "admin", "{}", false);
        ctx.status = "FAILED";
        ctx.error = "Connection timeout";

        Map<String, Object> statusMap = ctx.toStatusMap();
        assertEquals("FAILED", statusMap.get("status"));
        assertEquals("Connection timeout", statusMap.get("error"));
    }

    @Test
    void testPurgeContext_toJson_producesValidJson() throws Exception {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-789", "key", "QUALIFIED_NAME_PREFIX", "admin", "{}", false);
        ctx.status = "PENDING";

        String json = ctx.toJson();
        assertNotNull(json);
        assertFalse(json.isEmpty());

        // Should be parseable JSON
        Map parsed = MAPPER.readValue(json, Map.class);
        assertEquals("req-789", parsed.get("requestId"));
        assertEquals("PENDING", parsed.get("status"));
    }

    // ======================== BatchWork Tests ========================

    @Test
    void testBatchWork_poisonPill_isIdentifiable() {
        assertSame(BulkPurgeService.BatchWork.POISON_PILL, BulkPurgeService.BatchWork.POISON_PILL);
        assertEquals(-1, BulkPurgeService.BatchWork.POISON_PILL.batchIndex);
        assertTrue(BulkPurgeService.BatchWork.POISON_PILL.vertexIds.isEmpty());
    }

    @Test
    void testBatchWork_holdsVertexIds() {
        List<String> ids = Arrays.asList("v1", "v2", "v3");
        BulkPurgeService.BatchWork work = new BulkPurgeService.BatchWork(ids, 5);

        assertEquals(3, work.vertexIds.size());
        assertEquals(5, work.batchIndex);
        assertEquals("v1", work.vertexIds.get(0));
    }

    // ======================== ES Query Builder Tests ========================

    @Test
    void testBulkPurgeByConnection_generatesPrefixQueryWithTrailingSlash() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // Capture the ES query sent to the _count endpoint
        setupFullEsMock(0, Collections.emptyList());

        bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify ES _count request contains a prefix query with trailing "/"
        // on __qualifiedNameHierarchy so the Connection entity itself is excluded
        verify(mockEsClient, timeout(5000).atLeastOnce()).performRequest(argThat(request -> {
            if (!request.getEndpoint().contains("_count")) return false;
            try {
                String body = new String(request.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
                return body.contains("prefix") &&
                       body.contains(Constants.QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY) &&
                       body.contains(TEST_CONNECTION_QN + "/");
            } catch (Exception e) {
                return false;
            }
        }));
    }

    // ======================== Helper Methods ========================

    /**
     * Sets up a unified ES mock that routes requests based on endpoint.
     * Uses thenAnswer to create fresh response objects per call, avoiding
     * ByteArrayInputStream reuse issues.
     */
    private void setupFullEsMock(long count, List<String> vertexIds) throws Exception {
        when(mockEsClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request req = invocation.getArgument(0);
            String endpoint = req.getEndpoint();
            String method = req.getMethod();

            if (endpoint.contains("_count")) {
                return newMockResponse("{\"count\":" + count + "}");
            }
            if (endpoint.contains("_delete_by_query")) {
                return newMockResponse("{\"deleted\":0}");
            }
            if ("DELETE".equals(method)) {
                return newMockResponse("{}");
            }
            if (endpoint.equals("/_search/scroll")) {
                // Scroll continuation: always return empty hits
                return newMockResponse("{\"_scroll_id\":\"sid\",\"hits\":{\"hits\":[]}}");
            }
            if (endpoint.contains("_search")) {
                // Initial scroll: return vertex IDs
                StringBuilder hitsArray = new StringBuilder("[");
                for (int i = 0; i < vertexIds.size(); i++) {
                    if (i > 0) hitsArray.append(",");
                    hitsArray.append("{\"_id\":\"").append(vertexIds.get(i)).append("\"}");
                }
                hitsArray.append("]");
                return newMockResponse("{\"_scroll_id\":\"sid\",\"hits\":{\"hits\":" + hitsArray + "}}");
            }

            return newMockResponse("{}");
        });
    }

    /**
     * Creates a fresh mock Response with a new ByteArrayInputStream each time.
     */
    private Response newMockResponse(String body) {
        try {
            Response response = mock(Response.class);
            HttpEntity entity = mock(HttpEntity.class);
            when(entity.getContent()).thenReturn(
                    new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
            when(response.getEntity()).thenReturn(entity);
            return response;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
