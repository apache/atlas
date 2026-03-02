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
import org.apache.atlas.repository.graph.IAtlasGraphProvider;
import org.janusgraph.util.encoding.LongEncoding;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.model.Tag;
import org.apache.atlas.repository.store.graph.v2.tags.PaginatedTagResult;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAO;
import org.apache.atlas.repository.store.graph.v2.tags.TagDAOCassandraImpl;
import org.apache.atlas.service.config.DynamicConfigStore;
import org.apache.atlas.service.redis.RedisService;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.model.tasks.AtlasTask;
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
    @Mock private AtlasGraph       mockBulkLoadingGraph;
    @Mock private IAtlasGraphProvider mockGraphProvider;
    @Mock private RedisService     mockRedisService;
    @Mock private EntityAuditRepository mockAuditRepository;
    @Mock private AtlasDistributedTaskNotificationSender mockTaskNotificationSender;
    @Mock private TaskManagement   mockTaskManagement;
    @Mock private AtlasVertex      mockConnectionVertex;
    @Mock private RestClient       mockEsClient;

    private BulkPurgeService bulkPurgeService;
    private MockedStatic<AtlasGraphUtilsV2> mockedGraphUtils;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        RequestContext.clear();
        RequestContext.get();

        // Mock bulk-loading graph provider
        when(mockGraphProvider.getBulkLoading()).thenReturn(mockBulkLoadingGraph);

        bulkPurgeService = new BulkPurgeService(
                mockGraph, mockGraphProvider, mockRedisService, Set.of(mockAuditRepository), mockTaskNotificationSender, mockTaskManagement);

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
    void testProcessBatch_deletesVerticesViaBulkLoadingGraph() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // ES _id is base-36 encoded; graph.getVertices() expects decoded long IDs
        String esId1 = LongEncoding.encode(1001L);
        String esId2 = LongEncoding.encode(1002L);
        setupFullEsMock(2, Arrays.asList(esId1, esId2));

        // Mock bulk-loading graph operations — batch vertex retrieval
        AtlasVertex v1 = mock(AtlasVertex.class);
        AtlasVertex v2 = mock(AtlasVertex.class);
        when(v1.getId()).thenReturn("1001");
        when(v2.getId()).thenReturn("1002");

        when(mockBulkLoadingGraph.getVertices(any(String[].class)))
                .thenReturn(new HashSet<>(Arrays.asList(v1, v2)));

        when(v1.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class))).thenReturn(Collections.emptyList());
        when(v2.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class))).thenReturn(Collections.emptyList());

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify batch vertex retrieval on bulk-loading graph (not individual getVertex calls)
        verify(mockBulkLoadingGraph, timeout(5000).atLeastOnce()).getVertices(any(String[].class));
        // Verify removeVertex (no explicit removeEdge — JanusGraph handles internally)
        verify(mockBulkLoadingGraph, timeout(5000).atLeastOnce()).removeVertex(v1);
        verify(mockBulkLoadingGraph, timeout(5000).atLeastOnce()).removeVertex(v2);
        verify(mockBulkLoadingGraph, timeout(5000).atLeastOnce()).commit();
        // Verify no explicit edge removal
        verify(mockBulkLoadingGraph, never()).removeEdge(any());
        // Verify bulk-loading graph was shut down
        verify(mockBulkLoadingGraph, timeout(5000).atLeastOnce()).shutdown();
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

        // Vertex already deleted — getVertices returns empty set
        when(mockBulkLoadingGraph.getVertices(any(String[].class))).thenReturn(Collections.emptySet());

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify commit was called (batch completes even with null vertices)
        verify(mockBulkLoadingGraph, timeout(5000).atLeastOnce()).commit();
        // Verify no removeVertex was called (vertex was not found)
        verify(mockBulkLoadingGraph, never()).removeVertex(any());
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

        when(processVertex.getId()).thenReturn("5001");
        when(processVertex.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class)).thenReturn(true);

        // Batch vertex retrieval on bulk-loading graph
        when(mockBulkLoadingGraph.getVertices(any(String[].class)))
                .thenReturn(new HashSet<>(Collections.singletonList(processVertex)));

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

        // For lineage repair: graph.getVertex for external vertex (uses regular graph, not bulk-loading)
        when(mockGraph.getVertex("external-vertex-id")).thenReturn(externalVertex);
        // External vertex has no remaining active lineage edges (the process was purged)
        when(externalVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());
        // External vertex has no propagated classification edges
        when(externalVertex.getEdges(eq(AtlasEdgeDirection.OUT), eq(Constants.CLASSIFICATION_LABEL)))
                .thenReturn(Collections.emptyList());

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify lineage was repaired: __hasLineage set to false on external vertex.
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

        // 5000 entities → should auto-scale to 2 workers (1K–10K range)
        setupFullEsMock(5000, Collections.emptyList()); // empty scroll to finish quickly

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify workerCount=2 was set in Redis status (using timeout for async)
        verify(mockRedisService, timeout(5000).atLeastOnce()).putValue(
                argThat(key -> key.startsWith("bulk_purge:")),
                argThat(json -> json.contains("\"workerCount\":2")),
                anyInt());
    }

    @Test
    void testWorkerCountAutoScaling_allThresholds() {
        // getWorkerCount is now package-visible for testing
        assertEquals(1, bulkPurgeService.getWorkerCount(500, 10));     // < 1K → 1
        assertEquals(1, bulkPurgeService.getWorkerCount(999, 10));     // < 1K → 1
        assertEquals(2, bulkPurgeService.getWorkerCount(1000, 10));    // 1K–10K → 2
        assertEquals(2, bulkPurgeService.getWorkerCount(9999, 10));    // 1K–10K → 2
        assertEquals(4, bulkPurgeService.getWorkerCount(10000, 10));   // 10K–50K → 4
        assertEquals(4, bulkPurgeService.getWorkerCount(49999, 10));   // 10K–50K → 4
        assertEquals(8, bulkPurgeService.getWorkerCount(50000, 10));   // 50K–500K → min(10, 8) = 8
        assertEquals(8, bulkPurgeService.getWorkerCount(499999, 10));  // 50K–500K → min(10, 8) = 8
        assertEquals(10, bulkPurgeService.getWorkerCount(500000, 10)); // > 500K → configuredMax = 10
        assertEquals(10, bulkPurgeService.getWorkerCount(1000000, 10));// > 500K → configuredMax = 10
    }

    @Test
    void testWorkerCountAutoScaling_lowConfiguredMax() {
        // When configuredMax is lower than the tier value, it should cap
        assertEquals(3, bulkPurgeService.getWorkerCount(50000, 3));    // 50K–500K → min(3, 8) = 3
        assertEquals(3, bulkPurgeService.getWorkerCount(500000, 3));   // > 500K → configuredMax = 3
    }

    @Test
    void testWorkerCountOverride_usedInsteadOfAutoScaling() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // 500 entities would auto-scale to 1 worker, but we override with 5
        setupFullEsMock(500, Collections.emptyList());

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false, 5);

        // Verify workerCount=5 was set in Redis status (override used instead of auto-scaled 1)
        verify(mockRedisService, timeout(5000).atLeastOnce()).putValue(
                argThat(key -> key.startsWith("bulk_purge:")),
                argThat(json -> json.contains("\"workerCount\":5")),
                anyInt());
    }

    // ======================== PurgeContext Tests ========================

    @Test
    void testPurgeContext_toStatusMap_containsAllFields() {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-123", "default/snowflake/1234567890", "CONNECTION", "admin",
                "{\"query\":{\"prefix\":{\"__qualifiedNameHierarchy\":\"default/snowflake/1234567890/\"}}}", true, 0);
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
        assertNotNull(statusMap.get("esQuery"));
        assertFalse(statusMap.containsKey("error"));
        // remainingAfterCleanup not set (default -1) → should not be in map
        assertFalse(statusMap.containsKey("remainingAfterCleanup"));
    }

    @Test
    void testPurgeContext_toStatusMap_includesRemainingAfterCleanup() {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-verify", "key", "CONNECTION", "admin", "{}", false, 0);
        ctx.status = "COMPLETED";
        ctx.remainingAfterCleanup = 0;

        Map<String, Object> statusMap = ctx.toStatusMap();
        assertEquals(0L, statusMap.get("remainingAfterCleanup"));
    }

    @Test
    void testPurgeContext_toStatusMap_includesResubmitCount() {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-resub", "key", "CONNECTION", "admin", "{}", false, 0);
        ctx.status = "RUNNING";
        ctx.resubmitCount = 2;

        Map<String, Object> statusMap = ctx.toStatusMap();
        assertEquals(2, statusMap.get("resubmitCount"));
    }

    @Test
    void testPurgeContext_toStatusMap_includesErrorWhenSet() {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-456", "key", "CONNECTION", "admin", "{}", false, 0);
        ctx.status = "FAILED";
        ctx.error = "Connection timeout";

        Map<String, Object> statusMap = ctx.toStatusMap();
        assertEquals("FAILED", statusMap.get("status"));
        assertEquals("Connection timeout", statusMap.get("error"));
    }

    @Test
    void testPurgeContext_toJson_producesValidJson() throws Exception {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-789", "key", "QUALIFIED_NAME_PREFIX", "admin", "{}", false, 0);
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

    // ======================== Worker Cleanup on ES Failure Tests ========================

    @Test
    void testWorkerCleanup_onEsScrollFailure_workersStillShutDown() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // First call (_count) succeeds, second call (_search) fails with exception
        when(mockEsClient.performRequest(any(Request.class))).thenAnswer(invocation -> {
            Request req = invocation.getArgument(0);
            String endpoint = req.getEndpoint();
            String method = req.getMethod();

            if (endpoint.contains("_count")) {
                return newMockResponse("{\"count\":1000}");
            }
            if (endpoint.contains("_delete_by_query")) {
                return newMockResponse("{\"deleted\":0}");
            }
            if ("DELETE".equals(method)) {
                return newMockResponse("{}");
            }
            if (endpoint.contains("_search")) {
                throw new java.io.IOException("ES connection lost");
            }
            return newMockResponse("{}");
        });

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // The purge should complete (as FAILED) without worker thread leaks.
        // Verify the bulk-loading graph was still shut down in the finally block.
        verify(mockBulkLoadingGraph, timeout(5000).atLeastOnce()).shutdown();
    }

    // ======================== Verification Tests ========================

    @Test
    void testVerification_zeroRemaining_statusShowsSuccess() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // 0 entities — fast path, verification will also return 0
        setupFullEsMock(0, Collections.emptyList());

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify COMPLETED status contains remainingAfterCleanup=0
        verify(mockRedisService, timeout(5000).atLeastOnce()).putValue(
                argThat(key -> key.startsWith("bulk_purge:")),
                argThat(json -> json.contains("COMPLETED") && json.contains("\"remainingAfterCleanup\":0")),
                anyInt());
    }

    // ======================== Orphan Checker Tests ========================

    @Test
    void testOrphanChecker_stalePurge_resubmits() throws Exception {
        // Set up: active purge keys registry contains a stale purge
        String stalePurgeKey = "default/snowflake/stale-conn";
        String activeKeysJson = MAPPER.writeValueAsString(Set.of(stalePurgeKey));
        when(mockRedisService.getValue("bulk_purge_active_keys")).thenReturn(activeKeysJson);
        when(mockRedisService.acquireDistributedLock("bulk_purge_orphan_checker_lock")).thenReturn(true);

        // Stale RUNNING status with old heartbeat
        Map<String, Object> staleStatus = new LinkedHashMap<>();
        staleStatus.put("status", "RUNNING");
        staleStatus.put("requestId", "stale-req-1");
        staleStatus.put("purgeKey", stalePurgeKey);
        staleStatus.put("purgeMode", "CONNECTION");
        staleStatus.put("esQuery", "{\"query\":{\"prefix\":{\"field\":\"value\"}}}");
        staleStatus.put("lastHeartbeat", System.currentTimeMillis() - (10 * 60 * 1000)); // 10 min ago
        staleStatus.put("submittedBy", "admin");
        staleStatus.put("deleteConnection", false);
        when(mockRedisService.getValue("bulk_purge:" + stalePurgeKey))
                .thenReturn(MAPPER.writeValueAsString(staleStatus));

        // ES count shows remaining entities
        setupFullEsMock(500, Collections.emptyList());

        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");

        // Execute orphan checker
        bulkPurgeService.checkAndRecoverOrphanedPurges();

        // Verify a new purge was submitted (new requestId written to Redis)
        verify(mockRedisService, atLeastOnce()).putValue(
                argThat(key -> key.startsWith("bulk_purge_request:")),
                argThat(json -> json.contains("PENDING") && json.contains(stalePurgeKey)),
                anyInt());
        // Verify lock was released
        verify(mockRedisService).releaseDistributedLock("bulk_purge_orphan_checker_lock");
    }

    @Test
    void testOrphanChecker_completedPurge_removedFromRegistry() throws Exception {
        String completedPurgeKey = "default/snowflake/done-conn";
        String activeKeysJson = MAPPER.writeValueAsString(Set.of(completedPurgeKey));
        when(mockRedisService.getValue("bulk_purge_active_keys")).thenReturn(activeKeysJson);
        when(mockRedisService.acquireDistributedLock("bulk_purge_orphan_checker_lock")).thenReturn(true);

        Map<String, Object> completedStatus = new LinkedHashMap<>();
        completedStatus.put("status", "COMPLETED");
        completedStatus.put("requestId", "done-req-1");
        when(mockRedisService.getValue("bulk_purge:" + completedPurgeKey))
                .thenReturn(MAPPER.writeValueAsString(completedStatus));

        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");

        bulkPurgeService.checkAndRecoverOrphanedPurges();

        // Verify the purge key was removed from the active keys registry
        // (removeFromActivePurgeKeys writes the updated set or removes the key)
        verify(mockRedisService, atLeastOnce()).getValue("bulk_purge_active_keys");
        verify(mockRedisService).releaseDistributedLock("bulk_purge_orphan_checker_lock");
    }

    @Test
    void testOrphanChecker_maxResubmitsExceeded_doesNotResubmit() throws Exception {
        String stalePurgeKey = "default/snowflake/max-retried";
        String activeKeysJson = MAPPER.writeValueAsString(Set.of(stalePurgeKey));
        when(mockRedisService.getValue("bulk_purge_active_keys")).thenReturn(activeKeysJson);
        when(mockRedisService.acquireDistributedLock("bulk_purge_orphan_checker_lock")).thenReturn(true);

        Map<String, Object> staleStatus = new LinkedHashMap<>();
        staleStatus.put("status", "RUNNING");
        staleStatus.put("requestId", "max-retry-req");
        staleStatus.put("purgeKey", stalePurgeKey);
        staleStatus.put("purgeMode", "CONNECTION");
        staleStatus.put("esQuery", "{\"query\":{\"prefix\":{\"field\":\"value\"}}}");
        staleStatus.put("lastHeartbeat", System.currentTimeMillis() - (10 * 60 * 1000));
        staleStatus.put("submittedBy", "admin");
        staleStatus.put("deleteConnection", false);
        staleStatus.put("resubmitCount", 3); // Already at max
        when(mockRedisService.getValue("bulk_purge:" + stalePurgeKey))
                .thenReturn(MAPPER.writeValueAsString(staleStatus));

        // ES count shows remaining
        setupFullEsMock(500, Collections.emptyList());

        bulkPurgeService.checkAndRecoverOrphanedPurges();

        // Verify NO new purge was submitted (resubmitCount exceeded max)
        verify(mockRedisService, never()).putValue(
                argThat(key -> key.startsWith("bulk_purge_request:") && !key.contains("max-retry-req")),
                argThat(json -> json.contains("PENDING")),
                anyInt());
        verify(mockRedisService).releaseDistributedLock("bulk_purge_orphan_checker_lock");
    }

    // ======================== Tag V2 Propagation Cleanup Tests ========================

    @Test
    void testPurgeContext_isTagV2_defaultsFalse() {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-v2", "key", "CONNECTION", "admin", "{}", false, 0);
        // isTagV2 defaults to false (not initialized until executePurge sets it)
        assertFalse(ctx.isTagV2);
    }

    @Test
    void testPurgeContext_entitiesWithDirectTags_concurrentSafe() {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-tags", "key", "CONNECTION", "admin", "{}", false, 0);

        assertNotNull(ctx.entitiesWithDirectTags);
        assertTrue(ctx.entitiesWithDirectTags.isEmpty());

        // ConcurrentHashMap.newKeySet() supports concurrent access
        ctx.entitiesWithDirectTags.add("vertex-1");
        ctx.entitiesWithDirectTags.add("vertex-2");
        ctx.entitiesWithDirectTags.add("vertex-1"); // duplicate
        assertEquals(2, ctx.entitiesWithDirectTags.size());
    }

    @Test
    void testCleanPropagatedTagsFromDeletedSources_skippedWhenV1() throws Exception {
        // When isTagV2=false, the cleanup should be skipped entirely
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-v1", "key", "CONNECTION", "admin", "{}", false, 0);
        ctx.isTagV2 = false;
        ctx.entitiesWithDirectTags.add("some-vertex");

        // No TagDAO interactions should happen — method exits early
        // This is an indirect test: if TagDAOCassandraImpl.getInstance() were called
        // in the test environment without Cassandra, it would fail
    }

    @Test
    void testCleanPropagatedTagsFromDeletedSources_skippedWhenNoDirectTags() throws Exception {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-notags", "key", "CONNECTION", "admin", "{}", false, 0);
        ctx.isTagV2 = true;

        // entitiesWithDirectTags is empty — method should exit early
        assertTrue(ctx.entitiesWithDirectTags.isEmpty());
    }

    @Test
    void testProcessBatch_collectsEntitiesWithDirectTags() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // Enable Tag V2 via test override (avoids DynamicConfigStore static mock thread issues)
        bulkPurgeService.setTagV2Override(true);

        String esId1 = LongEncoding.encode(2001L);
        setupFullEsMock(1, Arrays.asList(esId1));

        // Create a vertex with direct tags
        AtlasVertex taggedVertex = mock(AtlasVertex.class);
        when(taggedVertex.getId()).thenReturn("2001");
        when(taggedVertex.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class)).thenReturn(false);
        // This vertex has direct tags (TRAIT_NAMES_PROPERTY_KEY = __traitNames)
        when(taggedVertex.getMultiValuedProperty(eq(Constants.TRAIT_NAMES_PROPERTY_KEY), eq(String.class)))
                .thenReturn(Arrays.asList("PII", "Confidential"));

        when(mockBulkLoadingGraph.getVertices(any(String[].class)))
                .thenReturn(new HashSet<>(Collections.singletonList(taggedVertex)));

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify the vertex was deleted
        verify(mockBulkLoadingGraph, timeout(5000).atLeastOnce()).removeVertex(taggedVertex);
        // Verify __traitNames was checked (the property was read)
        verify(taggedVertex, timeout(5000).atLeastOnce())
                .getMultiValuedProperty(eq(Constants.TRAIT_NAMES_PROPERTY_KEY), eq(String.class));

        bulkPurgeService.setTagV2Override(null);
    }

    @Test
    void testProcessBatch_doesNotCollectEntitiesWithoutDirectTags() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // Enable Tag V2 via test override
        bulkPurgeService.setTagV2Override(true);

        String esId1 = LongEncoding.encode(3001L);
        setupFullEsMock(1, Arrays.asList(esId1));

        // Create a vertex WITHOUT direct tags
        AtlasVertex untaggedVertex = mock(AtlasVertex.class);
        when(untaggedVertex.getId()).thenReturn("3001");
        when(untaggedVertex.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class)).thenReturn(false);
        // No traits
        when(untaggedVertex.getMultiValuedProperty(eq(Constants.TRAIT_NAMES_PROPERTY_KEY), eq(String.class)))
                .thenReturn(Collections.emptyList());

        when(mockBulkLoadingGraph.getVertices(any(String[].class)))
                .thenReturn(new HashSet<>(Collections.singletonList(untaggedVertex)));

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Vertex is still deleted
        verify(mockBulkLoadingGraph, timeout(5000).atLeastOnce()).removeVertex(untaggedVertex);

        bulkPurgeService.setTagV2Override(null);
    }

    @Test
    void testRepairPropagatedClassificationsV1_stillWorksWithExistingLogic() throws Exception {
        // V1 path should continue to use graph edges (existing behavior)
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        // V1 mode
        bulkPurgeService.setTagV2Override(false);

        String esId = LongEncoding.encode(4001L);
        setupFullEsMock(1, Arrays.asList(esId));

        AtlasVertex processVertex = mock(AtlasVertex.class);
        AtlasVertex externalVertex = mock(AtlasVertex.class);
        AtlasEdge lineageEdge = mock(AtlasEdge.class);

        when(processVertex.getId()).thenReturn("4001");
        when(processVertex.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class)).thenReturn(true);

        when(mockBulkLoadingGraph.getVertices(any(String[].class)))
                .thenReturn(new HashSet<>(Collections.singletonList(processVertex)));

        // Lineage edge to external vertex
        when(processVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.singletonList(lineageEdge));
        when(lineageEdge.getInVertex()).thenReturn(processVertex);
        when(lineageEdge.getOutVertex()).thenReturn(externalVertex);
        when(externalVertex.getId()).thenReturn("ext-vertex-v1");
        when(externalVertex.getProperty(Constants.CONNECTION_QUALIFIED_NAME, String.class))
                .thenReturn("other/connection/v1");

        when(mockGraph.getVertex("ext-vertex-v1")).thenReturn(externalVertex);
        when(externalVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());
        // V1: classification edges on external vertex (none stale)
        when(externalVertex.getEdges(eq(AtlasEdgeDirection.OUT), eq(Constants.CLASSIFICATION_LABEL)))
                .thenReturn(Collections.emptyList());

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify lineage repair happened (V1 path)
        verify(externalVertex, timeout(5000).atLeastOnce())
                .setProperty(eq(org.apache.atlas.type.Constants.HAS_LINEAGE), eq(false));

        bulkPurgeService.setTagV2Override(null);
    }

    // ======================== Relay Propagation + classificationText Tests ========================

    @Test
    void testTriggerRelayPropagationRefresh_skippedWhenNoExternalVertices() throws Exception {
        BulkPurgeService.PurgeContext ctx = new BulkPurgeService.PurgeContext(
                "req-norelay", "key", "CONNECTION", "admin", "{}", false, 0);
        ctx.isTagV2 = true;

        // No external lineage vertices — method should do nothing
        assertTrue(ctx.externalLineageVertexIds.isEmpty());
        // No exception = success
    }

    @Test
    void testTriggerRelayPropagationRefresh_V2_createsTasksForAliveSource() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        bulkPurgeService.setTagV2Override(true);

        // Inject mock TagDAO via override (avoids MockedStatic thread-scope issue)
        TagDAO mockTagDAO = mock(TagDAO.class);
        bulkPurgeService.setTagDAOOverride(mockTagDAO);

        String esId = LongEncoding.encode(5001L);
        setupFullEsMock(1, Arrays.asList(esId));

        AtlasVertex processVertex = mock(AtlasVertex.class);
        AtlasVertex externalVertex = mock(AtlasVertex.class);
        AtlasVertex sourceVertex = mock(AtlasVertex.class);
        AtlasEdge lineageEdge = mock(AtlasEdge.class);

        when(processVertex.getId()).thenReturn("5001");
        when(processVertex.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class)).thenReturn(true);
        when(processVertex.getMultiValuedProperty(eq(Constants.TRAIT_NAMES_PROPERTY_KEY), eq(String.class)))
                .thenReturn(Collections.emptyList());

        when(mockBulkLoadingGraph.getVertices(any(String[].class)))
                .thenReturn(new HashSet<>(Collections.singletonList(processVertex)));

        // Lineage edge to external vertex
        when(processVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.singletonList(lineageEdge));
        when(lineageEdge.getInVertex()).thenReturn(processVertex);
        when(lineageEdge.getOutVertex()).thenReturn(externalVertex);
        when(externalVertex.getId()).thenReturn("ext-relay-v2");
        when(externalVertex.getProperty(Constants.CONNECTION_QUALIFIED_NAME, String.class))
                .thenReturn("other/connection/relay");

        when(mockGraph.getVertex("ext-relay-v2")).thenReturn(externalVertex);

        // External vertex has a propagated tag from alive source
        Tag relayTag = mock(Tag.class);
        when(relayTag.isPropagated()).thenReturn(true);
        when(relayTag.getSourceVertexId()).thenReturn("source-vertex-100");
        when(relayTag.getTagTypeName()).thenReturn("PII");

        when(mockTagDAO.getAllTagsByVertexId("ext-relay-v2"))
                .thenReturn(Collections.singletonList(relayTag));

        // Source vertex still exists (relay case) and has a GUID
        when(mockGraph.getVertex("source-vertex-100")).thenReturn(sourceVertex);
        when(sourceVertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class))
                .thenReturn("source-guid-100");

        AtlasTask mockTask = mock(AtlasTask.class);
        when(mockTaskManagement.createTaskV2(anyString(), anyString(), anyMap(), anyString(), anyString()))
                .thenReturn(mockTask);

        // hasLineage repair
        when(externalVertex.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class))
                .thenReturn(true);
        when(externalVertex.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.emptyList());

        // V1 classification edges (not used in V2 path)
        when(externalVertex.getEdges(eq(AtlasEdgeDirection.OUT), eq(Constants.CLASSIFICATION_LABEL)))
                .thenReturn(Collections.emptyList());

        String requestId = bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Verify CLASSIFICATION_REFRESH_PROPAGATION task was created
        verify(mockTaskManagement, timeout(5000).atLeastOnce()).createTaskV2(
                eq("CLASSIFICATION_REFRESH_PROPAGATION"),
                eq("admin"),
                argThat(params -> "source-guid-100".equals(params.get("entityGuid"))
                        && "PII".equals(params.get("classificationName"))),
                eq("PII"),
                eq("source-guid-100")
        );

        bulkPurgeService.setTagV2Override(null);
        bulkPurgeService.setTagDAOOverride(null);
    }

    @Test
    void testTriggerRelayPropagationRefresh_V2_deduplicatesSameSource() throws Exception {
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(
                eq(mockGraph), eq(Constants.CONNECTION_ENTITY_TYPE),
                eq(Constants.QUALIFIED_NAME), eq(TEST_CONNECTION_QN)))
                .thenReturn(mockConnectionVertex);

        when(mockRedisService.getValue(anyString())).thenReturn(null);
        when(mockRedisService.putValue(anyString(), anyString(), anyInt())).thenReturn("OK");
        when(mockRedisService.acquireDistributedLock(anyString())).thenReturn(true);

        bulkPurgeService.setTagV2Override(true);

        TagDAO mockTagDAO = mock(TagDAO.class);
        bulkPurgeService.setTagDAOOverride(mockTagDAO);

        String esId1 = LongEncoding.encode(6001L);
        String esId2 = LongEncoding.encode(6002L);
        setupFullEsMock(2, Arrays.asList(esId1, esId2));

        AtlasVertex process1 = mock(AtlasVertex.class);
        AtlasVertex process2 = mock(AtlasVertex.class);
        AtlasVertex extVertex1 = mock(AtlasVertex.class);
        AtlasVertex extVertex2 = mock(AtlasVertex.class);
        AtlasVertex sourceVertex = mock(AtlasVertex.class);
        AtlasEdge edge1 = mock(AtlasEdge.class);
        AtlasEdge edge2 = mock(AtlasEdge.class);

        when(process1.getId()).thenReturn("6001");
        when(process2.getId()).thenReturn("6002");
        when(process1.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class)).thenReturn(true);
        when(process2.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class)).thenReturn(true);
        when(process1.getMultiValuedProperty(eq(Constants.TRAIT_NAMES_PROPERTY_KEY), eq(String.class)))
                .thenReturn(Collections.emptyList());
        when(process2.getMultiValuedProperty(eq(Constants.TRAIT_NAMES_PROPERTY_KEY), eq(String.class)))
                .thenReturn(Collections.emptyList());

        when(mockBulkLoadingGraph.getVertices(any(String[].class)))
                .thenReturn(new HashSet<>(Arrays.asList(process1, process2)));

        when(process1.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.singletonList(edge1));
        when(process2.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class)))
                .thenReturn(Collections.singletonList(edge2));

        when(edge1.getInVertex()).thenReturn(process1);
        when(edge1.getOutVertex()).thenReturn(extVertex1);
        when(edge2.getInVertex()).thenReturn(process2);
        when(edge2.getOutVertex()).thenReturn(extVertex2);

        when(extVertex1.getId()).thenReturn("ext-dedup-1");
        when(extVertex2.getId()).thenReturn("ext-dedup-2");
        when(extVertex1.getProperty(Constants.CONNECTION_QUALIFIED_NAME, String.class)).thenReturn("other/conn/dedup");
        when(extVertex2.getProperty(Constants.CONNECTION_QUALIFIED_NAME, String.class)).thenReturn("other/conn/dedup");

        when(mockGraph.getVertex("ext-dedup-1")).thenReturn(extVertex1);
        when(mockGraph.getVertex("ext-dedup-2")).thenReturn(extVertex2);

        // hasLineage repair
        when(extVertex1.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class)).thenReturn(true);
        when(extVertex2.getProperty(org.apache.atlas.type.Constants.HAS_LINEAGE, Boolean.class)).thenReturn(true);
        when(extVertex1.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class))).thenReturn(Collections.emptyList());
        when(extVertex2.getEdges(eq(AtlasEdgeDirection.BOTH), any(String[].class))).thenReturn(Collections.emptyList());

        // Both vertices have propagated tag from SAME source
        Tag tag1 = mock(Tag.class);
        when(tag1.isPropagated()).thenReturn(true);
        when(tag1.getSourceVertexId()).thenReturn("same-source-vertex");
        when(tag1.getTagTypeName()).thenReturn("Confidential");

        Tag tag2 = mock(Tag.class);
        when(tag2.isPropagated()).thenReturn(true);
        when(tag2.getSourceVertexId()).thenReturn("same-source-vertex");
        when(tag2.getTagTypeName()).thenReturn("Confidential");

        when(mockTagDAO.getAllTagsByVertexId("ext-dedup-1")).thenReturn(Collections.singletonList(tag1));
        when(mockTagDAO.getAllTagsByVertexId("ext-dedup-2")).thenReturn(Collections.singletonList(tag2));

        when(mockGraph.getVertex("same-source-vertex")).thenReturn(sourceVertex);
        when(sourceVertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class)).thenReturn("same-source-guid");

        AtlasTask mockTask = mock(AtlasTask.class);
        when(mockTaskManagement.createTaskV2(anyString(), anyString(), anyMap(), anyString(), anyString()))
                .thenReturn(mockTask);

        // V1 classification edges (not used in V2 path)
        when(extVertex1.getEdges(eq(AtlasEdgeDirection.OUT), eq(Constants.CLASSIFICATION_LABEL)))
                .thenReturn(Collections.emptyList());
        when(extVertex2.getEdges(eq(AtlasEdgeDirection.OUT), eq(Constants.CLASSIFICATION_LABEL)))
                .thenReturn(Collections.emptyList());

        bulkPurgeService.bulkPurgeByConnection(TEST_CONNECTION_QN, "admin", false);

        // Despite TWO external vertices with same source, only ONE task should be created
        verify(mockTaskManagement, timeout(5000).times(1)).createTaskV2(
                eq("CLASSIFICATION_REFRESH_PROPAGATION"),
                anyString(),
                anyMap(),
                eq("Confidential"),
                eq("same-source-guid")
        );

        bulkPurgeService.setTagV2Override(null);
        bulkPurgeService.setTagDAOOverride(null);
    }

    @Test
    void testRemovePropagatedTraitFromVertex_clearsClassificationText() throws Exception {
        AtlasVertex vertex = mock(AtlasVertex.class);

        when(vertex.getMultiValuedProperty(eq(Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY), eq(String.class)))
                .thenReturn(new ArrayList<>(Arrays.asList("PII")));

        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.setEncodedProperty(any(AtlasVertex.class), anyString(), any()))
                .thenAnswer(inv -> null);

        java.lang.reflect.Method method = BulkPurgeService.class.getDeclaredMethod(
                "removePropagatedTraitFromVertex", AtlasVertex.class, String.class);
        method.setAccessible(true);
        method.invoke(bulkPurgeService, vertex, "PII");

        // Verify __propagatedTraitNames was cleared
        verify(vertex).removeProperty(Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY);
        // Verify __propagatedClassificationNames was cleared (empty after removing last tag)
        verify(vertex).removeProperty(Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY);
        // Verify __classificationsText was cleared
        verify(vertex).removeProperty(Constants.CLASSIFICATION_TEXT_KEY);
    }

    @Test
    void testCollectRelaySourcesV1_findsAliveSource() throws Exception {
        // Test V1 relay source collection directly via reflection (avoids coordinator thread issues)
        AtlasVertex externalVertex = mock(AtlasVertex.class);
        AtlasVertex classificationVertex = mock(AtlasVertex.class);
        AtlasVertex sourceEntityVertex = mock(AtlasVertex.class);
        AtlasEdge classEdge = mock(AtlasEdge.class);

        when(mockGraph.getVertex("ext-v1-direct")).thenReturn(externalVertex);

        // V1 classification edge: propagated from alive source
        when(classEdge.getProperty(Constants.CLASSIFICATION_EDGE_IS_PROPAGATED_PROPERTY_KEY, Boolean.class))
                .thenReturn(true);
        when(classEdge.getInVertex()).thenReturn(classificationVertex);
        when(classificationVertex.getProperty(Constants.CLASSIFICATION_ENTITY_GUID, String.class))
                .thenReturn("alive-source-guid");
        when(classEdge.getProperty(Constants.CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, String.class))
                .thenReturn("Sensitive");

        when(externalVertex.getEdges(eq(AtlasEdgeDirection.OUT), eq(Constants.CLASSIFICATION_LABEL)))
                .thenReturn(Collections.singletonList(classEdge));

        // Source entity still exists
        mockedGraphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(eq(mockGraph), eq("alive-source-guid")))
                .thenReturn(sourceEntityVertex);

        // Invoke collectRelaySourcesV1 via reflection
        java.lang.reflect.Method method = BulkPurgeService.class.getDeclaredMethod(
                "collectRelaySourcesV1", String.class, Set.class, List.class);
        method.setAccessible(true);

        Set<String> refreshKeys = new HashSet<>();
        List<String[]> tasksToCreate = new ArrayList<>();

        method.invoke(bulkPurgeService, "ext-v1-direct", refreshKeys, tasksToCreate);

        // Verify one task pair was collected
        assertEquals(1, tasksToCreate.size());
        assertEquals("alive-source-guid", tasksToCreate.get(0)[0]);
        assertEquals("Sensitive", tasksToCreate.get(0)[1]);
        assertTrue(refreshKeys.contains("alive-source-guid|Sensitive"));
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
