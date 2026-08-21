/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.services;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasRunMode;
import org.apache.atlas.DeleteType;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditRowKind;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.model.instance.PurgeSummary;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.purge.PurgeExecutionStats;
import org.apache.atlas.repository.purge.PurgeUtils;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.IAtlasEntityChangeNotifier;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.tasks.GraphClaim;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Integration tests for {@link PurgeService}: scheduled purgeEntities, REST purgeByIds,
 * cron failure handling, and REST/cron overlap.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class PurgeServiceTest extends AtlasTestBase {
    private static final String CRON_ELIGIBLE_GUID = "11111111-1111-1111-1111-111111111111";
    @Inject
    private AtlasTypeDefStore typeDefStore;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasEntityStoreV2 entityStore;

    @Inject
    private AtlasGraph atlasGraph;

    @Inject
    private AtlasAuditService atlasAuditService;

    @BeforeClass
    public void setupClass() throws Exception {
        RequestContext.clear();
        super.initialize();
        basicSetup(typeDefStore, typeRegistry);
        Thread.sleep(1000);
    }

    @BeforeMethod
    public void setupMethod() {
        RequestContext.clear();
        RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);
    }

    @AfterClass
    public void tearDownClass() throws Exception {
        Thread.sleep(1000);
        AtlasGraphProvider.cleanup();
        super.cleanup();
    }

    // -------------------------------------------------------------------------
    // Scheduled purge (PurgeService.purgeEntities)
    // -------------------------------------------------------------------------

    @Test
    public void testPurgeEntities() throws Exception {
        AtlasEntity db = newHiveDb(null);
        persistAndGetGuid(db);
        AtlasEntity tbl = newHiveTable(db, null);
        String tblGuid = persistAndGetGuid(tbl);

        RequestContext.clear();
        entityStore.deleteByIds(Collections.singletonList(tblGuid));

        backdateModificationTimestamp(tblGuid, 31);
        reindexVertices(tblGuid);
        pauseForIndexCreation();

        ApplicationProperties.get().setProperty("atlas.purge.enabled.services", "hive");
        ApplicationProperties.get().setProperty("atlas.purge.workers.count", "1");
        ApplicationProperties.get().setProperty("atlas.purge.worker.batch.size", "1");

        RequestContext.clear();

        Object originalNotifier = injectNotifierSpy(entityStore);
        EntityMutationResponse purgeResponse = createPurgeService().purgeEntities();

        IAtlasEntityChangeNotifier spy = (IAtlasEntityChangeNotifier) getNotifier(entityStore);
        ArgumentCaptor<EntityMutationResponse> cap = ArgumentCaptor.forClass(EntityMutationResponse.class);
        Mockito.verify(spy, Mockito.timeout(5000).atLeastOnce())
                .onEntitiesMutated(cap.capture(), Mockito.eq(false));

        List<AtlasEntityHeader> allPurged = new ArrayList<>();
        for (EntityMutationResponse notified : cap.getAllValues()) {
            assertNotNull(notified);
            List<AtlasEntityHeader> batchPurged = notified.getPurgedEntities();
            if (batchPurged != null) {
                allPurged.addAll(batchPurged);
            }
        }

        assertTrue(allPurged.stream().anyMatch(h -> tblGuid.equals(h.getGuid())));
        restoreNotifier(entityStore, originalNotifier);

        assertNotNull(purgeResponse);
        assertNotNull(purgeResponse.getPurgeSummary());
        assertTrue(purgeResponse.getPurgeSummary().getRequestedCount() > 0,
                "Expected index scan to find at least one purge-eligible entity");

        List<AtlasEntityHeader> responsePurged = purgeResponse.getPurgedEntities();
        if (responsePurged != null && !responsePurged.isEmpty()) {
            long totalPurgedInSummary = purgeResponse.getPurgeSummary().getPurgedCount()
                    + purgeResponse.getPurgeSummary().getPurgedDependenciesCount();
            assertTrue(totalPurgedInSummary > 0,
                    "Expected purge summary to report at least one purged entity");
            assertTrue(responsePurged.stream().anyMatch(h -> tblGuid.equals(h.getGuid())));
        }

        assertFalse(RequestContext.get().isPurgeRequested(),
                "Scheduled purge should reset purgeRequested on the cron thread");
        assertEquals(RequestContext.get().getDeleteType(), DeleteType.DEFAULT,
                "Scheduled purge should reset delete type on the cron thread");
    }

    @Test
    public void scheduledPurge_purgesMultipleIndexHits() throws Exception {
        AtlasEntity db = newHiveDb(null);
        persistAndGetGuid(db);

        String tbl1Guid = persistAndGetGuid(newHiveTable(db, null));
        String tbl2Guid = persistAndGetGuid(newHiveTable(db, null));

        RequestContext.clear();
        entityStore.deleteByIds(Collections.singletonList(tbl1Guid));
        entityStore.deleteByIds(Collections.singletonList(tbl2Guid));

        backdateModificationTimestamp(tbl1Guid, 31);
        backdateModificationTimestamp(tbl2Guid, 31);
        reindexVertices(tbl1Guid, tbl2Guid);
        pauseForIndexCreation();

        ApplicationProperties.get().setProperty("atlas.purge.enabled.services", "hive");
        ApplicationProperties.get().setProperty("atlas.purge.workers.count", "1");
        ApplicationProperties.get().setProperty("atlas.purge.worker.batch.size", "1");

        RequestContext.clear();

        EntityMutationResponse purgeResponse = createPurgeService().purgeEntities();

        assertNotNull(purgeResponse);
        assertNotNull(purgeResponse.getPurgeSummary());
        assertEquals(purgeResponse.getPurgeSummary().getRequestedCount(), 2,
                "Expected two index hits for two purge-eligible tables");

        Set<String> purgedGuids = new HashSet<>();
        if (purgeResponse.getPurgedEntities() != null) {
            for (AtlasEntityHeader header : purgeResponse.getPurgedEntities()) {
                purgedGuids.add(header.getGuid());
            }
        }

        assertTrue(purgedGuids.contains(tbl1Guid), "Expected first table to be purged");
        assertTrue(purgedGuids.contains(tbl2Guid), "Expected second table to be purged");
        assertNull(findByGuidFresh(tbl1Guid), "First table should be removed from graph");
        assertNull(findByGuidFresh(tbl2Guid), "Second table should be removed from graph");

        assertTrue(RequestContext.get().getDeletedEntities().isEmpty(),
                "Cron producer thread should clear expansion delete records after each index hit");
        assertFalse(RequestContext.get().isPurgeRequested());
        assertEquals(RequestContext.get().getDeleteType(), DeleteType.DEFAULT);
    }

    /**
     * Design v3 allows REST purge while scheduled purge is in progress. Overlapping GUIDs must be
     * purged exactly once across both paths.
     */
    @Test
    public void concurrentScheduledAndRestPurgeOverlappingGuids() throws Exception {
        AtlasEntity db = newHiveDb(null);
        String dbGuid = persistAndGetGuid(db);
        String tbl1Guid = persistAndGetGuid(newHiveTable(db, null));
        String tbl2Guid = persistAndGetGuid(newHiveTable(db, null));

        RequestContext.clear();
        entityStore.deleteByIds(Arrays.asList(tbl1Guid, tbl2Guid));

        backdateModificationTimestamp(tbl1Guid, 31);
        backdateModificationTimestamp(tbl2Guid, 31);
        reindexVertices(tbl1Guid, tbl2Guid);
        pauseForIndexCreation();

        ApplicationProperties.get().setProperty("atlas.purge.enabled.services", "hive");
        ApplicationProperties.get().setProperty("atlas.purge.workers.count", "2");
        ApplicationProperties.get().setProperty("atlas.purge.worker.batch.size", "1");

        RequestContext.clear();
        GraphTransactionInterceptor.clearCache();
        try {
            atlasGraph.rollback();
        } catch (Exception ignored) { }

        Set<String> overlapGuids = new HashSet<>(Arrays.asList(tbl1Guid, tbl2Guid));
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicReference<EntityMutationResponse> scheduledResponse = new AtomicReference<>();
        AtomicReference<EntityMutationResponse> restResponse = new AtomicReference<>();
        AtomicReference<Exception> scheduledError = new AtomicReference<>();
        AtomicReference<Exception> restError = new AtomicReference<>();

        PurgeService purgeService = createPurgeService();

        Thread scheduledThread = new Thread(() -> {
            try {
                startGate.await();
                RequestContext.clear();
                scheduledResponse.set(purgeService.purgeEntities());
            } catch (Exception e) {
                scheduledError.set(e);
            } finally {
                RequestContext.clear();
                GraphTransactionInterceptor.clearCache();
            }
        }, "scheduled-purge-overlap");

        Thread restThread = new Thread(() -> {
            try {
                startGate.await();
                RequestContext.clear();
                RequestContext.get().setUser(TestUtilsV2.TEST_USER, null);
                restResponse.set(purgeService.purgeByIds(overlapGuids));
            } catch (Exception e) {
                restError.set(e);
            } finally {
                RequestContext.clear();
                GraphTransactionInterceptor.clearCache();
            }
        }, "rest-purge-overlap");

        scheduledThread.start();
        restThread.start();
        startGate.countDown();
        scheduledThread.join(TimeUnit.SECONDS.toMillis(120));
        restThread.join(TimeUnit.SECONDS.toMillis(120));

        if (scheduledError.get() != null) {
            throw scheduledError.get();
        }
        if (restError.get() != null) {
            throw restError.get();
        }

        EntityMutationResponse scheduledResult = scheduledResponse.get();
        EntityMutationResponse restResult = restResponse.get();
        assertNotNull(scheduledResult, "Scheduled purge should return a response");
        assertNotNull(restResult, "REST purge should return a response");

        assertNoInternalErrorPurgeFailures(scheduledResult, "scheduled purge");
        assertNoInternalErrorPurgeFailures(restResult, "REST purge");

        Map<String, Integer> purgedCountByGuid = new HashMap<>();
        collectPurgedGuidCounts(scheduledResult, purgedCountByGuid);
        collectPurgedGuidCounts(restResult, purgedCountByGuid);

        for (String guid : overlapGuids) {
            assertEquals(purgedCountByGuid.getOrDefault(guid, 0).intValue(), 1,
                    "Each overlapping GUID should be purged exactly once across both paths");
            assertNull(findByGuidFresh(guid), "Purged table should be removed from graph: " + guid);
        }

        assertConcurrentOverlapFailuresAreSkippable(scheduledResult, overlapGuids);
        assertConcurrentOverlapFailuresAreSkippable(restResult, overlapGuids);
        assertEquals(countFailures(scheduledResult, false) + countFailures(restResult, false), 0,
                "Concurrent overlap should not produce non-skippable purge failures");

        assertNotNull(findByGuidFresh(dbGuid), "Parent DB should remain after table purge");
        assertNoOrphanEdgesToPurgedGuids(dbGuid, overlapGuids);
    }

    @Test
    public void cronFailure_beforeAnyGuidsCollected_skipsSummaryAudit() throws Exception {
        AtlasGraph mockGraph = mock(AtlasGraph.class);
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);
        PurgeService purgeService = new PurgeService(mockGraph, mock(AtlasEntityStore.class),
                mock(AtlasTypeRegistry.class), mockAuditService);

        when(mockGraph.indexQuery(eq(Constants.VERTEX_INDEX), anyString()))
                .thenThrow(new RuntimeException("index query failed"));

        EntityMutationResponse response = purgeService.purgeEntities();

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertTrue(summary.getExecutionFailed());
        assertNull(summary.getRunId());

        verify(mockAuditService, never()).add(eq(AuditOperation.AUTO_PURGE), anyString(), anyString(),
                anyLong(), anyString(), eq(AuditRowKind.SUMMARY));
    }

    @Test
    public void cronFailure_afterGuidsCollected_writesSummaryAuditWithRunId() throws Exception {
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);
        PurgeService purgeService = new PurgeService(mock(AtlasGraph.class), mock(AtlasEntityStore.class),
                mock(AtlasTypeRegistry.class), mockAuditService);

        EntityMutationResponse response = new EntityMutationResponse();
        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Collections.singleton(CRON_ELIGIBLE_GUID));
        PurgeExecutionStats stats = new PurgeExecutionStats(originallyRequestedGuids, originallyRequestedGuids.size());
        stats.markExecutionFailed();

        invokeHandleCronPurgeFailure(purgeService, response, stats, originallyRequestedGuids);

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertTrue(summary.getExecutionFailed());
        assertNotNull(summary.getRunId());
        assertTrue(summary.getRequestedCount() > 0);

        verify(mockAuditService).add(eq(AuditOperation.AUTO_PURGE), eq(PurgeUtils.buildGuidParams(originallyRequestedGuids)),
                anyString(), eq(0L), eq(summary.getRunId()), eq(AuditRowKind.SUMMARY));
    }

    // -------------------------------------------------------------------------
    // PurgeService.purgeByIds — pre-validation and orchestration
    // -------------------------------------------------------------------------

    @Test
    public void testPurgeByIdsWithEmptySet() throws Exception {
        try {
            createPurgeService().purgeByIds(new HashSet<>());
            fail("Expected AtlasBaseException for empty GUID set");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
        }

        try {
            createPurgeService().purgeByIds(null);
            fail("Expected AtlasBaseException for null GUID set");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
        }
    }

    @Test
    public void testPurgeByIdsWithNonExistentEntities() throws Exception {
        Set<String> guids = new HashSet<>(Arrays.asList(
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222"));
        EntityMutationResponse response = createPurgeService().purgeByIds(guids);

        assertNotNull(response);
        assertTrue(response.getPurgedEntities() == null || response.getPurgedEntities().isEmpty());
        assertNotNull(response.getFailedEntities());
        assertEquals(response.getFailedEntities().size(), 2);
        assertEquals(response.getFailedEntities().get(0).getErrorCode(),
                AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode());
        assertNotNull(response.getPurgeSummary());
        assertEquals(response.getPurgeSummary().getRequestedCount(), 2);
        assertEquals(response.getPurgeSummary().getSkippedCount(), 2);
    }

    @Test
    public void purgeByIds_invalidUuid() throws Exception {
        EntityMutationResponse response = createPurgeService().purgeByIds(
                new HashSet<>(Collections.singletonList("invalid-uuid-format")));

        assertNotNull(response.getFailedEntities());
        assertEquals(response.getFailedEntities().size(), 1);
        assertEquals(response.getFailedEntities().get(0).getErrorCode(), AtlasErrorCode.INVALID_GUID.getErrorCode());
        assertEquals(response.getPurgeSummary().getFailedCount(), 1);
    }

    @Test
    public void purgeByIds_notInDeletedState() throws Exception {
        String guid = persistAndGetGuid(newHiveDb(null));

        EntityMutationResponse response = createPurgeService().purgeByIds(Collections.singleton(guid));

        assertNotNull(response.getFailedEntities());
        assertEquals(response.getFailedEntities().size(), 1);
        assertEquals(response.getFailedEntities().get(0).getErrorCode(),
                AtlasErrorCode.NOT_IN_DELETED_STATE.getErrorCode());
        assertEquals(response.getPurgeSummary().getSkippedCount(), 1);
    }

    @Test
    public void purgeByIds_success() throws Exception {
        ApplicationProperties.get().setProperty("atlas.purge.workers.count", "1");
        String guid = persistAndGetGuid(newHiveDb(null));
        entityStore.deleteById(guid);

        EntityMutationResponse response = createPurgeService().purgeByIds(Collections.singleton(guid));

        assertNotNull(response.getPurgedEntities());
        assertEquals(response.getPurgedEntities().size(), 1);
        assertEquals(response.getPurgedEntities().get(0).getGuid(), guid);
        assertEquals(response.getPurgeSummary().getPurgedCount(), 1);
    }

    @Test
    public void purgeByIds_partialPreScanAndPurge() throws Exception {
        ApplicationProperties.get().setProperty("atlas.purge.workers.count", "1");
        String guid = persistAndGetGuid(newHiveDb(null));
        entityStore.deleteById(guid);

        Set<String> guids = new HashSet<>(Arrays.asList(guid, "22222222-2222-2222-2222-222222222222"));
        EntityMutationResponse response = createPurgeService().purgeByIds(guids);

        assertNotNull(response.getPurgedEntities());
        assertEquals(response.getPurgedEntities().size(), 1);
        assertEquals(response.getFailedEntities().size(), 1);
        assertEquals(response.getPurgeSummary().getPurgedCount(), 1);
        assertEquals(response.getPurgeSummary().getSkippedCount(), 1);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private PurgeService createPurgeService() {
        return new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService);
    }

    private static void invokeHandleCronPurgeFailure(PurgeService purgeService,
                                                     EntityMutationResponse response,
                                                     PurgeExecutionStats stats,
                                                     Set<String> originallyRequestedGuids) throws Exception {
        Method handleCronPurgeFailure = PurgeService.class.getDeclaredMethod(
                "handleCronPurgeFailure",
                EntityMutationResponse.class,
                PurgeExecutionStats.class,
                Set.class);
        handleCronPurgeFailure.setAccessible(true);
        handleCronPurgeFailure.invoke(purgeService, response, stats, originallyRequestedGuids);
    }

    @Test
    public void testStart_skipsWhenRUNMODE_isNotMetadataServer() throws Exception {
        // PurgeService.start() must be a no-op for NOTIFICATION_PROCESSOR (and INITIALIZER).
        // Set atlas.enable.process.soft.delete=true so it would normally launch the thread.
        ApplicationProperties.get().setProperty("atlas.enable.process.soft.delete", "true");

        PurgeService purgeService = new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService);

        try (org.mockito.MockedStatic<AtlasRunMode> mockedMode =
                     org.mockito.Mockito.mockStatic(AtlasRunMode.class)) {
            AtlasRunMode mockMode = org.mockito.Mockito.mock(AtlasRunMode.class);
            org.mockito.Mockito.when(mockMode.runsMetadataServer()).thenReturn(false);
            mockedMode.when(AtlasRunMode::current).thenReturn(mockMode);

            // Should return without launching the cleanup thread
            purgeService.start();
            // If start() did NOT return early, it would call launchCleanUp() and start a thread.
            // We verify indirectly: no exception, method completes quickly.
        }

        // Reset
        ApplicationProperties.get().setProperty("atlas.enable.process.soft.delete", "false");
    }

    @Test
    public void testStart_skipsCleanupWhenOwnershipAlreadyHeld() throws Exception {
        ensurePurgeOwnershipState("owner-a", System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10));
        ApplicationProperties.get().setProperty("atlas.enable.process.soft.delete", "true");

        PurgeService purgeService = Mockito.spy(new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService));

        try (org.mockito.MockedStatic<AtlasRunMode> mockedMode = org.mockito.Mockito.mockStatic(AtlasRunMode.class)) {
            mockedMode.when(AtlasRunMode::current).thenReturn(AtlasRunMode.METADATA_SERVER);
            purgeService.start();
        }

        verify(purgeService, never()).launchCleanUp(anyString());
        assertEquals(purgeClaimOwner(), "owner-a");
        ApplicationProperties.get().setProperty("atlas.enable.process.soft.delete", "false");
    }

    @Test
    public void testStart_launchesCleanupWhenOwnershipCanBeClaimed() throws Exception {
        ensurePurgeOwnershipState("", 0L);
        ApplicationProperties.get().setProperty("atlas.enable.process.soft.delete", "true");

        PurgeService purgeService = Mockito.spy(new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService));
        doNothing().when(purgeService).launchCleanUp(anyString());

        try (org.mockito.MockedStatic<AtlasRunMode> mockedMode = org.mockito.Mockito.mockStatic(AtlasRunMode.class)) {
            mockedMode.when(AtlasRunMode::current).thenReturn(AtlasRunMode.METADATA_SERVER);
            purgeService.start();
        }

        verify(purgeService).launchCleanUp(anyString());
        assertNotNull(purgeClaimOwner());
        assertTrue(purgeClaimExpiry() > System.currentTimeMillis());
        ApplicationProperties.get().setProperty("atlas.enable.process.soft.delete", "false");
    }

    @Test
    public void tryClaimPurgeOwnership_deniesDifferentOwnerWithActiveLease() throws Exception {
        PurgeService purgeService = createPurgeService();
        ensurePurgeOwnershipState("existing-owner", System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5));

        boolean claimed = invokeTryClaimPurgeOwnership(purgeService, "new-owner", TimeUnit.MINUTES.toMillis(1));

        assertFalse(claimed);
        assertEquals(purgeClaimOwner(), "existing-owner");
    }

    @Test
    public void releasePurgeOwnership_onlyReleasesMatchingOwner() throws Exception {
        PurgeService purgeService = createPurgeService();
        ensurePurgeOwnershipState("owner-to-keep", System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5));

        invokeReleasePurgeOwnership(purgeService, "other-owner");
        assertEquals(purgeClaimOwner(), "owner-to-keep");

        invokeReleasePurgeOwnership(purgeService, "owner-to-keep");
        assertNull(purgeClaimOwner(), "Releasing the claim must leave it available to any node");
    }

    @Test
    public void stop_releasesOwnershipClaimedByThisNode() throws Exception {
        ensurePurgeOwnershipState("", 0L);
        ApplicationProperties.get().setProperty("atlas.enable.process.soft.delete", "true");

        PurgeService purgeService = Mockito.spy(new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService));
        doNothing().when(purgeService).launchCleanUp(anyString());

        try (org.mockito.MockedStatic<AtlasRunMode> mockedMode = org.mockito.Mockito.mockStatic(AtlasRunMode.class)) {
            mockedMode.when(AtlasRunMode::current).thenReturn(AtlasRunMode.METADATA_SERVER);
            purgeService.start();

            assertTrue(StringUtils.isNotBlank(purgeClaimOwner()), "start() should have claimed purge ownership");

            purgeService.stop();
        }

        assertNull(purgeClaimOwner(), "stop() should release the claim taken by this node");

        ApplicationProperties.get().setProperty("atlas.enable.process.soft.delete", "false");
    }

    @Test
    public void stop_leavesOwnershipHeldByAnotherNodeUntouched() throws Exception {
        long leaseUntil = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        ensurePurgeOwnershipState("owner-b", leaseUntil);
        ApplicationProperties.get().setProperty("atlas.enable.process.soft.delete", "true");

        PurgeService purgeService = new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService);

        try (org.mockito.MockedStatic<AtlasRunMode> mockedMode = org.mockito.Mockito.mockStatic(AtlasRunMode.class)) {
            mockedMode.when(AtlasRunMode::current).thenReturn(AtlasRunMode.METADATA_SERVER);
            purgeService.start();
            purgeService.stop();
        }

        assertEquals(purgeClaimOwner(), "owner-b",
                "A node that never claimed ownership must not release another node's lease");
        assertEquals(purgeClaimExpiry().longValue(), leaseUntil);

        ApplicationProperties.get().setProperty("atlas.enable.process.soft.delete", "false");
    }

    @Test
    public void stop_isNoOpWhenPurgeServiceNeverStarted() throws Exception {
        long leaseUntil = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        ensurePurgeOwnershipState("owner-c", leaseUntil);

        new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService).stop();

        assertEquals(purgeClaimOwner(), "owner-c");
        assertEquals(purgeClaimExpiry().longValue(), leaseUntil);
    }

    private AtlasEntity newHiveDb(String nameOpt) {
        String name = nameOpt != null ? nameOpt : RandomStringUtils.randomAlphanumeric(10);
        AtlasEntity db = new AtlasEntity("hive_db");
        db.setAttribute("name", name);
        db.setAttribute("qualifiedName", name);
        db.setAttribute("clusterName", "cl1");
        db.setAttribute("location", "/tmp");
        db.setAttribute("description", "test db");
        return db;
    }

    private AtlasEntity newHiveTable(AtlasEntity db, String nameOpt) {
        String name = nameOpt != null ? nameOpt : RandomStringUtils.randomAlphanumeric(10);
        AtlasEntity tbl = new AtlasEntity("hive_table");
        tbl.setAttribute("name", name);
        tbl.setAttribute("qualifiedName", name);
        tbl.setAttribute("description", "random table");
        tbl.setAttribute("type", "type");
        tbl.setAttribute("tableType", "MANAGED");
        tbl.setAttribute("db", AtlasTypeUtil.getAtlasObjectId(db));
        return tbl;
    }

    private String persistAndGetGuid(AtlasEntity entity) throws AtlasBaseException {
        EntityMutationResponse resp = entityStore.createOrUpdate(
                new AtlasEntityStream(new AtlasEntityWithExtInfo(entity)), false);
        AtlasEntityHeader hdr = resp.getFirstCreatedEntityByTypeName(entity.getTypeName());
        return hdr != null ? hdr.getGuid() : null;
    }

    private AtlasVertex findByGuidFresh(String guid) {
        GraphTransactionInterceptor.clearCache();
        return AtlasGraphUtilsV2.findByGuid(atlasGraph, guid);
    }

    private void backdateModificationTimestamp(String guid, int days) {
        AtlasVertex v = AtlasGraphUtilsV2.findByGuid(atlasGraph, guid);
        if (v != null) {
            long delta = days * 24L * 60 * 60 * 1000;
            long ts = System.currentTimeMillis() - delta;
            AtlasGraphUtilsV2.setProperty(v, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, ts);
            atlasGraph.commit();
            GraphTransactionInterceptor.clearCache();
        }
    }

    private void reindexVertices(String... guids) {
        List<AtlasElement> elements = new ArrayList<>();
        for (String g : guids) {
            if (g == null) {
                continue;
            }
            AtlasVertex v = AtlasGraphUtilsV2.findByGuid(atlasGraph, g);
            if (v != null) {
                elements.add(v);
            }
        }
        if (!elements.isEmpty()) {
            try {
                atlasGraph.getManagementSystem().reindex(Constants.VERTEX_INDEX, elements);
                atlasGraph.getManagementSystem().reindex(Constants.FULLTEXT_INDEX, elements);
                atlasGraph.commit();
                GraphTransactionInterceptor.clearCache();
            } catch (Exception ignored) { }
        }
    }

    private Object injectNotifierSpy(AtlasEntityStoreV2 storeV2) throws Exception {
        Field f = AtlasEntityStoreV2.class.getDeclaredField("entityChangeNotifier");
        f.setAccessible(true);
        Object original = f.get(storeV2);
        Object spy = Mockito.spy(original);
        f.set(storeV2, spy);
        return original;
    }

    private Object getNotifier(AtlasEntityStoreV2 storeV2) throws Exception {
        Field f = AtlasEntityStoreV2.class.getDeclaredField("entityChangeNotifier");
        f.setAccessible(true);
        return f.get(storeV2);
    }

    private void restoreNotifier(AtlasEntityStoreV2 storeV2, Object original) throws Exception {
        Field f = AtlasEntityStoreV2.class.getDeclaredField("entityChangeNotifier");
        f.setAccessible(true);
        f.set(storeV2, original);
    }

    private static void collectPurgedGuidCounts(EntityMutationResponse response, Map<String, Integer> purgedCountByGuid) {
        if (response == null || response.getPurgedEntities() == null) {
            return;
        }

        for (AtlasEntityHeader header : response.getPurgedEntities()) {
            purgedCountByGuid.merge(header.getGuid(), 1, Integer::sum);
        }
    }

    private static void assertNoInternalErrorPurgeFailures(EntityMutationResponse response, String pathLabel) {
        if (response.getFailedEntities() == null) {
            return;
        }

        for (FailedEntity failedEntity : response.getFailedEntities()) {
            assertFalse(AtlasErrorCode.INTERNAL_ERROR.getErrorCode().equals(failedEntity.getErrorCode()),
                    pathLabel + " should not record INTERNAL_ERROR for overlapping GUID handling: "
                            + failedEntity.getGuid());
        }
    }

    private static void assertConcurrentOverlapFailuresAreSkippable(EntityMutationResponse response,
                                                                    Set<String> overlapGuids) {
        if (response.getFailedEntities() == null) {
            return;
        }

        for (FailedEntity failedEntity : response.getFailedEntities()) {
            assertTrue(overlapGuids.contains(failedEntity.getGuid()),
                    "Concurrent overlap failure should reference an overlapping GUID: " + failedEntity.getGuid());
            assertTrue(PurgeUtils.isSkippablePurgeFailureCode(failedEntity.getErrorCode()),
                    "Concurrent overlap failure must be skippable, not " + failedEntity.getErrorCode());
        }
    }

    private static int countFailures(EntityMutationResponse response, boolean skippable) {
        if (response.getFailedEntities() == null) {
            return 0;
        }

        int count = 0;
        for (FailedEntity failedEntity : response.getFailedEntities()) {
            if (PurgeUtils.isSkippablePurgeFailureCode(failedEntity.getErrorCode()) == skippable) {
                count++;
            }
        }

        return count;
    }

    private void assertNoOrphanEdgesToPurgedGuids(String anchorGuid, Set<String> purgedGuids) {
        AtlasVertex anchorVertex = findByGuidFresh(anchorGuid);
        assertNotNull(anchorVertex, "Anchor vertex should exist for orphan-edge check");

        Iterator<AtlasEdge> edges = anchorVertex.getEdges(AtlasEdgeDirection.BOTH).iterator();
        while (edges.hasNext()) {
            AtlasEdge edge = edges.next();
            String outGuid = AtlasGraphUtilsV2.getIdFromVertex(edge.getOutVertex());
            String inGuid = AtlasGraphUtilsV2.getIdFromVertex(edge.getInVertex());
            String otherGuid = anchorGuid.equals(outGuid) ? inGuid : outGuid;

            assertFalse(purgedGuids.contains(otherGuid),
                    "Anchor vertex should not retain edges to purged GUID: " + otherGuid);
        }
    }

    private boolean invokeTryClaimPurgeOwnership(PurgeService purgeService, String ownerId, long leaseMillis) throws Exception {
        Method tryClaimPurgeOwnership = PurgeService.class.getDeclaredMethod("tryClaimPurgeOwnership", String.class, long.class);
        tryClaimPurgeOwnership.setAccessible(true);
        return (boolean) tryClaimPurgeOwnership.invoke(purgeService, ownerId, leaseMillis);
    }

    private void invokeReleasePurgeOwnership(PurgeService purgeService, String ownerId) throws Exception {
        Method releasePurgeOwnership = PurgeService.class.getDeclaredMethod("releasePurgeOwnership", String.class);
        releasePurgeOwnership.setAccessible(true);
        releasePurgeOwnership.invoke(purgeService, ownerId);
    }

    private String purgeClaimOwner() {
        return GraphClaim.claimedBy(GraphClaim.holderOf(atlasGraph, Constants.CLAIM_PURGE));
    }

    private Long purgeClaimExpiry() {
        return GraphClaim.expiryOf(GraphClaim.holderOf(atlasGraph, Constants.CLAIM_PURGE));
    }

    /**
     * Puts the purge claim into a known state.  A blank {@code ownerId} means unclaimed, which is the
     * absence of a claim vertex rather than a vertex with an empty owner.
     */
    private void ensurePurgeOwnershipState(String ownerId, long leaseUntil) {
        AtlasVertex existing = GraphClaim.holderOf(atlasGraph, Constants.CLAIM_PURGE);

        if (existing != null) {
            GraphClaim.releaseClaim(existing);
            atlasGraph.removeVertex(existing);
        }

        if (StringUtils.isNotBlank(ownerId)) {
            AtlasVertex claimVertex = atlasGraph.addVertex();

            AtlasGraphUtilsV2.setEncodedProperty(claimVertex, Constants.CLAIM_VERTEX_TYPE_KEY, Constants.CLAIM_VERTEX_TYPE_NAME);
            GraphClaim.claim(claimVertex, Constants.CLAIM_PURGE, ownerId);
            AtlasGraphUtilsV2.setEncodedProperty(claimVertex, Constants.CLAIM_EXPIRY_KEY, leaseUntil);
        }

        atlasGraph.commit();
        GraphTransactionInterceptor.clearCache();
    }
}
