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
import org.apache.atlas.DeleteType;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.purge.PurgeUtils;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.IAtlasEntityChangeNotifier;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.TestLoadModelUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = org.apache.atlas.TestModules.TestOnlyModule.class)
public class PurgeServiceTest extends AtlasTestBase {
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
    public void setup() throws Exception {
        RequestContext.clear();
        super.initialize();
        TestLoadModelUtils.loadBaseModel(typeDefStore, typeRegistry);
        TestLoadModelUtils.loadHiveModel(typeDefStore, typeRegistry);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) { }
    }

    @Test
    public void testPurgeEntities() throws Exception {
        // Approach (pre-commit check on async flow):
        // - Spy store's IAtlasEntityChangeNotifier and capture onEntitiesMutated(response,false) per
        //   worker batch to assert the purged GUID before commit/locking paths.
        // Create DB and table
        AtlasEntity db = newHiveDb(null);
        String dbGuid = persistAndGetGuid(db);
        AtlasEntity tbl = newHiveTable(db, null);
        String tblGuid = persistAndGetGuid(tbl);

        // Clear context so a prior test's purge flags do not turn deleteByIds into purge mode
        RequestContext.clear();

        // Soft-delete table
        EntityMutationResponse del = entityStore.deleteByIds(Collections.singletonList(tblGuid));
        assertNotNull(del);

        // Backdate timestamp beyond default 30d retention and reindex to reflect in Solr
        backdateModificationTimestamp(tblGuid, 31);
        reindexVertices(tblGuid);
        pauseForIndexCreation();

        // Enable hive purge and single worker for determinism
        ApplicationProperties.get().setProperty("atlas.purge.enabled.services", "hive");
        ApplicationProperties.get().setProperty("atlas.purge.workers.count", "1");
        ApplicationProperties.get().setProperty("atlas.purge.worker.batch.size", "1");

        // Clear context to avoid skipping due to prior delete tracking
        RequestContext.clear();

        // Inject notifier spy to capture pre-commit signal (pre-commit verification point)
        Object originalNotifier = injectNotifierSpy(entityStore);

        EntityMutationResponse purgeResponse = new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService).purgeEntities();

        // Verify notifier calls (one per worker batch; batch size 1 => one call per deletion candidate)
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

        // Restore original notifier to leave the store in a clean state
        restoreNotifier(entityStore, originalNotifier);

        // Verify purge response; notifier capture above is the pre-commit signal per worker batch.
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
    public void testPurgeEntitiesMultipleIndexHits() throws Exception {
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

        Object originalNotifier = injectNotifierSpy(entityStore);

        EntityMutationResponse purgeResponse = new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService).purgeEntities();

        restoreNotifier(entityStore, originalNotifier);

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
     * purged exactly once across both paths. When a path attempts to purge a GUID already removed
     * by the other path, it records {@code INSTANCE_GUID_NOT_FOUND} as a skip — not
     * {@code INTERNAL_ERROR}. Skippable failures are not required on every concurrent run (e.g.
     * each path may win a different GUID, or scheduled index scan may miss entities REST already
     * hard-purged).
     */
    @Test
    public void testConcurrentScheduledAndRestPurgeOverlappingGuids() throws Exception {
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

        // Setup mutations above can leave an open graph txn on this thread; release it before
        // concurrent purge workers start so main does not hold graphindex read locks.
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

        PurgeService purgeService = new PurgeService(atlasGraph, entityStore, typeRegistry, atlasAuditService);

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
                restResponse.set(entityStore.purgeByIds(overlapGuids));
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
        EntityMutationResponse resp = entityStore.createOrUpdate(new AtlasEntityStream(new AtlasEntityWithExtInfo(entity)), false);
        String typeName = entity.getTypeName();
        AtlasEntityHeader hdr = resp.getFirstCreatedEntityByTypeName(typeName);
        String guid = hdr != null ? hdr.getGuid() : null;
        return guid;
    }

    private AtlasVertex findByGuidFresh(String guid) {
        // WIM purge runs on worker threads; main-thread guidVertexCache can retain stale handles.
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

    // Test helper: swap store's private notifier field with a Mockito spy so we can
    // capture and assert the pre-commit mutation response invoked by the store.
    private Object injectNotifierSpy(AtlasEntityStoreV2 storeV2) throws Exception {
        Field f = AtlasEntityStoreV2.class.getDeclaredField("entityChangeNotifier");
        f.setAccessible(true);
        Object original = f.get(storeV2);
        Object spy = Mockito.spy(original);
        f.set(storeV2, spy);
        return original;
    }

    // Test helper: fetch the (spied) notifier instance currently installed on the store.
    private Object getNotifier(AtlasEntityStoreV2 storeV2) throws Exception {
        Field f = AtlasEntityStoreV2.class.getDeclaredField("entityChangeNotifier");
        f.setAccessible(true);
        return f.get(storeV2);
    }

    // Test helper: restore the original notifier instance after verification.
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
}
