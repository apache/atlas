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
package org.apache.atlas.repository.purge;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.DeleteType;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestModules;
import org.apache.atlas.TestUtilsV2;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.model.audit.AuditSearchParameters;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.model.instance.PurgeSummary;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.ogm.AtlasAuditEntryDTO;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.bootstrap.AtlasTypeDefStoreInitializer;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStoreV2;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStream;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.IAtlasEntityChangeNotifier;
import org.apache.atlas.services.PurgeAuditWriter;
import org.apache.atlas.services.PurgeService;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
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
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Consolidated ATLAS-5317 purge tests: audit write/read helpers, scheduled purge,
 * REST/cron overlap, and end-to-end delete + purge + audit search.
 */
@Guice(modules = TestModules.TestOnlyModule.class)
public class PurgeTest extends AtlasTestBase {
    private static final String TEST_RUN_ID                  = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    private static final String CLIENT_HOST                  = "127.0.0.0";
    private static final String DEFAULT_USER                 = "Admin";
    private static final String AUDIT_PARAMETER_RESOURCE_DIR = "auditSearchParameters";

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

    private AtlasDiscoveryService discoveryService;
    private AtlasAuditService     readPathAuditService;

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
        discoveryService     = mock(AtlasDiscoveryService.class);
        readPathAuditService = new AtlasAuditService(mock(DataAccess.class), discoveryService);
    }

    @AfterClass
    public void tearDownClass() throws Exception {
        Thread.sleep(1000);
        AtlasGraphProvider.cleanup();
        super.cleanup();
    }

    // -------------------------------------------------------------------------
    // Audit write/read (PurgeAuditWriter, AtlasAuditService purge run lookup)
    // -------------------------------------------------------------------------

    @Test
    public void auditWritePath_writesBatchAndSummaryAudits() throws Exception {
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);

        Set<String> batchGuids = new LinkedHashSet<>(Arrays.asList("guid2", "guid1"));
        EntityMutationResponse response = new EntityMutationResponse();
        AtlasEntityHeader header1 = new AtlasEntityHeader();
        header1.setGuid("guid1");
        AtlasEntityHeader header2 = new AtlasEntityHeader();
        header2.setGuid("guid2");
        response.addEntity(EntityOperation.PURGE, header1);
        response.addEntity(EntityOperation.PURGE, header2);

        PurgeAuditWriter.writeBatch(mockAuditService, AuditOperation.PURGE, TEST_RUN_ID, batchGuids, response);

        ArgumentCaptor<String> batchParamsCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> batchResultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), batchParamsCaptor.capture(), batchResultCaptor.capture(), eq(2L), eq(TEST_RUN_ID));

        assertEquals(batchParamsCaptor.getValue(), "guid1,guid2");
        assertEquals(batchResultCaptor.getValue(), "guid1,guid2");

        AtlasAuditEntry batchEntry = new AtlasAuditEntry();
        batchEntry.setResult(batchResultCaptor.getValue());
        assertFalse(PurgeUtils.isPurgeSummaryAudit(batchEntry));

        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Arrays.asList(
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222"));
        PurgeExecutionStats stats = new PurgeExecutionStats(originallyRequestedGuids, originallyRequestedGuids.size());

        AtlasEntityHeader purged = new AtlasEntityHeader();
        purged.setGuid("11111111-1111-1111-1111-111111111111");
        stats.recordBatchOutcome(Arrays.asList(purged), null, originallyRequestedGuids);

        PurgeAuditWriter.finishRun(mockAuditService, AuditOperation.PURGE, TEST_RUN_ID, originallyRequestedGuids, stats);

        ArgumentCaptor<String> summaryParamsCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> summaryResultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), summaryParamsCaptor.capture(), summaryResultCaptor.capture(), eq(1L), eq(TEST_RUN_ID));

        assertEquals(summaryParamsCaptor.getValue(), "11111111-1111-1111-1111-111111111111,22222222-2222-2222-2222-222222222222");

        PurgeSummary summary = AtlasJson.fromJson(summaryResultCaptor.getValue(), PurgeSummary.class);
        assertEquals(summary.getRunId(), TEST_RUN_ID);
        assertEquals(summary.getRequestedCount(), 2);
        assertEquals(summary.getPurgedCount(), 1);

        AtlasAuditEntry summaryEntry = new AtlasAuditEntry();
        summaryEntry.setResult(summaryResultCaptor.getValue());
        assertTrue(PurgeUtils.isPurgeSummaryAudit(summaryEntry));
    }

    @Test
    public void auditWritePath_writesBatchAuditWhenNothingPurged() throws Exception {
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);

        Set<String> batchGuids = new LinkedHashSet<>(Arrays.asList("guid-b", "guid-a"));
        EntityMutationResponse response = new EntityMutationResponse();
        response.addFailedEntity(new FailedEntity("guid-a", "ATLAS-500-00-001", "batch failed"));
        response.addFailedEntity(new FailedEntity("guid-b", "ATLAS-500-00-001", "batch failed"));

        PurgeAuditWriter.writeBatch(mockAuditService, AuditOperation.PURGE, TEST_RUN_ID, batchGuids, response);

        ArgumentCaptor<String> batchParamsCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> batchResultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), batchParamsCaptor.capture(),
                batchResultCaptor.capture(), eq(0L), eq(TEST_RUN_ID));

        assertEquals(batchParamsCaptor.getValue(), "guid-a,guid-b");
        assertEquals(batchResultCaptor.getValue(), "");

        AtlasAuditEntry batchEntry = new AtlasAuditEntry();
        batchEntry.setOperation(AuditOperation.PURGE);
        batchEntry.setResult(batchResultCaptor.getValue());
        assertTrue(PurgeUtils.isPurgeBatchAudit(batchEntry));
        assertFalse(PurgeUtils.isPurgeSummaryAudit(batchEntry));
    }

    @Test
    public void auditReadPath_getPurgeBatchAuditGuidsForRun_pagesUntilEmpty() throws Exception {
        when(discoveryService.searchWithParameters(any(SearchParameters.class))).thenAnswer(invocation -> {
            SearchParameters params = invocation.getArgument(0);
            int limit  = params.getLimit();
            int offset = params.getOffset();

            AtlasSearchResult result = new AtlasSearchResult(params);
            if (offset == 0) {
                List<AtlasEntityHeader> fullPage = new ArrayList<>();
                for (int i = 0; i < limit; i++) {
                    fullPage.add(buildAuditHeader("batch-guid-" + i, "entity-" + i));
                }
                result.setEntities(fullPage);
            } else if (offset == limit) {
                result.setEntities(Arrays.asList(buildAuditHeader("batch-guid-last", "entity-last")));
            } else {
                result.setEntities(new ArrayList<>());
            }
            return result;
        });

        AtlasAuditEntry summaryEntry = new AtlasAuditEntry();
        summaryEntry.setRunId(TEST_RUN_ID);

        List<String> batchGuids = readPathAuditService.getPurgeBatchAuditGuidsForRun(summaryEntry);

        assertEquals(batchGuids.size(), AtlasConfiguration.SEARCH_MAX_LIMIT.getInt() + 1);
    }

    @Test
    public void auditReadPath_filtersSummaryAndMergesBatchRows() throws Exception {
        PurgeSummary summary = new PurgeSummary(3, 2, 1, 0, 0, 0);
        summary.setRunId(TEST_RUN_ID);

        AtlasAuditEntry summaryEntry = new AtlasAuditEntry();
        summaryEntry.setGuid("summary-guid");
        summaryEntry.setRunId(TEST_RUN_ID);
        summaryEntry.setResult(AtlasJson.toJson(summary));

        Map<Integer, List<AtlasEntityHeader>> pagesByOffset = new HashMap<>();
        pagesByOffset.put(0, Arrays.asList(
                buildAuditHeader("summary-guid", AtlasJson.toJson(summary)),
                buildAuditHeader("batch-guid-1", "entity-1,entity-2"),
                buildAuditHeader("batch-guid-2", "entity-3")));

        when(discoveryService.searchWithParameters(any(SearchParameters.class))).thenAnswer(invocation -> {
            SearchParameters params = invocation.getArgument(0);
            AtlasSearchResult result = new AtlasSearchResult(params);
            result.setEntities(pagesByOffset.getOrDefault(params.getOffset(), new ArrayList<>()));
            return result;
        });

        assertEquals(readPathAuditService.getPurgeBatchAuditGuidsForRun(summaryEntry),
                Arrays.asList("batch-guid-1", "batch-guid-2"));
        assertEquals(readPathAuditService.getPurgedEntityGuidsForRun(summaryEntry),
                Arrays.asList("entity-1", "entity-2", "entity-3"));
    }

    // -------------------------------------------------------------------------
    // Scheduled purge (PurgeService.purgeEntities)
    // -------------------------------------------------------------------------

    @Test
    public void scheduledPurge_purgesEligibleDeletedEntity() throws Exception {
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

    // -------------------------------------------------------------------------
    // End-to-end delete + purge + audit search
    // -------------------------------------------------------------------------

    @Test
    public void deleteThenPurge_writesAuditsSearchableByAdminFilters() throws Exception {
        AtlasTypesDef sampleTypes   = TestUtilsV2.defineDeptEmployeeTypes();
        AtlasTypesDef typesToCreate = AtlasTypeDefStoreInitializer.getTypesToCreate(sampleTypes, typeRegistry);

        if (!typesToCreate.isEmpty()) {
            typeDefStore.createTypesDef(typesToCreate);
        }

        AtlasEntitiesWithExtInfo deptEg2      = TestUtilsV2.createDeptEg2();
        AtlasEntityStream        entityStream = new AtlasEntityStream(deptEg2);
        EntityMutationResponse   emr          = entityStore.createOrUpdate(entityStream, false);

        pauseForIndexCreation();

        assertNotNull(emr);
        assertNotNull(emr.getCreatedEntities());
        assertFalse(emr.getCreatedEntities().isEmpty());

        List<String> guids = emr.getCreatedEntities().stream()
                .map(AtlasEntityHeader::getGuid)
                .collect(Collectors.toList());

        EntityMutationResponse deleteResponse = entityStore.deleteByIds(guids);
        pauseForIndexCreation();

        assertSortedGuidsMatch(emr.getCreatedEntities(), deleteResponse.getDeletedEntities(), "deleteByIds");

        ApplicationProperties.get().setProperty("atlas.purge.workers.count", "1");

        Date startTimestamp = new Date();
        EntityMutationResponse purgeResponse = createPurgeService().purgeByIds(new HashSet<>(guids));

        pauseForIndexCreation();
        assertPurgeSucceededForRequestedGuids(guids, purgeResponse);

        atlasAuditService.add(DEFAULT_USER, AuditOperation.PURGE, CLIENT_HOST, startTimestamp, new Date(),
                guids.toString(), purgeResponse.getPurgedEntitiesIds(), purgeResponse.getPurgedEntities().size());

        assertAuditEntry(atlasAuditService, createAuditParameter("audit-search-parameter-without-filter"));
        assertAuditEntry(atlasAuditService, createAuditParameter("audit-search-parameter-purge"));
    }

    // -------------------------------------------------------------------------
    // PurgeService.purgeByIds — pre-validation and orchestration
    // -------------------------------------------------------------------------

    @Test
    public void purgeByIds_rejectsEmptyOrNullGuids() throws Exception {
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
    public void purgeByIds_preScanNonExistentEntities() throws Exception {
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

    private static AtlasEntityHeader buildAuditHeader(String guid, String result) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(AtlasAuditEntryDTO.ATTRIBUTE_OPERATION, AuditOperation.PURGE.name());
        attributes.put(AtlasAuditEntryDTO.ATTRIBUTE_RESULT, result);
        attributes.put(AtlasAuditEntryDTO.ATTRIBUTE_RESULT_COUNT, 0L);
        attributes.put(AtlasAuditEntryDTO.ATTRIBUTE_RUN_ID, TEST_RUN_ID);

        AtlasEntityHeader header = new AtlasEntityHeader();
        header.setGuid(guid);
        header.setAttributes(attributes);
        return header;
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

    private void assertSortedGuidsMatch(List<AtlasEntityHeader> expected, List<AtlasEntityHeader> actual, String operation) {
        assertNotNull(actual, operation + " returned null entities");
        assertEquals(toSortedGuidList(actual), toSortedGuidList(expected), operation + " guid mismatch");
    }

    private void assertPurgeSucceededForRequestedGuids(List<String> requestedGuids, EntityMutationResponse response) {
        assertNotNull(response.getPurgedEntities(), "purgeByIds returned no purged entities");
        assertNotNull(response.getPurgeSummary(), "purgeByIds returned no summary");

        Set<String> purgedGuids = response.getPurgedEntities().stream()
                .map(AtlasEntityHeader::getGuid)
                .collect(Collectors.toSet());

        Set<String> skippedGuids = new HashSet<>();
        if (response.getFailedEntities() != null) {
            for (FailedEntity failedEntity : response.getFailedEntities()) {
                if (PurgeUtils.isSkippablePurgeFailureCode(failedEntity.getErrorCode())) {
                    skippedGuids.add(failedEntity.getGuid());
                }
            }
        }

        for (String guid : requestedGuids) {
            assertTrue(purgedGuids.contains(guid) || skippedGuids.contains(guid),
                    "Expected requested guid to be purged or skipped as already removed: " + guid);
        }

        assertEquals(response.getPurgeSummary().getRequestedCount(), requestedGuids.size());
        assertEquals(response.getPurgeSummary().getPurgedCount() + response.getPurgeSummary().getSkippedRequestedCount(),
                requestedGuids.size());
        assertEquals(response.getPurgeSummary().getFailedCount(), 0);
    }

    private static List<String> toSortedGuidList(List<AtlasEntityHeader> headers) {
        return headers.stream()
                .map(AtlasEntityHeader::getGuid)
                .sorted()
                .collect(Collectors.toList());
    }

    private AuditSearchParameters createAuditParameter(String fileName) {
        try {
            return TestResourceFileUtils.readObjectFromJson(AUDIT_PARAMETER_RESOURCE_DIR, fileName, AuditSearchParameters.class);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        return null;
    }

    private void assertAuditEntry(AtlasAuditService auditService, AuditSearchParameters auditSearchParameters) {
        pauseForIndexCreation();

        List<AtlasAuditEntry> result;

        try {
            result = auditService.get(auditSearchParameters);
        } catch (Exception e) {
            throw new SkipException("audit entries not retrieved.");
        }

        assertNotNull(result);
        assertFalse(result.isEmpty());
    }
}
