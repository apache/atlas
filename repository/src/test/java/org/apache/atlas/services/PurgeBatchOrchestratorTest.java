package org.apache.atlas.services;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.model.instance.PurgeSummary;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.purge.PurgeExecutionStats;
import org.apache.atlas.repository.purge.PurgeTestUtils;
import org.apache.atlas.repository.purge.PurgeUtils;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class PurgeBatchOrchestratorTest {
    private PurgeBatchExecutor mockExecutor;
    private PurgeBatchOrchestrator orchestrator;
    private final AtlasEntityStore entityStore = mock(AtlasEntityStore.class, CALLS_REAL_METHODS);

    @BeforeMethod
    public void setup() {
        mockExecutor = mock(PurgeBatchExecutor.class);
        orchestrator = new PurgeBatchOrchestrator(mockExecutor, null, null);
        RequestContext.clear();
        RequestContext.get().setUser("testUser", null);
    }

    @AfterMethod
    public void teardown() {
        RequestContext.clear();
    }

    @Test
    public void testBatchFlushingAndResults() throws Exception {
        PurgeBatchOrchestrator.PurgeBatchManager manager = orchestrator.createManager(2, 2);

        // Setup mock response
        EntityMutationResponse mockResponse = new EntityMutationResponse();
        AtlasEntityHeader header1 = new AtlasEntityHeader();
        header1.setGuid("guid1");
        AtlasEntityHeader header2 = new AtlasEntityHeader();
        header2.setGuid("guid2");
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();
        mutatedEntities.put(EntityOperation.PURGE, Arrays.asList(header1, header2));
        mockResponse.setMutatedEntities(mutatedEntities);

        when(mockExecutor.executeBatch(anySet())).thenReturn(mockResponse);

        manager.checkProduce("guid1");
        manager.checkProduce("guid2");
        manager.checkProduce("guid3");

        manager.shutdown();

        Queue<Object> results = manager.getResults();
        assertNotNull(results);

        // One batch of 2 was processed, another batch of 1 was processed during shutdown
        verify(mockExecutor, times(2)).executeBatch(anySet());
    }

    @Test
    public void testExceptionHandlingConvertsToFailedEntities() throws Exception {
        PurgeBatchOrchestrator.PurgeBatchManager manager = orchestrator.createManager(1, 1);

        when(mockExecutor.executeBatch(anySet())).thenThrow(new OutOfMemoryError("OOM Test"));

        manager.checkProduce("guid1");
        manager.shutdown();

        Queue<Object> results = manager.getResults();
        assertEquals(results.size(), 1);

        Object result = results.poll();
        assertTrue(result instanceof PurgeBatchResult);
        PurgeBatchResult batchResult = (PurgeBatchResult) result;
        assertTrue(batchResult.hasBatchException());
        assertEquals(batchResult.getBatchException().getMessage(), "OOM Test");
        assertEquals(batchResult.getFailedEntities().size(), 1);
        assertEquals(batchResult.getFailedEntities().get(0).getGuid(), "guid1");
    }

    @Test
    public void testAuditWhenBatchCommitThrows() throws Exception {
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);
        PurgeBatchOrchestrator orchestratorWithAudit = new PurgeBatchOrchestrator(mockExecutor, mockAuditService,
                AuditOperation.PURGE);

        when(mockExecutor.executeBatch(anySet())).thenThrow(new RuntimeException("batch failed"));

        PurgeBatchOrchestrator.PurgeBatchManager manager = orchestratorWithAudit.createManager(1, 1);
        manager.checkProduce("guid1");
        manager.shutdown();

        ArgumentCaptor<String> resultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService, times(1)).add(
                eq(AuditOperation.PURGE), anyString(), resultCaptor.capture(), eq(0L));

        String result = resultCaptor.getValue();
        assertTrue(result.contains("purgedCount=0"));
        assertTrue(result.contains("failedCount=1"));
        assertTrue(result.contains("skippedCount=0"));
        assertTrue(result.contains("sampleFailedGuids=[guid1]"));
    }

    @Test
    public void testReconcileUnprocessedGuidsMarksMissingAsFailed() {
        EntityMutationResponse response = new EntityMutationResponse();
        AtlasEntityHeader header1 = new AtlasEntityHeader();
        header1.setGuid("guid1");
        response.addEntity(EntityOperation.PURGE, header1);

        List<FailedEntity> pendingFailures = new ArrayList<>();
        pendingFailures.add(new FailedEntity(
                "guid2",
                org.apache.atlas.AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode(),
                "already failed"));

        Set<String> submittedGuids = new LinkedHashSet<>(Arrays.asList("guid1", "guid2", "guid3"));
        int reconciledCount = PurgeBatchOrchestrator.reconcileUnprocessedGuids(submittedGuids, response, pendingFailures, null);

        assertEquals(reconciledCount, 1);
        assertEquals(pendingFailures.size(), 2);
        assertEquals(pendingFailures.get(1).getGuid(), "guid3");
        assertEquals(pendingFailures.get(1).getErrorCode(), org.apache.atlas.AtlasErrorCode.INTERNAL_ERROR.getErrorCode());
        assertEquals(pendingFailures.get(1).getErrorMessage(), PurgeBatchOrchestrator.UNPROCESSED_PURGE_GUID_MESSAGE);
    }

    @Test
    public void testReconcileUnprocessedGuidsNoOpWhenAllAccounted() {
        EntityMutationResponse response = new EntityMutationResponse();
        AtlasEntityHeader header1 = new AtlasEntityHeader();
        header1.setGuid("guid1");
        response.addEntity(EntityOperation.PURGE, header1);
        response.addFailedEntity(new FailedEntity(
                "guid2",
                org.apache.atlas.AtlasErrorCode.INTERNAL_ERROR.getErrorCode(),
                "batch failure"));

        int reconciledCount = PurgeBatchOrchestrator.reconcileUnprocessedGuids(
                Arrays.asList("guid1", "guid2"), response, null, null);

        assertEquals(reconciledCount, 0);
        assertEquals(response.getFailedEntities().size(), 1);
    }

    @Test
    public void testBatchWorkerReportsPurgedEntitiesOnSuccess() throws Exception {
        PurgeBatchOrchestrator.PurgeBatchManager manager = orchestrator.createManager(2, 1);

        EntityMutationResponse mockResponse = new EntityMutationResponse();
        AtlasEntityHeader header1 = new AtlasEntityHeader();
        header1.setGuid("guid1");
        AtlasEntityHeader header2 = new AtlasEntityHeader();
        header2.setGuid("guid2");
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();
        mutatedEntities.put(EntityOperation.PURGE, Arrays.asList(header1, header2));
        mockResponse.setMutatedEntities(mutatedEntities);

        when(mockExecutor.executeBatch(anySet())).thenReturn(mockResponse);

        manager.checkProduce("guid1");
        manager.checkProduce("guid2");
        manager.shutdown();

        Queue<Object> results = manager.getResults();
        assertEquals(results.size(), 1);

        Object result = results.poll();
        assertTrue(result instanceof PurgeBatchResult);
        PurgeBatchResult batchResult = (PurgeBatchResult) result;
        assertEquals(batchResult.getPurgedEntities().size(), 2);
        assertEquals(batchResult.getPurgedEntities().get(0).getGuid(), "guid1");
        assertEquals(batchResult.getPurgedEntities().get(1).getGuid(), "guid2");
    }

    @Test
    public void testBatchAlreadyRemovedEntitiesReportedAsSkipped() throws Exception {
        PurgeBatchOrchestrator.PurgeBatchManager manager = orchestrator.createManager(1, 1);

        EntityMutationResponse mockResponse = new EntityMutationResponse();
        mockResponse.addFailedEntity(new FailedEntity(
                "guid1",
                AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode(),
                "already removed"));

        when(mockExecutor.executeBatch(anySet())).thenReturn(mockResponse);

        manager.checkProduce("guid1");
        manager.shutdown();

        Queue<Object> results = manager.getResults();
        assertEquals(results.size(), 1);

        Object result = results.poll();
        assertTrue(result instanceof PurgeBatchResult);
        PurgeBatchResult batchResult = (PurgeBatchResult) result;
        assertEquals(batchResult.getFailedEntities().size(), 1);
        FailedEntity failedEntity = batchResult.getFailedEntities().get(0);
        assertEquals(failedEntity.getGuid(), "guid1");
        assertEquals(failedEntity.getErrorCode(), AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode());
        assertTrue(PurgeUtils.isSkippablePurgeFailureCode(failedEntity.getErrorCode()));
    }

    @Test
    public void testExecutePurgeSuccess() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        when(mockExecutor.getEntityStore()).thenReturn(mockStore);

        String rootGuid = "11111111-1111-1111-1111-111111111111";
        String dependencyGuid = "22222222-2222-2222-2222-222222222222";
        when(mockStore.accumulateDeletionCandidates(Collections.singleton(rootGuid)))
                .thenReturn(new LinkedHashSet<>(Collections.singletonList(dependencyGuid)));

        EntityMutationResponse batchResponse = new EntityMutationResponse();
        AtlasEntityHeader purgedDependency = new AtlasEntityHeader();
        purgedDependency.setGuid(dependencyGuid);
        AtlasEntityHeader purgedRoot = new AtlasEntityHeader();
        purgedRoot.setGuid(rootGuid);
        batchResponse.addEntity(EntityOperation.PURGE, purgedDependency);
        batchResponse.addEntity(EntityOperation.PURGE, purgedRoot);
        when(mockExecutor.executeBatch(anySet())).thenReturn(batchResponse);

        List<FailedEntity> failedEntities = new ArrayList<>();
        failedEntities.add(new FailedEntity(
                "33333333-3333-3333-3333-333333333333",
                AtlasErrorCode.INVALID_GUID.getErrorCode(),
                "invalid guid"));

        Set<String> validGuids = new LinkedHashSet<>(Collections.singletonList(rootGuid));
        PurgeExecutionStats stats = new PurgeExecutionStats(validGuids, validGuids.size());
        EntityMutationResponse response = orchestrator.executePurge(validGuids, failedEntities, stats);

        assertNotNull(response.getPurgedEntities());
        assertEquals(response.getPurgedEntities().size(), 2);
        assertNotNull(response.getFailedEntities());
        assertEquals(response.getFailedEntities().size(), 1);
        assertEquals(response.getFailedEntities().get(0).getErrorCode(), AtlasErrorCode.INVALID_GUID.getErrorCode());
        verify(mockStore).accumulateDeletionCandidates(Collections.singleton(rootGuid));
    }

    @Test
    public void testExecutePurgeRecordsExpansionFailure() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        when(mockExecutor.getEntityStore()).thenReturn(mockStore);

        String rootGuid = "11111111-1111-1111-1111-111111111111";
        when(mockStore.accumulateDeletionCandidates(Collections.singleton(rootGuid)))
                .thenThrow(new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, rootGuid));

        List<FailedEntity> failedEntities = new ArrayList<>();
        Set<String> validGuids = new LinkedHashSet<>(Collections.singletonList(rootGuid));
        PurgeExecutionStats stats = new PurgeExecutionStats(validGuids, validGuids.size());
        EntityMutationResponse response = orchestrator.executePurge(validGuids, failedEntities, stats);

        assertTrue(response.getPurgedEntities() == null || response.getPurgedEntities().isEmpty());
        assertEquals(failedEntities.size(), 1);
        assertEquals(failedEntities.get(0).getGuid(), rootGuid);
        assertEquals(failedEntities.get(0).getErrorCode(), AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode());
        assertNotNull(response.getFailedEntities());
        assertEquals(response.getFailedEntities().size(), 1);
    }

    @Test
    public void testExecutePurgeMergesPartialResultsWhenShutdownInterrupted() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        when(mockExecutor.getEntityStore()).thenReturn(mockStore);

        String rootGuid = "11111111-1111-1111-1111-111111111111";
        String dependencyGuid = "22222222-2222-2222-2222-222222222222";
        when(mockStore.accumulateDeletionCandidates(Collections.singleton(rootGuid)))
                .thenReturn(new LinkedHashSet<>(Collections.singletonList(dependencyGuid)));

        EntityMutationResponse batchResponse = new EntityMutationResponse();
        AtlasEntityHeader purgedHeader = new AtlasEntityHeader();
        purgedHeader.setGuid(dependencyGuid);
        batchResponse.addEntity(EntityOperation.PURGE, purgedHeader);
        when(mockExecutor.executeBatch(anySet())).thenReturn(batchResponse);

        PurgeBatchOrchestrator orchestratorSpy = spy(orchestrator);
        doAnswer(invocation -> {
            PurgeBatchOrchestrator.PurgeBatchManager manager =
                    (PurgeBatchOrchestrator.PurgeBatchManager) invocation.callRealMethod();
            PurgeBatchOrchestrator.PurgeBatchManager managerSpy = spy(manager);

            doAnswer(shutdownInvocation -> {
                managerSpy.getResults().offer(new PurgeBatchResult(
                        new LinkedHashSet<>(Collections.singletonList(dependencyGuid)),
                        Collections.singletonList(purgedHeader), null, false, null));
                throw new InterruptedException("simulated shutdown interrupt");
            }).when(managerSpy).shutdown();

            return managerSpy;
        }).when(orchestratorSpy).createManager(anyInt(), anyInt());

        List<FailedEntity> failedEntities = new ArrayList<>();
        Set<String> validGuids = new LinkedHashSet<>(Collections.singletonList(rootGuid));
        PurgeExecutionStats stats = new PurgeExecutionStats(validGuids, validGuids.size());
        EntityMutationResponse response = orchestratorSpy.executePurge(validGuids, failedEntities, stats);

        assertNotNull(response.getPurgedEntities());
        assertEquals(response.getPurgedEntities().size(), 1);
        assertEquals(response.getPurgedEntities().get(0).getGuid(), dependencyGuid);
        assertNotNull(response.getFailedEntities());
        assertEquals(response.getFailedEntities().size(), 1);
        assertEquals(response.getFailedEntities().get(0).getGuid(), rootGuid);
        assertEquals(response.getFailedEntities().get(0).getErrorCode(), AtlasErrorCode.INTERNAL_ERROR.getErrorCode());
        assertEquals(response.getFailedEntities().get(0).getErrorMessage(),
                PurgeBatchOrchestrator.UNPROCESSED_PURGE_GUID_MESSAGE);
        assertTrue(Thread.interrupted(), "Expected interrupt flag to be set after shutdown interruption");
    }

    @Test
    public void testExecutePurgeEmptyInputReturnsEmptyResponse() throws Exception {
        when(mockExecutor.getEntityStore()).thenReturn(mock(AtlasEntityStore.class));

        List<FailedEntity> failedEntities = new ArrayList<>();
        Set<String> validGuids = new LinkedHashSet<>();
        PurgeExecutionStats stats = new PurgeExecutionStats(validGuids, 0);
        EntityMutationResponse response = orchestrator.executePurge(validGuids, failedEntities, stats);

        assertNull(response.getPurgedEntities());
        assertNull(response.getFailedEntities());
        verify(mockExecutor, never()).executeBatch(anySet());
    }

    @Test
    public void testAuditWriterInvokedOnCommit() throws Exception {
        PurgeBatchExecutor mockExecutor = mock(PurgeBatchExecutor.class);
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);

        PurgeBatchOrchestrator orchestrator = new PurgeBatchOrchestrator(mockExecutor, mockAuditService,
                AuditOperation.PURGE);

        EntityMutationResponse mockResponse = new EntityMutationResponse();
        AtlasEntityHeader header1 = new AtlasEntityHeader();
        header1.setGuid("guid1");
        AtlasEntityHeader header2 = new AtlasEntityHeader();
        header2.setGuid("guid2");
        Map<EntityOperation, List<AtlasEntityHeader>> mutatedEntities = new HashMap<>();
        mutatedEntities.put(EntityOperation.PURGE, Arrays.asList(header1, header2));
        mockResponse.setMutatedEntities(mutatedEntities);

        when(mockExecutor.executeBatch(anySet())).thenReturn(mockResponse);

        PurgeBatchOrchestrator.PurgeBatchManager manager = orchestrator.createManager(2, 1);
        manager.checkProduce("guid1");
        manager.checkProduce("guid2");
        manager.shutdown();

        ArgumentCaptor<String> paramsCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService, times(1)).add(
                eq(AuditOperation.PURGE),
                paramsCaptor.capture(),
                resultCaptor.capture(),
                eq(2L));

        assertTrue(paramsCaptor.getValue().contains("processedCount=2"));
        assertTrue(paramsCaptor.getValue().contains("sampleGuids=[guid1,guid2]"));
        assertTrue(resultCaptor.getValue().contains("purgedCount=2"));
        assertTrue(resultCaptor.getValue().contains("failedCount=0"));
        assertTrue(resultCaptor.getValue().contains("skippedCount=0"));
    }

    @Test
    public void testWriteBatchAuditUsesSummarySkippedAndFailedCounts() throws Exception {
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);

        Set<String> batchGuids = new LinkedHashSet<>(Arrays.asList(
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222",
                "33333333-3333-3333-3333-333333333333"));

        EntityMutationResponse response = new EntityMutationResponse();
        response.addFailedEntity(new FailedEntity(
                "11111111-1111-1111-1111-111111111111",
                AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode(),
                "not found"));
        response.addFailedEntity(new FailedEntity(
                "22222222-2222-2222-2222-222222222222",
                AtlasErrorCode.NOT_IN_DELETED_STATE.getErrorCode(),
                "not deleted"));
        response.addFailedEntity(new FailedEntity(
                "33333333-3333-3333-3333-333333333333",
                AtlasErrorCode.INVALID_GUID.getErrorCode(),
                "invalid guid"));
        response.setSummary(new PurgeSummary(3, 0, 0, 1, 2));

        PurgeBatchOrchestrator.writeBatchAudit(mockAuditService, AuditOperation.PURGE, batchGuids, response);

        ArgumentCaptor<String> resultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), anyString(), resultCaptor.capture(), eq(0L));

        String result = resultCaptor.getValue();
        assertTrue(result.contains("purgedCount=0"));
        assertTrue(result.contains("failedCount=1"));
        assertTrue(result.contains("skippedCount=2"));
        assertTrue(result.contains("sampleFailedGuids=[33333333-3333-3333-3333-333333333333]"));
        assertTrue(result.contains("sampleSkippedGuids=[11111111-1111-1111-1111-111111111111,22222222-2222-2222-2222-222222222222]"));
    }

    @Test
    public void testWriteBatchAuditDerivesCountsWhenSummaryMissing() throws Exception {
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);

        Set<String> batchGuids = new LinkedHashSet<>(Collections.singletonList(
                "11111111-1111-1111-1111-111111111111"));

        EntityMutationResponse response = new EntityMutationResponse();
        response.addFailedEntity(new FailedEntity(
                "11111111-1111-1111-1111-111111111111",
                AtlasErrorCode.INTERNAL_ERROR.getErrorCode(),
                "batch failure"));

        PurgeBatchOrchestrator.writeBatchAudit(mockAuditService, AuditOperation.PURGE, batchGuids, response);

        ArgumentCaptor<String> resultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), anyString(), resultCaptor.capture(), eq(0L));

        String result = resultCaptor.getValue();
        assertTrue(result.contains("failedCount=1"));
        assertTrue(result.contains("skippedCount=0"));
        assertTrue(result.contains("sampleFailedGuids=[11111111-1111-1111-1111-111111111111]"));
    }

    @Test
    public void testWriteBatchAuditSplitsDependencyFailuresWhenOriginallyRequestedGuidsProvided() throws Exception {
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);

        Set<String> batchGuids = new LinkedHashSet<>(Arrays.asList(
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222"));
        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Collections.singletonList(
                "11111111-1111-1111-1111-111111111111"));

        EntityMutationResponse response = new EntityMutationResponse();
        response.addFailedEntity(new FailedEntity(
                "11111111-1111-1111-1111-111111111111",
                AtlasErrorCode.INTERNAL_ERROR.getErrorCode(),
                "requested failure"));
        response.addFailedEntity(new FailedEntity(
                "22222222-2222-2222-2222-222222222222",
                AtlasErrorCode.INTERNAL_ERROR.getErrorCode(),
                "dependency failure"));

        PurgeBatchOrchestrator.writeBatchAudit(mockAuditService, AuditOperation.PURGE, batchGuids, response,
                originallyRequestedGuids);

        ArgumentCaptor<String> resultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), anyString(), resultCaptor.capture(), eq(0L));

        String result = resultCaptor.getValue();
        assertTrue(result.contains("failedCount=1"));
        assertTrue(result.contains("failedDependenciesCount=1"));
    }

    @Test
    public void testWriteBatchAuditAllPreScanSkippedUsesSummaryCounts() throws Exception {
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);

        Set<String> batchGuids = new LinkedHashSet<>(Arrays.asList(
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222"));

        EntityMutationResponse response = new EntityMutationResponse();
        response.addFailedEntity(new FailedEntity(
                "11111111-1111-1111-1111-111111111111",
                AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode(),
                "not found"));
        response.addFailedEntity(new FailedEntity(
                "22222222-2222-2222-2222-222222222222",
                AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode(),
                "not found"));
        response.setSummary(new PurgeSummary(2, 0, 0, 0, 2));

        PurgeBatchOrchestrator.writeBatchAudit(mockAuditService, AuditOperation.PURGE, batchGuids, response);

        ArgumentCaptor<String> resultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), anyString(), resultCaptor.capture(), eq(0L));

        String result = resultCaptor.getValue();
        assertTrue(result.contains("failedCount=0"));
        assertTrue(result.contains("failedDependenciesCount=0"));
        assertTrue(result.contains("skippedCount=2"));
    }

    @Test
    public void testWriteBatchAuditIncludesFailedDependenciesCountFromSummary() throws Exception {
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);

        Set<String> batchGuids = new LinkedHashSet<>(Arrays.asList(
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222"));

        EntityMutationResponse response = new EntityMutationResponse();
        AtlasEntityHeader purgedRequested = new AtlasEntityHeader();
        purgedRequested.setGuid("11111111-1111-1111-1111-111111111111");
        response.addEntity(EntityOperation.PURGE, purgedRequested);
        response.addFailedEntity(new FailedEntity(
                "22222222-2222-2222-2222-222222222222",
                AtlasErrorCode.INTERNAL_ERROR.getErrorCode(),
                "dependency batch failure"));
        response.setSummary(new PurgeSummary(1, 1, 0, 0, 1, 0));

        PurgeBatchOrchestrator.writeBatchAudit(mockAuditService, AuditOperation.PURGE, batchGuids, response);

        ArgumentCaptor<String> resultCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), anyString(), resultCaptor.capture(), eq(1L));

        String result = resultCaptor.getValue();
        assertTrue(result.contains("purgedCount=1"));
        assertTrue(result.contains("failedCount=0"));
        assertTrue(result.contains("failedDependenciesCount=1"));
        assertTrue(result.contains("skippedCount=0"));
        assertTrue(result.contains("sampleFailedGuids=[22222222-2222-2222-2222-222222222222]"));
    }

    @Test
    public void testAuditWriterBoundsLargeBatchParams() throws Exception {
        PurgeBatchExecutor mockExecutor = mock(PurgeBatchExecutor.class);
        AtlasAuditService mockAuditService = mock(AtlasAuditService.class);
        PurgeBatchOrchestrator orchestrator = new PurgeBatchOrchestrator(mockExecutor, mockAuditService,
                AuditOperation.PURGE);

        Set<String> batchGuids = new LinkedHashSet<>();
        for (int i = 0; i < 12; i++) {
            batchGuids.add(String.format("11111111-1111-1111-1111-%012d", i));
        }

        EntityMutationResponse mockResponse = new EntityMutationResponse();
        AtlasEntityHeader header = new AtlasEntityHeader();
        header.setGuid("11111111-1111-1111-1111-000000000000");
        mockResponse.addEntity(EntityOperation.PURGE, header);

        when(mockExecutor.executeBatch(anySet())).thenReturn(mockResponse);

        PurgeBatchOrchestrator.PurgeBatchManager manager = orchestrator.createManager(12, 1);
        for (String guid : batchGuids) {
            manager.checkProduce(guid);
        }
        manager.shutdown();

        ArgumentCaptor<String> paramsCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockAuditService).add(eq(AuditOperation.PURGE), paramsCaptor.capture(), anyString(), eq(1L));
        assertTrue(paramsCaptor.getValue().contains("processedCount=12"));
        assertTrue(paramsCaptor.getValue().length() < batchGuids.toString().length());
    }

    @Test
    public void testAttachPurgeSummarySetsExecutionFailedForNonInternalErrorBatchFailure() {
        EntityMutationResponse response = new EntityMutationResponse();

        String requestedGuid = "11111111-1111-1111-1111-111111111111";
        String failedGuid    = "22222222-2222-2222-2222-222222222222";

        AtlasEntityHeader purgedRequested = new AtlasEntityHeader();
        purgedRequested.setGuid(requestedGuid);
        response.addEntity(EntityOperation.PURGE, purgedRequested);

        response.addFailedEntity(new FailedEntity(
                failedGuid,
                AtlasErrorCode.REFERENCED_ENTITY_NOT_FOUND.getErrorCode(),
                "Referenced entity missing during batch purge"));

        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Arrays.asList(requestedGuid, failedGuid));
        PurgeTestUtils.attachPurgeSummaryFromResponse(response, originallyRequestedGuids, originallyRequestedGuids.size());

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertTrue(summary.getExecutionFailed());
        assertEquals(summary.getPurgedCount(), 1);
        assertEquals(summary.getFailedCount(), 1);
        assertEquals(summary.getSkippedCount(), 0);
    }

    @Test
    public void testAttachPurgeSummaryDoesNotSetExecutionFailedForPreScanFailures() {
        EntityMutationResponse response = new EntityMutationResponse();

        String purgedGuid  = "11111111-1111-1111-1111-111111111111";
        String invalidGuid = "not-a-valid-uuid";

        AtlasEntityHeader purgedRequested = new AtlasEntityHeader();
        purgedRequested.setGuid(purgedGuid);
        response.addEntity(EntityOperation.PURGE, purgedRequested);

        response.addFailedEntity(new FailedEntity(
                invalidGuid,
                AtlasErrorCode.INVALID_GUID.getErrorCode(),
                "invalid guid"));

        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Arrays.asList(purgedGuid, invalidGuid));
        PurgeTestUtils.attachPurgeSummaryFromResponse(response, originallyRequestedGuids, 1);

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertFalse(summary.getExecutionFailed());
        assertEquals(summary.getPurgedCount(), 1);
        assertEquals(summary.getFailedCount(), 1);
        assertEquals(summary.getSkippedCount(), 0);
    }

    @Test
    public void testAttachPurgeSummaryCountsNonSkippableDependencyFailuresSeparately() {
        EntityMutationResponse response = new EntityMutationResponse();

        String requestedGuid  = "11111111-1111-1111-1111-111111111111";
        String dependencyGuid = "22222222-2222-2222-2222-222222222222";

        AtlasEntityHeader purgedRequested = new AtlasEntityHeader();
        purgedRequested.setGuid(requestedGuid);
        response.addEntity(EntityOperation.PURGE, purgedRequested);

        AtlasEntityHeader purgedDependency = new AtlasEntityHeader();
        purgedDependency.setGuid(dependencyGuid);
        response.addEntity(EntityOperation.PURGE, purgedDependency);

        response.addFailedEntity(new FailedEntity(
                dependencyGuid,
                AtlasErrorCode.INTERNAL_ERROR.getErrorCode(),
                "dependency batch failure"));

        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Arrays.asList(requestedGuid));
        PurgeTestUtils.attachPurgeSummaryFromResponse(response, originallyRequestedGuids, 1);

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertEquals(summary.getRequestedCount(), 1);
        assertEquals(summary.getPurgedCount(), 1);
        assertEquals(summary.getPurgedDependenciesCount(), 1);
        assertEquals(summary.getFailedCount(), 0);
        assertEquals(summary.getFailedDependenciesCount(), 0);
        assertEquals(response.getFailedEntities().size(), 1);
    }

    @Test
    public void testAttachPurgeSummaryIgnoresSkippableDependencyFailures() {
        EntityMutationResponse response = new EntityMutationResponse();

        String requestedGuid  = "11111111-1111-1111-1111-111111111111";
        String dependencyGuid = "22222222-2222-2222-2222-222222222222";

        AtlasEntityHeader purgedRequested = new AtlasEntityHeader();
        purgedRequested.setGuid(requestedGuid);
        response.addEntity(EntityOperation.PURGE, purgedRequested);

        response.addFailedEntity(new FailedEntity(
                dependencyGuid,
                AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode(),
                "dependency already removed by concurrent batch"));

        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Arrays.asList(requestedGuid));
        PurgeTestUtils.attachPurgeSummaryFromResponse(response, originallyRequestedGuids, 1);

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertEquals(summary.getRequestedCount(), 1);
        assertEquals(summary.getPurgedCount(), 1);
        assertEquals(summary.getPurgedDependenciesCount(), 0);
        assertEquals(summary.getFailedCount(), 0);
        assertEquals(summary.getFailedDependenciesCount(), 0);
        assertEquals(summary.getSkippedDependenciesCount(), 1);
        assertEquals(summary.getSkippedCount(), 1);
    }

    @Test
    public void testAttachPurgeSummaryCountsRequestedAndDependencyFailuresSeparately() {
        EntityMutationResponse response = new EntityMutationResponse();

        String requestedGuid  = "11111111-1111-1111-1111-111111111111";
        String dependencyGuid = "22222222-2222-2222-2222-222222222222";

        response.addFailedEntity(new FailedEntity(
                requestedGuid,
                AtlasErrorCode.INTERNAL_ERROR.getErrorCode(),
                "requested root expand failure"));
        response.addFailedEntity(new FailedEntity(
                dependencyGuid,
                AtlasErrorCode.INTERNAL_ERROR.getErrorCode(),
                "dependency batch failure"));

        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Arrays.asList(requestedGuid));
        PurgeTestUtils.attachPurgeSummaryFromResponse(response, originallyRequestedGuids, 1);

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertEquals(summary.getRequestedCount(), 1);
        assertEquals(summary.getPurgedCount(), 0);
        assertEquals(summary.getPurgedDependenciesCount(), 0);
        assertEquals(summary.getFailedCount(), 1);
        assertEquals(summary.getFailedDependenciesCount(), 1);
        assertEquals(summary.getSkippedCount(), 0);
        assertEquals(response.getFailedEntities().size(), 2);
    }

    @Test
    public void testAttachPurgeSummaryBalanceFormula() {
        EntityMutationResponse response = new EntityMutationResponse();

        String requestedGuid  = "11111111-1111-1111-1111-111111111111";
        String dependencyGuid = "22222222-2222-2222-2222-222222222222";
        String failedDependencyGuid = "33333333-3333-3333-3333-333333333333";

        response.addFailedEntity(new FailedEntity(
                requestedGuid,
                AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode(),
                "Not found"));
        response.addFailedEntity(new FailedEntity(
                dependencyGuid,
                AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getErrorCode(),
                "Dependency not found"));
        response.addFailedEntity(new FailedEntity(
                failedDependencyGuid,
                AtlasErrorCode.INTERNAL_ERROR.getErrorCode(),
                "Dependency failed"));

        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Collections.singletonList(requestedGuid));
        PurgeTestUtils.attachPurgeSummaryFromResponse(response, originallyRequestedGuids, 1);

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertEquals(summary.getRequestedCount(), 1);
        assertEquals(summary.getSkippedRequestedCount(), 1);
        assertEquals(summary.getSkippedDependenciesCount(), 1);
        assertEquals(summary.getSkippedCount(), 2);
        assertEquals(summary.getFailedCount(), 0);
        assertEquals(summary.getFailedDependenciesCount(), 1);
        assertEquals(summary.getPurgedCount(), 0);
        assertEquals(summary.getPurgedDependenciesCount(), 0);

        assertEquals(summary.getRequestedCount(),
                summary.getPurgedCount() + summary.getFailedCount() + summary.getSkippedRequestedCount());
    }

    @Test
    public void testAttachPurgeSummarySetsValidGuidCount() {
        EntityMutationResponse response = new EntityMutationResponse();
        Set<String> originallyRequestedGuids = new LinkedHashSet<>(
                Collections.singletonList("11111111-1111-1111-1111-111111111111"));

        PurgeTestUtils.attachPurgeSummaryFromResponse(response, originallyRequestedGuids, 1);

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertEquals(summary.getValidGuidCount(), 1);
    }

    @Test
    public void testReconcileUnprocessedGuidsUpdatesExecutionStats() {
        Set<String> originallyRequestedGuids = new LinkedHashSet<>(Arrays.asList(
                "11111111-1111-1111-1111-111111111111",
                "22222222-2222-2222-2222-222222222222",
                "33333333-3333-3333-3333-333333333333"));
        PurgeExecutionStats stats = new PurgeExecutionStats(originallyRequestedGuids, originallyRequestedGuids.size());
        stats.getProducedDeletionCandidates().addAll(originallyRequestedGuids);

        EntityMutationResponse response = new EntityMutationResponse();
        int reconciledCount = PurgeBatchOrchestrator.reconcileUnprocessedGuids(
                stats.getProducedDeletionCandidates(), response, null, stats);

        assertEquals(reconciledCount, 3);
        assertEquals(stats.getReconciledUnprocessedCount(), 3);
        assertEquals(stats.getFailedCount(), 3);
        PurgeUtils.attachPurgeSummary(response, stats);
        assertEquals(response.getPurgeSummary().getUnprocessedCount(), 3);
    }

    @Test
    public void testExecutePurgeExpandedWorkloadAccounting() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        when(mockExecutor.getEntityStore()).thenReturn(mockStore);

        Set<String> requestedGuids = new LinkedHashSet<>();
        for (int i = 0; i < 10; i++) {
            requestedGuids.add(String.format("11111111-1111-1111-1111-%012d", i));
        }

        when(mockStore.accumulateDeletionCandidates(any())).thenAnswer(invocation -> {
            Set<String> root = invocation.getArgument(0);
            String rootGuid = root.iterator().next();
            Set<String> deps = new LinkedHashSet<>();
            for (int j = 0; j < 4; j++) {
                deps.add(rootGuid.replaceFirst("1111", String.format("%04d", j + 2)));
            }
            return deps;
        });

        when(mockExecutor.executeBatch(anySet())).thenAnswer(invocation -> {
            EntityMutationResponse batchResponse = new EntityMutationResponse();
            Set<String> batchGuids = invocation.getArgument(0);
            for (String guid : batchGuids) {
                AtlasEntityHeader header = new AtlasEntityHeader();
                header.setGuid(guid);
                batchResponse.addEntity(EntityOperation.PURGE, header);
            }
            return batchResponse;
        });

        PurgeExecutionStats stats = new PurgeExecutionStats(requestedGuids, requestedGuids.size());
        EntityMutationResponse response = orchestrator.executePurge(requestedGuids, new ArrayList<>(), stats);
        PurgeUtils.attachPurgeSummary(response, stats);

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertEquals(summary.getRequestedCount(), 10);
        assertEquals(summary.getExpandedEntityCount(), 50);
        assertEquals(summary.getPurgedCount(), 10);
        assertEquals(summary.getPurgedDependenciesCount(), 40);
        assertEquals(summary.getUnprocessedCount(), 0);
        assertTrue(summary.getBatchCount() > 0);
        assertEquals(summary.getExpandedEntityCount(),
                summary.getPurgedCount() + summary.getPurgedDependenciesCount());
    }

    @Test
    public void testMultiWorkerPurgeStatsAccounting() throws Exception {
        AtlasEntityStore mockStore = mock(AtlasEntityStore.class);
        when(mockExecutor.getEntityStore()).thenReturn(mockStore);

        Set<String> requestedGuids = new LinkedHashSet<>();
        for (int i = 0; i < 6; i++) {
            requestedGuids.add(String.format("11111111-1111-1111-1111-%012d", i));
        }

        when(mockStore.accumulateDeletionCandidates(any())).thenAnswer(invocation -> {
            Set<String> root = invocation.getArgument(0);
            return new LinkedHashSet<>(root);
        });

        when(mockExecutor.executeBatch(anySet())).thenAnswer(invocation -> {
            Set<String> batchGuids = invocation.getArgument(0);
            EntityMutationResponse batchResponse = new EntityMutationResponse();
            for (String guid : batchGuids) {
                if ("11111111-1111-1111-1111-000000000003".equals(guid)) {
                    batchResponse.addFailedEntity(new FailedEntity(guid,
                            AtlasErrorCode.INTERNAL_ERROR.getErrorCode(), "batch failure"));
                } else {
                    AtlasEntityHeader header = new AtlasEntityHeader();
                    header.setGuid(guid);
                    batchResponse.addEntity(EntityOperation.PURGE, header);
                }
            }
            return batchResponse;
        });

        PurgeBatchOrchestrator orchestratorSpy = spy(orchestrator);
        doAnswer(invocation -> orchestrator.createManager(1, 2))
                .when(orchestratorSpy).createManager(anyInt(), anyInt());

        PurgeExecutionStats stats = new PurgeExecutionStats(requestedGuids, requestedGuids.size());
        EntityMutationResponse response = orchestratorSpy.executePurge(requestedGuids, new ArrayList<>(), stats);
        PurgeUtils.attachPurgeSummary(response, stats);

        PurgeSummary summary = response.getPurgeSummary();
        assertNotNull(summary);
        assertEquals(summary.getRequestedCount(), 6);
        assertEquals(summary.getPurgedCount(), 5);
        assertEquals(summary.getFailedCount(), 1);
        assertTrue(summary.getExecutionFailed());
        assertTrue(summary.getBatchCount() >= 6);
    }
}
