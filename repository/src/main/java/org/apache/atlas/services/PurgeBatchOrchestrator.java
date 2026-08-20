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
import org.apache.atlas.DeleteType;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.AtlasAuditEntry.AuditOperation;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.model.instance.FailedEntity;
import org.apache.atlas.pc.WorkItemBuilder;
import org.apache.atlas.pc.WorkItemConsumer;
import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.purge.PurgeExecutionStats;
import org.apache.atlas.repository.purge.PurgeUtils;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.commons.configuration2.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class PurgeBatchOrchestrator {
    private static final Logger LOG                = LoggerFactory.getLogger(PurgeBatchOrchestrator.class);
    public static final String  PURGE_WORKERS_NAME             = "Entity-Purge-Worker";
    private static final String PURGE_WORKER_BATCH_SIZE_KEY    = "atlas.purge.worker.batch.size";
    private static final String PURGE_WORKERS_COUNT_KEY        = "atlas.purge.workers.count";
    private static final int    DEFAULT_PURGE_WORKER_BATCH_SIZE  = 100;
    private static final int    DEFAULT_PURGE_WORKERS_COUNT      = 2;
    static final String         UNPROCESSED_PURGE_GUID_MESSAGE = "Not processed during purge shutdown";

    private final PurgeBatchExecutor     purgeBatchExecutor;
    private final AtlasAuditService      auditService;
    private final AuditOperation         auditOperation;

    public PurgeBatchOrchestrator(PurgeBatchExecutor purgeBatchExecutor, AtlasAuditService auditService,
                                  AuditOperation auditOperation) {
        this.purgeBatchExecutor    = purgeBatchExecutor;
        this.auditService          = auditService;
        this.auditOperation        = auditOperation;
    }

    public EntityMutationResponse executePurge(Set<String> validGuids, List<FailedEntity> failedEntities,
                                               PurgeExecutionStats stats, String runId) throws AtlasBaseException {
        if (failedEntities == null) {
            failedEntities = new ArrayList<>();
        }

        Configuration configuration = null;
        try {
            configuration = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.warn("executePurge: failed to load application properties, using defaults", e);
        }
        int batchSize  = getWorkerBatchSize(configuration);
        int numWorkers = getWorkersCount(configuration);

        Set<String> producedDeletionCandidates = stats != null
                ? stats.getProducedDeletionCandidates() : new LinkedHashSet<>();

        WorkItemManager<String, PurgeBatchConsumer> manager = createManager(batchSize, numWorkers, runId);

        LOG.info("executePurge: starting iterative expand+WIM purge with batchSize={}, numWorkers={}, validGuids={}",
                batchSize, numWorkers, validGuids.size());

        EntityMutationResponse response = new EntityMutationResponse();
        try {
            for (String validGuid : validGuids) {
                if (producedDeletionCandidates.contains(validGuid)) {
                    continue;
                }

                try {
                    expandDeletionCandidatesAndProduce(validGuid, producedDeletionCandidates, manager, purgeBatchExecutor.getEntityStore());
                } catch (Exception e) {
                    LOG.warn("executePurge: failed to accumulate deletion candidates for guid={}", validGuid, e);
                    FailedEntity failedEntity = PurgeUtils.classifyPurgeFailure(validGuid, e, null);
                    failedEntities.add(failedEntity);
                } finally {
                    clearPurgeCandidateExpansionState();
                }
            }

            LOG.info("executePurge: produced {} guid(s) from {} valid request guid(s)",
                    producedDeletionCandidates.size(), validGuids.size());
        } finally {
            try {
                manager.shutdown();
            } catch (InterruptedException e) {
                LOG.warn("executePurge: purge worker shutdown interrupted; collecting partial results", e);
                Thread.currentThread().interrupt();
            }

            aggregateBatchResults(manager.getResults(), response, stats, failedEntities);
            reconcileUnprocessedGuids(producedDeletionCandidates, response, null, stats);
        }

        return response;
    }

    public static int getWorkerBatchSize(Configuration configuration) {
        return configuration != null
                ? configuration.getInt(PURGE_WORKER_BATCH_SIZE_KEY, DEFAULT_PURGE_WORKER_BATCH_SIZE)
                : DEFAULT_PURGE_WORKER_BATCH_SIZE;
    }

    public static int getWorkersCount(Configuration configuration) {
        return configuration != null
                ? configuration.getInt(PURGE_WORKERS_COUNT_KEY, DEFAULT_PURGE_WORKERS_COUNT)
                : DEFAULT_PURGE_WORKERS_COUNT;
    }

    public static void expandDeletionCandidatesAndProduce(String rootGuid, Set<String> producedDeletionCandidates,
                                                          WorkItemManager<String, ?> manager,
                                                          AtlasEntityStore entityStore) throws AtlasBaseException {
        Set<String> deletionCandidateGuids = entityStore.accumulateDeletionCandidates(Collections.singleton(rootGuid));

        for (String guid : deletionCandidateGuids) {
            if (guid != null && producedDeletionCandidates.add(guid)) {
                manager.checkProduce(guid);
            }
        }

        if (producedDeletionCandidates.add(rootGuid)) {
            manager.checkProduce(rootGuid);
        }
    }

    /**
     * Clears graph and RequestContext caches after one root GUID is expanded and enqueued,
     * so the next expansion does not reuse stale vertex handles.
     */
    public static void clearPurgeCandidateExpansionState() {
        GraphTransactionInterceptor.clearCache();
        RequestContext.get().clearCache();
        initializePurgeContext();
    }

    public static void initializePurgeContext() {
        RequestContext.get().setDeleteType(DeleteType.HARD);
        RequestContext.get().setPurgeRequested(true);
    }

    public static void resetPurgeContext() {
        GraphTransactionInterceptor.clearCache();
        RequestContext.get().clearCache();
        RequestContext.get().setDeleteType(DeleteType.DEFAULT);
        RequestContext.get().setPurgeRequested(false);
    }

    public static int aggregateBatchResults(Queue<Object> results, EntityMutationResponse target,
                                            PurgeExecutionStats stats, List<FailedEntity> expansionFailures) {
        if (expansionFailures != null) {
            for (FailedEntity failedEntity : expansionFailures) {
                if (stats != null) {
                    stats.recordFailure(failedEntity);
                }
                target.addFailedEntity(failedEntity);
            }
        }

        if (results == null) {
            LOG.warn("Purge worker results queue was not initialized");
            return expansionFailures != null ? expansionFailures.size() : 0;
        }

        int resultCount = results.size();

        while (!results.isEmpty()) {
            Object res = results.poll();

            if (res instanceof PurgeBatchResult) {
                PurgeBatchResult batchResult = (PurgeBatchResult) res;

                if (stats != null) {
                    if (batchResult.hasBatchException()) {
                        stats.recordBatchThrowable(batchResult.getBatchGuids(), batchResult.getFailedEntities());
                    } else {
                        stats.recordBatchOutcome(batchResult.getPurgedEntities(), batchResult.getFailedEntities(),
                                batchResult.getBatchGuids());
                    }
                }

                for (AtlasEntityHeader header : batchResult.getPurgedEntities()) {
                    target.addEntity(EntityOperation.PURGE, header);
                }

                for (FailedEntity failedEntity : batchResult.getFailedEntities()) {
                    target.addFailedEntity(failedEntity);
                }
            } else {
                LOG.warn("Unexpected result type in purge worker queue: {}", res != null ? res.getClass().getName() : "null");
            }
        }

        return resultCount + (expansionFailures != null ? expansionFailures.size() : 0);
    }

    public static int reconcileUnprocessedGuids(Collection<String> submittedGuids, EntityMutationResponse response,
                                                List<FailedEntity> pendingFailures, PurgeExecutionStats stats) {
        if (submittedGuids == null || submittedGuids.isEmpty()) {
            return 0;
        }

        Set<String> accountedGuids = collectAccountedGuids(response, pendingFailures);
        int           reconciledCount = 0;

        for (String guid : submittedGuids) {
            if (guid == null || accountedGuids.contains(guid)) {
                continue;
            }

            FailedEntity failedEntity = PurgeUtils.classifyPurgeFailure(guid, null, UNPROCESSED_PURGE_GUID_MESSAGE);

            if (pendingFailures != null) {
                pendingFailures.add(failedEntity);
            } else if (response != null) {
                response.addFailedEntity(failedEntity);
            }

            if (stats != null) {
                stats.recordReconciledUnprocessed(1);
                stats.recordFailure(failedEntity);
            }

            accountedGuids.add(guid);
            reconciledCount++;
        }

        if (reconciledCount > 0) {
            LOG.warn("Purge reconciliation marked {} unprocessed guid(s) as failed", reconciledCount);
        }

        return reconciledCount;
    }

    private static Set<String> collectAccountedGuids(EntityMutationResponse response, List<FailedEntity> pendingFailures) {
        Set<String> accountedGuids = new HashSet<>();

        if (response != null) {
            if (response.getPurgedEntities() != null) {
                for (AtlasEntityHeader entityHeader : response.getPurgedEntities()) {
                    if (entityHeader.getGuid() != null) {
                        accountedGuids.add(entityHeader.getGuid());
                    }
                }
            }

            List<FailedEntity> failedEntities = response.getFailedEntities();
            if (failedEntities != null) {
                for (FailedEntity failedEntity : failedEntities) {
                    if (failedEntity.getGuid() != null) {
                        accountedGuids.add(failedEntity.getGuid());
                    }
                }
            }
        }

        if (pendingFailures != null) {
            for (FailedEntity failedEntity : pendingFailures) {
                if (failedEntity.getGuid() != null) {
                    accountedGuids.add(failedEntity.getGuid());
                }
            }
        }

        return accountedGuids;
    }

    public PurgeBatchManager createManager(int batchSize, int numWorkers, String runId) {
        PurgeWorkerContext workerContext = PurgeWorkerContext.capture(RequestContext.get(), runId);

        PurgeBatchConsumerBuilder builder = new PurgeBatchConsumerBuilder(purgeBatchExecutor, auditService,
                auditOperation, batchSize, workerContext);
        return new PurgeBatchManager(builder, batchSize, numWorkers);
    }

    /**
     * Immutable snapshot of request audit context propagated to purge worker threads.
     */
    static final class PurgeWorkerContext {
        private final String       user;
        private final Set<String>  userGroups;
        private final String       clientIPAddress;
        private final List<String> forwardedAddresses;
        private final String       runId;

        private PurgeWorkerContext(String user, Set<String> userGroups, String clientIPAddress,
                                   List<String> forwardedAddresses, String runId) {
            this.user               = user;
            this.userGroups         = userGroups;
            this.clientIPAddress    = clientIPAddress;
            this.forwardedAddresses = forwardedAddresses;
            this.runId              = runId;
        }

        static PurgeWorkerContext capture(RequestContext source, String runId) {
            Set<String>  groups    = source.getUserGroups();
            List<String> forwarded = source.getForwardedAddresses();

            return new PurgeWorkerContext(
                    source.getUser(),
                    groups != null ? new HashSet<>(groups) : null,
                    source.getClientIPAddress(),
                    forwarded != null ? new ArrayList<>(forwarded) : null,
                    runId);
        }

        String getRunId() {
            return runId;
        }

        void applyTo(RequestContext target) {
            target.setUser(user, userGroups);
            target.setClientIPAddress(clientIPAddress);
            target.setForwardedAddresses(forwardedAddresses);
        }
    }

    public static class PurgeBatchConsumer extends WorkItemConsumer<String> {
        private final Set<String> batch = new LinkedHashSet<>();
        private final PurgeBatchExecutor    purgeBatchExecutor;
        private final AtlasAuditService     auditService;
        private final AuditOperation        auditOperation;
        private final int                   batchSize;
        private final PurgeWorkerContext    workerContext;
        private int                         batchesProcessed;

        public PurgeBatchConsumer(BlockingQueue<String> queue, PurgeBatchExecutor purgeBatchExecutor,
                                  AtlasAuditService auditService, AuditOperation auditOperation,
                                  int batchSize, PurgeWorkerContext workerContext) {
            super(queue);
            this.purgeBatchExecutor    = purgeBatchExecutor;
            this.auditService          = auditService;
            this.auditOperation        = auditOperation;
            this.batchSize             = batchSize;
            this.workerContext         = workerContext;
            this.batchesProcessed      = 0;
            LOG.debug("Purge worker consumer started with batchSize={}", batchSize);
        }

        @Override
        protected void processItem(String guid) {
            LOG.debug("==> processing the entity {}", guid);
            batch.add(guid);
            commit();
        }

        @Override
        protected void doCommit() {
            if (batch.size() == batchSize) {
                attemptCommit();
            }
        }

        @Override
        protected void commitDirty() {
            if (!batch.isEmpty()) {
                attemptCommit();
            }
            super.commitDirty();
        }

        protected void attemptCommit() {
            if (batch.isEmpty()) {
                return;
            }

            Set<String> batchGuids = new LinkedHashSet<>(batch);

            RequestContext context = RequestContext.get();
            context.clearCache();
            try {
                workerContext.applyTo(context);
                initializePurgeContext();

                EntityMutationResponse res = purgeBatchExecutor.executeBatch(batchGuids);

                if (auditService != null && auditOperation != null) {
                    PurgeAuditWriter.writeBatch(auditService, auditOperation,
                            workerContext.getRunId(), batchGuids, res);
                }

                addResult(new PurgeBatchResult(batchGuids,
                        res != null ? res.getPurgedEntities() : null,
                        res != null ? res.getFailedEntities() : null,
                        false, null));
            } catch (Throwable e) {
                LOG.error("==> Exception in purge batch commit: {}", e.getMessage(), e);
                List<FailedEntity> batchFailures = new ArrayList<>();
                for (String guid : batch) {
                    LOG.warn("Purge batch failure for guid={}: {}", guid, e.getMessage());
                    FailedEntity failedEntity = PurgeUtils.classifyPurgeFailure(guid, e, null);
                    batchFailures.add(failedEntity);
                }

                if (auditService != null && auditOperation != null) {
                    EntityMutationResponse response = new EntityMutationResponse();
                    for (FailedEntity fe : batchFailures) {
                        response.addFailedEntity(fe);
                    }
                    PurgeAuditWriter.writeBatch(auditService, auditOperation,
                            workerContext.getRunId(), batchGuids, response);
                }

                addResult(new PurgeBatchResult(batchGuids, null, batchFailures, true, e));
            } finally {
                RequestContext.clear();
                batchesProcessed++;
                batch.clear();
                LOG.debug("Purge worker processed batch {}", batchesProcessed);
            }
        }
    }

    public static class PurgeBatchConsumerBuilder implements WorkItemBuilder<PurgeBatchConsumer, String> {
        private final PurgeBatchExecutor    purgeBatchExecutor;
        private final AtlasAuditService     auditService;
        private final AuditOperation        auditOperation;
        private final int                   batchSize;
        private final PurgeWorkerContext    workerContext;

        public PurgeBatchConsumerBuilder(PurgeBatchExecutor purgeBatchExecutor, AtlasAuditService auditService,
                                         AuditOperation auditOperation, int batchSize, PurgeWorkerContext workerContext) {
            this.purgeBatchExecutor    = purgeBatchExecutor;
            this.auditService          = auditService;
            this.auditOperation        = auditOperation;
            this.batchSize             = batchSize;
            this.workerContext         = workerContext;
        }

        @Override
        public PurgeBatchConsumer build(BlockingQueue<String> queue) {
            return new PurgeBatchConsumer(queue, purgeBatchExecutor, auditService,
                    auditOperation, batchSize, workerContext);
        }
    }

    public static class PurgeBatchManager extends WorkItemManager<String, PurgeBatchConsumer> {
        public PurgeBatchManager(PurgeBatchConsumerBuilder builder, int batchSize, int numWorkers) {
            super(builder, PURGE_WORKERS_NAME, batchSize, numWorkers, true);
        }

        @Override
        public void shutdown() throws InterruptedException {
            LOG.debug("Shutting down purge worker manager");
            drain();
            super.shutdown();
        }
    }
}
