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

package org.apache.atlas.services;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.AtlasService;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.pc.WorkItemBuilder;
import org.apache.atlas.pc.WorkItemConsumer;
import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery.Result;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerV1;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.service.Service;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.PURGE;
import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;

@AtlasService
@Order(9)
@Component
public class PurgeService implements Service {
    private static final Logger LOG       = LoggerFactory.getLogger(PurgeService.class);
    private static final Logger PERF_LOG  = AtlasPerfTracer.getPerfLogger("service.Purge");
    private final AtlasGraph atlasGraph;
    private static Configuration atlasProperties;
    private final AtlasEntityStore entityStore;
    private final AtlasTypeRegistry typeRegistry;

    private static final String  ENABLE_PROCESS_SOFT_DELETION         = "atlas.enable.process.soft.delete";
    private static final boolean ENABLE_PROCESS_SOFT_DELETION_DEFAULT = false;
    private static final String  PURGE_ENABLED_SERVICE_TYPES          = "atlas.purge.enabled.services";
    private static final String  SOFT_DELETE_ENABLED_PROCESS_TYPES    = "atlas.soft.delete.enabled.process.types";
    private static final String  PURGE_BATCH_SIZE                     = "atlas.purge.batch.size";
    private static final int     DEFAULT_PURGE_BATCH_SIZE             = 1000; // fetching limit at a time
    private static final String  PURGE_WORKER_BATCH_SIZE              = "atlas.purge.worker.batch.size";
    private static final int     DEFAULT_PURGE_WORKER_BATCH_SIZE      = 100;
    private static final String  CLEANUP_WORKER_BATCH_SIZE            = "atlas.cleanup.worker.batch.size";
    private static final int     DEFAULT_CLEANUP_WORKER_BATCH_SIZE    = 100;
    private static final String  PURGE_RETENTION_PERIOD               = "atlas.purge.deleted.entity.retention.days";
    private static final int     PURGE_RETENTION_PERIOD_DEFAULT       = 30; // days
    private static final String  PURGE_WORKERS_COUNT                  = "atlas.purge.workers.count";
    private static final int     DEFAULT_PURGE_WORKERS_COUNT          = 2;
    private static final String  CLEANUP_WORKERS_COUNT                = "atlas.cleanup.workers.count";
    private static final int     DEFAULT_CLEANUP_WORKERS_COUNT        = 2;
    private static final String  PROCESS_ENTITY_CLEANER_THREAD_NAME   = "Process-Entity-Cleaner";
    private final        String  indexSearchPrefix                    = AtlasGraphUtilsV2.getIndexSearchPrefix();
    private static final int     DEFAULT_CLEANUP_BATCH_SIZE           = 1000;
    private static final String  CLEANUP_WORKERS_NAME                 = "Process-Cleanup-Worker";
    private static final String  PURGE_WORKERS_NAME                   = "Entity-Purge-Worker";
    private static final String  DELETED                              = "DELETED";
    private static final String  ACTIVE                               = "ACTIVE";
    private static final String  AND_STR                              = " AND ";

    static {
        try {
            atlasProperties = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }
    }

    @Inject
    public PurgeService(AtlasGraph atlasgraph, AtlasEntityStore entityStore, AtlasTypeRegistry typeRegistry) {
        this.atlasGraph   = atlasgraph;
        this.entityStore  = entityStore;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void start() throws AtlasException {
        if (!getSoftDeletionFlag()) {
            LOG.info("==> cleanup not enabled");
            return;
        }

        LOG.info("==> PurgeService.start()");

        launchCleanUp();

        LOG.info("<== Launched the clean up thread");
    }

    @Override
    public void stop() throws AtlasException {
        LOG.info("==> stopping the purge service");
    }

    public void launchCleanUp() {
        LOG.info("==> launching the new thread");

        Thread thread = new Thread(
                () -> {
                    long startTime = System.currentTimeMillis();
                    LOG.info("==> {} started", PROCESS_ENTITY_CLEANER_THREAD_NAME);
                    softDeleteProcessEntities();
                    LOG.info("==> exiting thread {}", PROCESS_ENTITY_CLEANER_THREAD_NAME);
                    long endTime = System.currentTimeMillis();
                    LOG.info("==> completed cleanup {} seconds !", (endTime - startTime) / 1000);
                });

        thread.setName(PROCESS_ENTITY_CLEANER_THREAD_NAME);
        thread.start();
        LOG.info("==> launched the thread for the clean up");
    }

    @SuppressWarnings("unchecked")
    @Timed
    public EntityMutationResponse purgeEntities() {
        LOG.info("==> PurgeService.purgeEntities()");
        // index query of specific batch size
        AtlasPerfTracer perf = null;
        EntityMutationResponse entityMutationResponse = new EntityMutationResponse();
        RequestContext requestContext = RequestContext.get();
        requestContext.setDeleteType(DeleteType.HARD); // hard delete
        requestContext.setPurgeRequested(true);

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "PurgeService.purgeEntities");
            }

            Set<String> allEligibleTypes = getEntityTypes();

            try {
                //bring n number of entities like 1000 at point of type Processes
                WorkItemsQualifier wiq = createQualifier(typeRegistry, entityStore, atlasGraph, getPurgeWorkerBatchSize(), getPurgeWorkersCount(), true);

                String indexQuery = getBulkQueryString(allEligibleTypes, getPurgeRetentionPeriod());
                Iterator<Result> itr = atlasGraph.indexQuery(VERTEX_INDEX, indexQuery).vertices(0, getPurgeBatchSize());
                LOG.info("==>  fetched Deleted entities");

                if (!itr.hasNext()) {
                    LOG.info("==> no Purge Entities found");
                    return entityMutationResponse;
                }

                Set<String> producedDeletionCandidates = new HashSet<>(); // look up

                while (itr.hasNext()) {
                    AtlasVertex vertex = itr.next().getVertex();

                    if (vertex == null) {
                        continue;
                    }

                    String guid = vertex.getProperty(GUID_PROPERTY_KEY, String.class);

                    if (!producedDeletionCandidates.contains(guid)) {
                        Set<String> instanceVertex = new HashSet<>();
                        instanceVertex.add(guid);

                        Set<AtlasVertex> deletionCandidates = entityStore.accumulateDeletionCandidates(instanceVertex);

                        for (AtlasVertex deletionCandidate : deletionCandidates) {
                            String deletionCandidateGuid = deletionCandidate.getProperty(GUID_PROPERTY_KEY, String.class);
                            if (!producedDeletionCandidates.contains(deletionCandidateGuid)) {
                                producedDeletionCandidates.add(deletionCandidateGuid);
                                wiq.checkProduce(deletionCandidate);
                            }
                        }
                    }
                }

                wiq.shutdown();

                // collecting all the results
                Queue results = wiq.getResults();

                LOG.info("==> Purged {} !", results.size());

                while (!results.isEmpty()) {
                    AtlasEntityHeader entityHeader = (AtlasEntityHeader) results.poll();
                    if (entityHeader == null) {
                        continue;
                    }
                    entityMutationResponse.addEntity(PURGE, entityHeader);
                }
            } catch (Exception ex) {
                LOG.error("purge: failed!", ex);
            } finally {
                LOG.info("purge: Done!");
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }

        LOG.info("<== PurgeService.purgeEntities()");

        return entityMutationResponse;
    }

    @SuppressWarnings("unchecked")
    @Timed
    public void softDeleteProcessEntities() {
        LOG.info("==> softDeleteProcessEntities()");

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "PurgeService.softDeleteProcessEntities");
            }

            Set<String> validProcessTypes = getProcessTypes();
            try {
                //bring n number of entities like 1000 at point of type Processes
                WorkItemsQualifier wiq = createQualifier(typeRegistry, entityStore, atlasGraph, getCleanupWorkerBatchSize(), getCleanUpWorkersCount(), false);
                int offset = 0;
                boolean moreResults = true;

                while (moreResults) {
                    String indexQuery = getBulkQueryString(validProcessTypes, 0);
                    Iterator<Result> itr = atlasGraph.indexQuery(VERTEX_INDEX, indexQuery).vertices(offset, DEFAULT_CLEANUP_BATCH_SIZE);
                    LOG.info("==>  fetched entities");

                    if (!itr.hasNext()) {
                        moreResults = false;
                    }

                    while (itr.hasNext()) {
                        AtlasVertex vertex = itr.next().getVertex();
                        if (vertex != null) {
                            wiq.checkProduce(vertex);
                        }
                    }

                    offset += DEFAULT_CLEANUP_BATCH_SIZE;
                    LOG.info("==> offset {}", offset);
                }

                wiq.shutdown();
            } catch (Exception ex) {
                LOG.error("cleanUp: failed!", ex);
            } finally {
                LOG.info("cleanUp: Done!");
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }

        LOG.info("<== softDeleteProcessEntities()");
    }

    static class EntityQualifier extends WorkItemConsumer<AtlasVertex> {
        private final Set<String> batch = new HashSet<>();
        private final AtlasEntityStore entityStore;
        private final AtlasTypeRegistry typeRegistry;
        private final AtlasGraph atlasGraph;
        private final boolean isPurgeEnabled;
        private int batchesProcessed;
        private int batchSize;

        public EntityQualifier(BlockingQueue<AtlasVertex> queue, AtlasTypeRegistry typeRegistry, AtlasEntityStore entityStore, AtlasGraph atlasGraph, boolean isPurgeEnabled, int batchSize) {
            super(queue);
            this.typeRegistry     = typeRegistry;
            this.entityStore      = entityStore;
            this.atlasGraph       = atlasGraph;
            this.isPurgeEnabled   = isPurgeEnabled;
            this.batchesProcessed = 0;
            this.batchSize        = batchSize;

            if (isPurgeEnabled) {
                LOG.info("==> consumers are purge enabled , batch size is {}", batchSize);
            } else {
                LOG.info("==> consumers are soft delete enabled , batch size is {}", batchSize);
            }
        }

        @Override
        protected void processItem(AtlasVertex vertex) {
            String guid =  vertex.getProperty(GUID_PROPERTY_KEY, String.class);
            LOG.info("==> processing the entity {}", guid);

            try {
                if (!isPurgeEnabled && !isEligible(vertex)) {
                    return;
                }
                batch.add(guid);
                commit();
            } catch (Exception ex) {
                LOG.info("{}", ex.getMessage());
            }
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
            EntityMutationResponse res;
            List<AtlasEntityHeader> results = Collections.emptyList();

            try {
                if (isPurgeEnabled) {
                    // purging not by directly
                    res = entityStore.purgeEntitiesInBatch(batch);
                } else {
                    List<String> batchList = new ArrayList<>(batch);
                    res = entityStore.deleteByIds(batchList);
                }

                results = isPurgeEnabled ? res.getPurgedEntities() : res.getDeletedEntities();

                if (CollectionUtils.isEmpty(results)) {
                    return;
                }

                for (AtlasEntityHeader entityHeader : results) {
                    addResult(entityHeader); // adding results
                }
            } catch (Exception e) {
                LOG.info("==> Exception: {}", e.getMessage());
            } finally {
                batchesProcessed++;
                batch.clear();
                LOG.info("==> Processed {} batch number with total {} entities purged!", batchesProcessed, results.size());
            }
        }
    }

    static class EntityQualifierBuilder implements WorkItemBuilder<EntityQualifier, AtlasVertex> {
        private final AtlasTypeRegistry typeRegistry;
        private final AtlasEntityStore entityStore;
        private final AtlasGraph atlasGraph;
        private final boolean isPurgeEnabled;
        private int batchSize;

        public EntityQualifierBuilder(AtlasTypeRegistry typeRegistry, AtlasEntityStore entityStore, AtlasGraph atlasGraph, boolean isPurgeEnabled, int batchSize) {
            this.typeRegistry   = typeRegistry;
            this.entityStore    = entityStore;
            this.atlasGraph     = atlasGraph;
            this.isPurgeEnabled = isPurgeEnabled;
            this.batchSize      = batchSize;
        }

        @Override
        public EntityQualifier build(BlockingQueue<AtlasVertex> queue) {
            return new EntityQualifier(queue, typeRegistry, entityStore, atlasGraph, isPurgeEnabled, batchSize);
        }
    }

    static class WorkItemsQualifier extends WorkItemManager<AtlasVertex, EntityQualifier> {
        public WorkItemsQualifier(WorkItemBuilder builder, int batchSize, int numWorkers, boolean isPurgeEnabled) {
            super(builder, isPurgeEnabled ? PURGE_WORKERS_NAME : CLEANUP_WORKERS_NAME, batchSize, numWorkers, true);
        }

        @Override
        public void shutdown() throws InterruptedException {
            LOG.info("==> Shutting down manager!");
            super.shutdown();
        }
    }

    public WorkItemsQualifier createQualifier(AtlasTypeRegistry typeRegistry, AtlasEntityStore entityStore, AtlasGraph atlasGraph, int batchSize, int numWorkers, boolean isPurgeEnabled) {
        EntityQualifierBuilder eqb = new EntityQualifierBuilder(typeRegistry, entityStore, atlasGraph, isPurgeEnabled, batchSize);
        LOG.info("==> creating the purge entity producer");
        return new WorkItemsQualifier(eqb, batchSize, numWorkers, isPurgeEnabled);
    }

    public static boolean isEligible(AtlasVertex vertex) {
        return DeleteHandlerV1.isSoftDeletableProcess(vertex);
    }

    private String getBulkQueryString(Set<String> typeNames, int retentionPeriod) {
        String joinedTypes = typeNames.stream()
                .map(t -> "\"" + t + "\"")
                .collect(Collectors.joining(" OR ", "(", ")"));

        String indexQuery = getString(retentionPeriod, joinedTypes);

        LOG.info("bulk index query : {}", indexQuery);
        return indexQuery;
    }

    private long timeThresholdMillis(int retentionPeriod) {
        long currentTimeMillis = System.currentTimeMillis();
        long retentionPeriodMillis = retentionPeriod * 24L * 60 * 60 * 1000;  // Convert days to ms
        return currentTimeMillis - retentionPeriodMillis;
    }

    private String getString(int retentionDays, String joinedTypes) {
        String baseQuery = indexSearchPrefix + "\"" + ENTITY_TYPE_PROPERTY_KEY + "\": " + joinedTypes + AND_STR +
                indexSearchPrefix + "\"" + STATE_PROPERTY_KEY + "\": (%s)";

        String indexQuery = (retentionDays > 0)
                ? String.format(baseQuery + AND_STR + indexSearchPrefix + "\"" + MODIFICATION_TIMESTAMP_PROPERTY_KEY + "\": [* TO %s]", DELETED, timeThresholdMillis(retentionDays))
                : String.format(baseQuery, ACTIVE);

        return indexQuery;
    }

    public boolean getSoftDeletionFlag() {
        if (atlasProperties != null) {
            return atlasProperties.getBoolean(ENABLE_PROCESS_SOFT_DELETION, ENABLE_PROCESS_SOFT_DELETION_DEFAULT);
        }
        return false;
    }

    private int getPurgeRetentionPeriod() {
        int retentionPeriod = PURGE_RETENTION_PERIOD_DEFAULT;

        if (atlasProperties != null) {
            retentionPeriod = atlasProperties.getInt(PURGE_RETENTION_PERIOD, PURGE_RETENTION_PERIOD_DEFAULT);
        }

        return Math.max(PURGE_RETENTION_PERIOD_DEFAULT, retentionPeriod); // for enforcing the minimum retention period  of 30 days
    }

    private Set<String> getProcessTypes() {
        Set<String> processTypes = new HashSet<>();

        if (atlasProperties != null) {
            String[] eligibleTypes = atlasProperties.getStringArray(SOFT_DELETE_ENABLED_PROCESS_TYPES); // e.g. hive, spark
            for (String type : eligibleTypes) {
                if (typeRegistry.isRegisteredType(type)) {
                    processTypes.add(type);
                }
            }
        }

        return processTypes;
    }

    public Set<String> getEntityTypes() {
        Set<String> entityTypes = new HashSet<>();

        if (atlasProperties != null) {
            String[] eligibleServiceTypes = atlasProperties.getStringArray(PURGE_ENABLED_SERVICE_TYPES); // e.g. hive, spark
            Set<String> serviceTypes = Arrays.stream(eligibleServiceTypes).collect(Collectors.toSet());

            for (AtlasEntityDef entityDef : typeRegistry.getAllEntityDefs()) {
                if (serviceTypes.contains(entityDef.getServiceType())) {
                    entityTypes.add(entityDef.getName());
                }
            }
        }

        return entityTypes;
    }

    private int getPurgeBatchSize() {
        if (atlasProperties != null) {
            return atlasProperties.getInt(PURGE_BATCH_SIZE, DEFAULT_PURGE_BATCH_SIZE);
        }
        return DEFAULT_PURGE_BATCH_SIZE;
    }

    private int getPurgeWorkersCount() {
        if (atlasProperties != null) {
            return atlasProperties.getInt(PURGE_WORKERS_COUNT, DEFAULT_PURGE_WORKERS_COUNT);
        }
        return DEFAULT_PURGE_WORKERS_COUNT;
    }

    private int getCleanUpWorkersCount() {
        if (atlasProperties != null) {
            return atlasProperties.getInt(CLEANUP_WORKERS_COUNT, DEFAULT_CLEANUP_WORKERS_COUNT);
        }
        return DEFAULT_CLEANUP_WORKERS_COUNT;
    }

    private int getPurgeWorkerBatchSize() {
        if (atlasProperties != null) {
            return atlasProperties.getInt(PURGE_WORKER_BATCH_SIZE, DEFAULT_PURGE_WORKER_BATCH_SIZE);
        }
        return DEFAULT_PURGE_WORKER_BATCH_SIZE;
    }

    private int getCleanupWorkerBatchSize() {
        if (atlasProperties != null) {
            return atlasProperties.getInt(CLEANUP_WORKER_BATCH_SIZE, DEFAULT_CLEANUP_WORKER_BATCH_SIZE);
        }
        return DEFAULT_CLEANUP_WORKER_BATCH_SIZE;
    }
}
