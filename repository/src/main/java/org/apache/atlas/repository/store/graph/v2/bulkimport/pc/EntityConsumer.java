/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.store.graph.v2.bulkimport.pc;

import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.pc.WorkItemConsumer;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityStreamForImport;
import org.apache.atlas.repository.store.graph.v2.BulkImporterImpl;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class EntityConsumer extends WorkItemConsumer<AtlasEntity.AtlasEntityWithExtInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(EntityConsumer.class);
    private static final int MAX_COMMIT_RETRY_COUNT = 3;

    private final int batchSize;
    private AtomicLong counter = new AtomicLong(1);
    private AtomicLong currentBatch = new AtomicLong(1);

    private final AtlasGraph atlasGraph;
    private final AtlasEntityStore entityStoreV2;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityGraphRetriever;

    private List<AtlasEntity.AtlasEntityWithExtInfo> entityBuffer = new ArrayList<>();
    private List<EntityMutationResponse> localResults = new ArrayList<>();

    public EntityConsumer(AtlasGraph atlasGraph, AtlasEntityStore entityStore,
                          EntityGraphRetriever entityGraphRetriever, AtlasTypeRegistry typeRegistry,
                          BlockingQueue queue, int batchSize) {
        super(queue);

        this.atlasGraph = atlasGraph;
        this.entityStoreV2 = entityStore;
        this.entityGraphRetriever = entityGraphRetriever;
        this.typeRegistry = typeRegistry;
        this.batchSize = batchSize;
    }

    @Override
    protected void processItem(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) {
        int delta = (MapUtils.isEmpty(entityWithExtInfo.getReferredEntities())
                ? 1
                : entityWithExtInfo.getReferredEntities().size()) + 1;

        long currentCount = counter.addAndGet(delta);
        currentBatch.addAndGet(delta);
        entityBuffer.add(entityWithExtInfo);

        try {
            processEntity(entityWithExtInfo, currentCount);
            attemptCommit();
        } catch (Exception e) {
            LOG.info("Data loss: Please re-submit!", e);
        }
    }

    private void processEntity(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo, long currentCount) {
        try {
            RequestContext.get().setImportInProgress(true);
            AtlasEntityStreamForImport oneEntityStream = new AtlasEntityStreamForImport(entityWithExtInfo, null);

            LOG.debug("Processing: {}", currentCount);
            EntityMutationResponse result = entityStoreV2.createOrUpdateForImportNoCommit(oneEntityStream);
            localResults.add(result);
        } catch (AtlasBaseException e) {
            addResult(entityWithExtInfo.getEntity().getGuid());
            LOG.warn("Exception: {}", entityWithExtInfo.getEntity().getGuid(), e);
        } catch (AtlasSchemaViolationException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Entity: {}", entityWithExtInfo.getEntity().getGuid(), e);
            }

            BulkImporterImpl.updateVertexGuid(typeRegistry, entityGraphRetriever, entityWithExtInfo.getEntity());
        }
    }

    private void attemptCommit() {
        if (currentBatch.get() < batchSize) {
            return;
        }

        doCommit();
    }

    @Override
    protected void doCommit() {
        for (int retryCount = 1; retryCount <= MAX_COMMIT_RETRY_COUNT; retryCount++) {
            if (commitWithRetry(retryCount)) {
                return;
            }
        }

        LOG.error("Retries exceeded! Potential data loss! Please correct data and re-attempt. Buffer: {}: Counter: {}", entityBuffer.size(), counter.get());
        clear();
    }

    @Override
    protected void commitDirty() {
        super.commitDirty();
        LOG.info("Total: Commit: {}", counter.get());
        counter.set(0);
    }

    private boolean commitWithRetry(int retryCount) {
        try {
            atlasGraph.commit();
            if (LOG.isDebugEnabled()) {
                LOG.debug("Commit: Done!: Buffer: {}: Batch: {}: Counter: {}", entityBuffer.size(), currentBatch.get(), counter.get());
            }

            dispatchResults();
            return true;
        } catch (Exception ex) {
            rollbackPauseRetry(retryCount, ex);
            return false;
        }
    }

    private void rollbackPauseRetry(int retryCount, Exception ex) {
        atlasGraph.rollback();
        clearCache();

        LOG.error("Rollback: Done! Buffer: {}: Counter: {}: Retry count: {}", entityBuffer.size(), counter.get(), retryCount);
        pause(retryCount);
        String exceptionClass = ex.getClass().getSimpleName();
        if (!exceptionClass.equals("JanusGraphException") && !exceptionClass.equals("PermanentLockingException")) {
            LOG.warn("Commit error! Will pause and retry: Buffer: {}: Counter: {}: Retry count: {}", entityBuffer.size(), counter.get(), retryCount, ex);
        }
        retryProcessEntity(retryCount);
    }

    private void retryProcessEntity(int retryCount) {
        LOG.info("Replaying: Starting!: Buffer: {}: Retry count: {}", entityBuffer.size(), retryCount);
        for (AtlasEntity.AtlasEntityWithExtInfo e : entityBuffer) {
            processEntity(e, counter.get());
        }
        LOG.info("Replaying: Done!: Buffer: {}: Retry count: {}", entityBuffer.size(), retryCount);
    }

    private void dispatchResults() {
        localResults.stream().forEach(x -> {
            addResultsFromResponse(x.getCreatedEntities());
            addResultsFromResponse(x.getUpdatedEntities());
            addResultsFromResponse(x.getDeletedEntities());
        });

        clear();
    }

    private void pause(int retryCount) {
        try {
            Thread.sleep(1000 * retryCount);
        } catch (InterruptedException e) {
            LOG.error("pause: Interrupted!", e);
        }
    }

    private void addResultsFromResponse(List<AtlasEntityHeader> entities) {
        if (CollectionUtils.isEmpty(entities)) {
            return;
        }

        for (AtlasEntityHeader eh : entities) {
            addResult(eh.getGuid());
        }
    }

    private void clear() {
        localResults.clear();
        entityBuffer.clear();
        clearCache();
        currentBatch.set(0);
    }

    private void clearCache() {
        GraphTransactionInterceptor.clearCache();
        RequestContext.get().clearCache();
    }
}
