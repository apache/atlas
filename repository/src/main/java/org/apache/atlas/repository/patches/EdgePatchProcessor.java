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
package org.apache.atlas.repository.patches;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.pc.WorkItemBuilder;
import org.apache.atlas.pc.WorkItemConsumer;
import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.atlas.repository.Constants.ENTITY_TYPE_PROPERTY_KEY;

public abstract class EdgePatchProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(EdgePatchProcessor.class);

    public static final int NUM_WORKERS;
    public static final int BATCH_SIZE;

    private static final String NUM_WORKERS_PROPERTY   = "atlas.patch.numWorkers";
    private static final String BATCH_SIZE_PROPERTY    = "atlas.patch.batchSize";
    private static final String ATLAS_SOLR_SHARDS      = "ATLAS_SOLR_SHARDS";
    private static final String WORKER_NAME_PREFIX     = "patchWorkItem";
    private static final int    MAX_COMMIT_RETRY_COUNT = 3;

    private final AtlasGraph               graph;
    private final GraphBackedSearchIndexer indexer;
    private final AtlasTypeRegistry        typeRegistry;

    public EdgePatchProcessor(PatchContext context) {
        this.graph        = context.getGraph();
        this.indexer      = context.getIndexer();
        this.typeRegistry = context.getTypeRegistry();
    }

    public AtlasGraph getGraph() {
        return graph;
    }

    public GraphBackedSearchIndexer getIndexer() {
        return indexer;
    }

    public AtlasTypeRegistry getTypeRegistry() {
        return typeRegistry;
    }

    public void apply() throws AtlasBaseException {
        prepareForExecution();
        execute();
    }

    protected abstract void prepareForExecution() throws AtlasBaseException;

    protected abstract void submitEdgesToUpdate(WorkItemManager manager);

    protected abstract void processEdgesItem(String edgeId, AtlasEdge edge, String typeName, AtlasRelationshipType type) throws AtlasBaseException;

    private void execute() {
        WorkItemManager manager = new WorkItemManager(new ConsumerBuilder(graph, typeRegistry, this), WORKER_NAME_PREFIX, BATCH_SIZE, NUM_WORKERS, false);

        try {
            submitEdgesToUpdate(manager);

            manager.drain();
        } finally {
            try {
                manager.shutdown();
            } catch (InterruptedException e) {
                LOG.error("EdgePatchProcessor.execute(): interrupted during WorkItemManager shutdown.", e);
            }
        }
    }

    private static class ConsumerBuilder implements WorkItemBuilder<Consumer, String> {
        private final AtlasTypeRegistry  typeRegistry;
        private final AtlasGraph         graph;
        private final EdgePatchProcessor patchItemProcessor;

        public ConsumerBuilder(AtlasGraph graph, AtlasTypeRegistry typeRegistry, EdgePatchProcessor patchItemProcessor) {
            this.graph              = graph;
            this.typeRegistry       = typeRegistry;
            this.patchItemProcessor = patchItemProcessor;
        }

        @Override
        public Consumer build(BlockingQueue<String> queue) {
            return new Consumer(graph, typeRegistry, queue, patchItemProcessor);
        }
    }

    private static class Consumer extends WorkItemConsumer<String> {
        private final AtlasGraph        graph;
        private final AtlasTypeRegistry typeRegistry;
        private final AtomicLong         counter;
        private final EdgePatchProcessor individualItemProcessor;

        public Consumer(AtlasGraph graph, AtlasTypeRegistry typeRegistry, BlockingQueue<String> queue, EdgePatchProcessor individualItemProcessor) {
            super(queue);

            this.graph                   = graph;
            this.typeRegistry            = typeRegistry;
            this.counter                 = new AtomicLong(0);
            this.individualItemProcessor = individualItemProcessor;
        }

        @Override
        protected void commitDirty() {
            attemptCommit();

            LOG.info("Total: Commit: {}", counter.get());

            super.commitDirty();
        }

        @Override
        protected void doCommit() {
            if (counter.get() % BATCH_SIZE == 0) {
                LOG.info("Processed: {}", counter.get());

                attemptCommit();
            }
        }

        @Override
        protected void processItem(String edgeId) {
            counter.incrementAndGet();

            AtlasEdge edge = graph.getEdge(edgeId);

            if (edge == null) {
                LOG.warn("processItem(edgeId={}): AtlasEdge not found!", edgeId);

                return;
            }

            String                typeName         = edge.getProperty(ENTITY_TYPE_PROPERTY_KEY, String.class);
            AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(typeName);

            if (relationshipType == null) {
                return;
            }

            try {
                individualItemProcessor.processEdgesItem(edgeId, edge, typeName, relationshipType);

                doCommit();
            } catch (AtlasBaseException e) {
                LOG.error("Error processing: edgeId={}", edgeId, e);
            }
        }

        private void attemptCommit() {
            for (int retryCount = 1; retryCount <= MAX_COMMIT_RETRY_COUNT; retryCount++) {
                try {
                    graph.commit();

                    break;
                } catch (Exception ex) {
                    LOG.error("Commit exception: attempt {} of {}", retryCount, MAX_COMMIT_RETRY_COUNT, ex);

                    try {
                        Thread.sleep(300 * retryCount);
                    } catch (InterruptedException e) {
                        LOG.error("Commit exception: Pause: Interrupted!", e);
                    }
                }
            }
        }
    }

    static {
        int numWorkers = 3;
        int batchSize  = 300;

        try {
            Configuration config = ApplicationProperties.get();

            numWorkers = config.getInt(NUM_WORKERS_PROPERTY, config.getInt(ATLAS_SOLR_SHARDS, 1) * 3);
            batchSize  = config.getInt(BATCH_SIZE_PROPERTY, 300);

            LOG.info("EdgePatchProcessor: {}={}, {}={}", NUM_WORKERS_PROPERTY, numWorkers, BATCH_SIZE_PROPERTY, batchSize);
        } catch (Exception e) {
            LOG.error("Error retrieving configuration.", e);
        }

        NUM_WORKERS = numWorkers;
        BATCH_SIZE  = batchSize;
    }
}
