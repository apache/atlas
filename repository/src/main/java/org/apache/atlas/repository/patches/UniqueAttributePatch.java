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
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.pc.WorkItemBuilder;
import org.apache.atlas.pc.WorkItemConsumer;
import org.apache.atlas.pc.WorkItemManager;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer.UniqueKind;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.getIdFromVertex;

public class UniqueAttributePatch extends AtlasPatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(UniqueAttributePatch.class);

    private static final String PATCH_ID          = "JAVA_PATCH_0000_001";
    private static final String PATCH_DESCRIPTION = "Add __u_ property for each unique attribute of active entities";

    private final PatchContext context;

    public UniqueAttributePatch(PatchContext context) {
        super(context.getPatchRegistry(), PATCH_ID, PATCH_DESCRIPTION);

        this.context = context;
    }

    @Override
    public void apply() {
        UniqueAttributePatchProcessor patchProcessor = new UniqueAttributePatchProcessor(context);

        patchProcessor.apply();

        setStatus(APPLIED);

        LOG.info("UniqueAttributePatch.apply(): patchId={}, status={}", getPatchId(), getStatus());
    }

    public static class UniqueAttributePatchProcessor {
        private static final String NUM_WORKERS_PROPERTY = "atlas.patch.unique_attribute_patch.numWorkers";
        private static final String BATCH_SIZE_PROPERTY  = "atlas.patch.unique_attribute_patch.batchSize";
        private static final String ATLAS_SOLR_SHARDS    = "ATLAS_SOLR_SHARDS";
        private static final int    NUM_WORKERS;
        private static final int    BATCH_SIZE;

        private final AtlasGraph               graph;
        private final GraphBackedSearchIndexer indexer;
        private final AtlasTypeRegistry        typeRegistry;

        static {
            int numWorkers = 3;
            int batchSize  = 300;

            try {
                numWorkers = ApplicationProperties.get().getInt(NUM_WORKERS_PROPERTY, getDefaultNumWorkers());
                batchSize  = ApplicationProperties.get().getInt(BATCH_SIZE_PROPERTY, 300);

                LOG.info("UniqueAttributePatch: {}={}, {}={}", NUM_WORKERS_PROPERTY, numWorkers, BATCH_SIZE_PROPERTY, batchSize);
            } catch (Exception e) {
                LOG.error("Error retrieving configuration.", e);
            }

            NUM_WORKERS = numWorkers;
            BATCH_SIZE  = batchSize;
        }

        public UniqueAttributePatchProcessor(PatchContext context) {
            this.graph        = context.getGraph();
            this.indexer      = context.getIndexer();
            this.typeRegistry = context.getTypeRegistry();
        }

        public void apply() {
            createIndexForUniqueAttributes();
            addUniqueAttributeToAllVertices();
        }

        private void addUniqueAttributeToAllVertices() {
            Iterable<Object> iterable = graph.query().vertexIds();
            WorkItemManager manager = new WorkItemManager(new ConsumerBuilder(graph, typeRegistry), BATCH_SIZE, NUM_WORKERS);
            try {
                for (Iterator<Object> iter = iterable.iterator(); iter.hasNext(); ) {
                    Object vertexId = iter.next();
                    submitForProcessing((Long) vertexId, manager);
                }

                manager.drain();
            } finally {
                try {
                    manager.shutdown();
                } catch (InterruptedException e) {
                    LOG.error("UniqueAttributePatchProcessor.apply(): interrupted during WorkItemManager shutdown", e);
                }
            }
        }

        private void createIndexForUniqueAttributes() {
            for (AtlasEntityType entityType : typeRegistry.getAllEntityTypes()) {

                String typeName = entityType.getTypeName();
                Collection<AtlasAttribute> uniqAttributes = entityType.getUniqAttributes().values();

                if (CollectionUtils.isEmpty(uniqAttributes)) {
                    LOG.info("UniqueAttributePatchProcessor.apply(): no unique attribute for entity-type {}", typeName);

                    continue;
                }

                createIndexForUniqueAttributes(typeName, uniqAttributes);
            }
        }

        private void createIndexForUniqueAttributes(String typeName, Collection<AtlasAttribute> attributes) {
            try {
                AtlasGraphManagement management = graph.getManagementSystem();

                for (AtlasAttribute attribute : attributes) {
                    String uniquePropertyName = attribute.getVertexUniquePropertyName();

                    if (management.getPropertyKey(uniquePropertyName) != null) {
                        continue;
                    }

                    AtlasAttributeDef attributeDef   = attribute.getAttributeDef();
                    boolean           isIndexable    = attributeDef.getIsIndexable();
                    String            attribTypeName = attributeDef.getTypeName();
                    Class             propertyClass  = indexer.getPrimitiveClass(attribTypeName);
                    AtlasCardinality  cardinality    = indexer.toAtlasCardinality(attributeDef.getCardinality());

                    indexer.createVertexIndex(management, uniquePropertyName, UniqueKind.PER_TYPE_UNIQUE, propertyClass, cardinality, isIndexable, true);
                }

                indexer.commit(management);
                graph.commit();

                LOG.info("Unique attributes: type: {}: Registered!", typeName);
            } catch (IndexException e) {
                LOG.error("Error creating index: type: {}", typeName, e);
            }
        }

        private static int getDefaultNumWorkers() throws AtlasException {
            return ApplicationProperties.get().getInt(ATLAS_SOLR_SHARDS, 1) * 3;
        }

        private void submitForProcessing(Long vertexId, WorkItemManager manager) {
            manager.checkProduce(vertexId);
        }

        private static class ConsumerBuilder implements WorkItemBuilder<Consumer, Long> {
            private final AtlasTypeRegistry typeRegistry;
            private final AtlasGraph graph;

            public ConsumerBuilder(AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
                this.graph = graph;
                this.typeRegistry = typeRegistry;
            }

            @Override
            public Consumer build(BlockingQueue<Long> queue) {
                return new Consumer(graph, typeRegistry, queue);
            }
        }

        private static class Consumer extends WorkItemConsumer<Long> {
            private int MAX_COMMIT_RETRY_COUNT = 3;
            private final AtlasGraph graph;
            private final AtlasTypeRegistry typeRegistry;

            private final AtomicLong counter;

            public Consumer(AtlasGraph graph, AtlasTypeRegistry typeRegistry, BlockingQueue<Long> queue) {
                super(queue);

                this.graph        = graph;
                this.typeRegistry = typeRegistry;
                this.counter = new AtomicLong(0);
            }

            @Override
            protected void doCommit() {
                if (counter.get() % BATCH_SIZE == 0) {
                    LOG.info("Processed: {}", counter.get());

                    attemptCommit();
                }
            }

            @Override
            protected void commitDirty() {
                attemptCommit();

                LOG.info("Total: Commit: {}", counter.get());

                super.commitDirty();
            }

            private void attemptCommit() {
                for (int retryCount = 1; retryCount <= MAX_COMMIT_RETRY_COUNT; retryCount++) {
                    try {
                        graph.commit();

                        break;
                    } catch(Exception ex) {
                        LOG.error("Commit exception: ", retryCount, ex);

                        try {
                            Thread.currentThread().sleep(300 * retryCount);
                        } catch (InterruptedException e) {
                            LOG.error("Commit exception: Pause: Interrputed!", e);
                        }
                    }
                }
            }

            @Override
            protected void processItem(Long vertexId) {
                AtlasVertex vertex = graph.getVertex(Long.toString(vertexId));

                if (vertex == null) {
                    LOG.warn("processItem(vertexId={}): AtlasVertex not found!", vertexId);

                    return;
                }

                if (AtlasGraphUtilsV2.isTypeVertex(vertex)) {
                    return;
                }

                if (AtlasGraphUtilsV2.getState(vertex) != AtlasEntity.Status.ACTIVE) {
                    return;
                }

                String          typeName   = AtlasGraphUtilsV2.getTypeName(vertex);
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
                if (entityType == null) {
                    return;
                }

                processItem(vertexId, vertex, typeName, entityType);
            }

            private void processItem(Long vertexId, AtlasVertex vertex, String typeName, AtlasEntityType entityType) {
                try {
                    counter.incrementAndGet();
                    LOG.debug("processItem(typeName={}, vertexId={})", typeName, vertexId);

                    for (AtlasAttribute attribute : entityType.getUniqAttributes().values()) {
                        String                       uniquePropertyKey = attribute.getVertexUniquePropertyName();
                        Collection<? extends String> propertyKeys      = vertex.getPropertyKeys();
                        Object                       uniqAttrValue     = null;

                        if (propertyKeys == null || !propertyKeys.contains(uniquePropertyKey)) {
                            try {
                                String propertyKey = attribute.getVertexPropertyName();

                                uniqAttrValue = EntityGraphRetriever.mapVertexToPrimitive(vertex, propertyKey, attribute.getAttributeDef());

                                AtlasGraphUtilsV2.setEncodedProperty(vertex, uniquePropertyKey, uniqAttrValue);
                            } catch(AtlasSchemaViolationException ex) {
                                LOG.error("Duplicates detected: {}:{}:{}", typeName, uniqAttrValue, getIdFromVertex(vertex));
                                vertex.removeProperty(uniquePropertyKey);
                            }
                        }
                    }

                    LOG.debug("processItem(typeName={}, vertexId={}): Done!", typeName, vertexId);
                } catch (Exception ex) {
                    LOG.error("processItem(typeName={}, vertexId={}): failed!", typeName, vertexId, ex);
                } finally {
                    commit();
                }
            }
        }
    }
}
