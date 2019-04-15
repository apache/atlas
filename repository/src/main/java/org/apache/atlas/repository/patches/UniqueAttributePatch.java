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
import org.apache.atlas.repository.graphdb.*;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.atlas.model.patches.AtlasPatch.PatchStatus.APPLIED;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.getIdFromVertex;

public class UniqueAttributePatch extends AtlasPatchHandler {
    private static final Logger LOG = LoggerFactory.getLogger(UniqueAttributePatch.class);

    private static final String PATCH_ID          = "JAVA_PATCH_0000_001";
    private static final String PATCH_DESCRIPTION = "Add __u_ property for each unique attribute of active entities";

    private final AtlasGraph               graph;
    private final GraphBackedSearchIndexer indexer;
    private final AtlasTypeRegistry        typeRegistry;

    public UniqueAttributePatch(PatchContext context) {
        super(context.getPatchRegistry(), PATCH_ID, PATCH_DESCRIPTION);

        this.graph        = context.getGraph();
        this.indexer      = context.getIndexer();
        this.typeRegistry = context.getTypeRegistry();
    }

    @Override
    public void apply() {
        TypeNameAttributeCache        typeNameAttributeCache = registerUniqueAttributeForTypes();
        UniqueAttributePatchProcessor patchProcessor         = new UniqueAttributePatchProcessor(this.graph);

        patchProcessor.apply(typeNameAttributeCache.getAll());

        setStatus(APPLIED);

        LOG.info("UniqueAttributePatch: {}; status: {}", getPatchId(), getStatus());
    }

    private TypeNameAttributeCache registerUniqueAttributeForTypes() {
        TypeNameAttributeCache ret = new TypeNameAttributeCache();

        for (AtlasEntityType entityType : typeRegistry.getAllEntityTypes()) {
            createIndexForUniqueAttributes(entityType.getTypeName(), entityType.getUniqAttributes().values());

            ret.add(entityType, entityType.getUniqAttributes().values());
        }

        return ret;
    }

    private boolean createIndexForUniqueAttributes(String typeName, Collection<AtlasAttribute> attributes) {
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

            return true;
        } catch (IndexException e) {
            LOG.error("Error creating index: type: {}", typeName, e);
            return false;
        }
    }

    public static class UniqueAttributePatchProcessor {
        private static final String NUM_WORKERS_PROPERTY = "atlas.patch.unique_attribute_patch.numWorkers";
        private static final String BATCH_SIZE_PROPERTY  = "atlas.patch.unique_attribute_patch.batchSize";
        private static final String ATLAS_SOLR_SHARDS    = "ATLAS_SOLR_SHARDS";
        private static final int    NUM_WORKERS;
        private static final int    BATCH_SIZE;

        private final AtlasGraph graph;

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

        public UniqueAttributePatchProcessor(AtlasGraph graph) {
            this.graph = graph;
        }

        public void apply(final Map<String, Collection<AtlasAttribute>> typeUniqueAttributeCache) {
            WorkItemManager manager = null;

            try {
                Iterator<AtlasVertex> iterator = graph.getVertices().iterator();

                if (iterator.hasNext()) {
                    manager = new WorkItemManager<>(new ConsumerBuilder(graph), BATCH_SIZE, NUM_WORKERS);

                    LOG.info("Processing: Started...");

                    while (iterator.hasNext()) {
                        AtlasVertex vertex = iterator.next();

                        if (!AtlasGraphUtilsV2.isEntityVertex(vertex)) {
                            continue;
                        }

                        String typeName = AtlasGraphUtilsV2.getTypeName(vertex);

                        submitForProcessing(typeName, vertex, manager, typeUniqueAttributeCache.get(typeName));
                    }

                    manager.drain();
                }
            } catch (Exception ex) {
                LOG.error("Error: ", ex);
            } finally {
                if (manager != null) {
                    try {
                        manager.shutdown();
                    } catch (InterruptedException e) {
                        LOG.error("Interrupted", e);
                    }
                }
            }
        }

        private static int getDefaultNumWorkers() throws AtlasException {
            return ApplicationProperties.get().getInt(ATLAS_SOLR_SHARDS, 1) * 3;
        }

        private void submitForProcessing(String typeName, AtlasVertex vertex, WorkItemManager manager, Collection<AtlasAttribute> uniqAttributes) {
            WorkItem workItem = new WorkItem(typeName, (Long) vertex.getId(), uniqAttributes);

            manager.checkProduce(workItem);
        }


        private static class WorkItem {
            private final String                     typeName;
            private final long                       id;
            private final Collection<AtlasAttribute> uniqueAttributeValues;

            public WorkItem(String typeName, long id, Collection<AtlasAttribute> uniqueAttributeValues) {
                this.typeName              = typeName;
                this.id                    = id;
                this.uniqueAttributeValues = uniqueAttributeValues;
            }
        }

        private static class Consumer extends WorkItemConsumer<WorkItem> {
            private static int MAX_COMMIT_RETRY_COUNT = 3;

            private final AtlasGraph graph;
            private final AtomicLong counter;

            public Consumer(AtlasGraph graph, BlockingQueue<WorkItem> queue) {
                super(queue);

                this.graph   = graph;
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
            protected void processItem(WorkItem wi) {
                counter.incrementAndGet();

                String typeName = wi.typeName;

                if(wi.uniqueAttributeValues == null) {
                    return;
                }

                AtlasVertex vertex = graph.getVertex(Long.toString(wi.id));

                if (vertex == null) {
                    LOG.warn("processItem: AtlasVertex with id: ({}): not found!", wi.id);

                    return;
                }

                if (AtlasGraphUtilsV2.isTypeVertex(vertex)) {
                    return;
                }

                AtlasEntity.Status status = AtlasGraphUtilsV2.getState(vertex);

                if (status != AtlasEntity.Status.ACTIVE) {
                    return;
                }

                try {
                    LOG.debug("processItem: {}", wi.id);

                    for (AtlasAttribute attribute : wi.uniqueAttributeValues) {
                        String                       uniquePropertyKey = attribute.getVertexUniquePropertyName();
                        Collection<? extends String> propertyKeys      = vertex.getPropertyKeys();
                        Object                       uniqAttrValue     = null;

                        if (propertyKeys != null && propertyKeys.contains(uniquePropertyKey)) {
                            LOG.debug("processItem: {}: Skipped!", wi.id);
                        } else {
                            try {
                                String propertyKey = attribute.getVertexPropertyName();

                                uniqAttrValue = EntityGraphRetriever.mapVertexToPrimitive(vertex, propertyKey, attribute.getAttributeDef());

                                AtlasGraphUtilsV2.setEncodedProperty(vertex, uniquePropertyKey, uniqAttrValue);
                            } catch(AtlasSchemaViolationException ex) {
                                LOG.error("Duplicates detected: {}:{}:{}", typeName, uniqAttrValue, getIdFromVertex(vertex));
                            }
                        }

                        commit();
                    }

                    LOG.debug("processItem: {}: Done!", wi.id);
                } catch (Exception ex) {
                    LOG.error("Error found: {}: {}", typeName, wi.id, ex);
                }
            }
        }

        private class ConsumerBuilder implements WorkItemBuilder<Consumer, WorkItem> {
            private final AtlasGraph graph;

            public ConsumerBuilder(AtlasGraph graph) {
                this.graph = graph;
            }

            @Override
            public Consumer build(BlockingQueue<WorkItem> queue) {
                return new Consumer(graph, queue);
            }
        }
    }

    public static class TypeNameAttributeCache {
        private Map<String, Collection<AtlasAttribute>> typeUniqueAttributeCache = new HashMap<>();

        public void add(AtlasEntityType entityType, Collection<AtlasAttribute> values) {
            typeUniqueAttributeCache.put(entityType.getTypeName(), values);
        }

        public Collection<AtlasAttribute> get(String typeName) {
            return typeUniqueAttributeCache.get(typeName);
        }

        public boolean has(String typeName) {
            return typeUniqueAttributeCache.containsKey(typeName);
        }

        public Map<String, Collection<AtlasAttribute>> getAll() {
            return typeUniqueAttributeCache;
        }
    }
}
