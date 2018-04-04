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

package org.apache.atlas.repository.graphdb.janus.migration;

import org.apache.atlas.repository.graphdb.janus.migration.pc.WorkItemBuilder;
import org.apache.atlas.repository.graphdb.janus.migration.pc.WorkItemConsumer;
import org.apache.atlas.repository.graphdb.janus.migration.pc.WorkItemManager;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class PostProcessManager {
    private static class Consumer extends WorkItemConsumer<Object> {
        private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

        private final Graph              bulkLoadGraph;
        private final GraphSONUtility    utility;
        private final String[]           properties;
        private final MappedElementCache cache;
        private final int                batchSize;
        private       long               counter;
        private       long               batchCounter;

        public Consumer(BlockingQueue<Object> queue, Graph bulkLoadGraph, GraphSONUtility utility,
                        String[] properties, MappedElementCache cache, int batchSize) {
            super(queue);

            this.bulkLoadGraph = bulkLoadGraph;
            this.utility         = utility;
            this.properties      = properties;
            this.cache           = cache;
            this.batchSize       = batchSize;
            this.counter         = 0;
            this.batchCounter    = 0;
        }

        @Override
        public void processItem(Object vertexId) {
            batchCounter++;
            counter++;

            try {
                Vertex v = bulkLoadGraph.traversal().V(vertexId).next();

                for (String p : properties) {
                    utility.replaceReferencedEdgeIdForList(bulkLoadGraph, cache, v, p);
                }

                if (batchCounter >= batchSize) {
                    LOG.info("[{}]: batch: {}: commit", counter, batchCounter);
                    commit();
                    batchCounter = 0;
                }
            }
            catch (Exception ex) {
                LOG.error("processItem: v[{}] error!", vertexId, ex);
            }
        }

        @Override
        protected void doCommit() {
            bulkLoadGraph.tx().commit();
        }
    }

    private static class ConsumerBuilder implements WorkItemBuilder<Consumer, Object> {
        private final Graph              bulkLoadGraph;
        private final GraphSONUtility    utility;
        private final int                batchSize;
        private final MappedElementCache cache;
        private final String[]           vertexPropertiesToPostProcess;

        public ConsumerBuilder(Graph bulkLoadGraph, GraphSONUtility utility, String[] propertiesToPostProcess, int batchSize) {
            this.bulkLoadGraph                 = bulkLoadGraph;
            this.utility                       = utility;
            this.batchSize                     = batchSize;
            this.cache                         = new MappedElementCache();
            this.vertexPropertiesToPostProcess = propertiesToPostProcess;
        }

        @Override
        public Consumer build(BlockingQueue<Object> queue) {
            return new Consumer(queue, bulkLoadGraph, utility, vertexPropertiesToPostProcess, cache, batchSize);
        }
    }

    static class WorkItemsManager extends WorkItemManager<Object, Consumer> {
        public WorkItemsManager(WorkItemBuilder builder, int batchSize, int numWorkers) {
            super(builder, batchSize, numWorkers);
        }
    }

    public static WorkItemsManager create(Graph bGraph, GraphSONUtility utility, String[] propertiesToPostProcess, int batchSize, int numWorkers) {
        ConsumerBuilder cb = new ConsumerBuilder(bGraph, utility, propertiesToPostProcess, batchSize);

        return new WorkItemsManager(cb, batchSize, numWorkers);
    }
}
