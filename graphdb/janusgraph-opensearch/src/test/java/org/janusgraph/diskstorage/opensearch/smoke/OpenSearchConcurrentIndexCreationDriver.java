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
package org.janusgraph.diskstorage.opensearch.smoke;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.List;

/**
 * Validates that two Atlas/JanusGraph nodes creating the same OpenSearch-backed mixed index concurrently must
 * both succeed. Before the idempotent-create fix, the node that lost the check-then-create race failed with
 * {@code resource_already_exists_exception}. This driver reproduces that race against a real OpenSearch container.
 */
public final class OpenSearchConcurrentIndexCreationDriver {

    public static final String GRAPH_INDEX_NAME   = "concjanus";
    public static final String MIXED_INDEX_NAME   = "concmixed";
    public static final String BACKING_INDEX_NAME = "search";
    public static final String PHYSICAL_INDEX     = GRAPH_INDEX_NAME + "_" + MIXED_INDEX_NAME.toLowerCase();

    private OpenSearchConcurrentIndexCreationDriver() {
    }

    public static void execute() throws Exception {
        OpenSearchSmokeSupport.verifyOpenSearchReachable();
        OpenSearchSmokeSupport.registerOpenSearchBackend();
        OpenSearchSmokeSupport.deletePhysicalIndex(PHYSICAL_INDEX);

        int                       nodeCount = 2;
        CountDownLatch            startGate = new CountDownLatch(1);
        CountDownLatch            doneGate  = new CountDownLatch(nodeCount);
        List<Throwable>           failures  = new CopyOnWriteArrayList<>();

        for (int i = 0; i < nodeCount; i++) {
            final int nodeId = i;
            Thread thread = new Thread(() -> {
                try {
                    startGate.await(); // release both threads as simultaneously as possible to force the race
                    createMixedIndex();
                } catch (Throwable t) {
                    failures.add(t);
                } finally {
                    doneGate.countDown();
                }
            }, "os-index-creator-" + nodeId);
            thread.start();
        }

        startGate.countDown();
        doneGate.await();

        if (!failures.isEmpty()) {
            IllegalStateException error = new IllegalStateException(
                    "Concurrent OpenSearch index creation must succeed on all nodes but " + failures.size()
                            + " failed; first failure: " + failures.get(0));
            error.initCause(failures.get(0));
            throw error;
        }

        verifyPhysicalIndexCreated();
    }

    private static void createMixedIndex() {
        Properties properties = new Properties();
        properties.setProperty("storage.backend", "inmemory");
        properties.setProperty("index." + BACKING_INDEX_NAME + ".backend", "opensearch");
        properties.setProperty("index." + BACKING_INDEX_NAME + ".hostname", OpenSearchSmokeSupport.getOpenSearchHost());
        properties.setProperty("index." + BACKING_INDEX_NAME + ".port",
                String.valueOf(OpenSearchSmokeSupport.getOpenSearchPort()));
        properties.setProperty("index." + BACKING_INDEX_NAME + ".index-name", GRAPH_INDEX_NAME);
        properties.setProperty("index." + BACKING_INDEX_NAME + ".opensearch.setup-max-open-scroll-contexts", "false");

        org.apache.commons.configuration2.Configuration config =
                org.apache.commons.configuration2.ConfigurationConverter.getConfiguration(properties);

        try (JanusGraph graph = JanusGraphFactory.open(config)) {
            JanusGraphManagement mgmt = graph.openManagement();

            if (mgmt.getGraphIndex(MIXED_INDEX_NAME) == null) {
                PropertyKey nameField = mgmt.containsPropertyKey("nameField")
                        ? mgmt.getPropertyKey("nameField")
                        : mgmt.makePropertyKey("nameField").dataType(String.class).make();

                mgmt.buildIndex(MIXED_INDEX_NAME, Vertex.class)
                        .addKey(nameField, Mapping.STRING.asParameter())
                        .buildMixedIndex(BACKING_INDEX_NAME);
            }

            mgmt.commit();
        }
    }

    private static void verifyPhysicalIndexCreated() throws Exception {
        String mapping = OpenSearchSmokeSupport.httpGet("/" + PHYSICAL_INDEX + "/_mapping");
        if (!mapping.contains("nameField")) {
            throw new IllegalStateException("Physical index mapping missing expected field 'nameField': " + mapping);
        }

        String settings = OpenSearchSmokeSupport.httpGet("/" + PHYSICAL_INDEX + "/_settings");
        if (!settings.contains(PHYSICAL_INDEX)) {
            throw new IllegalStateException("Physical index settings missing for " + PHYSICAL_INDEX + ": " + settings);
        }
    }

    public static void main(String[] args) throws Exception {
        execute();
        System.out.println("[OK] Concurrent index creation succeeded on all nodes for " + PHYSICAL_INDEX);
    }
}
