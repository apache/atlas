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
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;

import java.time.Instant;
import java.util.Date;
import java.util.Properties;

/**
 * C3 validation: mixed-index creation and mapping lifecycle against OpenSearch 2.x.
 */
public final class OpenSearchMixedIndexLifecycleDriver {

    public static final String GRAPH_INDEX_NAME   = "c3janus";
    public static final String MIXED_INDEX_NAME   = "c3mixed";
    public static final String BACKING_INDEX_NAME = "search";
    public static final String PHYSICAL_INDEX     = GRAPH_INDEX_NAME + "_" + MIXED_INDEX_NAME.toLowerCase();

    private OpenSearchMixedIndexLifecycleDriver() {
    }

    public static void execute() throws Exception {
        OpenSearchSmokeSupport.verifyOpenSearchReachable();
        OpenSearchSmokeSupport.registerOpenSearchBackend();
        OpenSearchSmokeSupport.deletePhysicalIndex(PHYSICAL_INDEX);

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

            PropertyKey keywordField = mgmt.makePropertyKey("keywordField").dataType(String.class).make();
            PropertyKey textField = mgmt.makePropertyKey("textField").dataType(String.class).make();
            PropertyKey textStringField = mgmt.makePropertyKey("textStringField").dataType(String.class).make();
            PropertyKey intField = mgmt.makePropertyKey("intField").dataType(Integer.class).make();
            PropertyKey boolField = mgmt.makePropertyKey("boolField").dataType(Boolean.class).make();
            PropertyKey dateField = mgmt.makePropertyKey("dateField").dataType(Date.class).make();
            PropertyKey instantField = mgmt.makePropertyKey("instantField").dataType(Instant.class).make();

            mgmt.buildIndex(MIXED_INDEX_NAME, Vertex.class)
                    .addKey(keywordField, Mapping.STRING.asParameter())
                    .addKey(textField, Mapping.TEXT.asParameter())
                    .addKey(textStringField, Mapping.TEXTSTRING.asParameter())
                    .addKey(intField)
                    .addKey(boolField)
                    .addKey(dateField)
                    .addKey(instantField)
                    .buildMixedIndex(BACKING_INDEX_NAME);

            mgmt.commit();

            mgmt = graph.openManagement();
            PropertyKey longField = mgmt.makePropertyKey("longField").dataType(Long.class).make();
            JanusGraphIndex existingMixedIndex = mgmt.getGraphIndex(MIXED_INDEX_NAME);
            mgmt.addIndexKey(existingMixedIndex, longField);
            mgmt.commit();
        }

        verifyPhysicalIndexCreated();
    }

    public static void main(String[] args) throws Exception {
        execute();

        System.out.println("[OK] Mixed index committed: " + MIXED_INDEX_NAME);
        System.out.println("[INFO] Expected physical OpenSearch index: " + PHYSICAL_INDEX);
        System.out.println("C3 mixed-index lifecycle driver finished. Inspect OpenSearch with:");
        System.out.println("  curl -s http://" + OpenSearchSmokeSupport.getOpenSearchHost() + ":"
                + OpenSearchSmokeSupport.getOpenSearchPort() + "/" + PHYSICAL_INDEX);
    }

    private static void verifyPhysicalIndexCreated() throws Exception {
        String mapping = OpenSearchSmokeSupport.httpGet("/" + PHYSICAL_INDEX + "/_mapping");
        if (!mapping.contains("keywordField") || !mapping.contains("longField")) {
            throw new IllegalStateException("Physical index mapping missing expected fields: " + mapping);
        }

        String alias = OpenSearchSmokeSupport.httpGet("/_alias/" + GRAPH_INDEX_NAME);
        if (!alias.contains(PHYSICAL_INDEX)) {
            throw new IllegalStateException("Expected alias " + GRAPH_INDEX_NAME + " -> " + PHYSICAL_INDEX
                    + " but got: " + alias);
        }
    }
}
