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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.graph.GraphSandboxUtil;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.runner.OpenSearchITBase;
import org.apache.commons.configuration2.Configuration;
import org.janusgraph.core.JanusGraph;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Issue #7: {@code spark_process.executionId} ({@code long}, {@code searchWeight=10}) must register on the OpenSearch
 * mixed index without requesting a text+keyword subfield. Exercises the same {@link AtlasJanusGraphManagement#addMixedIndex}
 * parameters {@link org.apache.atlas.repository.graph.GraphBackedSearchIndexer#createVertexIndex} produces after
 * {@link org.apache.atlas.repository.graph.GraphBackedSearchIndexer#needsKeywordSubfield} excludes non-{@link String}
 * property types.
 */
public class OpenSearchLongAttributeMixedIndexIT extends OpenSearchITBase {
    private static final String GRAPH_INDEX_NAME = "longattr";
    private static final String PHYSICAL_INDEX   = GRAPH_INDEX_NAME + "_vertex_index";
    /** Same logical name as bootstrap ({@code spark_process.executionId}). */
    private static final String EXECUTION_ID_PROPERTY = "spark_process.executionId";

    private JanusGraph janusGraph;
    private String     executionIdIndexFieldName;

    @BeforeClass(dependsOnMethods = "startOpenSearchContainer")
    public void setUp() throws Exception {
        if (Runtime.version().feature() >= 25) {
            throw new org.testng.SkipException(
                    "AtlasJanusGraphDatabase static init is not supported on JDK 25+ in this test");
        }

        String opensearchHost = org.apache.atlas.runner.OpenSearchTestContainerRunner.getHost();
        int opensearchPort = org.apache.atlas.runner.OpenSearchTestContainerRunner.getPort();

        File dataDir = new File(System.getProperty("java.io.tmpdir"), "atlas-opensearch-long-attr-test");
        deleteRecursively(dataDir.toPath());
        System.setProperty("atlas.data", dataDir.getAbsolutePath());
        System.setProperty("atlas.properties", "atlas-opensearch-application.properties");

        ApplicationProperties.forceReload();
        GraphSandboxUtil.create("opensearch-long-attr");
        AtlasJanusGraphDatabase.unload();

        Configuration config = ApplicationProperties.get();
        config.setProperty("atlas.graph.index.search.hostname", opensearchHost);
        config.setProperty("atlas.graph.index.search.port", opensearchPort);
        config.setProperty("atlas.graph.index.search.index-name", GRAPH_INDEX_NAME);
        config.setProperty(ApplicationProperties.INDEX_RECOVERY_CONF, false);

        deletePhysicalIndexIfPresent(opensearchHost, opensearchPort);

        Class.forName(AtlasJanusGraphDatabase.class.getName());
        Configuration atlasJanusConfig = AtlasJanusGraphDatabase.getConfiguration();
        Configuration janusConfig = OpenSearchAtlasJanusTestSupport.buildJanusGraphConfiguration(
                atlasJanusConfig, opensearchHost, opensearchPort, GRAPH_INDEX_NAME);
        janusGraph = AtlasJanusGraphDatabase.initJanusGraph(janusConfig);

        AtlasJanusGraph atlasGraph = new AtlasJanusGraph(janusGraph);

        try (AtlasGraphManagement mgmt = atlasGraph.getManagementSystem()) {
            mgmt.createVertexMixedIndex(Constants.VERTEX_INDEX, Constants.BACKING_INDEX, java.util.Collections.emptyList());

            AtlasPropertyKey executionIdKey = mgmt.makePropertyKey(EXECUTION_ID_PROPERTY, Long.class, AtlasCardinality.SINGLE);
            // GraphBackedSearchIndexer path for long + searchWeight=10 after the fix: isStringField=false, withKeywordSubfield=false.
            executionIdIndexFieldName = mgmt.addMixedIndex(Constants.VERTEX_INDEX, executionIdKey, false, false);

            mgmt.updateSchemaStatus();
            mgmt.setIsSuccess(true);
        }
        atlasGraph.commit();

        Thread.sleep(1500);
    }

    @AfterClass
    public void tearDown() {
        if (janusGraph != null && janusGraph.isOpen()) {
            janusGraph.close();
        }
        AtlasJanusGraphDatabase.unload();
        ApplicationProperties.forceReload();
    }

    @Test
    public void longExecutionIdFieldRegistersWithoutKeywordSubfield() throws Exception {
        String host = org.apache.atlas.runner.OpenSearchTestContainerRunner.getHost();
        int port = org.apache.atlas.runner.OpenSearchTestContainerRunner.getPort();
        String mapping = httpGet(host, port, "/" + PHYSICAL_INDEX + "/_mapping");

        assertNotTextWithKeywordSubfield(mapping, executionIdIndexFieldName);
        assertFalse(AtlasOpenSearchIndexClient.usesKeywordSubfield(executionIdIndexFieldName),
                "long executionId must not be registered for .keyword term routing");
    }

    private static void assertNotTextWithKeywordSubfield(String mapping, String fieldName) {
        int fieldStart = mapping.indexOf("\"" + fieldName + "\"");
        assertTrue(fieldStart >= 0, "mapping should contain " + fieldName + ": " + mapping);

        String fieldSection = mapping.substring(fieldStart, Math.min(mapping.length(), fieldStart + 400));
        assertFalse(fieldSection.contains("\"keyword\""),
                fieldName + " must not use a keyword subfield (numeric mapping), got: " + fieldSection);
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }

        Files.walk(path)
                .sorted(java.util.Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static void deletePhysicalIndexIfPresent(String host, int port) throws IOException {
        URL url = new URL("http://" + host + ":" + port + "/" + PHYSICAL_INDEX);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("DELETE");
        connection.getResponseCode();
    }

    private static String httpGet(String host, int port, String path) throws IOException {
        URL url = new URL("http://" + host + ":" + port + path);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("GET");
        try (InputStream in = connection.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
