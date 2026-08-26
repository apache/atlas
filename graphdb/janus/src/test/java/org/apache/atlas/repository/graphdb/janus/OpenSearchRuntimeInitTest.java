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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.graph.GraphSandboxUtil;
import org.apache.atlas.runner.OpenSearchITBase;
import org.apache.commons.configuration2.Configuration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.StandardIndexProvider;
import org.janusgraph.diskstorage.opensearch.OpenSearchMajorVersion;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Validates Atlas JanusGraph initialization against OpenSearch 2.x via Testcontainers.
 */
public class OpenSearchRuntimeInitTest extends OpenSearchITBase {

    private static final String OPENSEARCH_INDEX_CLASS = "org.janusgraph.diskstorage.opensearch.AtlasOpenSearchIndex";

    private JanusGraph graph;

    @org.testng.annotations.BeforeClass(dependsOnMethods = "startOpenSearchContainer")
    public void setUp() throws Exception {
        if (Runtime.version().feature() >= 25) {
            throw new org.testng.SkipException(
                    "AtlasJanusGraphDatabase static init is not supported on JDK 25+ in this test");
        }

        String opensearchHost = org.apache.atlas.runner.OpenSearchTestContainerRunner.getHost();
        int opensearchPort = org.apache.atlas.runner.OpenSearchTestContainerRunner.getPort();

        File dataDir = new File(System.getProperty("java.io.tmpdir"), "atlas-opensearch-init-test");
        deleteRecursively(dataDir.toPath());
        System.setProperty("atlas.data", dataDir.getAbsolutePath());
        System.setProperty("atlas.properties", "atlas-opensearch-application.properties");

        ApplicationProperties.forceReload();
        GraphSandboxUtil.create("opensearch-runtime-init");
        AtlasJanusGraphDatabase.unload();

        Configuration config = ApplicationProperties.get();
        config.setProperty("atlas.graph.index.search.hostname", opensearchHost);
        config.setProperty("atlas.graph.index.search.port", opensearchPort);
        config.setProperty(ApplicationProperties.INDEX_RECOVERY_CONF, false);

        Class.forName(AtlasJanusGraphDatabase.class.getName());
    }

    @AfterClass
    public void tearDown() {
        if (graph != null && graph.isOpen()) {
            graph.close();
        }
        AtlasJanusGraphDatabase.unload();
        ApplicationProperties.forceReload();
    }

    @Test
    public void standardIndexProviderRegistersOpenSearchBeforeGraphOpen() {
        Map<String, String> providers = StandardIndexProvider.getAllProviderClasses();

        assertEquals(providers.get("opensearch"), OPENSEARCH_INDEX_CLASS,
                "StandardIndexProvider must map opensearch backend to AtlasOpenSearchIndex");
        assertEquals(providers.get("elasticsearch"),
                "org.janusgraph.diskstorage.es.ElasticSearch7Index",
                "Existing elasticsearch backend registration must remain unchanged");
    }

    @Test(dependsOnMethods = "standardIndexProviderRegistersOpenSearchBeforeGraphOpen")
    public void janusGraphOpensWithOpenSearchBackend() throws Exception {
        Configuration atlasJanusConfig = AtlasJanusGraphDatabase.getConfiguration();

        assertEquals(atlasJanusConfig.getString(AtlasJanusGraphDatabase.INDEX_BACKEND_CONF), "opensearch");

        String host = org.apache.atlas.runner.OpenSearchTestContainerRunner.getHost();
        int port = org.apache.atlas.runner.OpenSearchTestContainerRunner.getPort();
        Configuration janusConfig = OpenSearchAtlasJanusTestSupport.buildJanusGraphConfiguration(
                atlasJanusConfig, host, port, null);

        graph = AtlasJanusGraphDatabase.initJanusGraph(janusConfig);

        assertNotNull(graph);
        assertTrue(graph.isOpen());
        OpenSearchMajorVersion majorVersion = readOpenSearchMajorVersion();
        assertTrue(majorVersion == OpenSearchMajorVersion.TWO || majorVersion == OpenSearchMajorVersion.THREE,
                "Expected OpenSearch 2.x or 3.x, got " + majorVersion);
    }

    @Test(dependsOnMethods = "standardIndexProviderRegistersOpenSearchBeforeGraphOpen")
    public void janusGraphFactoryOpenResolvesOpenSearchIndexDirectly() throws Exception {
        String host = org.apache.atlas.runner.OpenSearchTestContainerRunner.getHost();
        int port = org.apache.atlas.runner.OpenSearchTestContainerRunner.getPort();
        Configuration conf2 = OpenSearchAtlasJanusTestSupport.buildJanusGraphConfiguration(
                AtlasJanusGraphDatabase.getConfiguration(), host, port, null);

        try (JanusGraph directGraph = JanusGraphFactory.open(conf2)) {
            assertNotNull(directGraph);
            assertTrue(directGraph.isOpen());
        }
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

    private static OpenSearchMajorVersion readOpenSearchMajorVersion() throws IOException {
        String host = org.apache.atlas.runner.OpenSearchTestContainerRunner.getHost();
        int port = org.apache.atlas.runner.OpenSearchTestContainerRunner.getPort();
        URL url = new URL("http://" + host + ":" + port + "/");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setConnectTimeout(2000);
        connection.setReadTimeout(2000);
        connection.setRequestMethod("GET");
        try (java.io.InputStream in = connection.getInputStream()) {
            String body = new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            int numberStart = body.indexOf("\"number\" : \"") + 12;
            int numberEnd = body.indexOf('"', numberStart);
            return OpenSearchMajorVersion.parse(body.substring(numberStart, numberEnd));
        }
    }
}
