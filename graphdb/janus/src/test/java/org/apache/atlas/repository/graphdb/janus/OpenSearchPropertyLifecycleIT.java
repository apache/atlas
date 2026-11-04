/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.runner.OpenSearchITBase;
import org.apache.atlas.runner.OpenSearchTestContainerRunner;
import org.apache.commons.configuration2.Configuration;
import org.janusgraph.core.JanusGraph;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Real JanusGraph + real OpenSearch lifecycle of a typedef attribute's vertex property, mirroring what Atlas does when an
 * attribute is deleted from a type and added back:
 * {@link AtlasJanusGraphManagement#deletePropertyKey(String)} only <em>renames</em> the JanusGraph key (the retired key
 * keeps its mixed-index registration and, with {@code index.search.map-name=true}, its mapped field name), and the
 * recreated key registers under the same logical name. Without a unique persisted {@code MAPPED_NAME} the second
 * registration fails with "Duplicate index field names" (or silently shares the retired key's mapping).
 *
 * <p>Every assertion is made against the real {@code _mapping} / {@code _search} responses, never mocks.
 */
public class OpenSearchPropertyLifecycleIT extends OpenSearchITBase {
    private static final String GRAPH_INDEX_NAME = "lifecycle";
    private static final String PHYSICAL_INDEX   = GRAPH_INDEX_NAME + "_vertex_index";
    private static final String PROPERTY         = "lifecycle_type.attr";

    private JanusGraph      janusGraph;
    private AtlasJanusGraph atlasGraph;
    private Configuration   janusConfig;
    private String          host;
    private int             port;

    @BeforeClass(dependsOnMethods = "startOpenSearchContainer")
    public void setUp() throws Exception {
        if (Runtime.version().feature() >= 25) {
            throw new org.testng.SkipException("AtlasJanusGraphDatabase static init is not supported on JDK 25+ in this test");
        }

        host = OpenSearchTestContainerRunner.getHost();
        port = OpenSearchTestContainerRunner.getPort();

        File dataDir = new File(System.getProperty("java.io.tmpdir"), "atlas-opensearch-lifecycle-test");

        deleteRecursively(dataDir.toPath());
        System.setProperty("atlas.data", dataDir.getAbsolutePath());
        System.setProperty("atlas.properties", "atlas-opensearch-application.properties");

        ApplicationProperties.forceReload();
        GraphSandboxUtil.create("opensearch-lifecycle");
        AtlasJanusGraphDatabase.unload();

        Configuration config = ApplicationProperties.get();

        config.setProperty("atlas.graph.index.search.hostname", host);
        config.setProperty("atlas.graph.index.search.port", port);
        config.setProperty("atlas.graph.index.search.index-name", GRAPH_INDEX_NAME);
        config.setProperty(ApplicationProperties.INDEX_RECOVERY_CONF, false);

        deletePhysicalIndexIfPresent();

        Class.forName(AtlasJanusGraphDatabase.class.getName());

        janusConfig = OpenSearchAtlasJanusTestSupport.buildJanusGraphConfiguration(
                AtlasJanusGraphDatabase.getConfiguration(), host, port, GRAPH_INDEX_NAME);
        janusGraph  = AtlasJanusGraphDatabase.initJanusGraph(janusConfig);
        atlasGraph  = new AtlasJanusGraph(janusGraph);

        try (AtlasGraphManagement mgmt = atlasGraph.getManagementSystem()) {
            mgmt.createVertexMixedIndex(Constants.VERTEX_INDEX, Constants.BACKING_INDEX, java.util.Collections.emptyList());
            mgmt.setIsSuccess(true);
        }

        atlasGraph.commit();
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
    public void deleteAndRecreateAcrossDatatypesAndRestartYieldsDistinctWorkingFields() throws Exception {
        Set<String> fields = new HashSet<>();

        // generation 0: text attribute (text + .keyword subfield, as for a searchWeight string attribute)
        String f0 = register(String.class, false, true);

        fields.add(f0);
        // JanusGraph hands out the physical (bullet-converted) name
        assertEquals(f0, PROPERTY.replace('.', '\u2022'), "the first registration keeps the legacy name-derived field name");
        index("alpha-doc", "alpha text value");

        // generation 1: attribute deleted, then recreated with the SAME datatype (the original duplicate-field scenario)
        delete();

        String f1 = register(String.class, false, true);

        assertTrue(fields.add(f1), "recreated attribute must not reuse retired field " + f0 + ", got " + f1);
        index("beta-doc", "beta text value");

        // generation 2: datatype change String -> Long
        delete();

        String f2 = register(Long.class, false, false);

        assertTrue(fields.add(f2), "datatype recreation must get a new field, got " + f2 + " in " + fields);
        indexNumber(42L);

        // generation 3: Long -> STRING-mapped (exact-match) attribute
        delete();

        String f3 = register(String.class, true, false);

        assertTrue(fields.add(f3), "STRING recreation must get a new field, got " + f3 + " in " + fields);
        index("GAMMA-Doc", "Exact Match Value");

        String mapping = httpGet("/" + PHYSICAL_INDEX + "/_mapping");

        for (String f : fields) {
            assertTrue(mapping.contains("\"" + f + "\""), "OpenSearch _mapping must contain " + f + ": " + mapping);
        }

        assertEquals(mappingTypeOf(mapping, f0), "text");
        assertEquals(mappingTypeOf(mapping, f1), "text");
        assertEquals(mappingTypeOf(mapping, f2), "long");
        assertEquals(mappingTypeOf(mapping, f3), "keyword");

        // each generation's documents are findable under that generation's own field only
        refresh();
        assertEquals(hits(f0, "alpha"), 1, "legacy doc is still searchable under its original field");
        assertEquals(hits(f1, "beta"), 1);
        assertEquals(hits(f1, "alpha"), 0, "recreated field must not see the retired key's documents");
        assertEquals(hitsTerm(f2, 42L), 1);
        assertEquals(hitsExact(f3, "Exact Match Value"), 1);
        assertEquals(hitsExact(f3, "exact match value"), 0, "STRING mapping is case-sensitive exact match");

        // restart: the persisted field name is authoritative and later generations keep incrementing
        janusGraph.close();
        janusGraph = AtlasJanusGraphDatabase.initJanusGraph(janusConfig);
        atlasGraph = new AtlasJanusGraph(janusGraph);

        try (AtlasGraphManagement mgmt = atlasGraph.getManagementSystem()) {
            AtlasPropertyKey key = mgmt.getPropertyKey(PROPERTY);

            assertEquals(mgmt.getIndexFieldName(Constants.VERTEX_INDEX, key, true, false), f3,
                    "after restart the persisted mapped name must be returned, not the name-derived default");
        }

        delete();

        String f4 = register(String.class, false, true);

        assertTrue(fields.add(f4), "post-restart recreation must still get a new field, got " + f4 + " in " + fields);

        String mappingAfter = httpGet("/" + PHYSICAL_INDEX + "/_mapping");

        for (String f : fields) {
            assertTrue(mappingAfter.contains("\"" + f + "\""), "OpenSearch _mapping must still contain " + f);
        }
    }

    private String register(Class<?> type, boolean isStringField, boolean withKeywordSubfield) throws Exception {
        String ret;

        try (AtlasGraphManagement mgmt = atlasGraph.getManagementSystem()) {
            AtlasPropertyKey key = mgmt.makePropertyKey(PROPERTY, type, AtlasCardinality.SINGLE);

            ret = mgmt.addMixedIndex(Constants.VERTEX_INDEX, key, isStringField, withKeywordSubfield);

            mgmt.updateSchemaStatus();
            mgmt.setIsSuccess(true);
        }

        atlasGraph.commit();
        Thread.sleep(1000);

        return ret;
    }

    private void delete() throws Exception {
        try (AtlasGraphManagement mgmt = atlasGraph.getManagementSystem()) {
            mgmt.deletePropertyKey(PROPERTY);
            mgmt.setIsSuccess(true);
        }

        atlasGraph.commit();
    }

    private void index(String vertexName, String value) throws Exception {
        AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> v = atlasGraph.addVertex();

        v.setProperty(PROPERTY, value);
        v.setProperty("lifecycle_type.label", vertexName);
        atlasGraph.commit();
        Thread.sleep(500);
    }

    private void indexNumber(long value) throws Exception {
        AtlasVertex<AtlasJanusVertex, AtlasJanusEdge> v = atlasGraph.addVertex();

        v.setProperty(PROPERTY, value);
        atlasGraph.commit();
        Thread.sleep(500);
    }

    private int hits(String field, String text) throws IOException {
        return count("{\"query\":{\"match\":{\"" + field + "\":\"" + text + "\"}}}");
    }

    private int hitsTerm(String field, long value) throws IOException {
        return count("{\"query\":{\"term\":{\"" + field + "\":" + value + "}}}");
    }

    private int hitsExact(String field, String value) throws IOException {
        return count("{\"query\":{\"term\":{\"" + field + "\":\"" + value + "\"}}}");
    }

    private int count(String body) throws IOException {
        String response = httpPost("/" + PHYSICAL_INDEX + "/_count", body);
        Matcher m = Pattern.compile("\"count\"\\s*:\\s*(\\d+)").matcher(response);

        assertTrue(m.find(), "unexpected _count response: " + response);

        return Integer.parseInt(m.group(1));
    }

    private static String mappingTypeOf(String mapping, String field) {
        Matcher m = Pattern.compile("\"" + Pattern.quote(field) + "\"\\s*:\\s*\\{[^{}]*?\"type\"\\s*:\\s*\"(\\w+)\"").matcher(mapping);

        assertTrue(m.find(), "no type for " + field + " in " + mapping);

        return m.group(1);
    }

    private void refresh() throws IOException {
        httpPost("/" + PHYSICAL_INDEX + "/_refresh", "");
    }

    private void deletePhysicalIndexIfPresent() throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL("http://" + host + ":" + port + "/" + PHYSICAL_INDEX).openConnection();

        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("DELETE");
        connection.getResponseCode();
    }

    private String httpGet(String path) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL("http://" + host + ":" + port + path).openConnection();

        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("GET");

        try (InputStream in = connection.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private String httpPost(String path, String body) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) new URL("http://" + host + ":" + port + path).openConnection();

        connection.setConnectTimeout(5000);
        connection.setReadTimeout(10000);
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);

        try (OutputStream out = connection.getOutputStream()) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }

        try (InputStream in = connection.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }

        Files.walk(path).sorted(java.util.Comparator.reverseOrder()).forEach(p -> {
            try {
                Files.deleteIfExists(p);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
