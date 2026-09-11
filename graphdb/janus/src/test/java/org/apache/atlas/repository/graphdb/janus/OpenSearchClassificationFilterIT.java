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
import org.apache.atlas.repository.graphdb.QuickSearchContext;
import org.apache.atlas.repository.graphdb.QuickSearchResult;
import org.apache.atlas.runner.OpenSearchITBase;
import org.apache.commons.configuration2.Configuration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphVertex;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Live end-to-end coverage that classification filtering actually restricts results against a real
 * OpenSearch server. Indexes vertices with a direct classification, a propagated-only classification, a different
 * (non-matching) classification, and no classification at all, then drives the exact production entry point —
 * {@link AtlasOpenSearchIndexClient#quickSearch(QuickSearchContext, Configuration)}, which builds the query via
 * {@link AtlasOpenSearchQueryBuilder#withClassificationTypeNames} — and asserts only the direct/propagated matches
 * come back. This complements (does not replace) the DSL-shape unit coverage in
 * {@code AtlasOpenSearchQueryBuilderTest#classificationFilterUsesShouldOverBothClassificationFieldsWithKeywordSubfield}.
 */
public class OpenSearchClassificationFilterIT extends OpenSearchITBase {
    private static final String GRAPH_INDEX_NAME = "classificationfilter";
    private static final String PHYSICAL_INDEX   = GRAPH_INDEX_NAME + "_vertex_index";

    private JanusGraph    janusGraph;
    private Configuration atlasConfig;

    @BeforeClass(dependsOnMethods = "startOpenSearchContainer")
    public void setUp() throws Exception {
        if (Runtime.version().feature() >= 25) {
            throw new org.testng.SkipException(
                    "AtlasJanusGraphDatabase static init is not supported on JDK 25+ in this test");
        }

        String opensearchHost = org.apache.atlas.runner.OpenSearchTestContainerRunner.getHost();
        int    opensearchPort = org.apache.atlas.runner.OpenSearchTestContainerRunner.getPort();

        File dataDir = new File(System.getProperty("java.io.tmpdir"), "atlas-opensearch-classification-filter-test");
        deleteRecursively(dataDir.toPath());
        System.setProperty("atlas.data", dataDir.getAbsolutePath());
        System.setProperty("atlas.properties", "atlas-opensearch-application.properties");

        ApplicationProperties.forceReload();
        GraphSandboxUtil.create("opensearch-classification-filter");
        AtlasJanusGraphDatabase.unload();

        atlasConfig = ApplicationProperties.get();
        atlasConfig.setProperty("atlas.graph.index.search.hostname", opensearchHost);
        atlasConfig.setProperty("atlas.graph.index.search.port", opensearchPort);
        atlasConfig.setProperty("atlas.graph.index.search.index-name", GRAPH_INDEX_NAME);
        atlasConfig.setProperty(ApplicationProperties.INDEX_RECOVERY_CONF, false);

        deletePhysicalIndexIfPresent(opensearchHost, opensearchPort);

        Class.forName(AtlasJanusGraphDatabase.class.getName());
        Configuration atlasJanusConfig = AtlasJanusGraphDatabase.getConfiguration();
        Configuration janusConfig = OpenSearchAtlasJanusTestSupport.buildJanusGraphConfiguration(
                atlasJanusConfig, opensearchHost, opensearchPort, GRAPH_INDEX_NAME);
        janusGraph = AtlasJanusGraphDatabase.initJanusGraph(janusConfig);

        AtlasJanusGraph atlasGraph = new AtlasJanusGraph(janusGraph);

        try (AtlasGraphManagement mgmt = atlasGraph.getManagementSystem()) {
            mgmt.createVertexMixedIndex(Constants.VERTEX_INDEX, Constants.BACKING_INDEX, Collections.emptyList());

            AtlasPropertyKey guidKey       = mgmt.makePropertyKey(Constants.GUID_PROPERTY_KEY, String.class, AtlasCardinality.SINGLE);
            AtlasPropertyKey classKey      = mgmt.makePropertyKey(Constants.CLASSIFICATION_NAMES_KEY, String.class, AtlasCardinality.SINGLE);
            AtlasPropertyKey propagatedKey = mgmt.makePropertyKey(Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY, String.class, AtlasCardinality.SINGLE);

            // guid: stored only (no search needed on it).
            mgmt.addMixedIndex(Constants.VERTEX_INDEX, guidKey, true, false);
            // classification fields: text + keyword subfield, mirroring the production GraphBackedSearchIndexer
            // path — this is what causes AtlasOpenSearchIndexClient.usesKeywordSubfield(...) to be true below.
            mgmt.addMixedIndex(Constants.VERTEX_INDEX, classKey, false, true);
            mgmt.addMixedIndex(Constants.VERTEX_INDEX, propagatedKey, false, true);

            mgmt.updateSchemaStatus();
            mgmt.setIsSuccess(true);
        }
        atlasGraph.commit();

        addVertex("guid-direct-pii", Constants.CLASSIFICATION_NAMES_KEY, "PII");
        addVertex("guid-propagated-pii", Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY, "PII");
        addVertex("guid-other-classification", Constants.CLASSIFICATION_NAMES_KEY, "CONFIDENTIAL");
        addVertex("guid-no-classification", null, null);
        janusGraph.tx().commit();

        Thread.sleep(1500);
    }

    private void addVertex(String guid, String classificationField, String classificationValue) {
        JanusGraphVertex vertex = janusGraph.addVertex();

        vertex.property(Constants.GUID_PROPERTY_KEY, guid);

        if (classificationField != null) {
            vertex.property(classificationField, classificationValue);
        }
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
    public void classificationFilterRestrictsResultsToDirectOrPropagatedMatches() throws Exception {
        assertTrue(AtlasOpenSearchIndexClient.usesKeywordSubfield(Constants.CLASSIFICATION_NAMES_KEY),
                "precondition: classification field must be keyword-registered like production");
        assertTrue(AtlasOpenSearchIndexClient.usesKeywordSubfield(Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY),
                "precondition: propagated classification field must be keyword-registered like production");

        Map<String, String> indexFieldNameCache = new HashMap<>();
        indexFieldNameCache.put(Constants.CLASSIFICATION_NAMES_KEY, Constants.CLASSIFICATION_NAMES_KEY);
        indexFieldNameCache.put(Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY, Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY);

        Set<String> classificationTypeNames = new HashSet<>();
        classificationTypeNames.add("PII");

        QuickSearchContext context = new QuickSearchContext(null, null, null, classificationTypeNames,
                indexFieldNameCache, false, false, 0, 50);

        QuickSearchResult result = AtlasOpenSearchIndexClient.quickSearch(context, atlasConfig);
        List<String>       guids = result.getEntityGuids();

        assertEquals(result.getTotalCount(), 2,
                "classification filter for PII must restrict to exactly the direct + propagated matches: " + guids);
        assertTrue(guids.contains("guid-direct-pii"), "expected direct PII match in " + guids);
        assertTrue(guids.contains("guid-propagated-pii"), "expected propagated PII match in " + guids);
        assertFalse(guids.contains("guid-other-classification"), "must not match a different classification: " + guids);
        assertFalse(guids.contains("guid-no-classification"), "must not match an unclassified vertex: " + guids);
    }

    @Test(dependsOnMethods = "classificationFilterRestrictsResultsToDirectOrPropagatedMatches")
    public void noClassificationFilterReturnsAllVertices() throws Exception {
        // Sanity check that the restriction above comes from the classification filter itself, not from some
        // unrelated cause (e.g. all documents being excluded) — with no classification filter, all 4 come back.
        QuickSearchContext context = new QuickSearchContext(null, null, null, Collections.emptySet(),
                new HashMap<>(), false, false, 0, 50);

        QuickSearchResult result = AtlasOpenSearchIndexClient.quickSearch(context, atlasConfig);

        assertEquals(result.getTotalCount(), 4,
                "expected all 4 seeded vertices when no classification filter is applied: " + result.getEntityGuids());
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
        URL            url        = new URL("http://" + host + ":" + port + "/" + PHYSICAL_INDEX);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);
        connection.setRequestMethod("DELETE");
        connection.getResponseCode();
    }
}
