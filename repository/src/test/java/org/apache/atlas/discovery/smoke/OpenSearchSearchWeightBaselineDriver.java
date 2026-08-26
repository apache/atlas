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
package org.apache.atlas.discovery.smoke;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.discovery.FreeTextSearchProcessor;
import org.apache.atlas.discovery.SearchContext;
import org.apache.atlas.discovery.SearchProcessor;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasAggregationEntry;
import org.apache.atlas.model.discovery.QuickSearchParameters;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AggregationContext;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.opensearch.OpenSearchMajorVersion;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * C5.5.1 read-only baseline: controlled ranking dataset + live payload capture.
 * Compares Atlas/JanusGraph quick-search ordering vs raw OpenSearch {@code query_string}
 * vs proposed {@code multi_match} field boosts. No production code changes.
 *
 * <pre>
 *   cd repository && mvn test-compile exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=org.apache.atlas.discovery.smoke.OpenSearchSearchWeightBaselineDriver \
 *     -Drat.skip=true -Dcheckstyle.skip=true -Dsortpom.skip=true -DskipCheck=true
 * </pre>
 */
public final class OpenSearchSearchWeightBaselineDriver {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String TYPE_ASSET   = "c55_asset";
    private static final String TYPE_DATASET = "c55_dataset";

    /** Conceptual Solr weights for documentation / multi_match probe. */
    private static final int WEIGHT_NAME  = 10;
    private static final int WEIGHT_OWNER = 3;

    private static final String LABEL_A = "A";
    private static final String LABEL_B = "B";
    private static final String LABEL_C = "C";
    private static final String LABEL_D = "D";

    private OpenSearchSearchWeightBaselineDriver() {
    }

    public static void main(String[] args) throws Exception {
        enableOpenSearchIndexDebugLogging();

        OpenSearchQuickSearchSmokeSupport.bootstrapApplicationProperties();
        OpenSearchQuickSearchSmokeSupport.verifyOpenSearchReachable();
        String osVersion = OpenSearchQuickSearchSmokeSupport.readOpenSearchVersion();
        OpenSearchMajorVersion major = OpenSearchQuickSearchSmokeSupport.readOpenSearchMajorVersion();

        if (!AtlasRepositoryConfiguration.isFreeTextSearchEnabled()) {
            throw new IllegalStateException("atlas.search.freetext.enable must be true");
        }

        OpenSearchQuickSearchSmokeSupport.registerAtlasOpenSearchIndex();
        OpenSearchQuickSearchSmokeSupport.deletePhysicalIndexIfPresent();

        JanusGraph janusGraph = JanusGraphFactory.open(OpenSearchQuickSearchSmokeSupport.buildJanusGraphConfiguration());
        AtlasTypeRegistry typeRegistry = buildTypeRegistry();
        IndexFieldNames indexFields = createSchema(janusGraph, typeRegistry);
        wireTypeRegistry(typeRegistry, indexFields);

        Map<String, String> guidByLabel = insertBaselineEntities(janusGraph, typeRegistry);
        AtlasGraph graph = new AtlasJanusGraph(janusGraph);
        graph.commit();
        Thread.sleep(1500);

        Map<String, String> osFieldNames = fetchOpenSearchFieldNames();

        System.out.println();
        System.out.println("C5.5.1 Search-Weight Ranking Baseline (read-only)");
        System.out.println("================================================");
        System.out.println("OpenSearch: " + osVersion + " (major=" + major + ")");
        System.out.println("Physical index: " + OpenSearchQuickSearchSmokeSupport.PHYSICAL_INDEX);
        System.out.println("JanusGraph index prefix: " + AtlasGraphUtilsV2.getIndexSearchPrefix());
        System.out.println("OpenSearch doc count: " + OpenSearchQuickSearchSmokeSupport.openSearchDocumentCount());
        System.out.println("Mapped name field:  " + osFieldNames.get("name"));
        System.out.println("Mapped owner field: " + osFieldNames.get("owner"));
        System.out.println();

        printSolrAvailability();
        printControlledDataset();

        String atlasQueryTerm = "customer";
        String janusQueryString = buildJanusGraphQueryString(atlasQueryTerm, typeRegistry);

        System.out.println("--- Atlas query string (JanusGraph RawQuery input) ---");
        System.out.println(janusQueryString);
        System.out.println();

        String queryStringPayload = buildQueryStringSearchPayload(janusQueryString, 20);
        String multiMatchPayload  = buildMultiMatchPayload(atlasQueryTerm, osFieldNames, 20);

        System.out.println("--- Live OpenSearch _search payload: query_string (JanusGraph equivalent) ---");
        System.out.println(prettyJson(queryStringPayload));
        System.out.println();

        System.out.println("--- Proposed _search payload: bool filter + multi_match field boosts ---");
        System.out.println(prettyJson(multiMatchPayload));
        System.out.println();

        List<RankedHit> atlasRanking = runAtlasQuickSearch(graph, typeRegistry, indexFields, atlasQueryTerm, guidByLabel);

        System.out.println("--- Atlas ordering (observation 1) ---");
        printHitList(atlasRanking);

        List<RankedHit> queryStringRanking = runRawSearch(queryStringPayload, guidByLabel, "query_string (observation 2)");
        List<RankedHit> multiMatchRanking  = runRawSearch(multiMatchPayload, guidByLabel, "multi_match name^10 owner^3 (observation 3)");

        printRankingMatrix(atlasRanking, queryStringRanking, multiMatchRanking);

        String invertedPayload = buildInvertedWeightPayload(atlasQueryTerm, osFieldNames, 20);
        List<RankedHit> invertedRanking = runRawSearch(invertedPayload, guidByLabel, "multi_match INVERTED name^3 owner^10 (observation 4)");
        System.out.println("--- Weight inversion (observation 4) ---");
        System.out.println("name^10/owner^3:  " + formatOrder(multiMatchRanking));
        System.out.println("name^3/owner^10:  " + formatOrder(invertedRanking));
        System.out.println("Ranking changed when weights inverted: " + !sameLabelOrder(multiMatchRanking, invertedRanking));
        System.out.println();

        runExtendedProbes(graph, typeRegistry, indexFields, guidByLabel, osFieldNames);
        runAggregationIndependenceProbe(graph, typeRegistry, indexFields);

        graph.shutdown();

        System.out.println();
        System.out.println("C5.5.1 BASELINE COMPLETE (read-only — no production changes)");
    }

    // -------------------------------------------------------------------------
    // Core ranking experiment
    // -------------------------------------------------------------------------

    private static void printControlledDataset() {
        System.out.println("--- Controlled dataset (conceptual weights: name^10, owner^3) ---");
        System.out.println("  A: name=customer       owner=alice   (name match, high weight)");
        System.out.println("  B: name=sales          owner=customer (owner match, low weight)");
        System.out.println("  C: name=customer sales owner=bob     (name token match, high weight)");
        System.out.println("  D: name=other          owner=other   labels=customer (labels^10 probe)");
        System.out.println();
        System.out.println("Expected Solr/edismax ordering (name^10 > owner^3): A, C, B (D via labels separate probe)");
        System.out.println();
    }

    private static void printSolrAvailability() {
        System.out.println("--- Solr baseline ---");
        try {
            java.net.URL url = new java.net.URL("http://localhost:8983/solr/");
            java.net.HttpURLConnection c = (java.net.HttpURLConnection) url.openConnection();
            c.setConnectTimeout(2000);
            c.setReadTimeout(2000);
            c.setRequestMethod("GET");
            if (c.getResponseCode() == 200) {
                System.out.println("Solr reachable at localhost:8983 — manual /freetext comparison not automated in this driver.");
            } else {
                System.out.println("Solr not available (HTTP " + c.getResponseCode() + ") — Solr ordering marked N/A.");
            }
        } catch (Exception e) {
            System.out.println("Solr not available — Solr ordering marked N/A.");
        }
        System.out.println();
    }

    private static String buildJanusGraphQueryString(String queryTerm, AtlasTypeRegistry typeRegistry)
            throws AtlasBaseException {
        String query = queryTerm;
        if (!AtlasAttribute.hastokenizeChar(query)) {
            query = query + "*";
        }

        AtlasEntityType datasetType = typeRegistry.getEntityTypeByName(TYPE_DATASET);
        String typeFilter = AtlasAttribute.escapeIndexQueryValue(
                Collections.singleton(datasetType.getTypeName()), true);

        String prefix = AtlasGraphUtilsV2.getIndexSearchPrefix();
        return query + SearchProcessor.AND_STR + typeFilter + SearchProcessor.AND_STR
                + "(" + prefix + "\"" + Constants.STATE_PROPERTY_KEY + "\":ACTIVE)";
    }

    private static List<RankedHit> runAtlasQuickSearch(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                                       IndexFieldNames indexFields, String queryTerm,
                                                       Map<String, String> guidByLabel) throws Exception {
        QuickSearchParameters qp = new QuickSearchParameters();
        qp.setQuery(queryTerm);
        qp.setTypeName(TYPE_DATASET);
        qp.setExcludeDeletedEntities(true);
        qp.setIncludeSubTypes(true);
        qp.setLimit(20);
        qp.setOffset(0);

        SearchParameters sp = EntityDiscoveryService.createSearchParameters(qp);
        String query = sp.getQuery();
        if (query != null && !AtlasAttribute.hastokenizeChar(query)) {
            query = query + "*";
        }
        sp.setQuery(query);

        SearchContext ctx = new SearchContext(sp, typeRegistry, graph, buildIndexedKeys(typeRegistry, indexFields));
        SearchProcessor processor = ctx.getSearchProcessor();

        System.out.println("--- Atlas quick-search path ---");
        System.out.println("Processor: " + processor.getClass().getSimpleName());
        if (!(processor instanceof FreeTextSearchProcessor)) {
            System.out.println("WARN: expected FreeTextSearchProcessor");
        }

        List<AtlasVertex> vertices = processor.execute();

        List<RankedHit> hits = new ArrayList<>();
        int rank = 1;
        for (AtlasVertex v : vertices) {
            String guid = v.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
            String label = labelForGuid(guid, guidByLabel);
            hits.add(new RankedHit(rank++, label, guid, Double.NaN, "Atlas/JanusGraph"));
        }
        return hits;
    }

    private static List<RankedHit> runRawSearch(String payload, Map<String, String> guidByLabel, String probeLabel) throws Exception {
        System.out.println("--- Raw OpenSearch: " + probeLabel + " ---");
        String response;
        try {
            response = OpenSearchQuickSearchSmokeSupport.httpPost(
                    "/" + OpenSearchQuickSearchSmokeSupport.PHYSICAL_INDEX + "/_search", payload);
        } catch (IOException e) {
            System.out.println("WARN: " + e.getMessage());
            System.out.println("(Atlas/JanusGraph query_string syntax is not always directly POSTable to OpenSearch)");
            return Collections.emptyList();
        }
        JsonNode root = MAPPER.readTree(response);
        JsonNode hitsNode = root.path("hits").path("hits");

        List<RankedHit> hits = new ArrayList<>();
        int rank = 1;
        for (JsonNode hit : hitsNode) {
            String id = hit.path("_id").asText();
            double score = hit.path("_score").asDouble();
            JsonNode source = hit.path("_source");
            String guid = source.isObject() && source.has("__guid") ? source.get("__guid").asText() : id;
            String entityLabel = labelForGuid(guid, guidByLabel);
            if (entityLabel == null) {
                entityLabel = guid.length() > 8 ? guid.substring(0, 8) + "…" : guid;
            }
            hits.add(new RankedHit(rank++, entityLabel, guid, score, null));
        }
        return hits;
    }

    private static void printHitList(List<RankedHit> hits) {
        if (hits.isEmpty()) {
            System.out.println("  (no hits)");
        } else {
            for (RankedHit h : hits) {
                System.out.printf("  %d. %s%n", h.rank, h.label);
            }
        }
        System.out.println();
    }

    private static String formatOrder(List<RankedHit> hits) {
        return hits.stream().map(h -> h.label).collect(Collectors.joining(" → "));
    }

    private static void printRankingMatrix(List<RankedHit> atlas, List<RankedHit> queryString, List<RankedHit> multiMatch) {
        System.out.println("--- Ranking comparison (query: customer*) ---");
        System.out.printf("%-28s %-22s %-22s%n", "Atlas/JanusGraph order", "OS query_string order", "OS multi_match order");
        int rows = Math.max(atlas.size(), Math.max(queryString.size(), multiMatch.size()));
        for (int i = 0; i < rows; i++) {
            System.out.printf("%-28s %-22s %-22s%n",
                    formatHit(atlas, i),
                    formatHit(queryString, i),
                    formatHit(multiMatch, i));
        }
        System.out.println();
        System.out.println("Solr /freetext ordering: N/A (Solr not in baseline environment)");
        System.out.println();

        boolean atlasMatchesMultiMatch = sameLabelOrder(atlas, multiMatch);
        boolean atlasMatchesQueryString = sameLabelOrder(atlas, queryString);
        boolean queryStringMatchesMultiMatch = sameLabelOrder(queryString, multiMatch);

        System.out.println("--- Baseline conclusions ---");
        System.out.println("Atlas order == query_string order: " + atlasMatchesQueryString
                + " (expected true — same underlying JanusGraph path)");
        System.out.println("Atlas order == multi_match order:  " + atlasMatchesMultiMatch
                + " (false implies weighted search path would change ranking)");
        System.out.println("query_string == multi_match:       " + queryStringMatchesMultiMatch);
        if (!atlasMatchesMultiMatch) {
            System.out.println("→ C5.5.2 should target ranking parity via Atlas-level multi_match (Option B).");
        }
        System.out.println();
    }

    // -------------------------------------------------------------------------
    // Extended probes (read-only)
    // -------------------------------------------------------------------------

    private static void runExtendedProbes(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                          IndexFieldNames indexFields, Map<String, String> guidByLabel,
                                          Map<String, String> osFieldNames) throws Exception {
        System.out.println("--- Extended probes ---");

        // labels^10 probe via raw multi_match
        String labelsPayload = buildMultiMatchPayloadWithExtraField("customer", osFieldNames,
                osFieldNames.get("labels"), 10, 10);
        List<RankedHit> labelsHits = runRawSearch(labelsPayload, guidByLabel, "labels^10 probe");
        System.out.println("labels^10 multi_match order: " + labelsHits.stream()
                .map(h -> h.label).collect(Collectors.joining(" > ")));

        // owner-only weight dominance: owner^10 name^3 inverted
        String invertedPayload = buildInvertedWeightPayload("customer", osFieldNames, 10);
        List<RankedHit> invertedHits = runRawSearch(invertedPayload, guidByLabel, "extended inverted weights");
        System.out.println("inverted owner^10/name^3 order: " + invertedHits.stream()
                .map(h -> h.label + "(" + String.format("%.2f", h.score) + ")")
                .collect(Collectors.joining(" > ")));

        // Type filter + quick search
        QuickSearchParameters qp = new QuickSearchParameters();
        qp.setQuery("customer");
        qp.setTypeName(TYPE_DATASET);
        qp.setExcludeDeletedEntities(true);
        qp.setIncludeSubTypes(true);
        qp.setLimit(10);
        SearchParameters sp = EntityDiscoveryService.createSearchParameters(qp);
        sp.setQuery(sp.getQuery() + "*");
        SearchContext ctx = new SearchContext(sp, typeRegistry, graph, buildIndexedKeys(typeRegistry, indexFields));
        int typeFilteredCount = ctx.getSearchProcessor().execute().size();
        System.out.println("Type-filtered Atlas hits (c55_dataset): " + typeFilteredCount);

        // Property filter
        FilterCriteria filter = new FilterCriteria();
        filter.setAttributeName("owner");
        filter.setOperator(Operator.EQ);
        filter.setAttributeValue("customer");
        qp.setEntityFilters(filter);
        sp = EntityDiscoveryService.createSearchParameters(qp);
        sp.setQuery("customer*");
        ctx = new SearchContext(sp, typeRegistry, graph, buildIndexedKeys(typeRegistry, indexFields));
        int propFilteredCount = ctx.getSearchProcessor().execute().size();
        System.out.println("Owner=customer filter + customer* query hits: " + propFilteredCount + " (expect B only)");

        // Pagination
        qp.setEntityFilters(null);
        qp.setLimit(2);
        qp.setOffset(0);
        sp = EntityDiscoveryService.createSearchParameters(qp);
        sp.setQuery("customer*");
        ctx = new SearchContext(sp, typeRegistry, graph, buildIndexedKeys(typeRegistry, indexFields));
        long total = ctx.getSearchProcessor().getResultCount();
        int page1 = ctx.getSearchProcessor().execute().size();
        qp.setOffset(2);
        sp = EntityDiscoveryService.createSearchParameters(qp);
        sp.setQuery("customer*");
        ctx = new SearchContext(sp, typeRegistry, graph, buildIndexedKeys(typeRegistry, indexFields));
        int page2 = ctx.getSearchProcessor().execute().size();
        System.out.println("Pagination: total=" + total + " page1=" + page1 + " page2=" + page2);

        System.out.println();
    }

    private static void runAggregationIndependenceProbe(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                                        IndexFieldNames indexFields) throws Exception {
        AtlasGraphIndexClient client = graph.getGraphIndexClient();
        Set<AtlasEntityType> types = Collections.singleton(typeRegistry.getEntityTypeByName(TYPE_DATASET));
        Map<String, String> cache = new HashMap<>();
        cache.put(Constants.ENTITY_TYPE_PROPERTY_KEY, indexFields.typeIndexField);
        cache.put(Constants.STATE_PROPERTY_KEY, indexFields.stateIndexField);

        AggregationContext ctx = new AggregationContext("customer", null, types, Collections.emptySet(),
                Collections.singleton(Constants.ENTITY_TYPE_PROPERTY_KEY), Collections.emptySet(),
                cache, true, true);

        Map<String, List<AtlasAggregationEntry>> metrics = client.getAggregatedMetrics(ctx);
        long typeCount = metrics.getOrDefault(Constants.ENTITY_TYPE_PROPERTY_KEY, Collections.emptyList())
                .stream().mapToLong(AtlasAggregationEntry::getCount).sum();

        System.out.println("--- Aggregation independence (C5.4 path, unaffected by search weights) ---");
        System.out.println("typeName agg total with query=customer: " + typeCount + " (expect 4 baseline entities)");
        System.out.println();
    }

    // -------------------------------------------------------------------------
    // Payload builders
    // -------------------------------------------------------------------------

    private static String buildQueryStringSearchPayload(String query, int size) throws Exception {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", Collections.singletonMap("query_string",
                Collections.singletonMap("query", query)));
        body.put("size", size);
        body.put("_source", Collections.singletonList("__guid"));
        return MAPPER.writeValueAsString(body);
    }

    private static String buildMultiMatchPayload(String queryTerm, Map<String, String> osFields, int size)
            throws Exception {
        List<String> fields = new ArrayList<>();
        fields.add(osFields.get("name") + "^" + WEIGHT_NAME);
        fields.add(osFields.get("owner") + "^" + WEIGHT_OWNER);

        Map<String, Object> multiMatch = new LinkedHashMap<>();
        multiMatch.put("query", queryTerm);
        multiMatch.put("fields", fields);
        multiMatch.put("type", "best_fields");

        Map<String, Object> bool = new LinkedHashMap<>();
        bool.put("must", Collections.singletonList(Collections.singletonMap("multi_match", multiMatch)));
        bool.put("filter", Collections.singletonList(
                Collections.singletonMap("term", Collections.singletonMap("__state", "ACTIVE"))));

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", Collections.singletonMap("bool", bool));
        body.put("size", size);
        body.put("_source", Collections.singletonList("__guid"));
        return MAPPER.writeValueAsString(body);
    }

    private static String buildMultiMatchPayloadWithExtraField(String queryTerm, Map<String, String> osFields,
                                                               String extraField, int extraWeight, int size)
            throws Exception {
        List<String> fields = new ArrayList<>();
        fields.add(osFields.get("name") + "^" + WEIGHT_NAME);
        fields.add(osFields.get("owner") + "^" + WEIGHT_OWNER);
        if (extraField != null) {
            fields.add(extraField + "^" + extraWeight);
        }
        Map<String, Object> multiMatch = new LinkedHashMap<>();
        multiMatch.put("query", queryTerm);
        multiMatch.put("fields", fields);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", Collections.singletonMap("multi_match", multiMatch));
        body.put("size", size);
        body.put("_source", Collections.singletonList("__guid"));
        return MAPPER.writeValueAsString(body);
    }

    private static String buildInvertedWeightPayload(String queryTerm, Map<String, String> osFields, int size)
            throws Exception {
        List<String> fields = new ArrayList<>();
        fields.add(osFields.get("name") + "^3");
        fields.add(osFields.get("owner") + "^10");
        Map<String, Object> multiMatch = new LinkedHashMap<>();
        multiMatch.put("query", queryTerm);
        multiMatch.put("fields", fields);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("query", Collections.singletonMap("multi_match", multiMatch));
        body.put("size", size);
        body.put("_source", Collections.singletonList("__guid"));
        return MAPPER.writeValueAsString(body);
    }

    // -------------------------------------------------------------------------
    // Fixture
    // -------------------------------------------------------------------------

    private static Map<String, String> insertBaselineEntities(JanusGraph graph, AtlasTypeRegistry typeRegistry)
            throws Exception {
        Map<String, String> guidByLabel = new LinkedHashMap<>();
        guidByLabel.put(LABEL_A, insertEntity(graph, typeRegistry, "customer", "alice", null));
        guidByLabel.put(LABEL_B, insertEntity(graph, typeRegistry, "sales", "customer", null));
        guidByLabel.put(LABEL_C, insertEntity(graph, typeRegistry, "customer sales", "bob", null));
        guidByLabel.put(LABEL_D, insertEntity(graph, typeRegistry, "other", "other", "customer"));
        return guidByLabel;
    }

    private static String insertEntity(JanusGraph graph, AtlasTypeRegistry typeRegistry,
                                       String name, String owner, String labels) throws Exception {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(TYPE_DATASET);
        String guid = UUID.randomUUID().toString();
        org.apache.tinkerpop.gremlin.structure.Vertex v = graph.addVertex();
        v.property(Constants.GUID_PROPERTY_KEY, guid);
        v.property(Constants.ENTITY_TYPE_PROPERTY_KEY, TYPE_DATASET);
        v.property(Constants.STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());
        v.property(entityType.getAttribute("name").getVertexPropertyName(), name);
        v.property(entityType.getAttribute("owner").getVertexPropertyName(), owner);
        if (labels != null) {
            v.property(entityType.getAttribute("labels").getVertexPropertyName(), labels);
        }
        return guid;
    }

    private static IndexFieldNames createSchema(JanusGraph graph, AtlasTypeRegistry typeRegistry) throws Exception {
        AtlasEntityType assetType = typeRegistry.getEntityTypeByName(TYPE_ASSET);
        String nameProperty   = assetType.getAttribute("name").getVertexPropertyName();
        String ownerProperty  = assetType.getAttribute("owner").getVertexPropertyName();
        String labelsProperty = assetType.getAttribute("labels").getVertexPropertyName();

        JanusGraphManagement mgmt = graph.openManagement();
        PropertyKey guidKey    = mgmt.makePropertyKey(Constants.GUID_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey typeKey    = mgmt.makePropertyKey(Constants.ENTITY_TYPE_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey stateKey   = mgmt.makePropertyKey(Constants.STATE_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey nameKey    = mgmt.makePropertyKey(nameProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey ownerKey   = mgmt.makePropertyKey(ownerProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey labelsKey  = mgmt.makePropertyKey(labelsProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();

        mgmt.buildIndex(OpenSearchQuickSearchSmokeSupport.VERTEX_INDEX, Vertex.class)
                .addKey(guidKey, Mapping.STRING.asParameter())
                .addKey(typeKey, Mapping.STRING.asParameter())
                .addKey(stateKey, Mapping.STRING.asParameter())
                .addKey(nameKey, Mapping.TEXT.asParameter())
                .addKey(ownerKey, Mapping.STRING.asParameter())
                .addKey(labelsKey, Mapping.STRING.asParameter())
                .buildMixedIndex(OpenSearchQuickSearchSmokeSupport.BACKING_INDEX_NAME);

        IndexFieldNames fields = new IndexFieldNames();
        fields.guidIndexField    = guidKey.name();
        fields.typeIndexField    = typeKey.name();
        fields.stateIndexField   = stateKey.name();
        fields.nameIndexField    = nameKey.name();
        fields.ownerIndexField   = ownerKey.name();
        fields.labelsIndexField  = labelsKey.name();
        mgmt.commit();
        graph.tx().commit();
        return fields;
    }

    private static AtlasTypeRegistry buildTypeRegistry() throws Exception {
        AtlasTypeRegistry registry = new AtlasTypeRegistry();
        AtlasEntityDef assetDef = new AtlasEntityDef();
        assetDef.setName(TYPE_ASSET);
        assetDef.setAttributeDefs(new ArrayList<>());
        assetDef.getAttributeDefs().add(attr("name", WEIGHT_NAME));
        assetDef.getAttributeDefs().add(attr("owner", WEIGHT_OWNER));
        assetDef.getAttributeDefs().add(attr("labels", 10));

        AtlasEntityDef datasetDef = new AtlasEntityDef();
        datasetDef.setName(TYPE_DATASET);
        datasetDef.setSuperTypes(Collections.singleton(TYPE_ASSET));

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(assetDef);
        typesDef.getEntityDefs().add(datasetDef);
        registry.updateTypes(typesDef);
        return registry;
    }

    private static AtlasAttributeDef attr(String name, int searchWeight) {
        AtlasAttributeDef a = new AtlasAttributeDef(name, "string");
        a.setIndexType(AtlasAttributeDef.IndexType.STRING);
        a.setSearchWeight(searchWeight);
        return a;
    }

    private static void wireTypeRegistry(AtlasTypeRegistry registry, IndexFieldNames fields) {
        wire(registry.getEntityTypeByName(TYPE_ASSET), fields);
        wire(registry.getEntityTypeByName(TYPE_DATASET), fields);
        registry.addIndexFieldName(Constants.ENTITY_TYPE_PROPERTY_KEY, fields.typeIndexField);
        registry.addIndexFieldName(Constants.STATE_PROPERTY_KEY, fields.stateIndexField);
    }

    private static void wire(AtlasEntityType type, IndexFieldNames fields) {
        type.getAttribute("name").setIndexFieldName(fields.nameIndexField);
        type.getAttribute("owner").setIndexFieldName(fields.ownerIndexField);
        type.getAttribute("labels").setIndexFieldName(fields.labelsIndexField);
    }

    private static Set<String> buildIndexedKeys(AtlasTypeRegistry registry, IndexFieldNames fields) {
        Set<String> keys = new LinkedHashSet<>();
        keys.add(fields.nameIndexField);
        keys.add(fields.ownerIndexField);
        keys.add(fields.labelsIndexField);
        keys.add(fields.typeIndexField);
        keys.add(fields.stateIndexField);
        return keys;
    }

    private static Map<String, String> fetchOpenSearchFieldNames() throws Exception {
        String mapping = OpenSearchQuickSearchSmokeSupport.httpGet(
                "/" + OpenSearchQuickSearchSmokeSupport.PHYSICAL_INDEX + "/_mapping");
        JsonNode props = MAPPER.readTree(mapping)
                .path(OpenSearchQuickSearchSmokeSupport.PHYSICAL_INDEX)
                .path("mappings").path("properties");
        Map<String, String> ret = new HashMap<>();
        List<String> names = new ArrayList<>();
        props.fieldNames().forEachRemaining(names::add);
        for (String key : names) {
            if (key.contains("name") && !key.contains("type") && !key.contains("owner")) {
                ret.put("name", key);
            } else if (key.contains("owner")) {
                ret.put("owner", key);
            } else if (key.contains("labels") || key.contains("label")) {
                ret.put("labels", key);
            }
        }
        return ret;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void enableOpenSearchIndexDebugLogging() {
        ch.qos.logback.classic.Logger logger = (ch.qos.logback.classic.Logger)
                LoggerFactory.getLogger("org.janusgraph.diskstorage.opensearch.OpenSearchIndex");
        logger.setLevel(Level.DEBUG);
    }

    private static String prettyJson(String json) {
        try {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(MAPPER.readTree(json));
        } catch (Exception e) {
            return json;
        }
    }

    private static String labelForGuid(String guid, Map<String, String> guidByLabel) {
        for (Map.Entry<String, String> e : guidByLabel.entrySet()) {
            if (e.getValue().equals(guid)) {
                return e.getKey();
            }
        }
        return null;
    }

    private static String formatHit(List<RankedHit> hits, int index) {
        if (index >= hits.size()) {
            return "-";
        }
        RankedHit h = hits.get(index);
        if (Double.isNaN(h.score)) {
            return h.label;
        }
        return h.label + " (" + String.format("%.2f", h.score) + ")";
    }

    private static boolean sameLabelOrder(List<RankedHit> a, List<RankedHit> b) {
        List<String> la = a.stream().map(h -> h.label).collect(Collectors.toList());
        List<String> lb = b.stream().map(h -> h.label).collect(Collectors.toList());
        return la.equals(lb);
    }

    private static final class RankedHit {
        final int    rank;
        final String label;
        final String id;
        final double score;
        final String source;

        RankedHit(int rank, String label, String id, double score, String source) {
            this.rank   = rank;
            this.label  = label;
            this.id     = id;
            this.score  = score;
            this.source = source;
        }
    }

    private static final class IndexFieldNames {
        private String guidIndexField;
        private String typeIndexField;
        private String stateIndexField;
        private String nameIndexField;
        private String ownerIndexField;
        private String labelsIndexField;
    }
}
