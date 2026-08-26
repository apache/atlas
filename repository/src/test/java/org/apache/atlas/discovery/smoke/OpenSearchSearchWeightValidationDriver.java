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
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.opensearch.OpenSearchMajorVersion;

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
 * C5.5.4 validation: weighted OpenSearch quick-search through production path
 * (SolrIndexHelper weight map → applySearchWeight → FreeTextSearchProcessor → quickSearch).
 *
 * <pre>
 *   cd repository && mvn test-compile exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=org.apache.atlas.discovery.smoke.OpenSearchSearchWeightValidationDriver \
 *     -Drat.skip=true -Dcheckstyle.skip=true -Dsortpom.skip=true -DskipCheck=true
 * </pre>
 */
public final class OpenSearchSearchWeightValidationDriver {

    private static final String TYPE_ASSET   = "c554_asset";
    private static final String TYPE_DATASET = "c554_dataset";

    private static final int WEIGHT_NAME  = 10;
    private static final int WEIGHT_OWNER = 3;

    private static final String LABEL_A = "A";
    private static final String LABEL_B = "B";
    private static final String LABEL_C = "C";
    private static final String LABEL_D = "D";

    private static final Map<String, String> RESULTS = new LinkedHashMap<>();
    private static Map<String, String>       guidByLabel = Collections.emptyMap();

    private OpenSearchSearchWeightValidationDriver() {
    }

    public static boolean execute() throws Exception {
        RESULTS.clear();

        OpenSearchQuickSearchSmokeSupport.bootstrapApplicationProperties();
        OpenSearchQuickSearchSmokeSupport.verifyOpenSearchReachable();

        if (!AtlasRepositoryConfiguration.isFreeTextSearchEnabled()) {
            throw new IllegalStateException("atlas.search.freetext.enable must be true");
        }

        OpenSearchQuickSearchSmokeSupport.registerAtlasOpenSearchIndex();
        OpenSearchQuickSearchSmokeSupport.deletePhysicalIndexIfPresent();

        JanusGraph janusGraph = JanusGraphFactory.open(OpenSearchQuickSearchSmokeSupport.buildJanusGraphConfiguration());
        AtlasTypeRegistry typeRegistry = buildTypeRegistry();
        IndexFieldNames indexFields = createSchema(janusGraph, typeRegistry);
        wireTypeRegistry(typeRegistry, indexFields);

        Map<String, String> entityGuids = insertBaselineEntities(janusGraph, typeRegistry);
        guidByLabel = entityGuids;
        AtlasGraph graph = new AtlasJanusGraph(janusGraph);
        graph.commit();
        Thread.sleep(1500);

        applySearchWeights(graph, indexFields);

        runValidations(graph, typeRegistry, indexFields, guidByLabel);

        graph.shutdown();

        return RESULTS.values().stream().allMatch("PASS"::equals);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("C5.5.4 Search-Weight Validation (production path)");
        boolean allPassed = execute();
        for (Map.Entry<String, String> entry : RESULTS.entrySet()) {
            System.out.printf("%-32s %s%n", entry.getKey(), entry.getValue());
        }
        System.out.println(allPassed ? "C5.5.4 RESULT: PASS" : "C5.5.4 RESULT: FAIL");
        if (!allPassed) {
            System.exit(1);
        }
    }

    private static void runValidations(AtlasGraph graph, AtlasTypeRegistry typeRegistry, IndexFieldNames indexFields,
                                       Map<String, String> guidByLabel) throws Exception {
        record("Weighted ranking name^10 owner^3", () -> {
            List<String> order = searchOrder(graph, typeRegistry, indexFields, "customer", TYPE_DATASET, 20, 0);
            assertTrue(order.size() >= 3, "expected at least 3 hits, got " + order);
            assertTrue(order.indexOf(LABEL_A) < order.indexOf(LABEL_B),
                    "name match (A) should rank above owner match (B): " + order);
            assertTrue(order.indexOf(LABEL_A) < order.indexOf(LABEL_C),
                    "A should rank above C: " + order);
        });

        record("Inverted weights change ranking", () -> {
            List<String> baseline = searchOrder(graph, typeRegistry, indexFields, "customer", TYPE_DATASET, 20, 0);
            applyInvertedWeights(graph, indexFields);
            List<String> inverted = searchOrder(graph, typeRegistry, indexFields, "customer", TYPE_DATASET, 20, 0);
            applySearchWeights(graph, indexFields);
            assertTrue(!baseline.equals(inverted), "inverting weights should change order: " + baseline + " vs " + inverted);
            assertTrue(inverted.indexOf(LABEL_B) < inverted.indexOf(LABEL_A),
                    "owner^10 should put B above A: " + inverted);
        });

        record("Type filter", () -> {
            List<String> order = searchOrder(graph, typeRegistry, indexFields, "customer", TYPE_DATASET, 10, 0);
            assertTrue(order.contains(LABEL_A), "type-filtered search should include A");
            assertFalse(order.contains("unknown"), "unexpected label");
        });

        record("Property filter owner=customer", () -> {
            FilterCriteria filter = new FilterCriteria();
            filter.setAttributeName("owner");
            filter.setOperator(Operator.EQ);
            filter.setAttributeValue("customer");
            List<String> order = searchWithFilter(graph, typeRegistry, indexFields, "customer", TYPE_DATASET, filter, 10, 0);
            assertTrue(order.size() == 1 && LABEL_B.equals(order.get(0)),
                    "owner=customer should return B only, got " + order);
        });

        record("Pagination + count", () -> {
            QuickSearchOutcome page1 = quickSearch(graph, typeRegistry, indexFields,
                    params("customer", TYPE_DATASET, 2, 0, null));
            QuickSearchOutcome page2 = quickSearch(graph, typeRegistry, indexFields,
                    params("customer", TYPE_DATASET, 2, 2, null));
            assertTrue(page1.approximateCount >= 3, "total count=" + page1.approximateCount);
            assertTrue(page1.resultSize <= 2, "page1 size=" + page1.resultSize);
            assertTrue(page2.resultSize >= 1, "page2 size=" + page2.resultSize);
        });

        record("Empty weight map fallback", () -> {
            graph.getGraphIndexClient().applySearchWeight(Constants.VERTEX_INDEX, Collections.emptyMap());
            List<String> order = searchOrder(graph, typeRegistry, indexFields, "customer", TYPE_DATASET, 20, 0);
            applySearchWeights(graph, indexFields);
            assertTrue(order.size() >= 3, "fallback should still return hits, got " + order);
        });

        record("Aggregation independence", () -> {
            AtlasGraphIndexClient client = graph.getGraphIndexClient();
            Map<String, String> cache = new HashMap<>();
            cache.put(Constants.ENTITY_TYPE_PROPERTY_KEY, indexFields.typeIndexField);
            cache.put(Constants.STATE_PROPERTY_KEY, indexFields.stateIndexField);
            AggregationContext ctx = new AggregationContext("customer", null,
                    Collections.singleton(typeRegistry.getEntityTypeByName(TYPE_DATASET)),
                    Collections.emptySet(), Collections.singleton(Constants.ENTITY_TYPE_PROPERTY_KEY),
                    Collections.emptySet(), cache, true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = client.getAggregatedMetrics(ctx);
            long typeCount = metrics.getOrDefault(Constants.ENTITY_TYPE_PROPERTY_KEY, Collections.emptyList())
                    .stream().mapToLong(AtlasAggregationEntry::getCount).sum();
            assertTrue(typeCount >= 4, "aggregation count=" + typeCount);
        });

        record("Uses FreeTextSearchProcessor", () -> {
            SearchContext ctx = buildSearchContext(graph, typeRegistry, indexFields,
                    params("customer", TYPE_DATASET, 10, 0, null));
            assertTrue(ctx.getSearchProcessor() instanceof FreeTextSearchProcessor,
                    "processor=" + ctx.getSearchProcessor().getClass().getSimpleName());
        });
    }

    private static List<String> searchOrder(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                            IndexFieldNames indexFields, String query, String typeName,
                                            int limit, int offset) throws AtlasBaseException {
        QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields, params(query, typeName, limit, offset, null));
        List<String> labels = new ArrayList<>();
        for (AtlasVertex v : out.vertices) {
            String guid = v.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
            labels.add(labelForGuid(guid));
        }
        return labels;
    }

    private static List<String> searchWithFilter(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                                 IndexFieldNames indexFields, String query, String typeName,
                                                 FilterCriteria filter, int limit, int offset) throws AtlasBaseException {
        QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                params(query, typeName, limit, offset, filter));
        List<String> labels = new ArrayList<>();
        for (AtlasVertex v : out.vertices) {
            String guid = v.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
            labels.add(labelForGuid(guid));
        }
        return labels;
    }

    private static QuickSearchOutcome quickSearch(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                                  IndexFieldNames indexFields,
                                                  QuickSearchParameters quickSearchParameters) throws AtlasBaseException {
        String query = quickSearchParameters.getQuery();
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(query)
                && !org.apache.atlas.type.AtlasStructType.AtlasAttribute.hastokenizeChar(query)) {
            query = query + "*";
        }
        quickSearchParameters.setQuery(query);

        SearchParameters searchParameters = EntityDiscoveryService.createSearchParameters(quickSearchParameters);
        SearchContext searchContext = buildSearchContext(graph, typeRegistry, indexFields, quickSearchParameters);

        SearchProcessor processor = searchContext.getSearchProcessor();
        List<AtlasVertex> vertices = processor.execute();
        long count = processor.getResultCount();

        QuickSearchOutcome outcome = new QuickSearchOutcome();
        outcome.approximateCount = count;
        outcome.resultSize       = vertices.size();
        outcome.vertices         = vertices;
        return outcome;
    }

    private static SearchContext buildSearchContext(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                                    IndexFieldNames indexFields,
                                                    QuickSearchParameters quickSearchParameters) throws AtlasBaseException {
        SearchParameters searchParameters = EntityDiscoveryService.createSearchParameters(quickSearchParameters);
        return new SearchContext(searchParameters, typeRegistry, graph, buildIndexedKeys(typeRegistry, indexFields));
    }

    private static QuickSearchParameters params(String query, String typeName, int limit, int offset,
                                                FilterCriteria entityFilter) {
        QuickSearchParameters p = new QuickSearchParameters();
        p.setQuery(query);
        p.setTypeName(typeName);
        p.setLimit(limit);
        p.setOffset(offset);
        p.setExcludeDeletedEntities(true);
        p.setIncludeSubTypes(true);
        p.setEntityFilters(entityFilter);
        return p;
    }

    private static void applySearchWeights(AtlasGraph graph, IndexFieldNames indexFields) throws org.apache.atlas.AtlasException {
        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put(indexFields.nameIndexField, WEIGHT_NAME);
        weights.put(indexFields.ownerIndexField, WEIGHT_OWNER);
        weights.put(indexFields.labelsIndexField, 10);
        graph.getGraphIndexClient().applySearchWeight(Constants.VERTEX_INDEX, weights);
    }

    private static void applyInvertedWeights(AtlasGraph graph, IndexFieldNames indexFields) throws org.apache.atlas.AtlasException {
        Map<String, Integer> weights = new LinkedHashMap<>();
        weights.put(indexFields.nameIndexField, WEIGHT_OWNER);
        weights.put(indexFields.ownerIndexField, WEIGHT_NAME);
        weights.put(indexFields.labelsIndexField, 10);
        graph.getGraphIndexClient().applySearchWeight(Constants.VERTEX_INDEX, weights);
    }

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
        Vertex v = graph.addVertex();
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
        PropertyKey guidKey   = mgmt.makePropertyKey(Constants.GUID_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey typeKey   = mgmt.makePropertyKey(Constants.ENTITY_TYPE_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey stateKey  = mgmt.makePropertyKey(Constants.STATE_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey nameKey   = mgmt.makePropertyKey(nameProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey ownerKey  = mgmt.makePropertyKey(ownerProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey labelsKey = mgmt.makePropertyKey(labelsProperty).dataType(String.class)
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
        fields.guidIndexField   = guidKey.name();
        fields.typeIndexField   = typeKey.name();
        fields.stateIndexField  = stateKey.name();
        fields.nameIndexField   = nameKey.name();
        fields.ownerIndexField  = ownerKey.name();
        fields.labelsIndexField = labelsKey.name();
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

    private static String labelForGuid(String guid) {
        for (Map.Entry<String, String> e : guidByLabel.entrySet()) {
            if (e.getValue().equals(guid)) {
                return e.getKey();
            }
        }
        return guid != null && guid.length() > 8 ? guid.substring(0, 8) + "…" : guid;
    }

    private static void record(String name, ValidationRunnable runnable) {
        try {
            runnable.run();
            RESULTS.put(name, "PASS");
        } catch (AssertionError | Exception e) {
            RESULTS.put(name, "FAIL: " + e.getMessage());
        }
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void assertFalse(boolean condition, String message) {
        if (condition) {
            throw new AssertionError(message);
        }
    }

    @FunctionalInterface
    private interface ValidationRunnable {
        void run() throws Exception;
    }

    private static final class QuickSearchOutcome {
        long approximateCount;
        int  resultSize;
        List<AtlasVertex> vertices;
    }

    private static final class IndexFieldNames {
        String guidIndexField;
        String typeIndexField;
        String stateIndexField;
        String nameIndexField;
        String ownerIndexField;
        String labelsIndexField;
    }
}
