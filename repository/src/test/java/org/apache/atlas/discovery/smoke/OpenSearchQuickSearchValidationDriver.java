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
import org.apache.atlas.model.discovery.QuickSearchParameters;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.lang3.StringUtils;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Validates Atlas discovery quick-search through the existing stack
 * (EntityDiscoveryService query shaping → SearchContext → FreeTextSearchProcessor → graph.indexQuery)
 * against a real OpenSearch server. No new discovery logic; validation only. The server version is configurable
 * (system property {@code opensearch.docker.version} / {@code opensearch.docker.image}) — the dedicated
 * {@code opensearch-it} CI job validates OpenSearch 3.7 and 3.8. OpenSearch 2.x is NOT currently part of the CI
 * matrix; this driver has not been verified against 2.x in CI.
 *
 * <pre>
 *   mvn -pl repository -am test-compile exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=org.apache.atlas.discovery.smoke.OpenSearchQuickSearchValidationDriver \
 *     -Drat.skip=true -Dcheckstyle.skip=true -Dsortpom.skip=true
 * </pre>
 */
public final class OpenSearchQuickSearchValidationDriver {
    private static final String TYPE_DATASET = "c5_dataset";
    private static final String TYPE_TABLE   = "c5_table";
    private static final String TYPE_ASSET     = "c5_asset";

    private static final Map<String, String> RESULTS = new LinkedHashMap<>();

    private OpenSearchQuickSearchValidationDriver() {
    }

    public static boolean execute() throws Exception {
        RESULTS.clear();

        OpenSearchQuickSearchSmokeSupport.bootstrapApplicationProperties();
        OpenSearchQuickSearchSmokeSupport.verifyOpenSearchReachable();

        OpenSearchQuickSearchSmokeSupport.registerAtlasOpenSearchIndex();
        if (!AtlasRepositoryConfiguration.isFreeTextSearchEnabled()) {
            throw new IllegalStateException("atlas.search.freetext.enable must be true for quick search validation");
        }

        OpenSearchQuickSearchSmokeSupport.deletePhysicalIndexIfPresent();

        JanusGraph janusGraph = JanusGraphFactory.open(OpenSearchQuickSearchSmokeSupport.buildJanusGraphConfiguration());
        AtlasTypeRegistry typeRegistry = buildTypeRegistry(new IndexFieldNames());
        IndexFieldNames indexFields = createSchema(janusGraph, typeRegistry);
        wireIndexFieldNames(typeRegistry, indexFields);
        AtlasGraph graph = new AtlasJanusGraph(janusGraph);
        insertTestEntities(graph, typeRegistry);

        graph.commit();

        Thread.sleep(1500);

        runValidations(graph, typeRegistry, indexFields);

        graph.shutdown();

        return RESULTS.values().stream().allMatch("PASS"::equals);
    }

    /**
     * OpenSearch IT entry point: quick search with {@code __customAttributes} entity filters on the shared c5 fixture.
     */
    public static boolean executeCustomAttributesQuickSearch() throws Exception {
        RESULTS.clear();

        OpenSearchQuickSearchSmokeSupport.bootstrapApplicationProperties();
        OpenSearchQuickSearchSmokeSupport.verifyOpenSearchReachable();

        OpenSearchQuickSearchSmokeSupport.registerAtlasOpenSearchIndex();
        if (!AtlasRepositoryConfiguration.isFreeTextSearchEnabled()) {
            throw new IllegalStateException("atlas.search.freetext.enable must be true for quick search validation");
        }

        OpenSearchQuickSearchSmokeSupport.deletePhysicalIndexIfPresent();

        JanusGraph janusGraph = JanusGraphFactory.open(OpenSearchQuickSearchSmokeSupport.buildJanusGraphConfiguration());
        AtlasTypeRegistry typeRegistry = buildTypeRegistry(new IndexFieldNames());
        IndexFieldNames indexFields = createSchema(janusGraph, typeRegistry);
        wireIndexFieldNames(typeRegistry, indexFields);
        AtlasGraph graph = new AtlasJanusGraph(janusGraph);
        insertFilterDemoEntities(graph, typeRegistry);
        insertTestEntities(graph, typeRegistry);

        graph.commit();

        Thread.sleep(1500);

        runCustomAttributesValidations(graph, typeRegistry, indexFields);

        graph.shutdown();

        return RESULTS.values().stream().allMatch("PASS"::equals);
    }

    static void printResults() {
        for (Map.Entry<String, String> entry : RESULTS.entrySet()) {
            System.out.printf("  %-40s %s%n", entry.getKey(), entry.getValue());
        }
    }

    public static void main(String[] args) throws Exception {
        String osVersion = OpenSearchQuickSearchSmokeSupport.readOpenSearchVersion();
        OpenSearchMajorVersion major = OpenSearchQuickSearchSmokeSupport.readOpenSearchMajorVersion();

        System.out.println();
        System.out.println("Atlas Quick Search Validation");
        System.out.println("----------------------------------");
        System.out.println("Backend: OpenSearch");
        System.out.println("OpenSearch: " + osVersion + " (major=" + major + ")");
        System.out.println("JanusGraph index prefix: " + AtlasGraphUtilsV2.getIndexSearchPrefix());
        System.out.println("Physical index: " + OpenSearchQuickSearchSmokeSupport.PHYSICAL_INDEX);
        System.out.println();

        boolean allPassed = execute();

        System.out.println();
        for (Map.Entry<String, String> entry : RESULTS.entrySet()) {
            System.out.printf("%-24s %s%n", entry.getKey(), entry.getValue());
        }
        System.out.println();
        System.out.println(allPassed ? "RESULT: PASS" : "RESULT: FAIL");

        if (!allPassed) {
            System.exit(1);
        }
    }

    // -------------------------------------------------------------------------
    // Validations (Atlas discovery path)
    // -------------------------------------------------------------------------

    private static void runValidations(AtlasGraph graph, AtlasTypeRegistry typeRegistry, IndexFieldNames indexFields)
            throws Exception {
        record("Basic keyword", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields, params("atlas", TYPE_DATASET, 10, 0, null, null));
            assertTrue(out.resultSize >= 2, "expected at least 2 atlas matches, got " + out.resultSize);
            assertTrue(out.approximateCount >= 2, "count=" + out.approximateCount);
            debugTrace(out, "{\"query_string\":{\"query\":\"atlas*\",\"fields\":[\"*\"]}}");
        });

        record("Multiple keywords", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields, params("alice atlas", TYPE_DATASET, 10, 0, null, null));
            assertTrue(out.resultSize >= 1, "expected alice atlas match");
        });

        record("Type filter", () -> {
            QuickSearchOutcome dataset = quickSearch(graph, typeRegistry, indexFields, params("atlas", TYPE_DATASET, 10, 0, null, null));
            QuickSearchOutcome table   = quickSearch(graph, typeRegistry, indexFields, params("atlas", TYPE_TABLE, 10, 0, null, null));
            assertTrue(dataset.resultSize >= 2, "dataset hits=" + dataset.resultSize);
            assertTrue(table.resultSize >= 1, "table hits=" + table.resultSize);
            assertTrue(dataset.resultSize != table.resultSize || table.resultSize == 1,
                    "type filter should differentiate results");
        });

        record("Property filter", () -> {
            FilterCriteria filter = new FilterCriteria();
            filter.setAttributeName("owner");
            filter.setAttributeValue("team-alpha");
            filter.setOperator(Operator.EQ);
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("atlas", TYPE_DATASET, 10, 0, filter, null));
            assertTrue(out.resultSize >= 1, "owner filter expected hits, got " + out.resultSize);
        });

        record("Pagination", () -> {
            QuickSearchOutcome page1 = quickSearch(graph, typeRegistry, indexFields, params("report", null, 2, 0, null, null));
            QuickSearchOutcome page2 = quickSearch(graph, typeRegistry, indexFields, params("report", null, 2, 2, null, null));
            assertEquals(page1.resultSize, 2, "page1 size");
            assertTrue(page2.resultSize >= 1, "page2 size");
            assertDifferentGuids(page1, page2);
        });

        record("Result count", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields, params("atlas", null, 50, 0, null, null));
            assertTrue(out.approximateCount >= 3, "approximateCount=" + out.approximateCount);
            assertTrue(out.resultSize >= 3, "resultSize=" + out.resultSize);
        });

        record("No-result query", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("zzznomatch999", TYPE_DATASET, 10, 0, null, null));
            assertEquals(out.resultSize, 0, "resultSize");
            assertEquals(out.approximateCount, 0L, "approximateCount");
        });

        record("Case/partial search", () -> {
            QuickSearchOutcome lower = quickSearch(graph, typeRegistry, indexFields, params("alice", TYPE_DATASET, 10, 0, null, null));
            QuickSearchOutcome upper = quickSearch(graph, typeRegistry, indexFields, params("ALICE", TYPE_DATASET, 10, 0, null, null));
            assertTrue(lower.resultSize >= 1, "lowercase partial");
            assertTrue(upper.resultSize >= 1, "uppercase partial (Atlas adds trailing *)");
        });

        record("Multiple entity types", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields, params("report", null, 20, 0, null, null));
            assertTrue(out.resultSize >= 2, "hits across types=" + out.resultSize);
            assertTrue(out.typesSeen.contains(TYPE_DATASET) && out.typesSeen.contains(TYPE_TABLE),
                    "types seen: " + out.typesSeen);
        });

        record("Existing index", () -> {
            long count = OpenSearchQuickSearchSmokeSupport.openSearchDocumentCount();
            assertTrue(count >= 5, "index should already contain seeded docs, count=" + count);
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields, params("charlie", TYPE_DATASET, 5, 0, null, null));
            assertTrue(out.resultSize >= 1, "search on existing index");
        });

        record("Prefix wildcard query", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields, params("custo*", TYPE_DATASET, 10, 0, null, null));
            assertTrue(out.resultSize >= 1, "custo* should match Customer Atlas Data, got " + out.resultSize);
            assertTrue(out.approximateCount >= 1, "approximateCount=" + out.approximateCount);
        });
    }

    private static void runCustomAttributesValidations(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                                       IndexFieldNames indexFields) throws Exception {
        record("Free-text baseline filterdemo", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("filterdemo", TYPE_DATASET, 20, 0, null, null));
            assertTrue(out.resultSize == 4, "expected 4 filterdemo entities, got " + out.resultSize);
        });

        record("Custom attr filter only env=production", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("", TYPE_DATASET, 20, 0, customAttrFilter("env=production"), null));
            assertTrue(out.resultSize == 2, "expected 2 production entities, got " + out.resultSize);
        });

        record("Free-text + custom attr env=production", () -> {
            SearchContext ctx = new SearchContext(
                    EntityDiscoveryService.createSearchParameters(
                            params("filterdemo", TYPE_DATASET, 20, 0, customAttrFilter("env=production"), null)),
                    typeRegistry, graph, buildIndexedKeys(typeRegistry, indexFields));
            assertTrue(ctx.getSearchProcessor() instanceof FreeTextSearchProcessor,
                    "expected FreeTextSearchProcessor, got " + ctx.getSearchProcessor().getClass().getSimpleName());

            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("filterdemo", TYPE_DATASET, 20, 0, customAttrFilter("env=production"), null));
            assertTrue(out.resultSize == 2, "query+filter expected 2 hits, got " + out.resultSize);
        });

        record("Free-text + custom attr env=staging", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("filterdemo", TYPE_DATASET, 20, 0, customAttrFilter("env=staging"), null));
            assertTrue(out.resultSize == 1, "expected 1 staging entity, got " + out.resultSize);
        });

        record("Free-text + custom attr costCenter=CC100", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("filterdemo", TYPE_DATASET, 20, 0, customAttrFilter("costCenter=CC100"), null));
            assertTrue(out.resultSize == 2, "expected prod_alpha and stage (CC100), got " + out.resultSize);
        });
    }

    private static FilterCriteria customAttrFilter(String keyValue) {
        FilterCriteria filter = new FilterCriteria();
        filter.setAttributeName(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY);
        filter.setOperator(Operator.CONTAINS);
        filter.setAttributeValue(keyValue);
        return filter;
    }

    /**
     * Mirrors {@link EntityDiscoveryService#quickSearch} search half (no aggregations).
     */
    private static QuickSearchOutcome quickSearch(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                                IndexFieldNames indexFields,
                                                QuickSearchParameters quickSearchParameters) throws AtlasBaseException {
        String query = quickSearchParameters.getQuery();
        if (StringUtils.isNotEmpty(query) && !AtlasAttribute.hastokenizeChar(query)) {
            query = query + "*";
        }
        quickSearchParameters.setQuery(query);

        SearchParameters searchParameters = EntityDiscoveryService.createSearchParameters(quickSearchParameters);
        SearchContext searchContext = new SearchContext(searchParameters, typeRegistry, graph,
                buildIndexedKeys(typeRegistry, indexFields));

        SearchProcessor processor = searchContext.getSearchProcessor();
        if (!(processor instanceof FreeTextSearchProcessor) && searchContext.getSearchParameters().getQuery() != null) {
            // SearchContext may chain processors; ensure we exercise the full processor chain as quickSearch does.
        }

        List<AtlasVertex> vertices = processor.execute();
        long              count    = processor.getResultCount();

        QuickSearchOutcome outcome = new QuickSearchOutcome();
        outcome.atlasQuery       = searchParameters.getQuery();
        outcome.approximateCount = count;
        outcome.resultSize       = vertices.size();
        outcome.vertices         = vertices;
        outcome.typesSeen        = new ArrayList<>();

        for (AtlasVertex vertex : vertices) {
            String typeName = vertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
            if (typeName != null && !outcome.typesSeen.contains(typeName)) {
                outcome.typesSeen.add(typeName);
            }
        }

        return outcome;
    }

    private static QuickSearchParameters params(String query, String typeName, int limit, int offset,
                                                FilterCriteria entityFilter, String sortBy) {
        QuickSearchParameters p = new QuickSearchParameters();
        p.setQuery(query);
        p.setTypeName(typeName);
        p.setLimit(limit);
        p.setOffset(offset);
        p.setExcludeDeletedEntities(true);
        p.setIncludeSubTypes(true);
        p.setEntityFilters(entityFilter);
        p.setSortBy(sortBy);
        return p;
    }

    private static void debugTrace(QuickSearchOutcome outcome, String exampleOsQuery) {
        try {
            System.out.println("[DEBUG] Atlas query (post suffix): " + outcome.atlasQuery);
            System.out.println("[DEBUG] Processor approximate count: " + outcome.approximateCount);
            OpenSearchQuickSearchSmokeSupport.debugOpenSearchSearch(exampleOsQuery);
        } catch (Exception e) {
            System.out.println("[DEBUG] OpenSearch debug trace skipped: " + e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Fixture setup
    // -------------------------------------------------------------------------

    private static IndexFieldNames createSchema(JanusGraph graph, AtlasTypeRegistry typeRegistry)
            throws org.apache.atlas.exception.AtlasBaseException {
        AtlasEntityType assetType = typeRegistry.getEntityTypeByName(TYPE_ASSET);
        String nameProperty  = assetType.getAttribute("name").getVertexPropertyName();
        String ownerProperty = assetType.getAttribute("owner").getVertexPropertyName();

        JanusGraphManagement mgmt = graph.openManagement();

        PropertyKey guidKey  = mgmt.makePropertyKey(Constants.GUID_PROPERTY_KEY).dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey typeKey  = mgmt.makePropertyKey(Constants.ENTITY_TYPE_PROPERTY_KEY).dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey stateKey = mgmt.makePropertyKey(Constants.STATE_PROPERTY_KEY).dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey nameKey  = mgmt.makePropertyKey(nameProperty).dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey ownerKey = mgmt.makePropertyKey(ownerProperty).dataType(String.class).cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey customKey = mgmt.makePropertyKey(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();

        mgmt.buildIndex(OpenSearchQuickSearchSmokeSupport.VERTEX_INDEX, Vertex.class)
                .addKey(guidKey, Mapping.STRING.asParameter())
                .addKey(typeKey, Mapping.STRING.asParameter())
                .addKey(stateKey, Mapping.STRING.asParameter())
                .addKey(nameKey, Mapping.TEXT.asParameter())
                .addKey(ownerKey, Mapping.STRING.asParameter())
                .addKey(customKey, Mapping.TEXT.asParameter())
                .buildMixedIndex(OpenSearchQuickSearchSmokeSupport.BACKING_INDEX_NAME);

        IndexFieldNames fields = new IndexFieldNames();
        fields.guidIndexField             = guidKey.name();
        fields.typeIndexField             = typeKey.name();
        fields.stateIndexField            = stateKey.name();
        fields.nameIndexField             = nameKey.name();
        fields.ownerIndexField            = ownerKey.name();
        fields.customAttributesIndexField = customKey.name();

        mgmt.commit();
        graph.tx().commit();
        return fields;
    }

    private static AtlasTypeRegistry buildTypeRegistry(IndexFieldNames indexFields) throws org.apache.atlas.exception.AtlasBaseException {
        AtlasTypeRegistry registry = new AtlasTypeRegistry();

        AtlasEntityDef assetDef   = entityDef(TYPE_ASSET, Collections.emptySet(), indexFields, true);
        AtlasEntityDef datasetDef = entityDef(TYPE_DATASET, Collections.singleton(TYPE_ASSET), indexFields, false);
        AtlasEntityDef tableDef   = entityDef(TYPE_TABLE, Collections.singleton(TYPE_ASSET), indexFields, false);

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(assetDef);
        typesDef.getEntityDefs().add(datasetDef);
        typesDef.getEntityDefs().add(tableDef);
        registry.updateTypes(typesDef);

        return registry;
    }

    private static void wireIndexFieldNames(AtlasTypeRegistry registry, IndexFieldNames indexFields) {
        wireIndexFieldNames(registry.getEntityTypeByName(TYPE_ASSET), indexFields);
        wireIndexFieldNames(registry.getEntityTypeByName(TYPE_DATASET), indexFields);
        wireIndexFieldNames(registry.getEntityTypeByName(TYPE_TABLE), indexFields);
        // OpenSearch physical field for JSON custom attrs is __customAttributes (see mixed-index mapping).
        String customAttributesOsField = "__customAttributes";
        registry.getEntityTypeByName(TYPE_ASSET).getAttribute(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY)
                .setIndexFieldName(customAttributesOsField);
        registry.getEntityTypeByName(TYPE_DATASET).getAttribute(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY)
                .setIndexFieldName(customAttributesOsField);
        registry.addIndexFieldName(Constants.ENTITY_TYPE_PROPERTY_KEY, indexFields.typeIndexField);
        registry.addIndexFieldName(Constants.STATE_PROPERTY_KEY, indexFields.stateIndexField);
        registry.addIndexFieldName(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY, customAttributesOsField);
    }

    private static AtlasEntityDef entityDef(String typeName, java.util.Set<String> superTypes,
                                              IndexFieldNames indexFields, boolean includeIndexedAttrs) {
        AtlasEntityDef def = new AtlasEntityDef();
        def.setName(typeName);
        def.setSuperTypes(superTypes);

        if (includeIndexedAttrs) {
            List<AtlasAttributeDef> attrs = new ArrayList<>();
            attrs.add(indexedAttr("name", indexFields.nameIndexField));
            attrs.add(indexedAttr("owner", indexFields.ownerIndexField));
            def.setAttributeDefs(attrs);
        }
        return def;
    }

    private static AtlasAttributeDef indexedAttr(String name, String indexFieldName) {
        AtlasAttributeDef attr = new AtlasAttributeDef(name, "string");
        attr.setIndexType(AtlasAttributeDef.IndexType.STRING);
        return attr;
    }

    private static void wireIndexFieldNames(AtlasEntityType type, IndexFieldNames indexFields) {
        type.getAttribute("name").setIndexFieldName(indexFields.nameIndexField);
        type.getAttribute("owner").setIndexFieldName(indexFields.ownerIndexField);
    }

    private static Set<String> buildIndexedKeys(AtlasTypeRegistry typeRegistry, IndexFieldNames indexFields)
            throws org.apache.atlas.exception.AtlasBaseException {
        Set<String> keys = new HashSet<>();
        AtlasEntityType assetType = typeRegistry.getEntityTypeByName(TYPE_ASSET);
        keys.add(assetType.getAttribute("name").getVertexPropertyName());
        keys.add(assetType.getAttribute("owner").getVertexPropertyName());
        keys.add(indexFields.guidIndexField);
        keys.add(indexFields.typeIndexField);
        keys.add(indexFields.stateIndexField);
        keys.add(indexFields.customAttributesIndexField);
        return keys;
    }

    private static Map<String, String> insertFilterDemoEntities(AtlasGraph graph, AtlasTypeRegistry typeRegistry)
            throws org.apache.atlas.exception.AtlasBaseException {
        Map<String, String> guids = new LinkedHashMap<>();
        guids.put("PROD_ALPHA", insertEntity(graph, typeRegistry, TYPE_DATASET, "filterdemo_prod_alpha", "team-alpha",
                customAttrMap("env", "production", "costCenter", "CC100")));
        guids.put("PROD_BETA", insertEntity(graph, typeRegistry, TYPE_DATASET, "filterdemo_prod_beta", "team-beta",
                customAttrMap("env", "production", "costCenter", "CC200")));
        guids.put("STAGE", insertEntity(graph, typeRegistry, TYPE_DATASET, "filterdemo_stage_gamma", "team-alpha",
                customAttrMap("env", "staging", "costCenter", "CC100")));
        guids.put("PLAIN", insertEntity(graph, typeRegistry, TYPE_DATASET, "filterdemo_plain_table", "team-gamma", null));
        return guids;
    }

    private static Map<String, String> customAttrMap(String k1, String v1, String k2, String v2) {
        Map<String, String> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    private static void insertTestEntities(AtlasGraph graph, AtlasTypeRegistry typeRegistry)
            throws org.apache.atlas.exception.AtlasBaseException {
        insertEntity(graph, typeRegistry, TYPE_DATASET, "Alice Atlas World", "team-alpha");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "Bob Atlas Platform", "team-beta");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "Charlie Marketing Report", "team-alpha");
        insertEntity(graph, typeRegistry, TYPE_TABLE, "Atlas Report Summary", "team-gamma");
        insertEntity(graph, typeRegistry, TYPE_TABLE, "Sales Report Quarterly", "team-delta");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "Customer Atlas Data", "team-alpha");
    }

    private static String insertEntity(AtlasGraph graph, AtlasTypeRegistry typeRegistry, String typeName,
                                       String name, String owner, Map<String, String> customAttributes)
            throws org.apache.atlas.exception.AtlasBaseException {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        String guid = UUID.randomUUID().toString();
        AtlasVertex vertex = graph.addVertex();
        vertex.setProperty(Constants.GUID_PROPERTY_KEY, guid);
        vertex.setProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, typeName);
        vertex.setProperty(Constants.STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());
        vertex.setProperty(entityType.getAttribute("name").getVertexPropertyName(), name);
        vertex.setProperty(entityType.getAttribute("owner").getVertexPropertyName(), owner);
        if (customAttributes != null && !customAttributes.isEmpty()) {
            vertex.setProperty(Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY, AtlasType.toJson(customAttributes));
        }
        return guid;
    }

    private static void insertEntity(AtlasGraph graph, AtlasTypeRegistry typeRegistry, String typeName,
                                     String name, String owner) throws org.apache.atlas.exception.AtlasBaseException {
        insertEntity(graph, typeRegistry, typeName, name, owner, null);
    }

    // -------------------------------------------------------------------------
    // Test harness helpers
    // -------------------------------------------------------------------------

    private interface ValidationStep {
        void run() throws Exception;
    }

    private static void record(String name, ValidationStep step) {
        try {
            step.run();
            RESULTS.put(name, "PASS");
        } catch (AssertionError | Exception e) {
            RESULTS.put(name, "FAIL (" + e.getMessage() + ")");
        }
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void assertEquals(long actual, long expected, String label) {
        if (actual != expected) {
            throw new AssertionError(label + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertEquals(int actual, int expected, String label) {
        if (actual != expected) {
            throw new AssertionError(label + ": expected " + expected + " but was " + actual);
        }
    }

    private static void assertDifferentGuids(QuickSearchOutcome a, QuickSearchOutcome b) {
        String guidA = a.vertices.isEmpty() ? null : a.vertices.get(0).getProperty(Constants.GUID_PROPERTY_KEY, String.class);
        String guidB = b.vertices.isEmpty() ? null : b.vertices.get(0).getProperty(Constants.GUID_PROPERTY_KEY, String.class);
        if (guidA != null && guidA.equals(guidB)) {
            throw new AssertionError("pagination returned duplicate first GUID");
        }
    }

    private static final class IndexFieldNames {
        private String guidIndexField;
        private String typeIndexField;
        private String stateIndexField;
        private String nameIndexField;
        private String ownerIndexField;
        private String customAttributesIndexField;
    }

    private static final class QuickSearchOutcome {
        private String             atlasQuery;
        private long                 approximateCount;
        private int                  resultSize;
        private List<AtlasVertex>    vertices = Collections.emptyList();
        private List<String>         typesSeen = Collections.emptyList();
    }
}
