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

import org.apache.atlas.discovery.FreeTextSearchProcessor;
import org.apache.atlas.discovery.SearchAggregatorImpl;
import org.apache.atlas.discovery.SearchContext;
import org.apache.atlas.model.discovery.QuickSearchParameters;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.model.discovery.AtlasAggregationEntry;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AggregationContext;
import org.apache.atlas.repository.graphdb.QuickSearchContext;
import org.apache.atlas.repository.graphdb.QuickSearchResult;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * C5.4 validation: Atlas aggregations through {@link AtlasGraphIndexClient#getAggregatedMetrics}
 * and {@link SearchAggregatorImpl} against OpenSearch 2.x.
 *
 * <pre>
 *   cd repository && mvn test-compile exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=org.apache.atlas.discovery.smoke.OpenSearchAggregationsValidationDriver \
 *     -Drat.skip=true -Dcheckstyle.skip=true -Dsortpom.skip=true -DskipCheck=true
 * </pre>
 */
public final class OpenSearchAggregationsValidationDriver {

    private static final String TYPE_DATASET = "c54_dataset";
    private static final String TYPE_TABLE   = "c54_table";
    private static final String TYPE_ASSET   = "c54_asset";
    private static final String SERVICE_A    = "c54_service_a";
    private static final String SERVICE_B    = "c54_service_b";

    private static final Map<String, String> RESULTS = new LinkedHashMap<>();

    private OpenSearchAggregationsValidationDriver() {
    }

    public static boolean execute() throws Exception {
        RESULTS.clear();

        OpenSearchQuickSearchSmokeSupport.bootstrapApplicationProperties();
        OpenSearchQuickSearchSmokeSupport.verifyOpenSearchReachable();
        OpenSearchQuickSearchSmokeSupport.registerAtlasOpenSearchIndex();
        OpenSearchQuickSearchSmokeSupport.deletePhysicalIndexIfPresent();

        JanusGraph janusGraph = JanusGraphFactory.open(OpenSearchQuickSearchSmokeSupport.buildJanusGraphConfiguration());
        AtlasTypeRegistry typeRegistry = buildTypeRegistry(new IndexFieldNames());
        IndexFieldNames indexFields = createSchema(janusGraph, typeRegistry);
        wireTypeRegistry(typeRegistry, indexFields);

        AtlasGraph graph = new AtlasJanusGraph(janusGraph);
        insertTestEntities(graph, typeRegistry);
        graph.commit();

        Thread.sleep(1500);

        AtlasGraphIndexClient indexClient = graph.getGraphIndexClient();

        runValidations(indexClient, graph, typeRegistry, indexFields);

        graph.shutdown();

        return RESULTS.values().stream().allMatch("PASS"::equals);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("C5.4 Atlas Aggregations Validation");
        boolean allPassed = execute();
        for (Map.Entry<String, String> entry : RESULTS.entrySet()) {
            System.out.printf("%-32s %s%n", entry.getKey(), entry.getValue());
        }
        System.out.println(allPassed ? "C5.4 RESULT: PASS" : "C5.4 RESULT: FAIL");
        if (!allPassed) {
            System.exit(1);
        }
    }

    private static void runValidations(AtlasGraphIndexClient indexClient, AtlasGraph graph,
                                       AtlasTypeRegistry typeRegistry, IndexFieldNames indexFields) throws Exception {
        Set<AtlasEntityType> allTypes = entityTypes(typeRegistry, TYPE_DATASET, TYPE_TABLE);
        Map<String, String> indexFieldNameCache = buildIndexFieldNameCache(typeRegistry, indexFields);

        record("Single aggregation field", () -> {
            AggregationContext ctx = aggContext("", null, allTypes, indexFieldNameCache,
                    setOf(Constants.ENTITY_TYPE_PROPERTY_KEY), Collections.emptySet(), true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            assertTrue(metrics.containsKey(Constants.ENTITY_TYPE_PROPERTY_KEY),
                    "expected typeName aggregation");
            assertTrue(!metrics.get(Constants.ENTITY_TYPE_PROPERTY_KEY).isEmpty(),
                    "expected non-empty typeName buckets");
        });

        record("Multiple aggregation fields", () -> {
            Set<String> fields = linkedSet(Constants.ENTITY_TYPE_PROPERTY_KEY, Constants.STATE_PROPERTY_KEY);
            AggregationContext ctx = aggContext("", null, allTypes, indexFieldNameCache,
                    fields, Collections.emptySet(), true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            assertTrue(metrics.containsKey(Constants.ENTITY_TYPE_PROPERTY_KEY), "missing typeName");
            assertTrue(metrics.containsKey(Constants.STATE_PROPERTY_KEY), "missing __state");
            assertTrue(metrics.get(Constants.STATE_PROPERTY_KEY).stream()
                    .anyMatch(e -> AtlasEntity.Status.ACTIVE.name().equals(e.getName())),
                    "expected ACTIVE state bucket");
        });

        record("Entity/type filtering", () -> {
            Set<AtlasEntityType> datasetOnly = entityTypes(typeRegistry, TYPE_DATASET);
            AggregationContext ctx = aggContext("", null, datasetOnly, indexFieldNameCache,
                    setOf(Constants.ENTITY_TYPE_PROPERTY_KEY), Collections.emptySet(), true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            List<String> typeNames = metrics.get(Constants.ENTITY_TYPE_PROPERTY_KEY).stream()
                    .map(AtlasAggregationEntry::getName).collect(Collectors.toList());
            assertTrue(typeNames.contains(TYPE_DATASET), "expected dataset type");
            assertTrue(!typeNames.contains(TYPE_TABLE), "table type should be filtered out, got " + typeNames);
        });

        record("Attribute filtering", () -> {
            FilterCriteria filter = leafFilter("owner", Operator.EQ, "team-alpha");
            Set<AtlasAttribute> attrs = ownerAttribute(typeRegistry);
            AggregationContext ctx = aggContext("", filter, allTypes, indexFieldNameCache,
                    Collections.emptySet(), attrs, true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            String ownerKey = ownerAttribute(typeRegistry).iterator().next().getQualifiedName();
            List<AtlasAggregationEntry> ownerBuckets = metrics.get(ownerKey);
            assertTrue(ownerBuckets != null && ownerBuckets.size() == 1,
                    "expected single owner bucket after filter, got " + ownerBuckets);
            assertTrue("team-alpha".equals(ownerBuckets.get(0).getName()), "expected team-alpha bucket");
            assertEquals(ownerBuckets.get(0).getCount(), 4L, "team-alpha count");
        });

        record("Aggregation matches quick-search filter scope", () -> {
            FilterCriteria filter = leafFilter("owner", Operator.EQ, "team-alpha");
            QuickSearchParameters quickParams = quickSearchParams("atlas", TYPE_DATASET, filter);
            SearchParameters searchParameters = EntityDiscoveryService.createSearchParameters(quickParams);
            SearchContext searchContext = new SearchContext(searchParameters, typeRegistry, graph, Collections.emptySet());

            QuickSearchResult quickResult = runQuickSearchIndexQuery(searchContext, indexFields);
            SearchAggregatorImpl aggregator = new SearchAggregatorImpl(searchContext);
            Map<String, List<AtlasAggregationEntry>> metrics = aggregator.getAggregatedMetrics(
                    setOf(Constants.ENTITY_TYPE_PROPERTY_KEY), Collections.emptySet());

            long typeNameBucketTotal = metrics.get(Constants.ENTITY_TYPE_PROPERTY_KEY).stream()
                    .mapToLong(AtlasAggregationEntry::getCount).sum();
            assertEquals(typeNameBucketTotal, quickResult.getTotalCount(),
                    "aggregation buckets should match quick-search total under same filters");
        });

        record("Wildcard query aggregation parity", () -> {
            QuickSearchParameters quickParams = quickSearchParams("custo*", TYPE_DATASET, null);
            SearchParameters searchParameters = EntityDiscoveryService.createSearchParameters(quickParams);
            SearchContext searchContext = new SearchContext(searchParameters, typeRegistry, graph, Collections.emptySet());

            QuickSearchResult quickResult = runQuickSearchIndexQuery(searchContext, indexFields);
            assertTrue(quickResult.getTotalCount() >= 1, "wildcard quick-search should match customer entity");

            SearchAggregatorImpl aggregator = new SearchAggregatorImpl(searchContext);
            Map<String, List<AtlasAggregationEntry>> metrics = aggregator.getAggregatedMetrics(
                    setOf(Constants.ENTITY_TYPE_PROPERTY_KEY), Collections.emptySet());

            long typeNameBucketTotal = metrics.get(Constants.ENTITY_TYPE_PROPERTY_KEY).stream()
                    .mapToLong(AtlasAggregationEntry::getCount).sum();
            assertEquals(typeNameBucketTotal, quickResult.getTotalCount(),
                    "wildcard aggregation total should match quick-search total");
        });

        record("Empty result", () -> {
            AggregationContext ctx = aggContext("zzz-no-match-xyz", null, allTypes, indexFieldNameCache,
                    setOf(Constants.ENTITY_TYPE_PROPERTY_KEY), Collections.emptySet(), true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            List<AtlasAggregationEntry> buckets = metrics.get(Constants.ENTITY_TYPE_PROPERTY_KEY);
            assertTrue(buckets == null || buckets.isEmpty(),
                    "expected empty typeName buckets for non-matching query, got " + buckets);
        });

        record("Bucket counts", () -> {
            AggregationContext ctx = aggContext("", null, allTypes, indexFieldNameCache,
                    Collections.emptySet(), ownerAttribute(typeRegistry), true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            String ownerKey = ownerAttribute(typeRegistry).iterator().next().getQualifiedName();
            long teamAlphaCount = metrics.get(ownerKey).stream()
                    .filter(e -> "team-alpha".equals(e.getName()))
                    .mapToLong(AtlasAggregationEntry::getCount)
                    .findFirst().orElse(-1);
            assertEquals(teamAlphaCount, 4L, "team-alpha document count");
        });

        record("Multiple entity types", () -> {
            AggregationContext ctx = aggContext("", null, allTypes, indexFieldNameCache,
                    setOf(Constants.ENTITY_TYPE_PROPERTY_KEY), Collections.emptySet(), true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            Set<String> types = metrics.get(Constants.ENTITY_TYPE_PROPERTY_KEY).stream()
                    .map(AtlasAggregationEntry::getName).collect(Collectors.toSet());
            assertTrue(types.contains(TYPE_DATASET) && types.contains(TYPE_TABLE),
                    "expected both entity types in aggregation, got " + types);
        });

        record("Pagination independence", () -> {
            // Aggregations always use size=0; verify counts are full-set not truncated.
            AggregationContext ctx = aggContext("", null, allTypes, indexFieldNameCache,
                    setOf(Constants.ENTITY_TYPE_PROPERTY_KEY), Collections.emptySet(), true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            long totalFromAggs = metrics.get(Constants.ENTITY_TYPE_PROPERTY_KEY).stream()
                    .mapToLong(AtlasAggregationEntry::getCount).sum();
            assertEquals(totalFromAggs, 13L, "aggregation total doc count (13 entities)");
        });

        record("Search query + aggregation", () -> {
            AggregationContext ctx = aggContext("Atlas", null, allTypes, indexFieldNameCache,
                    Collections.emptySet(), ownerAttribute(typeRegistry), true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            String ownerKey = ownerAttribute(typeRegistry).iterator().next().getQualifiedName();
            long totalOwners = metrics.get(ownerKey).stream().mapToLong(AtlasAggregationEntry::getCount).sum();
            assertTrue(totalOwners >= 5 && totalOwners <= 10,
                    "Atlas query should match subset of docs, total owner agg=" + totalOwners);
        });

        record("Atlas response shape", () -> {
            AggregationContext ctx = aggContext("", null, allTypes, indexFieldNameCache,
                    setOf(Constants.STATE_PROPERTY_KEY), Collections.emptySet(), true, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            AtlasAggregationEntry entry = metrics.get(Constants.STATE_PROPERTY_KEY).get(0);
            assertTrue(entry.getName() != null && entry.getCount() > 0,
                    "AtlasAggregationEntry should expose name and positive count");
        });

        record("ServiceType post-processing", () -> {
            SearchParameters params = new SearchParameters();
            params.setTypeName(TYPE_DATASET + "," + TYPE_TABLE);
            params.setExcludeDeletedEntities(true);
            params.setIncludeSubTypes(true);

            SearchContext searchContext = new SearchContext(params, typeRegistry, graph, Collections.emptySet());
            SearchAggregatorImpl aggregator = new SearchAggregatorImpl(searchContext);

            // Match EntityDiscoveryService: typeName and __state both populate indexFieldNameCache.
            Set<String> fields = linkedSet(Constants.ENTITY_TYPE_PROPERTY_KEY, Constants.STATE_PROPERTY_KEY);
            Map<String, List<AtlasAggregationEntry>> metrics =
                    aggregator.getAggregatedMetrics(fields, Collections.emptySet());

            assertTrue(metrics.containsKey("ServiceType"), "ServiceType metric should be added");
            assertTrue(metrics.get("ServiceType").stream().anyMatch(e -> SERVICE_A.equals(e.getName()) && e.getCount() > 0),
                    "service A should have non-zero count");
            assertTrue(metrics.get("ServiceType").stream().noneMatch(e -> e.getCount() == 0),
                    "zero-count service entries should be removed");
        });

        record("__state ACTIVE/DELETED counts", () -> {
            insertEntity(graph, typeRegistry, TYPE_DATASET, "Deleted One", "team-alpha",
                    AtlasEntity.Status.DELETED);
            insertEntity(graph, typeRegistry, TYPE_DATASET, "Deleted Two", "team-beta",
                    AtlasEntity.Status.DELETED);
            graph.commit();
            Thread.sleep(1500);

            AggregationContext ctx = aggContext("", null, allTypes, indexFieldNameCache,
                    setOf(Constants.STATE_PROPERTY_KEY), Collections.emptySet(), false, true);
            Map<String, List<AtlasAggregationEntry>> metrics = indexClient.getAggregatedMetrics(ctx);

            Map<String, Long> stateCounts = metrics.get(Constants.STATE_PROPERTY_KEY).stream()
                    .collect(Collectors.toMap(AtlasAggregationEntry::getName, AtlasAggregationEntry::getCount));
            assertEquals(stateCounts.get(AtlasEntity.Status.ACTIVE.name()).longValue(), 13L,
                    "ACTIVE entity count");
            assertEquals(stateCounts.get(AtlasEntity.Status.DELETED.name()).longValue(), 2L,
                    "DELETED entity count");
        });

        record("Existing index", () -> {
            AggregationContext ctx = aggContext("", null, allTypes, indexFieldNameCache,
                    setOf(Constants.ENTITY_TYPE_PROPERTY_KEY), Collections.emptySet(), true, true);
            Map<String, List<AtlasAggregationEntry>> first  = indexClient.getAggregatedMetrics(ctx);
            Map<String, List<AtlasAggregationEntry>> second = indexClient.getAggregatedMetrics(ctx);

            assertEquals(first.size(), second.size(), "repeat agg on existing index");
            assertTrue(!second.get(Constants.ENTITY_TYPE_PROPERTY_KEY).isEmpty(),
                    "existing index should still return buckets");
        });
    }

    // -------------------------------------------------------------------------
    // Fixture setup (AtlasJanusGraphManagement path; __state and __typeName use isStringField=false)
    // -------------------------------------------------------------------------

    private static IndexFieldNames createSchema(JanusGraph janusGraph, AtlasTypeRegistry typeRegistry) throws Exception {
        AtlasEntityType assetType = typeRegistry.getEntityTypeByName(TYPE_ASSET);
        String nameProperty  = assetType.getAttribute("name").getVertexPropertyName();
        String ownerProperty = assetType.getAttribute("owner").getVertexPropertyName();

        AtlasJanusGraph graph = new AtlasJanusGraph(janusGraph);

        IndexFieldNames fields = new IndexFieldNames();

        try (AtlasGraphManagement mgmt = graph.getManagementSystem()) {
            mgmt.createVertexMixedIndex(OpenSearchQuickSearchSmokeSupport.VERTEX_INDEX,
                    OpenSearchQuickSearchSmokeSupport.BACKING_INDEX_NAME, Collections.emptyList());

            AtlasPropertyKey guidKey  = mgmt.makePropertyKey(Constants.GUID_PROPERTY_KEY, String.class, AtlasCardinality.SINGLE);
            AtlasPropertyKey typeKey  = mgmt.makePropertyKey(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class, AtlasCardinality.SINGLE);
            AtlasPropertyKey stateKey = mgmt.makePropertyKey(Constants.STATE_PROPERTY_KEY, String.class, AtlasCardinality.SINGLE);
            AtlasPropertyKey nameKey  = mgmt.makePropertyKey(nameProperty, String.class, AtlasCardinality.SINGLE);
            AtlasPropertyKey ownerKey = mgmt.makePropertyKey(ownerProperty, String.class, AtlasCardinality.SINGLE);

            fields.guidIndexField  = mgmt.addMixedIndex(OpenSearchQuickSearchSmokeSupport.VERTEX_INDEX, guidKey, false);
            fields.typeIndexField  = mgmt.addMixedIndex(OpenSearchQuickSearchSmokeSupport.VERTEX_INDEX, typeKey, false, true);
            fields.stateIndexField = mgmt.addMixedIndex(OpenSearchQuickSearchSmokeSupport.VERTEX_INDEX, stateKey, false, true);
            fields.nameIndexField  = mgmt.addMixedIndex(OpenSearchQuickSearchSmokeSupport.VERTEX_INDEX, nameKey, true);
            fields.ownerIndexField = mgmt.addMixedIndex(OpenSearchQuickSearchSmokeSupport.VERTEX_INDEX, ownerKey, true);

            mgmt.updateSchemaStatus();
            mgmt.setIsSuccess(true);
        }
        graph.commit();

        return fields;
    }

    private static AtlasTypeRegistry buildTypeRegistry(IndexFieldNames indexFields) throws Exception {
        AtlasTypeRegistry registry = new AtlasTypeRegistry();

        AtlasEntityDef assetDef   = entityDef(TYPE_ASSET, Collections.emptySet(), SERVICE_A, true);
        AtlasEntityDef datasetDef = entityDef(TYPE_DATASET, Collections.singleton(TYPE_ASSET), SERVICE_A, false);
        AtlasEntityDef tableDef   = entityDef(TYPE_TABLE, Collections.singleton(TYPE_ASSET), SERVICE_B, false);

        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.getEntityDefs().add(assetDef);
        typesDef.getEntityDefs().add(datasetDef);
        typesDef.getEntityDefs().add(tableDef);
        registry.updateTypes(typesDef);

        return registry;
    }

    private static void wireTypeRegistry(AtlasTypeRegistry registry, IndexFieldNames indexFields) {
        wireType(registry.getEntityTypeByName(TYPE_ASSET), indexFields);
        wireType(registry.getEntityTypeByName(TYPE_DATASET), indexFields);
        wireType(registry.getEntityTypeByName(TYPE_TABLE), indexFields);

        registry.addIndexFieldName(Constants.ENTITY_TYPE_PROPERTY_KEY, indexFields.typeIndexField);
        registry.addIndexFieldName(Constants.STATE_PROPERTY_KEY, indexFields.stateIndexField);
        registry.addIndexFieldName("owner", indexFields.ownerIndexField);
        registry.addIndexFieldName("name", indexFields.nameIndexField);
    }

    private static void wireType(AtlasEntityType type, IndexFieldNames indexFields) {
        type.getAttribute("name").setIndexFieldName(indexFields.nameIndexField);
        type.getAttribute("owner").setIndexFieldName(indexFields.ownerIndexField);
    }

    private static AtlasEntityDef entityDef(String typeName, Set<String> superTypes, String serviceType,
                                            boolean includeIndexedAttrs) {
        AtlasEntityDef def = new AtlasEntityDef();
        def.setName(typeName);
        def.setSuperTypes(superTypes);
        def.setServiceType(serviceType);

        if (includeIndexedAttrs) {
            List<AtlasAttributeDef> attrs = new ArrayList<>();
            attrs.add(indexedAttr("name"));
            attrs.add(indexedAttr("owner"));
            def.setAttributeDefs(attrs);
        }
        return def;
    }

    private static AtlasAttributeDef indexedAttr(String name) {
        AtlasAttributeDef attr = new AtlasAttributeDef(name, "string");
        attr.setIndexType(AtlasAttributeDef.IndexType.STRING);
        return attr;
    }

    private static void insertTestEntities(AtlasGraph graph, AtlasTypeRegistry typeRegistry) throws Exception {
        insertEntity(graph, typeRegistry, TYPE_DATASET, "Alice Atlas World", "team-alpha");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "Bob Atlas Platform", "team-beta");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "Charlie Atlas Marketing", "team-alpha");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "Delta Atlas Ops", "team-alpha");
        insertEntity(graph, typeRegistry, TYPE_TABLE, "Atlas Report Summary", "team-gamma");
        insertEntity(graph, typeRegistry, TYPE_TABLE, "Sales Atlas Quarterly", "team-delta");
        insertEntity(graph, typeRegistry, TYPE_TABLE, "Team-Omega Special", "Team-Omega");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "Customer Atlas Data", "team-alpha");

        for (int i = 1; i <= 5; i++) {
            insertEntity(graph, typeRegistry, TYPE_DATASET,
                    "Rank Entity " + i, String.format("rank-%02d", i));
        }
    }

    private static void insertEntity(AtlasGraph graph, AtlasTypeRegistry typeRegistry, String typeName,
                                     String name, String owner) throws Exception {
        insertEntity(graph, typeRegistry, typeName, name, owner, AtlasEntity.Status.ACTIVE);
    }

    private static void insertEntity(AtlasGraph graph, AtlasTypeRegistry typeRegistry, String typeName,
                                     String name, String owner, AtlasEntity.Status state) throws Exception {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        AtlasVertex vertex = graph.addVertex();
        vertex.setProperty(Constants.GUID_PROPERTY_KEY, UUID.randomUUID().toString());
        vertex.setProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, typeName);
        vertex.setProperty(Constants.STATE_PROPERTY_KEY, state.name());
        vertex.setProperty(entityType.getAttribute("name").getVertexPropertyName(), name);
        vertex.setProperty(entityType.getAttribute("owner").getVertexPropertyName(), owner);
    }

    // -------------------------------------------------------------------------
    // Aggregation helpers
    // -------------------------------------------------------------------------

    private static QuickSearchParameters quickSearchParams(String query, String typeName, FilterCriteria filter) {
        QuickSearchParameters params = new QuickSearchParameters();
        params.setQuery(query);
        params.setTypeName(typeName);
        params.setEntityFilters(filter);
        params.setLimit(50);
        params.setOffset(0);
        params.setExcludeDeletedEntities(true);
        params.setIncludeSubTypes(true);
        return params;
    }

    private static QuickSearchResult runQuickSearchIndexQuery(SearchContext searchContext,
                                                              IndexFieldNames indexFields) throws Exception {
        SearchParameters searchParameters = searchContext.getSearchParameters();
        String query = searchParameters.getQuery();

        if (StringUtils.isNotEmpty(query) && !org.apache.atlas.type.AtlasStructType.AtlasAttribute.hastokenizeChar(query)) {
            query = query + "*";
        }

        searchParameters.setQuery(query);

        Map<String, String> indexFieldNameCache = FreeTextSearchProcessor.buildOpenSearchIndexFieldNameCache(
                searchContext.getTypeRegistry());
        indexFieldNameCache.put(Constants.ENTITY_TYPE_PROPERTY_KEY, indexFields.typeIndexField);
        indexFieldNameCache.put(Constants.STATE_PROPERTY_KEY, indexFields.stateIndexField);

        QuickSearchContext quickSearchContext = new QuickSearchContext(query, searchParameters.getEntityFilters(),
                FreeTextSearchProcessor.resolveOpenSearchEntityTypes(searchContext),
                FreeTextSearchProcessor.resolveOpenSearchClassificationTypeNames(searchContext),
                indexFieldNameCache, searchParameters.getExcludeDeletedEntities(),
                searchParameters.getIncludeSubTypes(), 0, 0);

        AtlasGraphIndexClient indexClient = searchContext.getGraph().getGraphIndexClient();
        return indexClient.quickSearch(quickSearchContext);
    }

    private static AggregationContext aggContext(String query, FilterCriteria filter,
                                                 Set<AtlasEntityType> entityTypes,
                                                 Map<String, String> indexFieldNameCache,
                                                 Set<String> commonFields, Set<AtlasAttribute> attributes,
                                                 boolean excludeDeleted, boolean includeSubTypes) {
        return new AggregationContext(query, filter, entityTypes, Collections.emptySet(), commonFields, attributes,
                indexFieldNameCache, excludeDeleted, includeSubTypes);
    }

    private static Map<String, String> buildIndexFieldNameCache(AtlasTypeRegistry typeRegistry,
                                                                IndexFieldNames indexFields) {
        Map<String, String> cache = new HashMap<>();
        cache.put(Constants.ENTITY_TYPE_PROPERTY_KEY, indexFields.typeIndexField);
        cache.put(Constants.STATE_PROPERTY_KEY, indexFields.stateIndexField);
        cache.put("owner", indexFields.ownerIndexField);
        cache.put("name", indexFields.nameIndexField);
        return cache;
    }

    private static Set<AtlasEntityType> entityTypes(AtlasTypeRegistry registry, String... names) {
        Set<AtlasEntityType> ret = new LinkedHashSet<>();
        for (String name : names) {
            ret.add(registry.getEntityTypeByName(name));
        }
        return ret;
    }

    private static Set<AtlasAttribute> ownerAttribute(AtlasTypeRegistry registry) {
        AtlasAttribute owner = registry.getEntityTypeByName(TYPE_ASSET).getAttribute("owner");
        return Collections.singleton(owner);
    }

    private static FilterCriteria leafFilter(String attributeName, Operator operator, String value) {
        FilterCriteria criteria = new FilterCriteria();
        criteria.setAttributeName(attributeName);
        criteria.setOperator(operator);
        criteria.setAttributeValue(value);
        return criteria;
    }

    @SafeVarargs
    private static <T> Set<T> setOf(T... items) {
        Set<T> ret = new LinkedHashSet<>();
        Collections.addAll(ret, items);
        return ret;
    }

    @SafeVarargs
    private static <T> Set<T> linkedSet(T... items) {
        return setOf(items);
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

    private static final class IndexFieldNames {
        private String guidIndexField;
        private String typeIndexField;
        private String stateIndexField;
        private String nameIndexField;
        private String ownerIndexField;
    }
}
