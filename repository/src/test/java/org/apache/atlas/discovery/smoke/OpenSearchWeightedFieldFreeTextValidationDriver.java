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
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Validates the {@code multi_match}-based OpenSearch free-text query architecture through the real production
 * path (SolrIndexHelper-equivalent weight map -&gt; applySearchWeight -&gt; FreeTextSearchProcessor -&gt;
 * AtlasOpenSearchQueryBuilder -&gt; quickSearch), against a live OpenSearch instance.
 *
 * <p>This supersedes the earlier {@code 900-field cap + match_phrase_prefix + manual dis_max} design. That
 * earlier design merely moved the failure threshold (truncating the weighted-field set instead of overflowing
 * OpenSearch's clause budget) and imposed stricter, order-dependent multi-word semantics than Atlas/Solr actually
 * have. The current architecture builds a single {@code multi_match} (or, for an explicit user-typed wildcard, a
 * single {@code query_string}) clause whose {@code fields} parameter lists every weighted field, letting
 * OpenSearch perform the per-field fan-out internally -- so there is no field-count cap of any kind.
 *
 * <ul>
 *   <li><b>Issue 5</b>: {@code searchWeights} carries one entry per indexable string attribute system-wide, with
 *       no upper bound; a free-text quick search must not fail with {@code too_many_nested_clauses} once that
 *       count exceeds OpenSearch/Lucene's default {@code maxClauseCount=1024} -- and, unlike the superseded
 *       design, no weighted field may be silently dropped from the query regardless of how many fields exist.</li>
 *   <li><b>Issue 11</b>: {@link EntityDiscoveryService#quickSearch} implicitly appends a trailing {@code *} to any
 *       bare (punctuation-/whitespace-free) word; the resulting trailing-wildcard search must be
 *       case-insensitive and support genuine partial-prefix matches, exactly like the analyzer that indexed the
 *       field, and multi-word queries must remain order-independent (OR-of-terms), matching Solr's actual eDisMax
 *       configuration -- not the stricter adjacent/ordered semantics {@code match_phrase_prefix} would impose.</li>
 * </ul>
 *
 * <pre>
 *   cd repository &amp;&amp; mvn test-compile exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=org.apache.atlas.discovery.smoke.OpenSearchWeightedFieldFreeTextValidationDriver \
 *     -Drat.skip=true -Dcheckstyle.skip=true -Dsortpom.skip=true
 * </pre>
 */
public final class OpenSearchWeightedFieldFreeTextValidationDriver {
    private static final String TYPE_ASSET   = "wfft_asset";
    private static final String TYPE_DATASET = "wfft_dataset";

    /** Well beyond OpenSearch/Lucene's default maxClauseCount (1024) and beyond the superseded 900-field cap. */
    private static final int SYNTHETIC_WEIGHTED_FIELD_COUNT = 3000;
    private static final int SYNTHETIC_BOOST                = 3;
    /** Deliberately LOWER than the synthetic boost so a boost-descending truncation (the superseded design's
     *  strategy) would have dropped this field first -- proving the current design does not merely move that
     *  cutoff. */
    private static final int LOW_WEIGHT_MARKER_BOOST         = 1;

    private static final String LABEL_TARGET    = "target";
    private static final String LABEL_OTHER     = "other";
    private static final String LABEL_MULTIWORD = "multiword";

    private static final String LOW_WEIGHT_MARKER_VALUE = "raretermxyz-" + UUID.randomUUID().toString().substring(0, 8);

    private static final Map<String, String> RESULTS = new LinkedHashMap<>();
    private static Map<String, String>        guidByLabel;

    private OpenSearchWeightedFieldFreeTextValidationDriver() {
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

        guidByLabel = insertBaselineEntities(janusGraph, typeRegistry);
        AtlasGraph graph = new AtlasJanusGraph(janusGraph);
        graph.commit();
        Thread.sleep(1500);

        // Apply a system-wide weight map that deliberately exceeds OpenSearch/Lucene's default maxClauseCount
        // (1024) AND the superseded 900-field cap -- this mirrors production, where SolrIndexHelper assigns a
        // weight to every indexable string attribute across every registered type, independent of the current
        // search's typeName restriction.
        applyOversizedSearchWeights(graph, indexFields);

        runValidations(graph, typeRegistry, indexFields);

        graph.shutdown();

        return RESULTS.values().stream().allMatch("PASS"::equals);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("OpenSearch multi_match free-text architecture validation (Issue 5 + Issue 11)");
        boolean allPassed = execute();
        for (Map.Entry<String, String> entry : RESULTS.entrySet()) {
            System.out.printf("%-72s %s%n", entry.getKey(), entry.getValue());
        }
        System.out.println(allPassed ? "RESULT: PASS" : "RESULT: FAIL");
        if (!allPassed) {
            System.exit(1);
        }
    }

    private static void runValidations(AtlasGraph graph, AtlasTypeRegistry typeRegistry, IndexFieldNames indexFields)
            throws Exception {
        // ================================================================================================
        // Issue 5: large field count must succeed, and NOTHING may be truncated.
        // ================================================================================================
        record("[Issue5] " + SYNTHETIC_WEIGHTED_FIELD_COUNT + " weighted fields + plain free-text query succeeds", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("customer", TYPE_DATASET, 10, 0, null));
            assertTrue(out.resultSize >= 1, "expected at least 1 hit, got " + out.resultSize);
        });

        record("[Issue5] large field count + typeName restriction is still honored", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("customer", TYPE_DATASET, 10, 0, null));
            assertTrue(out.vertices.stream().allMatch(v ->
                            TYPE_DATASET.equals(v.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class))),
                    "typeName restriction must still be honored");
        });

        record("[Issue5] large field count + entity filter succeeds", () -> {
            FilterCriteria filter = new FilterCriteria();
            filter.setAttributeName("owner");
            filter.setOperator(Operator.EQ);
            filter.setAttributeValue("team-target");

            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("customer", TYPE_DATASET, 10, 0, filter));
            assertTrue(out.resultSize == 1, "owner=team-target should return exactly 1 hit, got " + out.resultSize);
        });

        record("[Issue5] uses FreeTextSearchProcessor (production path, not a synthetic shortcut)", () -> {
            SearchContext ctx = buildSearchContext(graph, typeRegistry, indexFields,
                    params("customer", TYPE_DATASET, 10, 0, null));
            assertTrue(ctx.getSearchProcessor() instanceof org.apache.atlas.discovery.FreeTextSearchProcessor,
                    "processor=" + ctx.getSearchProcessor().getClass().getSimpleName());
        });

        // No truncation: a value that exists ONLY in a low-weight field positioned well beyond where the
        // superseded 900-field/boost-descending cap would have cut off must still be found.
        record("[Issue5] value in a low-weight field beyond the superseded cap boundary is still found", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params(LOW_WEIGHT_MARKER_VALUE, TYPE_DATASET, 10, 0, null));
            assertTrue(out.resultSize == 1,
                    "expected the low-weight-field-only value to be found (no truncation), got " + out.resultSize);
        });

        // ================================================================================================
        // Issue 11: case handling on the implicit trailing-wildcard ('*' auto-appended by EntityDiscoveryService).
        // ================================================================================================
        record("[Issue11] uppercase full-word query matches (ENTITYDESCRIPTION)", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("ENTITYDESCRIPTION", TYPE_DATASET, 10, 0, null));
            assertTrue(out.resultSize == 1, "expected 1 hit for uppercase query, got " + out.resultSize);
        });

        record("[Issue11] lowercase full-word query matches (entitydescription)", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("entitydescription", TYPE_DATASET, 10, 0, null));
            assertTrue(out.resultSize == 1, "expected 1 hit for lowercase query, got " + out.resultSize);
        });

        record("[Issue11] mixed-case full-word query matches (EntityDescription)", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("EntityDescription", TYPE_DATASET, 10, 0, null));
            assertTrue(out.resultSize == 1, "expected 1 hit for mixed-case query, got " + out.resultSize);
        });

        record("[Issue11] uppercase partial-prefix query matches (ENTITYDESC)", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("ENTITYDESC", TYPE_DATASET, 10, 0, null));
            assertTrue(out.resultSize == 1, "expected 1 hit for partial-prefix query, got " + out.resultSize);
        });

        record("[Issue11] mixed-case partial-prefix query matches (entityDESC)", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("entityDESC", TYPE_DATASET, 10, 0, null));
            assertTrue(out.resultSize == 1, "expected 1 hit for mixed-case partial-prefix query, got " + out.resultSize);
        });

        record("[Issue11] non-matching prefix correctly returns no hits (ENTITYZZZ)", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("ENTITYZZZ", TYPE_DATASET, 10, 0, null));
            assertTrue(out.resultSize == 0, "expected 0 hits for a genuinely non-matching prefix, got " + out.resultSize);
        });

        // ================================================================================================
        // Issue 11: multi-word order-independence (bool_prefix/best_fields OR-of-terms, not match_phrase_prefix's
        // adjacent/ordered phrase semantics). Fixture: description = "alpha beta gamma delta".
        // ================================================================================================
        record("[Issue11] multi-word query in original order matches (alpha beta)", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("alpha beta", TYPE_DATASET, 10, 0, null));
            assertTrue(containsGuid(out, LABEL_MULTIWORD), "expected the multiword fixture entity to match");
        });

        record("[Issue11] multi-word query in reversed order still matches (delta alpha)", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("delta alpha", TYPE_DATASET, 10, 0, null));
            assertTrue(containsGuid(out, LABEL_MULTIWORD),
                    "reversed-order multi-word query must still match (OR-of-terms, not an ordered phrase)");
        });

        record("[Issue11] multi-word query with non-adjacent terms still matches (beta delta)", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("beta delta", TYPE_DATASET, 10, 0, null));
            assertTrue(containsGuid(out, LABEL_MULTIWORD),
                    "non-adjacent terms (separated by 'gamma' in the indexed text) must still match");
        });

        // ================================================================================================
        // Issue 11: punctuation/special characters must not trigger a query-parser exception. multi_match's
        // query text is never parsed as Lucene syntax, so none of these may throw.
        // ================================================================================================
        for (String punctuationQuery : new String[] {"A:B", "A(B)", "A+B", "A\"B", "A?B", "A\\B", "customer:"}) {
            record("[Issue11] punctuation query does not throw (\"" + punctuationQuery + "\")", () -> {
                QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                        params(punctuationQuery, TYPE_DATASET, 10, 0, null));
                assertTrue(out.resultSize >= 0, "query must complete without throwing");
            });
        }

        // Explicit, user-typed mid-string wildcard (NOT the implicit trailing '*') must still be honored as a
        // genuine wildcard pattern (routed through query_string, not multi_match).
        record("[Issue11] explicit mid-string wildcard query matches (entity*ption)", () -> {
            QuickSearchOutcome out = quickSearch(graph, typeRegistry, indexFields,
                    params("entity*ption", TYPE_DATASET, 10, 0, null));
            assertTrue(containsGuid(out, LABEL_TARGET), "expected explicit mid-string wildcard to match ENTITYDESCRIPTION");
        });
    }

    // -------------------------------------------------------------------------------------------------------------
    // Fixture / harness (mirrors OpenSearchSearchWeightValidationDriver's pattern)
    // -------------------------------------------------------------------------------------------------------------

    private static void applyOversizedSearchWeights(AtlasGraph graph, IndexFieldNames indexFields)
            throws org.apache.atlas.AtlasException {
        Map<String, Integer> weights = new LinkedHashMap<>();

        // The real, queryable fields used by the validations above.
        weights.put(indexFields.nameIndexField, 10);
        weights.put(indexFields.descriptionIndexField, 10);
        weights.put(indexFields.ownerIndexField, 3);
        // Deliberately LOW boost, relative to the synthetic entries below -- proves the current design does not
        // silently drop low-weight fields regardless of how many higher-weight fields exist system-wide.
        weights.put(indexFields.lowWeightMarkerIndexField, LOW_WEIGHT_MARKER_BOOST);

        // Plus enough purely synthetic entries to push the system-wide weighted-field count well past OpenSearch/
        // Lucene's default maxClauseCount (1024) and past the superseded 900-field cap -- representing indexable
        // string attributes belonging to every OTHER registered type in a real deployment (SolrIndexHelper
        // computes this map system-wide, not scoped to the type being searched).
        for (int i = 0; i < SYNTHETIC_WEIGHTED_FIELD_COUNT; i++) {
            weights.put(String.format("other_type_%04d%sattr", i, "\u2022"), SYNTHETIC_BOOST);
        }

        graph.getGraphIndexClient().applySearchWeight(Constants.VERTEX_INDEX, weights);
    }

    private static QuickSearchOutcome quickSearch(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                                   IndexFieldNames indexFields,
                                                   QuickSearchParameters quickSearchParameters) throws AtlasBaseException {
        // Mirrors EntityDiscoveryService.quickSearch()'s implicit trailing-wildcard behavior exactly.
        String query = quickSearchParameters.getQuery();
        if (StringUtils.isNotEmpty(query) && !org.apache.atlas.type.AtlasStructType.AtlasAttribute.hastokenizeChar(query)) {
            query = query + "*";
        }
        quickSearchParameters.setQuery(query);

        SearchContext searchContext = buildSearchContext(graph, typeRegistry, indexFields, quickSearchParameters);
        SearchProcessor processor   = searchContext.getSearchProcessor();
        List<AtlasVertex> vertices  = processor.execute();

        QuickSearchOutcome outcome = new QuickSearchOutcome();
        outcome.resultSize = vertices.size();
        outcome.vertices    = vertices;
        return outcome;
    }

    private static boolean containsGuid(QuickSearchOutcome outcome, String label) {
        String guid = guidByLabel.get(label);
        return outcome.vertices.stream().anyMatch(v -> guid.equals(v.getProperty(Constants.GUID_PROPERTY_KEY, String.class)));
    }

    private static SearchContext buildSearchContext(AtlasGraph graph, AtlasTypeRegistry typeRegistry,
                                                     IndexFieldNames indexFields,
                                                     QuickSearchParameters quickSearchParameters) throws AtlasBaseException {
        SearchParameters searchParameters = EntityDiscoveryService.createSearchParameters(quickSearchParameters);
        return new SearchContext(searchParameters, typeRegistry, graph, buildIndexedKeys(indexFields));
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

    private static Map<String, String> insertBaselineEntities(JanusGraph graph, AtlasTypeRegistry typeRegistry) {
        Map<String, String> guids = new LinkedHashMap<>();

        guids.put(LABEL_TARGET, insertEntity(graph, typeRegistry, "customer-alpha", "team-target", "ENTITYDESCRIPTION",
                LOW_WEIGHT_MARKER_VALUE));
        guids.put(LABEL_OTHER, insertEntity(graph, typeRegistry, "unrelated-record", "team-other", "some other text",
                null));
        guids.put(LABEL_MULTIWORD, insertEntity(graph, typeRegistry, "multiword-record", "team-other",
                "alpha beta gamma delta", null));

        return guids;
    }

    private static String insertEntity(JanusGraph graph, AtlasTypeRegistry typeRegistry,
                                       String name, String owner, String description, String lowWeightMarker) {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(TYPE_DATASET);
        String guid = UUID.randomUUID().toString();
        Vertex v = graph.addVertex();

        v.property(Constants.GUID_PROPERTY_KEY, guid);
        v.property(Constants.ENTITY_TYPE_PROPERTY_KEY, TYPE_DATASET);
        v.property(Constants.STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());
        v.property(entityType.getAttribute("name").getVertexPropertyName(), name);
        v.property(entityType.getAttribute("owner").getVertexPropertyName(), owner);
        v.property(entityType.getAttribute("description").getVertexPropertyName(), description);

        if (StringUtils.isNotEmpty(lowWeightMarker)) {
            v.property(entityType.getAttribute("lowWeightMarker").getVertexPropertyName(), lowWeightMarker);
        }

        return guid;
    }

    private static IndexFieldNames createSchema(JanusGraph graph, AtlasTypeRegistry typeRegistry) {
        AtlasEntityType assetType             = typeRegistry.getEntityTypeByName(TYPE_ASSET);
        String nameProperty            = assetType.getAttribute("name").getVertexPropertyName();
        String ownerProperty           = assetType.getAttribute("owner").getVertexPropertyName();
        String descriptionProperty     = assetType.getAttribute("description").getVertexPropertyName();
        String lowWeightMarkerProperty = assetType.getAttribute("lowWeightMarker").getVertexPropertyName();

        JanusGraphManagement mgmt = graph.openManagement();

        PropertyKey guidKey = mgmt.makePropertyKey(Constants.GUID_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey typeKey = mgmt.makePropertyKey(Constants.ENTITY_TYPE_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey stateKey = mgmt.makePropertyKey(Constants.STATE_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey nameKey = mgmt.makePropertyKey(nameProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey ownerKey = mgmt.makePropertyKey(ownerProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey descriptionKey = mgmt.makePropertyKey(descriptionProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey lowWeightMarkerKey = mgmt.makePropertyKey(lowWeightMarkerProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();

        mgmt.buildIndex(OpenSearchQuickSearchSmokeSupport.VERTEX_INDEX, Vertex.class)
                .addKey(guidKey, Mapping.STRING.asParameter())
                .addKey(typeKey, Mapping.STRING.asParameter())
                .addKey(stateKey, Mapping.STRING.asParameter())
                .addKey(nameKey, Mapping.TEXT.asParameter())
                .addKey(ownerKey, Mapping.STRING.asParameter())
                .addKey(descriptionKey, Mapping.TEXT.asParameter())
                .addKey(lowWeightMarkerKey, Mapping.TEXT.asParameter())
                .buildMixedIndex(OpenSearchQuickSearchSmokeSupport.BACKING_INDEX_NAME);

        IndexFieldNames fields = new IndexFieldNames();
        fields.guidIndexField           = guidKey.name();
        fields.typeIndexField           = typeKey.name();
        fields.stateIndexField          = stateKey.name();
        fields.nameIndexField           = nameKey.name();
        fields.ownerIndexField          = ownerKey.name();
        fields.descriptionIndexField    = descriptionKey.name();
        fields.lowWeightMarkerIndexField = lowWeightMarkerKey.name();

        mgmt.commit();
        graph.tx().commit();

        return fields;
    }

    private static AtlasTypeRegistry buildTypeRegistry() throws AtlasBaseException {
        AtlasTypeRegistry registry = new AtlasTypeRegistry();

        AtlasEntityDef assetDef = new AtlasEntityDef();
        assetDef.setName(TYPE_ASSET);
        assetDef.setAttributeDefs(new ArrayList<>());
        assetDef.getAttributeDefs().add(attr("name", 10));
        assetDef.getAttributeDefs().add(attr("owner", 3));
        assetDef.getAttributeDefs().add(attr("description", 10));
        assetDef.getAttributeDefs().add(attr("lowWeightMarker", LOW_WEIGHT_MARKER_BOOST));

        AtlasEntityDef datasetDef = new AtlasEntityDef();
        datasetDef.setName(TYPE_DATASET);
        datasetDef.setSuperTypes(java.util.Collections.singleton(TYPE_ASSET));

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
        type.getAttribute("description").setIndexFieldName(fields.descriptionIndexField);
        type.getAttribute("lowWeightMarker").setIndexFieldName(fields.lowWeightMarkerIndexField);
    }

    private static Set<String> buildIndexedKeys(IndexFieldNames fields) {
        Set<String> keys = new java.util.LinkedHashSet<>();
        keys.add(fields.nameIndexField);
        keys.add(fields.ownerIndexField);
        keys.add(fields.descriptionIndexField);
        keys.add(fields.lowWeightMarkerIndexField);
        keys.add(fields.typeIndexField);
        keys.add(fields.stateIndexField);
        return keys;
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

    @FunctionalInterface
    private interface ValidationRunnable {
        void run() throws Exception;
    }

    private static final class QuickSearchOutcome {
        int resultSize;
        List<AtlasVertex> vertices;
    }

    private static final class IndexFieldNames {
        String guidIndexField;
        String typeIndexField;
        String stateIndexField;
        String nameIndexField;
        String ownerIndexField;
        String descriptionIndexField;
        String lowWeightMarkerIndexField;
    }
}
