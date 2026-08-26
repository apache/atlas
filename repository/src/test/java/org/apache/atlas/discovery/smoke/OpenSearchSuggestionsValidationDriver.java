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

import org.apache.atlas.discovery.SuggestionsProviderImpl;
import org.apache.atlas.model.discovery.AtlasSuggestionsResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndexClient;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasJanusGraph;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.Mapping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * C5.3 validation: Atlas suggestions through {@link SuggestionsProviderImpl} and
 * {@link AtlasJanusGraphIndexClient#getSuggestions(String, String)} against OpenSearch 2.x.
 *
 * <pre>
 *   cd repository && mvn test-compile exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=org.apache.atlas.discovery.smoke.OpenSearchSuggestionsValidationDriver \
 *     -Drat.skip=true -Dcheckstyle.skip=true -Dsortpom.skip=true -DskipCheck=true
 * </pre>
 */
public final class OpenSearchSuggestionsValidationDriver {

    private static final String TYPE_DATASET = "c53_dataset";
    private static final String TYPE_TABLE   = "c53_table";
    private static final String TYPE_ASSET     = "c53_asset";

    private static final Map<String, String> RESULTS = new LinkedHashMap<>();

    private static final int ATLAS_SUGGESTION_LIMIT = 5;

    private OpenSearchSuggestionsValidationDriver() {
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
        SuggestionsProviderImpl suggestionsProvider = new SuggestionsProviderImpl(graph, typeRegistry);

        indexClient.applySuggestionFields(Constants.VERTEX_INDEX,
                Arrays.asList(indexFields.ownerIndexField, indexFields.nameIndexField));

        runValidations(indexClient, suggestionsProvider, indexFields);

        graph.shutdown();

        return RESULTS.values().stream().allMatch("PASS"::equals);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("C5.3 Atlas Suggestions Validation");
        boolean allPassed = execute();
        for (Map.Entry<String, String> entry : RESULTS.entrySet()) {
            System.out.printf("%-28s %s%n", entry.getKey(), entry.getValue());
        }
        System.out.println(allPassed ? "C5.3 RESULT: PASS" : "C5.3 RESULT: FAIL");
        if (!allPassed) {
            System.exit(1);
        }
    }

    private static void runValidations(AtlasGraphIndexClient indexClient, SuggestionsProviderImpl suggestionsProvider,
                                       IndexFieldNames indexFields) throws Exception {
        record("Basic prefix", () -> {
            List<String> suggestions = indexClient.getSuggestions("team", indexFields.ownerIndexField);
            assertTrue(!suggestions.isEmpty(), "expected suggestions for prefix team");
            assertTrue(suggestions.stream().anyMatch(s -> s.startsWith("team-")), "expected team-* terms");
        });

        record("No matching prefix", () -> {
            List<String> suggestions = indexClient.getSuggestions("zzz-no-match", indexFields.ownerIndexField);
            assertTrue(suggestions.isEmpty(), "expected empty list, got " + suggestions);
        });

        record("Multiple matching terms", () -> {
            List<String> suggestions = indexClient.getSuggestions("team", indexFields.ownerIndexField);
            assertTrue(suggestions.size() >= 3, "expected at least 3 team-* terms, got " + suggestions.size());
        });

        record("Frequency ordering", () -> {
            List<String> suggestions = indexClient.getSuggestions("team", indexFields.ownerIndexField);
            assertTrue("team-alpha".equals(suggestions.get(0)),
                    "team-alpha (3 docs) should rank first, got " + suggestions);
        });

        record("Maximum results", () -> {
            List<String> suggestions = indexClient.getSuggestions("rank", indexFields.ownerIndexField);
            assertEquals(suggestions.size(), ATLAS_SUGGESTION_LIMIT,
                    "expected max " + ATLAS_SUGGESTION_LIMIT + " suggestions");
        });

        record("Suggestion field", () -> {
            List<String> ownerSuggestions = indexClient.getSuggestions("team", indexFields.ownerIndexField);
            List<String> nameSuggestions  = indexClient.getSuggestions("Alice", indexFields.nameIndexField);
            assertTrue(!ownerSuggestions.isEmpty(), "owner field should return suggestions");
            assertTrue(!nameSuggestions.isEmpty(), "name field should return suggestions");
            assertTrue(ownerSuggestions.stream().noneMatch(s -> s.startsWith("Alice")),
                    "owner suggestions should not contain name values");
        });

        record("Multiple entity types", () -> {
            List<String> suggestions = indexClient.getSuggestions("team", indexFields.ownerIndexField);
            assertTrue(suggestions.size() >= 3,
                    "team-* owners should appear across dataset and table entities, got " + suggestions);
        });

        record("Case behavior", () -> {
            List<String> lower = indexClient.getSuggestions("team", indexFields.ownerIndexField);
            List<String> upper = indexClient.getSuggestions("Team", indexFields.ownerIndexField);
            assertTrue(!lower.isEmpty(), "lowercase prefix should match keyword owner values");
            // Keyword fields are case-sensitive; Team-* owner exists but team-* prefix should not match it.
            assertTrue(upper.stream().noneMatch("team-alpha"::equals),
                    "uppercase prefix should not return lowercase-only values on keyword field");
        });

        record("High-weight suggestion field", () -> {
            List<String> suggestions = indexClient.getSuggestions("team", null);
            assertTrue(!suggestions.isEmpty(), "configured suggestion fields should return results without explicit field");
            assertTrue(suggestions.stream().anyMatch(s -> s.startsWith("team-")),
                    "should include owner-field team-* terms, got " + suggestions);
        });

        record("Explicit field only", () -> {
            List<String> ownerOnly = indexClient.getSuggestions("Alice", indexFields.ownerIndexField);
            List<String> nameOnly  = indexClient.getSuggestions("Alice", indexFields.nameIndexField);
            assertTrue(ownerOnly.isEmpty(), "owner field should not match Alice name values");
            assertTrue(!nameOnly.isEmpty(), "name field should match Alice values");
        });

        record("Cross-field frequency merge", () -> {
            List<String> suggestions = indexClient.getSuggestions("customer", null);
            assertTrue("customer".equals(suggestions.get(0)),
                    "merged customer frequency should rank first, got " + suggestions);
            assertTrue(suggestions.contains("customer_data"),
                    "customer_data should appear in merged suggestions");
        });

        record("Deduplication", () -> {
            List<String> suggestions = indexClient.getSuggestions("customer", null);
            long customerCount = suggestions.stream().filter("customer"::equals).count();
            assertEquals((int) customerCount, 1, "customer should appear once after merge");
        });

        record("Special character prefixes", () -> {
            for (String prefix : Arrays.asList("cust-", "cust_", "cust.", "cust+", "cust*", "cust?", "cust(", "cust[", "cust\\")) {
                List<String> suggestions = indexClient.getSuggestions(prefix, indexFields.ownerIndexField);
                assertTrue(suggestions != null, "prefix " + prefix + " should not cause errors");
            }
            List<String> dashMatch = indexClient.getSuggestions("cust-", indexFields.ownerIndexField);
            assertTrue(dashMatch.stream().anyMatch(s -> s.equals("cust-special")),
                    "cust- prefix should match cust-special, got " + dashMatch);
        });

        record("Exclude deleted entities", () -> {
            List<String> suggestions = indexClient.getSuggestions("deleted-team", indexFields.ownerIndexField);
            assertTrue(suggestions.isEmpty(),
                    "DELETED entity terms should be excluded, got " + suggestions);
        });

        record("Single request latency", () -> {
            long start = System.nanoTime();
            List<String> suggestions = indexClient.getSuggestions("team", null);
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            assertTrue(!suggestions.isEmpty(), "multi-field suggestions should return results");
            System.out.printf("  [perf] multi-field suggestions (2 fields, 1 request): %d ms, %d results%n",
                    elapsedMs, suggestions.size());
        });

        record("Low-weight field", () -> {
            List<String> suggestions = indexClient.getSuggestions("c53", null);
            assertTrue(suggestions.isEmpty(),
                    "typeName values should be excluded from in-memory suggestion fields, got " + suggestions);
        });

        record("Existing index", () -> {
            AtlasSuggestionsResult result = suggestionsProvider.getSuggestions("team", "owner");
            assertTrue(result.getSuggestions() != null && !result.getSuggestions().isEmpty(),
                    "SuggestionsProvider path should work on existing index");
        });

        record("Provider API shape", () -> {
            AtlasSuggestionsResult result = suggestionsProvider.getSuggestions("team", "owner");
            assertTrue("team".equals(result.getPrefixString()), "prefixString preserved");
            assertTrue("owner".equals(result.getFieldName()), "fieldName preserved");
            assertTrue(result.getSuggestions().size() <= ATLAS_SUGGESTION_LIMIT,
                    "response respects Atlas suggestion limit");
        });
    }

    // -------------------------------------------------------------------------
    // Fixture setup (extends C5.2 pattern; STRING mapping on name for Solr parity)
    // -------------------------------------------------------------------------

    private static IndexFieldNames createSchema(JanusGraph graph, AtlasTypeRegistry typeRegistry) throws Exception {
        AtlasEntityType assetType = typeRegistry.getEntityTypeByName(TYPE_ASSET);
        String nameProperty  = assetType.getAttribute("name").getVertexPropertyName();
        String ownerProperty = assetType.getAttribute("owner").getVertexPropertyName();

        JanusGraphManagement mgmt = graph.openManagement();

        PropertyKey guidKey  = mgmt.makePropertyKey(Constants.GUID_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey typeKey  = mgmt.makePropertyKey(Constants.ENTITY_TYPE_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey stateKey = mgmt.makePropertyKey(Constants.STATE_PROPERTY_KEY).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey nameKey  = mgmt.makePropertyKey(nameProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();
        PropertyKey ownerKey = mgmt.makePropertyKey(ownerProperty).dataType(String.class)
                .cardinality(org.janusgraph.core.Cardinality.SINGLE).make();

        mgmt.buildIndex(OpenSearchQuickSearchSmokeSupport.VERTEX_INDEX, Vertex.class)
                .addKey(guidKey, Mapping.STRING.asParameter())
                .addKey(typeKey, Mapping.STRING.asParameter())
                .addKey(stateKey, Mapping.STRING.asParameter())
                .addKey(nameKey, Mapping.STRING.asParameter())
                .addKey(ownerKey, Mapping.STRING.asParameter())
                .buildMixedIndex(OpenSearchQuickSearchSmokeSupport.BACKING_INDEX_NAME);

        IndexFieldNames fields = new IndexFieldNames();
        fields.guidIndexField  = guidKey.name();
        fields.typeIndexField  = typeKey.name();
        fields.stateIndexField = stateKey.name();
        fields.nameIndexField  = nameKey.name();
        fields.ownerIndexField = ownerKey.name();

        mgmt.commit();
        graph.tx().commit();
        return fields;
    }

    private static AtlasTypeRegistry buildTypeRegistry(IndexFieldNames indexFields) throws Exception {
        AtlasTypeRegistry registry = new AtlasTypeRegistry();

        AtlasEntityDef assetDef   = entityDef(TYPE_ASSET, Collections.emptySet(), true);
        AtlasEntityDef datasetDef = entityDef(TYPE_DATASET, Collections.singleton(TYPE_ASSET), false);
        AtlasEntityDef tableDef   = entityDef(TYPE_TABLE, Collections.singleton(TYPE_ASSET), false);

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

        registry.addIndexFieldName("owner", indexFields.ownerIndexField);
        registry.addIndexFieldName("name", indexFields.nameIndexField);
    }

    private static void wireType(AtlasEntityType type, IndexFieldNames indexFields) {
        type.getAttribute("name").setIndexFieldName(indexFields.nameIndexField);
        type.getAttribute("owner").setIndexFieldName(indexFields.ownerIndexField);
    }

    private static AtlasEntityDef entityDef(String typeName, java.util.Set<String> superTypes, boolean includeIndexedAttrs) {
        AtlasEntityDef def = new AtlasEntityDef();
        def.setName(typeName);
        def.setSuperTypes(superTypes);

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
        insertEntity(graph, typeRegistry, TYPE_DATASET, "merge-foo-1", "customer");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "merge-foo-2", "customer");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "merge-foo-3", "customer");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "customer", "merge-owner-1");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "customer", "merge-owner-2");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "customer_data report", "customer_data");
        insertEntity(graph, typeRegistry, TYPE_DATASET, "special chars", "cust-special");

        insertEntity(graph, typeRegistry, TYPE_DATASET, "deleted entity", "deleted-team-x",
                AtlasEntity.Status.DELETED);

        for (int i = 1; i <= 9; i++) {
            insertEntity(graph, typeRegistry, TYPE_DATASET,
                    "Rank Entity " + i, String.format("rank-%02d", i));
        }
    }

    private static void insertEntity(AtlasGraph graph, AtlasTypeRegistry typeRegistry, String typeName,
                                     String name, String owner) throws Exception {
        insertEntity(graph, typeRegistry, typeName, name, owner, AtlasEntity.Status.ACTIVE);
    }

    private static void insertEntity(AtlasGraph graph, AtlasTypeRegistry typeRegistry, String typeName,
                                     String name, String owner, AtlasEntity.Status status) throws Exception {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        AtlasVertex vertex = graph.addVertex();
        vertex.setProperty(Constants.GUID_PROPERTY_KEY, UUID.randomUUID().toString());
        vertex.setProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, typeName);
        vertex.setProperty(Constants.STATE_PROPERTY_KEY, status.name());
        vertex.setProperty(entityType.getAttribute("name").getVertexPropertyName(), name);
        vertex.setProperty(entityType.getAttribute("owner").getVertexPropertyName(), owner);
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
