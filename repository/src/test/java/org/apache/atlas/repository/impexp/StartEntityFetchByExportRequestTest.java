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

package org.apache.atlas.repository.impexp;

import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.repository.AtlasTestBase;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasGremlin3QueryProvider;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.impexp.StartEntityFetchByExportRequest.BINDING_PARAMETER_ATTR_NAME;
import static org.apache.atlas.repository.impexp.StartEntityFetchByExportRequest.BINDING_PARAMETER_TYPENAME;
import static org.apache.atlas.repository.impexp.StartEntityFetchByExportRequest.BINDING_PARAMTER_ATTR_VALUE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class StartEntityFetchByExportRequestTest extends AtlasTestBase {
    @Inject
    private AtlasGraph atlasGraph;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private AtlasTypeDefStore typeDefStore;

    private AtlasGremlin3QueryProvider         atlasGremlin3QueryProvider;
    private StartEntityFetchByExportRequestSpy startEntityFetchByExportRequestSpy;

    @BeforeClass
    void setup() throws IOException, AtlasBaseException {
        super.basicSetup(typeDefStore, typeRegistry);

        atlasGremlin3QueryProvider         = new AtlasGremlin3QueryProvider();
        startEntityFetchByExportRequestSpy = new StartEntityFetchByExportRequestSpy(atlasGraph, typeRegistry);
    }

    @Test
    public void fetchTypeGuid() {
        String              exportRequestJson = "{ \"itemsToExport\": [ { \"typeName\": \"hive_db\", \"guid\": \"111-222-333\" } ]}";
        AtlasExportRequest  exportRequest     = AtlasType.fromJson(exportRequestJson, AtlasExportRequest.class);
        List<AtlasObjectId> objectGuidMap     = startEntityFetchByExportRequestSpy.get(exportRequest);

        assertEquals(objectGuidMap.get(0).getGuid(), "111-222-333");
    }

    @Test
    public void fetchTypeUniqueAttributes() {
        String             exportRequestJson = "{ \"itemsToExport\": [ { \"typeName\": \"hive_db\", \"uniqueAttributes\": {\"qualifiedName\": \"stocks@cl1\"} } ]}";
        AtlasExportRequest exportRequest     = AtlasType.fromJson(exportRequestJson, AtlasExportRequest.class);

        startEntityFetchByExportRequestSpy.clearRecordedCalls();
        startEntityFetchByExportRequestSpy.get(exportRequest);

        assertFalse(startEntityFetchByExportRequestSpy.getRecordedBindings().isEmpty());

        boolean foundHiveDbSearch = startEntityFetchByExportRequestSpy.getRecordedBindings().stream()
                .anyMatch(b -> b.get(BINDING_PARAMETER_TYPENAME).equals("hive_db") &&
                        b.get(BINDING_PARAMETER_ATTR_NAME).equals("Referenceable.qualifiedName") &&
                        b.get(BINDING_PARAMTER_ATTR_VALUE).equals("stocks@cl1"));

        assertTrue(foundHiveDbSearch, "Should have searched for hive_db specifically");
    }

    @Test
    public void fetchReferenceableUniqueAttributes() {
        String             exportRequestJson = "{ \"itemsToExport\": [ { \"typeName\": \"Referenceable\", \"uniqueAttributes\": {\"qualifiedName\": \"stocks@cl1\"} } ]}";
        AtlasExportRequest exportRequest     = AtlasType.fromJson(exportRequestJson, AtlasExportRequest.class);

        startEntityFetchByExportRequestSpy.clearRecordedCalls();
        startEntityFetchByExportRequestSpy.get(exportRequest);

        List<Map<String, Object>> allBindings = startEntityFetchByExportRequestSpy.getRecordedBindings();

        assertTrue(allBindings.size() > 1, "Should have triggered multiple queries for Referenceable subtypes");

        boolean foundHiveTable = allBindings.stream()
                .anyMatch(b -> b.get(BINDING_PARAMETER_TYPENAME).equals("hive_table"));

        assertTrue(foundHiveTable, "Search should have included subtype: hive_table");
    }

    @Test
    public void fetchTypeExpansion() {
        String             exportRequestJson = "{ \"itemsToExport\": [ { \"typeName\": \"Asset\" } ], \"options\": {\"matchType\": \"forType\"} }";
        AtlasExportRequest exportRequest     = AtlasType.fromJson(exportRequestJson, AtlasExportRequest.class);

        startEntityFetchByExportRequestSpy.clearRecordedCalls();
        startEntityFetchByExportRequestSpy.get(exportRequest);

        Map<String, Object> lastBindings = startEntityFetchByExportRequestSpy.getSuppliedBindingsMap();
        Set<String> boundTypes = (Set<String>) lastBindings.get(BINDING_PARAMETER_TYPENAME);

        assertTrue(boundTypes.contains("Asset"));
        assertTrue(boundTypes.contains("hive_db")); // Asset is a supertype of hive_db
    }

    @Test
    public void fetchEmptyTypeUniqueAttributes() throws AtlasBaseException {
        String             exportRequestJson = "{ \"itemsToExport\": [ { \"typeName\": \"\", \"uniqueAttributes\": {\"qualifiedName\": \"stocks@cl1\"} } ]}";
        AtlasExportRequest exportRequest     = AtlasType.fromJson(exportRequestJson, AtlasExportRequest.class);

        startEntityFetchByExportRequestSpy.clearRecordedCalls();
        startEntityFetchByExportRequestSpy.get(exportRequest);

        Map<String, Object> lastBindings = startEntityFetchByExportRequestSpy.getSuppliedBindingsMap();

        assertTrue(lastBindings != null && lastBindings.containsKey(BINDING_PARAMETER_TYPENAME), "Bindings should contain typeName");

        Set<String> boundTypes = (Set<String>) lastBindings.get(BINDING_PARAMETER_TYPENAME);
        int expectedSize = typeRegistry.getAllEntityDefNames().size();

        assertEquals(boundTypes.size(), expectedSize, "Should contain all entity types from registry");
    }

    @Test
    public void fetchUnknownType() {
        String             exportRequestJson = "{ \"itemsToExport\": [ { \"typeName\": \"InvalidType\" } ]}";
        AtlasExportRequest exportRequest     = AtlasType.fromJson(exportRequestJson, AtlasExportRequest.class);

        List<AtlasObjectId> result = startEntityFetchByExportRequestSpy.get(exportRequest);
        assertTrue(result.isEmpty());
    }

    /**
     * Spy class to capture Gremlin queries and bindings.
     * to record multiple calls because the new implementation uses loops.
     */
    private class StartEntityFetchByExportRequestSpy extends StartEntityFetchByExportRequest {
        private String                    lastQuery;
        private Map<String, Object>       lastBindings;
        private List<Map<String, Object>> recordedBindings = new ArrayList<>();

        public StartEntityFetchByExportRequestSpy(AtlasGraph atlasGraph, AtlasTypeRegistry typeRegistry) {
            super(atlasGraph, typeRegistry, atlasGremlin3QueryProvider);
        }

        public String getGeneratedQuery() {
            return lastQuery;
        }

        public Map<String, Object> getSuppliedBindingsMap() {
            return lastBindings;
        }

        public List<Map<String, Object>> getRecordedBindings() {
            return recordedBindings;
        }

        public void clearRecordedCalls() {
            recordedBindings.clear();
            lastQuery = null;
            lastBindings = null;
        }

        @Override
        List<String> executeGremlinQuery(String query, Map<String, Object> bindings) {
            this.lastQuery    = query;
            this.lastBindings = bindings;
            this.recordedBindings.add(new java.util.HashMap<>(bindings));

            return Collections.emptyList();
        }
    }
}
