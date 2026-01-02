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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.impexp.AtlasExportRequest.MATCH_TYPE_FOR_TYPE;
import static org.apache.atlas.repository.impexp.StartEntityFetchByExportRequest.BINDING_PARAMETER_ATTR_NAME;
import static org.apache.atlas.repository.impexp.StartEntityFetchByExportRequest.BINDING_PARAMETER_TYPENAME;
import static org.apache.atlas.repository.impexp.StartEntityFetchByExportRequest.BINDING_PARAMTER_ATTR_VALUE;
import static org.testng.Assert.assertEquals;
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

        startEntityFetchByExportRequestSpy.get(exportRequest);

        assertEquals(startEntityFetchByExportRequestSpy.getGeneratedQuery(), startEntityFetchByExportRequestSpy.getQueryTemplateForMatchType(exportRequest.getMatchTypeOptionValue()));
        assertEquals(startEntityFetchByExportRequestSpy.getSuppliedBindingsMap().get(BINDING_PARAMETER_TYPENAME), "hive_db");
        assertEquals(startEntityFetchByExportRequestSpy.getSuppliedBindingsMap().get(BINDING_PARAMETER_ATTR_NAME), "Referenceable.qualifiedName");
        assertEquals(startEntityFetchByExportRequestSpy.getSuppliedBindingsMap().get(BINDING_PARAMTER_ATTR_VALUE), "stocks@cl1");
    }

    @Test
    public void fetchReferenceableUniqueAttributes() {
        String             exportRequestJson = "{ \"itemsToExport\": [ { \"typeName\": \"Referenceable\", \"uniqueAttributes\": {\"qualifiedName\": \"stocks@cl1\"} } ]}";
        AtlasExportRequest exportRequest     = AtlasType.fromJson(exportRequestJson, AtlasExportRequest.class);

        startEntityFetchByExportRequestSpy.get(exportRequest);

        assertEquals(startEntityFetchByExportRequestSpy.getGeneratedQuery(), startEntityFetchByExportRequestSpy.getQueryTemplateForMatchType(MATCH_TYPE_FOR_TYPE));

        Object typeNameBinding = startEntityFetchByExportRequestSpy.getSuppliedBindingsMap().get(BINDING_PARAMETER_TYPENAME);
        assertTrue(typeNameBinding instanceof Set);

        Set<String> boundTypes = (Set<String>) typeNameBinding;
        assertTrue(boundTypes.size() > 1);
        assertTrue(boundTypes.contains("hive_table"));
    }

    @Test
    public void fetchTypeExpansion() {
        String             exportRequestJson = "{ \"itemsToExport\": [ { \"typeName\": \"Asset\" } ], \"options\": {\"matchType\": \"forType\"} }";
        AtlasExportRequest exportRequest     = AtlasType.fromJson(exportRequestJson, AtlasExportRequest.class);

        startEntityFetchByExportRequestSpy.get(exportRequest);

        Set<String> boundTypes = (Set<String>) startEntityFetchByExportRequestSpy.getSuppliedBindingsMap().get(BINDING_PARAMETER_TYPENAME);
        assertTrue(boundTypes.contains("Asset"));
        assertTrue(boundTypes.contains("hive_db"));
    }

    @Test
    public void fetchEmptyTypeUniqueAttributes() {
        String             exportRequestJson = "{ \"itemsToExport\": [ { \"typeName\": \"\", \"uniqueAttributes\": {\"qualifiedName\": \"stocks@cl1\"} } ]}";
        AtlasExportRequest exportRequest     = AtlasType.fromJson(exportRequestJson, AtlasExportRequest.class);

        startEntityFetchByExportRequestSpy.get(exportRequest);

        Set<String> boundTypes = (Set<String>) startEntityFetchByExportRequestSpy.getSuppliedBindingsMap().get(BINDING_PARAMETER_TYPENAME);
        assertEquals(boundTypes.size(), typeRegistry.getAllEntityDefNames().size());
    }

    @Test
    public void fetchUnknownType() {
        String             exportRequestJson = "{ \"itemsToExport\": [ { \"typeName\": \"InvalidType\" } ]}";
        AtlasExportRequest exportRequest     = AtlasType.fromJson(exportRequestJson, AtlasExportRequest.class);

        List<AtlasObjectId> result = startEntityFetchByExportRequestSpy.get(exportRequest);
        assertTrue(result.isEmpty());
    }

    @BeforeClass
    void setup() throws IOException, AtlasBaseException {
        super.basicSetup(typeDefStore, typeRegistry);

        atlasGremlin3QueryProvider         = new AtlasGremlin3QueryProvider();
        startEntityFetchByExportRequestSpy = new StartEntityFetchByExportRequestSpy(atlasGraph, typeRegistry);
    }

    private class StartEntityFetchByExportRequestSpy extends StartEntityFetchByExportRequest {
        String              generatedQuery;
        Map<String, Object> suppliedBindingsMap;

        public StartEntityFetchByExportRequestSpy(AtlasGraph atlasGraph, AtlasTypeRegistry typeRegistry) {
            super(atlasGraph, typeRegistry, atlasGremlin3QueryProvider);
        }

        public String getGeneratedQuery() {
            return generatedQuery;
        }

        public Map<String, Object> getSuppliedBindingsMap() {
            return suppliedBindingsMap;
        }

        @Override
        List<String> executeGremlinQuery(String query, Map<String, Object> bindings) {
            this.generatedQuery      = query;
            this.suppliedBindingsMap = bindings;

            return Collections.emptyList();
        }
    }
}