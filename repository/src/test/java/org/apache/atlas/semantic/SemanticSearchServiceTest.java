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
package org.apache.atlas.semantic;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.discovery.SemanticSearchParameters;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class SemanticSearchServiceTest {
    private static final class StubSemanticStore extends OpenSearchSemanticStore {
        @Override
        public List<VectorSearchHit> neuralSearch(String queryText, int topK, VectorSearchFilter filter) {
            return Collections.emptyList();
        }
    }

    @BeforeMethod
    public void loadSemanticTestConfig() throws Exception {
        System.setProperty(ApplicationProperties.ATLAS_PROPERTIES_FILENAME_SYSTEM_CONF,
                "atlas-semantic-test-application.properties");
        ApplicationProperties.forceReload();
    }

    @AfterMethod
    public void clearSemanticTestConfig() {
        System.clearProperty(ApplicationProperties.ATLAS_PROPERTIES_FILENAME_SYSTEM_CONF);
        ApplicationProperties.forceReload();
    }

    @Test
    public void semanticSearchRequiresEnableFlag() {
        System.setProperty(ApplicationProperties.ATLAS_PROPERTIES_FILENAME_SYSTEM_CONF,
                "atlas-application.properties");
        ApplicationProperties.forceReload();

        SemanticSearchService service = new SemanticSearchService(
                new StubSemanticStore(),
                null,
                new AtlasTypeRegistry(),
                null);

        SemanticSearchParameters params = new SemanticSearchParameters();
        params.setQuery("test");

        expectThrows(Exception.class, () -> service.semanticSearch(params));
    }

    @Test
    public void buildSearchResultUsesSemanticQueryType() throws Exception {
        SemanticSearchService service = new SemanticSearchService(
                new StubSemanticStore(),
                null,
                new AtlasTypeRegistry(),
                null);

        SemanticSearchParameters params = new SemanticSearchParameters();
        params.setQuery("cars");

        AtlasSearchResult result = service.semanticSearch(params);
        assertEquals(result.getQueryType(), AtlasQueryType.SEMANTIC);
    }
}
