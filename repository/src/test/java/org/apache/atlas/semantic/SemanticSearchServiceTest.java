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
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.discovery.SemanticSearchParameters;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class SemanticSearchServiceTest {
    private static final class StubSemanticStore implements SemanticVectorStore {
        @Override
        public void initialize() {
        }

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public void updateEmbedding(String guid, String text) {
        }

        @Override
        public List<VectorSearchHit> searchByText(String text, int topK, VectorSearchFilter filter) {
            return Collections.emptyList();
        }

        @Override
        public List<VectorSearchHit> searchByVector(List<?> vector, int topK, VectorSearchFilter filter) {
            return Collections.emptyList();
        }

        @Override
        public List<?> getStoredEmbedding(String guid) {
            return null;
        }

        @Override
        public void scanEntities(boolean missingEmbeddingOnly, int pageSize, Consumer<List<String>> onPage) {
        }
    }

    // once per class, before AtlasConfiguration is loaded: it keeps the Configuration instance it first sees
    @BeforeClass
    public void loadSemanticTestConfig() throws Exception {
        System.setProperty(ApplicationProperties.ATLAS_PROPERTIES_FILENAME_SYSTEM_CONF,
                "atlas-semantic-test-application.properties");
        ApplicationProperties.forceReload();
    }

    @AfterClass
    public void clearSemanticTestConfig() {
        System.clearProperty(ApplicationProperties.ATLAS_PROPERTIES_FILENAME_SYSTEM_CONF);
        ApplicationProperties.forceReload();
    }

    @Test
    public void semanticSearchRequiresEnableFlag() throws Exception {
        String enabledConf = AtlasConfiguration.SEMANTIC_ENABLED.getPropertyName();

        ApplicationProperties.get().setProperty(enabledConf, false);
        try {
            SemanticSearchService service = new SemanticSearchService(
                    new StubSemanticStore(),
                    null,
                    new AtlasTypeRegistry(),
                    null);

            SemanticSearchParameters params = new SemanticSearchParameters();
            params.setQuery("test");

            AtlasBaseException e = expectThrows(AtlasBaseException.class, () -> service.semanticSearch(params));
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.SEMANTIC_SEARCH_DISABLED);
        } finally {
            ApplicationProperties.get().setProperty(enabledConf, true);
        }
    }

    @Test
    public void semanticSearchRejectsUnknownType() {
        SemanticSearchService service = new SemanticSearchService(
                new StubSemanticStore(),
                null,
                new AtlasTypeRegistry(),
                null);

        SemanticSearchParameters params = new SemanticSearchParameters();
        params.setQuery("cars");
        params.setTypeName("no_such_type");

        AtlasBaseException e = expectThrows(AtlasBaseException.class, () -> service.semanticSearch(params));
        assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.UNKNOWN_TYPENAME);
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
