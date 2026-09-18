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

import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.discovery.SemanticSearchParameters;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

public class SemanticSearchServiceTest {
    @Test
    public void semanticSearchRequiresEnableFlag() {
        SemanticSearchService service = new SemanticSearchService(
                mock(OpenSearchSemanticStore.class),
                mock(AtlasGraph.class),
                mock(AtlasTypeRegistry.class),
                mock(SemanticTextBuilder.class));

        SemanticSearchParameters params = new SemanticSearchParameters();
        params.setQuery("test");

        expectThrows(Exception.class, () -> service.semanticSearch(params));
    }

    @Test
    public void buildSearchResultUsesSemanticQueryType() throws Exception {
        OpenSearchSemanticStore store = mock(OpenSearchSemanticStore.class);
        when(store.neuralSearch(anyString(), anyInt(), any())).thenReturn(Collections.emptyList());

        SemanticSearchService service = new SemanticSearchService(
                store,
                mock(AtlasGraph.class),
                mock(AtlasTypeRegistry.class),
                mock(SemanticTextBuilder.class));

        SemanticSearchParameters params = new SemanticSearchParameters();
        params.setQuery("cars");

        try {
            System.setProperty("atlas.search.semantic.enable", "true");
            System.setProperty("atlas.search.semantic.opensearch.model.id", "test-model");
            System.setProperty("atlas.search.semantic.opensearch.embedding.dimension", "384");
            org.apache.atlas.ApplicationProperties.forceReload();

            AtlasSearchResult result = service.semanticSearch(params);
            assertEquals(result.getQueryType(), AtlasQueryType.SEMANTIC);
        } finally {
            System.clearProperty("atlas.search.semantic.enable");
            System.clearProperty("atlas.search.semantic.opensearch.model.id");
            System.clearProperty("atlas.search.semantic.opensearch.embedding.dimension");
            org.apache.atlas.ApplicationProperties.forceReload();
        }
    }
}
