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
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.discovery.SemanticSearchParameters;
import org.apache.atlas.model.discovery.SimilarEntitySearchParameters;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;

public class SemanticSearchServiceTest {
    private MockedStatic<AtlasGraphUtilsV2>          graphUtils;
    private MockedStatic<AtlasAuthorizationUtils>    authorization;
    private MockedConstruction<EntityGraphRetriever> retrievers;
    private SemanticVectorStore                      semanticStore;
    private SemanticTextBuilder                      textBuilder;
    private EntityGraphRetriever                     entityRetriever;
    private SemanticSearchService                    service;

    @BeforeClass
    public void enableSemanticSearch() throws Exception {
        ApplicationProperties.get().setProperty(AtlasConfiguration.SEMANTIC_ENABLED.getPropertyName(), true);
    }

    @AfterClass
    public void resetSemanticSearch() throws Exception {
        ApplicationProperties.get().clearProperty(AtlasConfiguration.SEMANTIC_ENABLED.getPropertyName());
    }

    @BeforeMethod
    public void setUp() {
        graphUtils    = mockStatic(AtlasGraphUtilsV2.class);
        authorization = mockStatic(AtlasAuthorizationUtils.class);
        retrievers    = mockConstruction(EntityGraphRetriever.class);
        semanticStore = mock(SemanticVectorStore.class);
        textBuilder   = mock(SemanticTextBuilder.class);
        service       = new SemanticSearchService(semanticStore, null, new AtlasTypeRegistry(), textBuilder);

        entityRetriever = retrievers.constructed().get(0);
    }

    @AfterMethod
    public void tearDown() {
        retrievers.close();
        authorization.close();
        graphUtils.close();
    }

    @Test
    public void semanticSearchRequiresEnableFlag() throws Exception {
        String enabledConf = AtlasConfiguration.SEMANTIC_ENABLED.getPropertyName();

        ApplicationProperties.get().setProperty(enabledConf, false);
        try {
            AtlasBaseException e = expectThrows(AtlasBaseException.class, () -> service.semanticSearch(query("test")));
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.SEMANTIC_SEARCH_DISABLED);
        } finally {
            ApplicationProperties.get().setProperty(enabledConf, true);
        }
    }

    @Test
    public void semanticSearchRejectsUnknownType() {
        SemanticSearchParameters params = query("cars");
        params.setTypeName("no_such_type");

        AtlasBaseException e = expectThrows(AtlasBaseException.class, () -> service.semanticSearch(params));
        assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.UNKNOWN_TYPENAME);
    }

    @Test
    public void semanticSearchKeepsHitsAboveMinScoreAndSkipsMissingAndDeletedEntities() throws Exception {
        entity("g1", AtlasEntity.Status.ACTIVE);
        entity("g3", AtlasEntity.Status.DELETED);
        entity("g4", AtlasEntity.Status.ACTIVE);
        entity("g5", AtlasEntity.Status.ACTIVE);
        when(semanticStore.searchByText(eq("cars"), anyInt(), any())).thenReturn(Arrays.asList(
                new VectorSearchHit("g1", 0.9), new VectorSearchHit("g2-missing", 0.8), new VectorSearchHit("g3", 0.7),
                new VectorSearchHit("g4", 0.6), new VectorSearchHit("g5", 0.2)));

        SemanticSearchParameters params = query("cars");
        params.setTopK(10);
        params.setMinScore(0.5);
        AtlasSearchResult result = service.semanticSearch(params);

        assertEquals(result.getQueryType(), AtlasQueryType.SEMANTIC);
        assertEquals(guids(result), Arrays.asList("g1", "g4"));
        assertEquals(result.getFullTextResult().get(0).getScore(), 0.9, 0.0);
        verify(semanticStore).searchByText(eq("cars"), eq(20), any());
    }

    @Test
    public void semanticSearchReturnsAtMostTopKAndCanIncludeDeletedEntities() throws Exception {
        entity("g1", AtlasEntity.Status.DELETED);
        entity("g2", AtlasEntity.Status.ACTIVE);
        when(semanticStore.searchByText(eq("cars"), anyInt(), any())).thenReturn(Arrays.asList(
                new VectorSearchHit("g1", 0.9), new VectorSearchHit("g2", 0.8)));

        SemanticSearchParameters params = query("cars");
        params.setTopK(1);
        params.setExcludeDeletedEntities(false);

        assertEquals(guids(service.semanticSearch(params)), Collections.singletonList("g1"));
    }

    @Test
    public void semanticSearchRejectsTopKAboveMaximum() {
        SemanticSearchParameters params = query("cars");
        params.setTopK(101);

        AtlasBaseException e = expectThrows(AtlasBaseException.class, () -> service.semanticSearch(params));
        assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
    }

    @Test
    public void semanticSearchMapsIndexNotReadyTo503() throws Exception {
        when(semanticStore.searchByText(anyString(), anyInt(), any()))
                .thenThrow(new SemanticSearchException(AtlasErrorCode.SEMANTIC_SEARCH_NOT_READY, "the embedding field is not set up yet"));

        AtlasBaseException e = expectThrows(AtlasBaseException.class, () -> service.semanticSearch(query("cars")));
        assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.SEMANTIC_SEARCH_NOT_READY);
    }

    @Test
    public void semanticSearchHidesBackendErrorDetails() throws Exception {
        when(semanticStore.searchByText(anyString(), anyInt(), any()))
                .thenThrow(new SemanticSearchException("HTTP request failed with status 500: index janusgraph_vertex_index ...", false));

        AtlasBaseException e = expectThrows(AtlasBaseException.class, () -> service.semanticSearch(query("cars")));
        assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.DISCOVERY_QUERY_FAILED);
        assertFalse(e.getMessage().contains("janusgraph_vertex_index"));
    }

    @Test
    public void similarEntitiesSearchesByStoredEmbeddingExcludingTheSourceEntity() throws Exception {
        sourceEntity("src");
        entity("g1", AtlasEntity.Status.ACTIVE);
        List<Double> embedding = Arrays.asList(0.1, 0.2);
        doReturn(embedding).when(semanticStore).getStoredEmbedding("src");
        when(semanticStore.searchByVector(eq(embedding), anyInt(), any())).thenReturn(Collections.singletonList(new VectorSearchHit("g1", 0.8)));

        AtlasSearchResult result = service.similarEntities("src", new SimilarEntitySearchParameters());

        assertEquals(guids(result), Collections.singletonList("g1"));
        verify(semanticStore).searchByVector(eq(embedding), anyInt(), argThat(f -> f.getExcludeGuids().contains("src")));
        verify(semanticStore, never()).searchByText(anyString(), anyInt(), any());
    }

    @Test
    public void similarEntitiesFallsBackToTheEntityTextWithoutStoredEmbedding() throws Exception {
        AtlasVertex source = sourceEntity("src");
        when(textBuilder.buildText(source)).thenReturn("orders table");
        when(semanticStore.searchByText(eq("orders table"), anyInt(), any())).thenReturn(Collections.emptyList());

        service.similarEntities("src", new SimilarEntitySearchParameters());

        verify(semanticStore).searchByText(eq("orders table"), anyInt(), any());
        verify(semanticStore, never()).searchByVector(anyList(), anyInt(), any());
    }

    @Test
    public void similarEntitiesRejectsEntityWithoutText() throws Exception {
        AtlasVertex source = sourceEntity("src");
        when(textBuilder.buildText(source)).thenReturn("");

        AtlasBaseException e = expectThrows(AtlasBaseException.class,
                () -> service.similarEntities("src", new SimilarEntitySearchParameters()));
        assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
    }

    private static SemanticSearchParameters query(String text) {
        SemanticSearchParameters params = new SemanticSearchParameters();
        params.setQuery(text);
        return params;
    }

    private void entity(String guid, AtlasEntity.Status state) throws AtlasBaseException {
        AtlasVertex vertex = mock(AtlasVertex.class);
        graphUtils.when(() -> AtlasGraphUtilsV2.findByGuid(guid)).thenReturn(vertex);
        graphUtils.when(() -> AtlasGraphUtilsV2.getState(vertex)).thenReturn(state);
        when(entityRetriever.toAtlasEntityHeader(eq(vertex), any())).thenReturn(header(guid, state));
    }

    private AtlasVertex sourceEntity(String guid) throws AtlasBaseException {
        AtlasVertex vertex = mock(AtlasVertex.class);
        when(entityRetriever.getEntityVertex(guid)).thenReturn(vertex);
        when(entityRetriever.toAtlasEntityHeaderWithClassifications(vertex)).thenReturn(header(guid, AtlasEntity.Status.ACTIVE));
        return vertex;
    }

    private static AtlasEntityHeader header(String guid, AtlasEntity.Status state) {
        AtlasEntityHeader header = new AtlasEntityHeader("DataSet");
        header.setGuid(guid);
        header.setStatus(state);
        return header;
    }

    private static List<String> guids(AtlasSearchResult result) {
        return result.getFullTextResult().stream().map(r -> r.getEntity().getGuid()).collect(Collectors.toList());
    }
}
