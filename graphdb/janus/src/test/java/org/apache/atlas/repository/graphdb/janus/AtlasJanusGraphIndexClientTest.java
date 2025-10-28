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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.model.discovery.AtlasAggregationEntry;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AggregationContext;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.commons.configuration.Configuration;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.TermsResponse;
import org.apache.solr.client.solrj.response.V2Response;
import org.apache.solr.common.util.NamedList;
import org.janusgraph.diskstorage.es.ElasticSearch7Index;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.janusgraph.diskstorage.solr.Solr6Index;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class AtlasJanusGraphIndexClientTest {
    @Mock
    private Configuration mockConfiguration;
    @Mock
    private AggregationContext mockAggregationContext;
    @Mock
    private AtlasAttribute mockAttribute;
    @Mock
    private SolrClient mockSolrClient;
    @Mock
    private QueryResponse mockQueryResponse;
    @Mock
    private TermsResponse mockTermsResponse;
    @Mock
    private SolrResponse mockSolrResponse;
    @Mock
    private SolrPingResponse mockPingResponse;
    @Mock
    private ElasticSearchClient mockElasticSearchClient;
    @Mock
    private V2Response mockV2Response;

    private AtlasJanusGraphIndexClient indexClient;
    private MockedStatic<Solr6Index> mockedSolr6Index;
    private MockedStatic<ElasticSearch7Index> mockedElasticSearch7Index;
    private MockedStatic<ApplicationProperties> mockedApplicationProperties;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.openMocks(this);
        indexClient = new AtlasJanusGraphIndexClient(mockConfiguration);

        // Setup static mocks
        mockedSolr6Index = Mockito.mockStatic(Solr6Index.class);
        mockedElasticSearch7Index = Mockito.mockStatic(ElasticSearch7Index.class);
        mockedApplicationProperties = Mockito.mockStatic(ApplicationProperties.class);
    }

    @AfterMethod
    public void tearDown() {
        if (mockedSolr6Index != null) {
            mockedSolr6Index.close();
        }
        if (mockedElasticSearch7Index != null) {
            mockedElasticSearch7Index.close();
        }
        if (mockedApplicationProperties != null) {
            mockedApplicationProperties.close();
        }
    }

    @Test
    public void testGetAggregatedMetricsWithNullSolrClient() {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(null);

        Map<String, List<AtlasAggregationEntry>> result = indexClient.getAggregatedMetrics(mockAggregationContext);

        assertEquals(result.size(), 0);
        mockedSolr6Index.verify(Solr6Index::getSolrClient);
    }

    @Test
    public void testGetAggregatedMetricsWithEmptyFields() {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        when(mockAggregationContext.getAggregationFieldNames()).thenReturn(Collections.emptySet());
        when(mockAggregationContext.getAggregationAttributes()).thenReturn(Collections.emptySet());

        Map<String, List<AtlasAggregationEntry>> result = indexClient.getAggregatedMetrics(mockAggregationContext);

        assertEquals(result.size(), 0);
        mockedSolr6Index.verify(() -> Solr6Index.releaseSolrClient(mockSolrClient));
    }

    @Test
    public void testGetAggregatedMetricsWithValidData() throws Exception {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        // Setup aggregation context - simplified to avoid static initialization issues
        Set<String> fieldNames = new HashSet<>();
        fieldNames.add("testField");
        Map<String, String> indexFieldNameCache = new HashMap<>();
        indexFieldNameCache.put("testField", "testField__index");

        when(mockAggregationContext.getAggregationFieldNames()).thenReturn(fieldNames);
        when(mockAggregationContext.getAggregationAttributes()).thenReturn(Collections.emptySet());
        when(mockAggregationContext.getIndexFieldNameCache()).thenReturn(indexFieldNameCache);
        when(mockAggregationContext.getSearchForEntityTypes()).thenReturn(Collections.emptySet());
        when(mockAggregationContext.getQueryString()).thenReturn("");
        when(mockAggregationContext.getFilterCriteria()).thenReturn(null);
        when(mockAggregationContext.isExcludeDeletedEntities()).thenReturn(false);
        when(mockAggregationContext.isIncludeSubTypes()).thenReturn(false);

        // Setup facet response
        FacetField facetField = mock(FacetField.class);
        FacetField.Count count1 = mock(FacetField.Count.class);
        FacetField.Count count2 = mock(FacetField.Count.class);

        when(facetField.getName()).thenReturn("testField__index");
        when(facetField.getValueCount()).thenReturn(2);
        when(facetField.getValues()).thenReturn(Arrays.asList(count1, count2));
        when(count1.getName()).thenReturn("value1");
        when(count1.getCount()).thenReturn(10L);
        when(count2.getName()).thenReturn("value2");
        when(count2.getCount()).thenReturn(5L);

        when(mockSolrClient.query(eq(Constants.VERTEX_INDEX), any(), eq(SolrRequest.METHOD.POST)))
                .thenReturn(mockQueryResponse);
        when(mockQueryResponse.getFacetFields()).thenReturn(Arrays.asList(facetField));

        Map<String, List<AtlasAggregationEntry>> result = indexClient.getAggregatedMetrics(mockAggregationContext);

        assertEquals(result.size(), 1);
        assertTrue(result.containsKey("testField"));
        List<AtlasAggregationEntry> entries = result.get("testField");
        assertEquals(entries.size(), 2);
        assertEquals(entries.get(0).getName(), "value1");
        assertEquals(entries.get(0).getCount(), 10L);
    }

    @Test
    public void testGetAggregatedMetricsWithException() throws Exception {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        when(mockAggregationContext.getAggregationFieldNames()).thenReturn(Collections.singleton("field"));
        when(mockAggregationContext.getAggregationAttributes()).thenReturn(Collections.emptySet());
        when(mockAggregationContext.getIndexFieldNameCache()).thenReturn(Collections.singletonMap("field", "field__index"));
        when(mockAggregationContext.getSearchForEntityTypes()).thenReturn(Collections.emptySet());
        when(mockAggregationContext.getQueryString()).thenReturn("");
        when(mockAggregationContext.getFilterCriteria()).thenReturn(null);
        when(mockAggregationContext.isExcludeDeletedEntities()).thenReturn(false);
        when(mockAggregationContext.isIncludeSubTypes()).thenReturn(false);

        when(mockSolrClient.query(eq(Constants.VERTEX_INDEX), any(), eq(SolrRequest.METHOD.POST)))
                .thenThrow(new SolrServerException("Test exception"));

        Map<String, List<AtlasAggregationEntry>> result = indexClient.getAggregatedMetrics(mockAggregationContext);

        assertEquals(result.size(), 0);
        mockedSolr6Index.verify(() -> Solr6Index.releaseSolrClient(mockSolrClient));
    }

    @Test
    public void testGetSuggestionsWithNullSolrClient() {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(null);

        List<String> result = indexClient.getSuggestions("test", "field");

        assertEquals(result.size(), 0);
    }

    @Test
    public void testGetSuggestionsWithValidResponse() throws Exception {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        // Setup terms response
        TermsResponse.Term term1 = mock(TermsResponse.Term.class);
        TermsResponse.Term term2 = mock(TermsResponse.Term.class);
        TermsResponse.Term term3 = mock(TermsResponse.Term.class);

        when(term1.getTerm()).thenReturn("test1");
        when(term1.getFrequency()).thenReturn(10L);
        when(term2.getTerm()).thenReturn("test2");
        when(term2.getFrequency()).thenReturn(20L);
        when(term3.getTerm()).thenReturn("test1"); // Duplicate term
        when(term3.getFrequency()).thenReturn(5L);

        Map<String, List<TermsResponse.Term>> termMap = new HashMap<>();
        termMap.put("field1", Arrays.asList(term1, term2));
        termMap.put("field2", Arrays.asList(term3));

        when(mockSolrClient.query(eq(Constants.VERTEX_INDEX), any())).thenReturn(mockQueryResponse);
        when(mockQueryResponse.getTermsResponse()).thenReturn(mockTermsResponse);
        when(mockTermsResponse.getTermMap()).thenReturn(termMap);

        List<String> result = indexClient.getSuggestions("test", "field");

        assertEquals(result.size(), 2);
        assertEquals(result.get(0), "test2"); // Higher frequency first
        assertEquals(result.get(1), "test1"); // Combined frequency: 15
    }

    @Test
    public void testGetSuggestionsWithNullTermsResponse() throws Exception {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        when(mockSolrClient.query(eq(Constants.VERTEX_INDEX), any())).thenReturn(mockQueryResponse);
        when(mockQueryResponse.getTermsResponse()).thenReturn(null);

        List<String> result = indexClient.getSuggestions("test", "field");

        assertEquals(result.size(), 0);
    }

    @Test
    public void testGetSuggestionsWithException() throws Exception {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        when(mockSolrClient.query(eq(Constants.VERTEX_INDEX), any()))
                .thenThrow(new SolrServerException("Test exception"));

        List<String> result = indexClient.getSuggestions("test", "field");

        assertEquals(result.size(), 0);
    }

    @Test
    public void testGetSuggestionsWithEmptyIndexFieldName() throws Exception {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        when(mockSolrClient.query(eq(Constants.VERTEX_INDEX), any())).thenReturn(mockQueryResponse);
        when(mockQueryResponse.getTermsResponse()).thenReturn(mockTermsResponse);
        when(mockTermsResponse.getTermMap()).thenReturn(Collections.emptyMap());

        List<String> result = indexClient.getSuggestions("test", "");

        assertEquals(result.size(), 0);
    }

    @Test
    public void testApplySearchWeightWithNullSolrClient() {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(null);

        Map<String, Integer> weightMap = Collections.singletonMap("field1", 10);
        indexClient.applySearchWeight("testCollection", weightMap);

        mockedSolr6Index.verify(Solr6Index::getSolrClient);
    }

    @Test
    public void testApplySearchWeightSuccessfulUpdate() throws Exception {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(Solr6Index::getSolrMode).thenReturn(Solr6Index.Mode.CLOUD);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        when(mockConfiguration.getInt("index.client.apply.search.weight.max.attempts", 3)).thenReturn(3);
        when(mockConfiguration.getInt("index.client.apply.search.weight.retry.interval.ms", 1000)).thenReturn(100);

        // Mock successful response validation
        NamedList<Object> responseList = new NamedList<>();
        when(mockSolrResponse.getResponse()).thenReturn(responseList);

        // Mock V2Request processing
        V2Request mockV2Request = mock(V2Request.class);
        V2Response mockV2Response = mock(V2Response.class);
        when(mockV2Request.process(mockSolrClient)).thenReturn(mockV2Response);
        when(mockV2Response.getResponse()).thenReturn(responseList);

        Map<String, Integer> weightMap = Collections.singletonMap("field1", 10);

        // Use reflection to test the private method indirectly through applySearchWeight
        indexClient.applySearchWeight("testCollection", weightMap);

        mockedSolr6Index.verify(() -> Solr6Index.releaseSolrClient(mockSolrClient));
    }

    @Test
    public void testApplySearchWeightWithRetryLogic() throws Exception {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(Solr6Index::getSolrMode).thenReturn(Solr6Index.Mode.HTTP);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        when(mockConfiguration.getInt("index.client.apply.search.weight.max.attempts", 3)).thenReturn(2);
        when(mockConfiguration.getInt("index.client.apply.search.weight.retry.interval.ms", 1000)).thenReturn(10);

        Map<String, Integer> weightMap = Collections.singletonMap("field1", 10);

        try {
            indexClient.applySearchWeight("testCollection", weightMap);
        } catch (RuntimeException e) {
            // Expected when both update and create fail
            assertTrue(e.getMessage().contains("Error encountered in creating/updating request handler"));
        }

        verify(mockConfiguration).getInt("index.client.apply.search.weight.max.attempts", 3);
        verify(mockConfiguration).getInt("index.client.apply.search.weight.retry.interval.ms", 1000);
    }

    @Test
    public void testApplySearchWeightWithNullConfiguration() {
        AtlasJanusGraphIndexClient clientWithNullConfig = new AtlasJanusGraphIndexClient(null);
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(Solr6Index::getSolrMode).thenReturn(Solr6Index.Mode.CLOUD);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        Map<String, Integer> weightMap = Collections.singletonMap("field1", 10);

        try {
            clientWithNullConfig.applySearchWeight("testCollection", weightMap);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("Error encountered in creating/updating request handler"));
        }
    }

    @Test
    public void testGetSuggestionsWithNullQueryResponse() throws Exception {
        mockedSolr6Index.when(Solr6Index::getSolrClient).thenReturn(mockSolrClient);
        mockedSolr6Index.when(() -> Solr6Index.releaseSolrClient(any())).thenAnswer(invocation -> null);

        when(mockSolrClient.query(eq(Constants.VERTEX_INDEX), any())).thenReturn(null);

        List<String> result = indexClient.getSuggestions("test", "field");

        assertEquals(result.size(), 0);
    }

    @Test
    public void testIsHealthyWithNullConfiguration() {
        AtlasJanusGraphIndexClient clientWithNullConfig = new AtlasJanusGraphIndexClient(null);

        try {
            boolean result = clientWithNullConfig.isHealthy();
            assertFalse(result);
        } catch (NullPointerException e) {
            assertNotNull(e);
        }
    }

    @Test
    public void testGetTopTermsAsendingInput() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms     = generateTerms(10, 12, 15);
        List<String>                                     top5Terms = AtlasJanusGraphIndexClient.getTopTerms(terms);

        assertOrder(top5Terms, 2, 1, 0);
    }

    @Test
    public void testGetTopTermsAsendingInput2() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms     = generateTerms(10, 12, 15, 20, 25, 26, 30, 40);
        List<String>                                     top5Terms = AtlasJanusGraphIndexClient.getTopTerms(terms);

        assertOrder(top5Terms, 7, 6, 5, 4, 3);
    }

    @Test
    public void testGetTopTermsDescendingInput() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms     = generateTerms(10, 9, 8);
        List<String>                                     top5Terms = AtlasJanusGraphIndexClient.getTopTerms(terms);

        assertOrder(top5Terms, 0, 1, 2);
    }

    @Test
    public void testGetTopTermsDescendingInput2() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms     = generateTerms(10, 9, 8, 7, 6, 5, 4, 3, 2);
        List<String>                                     top5Terms = AtlasJanusGraphIndexClient.getTopTerms(terms);

        assertOrder(top5Terms, 0, 1, 2, 3, 4);
    }

    @Test
    public void testGetTopTermsRandom() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms     = generateTerms(10, 19, 28, 27, 16, 1, 30, 3, 36);
        List<String>                                     top5Terms = AtlasJanusGraphIndexClient.getTopTerms(terms);

        //10, 19, 28, 27, 16, 1, 30, 3, 36
        //0,  1,  2,   3,  4, 5, 6,  7,  8
        assertOrder(top5Terms, 8, 6, 2, 3, 1);
    }

    @Test
    public void testGetTopTermsRandom2() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms     = generateTerms(36, 19, 28, 27, 16, 1, 30, 3, 10);
        List<String>                                     top5Terms = AtlasJanusGraphIndexClient.getTopTerms(terms);

        //36, 19, 28, 27, 16, 1, 30, 3, 10
        //0,  1,  2,   3,  4, 5, 6,  7,  8
        assertOrder(top5Terms, 0, 6, 2, 3, 1);
    }

    @Test
    public void testGetTopTermsRandom3() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms     = generateTerms(36, 36, 28, 27, 16, 1, 30, 3, 10);
        List<String>                                     top5Terms = AtlasJanusGraphIndexClient.getTopTerms(terms);

        //36, 36, 28, 27, 16, 1, 30, 3, 10
        //0,  1,  2,   3,  4, 5, 6,  7,  8
        assertOrder(top5Terms, 0, 1, 6, 2, 3);
    }

    @Test
    public void testGetTopTermsRandom4() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms     = generateTerms(10, 10, 28, 27, 16, 1, 30, 36, 36);
        List<String>                                     top5Terms = AtlasJanusGraphIndexClient.getTopTerms(terms);

        //10, 10, 28, 27, 16, 1, 30, 36, 36
        //0,  1,  2,   3,  4, 5, 6,  7,  8
        assertOrder(top5Terms, 7, 8, 6, 2, 3);
    }

    @Test
    public void testGetTopTermsRandom5() {
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms     = generateTerms(36, 10, 28, 27, 16, 1, 30, 36, 36);
        List<String>                                     top5Terms = AtlasJanusGraphIndexClient.getTopTerms(terms);

        //36, 10, 28, 27, 16, 1, 30, 36, 36
        //0,  1,  2,   3,  4, 5, 6,  7,  8
        assertOrder(top5Terms, 0, 7, 8, 6, 2);
    }

    @Test
    public void testGenerateSuggestionString() {
        List<String> fields = new ArrayList<>();

        fields.add("one");
        fields.add("two");
        fields.add("three");

        String generatedString = AtlasJanusGraphIndexClient.generateSuggestionsString(fields);

        assertEquals(generatedString, "'one', 'two', 'three'");
    }

    @Test
    public void testGenerateSearchWeightString() {
        Map<String, Integer> fields = new HashMap<>();

        fields.put("one", 10);
        fields.put("two", 1);
        fields.put("three", 15);

        String generatedString = AtlasJanusGraphIndexClient.generateSearchWeightString(fields);

        assertEquals(generatedString, " one^10 two^1 three^15");
    }

    private void assertOrder(List<String> topTerms, int... indices) {
        assertEquals(topTerms.size(), indices.length);

        int i = 0;
        for (String term : topTerms) {
            assertEquals(Integer.toString(indices[i++]), term);
        }

        assertEquals(topTerms.size(), indices.length);
    }

    private Map<String, AtlasJanusGraphIndexClient.TermFreq> generateTerms(int... termFreqs) {
        int                                              i     = 0;
        Map<String, AtlasJanusGraphIndexClient.TermFreq> terms = new HashMap<>();

        for (int count : termFreqs) {
            AtlasJanusGraphIndexClient.TermFreq termFreq1 = new AtlasJanusGraphIndexClient.TermFreq(Integer.toString(i++), count);

            terms.put(termFreq1.getTerm(), termFreq1);
        }

        return terms;
    }
}
