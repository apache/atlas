/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.SortOrder;
import org.apache.atlas.common.TestUtility;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasQuickSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResultDownloadStatus;
import org.apache.atlas.model.discovery.AtlasSuggestionsResult;
import org.apache.atlas.model.discovery.QuickSearchParameters;
import org.apache.atlas.model.discovery.RelationshipSearchParameters;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.profile.AtlasUserSavedSearch;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.configuration.Configuration;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.common.TestUtility.generateString;
import static org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType.BASIC;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.ATTRIBUTE_LABEL_MAP_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.CSV_FILE_NAME_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.SEARCH_PARAMETERS_JSON_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.SEARCH_TYPE_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

public class DiscoveryRESTTest {
    private final int defaultMaxDslQueryStrLength = 4096;
    private final int defaultMaxFulltextQueryStrLength = 4096;

    @Mock
    private AtlasTypeRegistry mockTypeRegistry;

    @Mock
    private AtlasDiscoveryService mockDiscoveryService;

    @Mock
    private AtlasEntityType mockEntityType;

    @Mock
    private Configuration mockConfiguration;

    @InjectMocks
    private DiscoveryREST discoveryREST;

    @BeforeMethod
    public void resetMocks() {
        MockitoAnnotations.openMocks(this);
        mockDefaultConfiguration();
        discoveryREST = new DiscoveryREST(mockTypeRegistry, mockDiscoveryService, mockConfiguration);
    }

    @Test
    public void testSearchUsingDSL_Success() throws AtlasBaseException {
        String query = "from DataSet";
        String typeName = "DataSet";
        String classification = "PII";
        int limit = 10;
        int offset = 0;

        String dslQuery = "from DataSet where classification = 'PII'";

        AtlasSearchResult expectedResult = getAtlasSearchResult();

        when(mockDiscoveryService.getDslQueryUsingTypeNameClassification(query, typeName, classification))
                .thenReturn(dslQuery);
        when(mockDiscoveryService.searchUsingDslQuery(dslQuery, limit, offset)).thenReturn(expectedResult);

        AtlasSearchResult actualResult = discoveryREST.searchUsingDSL(query, typeName, classification, limit, offset);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);

        verify(mockDiscoveryService).getDslQueryUsingTypeNameClassification(query, typeName, classification);
        verify(mockDiscoveryService).searchUsingDslQuery(dslQuery, limit, offset);
    }

    @Test
    public void testSearchUsingDSL_InvalidQueryTooLong_ThrowsException() {
        String query = generateString(defaultMaxDslQueryStrLength + 1, 'a');
        String typeName = "DataSet2";
        String classification = "PII2";
        int limit = 10;
        int offset = 0;

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.searchUsingDSL(query, typeName, classification, limit, offset));

        TestUtility.assertInvalidQueryLength(exception, Constants.MAX_DSL_QUERY_STR_LENGTH);
    }

    @Test
    public void testDslSearchCreateFile_Success() throws AtlasBaseException {
        Map<String, Object> parameterMap = new HashMap<>();
        Map<String, Object> searchParamsMap = Collections.emptyMap();
        parameterMap.put("searchParameters", searchParamsMap);
        discoveryREST.dslSearchCreateFile(parameterMap);
        verify(mockDiscoveryService, times(1)).createAndQueueSearchResultDownloadTask(anyMap());
    }

    @Test
    public void testDslSearchCreateFile_WhenPendingTasksExist_ThrowsException() throws AtlasBaseException {
        Map<String, Object> parameterMap = new HashMap<>();
        Map<String, Object> searchParamsMap = Collections.emptyMap();
        parameterMap.put("searchParameters", searchParamsMap);
        doThrow(new AtlasBaseException(AtlasErrorCode.PENDING_TASKS_ALREADY_IN_PROGRESS, "50"))
                .when(mockDiscoveryService).createAndQueueSearchResultDownloadTask(anyMap());

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> {
            discoveryREST.dslSearchCreateFile(parameterMap);
        });
        verify(mockDiscoveryService, times(1)).createAndQueueSearchResultDownloadTask(anyMap());
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.PENDING_TASKS_ALREADY_IN_PROGRESS);
        assertEquals(exception.getMessage(), AtlasErrorCode.PENDING_TASKS_ALREADY_IN_PROGRESS.getFormattedErrorMessage("50"));
    }

    @Test
    public void testSearchUsingFullText_Success() throws AtlasBaseException {
        String query = "search";
        boolean excludeDeleted = false;
        int limit = 10;
        int offset = 0;

        AtlasSearchResult expectedResult = getAtlasSearchResult();
        when(mockDiscoveryService.searchUsingFullTextQuery(query, excludeDeleted, limit, offset))
                .thenReturn(expectedResult);

        AtlasSearchResult actualResult = discoveryREST.searchUsingFullText(query, excludeDeleted, limit, offset);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockDiscoveryService).searchUsingFullTextQuery(query, excludeDeleted, limit, offset);
    }

    @Test
    public void testSearchUsingFullText_QueryTooLong_ThrowsException() {
        String longQuery = generateString(defaultMaxFulltextQueryStrLength + 1, 'x'); // Query Length Too Long

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.searchUsingFullText(longQuery, false, 10, 0));

        TestUtility.assertInvalidQueryLength(exception, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
    }

    @Test
    public void testSearchUsingBasic_Success() throws Exception {
        String query = "employee";
        String typeName = "Person";
        String classification = "PII";
        String sortBy = "name";
        SortOrder sortOrder = SortOrder.ASCENDING;
        boolean excludeDeleted = false;
        int limit = 10;
        int offset = 0;
        String marker = null;

        AtlasSearchResult expectedResult = getAtlasSearchResult();
        when(mockDiscoveryService.searchWithParameters(any(SearchParameters.class))).thenReturn(expectedResult);

        AtlasSearchResult actualResult = discoveryREST.searchUsingBasic(query, typeName, classification, sortBy, sortOrder,
                excludeDeleted, limit, offset, marker);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockDiscoveryService, times(1)).searchWithParameters(any(SearchParameters.class));
    }

    @Test
    public void testSearchUsingBasic_QueryTooLong_ThrowsException() {
        String longQuery = generateString(defaultMaxFulltextQueryStrLength + 1, 'x'); // Query Length Too Long

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.searchUsingBasic(longQuery, null, null, null, null, false, 10, 0, null));

        TestUtility.assertInvalidQueryLength(exception, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
    }

    // Attributes
    @Test
    public void testSearchUsingAttribute_Success() throws Exception {
        String attrName = "name";
        String attrValuePrefix = "John";
        String typeName = "Person";
        int limit = 10;
        int offset = 0;

        AtlasSearchResult expectedResult = getAtlasSearchResult();

        SearchParameters.FilterCriteria entityFilter = new SearchParameters.FilterCriteria();

        entityFilter.setAttributeName(attrName);
        entityFilter.setOperator(SearchParameters.Operator.STARTS_WITH);
        entityFilter.setAttributeValue(attrValuePrefix);

        SearchParameters searchParams = buildAndGetSearchParameters(offset, limit, typeName, entityFilter, null);

        doReturn(expectedResult).when(mockDiscoveryService).searchWithParameters(searchParams);

        AtlasSearchResult actualResult = discoveryREST.searchUsingAttribute(attrName, attrValuePrefix, typeName, limit, offset);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockDiscoveryService, times(1)).searchWithParameters(searchParams);
    }

    @Test
    public void testSearchUsingAttribute_EmptyAttrNameAndValue_ThrowsException() {
        String attrName = ""; // Empty Attribute Name
        String attrValue = ""; // Empty Attribute Value Prefix
        String typeName = "Person";

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.searchUsingAttribute(attrName, attrValue, typeName, 10, 0));

        TestUtility.assertInvalidParameters(exception, String.format("attrName : %s, attrValue: %s for attribute search.", attrName, attrValue));
    }

    @Test
    public void testSearchUsingAttribute_AttrNameResolvedFromType_Success() throws Exception {
        String attrName = null; // Null Attribute Name, Should be resolved if Attribute Value present
        String attrValue = "John";
        String typeName = "Person";
        int limit = 10;
        int offset = 0;

        AtlasSearchResult expectedResult = getAtlasSearchResult();

        SearchParameters.FilterCriteria entityFilter = new SearchParameters.FilterCriteria();

        entityFilter.setAttributeName(AtlasClient.NAME);
        entityFilter.setOperator(SearchParameters.Operator.STARTS_WITH);
        entityFilter.setAttributeValue(attrValue);

        SearchParameters searchParams = buildAndGetSearchParameters(offset, limit, typeName, entityFilter, null);

        doReturn(expectedResult).when(mockDiscoveryService).searchWithParameters(searchParams);

        AtlasStructType.AtlasAttribute mockAttr = mock(AtlasStructType.AtlasAttribute.class);
        when(mockEntityType.getAttribute(AtlasClient.NAME)).thenReturn(mockAttr);
        when(mockTypeRegistry.getEntityTypeByName(typeName)).thenReturn(mockEntityType);

        AtlasSearchResult actualResult = discoveryREST.searchUsingAttribute(attrName, attrValue, typeName, limit,
                offset);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockDiscoveryService, times(1)).searchWithParameters(searchParams);
    }

    @Test
    public void testSearchWithValidParameters_Success() throws AtlasBaseException {
        String typeName = "hive_table";
        int offset = 0;
        int limit = 10;
        SearchParameters params = buildAndGetSearchParameters(offset, limit, typeName, null, null);

        AtlasSearchResult expectedResult = getAtlasSearchResult();
        when(mockDiscoveryService.searchWithParameters(params)).thenReturn(expectedResult);

        AtlasSearchResult actualResult = discoveryREST.searchWithParameters(params);

        assertEquals(actualResult, expectedResult);
        verify(mockDiscoveryService, times(1)).searchWithParameters(params);
    }

    @Test
    public void testSearchWithValidParameters_NegativeLimit_ThrowsException() {
        String typeName = "hive_table";
        int offset = 0;
        int limit = -1; // Negative Limit
        SearchParameters params = buildAndGetSearchParameters(offset, limit, typeName, null, null);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.searchWithParameters(params));

        TestUtility.assertBadRequests(exception, "Limit/offset should be non-negative");
    }

    @Test
    public void testSearchWithValidParameters_InvalidSearchParams_ThrowsException() {
        String typeName = ""; // Empty Type Name
        int offset = 0;
        int limit = 10;
        SearchParameters params = buildAndGetSearchParameters(offset, limit, typeName, null, null);

        // In case of Empty Type Name, Classification, Query and Term Name, Exception Thrown as INVALID_SEARCH_PARAMS
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.searchWithParameters(params));

        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INVALID_SEARCH_PARAMS);
    }

    @Test
    public void testSearchWithValidParameters_EmptyTypeName_ThrowsException() {
        String typeName = ""; // Empty Type Name
        int offset = 0;
        int limit = 10;

        SearchParameters.FilterCriteria entityFilter = new SearchParameters.FilterCriteria();
        entityFilter.setOperator(SearchParameters.Operator.EQ); // Requires a value to be set
        entityFilter.setAttributeName("attr");
        entityFilter.setAttributeValue("some_value");

        SearchParameters params = buildAndGetSearchParameters(offset, limit, typeName, entityFilter, null);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.searchWithParameters(params));

        TestUtility.assertBadRequests(exception, "EntityFilters specified without Type name");
    }

    @Test
    public void testSearchWithValidParameters_EmptyClassification_ThrowsException() {
        String typeName = null;
        int offset = 0;
        int limit = 10;

        SearchParameters.FilterCriteria tagFilter = new SearchParameters.FilterCriteria();
        tagFilter.setOperator(SearchParameters.Operator.EQ); // Requires a value to be set
        tagFilter.setAttributeName("attr");
        tagFilter.setAttributeValue("some_value");

        SearchParameters params = buildAndGetSearchParameters(offset, limit, typeName, null, tagFilter);

        // Tag filter provided but Classification Not provided, Results in validation Exception
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.searchWithParameters(params));

        TestUtility.assertBadRequests(exception, "TagFilters specified without tag name");
    }

    @Test
    public void testBasicSearchCreateFile_Success() throws Exception {
        Map<String, Object> parameterMap = new HashMap<>();
        Map<String, String> attributeLabelMap = Collections.emptyMap();
        Map<String, Object> searchParamsMap = new HashMap<>();
        searchParamsMap.put("offset", 0);
        searchParamsMap.put("limit", 10);
        searchParamsMap.put("typeName", "hive_table");

        parameterMap.put("attributeLabelMap", attributeLabelMap);
        parameterMap.put("searchParameters", searchParamsMap);

        SearchParameters searchParameters = AtlasJson.fromLinkedHashMap(parameterMap.get("searchParameters"),
                SearchParameters.class);
        String searchParametersJSON = AtlasJson.toJson(searchParameters);
        String attributeLabelMapJSON = AtlasJson.toJson(attributeLabelMap);

        discoveryREST.basicSearchCreateFile(parameterMap);
        verify(mockDiscoveryService).createAndQueueSearchResultDownloadTask(
                argThat(map -> map.containsKey(CSV_FILE_NAME_KEY) && BASIC.equals(map.get(SEARCH_TYPE_KEY))
                        && attributeLabelMapJSON.equals(map.get(ATTRIBUTE_LABEL_MAP_KEY))
                        && searchParametersJSON.equals(map.get(SEARCH_PARAMETERS_JSON_KEY))));
    }

    @Test
    public void testBasicSearchCreateFile_PendingTasksExist_ThrowsException() throws Exception {
        Map<String, Object> parameterMap = new HashMap<>();
        Map<String, String> attributeLabelMap = Collections.emptyMap();
        Map<String, Object> searchParamsMap = new HashMap<>();
        searchParamsMap.put("offset", 0);
        searchParamsMap.put("limit", 10);
        searchParamsMap.put("typeName", "hive_table");

        parameterMap.put("attributeLabelMap", attributeLabelMap);
        parameterMap.put("searchParameters", searchParamsMap);

        SearchParameters searchParameters = AtlasJson.fromLinkedHashMap(parameterMap.get("searchParameters"),
                SearchParameters.class);
        String searchParametersJSON = AtlasJson.toJson(searchParameters);
        String attributeLabelMapJSON = AtlasJson.toJson(attributeLabelMap);

        doThrow(new AtlasBaseException(AtlasErrorCode.PENDING_TASKS_ALREADY_IN_PROGRESS, "50"))
                .when(mockDiscoveryService).createAndQueueSearchResultDownloadTask(anyMap());

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.basicSearchCreateFile(parameterMap));

        verify(mockDiscoveryService).createAndQueueSearchResultDownloadTask(
                argThat(map -> map.containsKey(CSV_FILE_NAME_KEY) && BASIC.equals(map.get(SEARCH_TYPE_KEY))
                        && attributeLabelMapJSON.equals(map.get(ATTRIBUTE_LABEL_MAP_KEY))
                        && searchParametersJSON.equals(map.get(SEARCH_PARAMETERS_JSON_KEY))));

        verify(mockDiscoveryService, times(1)).createAndQueueSearchResultDownloadTask(anyMap());

        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.PENDING_TASKS_ALREADY_IN_PROGRESS);
        assertEquals(exception.getMessage(), AtlasErrorCode.BAD_REQUEST.getFormattedErrorMessage("There are already 50 pending tasks in queue"));
    }

    @Test
    public void testGetSearchResultDownloadStatus_Success() throws Exception {
        AtlasSearchResultDownloadStatus expectedResult = mock(AtlasSearchResultDownloadStatus.class);
        when(mockDiscoveryService.getSearchResultDownloadStatus()).thenReturn(expectedResult);

        AtlasSearchResultDownloadStatus actualResult = discoveryREST.getSearchResultDownloadStatus();

        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void testRelationSearch_DefaultDisabled_ThrowsException() {
        // Relation Search is Disabled By Default
        RelationshipSearchParameters relationshipSearchParam = new RelationshipSearchParameters();
        relationshipSearchParam.setLimit(10);
        relationshipSearchParam.setOffset(0);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.relationSearch(relationshipSearchParam));

        TestUtility.assertBadRequests(exception, "Relationship search is currently disabled, set property " + AtlasConfiguration.RELATIONSHIP_SEARCH_ENABLED.getPropertyName() + " = true to enable");
    }

    @Test
    public void testRelationSearch_DefaultDisabled_NegativeLimit_ThrowsException() {
        RelationshipSearchParameters relationshipSearchParam = new RelationshipSearchParameters();
        relationshipSearchParam.setLimit(-1); // Providing a negative limit causes validation to fail
        relationshipSearchParam.setOffset(0);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.relationSearch(relationshipSearchParam));

        TestUtility.assertBadRequests(exception, "Limit/offset should be non-negative");
    }

    @Test
    public void testRelationSearch_DefaultDisabled_NegativeOffset_ThrowsException() {
        RelationshipSearchParameters relationshipSearchParam = new RelationshipSearchParameters();
        relationshipSearchParam.setLimit(10);
        relationshipSearchParam.setOffset(-1); // Providing a negative offset causes validation to fail

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.relationSearch(relationshipSearchParam));

        TestUtility.assertBadRequests(exception, "Limit/offset should be non-negative");
    }

    @Test
    public void testRelationSearch_DefaultDisabled_EmptyTypeName_ThrowsException() {
        RelationshipSearchParameters relationshipSearchParam = new RelationshipSearchParameters();
        relationshipSearchParam.setLimit(10);
        relationshipSearchParam.setOffset(0);
        // Type Name not Provided causes validation to fail

        SearchParameters.FilterCriteria filterCriteria = new SearchParameters.FilterCriteria();
        filterCriteria.setAttributeName("relation_name");
        relationshipSearchParam.setRelationshipFilters(filterCriteria);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.relationSearch(relationshipSearchParam));

        TestUtility.assertBadRequests(exception, "RelationshipFilters specified without Type name");
    }

    @Test
    public void testSearchRelatedEntities_Success() throws Exception {
        String guid = "1234-5678";
        String relation = "relatedTo";
        Set<String> attributes = new HashSet<>(Arrays.asList("name", "description"));
        String sortByAttribute = "name";
        SortOrder sortOrder = SortOrder.ASCENDING;
        boolean excludeDeletedEntities = true;
        boolean includeClassificationAttributes = false;
        boolean getApproximateCount = false;
        int limit = 10;
        int offset = 0;

        AtlasSearchResult expectedResult = getAtlasSearchResult();

        when(mockDiscoveryService.searchRelatedEntities(eq(guid), eq(relation), eq(getApproximateCount),
                any(SearchParameters.class))).thenReturn(expectedResult);

        AtlasSearchResult actualResult = discoveryREST.searchRelatedEntities(guid, relation, attributes, sortByAttribute,
                sortOrder, excludeDeletedEntities, includeClassificationAttributes, getApproximateCount, limit, offset);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockDiscoveryService, times(1)).searchRelatedEntities(eq(guid), eq(relation), eq(getApproximateCount),
                any(SearchParameters.class));
    }

    @Test
    public void testSearchRelatedEntities_InvalidQueryLength_ThrowsException() {
        // Query Length exceeds the Configured Query Limit, Causes validation to Fail
        String guid = generateString(AtlasConfiguration.QUERY_PARAM_MAX_LENGTH.getInt() + 1, 'a'); // too long
        String relation = "relatedTo";
        Set<String> attributes = new HashSet<>(Arrays.asList("name", "description"));
        String sortByAttribute = "name";
        SortOrder sortOrder = SortOrder.ASCENDING;
        boolean excludeDeletedEntities = true;
        boolean includeClassificationAttributes = false;
        boolean getApproximateCount = false;
        int limit = 10;
        int offset = 0;

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.searchRelatedEntities(guid, relation, attributes, sortByAttribute, sortOrder,
                        excludeDeletedEntities, includeClassificationAttributes, getApproximateCount, limit, offset));

        TestUtility.assertInvalidParamLength(exception, "guid");
    }

    @Test
    public void testAddSavedSearch_ValidInput_Success() throws AtlasBaseException {
        String name = "Name1";
        String ownerName = "owner1";
        String guid = "5af3e7e8-68a6-4cf6-b88e-4c619731f3e2";
        String query = "test query";
        String typeName = "typeName";

        SearchParameters params = buildAndGetSearchParameters(typeName, query, null);

        AtlasUserSavedSearch savedSearch = buildAndGetAtlasUserSavedSearch(name, ownerName, guid, null, params);

        AtlasUserSavedSearch expectedResult = buildAndGetAtlasUserSavedSearch(name, ownerName, null, AtlasUserSavedSearch.SavedSearchType.BASIC, params);
        when(mockDiscoveryService.addSavedSearch(anyString(), eq(savedSearch))).thenReturn(expectedResult);

        AtlasUserSavedSearch actualResult = discoveryREST.addSavedSearch(savedSearch); // should not throw

        verify(mockDiscoveryService, times(1)).addSavedSearch(anyString(), eq(savedSearch));
        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void testAddSavedSearch_InvalidQueryLength_ThrowsException() {
        String name = "Test";
        String ownerName = "owner";
        String guid = "guid";

        SearchParameters params = buildAndGetSearchParameters(null, generateString(defaultMaxFulltextQueryStrLength + 1, 'a'), null);

        AtlasUserSavedSearch savedSearch = buildAndGetAtlasUserSavedSearch(name, ownerName, guid, null, params);

        // Query Length exceeds the Configured Query Limit, Causes validation to Fail
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.addSavedSearch(savedSearch));

        TestUtility.assertInvalidQueryLength(exception, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
    }

    @Test
    public void testAddSavedSearch_MissingFilterCondition_ThrowsException() {
        SearchParameters.FilterCriteria parent = new SearchParameters.FilterCriteria();
        SearchParameters.FilterCriteria child1 = new SearchParameters.FilterCriteria();
        child1.setOperator(SearchParameters.Operator.EQ);
        child1.setAttributeName("attributeName");
        child1.setAttributeValue("attributeValue");

        // in Filter Condition not set
        parent.setCriterion(Collections.singletonList(child1));

        SearchParameters params = buildAndGetSearchParameters(null, null, parent);

        AtlasUserSavedSearch savedSearch = buildAndGetAtlasUserSavedSearch(null, null, null, null, params);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.addSavedSearch(savedSearch));

        assertEquals(exception.getMessage(), "Condition (AND/OR) must be specified when using multiple filters.");
    }

    @Test
    public void testAddSavedSearch_BlankAttributeName_ThrowsException() {
        SearchParameters.FilterCriteria filter = new SearchParameters.FilterCriteria();
        filter.setOperator(SearchParameters.Operator.EQ);
        filter.setAttributeName(""); // Empty Attribute Name
        filter.setAttributeValue("attributeValue");

        SearchParameters params = buildAndGetSearchParameters(null, null, filter);
        AtlasUserSavedSearch savedSearch = buildAndGetAtlasUserSavedSearch(null, null, null, null, params);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.addSavedSearch(savedSearch));

        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.BLANK_NAME_ATTRIBUTE);
    }

    @Test
    public void testAddSavedSearch_MissingOperator_ThrowsException() {
        SearchParameters.FilterCriteria filter = new SearchParameters.FilterCriteria();
        filter.setAttributeName("attributeName");
        filter.setAttributeValue("attributeValue");
        // Operator Not set in Filter Criteria

        SearchParameters params = buildAndGetSearchParameters(null, null, filter);

        AtlasUserSavedSearch savedSearch = buildAndGetAtlasUserSavedSearch(null, null, null, null, params);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.addSavedSearch(savedSearch));
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INVALID_OPERATOR);
    }

    @Test
    public void testAddSavedSearch_BlankValueWithOperator_ThrowsException() {
        SearchParameters.FilterCriteria filter = new SearchParameters.FilterCriteria();
        filter.setOperator(SearchParameters.Operator.EQ); // Requires a value to be set
        filter.setAttributeName("attributeName");
        filter.setAttributeValue(""); // blank

        SearchParameters params = buildAndGetSearchParameters(null, null, filter);

        AtlasUserSavedSearch savedSearch = buildAndGetAtlasUserSavedSearch(null, null, null, null, params);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.addSavedSearch(savedSearch));
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.BLANK_VALUE_ATTRIBUTE);
    }

    @Test
    public void testUpdateSavedSearch_Success() throws AtlasBaseException {
        String name = "Name1";
        String ownerName = "owner1";
        String guid = "5af3e7e8-68a6-4cf6-b88e-4c619731f3e2";
        String paramQuery = "test query";
        String paramTypeName = "typeName";

        SearchParameters params = buildAndGetSearchParameters(paramTypeName, paramQuery, null);

        AtlasUserSavedSearch savedSearch = buildAndGetAtlasUserSavedSearch(name, ownerName, guid, null, params);

        AtlasUserSavedSearch expectedResult = buildAndGetAtlasUserSavedSearch(name, ownerName, null, AtlasUserSavedSearch.SavedSearchType.BASIC, params);
        when(mockDiscoveryService.updateSavedSearch(anyString(), eq(savedSearch))).thenReturn(expectedResult);

        AtlasUserSavedSearch actualResult = discoveryREST.updateSavedSearch(savedSearch); // should not throw

        verify(mockDiscoveryService, times(1)).updateSavedSearch(anyString(), eq(savedSearch));
        assertEquals(actualResult, expectedResult);
    }

    @Test
    public void testUpdateSavedSearch_EmptyGUID_ThrowsException() throws AtlasBaseException {
        String name = "Name1";
        String ownerName = "owner1";
        String guid = ""; // Empty guid, cause validation to fail
        String paramQuery = "test query";
        String paramTypeName = "typeName";

        SearchParameters params = buildAndGetSearchParameters(paramTypeName, paramQuery, null);

        AtlasUserSavedSearch savedSearch = buildAndGetAtlasUserSavedSearch(name, ownerName, guid, null, params);

        when(mockDiscoveryService.updateSavedSearch(anyString(), eq(savedSearch)))
                .thenThrow(new AtlasBaseException(AtlasErrorCode.INVALID_OBJECT_ID, savedSearch.getGuid()));

        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.updateSavedSearch(savedSearch));

        verify(mockDiscoveryService, times(1)).updateSavedSearch(anyString(), eq(savedSearch));
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INVALID_OBJECT_ID);
        assertEquals(exception.getMessage(), AtlasErrorCode.INVALID_OBJECT_ID.getFormattedErrorMessage(savedSearch.getGuid()));
    }

    @Test
    public void testGetSavedSearch_Success() throws Exception {
        AtlasUserSavedSearch expectedSearch = new AtlasUserSavedSearch();
        when(mockDiscoveryService.getSavedSearchByName(anyString(), eq("userA"), eq("searchA")))
                .thenReturn(expectedSearch);

        AtlasUserSavedSearch result = discoveryREST.getSavedSearch("searchA", "userA");

        assertNotNull(result);
        verify(mockDiscoveryService).getSavedSearchByName(anyString(), eq("userA"), eq("searchA"));
    }

    @Test
    public void testGetSavedSearches_Success() throws Exception {
        List<AtlasUserSavedSearch> savedSearches = Collections.singletonList(new AtlasUserSavedSearch());
        when(mockDiscoveryService.getSavedSearches(anyString(), eq("userA"))).thenReturn(savedSearches);

        List<AtlasUserSavedSearch> result = discoveryREST.getSavedSearches("userA");

        assertEquals(result.size(), 1);
        verify(mockDiscoveryService).getSavedSearches(anyString(), eq("userA"));
    }

    @Test
    public void testDeleteSavedSearch_Success() throws Exception {
        discoveryREST.deleteSavedSearch("guid-123");

        verify(mockDiscoveryService).deleteSavedSearch(anyString(), eq("guid-123"));
    }

    @Test
    public void testExecuteSavedSearchByName_Success() throws Exception {
        AtlasUserSavedSearch savedSearch = new AtlasUserSavedSearch();
        AtlasSearchResult expectedResult = getAtlasSearchResult();
        when(mockDiscoveryService.getSavedSearchByName(anyString(), eq("userA"), eq("searchA")))
                .thenReturn(savedSearch);
        when(mockDiscoveryService.searchWithParameters(savedSearch.getSearchParameters())).thenReturn(expectedResult);

        AtlasSearchResult actualResult = discoveryREST.executeSavedSearchByName("searchA", "userA");

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockDiscoveryService).getSavedSearchByName(anyString(), eq("userA"), eq("searchA"));
    }

    @Test
    public void testExecuteSavedSearchByGuid_Success() throws Exception {
        AtlasUserSavedSearch savedSearch = new AtlasUserSavedSearch();
        AtlasSearchResult expectedResult = getAtlasSearchResult();

        when(mockDiscoveryService.getSavedSearchByGuid(anyString(), eq("guid-123"))).thenReturn(savedSearch);
        when(mockDiscoveryService.searchWithParameters(savedSearch.getSearchParameters())).thenReturn(expectedResult);

        AtlasSearchResult actualResult = discoveryREST.executeSavedSearchByGuid("guid-123");

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockDiscoveryService).getSavedSearchByGuid(anyString(), eq("guid-123"));
    }

    @Test
    public void testQuickSearchGET_Success() throws Exception {
        AtlasQuickSearchResult expectedResult = new AtlasQuickSearchResult();
        AtlasSearchResult searchResult = getAtlasSearchResult();
        searchResult.setQueryText("sample_text");
        expectedResult.setSearchResults(searchResult);
        when(mockDiscoveryService.quickSearch(any())).thenReturn(expectedResult);

        AtlasQuickSearchResult actualResult = discoveryREST.quickSearch("query", "typeName", true, 0, 10, "name",
                SortOrder.ASCENDING);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        assertEquals(actualResult.getSearchResults().getQueryText(), searchResult.getQueryText());
        verify(mockDiscoveryService).quickSearch(any(QuickSearchParameters.class));
    }

    @Test
    public void testQuickSearchGET_QueryLengthTooLong_ThrowsException() {
        String longQuery = generateString(defaultMaxFulltextQueryStrLength + 1, 'a');
        AtlasBaseException exception = expectThrows(AtlasBaseException.class,
                () -> discoveryREST.quickSearch(longQuery, "typeName", true, 0, 10, "name", SortOrder.ASCENDING));
        TestUtility.assertInvalidQueryLength(exception, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
    }

    @Test
    public void testQuickSearchPOST_Success() throws Exception {
        String typeName = "typeName";
        String query = "query";
        QuickSearchParameters params = buildAndGetQuickSearchParameters(typeName, query, null, null);
        params.setLimit(10);
        params.setOffset(0);

        AtlasQuickSearchResult expectedResult = new AtlasQuickSearchResult();
        when(mockDiscoveryService.quickSearch(params)).thenReturn(expectedResult);

        AtlasQuickSearchResult actualResult = discoveryREST.quickSearch(params);

        assertNotNull(actualResult);
        assertEquals(actualResult, expectedResult);
        verify(mockDiscoveryService).quickSearch(params);
    }

    @Test
    public void testQuickSearchPOST_NegativeLimit_ThrowsException() {
        String typeName = "typeName";
        String query = "query";
        QuickSearchParameters params = buildAndGetQuickSearchParameters(typeName, query, null, null);
        params.setLimit(-1);
        params.setOffset(0);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.quickSearch(params));
        TestUtility.assertBadRequests(exception, "Limit/offset should be non-negative");
    }

    @Test
    public void testQuickSearchPOST_NegativeOffset_ThrowsException() {
        String typeName = "typeName";
        String query = "query";
        QuickSearchParameters params = buildAndGetQuickSearchParameters(typeName, query, null, null);
        params.setLimit(10);
        params.setOffset(-1);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.quickSearch(params));
        TestUtility.assertBadRequests(exception, "Limit/offset should be non-negative");
    }

    @Test
    public void testQuickSearchPOST_EmptyTypeNameWithEntityFilter_ThrowsException() {
        SearchParameters.FilterCriteria mockFilterCriteria = mock(SearchParameters.FilterCriteria.class);
        when(mockFilterCriteria.getAttributeName()).thenReturn("attribute_name");
        List<SearchParameters.FilterCriteria> mockLst = mock(List.class);
        when(mockFilterCriteria.getCriterion()).thenReturn(mockLst);

        String typeName = "";
        String query = "query";
        QuickSearchParameters params = buildAndGetQuickSearchParameters(typeName, query, null, mockFilterCriteria);
        params.setLimit(10);
        params.setOffset(0);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.quickSearch(params));
        TestUtility.assertBadRequests(exception, "EntityFilters specified without Type name");
    }

    @Test
    public void testQuickSearchPOST_EmptyTypeNameWithSortByFilter_ThrowsException() {
        String typeName = "";
        String query = "query";
        String sortBy = "name";
        QuickSearchParameters params = buildAndGetQuickSearchParameters(typeName, query, sortBy, null);
        params.setLimit(10);
        params.setOffset(0);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.quickSearch(params));

        TestUtility.assertBadRequests(exception, "SortBy specified without Type name");
    }

    @Test
    public void testQuickSearchPOST_EmptyTypeName_EmptyQuery_ThrowsException() {
        String typeName = "";
        String query = "";
        String sortBy = "name";
        QuickSearchParameters params = buildAndGetQuickSearchParameters(typeName, query, sortBy, null);
        params.setLimit(10);
        params.setOffset(0);

        AtlasBaseException exception = expectThrows(AtlasBaseException.class, () -> discoveryREST.quickSearch(params));
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INVALID_SEARCH_PARAMS);
    }

    @Test
    public void testGetSuggestions_Success() {
        AtlasSuggestionsResult expectedResult = new AtlasSuggestionsResult("pref", "field");
        when(mockDiscoveryService.getSuggestions("pref", "field")).thenReturn(expectedResult);

        AtlasSuggestionsResult result = discoveryREST.getSuggestions("pref", "field");

        assertNotNull(result);
        verify(mockDiscoveryService).getSuggestions("pref", "field");
    }

    private AtlasSearchResult getAtlasSearchResult() {
        return new AtlasSearchResult();
    }

    private void mockDefaultConfiguration() {
        when(mockConfiguration.getInt(eq(Constants.MAX_DSL_QUERY_STR_LENGTH), anyInt()))
                .thenReturn(defaultMaxDslQueryStrLength);
        when(mockConfiguration.getInt(eq(Constants.MAX_FULLTEXT_QUERY_STR_LENGTH), anyInt()))
                .thenReturn(defaultMaxFulltextQueryStrLength);
    }

    private SearchParameters buildAndGetSearchParameters(int offset, int limit, String typeName, SearchParameters.FilterCriteria entityFilter, SearchParameters.FilterCriteria tagFilter) {
        SearchParameters ret = new SearchParameters();
        ret.setTypeName(typeName);
        ret.setOffset(offset);
        ret.setLimit(limit);
        ret.setEntityFilters(entityFilter);
        ret.setTagFilters(tagFilter);
        return ret;
    }

    private SearchParameters buildAndGetSearchParameters(String typeName, String query, SearchParameters.FilterCriteria entityFilter) {
        SearchParameters ret = new SearchParameters();
        if (typeName != null) {
            ret.setTypeName(typeName);
        }
        if (query != null) {
            ret.setQuery(query);
        }
        if (entityFilter != null) {
            ret.setEntityFilters(entityFilter);
        }
        return ret;
    }

    private AtlasUserSavedSearch buildAndGetAtlasUserSavedSearch(String name, String ownerName, String guid, AtlasUserSavedSearch.SavedSearchType searchType, SearchParameters searchParameters) {
        AtlasUserSavedSearch ret = new AtlasUserSavedSearch();
        ret.setName(name);
        ret.setOwnerName(ownerName);
        if (guid != null) {
            ret.setGuid(guid);
        }
        if (searchParameters != null) {
            ret.setSearchParameters(searchParameters);
        }
        if (searchType != null) {
            ret.setSearchType(searchType);
        }
        return ret;
    }

    private QuickSearchParameters buildAndGetQuickSearchParameters(String typeName, String query, String sortBy, SearchParameters.FilterCriteria entityFilter) {
        QuickSearchParameters ret = new QuickSearchParameters();
        if (typeName != null) {
            ret.setTypeName(typeName);
        }
        if (query != null) {
            ret.setQuery(query);
        }
        if (sortBy != null) {
            ret.setSortBy(sortBy);
        }
        if (entityFilter != null) {
            ret.setEntityFilters(entityFilter);
        }
        return ret;
    }
}
