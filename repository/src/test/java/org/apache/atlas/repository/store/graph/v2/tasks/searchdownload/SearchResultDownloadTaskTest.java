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
package org.apache.atlas.repository.store.graph.v2.tasks.searchdownload;

import org.apache.atlas.RequestContext;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasJson;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType.BASIC;
import static org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType.DSL;
import static org.apache.atlas.model.tasks.AtlasTask.Status.FAILED;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.ATTRIBUTE_LABEL_MAP_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.CLASSIFICATION_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.CSV_FILE_NAME_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.DOWNLOAD_DIR_PATH;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.LIMIT_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.OFFSET_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.QUERY_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.SEARCH_PARAMETERS_JSON_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.SEARCH_TYPE_KEY;
import static org.apache.atlas.repository.store.graph.v2.tasks.searchdownload.SearchResultDownloadTask.TYPE_NAME_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class SearchResultDownloadTaskTest {
    @Mock private AtlasDiscoveryService discoveryService;
    @Mock private AtlasTypeRegistry typeRegistry;
    @Mock private AtlasTask task;

    private SearchResultDownloadTask downloadTask;
    private MockedStatic<RequestContext> requestContextMock;
    private MockedStatic<AtlasJson> atlasJsonMock;

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup basic task mocks
        when(task.getGuid()).thenReturn("test-task-guid");
        when(task.getCreatedBy()).thenReturn("testUser");

        downloadTask = new SearchResultDownloadTask(task, discoveryService, typeRegistry);

        // Mock RequestContext
        requestContextMock = mockStatic(RequestContext.class);
        RequestContext mockContext = mock(RequestContext.class);
        requestContextMock.when(() -> RequestContext.get()).thenReturn(mockContext);
        requestContextMock.when(() -> RequestContext.clear()).then(invocation -> null);
        requestContextMock.when(() -> RequestContext.getCurrentUser()).thenReturn("testUser");
        lenient().doNothing().when(mockContext).setUser(anyString(), any());

        // Mock AtlasJson
        atlasJsonMock = mockStatic(AtlasJson.class);
    }

    @AfterMethod
    public void tearDown() {
        if (requestContextMock != null) {
            requestContextMock.close();
        }
        if (atlasJsonMock != null) {
            atlasJsonMock.close();
        }
    }

    @Test
    public void testPerformWithEmptyParameters() throws Exception {
        when(task.getParameters()).thenReturn(null);

        AtlasTask.Status result = downloadTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithEmptyUserName() throws Exception {
        Map<String, Object> params = createValidBasicSearchParameters();
        when(task.getParameters()).thenReturn(params);
        when(task.getCreatedBy()).thenReturn("");

        AtlasTask.Status result = downloadTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithNullUserName() throws Exception {
        Map<String, Object> params = createValidBasicSearchParameters();
        when(task.getParameters()).thenReturn(params);
        when(task.getCreatedBy()).thenReturn(null);

        AtlasTask.Status result = downloadTask.perform();

        assertEquals(result, FAILED);
    }

    @Test
    public void testPerformWithException() throws Exception {
        Map<String, Object> params = createValidBasicSearchParameters();
        when(task.getParameters()).thenReturn(params);

        when(discoveryService.searchWithParameters(any(SearchParameters.class))).thenThrow(new RuntimeException("Test exception"));

        expectThrows(() -> {
            try {
                downloadTask.perform();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testRunWithBasicSearchAndEmptyResults() throws Exception {
        Map<String, Object> params = createValidBasicSearchParameters();

        SearchParameters searchParams = new SearchParameters();
        atlasJsonMock.when(() -> AtlasJson.fromJson(anyString(), eq(SearchParameters.class))).thenReturn(searchParams);

        AtlasSearchResult searchResult = new AtlasSearchResult();
        searchResult.setEntities(new ArrayList<>());
        when(discoveryService.searchWithParameters(any(SearchParameters.class))).thenReturn(searchResult);

        Map<String, String> attributeLabelMap = new HashMap<>();
        atlasJsonMock.when(() -> AtlasJson.fromJson(anyString(), eq(Map.class))).thenReturn(attributeLabelMap);

        invokeRunMethod(params);
    }

    @Test
    public void testRunWithDSLSearchAndEmptyResults() throws Exception {
        Map<String, Object> params = createValidDSLSearchParameters();

        when(discoveryService.getDslQueryUsingTypeNameClassification(anyString(), anyString(), anyString())).thenReturn("processed query");

        AtlasSearchResult searchResult = new AtlasSearchResult();
        searchResult.setEntities(new ArrayList<>());
        when(discoveryService.searchUsingDslQuery(anyString(), anyInt(), anyInt())).thenReturn(searchResult);

        Map<String, String> attributeLabelMap = new HashMap<>();
        atlasJsonMock.when(() -> AtlasJson.fromJson(anyString(), eq(Map.class))).thenReturn(attributeLabelMap);

        invokeRunMethod(params);
    }

    @Test
    public void testConstructor() {
        SearchResultDownloadTask task = new SearchResultDownloadTask(this.task, discoveryService, typeRegistry);
        assertNotNull(task);
    }

    @Test
    public void testStaticConstants() {
        assertEquals(SEARCH_PARAMETERS_JSON_KEY, "search_parameters_json");
        assertEquals(CSV_FILE_NAME_KEY, "csv_file_Name");
        assertEquals(SEARCH_TYPE_KEY, "search_type");
        assertEquals(ATTRIBUTE_LABEL_MAP_KEY, "attribute_label_map");
        assertEquals(QUERY_KEY, "query");
        assertEquals(TYPE_NAME_KEY, "type_name");
        assertEquals(CLASSIFICATION_KEY, "classification");
        assertEquals(LIMIT_KEY, "limit");
        assertEquals(OFFSET_KEY, "offset");
        assertNotNull(DOWNLOAD_DIR_PATH);
    }

    @Test
    public void testDownloadDirPathInitialization() throws Exception {
        Field downloadDirPathField = SearchResultDownloadTask.class.getDeclaredField("DOWNLOAD_DIR_PATH");
        downloadDirPathField.setAccessible(true);
        String downloadDirPath = (String) downloadDirPathField.get(null);

        assertNotNull(downloadDirPath);
        assertTrue(downloadDirPath.contains("search_result_downloads"));
    }

    @Test
    public void testPerformWithEmptyParametersMap() throws Exception {
        when(task.getParameters()).thenReturn(new HashMap<>());

        AtlasTask.Status result = downloadTask.perform();

        assertEquals(result, FAILED);
    }

    // Helper methods
    private Map<String, Object> createValidBasicSearchParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put(SEARCH_TYPE_KEY, BASIC);
        params.put(SEARCH_PARAMETERS_JSON_KEY, "{ \"typeName\":\"Table\"}");
        params.put(CSV_FILE_NAME_KEY, "test-file.csv");
        params.put(ATTRIBUTE_LABEL_MAP_KEY, "{ \"Name\":\"name\"}");
        return params;
    }

    private Map<String, Object> createValidDSLSearchParameters() {
        Map<String, Object> params = new HashMap<>();
        params.put(SEARCH_TYPE_KEY, DSL);
        params.put(QUERY_KEY, "Table");
        params.put(TYPE_NAME_KEY, "Table");
        params.put(CLASSIFICATION_KEY, "PII");
        params.put(OFFSET_KEY, 0);
        params.put(CSV_FILE_NAME_KEY, "test-file.csv");
        params.put(ATTRIBUTE_LABEL_MAP_KEY, "{ \"Name\":\"name\"}");
        return params;
    }

    private void setupMocksForDSLSearch() throws AtlasBaseException {
        when(discoveryService.getDslQueryUsingTypeNameClassification(anyString(), anyString(), anyString())).thenReturn("processed query");

        AtlasSearchResult searchResult = createMockSearchResultWithEntities();
        when(discoveryService.searchUsingDslQuery(anyString(), anyInt(), anyInt())).thenReturn(searchResult);

        Map<String, String> attributeLabelMap = createMockAttributeLabelMap();
        atlasJsonMock.when(() -> AtlasJson.fromJson(anyString(), eq(Map.class))).thenReturn(attributeLabelMap);
    }

    private AtlasSearchResult createMockSearchResultWithEntities() {
        AtlasSearchResult searchResult = new AtlasSearchResult();

        AtlasEntityHeader entity1 = new AtlasEntityHeader();
        entity1.setTypeName("Table");
        entity1.setGuid("guid1");
        entity1.setDisplayText("TestTable1");
        entity1.setClassificationNames(Arrays.asList("PII", "Sensitive"));
        entity1.setMeaningNames(Arrays.asList("BusinessTerm1"));

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", "TestTable1");
        attributes.put("owner", "testUser");
        entity1.setAttributes(attributes);

        searchResult.setEntities(Arrays.asList(entity1));
        return searchResult;
    }

    private Map<String, String> createMockAttributeLabelMap() {
        Map<String, String> map = new HashMap<>();
        map.put("Name", "name");
        map.put("Owner", "owner");
        map.put("Database", "database");
        return map;
    }

    private void invokeRunMethod(Map<String, Object> params) throws Exception {
        Method runMethod = SearchResultDownloadTask.class.getDeclaredMethod("run", Map.class);
        runMethod.setAccessible(true);
        runMethod.invoke(downloadTask, params);
    }

    private <T extends Throwable> void expectThrows(Runnable runnable) {
        try {
            runnable.run();
            throw new AssertionError("Expected " + RuntimeException.class.getSimpleName() + " to be thrown");
        } catch (Throwable throwable) {
            if (throwable instanceof RuntimeException) {
                return;
            }
            throw new AssertionError("Expected " + RuntimeException.class.getSimpleName() + " but got " +
                throwable.getClass().getSimpleName(), throwable);
        }
    }
}
