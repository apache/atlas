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

package org.apache.atlas.web.resources;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.discovery.SearchContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.PList;
import org.apache.atlas.model.audit.AtlasAuditEntry;
import org.apache.atlas.model.audit.AuditReductionCriteria;
import org.apache.atlas.model.audit.AuditSearchParameters;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.impexp.AsyncImportStatus;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasExportResult;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.impexp.AtlasServer;
import org.apache.atlas.model.impexp.ExportImportAuditEntry;
import org.apache.atlas.model.instance.AtlasCheckStateRequest;
import org.apache.atlas.model.instance.AtlasCheckStateResult;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.metrics.AtlasMetrics;
import org.apache.atlas.model.metrics.AtlasMetricsMapToChart;
import org.apache.atlas.model.metrics.AtlasMetricsStat;
import org.apache.atlas.model.patches.AtlasPatch.AtlasPatches;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.repository.audit.AtlasAuditReductionService;
import org.apache.atlas.repository.audit.AtlasAuditService;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.impexp.AtlasServerService;
import org.apache.atlas.repository.impexp.ExportImportAuditService;
import org.apache.atlas.repository.impexp.ExportService;
import org.apache.atlas.repository.impexp.ImportService;
import org.apache.atlas.repository.impexp.MigrationProgressService;
import org.apache.atlas.repository.patches.AtlasPatchManager;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.services.MetricsService;
import org.apache.atlas.services.PurgeService;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasMetricsUtil;
import org.apache.atlas.util.SearchTracker;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.web.model.DebugMetrics;
import org.apache.atlas.web.service.AtlasDebugMetricsSink;
import org.apache.atlas.web.service.ServiceState;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class AdminResourceTest {
    @Mock
    private ServiceState serviceState;

    @Mock
    private MetricsService metricsService;

    @Mock
    private AtlasTypeRegistry typeRegistry;

    @Mock
    private ExportService exportService;

    @Mock
    private ImportService importService;

    @Mock
    private SearchTracker activeSearches;

    @Mock
    private MigrationProgressService migrationProgressService;

    @Mock
    private AtlasServerService atlasServerService;

    @Mock
    private ExportImportAuditService exportImportAuditService;

    @Mock
    private AtlasEntityStore entityStore;

    @Mock
    private AtlasPatchManager patchManager;

    @Mock
    private AtlasAuditService auditService;

    @Mock
    private EntityAuditRepository auditRepository;

    @Mock
    private TaskManagement taskManagement;

    @Mock
    private AtlasDebugMetricsSink debugMetricsRESTSink;

    @Mock
    private AtlasAuditReductionService auditReductionService;

    @Mock
    private AtlasMetricsUtil atlasMetricsUtil;

    @Mock
    private PurgeService purgeService;

    @Mock
    private HttpServletRequest httpServletRequest;

    @Mock
    private HttpServletResponse httpServletResponse;

    @Mock
    private HttpSession httpSession;

    @Mock
    private Authentication authentication;

    @Mock
    private SecurityContext securityContext;

    @Mock
    private Configuration mockConfiguration;

    @Mock
    private InputStream inputStream;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testStatusOfActiveServerIsReturned() throws IOException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        AdminResource adminResource = new AdminResource(serviceState, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        Response      response      = adminResource.getStatus();

        assertEquals(response.getStatus(), HttpServletResponse.SC_OK);

        JsonNode entity = AtlasJson.parseToV1JsonNode((String) response.getEntity());

        assertEquals(entity.get("Status").asText(), "ACTIVE");
    }

    @Test
    public void testResourceGetsValueFromServiceState() throws IOException {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        AdminResource adminResource = new AdminResource(serviceState, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        Response      response      = adminResource.getStatus();

        verify(serviceState).getState();

        JsonNode entity = AtlasJson.parseToV1JsonNode((String) response.getEntity());

        assertEquals(entity.get("Status").asText(), "PASSIVE");
    }

    // Helper method to create AdminResource with all mocked dependencies
    private AdminResource createAdminResource() {
        return new AdminResource(serviceState, metricsService, typeRegistry, exportService, importService,
                activeSearches, migrationProgressService, atlasServerService, exportImportAuditService,
                entityStore, patchManager, auditService, auditRepository, taskManagement,
                debugMetricsRESTSink, auditReductionService, atlasMetricsUtil, purgeService);
    }

    // Helper method to inject HttpServletRequest via reflection
    private void injectHttpServletRequest(AdminResource adminResource) throws Exception {
        Field requestField = AdminResource.class.getDeclaredField("httpServletRequest");
        requestField.setAccessible(true);
        requestField.set(adminResource, httpServletRequest);
    }

    // Helper method to inject HttpServletResponse via reflection
    private void injectHttpServletResponse(AdminResource adminResource) throws Exception {
        Field responseField = AdminResource.class.getDeclaredField("httpServletResponse");
        responseField.setAccessible(true);
        responseField.set(adminResource, httpServletResponse);
    }

    @Test
    public void testGetThreadDump() {
        AdminResource adminResource = createAdminResource();
        String threadDump = adminResource.getThreadDump();

        assertNotNull(threadDump);
        assertTrue(threadDump.length() > 0);
        assertTrue(threadDump.contains("State:"));
    }

    @Test
    public void testGetVersion() throws Exception {
        AdminResource adminResource = createAdminResource();

        // Test version retrieval
        Response response = adminResource.getVersion();

        assertNotNull(response);
        assertEquals(response.getStatus(), HttpServletResponse.SC_OK);

        // Verify the response contains version information
        String responseEntity = (String) response.getEntity();
        assertNotNull(responseEntity);
        assertTrue(responseEntity.contains("Version") || responseEntity.contains("UNKNOWN"));
    }

    @Test
    public void testGetUserProfile() throws Exception {
        AdminResource adminResource = createAdminResource();

        // Mock HttpServletRequest and HttpSession
        when(httpServletRequest.getSession()).thenReturn(httpSession);
        when(httpSession.getAttribute("CSRF_TOKEN")).thenReturn("test-csrf-token");

        // Inject the mocked request
        injectHttpServletRequest(adminResource);

        Response response = adminResource.getUserProfile(httpServletRequest);

        assertNotNull(response);
        assertEquals(response.getStatus(), HttpServletResponse.SC_OK);

        String responseEntity = (String) response.getEntity();
        assertNotNull(responseEntity);
        assertTrue(responseEntity.contains("CSRF_TOKEN") || responseEntity.contains("atlas.rest-csrf.enabled"));
    }

    @Test
    public void testGetMetrics() throws Exception {
        AtlasMetrics mockMetrics = mock(AtlasMetrics.class);
        when(metricsService.getMetrics(anyBoolean())).thenReturn(mockMetrics);

        AdminResource adminResource = createAdminResource();

        AtlasMetrics result = adminResource.getMetrics(true);

        assertNotNull(result);
        verify(metricsService).getMetrics(true);
    }

    @Test
    public void testGetAllMetrics() throws Exception {
        List<AtlasMetricsStat> mockMetricsList = new ArrayList<>();
        AtlasMetricsStat mockStat = mock(AtlasMetricsStat.class);
        mockMetricsList.add(mockStat);

        when(metricsService.getAllMetricsStats(anyBoolean())).thenReturn(mockMetricsList);

        AdminResource adminResource = createAdminResource();

        List<AtlasMetricsStat> result = adminResource.getAllMetrics(true);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(metricsService).getAllMetricsStats(true);
    }

    @Test
    public void testGetMetricsByCollectionTime() throws Exception {
        AtlasMetricsStat mockStat = mock(AtlasMetricsStat.class);
        String collectionTime = "2023-01-01T00:00:00Z";

        when(metricsService.getMetricsStatByCollectionTime(collectionTime)).thenReturn(mockStat);

        AdminResource adminResource = createAdminResource();

        AtlasMetricsStat result = adminResource.getMetricsByCollectionTime(collectionTime);

        assertNotNull(result);
        verify(metricsService).getMetricsStatByCollectionTime(collectionTime);
    }

    @Test
    public void testPurgeByIds() throws Exception {
        Set<String> guids = new HashSet<>();
        guids.add("guid1");
        guids.add("guid2");

        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        List<AtlasEntityHeader> purgedEntities = new ArrayList<>();
        AtlasEntityHeader mockHeader = mock(AtlasEntityHeader.class);
        purgedEntities.add(mockHeader);

        when(entityStore.purgeByIds(guids)).thenReturn(mockResponse);
        when(mockResponse.getPurgedEntities()).thenReturn(purgedEntities);
        when(mockResponse.getPurgedEntitiesIds()).thenReturn(String.valueOf(new ArrayList<>(guids)));

        AdminResource adminResource = createAdminResource();

        EntityMutationResponse result = adminResource.purgeByIds(guids);

        assertNotNull(result);
        verify(entityStore).purgeByIds(guids);
    }

    @Test
    public void testPurgeByIdsWithEmptyGuidSet() throws Exception {
        Set<String> emptyGuids = new HashSet<>();

        EntityMutationResponse mockResponse = mock(EntityMutationResponse.class);
        when(entityStore.purgeByIds(emptyGuids)).thenReturn(mockResponse);
        when(mockResponse.getPurgedEntities()).thenReturn(new ArrayList<>());

        AdminResource adminResource = createAdminResource();

        EntityMutationResponse result = adminResource.purgeByIds(emptyGuids);

        assertNotNull(result);
        verify(entityStore).purgeByIds(emptyGuids);
        // Should not call audit service for empty results
        verify(auditService, never()).add(any(), anyString(), any(), anyInt());
    }

    @Test
    public void testGetActiveSearches() {
        Set<String> mockActiveSearches = new HashSet<>();
        mockActiveSearches.add("search1");
        mockActiveSearches.add("search2");

        when(activeSearches.getActiveSearches()).thenReturn(mockActiveSearches);

        AdminResource adminResource = createAdminResource();

        Set<String> result = adminResource.getActiveSearches();

        assertNotNull(result);
        assertEquals(result.size(), 2);
        assertTrue(result.contains("search1"));
        assertTrue(result.contains("search2"));
        verify(activeSearches).getActiveSearches();
    }

    @Test
    public void testTerminateActiveSearch() {
        String searchId = "test-search-id";
        SearchContext mockSearchContext = mock(SearchContext.class);

        when(activeSearches.terminate(searchId)).thenReturn(mockSearchContext);

        AdminResource adminResource = createAdminResource();

        boolean result = adminResource.terminateActiveSearch(searchId);

        assertTrue(result);
        verify(activeSearches).terminate(searchId);
    }

    @Test
    public void testTerminateActiveSearchNotFound() {
        String searchId = "non-existent-search-id";

        when(activeSearches.terminate(searchId)).thenReturn(null);

        AdminResource adminResource = createAdminResource();

        boolean result = adminResource.terminateActiveSearch(searchId);

        assertFalse(result);
        verify(activeSearches).terminate(searchId);
    }

    @Test
    public void testCheckState() throws Exception {
        AtlasCheckStateRequest request = mock(AtlasCheckStateRequest.class);
        AtlasCheckStateResult mockResult = mock(AtlasCheckStateResult.class);

        when(entityStore.checkState(request)).thenReturn(mockResult);

        AdminResource adminResource = createAdminResource();

        AtlasCheckStateResult result = adminResource.checkState(request);

        assertNotNull(result);
        verify(entityStore).checkState(request);
    }

    @Test
    public void testGetAtlasPatches() {
        AtlasPatches mockPatches = mock(AtlasPatches.class);

        when(patchManager.getAllPatches()).thenReturn(mockPatches);

        AdminResource adminResource = createAdminResource();

        AtlasPatches result = adminResource.getAtlasPatches();

        assertNotNull(result);
        verify(patchManager).getAllPatches();
    }

    @Test
    public void testGetTaskStatus() throws Exception {
        List<String> guids = new ArrayList<>();
        guids.add("task1");
        guids.add("task2");

        List<AtlasTask> mockTasks = new ArrayList<>();
        AtlasTask mockTask = mock(AtlasTask.class);
        mockTasks.add(mockTask);

        when(taskManagement.getByGuids(guids)).thenReturn(mockTasks);

        AdminResource adminResource = createAdminResource();

        List<AtlasTask> result = adminResource.getTaskStatus(guids);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(taskManagement).getByGuids(guids);
    }

    @Test
    public void testGetTaskStatusWithEmptyGuids() throws Exception {
        List<AtlasTask> mockAllTasks = new ArrayList<>();
        AtlasTask mockTask = mock(AtlasTask.class);
        mockAllTasks.add(mockTask);

        when(taskManagement.getAll()).thenReturn(mockAllTasks);

        AdminResource adminResource = createAdminResource();

        List<AtlasTask> result = adminResource.getTaskStatus(null);

        assertNotNull(result);
        verify(taskManagement).getAll();
    }

    @Test
    public void testDeleteTask() throws Exception {
        List<String> guids = new ArrayList<>();
        guids.add("task1");
        guids.add("task2");

        AdminResource adminResource = createAdminResource();

        adminResource.deleteTask(guids);

        verify(taskManagement).deleteByGuids(guids);
    }

    @Test
    public void testDeleteTaskWithEmptyGuids() throws Exception {
        AdminResource adminResource = createAdminResource();

        adminResource.deleteTask(null);

        // Should not call deleteByGuids when guids is null or empty
        verify(taskManagement, never()).deleteByGuids(any());
    }

    @Test
    public void testGetDebugMetrics() {
        Map<String, DebugMetrics> mockMetrics = new HashMap<>();
        DebugMetrics mockDebugMetrics = mock(DebugMetrics.class);
        mockMetrics.put("test", mockDebugMetrics);

        when(debugMetricsRESTSink.getMetrics()).thenReturn((HashMap<String, DebugMetrics>) mockMetrics);

        AdminResource adminResource = createAdminResource();

        Map<String, DebugMetrics> result = adminResource.getDebugMetrics();

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(debugMetricsRESTSink).getMetrics();
    }

    @Test
    public void testServiceLivelinessWhenActive() throws Exception {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);

        AdminResource adminResource = createAdminResource();

        Response response = adminResource.serviceLiveliness();

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        assertEquals(response.getEntity(), "Service is live");
    }

    @Test
    public void testServiceLivelinessWhenMigrating() throws Exception {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.MIGRATING);

        AdminResource adminResource = createAdminResource();

        Response response = adminResource.serviceLiveliness();

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        assertEquals(response.getEntity(), "Service is live");
    }

    @Test
    public void testServiceLivelinessWhenNotActive() {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);

        AdminResource adminResource = createAdminResource();

        try {
            adminResource.serviceLiveliness();
            fail("Expected AtlasBaseException to be thrown");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INTERNAL_ERROR);
            assertTrue(e.getMessage().contains("Atlas Service is not live"));
        }
    }

    @Test
    public void testServiceReadinessWhenActive() throws Exception {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(atlasMetricsUtil.isIndexStoreActive()).thenReturn(true);
        when(atlasMetricsUtil.isBackendStoreActive()).thenReturn(true);

        AdminResource adminResource = createAdminResource();

        Response response = adminResource.serviceReadiness();

        assertNotNull(response);
        assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        assertEquals(response.getEntity(), "Service is ready to accept requests");
    }

    @Test
    public void testServiceReadinessWhenIndexStoreInactive() {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(atlasMetricsUtil.isIndexStoreActive()).thenReturn(false);
        when(atlasMetricsUtil.isBackendStoreActive()).thenReturn(true);

        AdminResource adminResource = createAdminResource();

        try {
            adminResource.serviceReadiness();
            fail("Expected AtlasBaseException to be thrown");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INTERNAL_ERROR);
            assertTrue(e.getMessage().contains("Service not ready to accept client requests"));
        }
    }

    @Test
    public void testServiceReadinessWhenBackendStoreInactive() {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.ACTIVE);
        when(atlasMetricsUtil.isIndexStoreActive()).thenReturn(true);
        when(atlasMetricsUtil.isBackendStoreActive()).thenReturn(false);

        AdminResource adminResource = createAdminResource();

        try {
            adminResource.serviceReadiness();
            fail("Expected AtlasBaseException to be thrown");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INTERNAL_ERROR);
            assertTrue(e.getMessage().contains("Service not ready to accept client requests"));
        }
    }

    @Test
    public void testServiceReadinessWhenServiceNotActive() {
        when(serviceState.getState()).thenReturn(ServiceState.ServiceStateValue.PASSIVE);
        when(atlasMetricsUtil.isIndexStoreActive()).thenReturn(true);
        when(atlasMetricsUtil.isBackendStoreActive()).thenReturn(true);

        AdminResource adminResource = createAdminResource();

        try {
            adminResource.serviceReadiness();
            fail("Expected AtlasBaseException to be thrown");
        } catch (AtlasBaseException e) {
            assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INTERNAL_ERROR);
            assertTrue(e.getMessage().contains("Service not ready to accept client requests"));
        }
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testGetMetricsByCollectionTimeWithException() throws Exception {
        String collectionTime = "invalid-time";

        when(metricsService.getMetricsStatByCollectionTime(collectionTime))
                .thenThrow(new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid collection time"));

        AdminResource adminResource = createAdminResource();

        adminResource.getMetricsByCollectionTime(collectionTime);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testCheckStateWithException() throws Exception {
        AtlasCheckStateRequest request = mock(AtlasCheckStateRequest.class);

        when(entityStore.checkState(request))
                .thenThrow(new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Check state failed"));

        AdminResource adminResource = createAdminResource();

        adminResource.checkState(request);
    }

    @Test
    public void testConstructorWithNullAtlasProperties() throws Exception {
        AdminResource adminResource = createAdminResource();

        // Use reflection to verify default values are set correctly
        Field defaultUIVersionField = AdminResource.class.getDeclaredField("defaultUIVersion");
        defaultUIVersionField.setAccessible(true);
        String defaultUIVersion = (String) defaultUIVersionField.get(adminResource);
        assertNotNull(defaultUIVersion);

        Field isTimezoneFormatEnabledField = AdminResource.class.getDeclaredField("isTimezoneFormatEnabled");
        isTimezoneFormatEnabledField.setAccessible(true);
        boolean isTimezoneFormatEnabled = (boolean) isTimezoneFormatEnabledField.get(adminResource);
        // This can be either true or false depending on configuration

        Field isDebugMetricsEnabledField = AdminResource.class.getDeclaredField("isDebugMetricsEnabled");
        isDebugMetricsEnabledField.setAccessible(true);
        boolean isDebugMetricsEnabled = (boolean) isDebugMetricsEnabledField.get(adminResource);
        // This can be either true or false depending on configuration

        assertNotNull(adminResource);
    }

    @Test
    public void testGetUserProfileWithAuthentication() throws Exception {
        AdminResource adminResource = createAdminResource();

        // Mock authentication
        Collection<GrantedAuthority> authorities = new ArrayList<>();
        authorities.add(new SimpleGrantedAuthority("ROLE_USER"));
        authorities.add(new SimpleGrantedAuthority("ROLE_ADMIN"));

        when(authentication.getName()).thenReturn("testuser");
        when(authentication.getAuthorities()).thenReturn((Collection) authorities);
        when(securityContext.getAuthentication()).thenReturn(authentication);

        // Mock HttpServletRequest and HttpSession
        when(httpServletRequest.getSession()).thenReturn(httpSession);
        when(httpSession.getAttribute("CSRF_TOKEN")).thenReturn(null);

        // Inject the mocked request
        injectHttpServletRequest(adminResource);

        try (MockedStatic<SecurityContextHolder> mockedStatic = mockStatic(SecurityContextHolder.class)) {
            mockedStatic.when(SecurityContextHolder::getContext).thenReturn(securityContext);

            Response response = adminResource.getUserProfile(httpServletRequest);

            assertNotNull(response);
            assertEquals(response.getStatus(), HttpServletResponse.SC_OK);

            String responseEntity = (String) response.getEntity();
            assertNotNull(responseEntity);
            assertTrue(responseEntity.contains("userName") || responseEntity.contains("groups"));
        }
    }

    @Test
    public void testScheduleSaveAndDeleteMetrics() throws Exception {
        AdminResource adminResource = createAdminResource();

        AtlasMetrics mockMetrics = mock(AtlasMetrics.class);
        AtlasMetricsStat mockMetricsStat = mock(AtlasMetricsStat.class);

        // Mock the getMetric method to return a valid timestamp for collection time
        when(mockMetrics.getMetric(anyString(), anyString())).thenReturn(System.currentTimeMillis());
        when(metricsService.getMetrics(false)).thenReturn(mockMetrics);

        adminResource.scheduleSaveAndDeleteMetrics();

        verify(metricsService).getMetrics(false);
        verify(metricsService).saveMetricsStat(any(AtlasMetricsStat.class));
        verify(metricsService).purgeMetricsStats();
    }

    @Test
    public void testGetMetricsInTimeRange() throws Exception {
        String startTime = "1609459200000"; // Jan 1, 2021
        String endTime = "1640995200000"; // Jan 1, 2022
        List<String> typeNames = new ArrayList<>();
        typeNames.add("DataSet");
        typeNames.add("Table");

        List<AtlasMetricsStat> mockResults = new ArrayList<>();
        AtlasMetricsStat mockStat = mock(AtlasMetricsStat.class);
        mockResults.add(mockStat);

        when(metricsService.getMetricsInRangeByTypeNames(Long.parseLong(startTime), Long.parseLong(endTime), typeNames))
                .thenReturn(mockResults);

        AdminResource adminResource = createAdminResource();

        List<AtlasMetricsStat> result = adminResource.getMetricsInTimeRange(startTime, endTime, typeNames);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(metricsService).getMetricsInRangeByTypeNames(Long.parseLong(startTime), Long.parseLong(endTime), typeNames);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testGetMetricsInTimeRangeWithBlankStartTime() throws Exception {
        AdminResource adminResource = createAdminResource();
        adminResource.getMetricsInTimeRange("", "1640995200000", new ArrayList<>());
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testGetMetricsInTimeRangeWithBlankEndTime() throws Exception {
        AdminResource adminResource = createAdminResource();
        adminResource.getMetricsInTimeRange("1609459200000", "", new ArrayList<>());
    }

    @Test
    public void testGetMetricsForChartByTypeNames() throws Exception {
        String startTime = "1609459200000";
        String endTime = "1640995200000";
        List<String> typeNames = new ArrayList<>();
        typeNames.add("DataSet");

        Map<String, List<AtlasMetricsMapToChart>> mockResults = new HashMap<>();
        List<AtlasMetricsMapToChart> chartData = new ArrayList<>();
        AtlasMetricsMapToChart mockChart = mock(AtlasMetricsMapToChart.class);
        chartData.add(mockChart);
        mockResults.put("DataSet", chartData);

        when(metricsService.getMetricsForChartByTypeNames(Long.parseLong(startTime), Long.parseLong(endTime), typeNames))
                .thenReturn(mockResults);

        AdminResource adminResource = createAdminResource();

        Map<String, List<AtlasMetricsMapToChart>> result = adminResource.getMetricsForChartByTypeNames(startTime, endTime, typeNames);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(metricsService).getMetricsForChartByTypeNames(Long.parseLong(startTime), Long.parseLong(endTime), typeNames);
    }

    @Test(expectedExceptions = AtlasBaseException.class)
    public void testGetMetricsForChartByTypeNamesWithBlankStartTime() throws Exception {
        AdminResource adminResource = createAdminResource();
        adminResource.getMetricsForChartByTypeNames("", "1640995200000", new ArrayList<>());
    }

    @Test
    public void testAbortAsyncImport() throws Exception {
        String importId = "import-123";

        AdminResource adminResource = createAdminResource();

        adminResource.abortAsyncImport(importId);

        verify(importService).abortAsyncImport(importId);
    }

    @Test
    public void testGetAsyncImportStatus() throws Exception {
        int offset = 0;
        int limit = 10;

        PList<AsyncImportStatus> mockResult = mock(PList.class);

        when(importService.getAsyncImportsStatus(offset, limit)).thenReturn(mockResult);

        AdminResource adminResource = createAdminResource();

        PList<AsyncImportStatus> result = adminResource.getAsyncImportStatus(offset, limit);

        assertNotNull(result);
        verify(importService).getAsyncImportsStatus(offset, limit);
    }

    @Test
    public void testGetAsyncImportStatusById() throws Exception {
        String importId = "import-123";

        AtlasAsyncImportRequest mockRequest = mock(AtlasAsyncImportRequest.class);

        when(importService.getAsyncImportRequest(importId)).thenReturn(mockRequest);

        AdminResource adminResource = createAdminResource();

        AtlasAsyncImportRequest result = adminResource.getAsyncImportStatusById(importId);

        assertNotNull(result);
        verify(importService).getAsyncImportRequest(importId);
    }

    @Test
    public void testGetCluster() throws Exception {
        String serverName = "test-server";

        AtlasServer mockServer = mock(AtlasServer.class);

        when(atlasServerService.get(any(AtlasServer.class))).thenReturn(mockServer);

        AdminResource adminResource = createAdminResource();

        AtlasServer result = adminResource.getCluster(serverName);

        assertNotNull(result);
        verify(atlasServerService).get(any(AtlasServer.class));
    }

    @Test
    public void testGetExportImportAudit() throws Exception {
        String serverName = "test-server";
        String userName = "test-user";
        String operation = "EXPORT";
        String startTime = "2021-01-01";
        String endTime = "2021-12-31";
        int limit = 10;
        int offset = 0;

        List<ExportImportAuditEntry> mockResults = new ArrayList<>();
        ExportImportAuditEntry mockEntry = mock(ExportImportAuditEntry.class);
        mockResults.add(mockEntry);

        when(exportImportAuditService.get(userName, operation, serverName, startTime, endTime, limit, offset))
                .thenReturn(mockResults);

        AdminResource adminResource = createAdminResource();

        List<ExportImportAuditEntry> result = adminResource.getExportImportAudit(serverName, userName, operation,
                startTime, endTime, limit, offset);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(exportImportAuditService).get(userName, operation, serverName, startTime, endTime, limit, offset);
    }

    @Test
    public void testAgeoutAuditDataWithConfig() throws Exception {
        AuditReductionCriteria criteria = mock(AuditReductionCriteria.class);
        List<AtlasTask> mockTasks = new ArrayList<>();
        AtlasTask mockTask = mock(AtlasTask.class);
        mockTasks.add(mockTask);

        when(auditReductionService.startAuditAgingByConfig()).thenReturn(mockTasks);

        AdminResource adminResource = createAdminResource();

        List<AtlasTask> result = adminResource.ageoutAuditData(criteria, true);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(auditReductionService).startAuditAgingByConfig();
    }

    @Test
    public void testAgeoutAuditDataWithCriteria() throws Exception {
        AuditReductionCriteria criteria = mock(AuditReductionCriteria.class);
        when(criteria.isAuditAgingEnabled()).thenReturn(true);
        when(criteria.getDefaultAgeoutTTLInDays()).thenReturn(0);

        List<Map<String, Object>> mockCriteriaMap = new ArrayList<>();
        Map<String, Object> criteriaItem = new HashMap<>();
        criteriaItem.put("type", "test");
        mockCriteriaMap.add(criteriaItem);

        List<AtlasTask> mockTasks = new ArrayList<>();
        AtlasTask mockTask = mock(AtlasTask.class);
        mockTasks.add(mockTask);

        when(auditReductionService.buildAgeoutCriteriaForAllAgingTypes(criteria)).thenReturn(mockCriteriaMap);
        when(auditReductionService.startAuditAgingByCriteria(mockCriteriaMap)).thenReturn(mockTasks);

        AdminResource adminResource = createAdminResource();

        List<AtlasTask> result = adminResource.ageoutAuditData(criteria, false);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(auditReductionService).buildAgeoutCriteriaForAllAgingTypes(criteria);
        verify(auditReductionService).startAuditAgingByCriteria(mockCriteriaMap);
    }

    @Test
    public void testAgeoutAuditDataWithDisabledAging() throws Exception {
        AuditReductionCriteria criteria = mock(AuditReductionCriteria.class);
        when(criteria.isAuditAgingEnabled()).thenReturn(false);

        AdminResource adminResource = createAdminResource();

        List<AtlasTask> result = adminResource.ageoutAuditData(criteria, false);

        assertNull(result);
        verify(auditReductionService, never()).startAuditAgingByCriteria(any());
    }

    @Test
    public void testGetAtlasAudits() throws Exception {
        AuditSearchParameters searchParameters = mock(AuditSearchParameters.class);
        List<AtlasAuditEntry> mockResults = new ArrayList<>();
        AtlasAuditEntry mockEntry = mock(AtlasAuditEntry.class);
        mockResults.add(mockEntry);

        when(auditService.get(searchParameters)).thenReturn(mockResults);

        AdminResource adminResource = createAdminResource();

        List<AtlasAuditEntry> result = adminResource.getAtlasAudits(searchParameters);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(auditService).get(searchParameters);
    }

    @Test
    public void testGetAuditDetails() throws Exception {
        String auditGuid = "audit-123";
        int limit = 10;
        int offset = 0;

        AtlasAuditEntry mockAuditEntry = mock(AtlasAuditEntry.class);
        when(mockAuditEntry.getResult()).thenReturn("guid1,guid2");
        when(mockAuditEntry.getOperation()).thenReturn(AtlasAuditEntry.AuditOperation.PURGE);

        List<EntityAuditEventV2> mockEvents = new ArrayList<>();
        EntityAuditEventV2 mockEvent = mock(EntityAuditEventV2.class);
        AtlasEntityHeader mockHeader = mock(AtlasEntityHeader.class);
        when(mockEvent.getEntityHeader()).thenReturn(mockHeader);
        mockEvents.add(mockEvent);

        when(auditService.toAtlasAuditEntry(any(AtlasEntityWithExtInfo.class))).thenReturn(mockAuditEntry);
        when(entityStore.getById(auditGuid, false, true)).thenReturn(mock(AtlasEntityWithExtInfo.class));
        when(auditRepository.listEventsV2(anyString(), any(EntityAuditEventV2.EntityAuditActionV2.class), any(), any(Short.class))).thenReturn(mockEvents);

        AdminResource adminResource = createAdminResource();

        List<AtlasEntityHeader> result = adminResource.getAuditDetails(auditGuid, limit, offset);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        verify(entityStore).getById(auditGuid, false, true);
        verify(auditService).toAtlasAuditEntry(any());
    }

    @Test
    public void testGetAuditDetailsWithNullResult() throws Exception {
        String auditGuid = "audit-123";

        AtlasAuditEntry mockAuditEntry = mock(AtlasAuditEntry.class);
        when(mockAuditEntry.getResult()).thenReturn(null);

        when(auditService.toAtlasAuditEntry(any())).thenReturn(mockAuditEntry);
        when(entityStore.getById(auditGuid, false, true)).thenReturn(mock(AtlasEntityWithExtInfo.class));

        AdminResource adminResource = createAdminResource();

        List<AtlasEntityHeader> result = adminResource.getAuditDetails(auditGuid, 10, 0);

        assertNotNull(result);
        assertTrue(result.isEmpty());
        verify(entityStore).getById(auditGuid, false, true);
    }

    @Test
    public void testGetEditableEntityTypesWithStringValue() throws Exception {
        when(mockConfiguration.containsKey("atlas.ui.editable.entity.types")).thenReturn(true);
        when(mockConfiguration.getProperty("atlas.ui.editable.entity.types")).thenReturn("DataSet,Table");

        AdminResource adminResource = createAdminResource();

        // Use reflection to call the private method
        java.lang.reflect.Method method = AdminResource.class.getDeclaredMethod("getEditableEntityTypes", Configuration.class);
        method.setAccessible(true);
        String result = (String) method.invoke(adminResource, mockConfiguration);

        assertEquals(result, "DataSet,Table");
    }

    @Test
    public void testGetEditableEntityTypesWithCollectionValue() throws Exception {
        Collection<String> typeCollection = new ArrayList<>();
        typeCollection.add("DataSet");
        typeCollection.add("Table");

        when(mockConfiguration.containsKey("atlas.ui.editable.entity.types")).thenReturn(true);
        when(mockConfiguration.getProperty("atlas.ui.editable.entity.types")).thenReturn(typeCollection);

        AdminResource adminResource = createAdminResource();

        // Use reflection to call the private method
        java.lang.reflect.Method method = AdminResource.class.getDeclaredMethod("getEditableEntityTypes", Configuration.class);
        method.setAccessible(true);
        String result = (String) method.invoke(adminResource, mockConfiguration);

        assertEquals(result, "DataSet,Table");
    }

    @Test
    public void testGetEditableEntityTypesWithNullConfig() throws Exception {
        AdminResource adminResource = createAdminResource();

        // Use reflection to call the private method
        java.lang.reflect.Method method = AdminResource.class.getDeclaredMethod("getEditableEntityTypes", Configuration.class);
        method.setAccessible(true);
        String result = (String) method.invoke(adminResource, (Configuration) null);

        assertEquals(result, "hdfs_path");
    }

    @Test
    public void testSaveMetricsPrivateMethod() throws Exception {
        AtlasMetrics mockMetrics = mock(AtlasMetrics.class);
        when(mockMetrics.getMetric(anyString(), anyString())).thenReturn(System.currentTimeMillis());
        when(metricsService.getMetrics(false)).thenReturn(mockMetrics);

        AdminResource adminResource = createAdminResource();

        // Use reflection to call the private method
        java.lang.reflect.Method method = AdminResource.class.getDeclaredMethod("saveMetrics");
        method.setAccessible(true);
        method.invoke(adminResource);

        verify(metricsService).getMetrics(false);
        verify(metricsService).saveMetricsStat(any(AtlasMetricsStat.class));
    }

    @Test
    public void testAcquireExportImportLockWhenAlreadyLocked() throws Exception {
        AdminResource adminResource = createAdminResource();

        // Use reflection to get the lock and lock it
        Field lockField = AdminResource.class.getDeclaredField("importExportOperationLock");
        lockField.setAccessible(true);
        java.util.concurrent.locks.ReentrantLock lock = (java.util.concurrent.locks.ReentrantLock) lockField.get(adminResource);
        lock.lock();

        try {
            // Use reflection to call the private method
            java.lang.reflect.Method method = AdminResource.class.getDeclaredMethod("acquireExportImportLock", String.class);
            method.setAccessible(true);

            try {
                method.invoke(adminResource, "test");
                fail("Expected AtlasBaseException to be thrown");
            } catch (java.lang.reflect.InvocationTargetException e) {
                assertTrue(e.getCause() instanceof AtlasBaseException);
                assertEquals(((AtlasBaseException) e.getCause()).getAtlasErrorCode(), AtlasErrorCode.FAILED_TO_OBTAIN_IMPORT_EXPORT_LOCK);
            }
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void testExportWithSkipLineageOption() throws Exception {
        AdminResource adminResource = createAdminResource();
        injectHttpServletResponse(adminResource);
        injectHttpServletRequest(adminResource);

        AtlasExportRequest request = mock(AtlasExportRequest.class);
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasExportRequest.OPTION_SKIP_LINEAGE, "true");

        when(request.getOptions()).thenReturn(options);
        when(httpServletResponse.getOutputStream()).thenReturn(mock(javax.servlet.ServletOutputStream.class));

        AtlasExportResult mockResult = mock(AtlasExportResult.class);
        when(mockResult.getRequest()).thenReturn(request);
        when(mockResult.getOperationStatus()).thenReturn(AtlasExportResult.OperationStatus.SUCCESS);

        when(exportService.run(any(), eq(request), anyString(), anyString(), anyString())).thenReturn(mockResult);

        try (MockedStatic<AtlasAuthorizationUtils> mockedUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<org.apache.commons.collections.CollectionUtils> mockedCollectionUtils = mockStatic(org.apache.commons.collections.CollectionUtils.class)) {
            mockedUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(org.apache.atlas.authorize.AtlasAdminAccessRequest.class), anyString())).then(invocation -> null);
            mockedUtils.when(AtlasAuthorizationUtils::getCurrentUserName).thenReturn("testuser");
            mockedUtils.when(() -> AtlasAuthorizationUtils.getRequestIpAddress(any())).thenReturn("127.0.0.1");

            mockedCollectionUtils.when(() -> org.apache.commons.collections.CollectionUtils.isNotEmpty(any())).thenReturn(true);

            try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
                mockedServlets.when(() -> Servlets.getHostName(any())).thenReturn("localhost");

                // This should not acquire lock due to OPTION_SKIP_LINEAGE
                Response response = adminResource.export(request);

                assertNotNull(response);
                verify(exportService).run(any(), eq(request), anyString(), anyString(), anyString());
            }
        }
    }

    @Test
    public void testExportWithEmptyExportAndOmitZipResponse() throws Exception {
        AdminResource adminResource = createAdminResource();
        injectHttpServletResponse(adminResource);
        injectHttpServletRequest(adminResource);

        AtlasExportRequest request = mock(AtlasExportRequest.class);
        when(request.getOptions()).thenReturn(null);
        when(request.getOmitZipResponseForEmptyExport()).thenReturn(true);
        when(httpServletResponse.getOutputStream()).thenReturn(mock(javax.servlet.ServletOutputStream.class));

        AtlasExportResult mockResult = mock(AtlasExportResult.class);
        when(mockResult.getRequest()).thenReturn(request);
        when(mockResult.getOperationStatus()).thenReturn(AtlasExportResult.OperationStatus.SUCCESS);

        when(exportService.run(any(), eq(request), anyString(), anyString(), anyString())).thenReturn(mockResult);

        try (MockedStatic<AtlasAuthorizationUtils> mockedUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<org.apache.commons.collections.CollectionUtils> mockedCollectionUtils = mockStatic(org.apache.commons.collections.CollectionUtils.class)) {
            mockedUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(org.apache.atlas.authorize.AtlasAdminAccessRequest.class), anyString())).then(invocation -> null);
            mockedUtils.when(AtlasAuthorizationUtils::getCurrentUserName).thenReturn("testuser");
            mockedUtils.when(() -> AtlasAuthorizationUtils.getRequestIpAddress(any())).thenReturn("127.0.0.1");

            mockedCollectionUtils.when(() -> org.apache.commons.collections.CollectionUtils.isNotEmpty(any())).thenReturn(false);

            try (MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
                mockedServlets.when(() -> Servlets.getHostName(any())).thenReturn("localhost");

                Response response = adminResource.export(request);

                assertNotNull(response);
                // Should return NO_CONTENT for empty export with omit flag
                assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());
            }
        }
    }

    @Test
    public void testImportDataWithEmptyZipException() throws Exception {
        AdminResource adminResource = createAdminResource();
        injectHttpServletRequest(adminResource);

        String jsonData = "{}";

        AtlasImportRequest mockRequest = mock(AtlasImportRequest.class);
        when(mockRequest.getOptions()).thenReturn(new HashMap<>());

        when(importService.run(any(InputStream.class), any(AtlasImportRequest.class), anyString(), anyString(), anyString()))
                .thenThrow(new AtlasBaseException(AtlasErrorCode.IMPORT_ATTEMPTING_EMPTY_ZIP, "Empty zip"));

        try (MockedStatic<AtlasAuthorizationUtils> mockedUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<AtlasType> mockedType = mockStatic(org.apache.atlas.type.AtlasType.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(org.apache.atlas.authorize.AtlasAdminAccessRequest.class), anyString())).then(invocation -> null);
            mockedType.when(() -> AtlasType.fromJson(jsonData, AtlasImportRequest.class)).thenReturn(mockRequest);
            mockedServlets.when(() -> Servlets.getUserName(any())).thenReturn("testuser");
            mockedServlets.when(() -> Servlets.getHostName(any())).thenReturn("localhost");
            mockedUtils.when(() -> AtlasAuthorizationUtils.getRequestIpAddress(any())).thenReturn("127.0.0.1");

            AtlasImportResult result = adminResource.importData(jsonData, inputStream);

            assertNotNull(result);
            // Should return empty result for IMPORT_ATTEMPTING_EMPTY_ZIP - this specific exception returns early
            verify(importService).run(any(InputStream.class), any(AtlasImportRequest.class), anyString(), anyString(), anyString());
        }
    }

    @Test
    public void testImportDataWithGenericException() throws Exception {
        AdminResource adminResource = createAdminResource();
        injectHttpServletRequest(adminResource);

        String jsonData = "{}";

        AtlasImportRequest mockRequest = mock(AtlasImportRequest.class);
        when(mockRequest.getOptions()).thenReturn(new HashMap<>());

        when(importService.run(any(InputStream.class), any(AtlasImportRequest.class), anyString(), anyString(), anyString()))
                .thenThrow(new RuntimeException("Generic error"));

        try (MockedStatic<AtlasAuthorizationUtils> mockedUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<AtlasType> mockedType = mockStatic(org.apache.atlas.type.AtlasType.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(org.apache.atlas.authorize.AtlasAdminAccessRequest.class), anyString())).then(invocation -> null);
            mockedType.when(() -> AtlasType.fromJson(jsonData, AtlasImportRequest.class)).thenReturn(mockRequest);
            mockedServlets.when(() -> Servlets.getUserName(any())).thenReturn("testuser");
            mockedServlets.when(() -> Servlets.getHostName(any())).thenReturn("localhost");
            mockedUtils.when(() -> AtlasAuthorizationUtils.getRequestIpAddress(any())).thenReturn("127.0.0.1");

            try {
                adminResource.importData(jsonData, inputStream);
                fail("Expected AtlasBaseException to be thrown");
            } catch (AtlasBaseException e) {
                // Expected
                assertNotNull(e);
            }
        }
    }

    @Test
    public void testImportDataWithReplicatedFromOption() throws Exception {
        AdminResource adminResource = createAdminResource();
        injectHttpServletRequest(adminResource);

        String jsonData = "{}";

        AtlasImportRequest mockRequest = mock(AtlasImportRequest.class);
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, "source-cluster");
        when(mockRequest.getOptions()).thenReturn((Map) options);

        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        when(mockResult.getOperationStatus()).thenReturn(AtlasImportResult.OperationStatus.SUCCESS);
        when(mockResult.getProcessedEntities()).thenReturn(new ArrayList<>());
        when(mockResult.getMetrics()).thenReturn(new HashMap<String, Integer>());
        when(mockResult.getExportResult()).thenReturn(mock(AtlasExportResult.class));
        when(mockResult.getExportResult().getRequest()).thenReturn(null);

        when(importService.run(any(InputStream.class), any(AtlasImportRequest.class), anyString(), anyString(), anyString()))
                .thenReturn(mockResult);

        try (MockedStatic<AtlasAuthorizationUtils> mockedUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<AtlasType> mockedType = mockStatic(org.apache.atlas.type.AtlasType.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class);
                MockedStatic<AtlasJson> mockedJson = mockStatic(AtlasJson.class)) {
            mockedUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(org.apache.atlas.authorize.AtlasAdminAccessRequest.class), anyString())).then(invocation -> null);
            mockedType.when(() -> AtlasType.fromJson(jsonData, AtlasImportRequest.class)).thenReturn(mockRequest);
            mockedServlets.when(() -> Servlets.getUserName(any())).thenReturn("testuser");
            mockedServlets.when(() -> Servlets.getHostName(any())).thenReturn("localhost");
            mockedUtils.when(() -> AtlasAuthorizationUtils.getRequestIpAddress(any())).thenReturn("127.0.0.1");
            mockedJson.when(() -> AtlasJson.toJson(any())).thenReturn("{}");

            AtlasImportResult result = adminResource.importData(jsonData, inputStream);

            assertNotNull(result);
            // This path should not acquire the lock due to OPTION_KEY_REPLICATED_FROM
            verify(importService).run(any(InputStream.class), any(AtlasImportRequest.class), anyString(), anyString(), anyString());
            verify(auditService).add(any(), anyString(), anyString(), anyLong());
        }
    }

    @Test
    public void testImportAsyncWithEmptyZipException() throws Exception {
        AdminResource adminResource = createAdminResource();
        injectHttpServletRequest(adminResource);

        String jsonData = "{}";

        AtlasImportRequest mockRequest = mock(AtlasImportRequest.class);
        when(mockRequest.getOptions()).thenReturn(new HashMap<>());

        when(importService.run(any(AtlasImportRequest.class), any(InputStream.class), anyString(), anyString(), anyString()))
                .thenThrow(new AtlasBaseException(AtlasErrorCode.IMPORT_ATTEMPTING_EMPTY_ZIP, "Empty zip"));

        try (MockedStatic<AtlasAuthorizationUtils> mockedUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<AtlasType> mockedType = mockStatic(org.apache.atlas.type.AtlasType.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(org.apache.atlas.authorize.AtlasAdminAccessRequest.class), anyString())).then(invocation -> null);
            mockedUtils.when(() -> AtlasAuthorizationUtils.getRequestIpAddress(any())).thenReturn("127.0.0.1");
            mockedServlets.when(() -> Servlets.getUserName(any())).thenReturn("testuser");
            mockedServlets.when(() -> Servlets.getHostName(any())).thenReturn("localhost");
            mockedType.when(() -> AtlasType.fromJson(jsonData, AtlasImportRequest.class)).thenReturn(mockRequest);

            AtlasAsyncImportRequest result = adminResource.importAsync(jsonData, inputStream);

            assertNotNull(result);
        }
    }

    @Test
    public void testImportFileWithReplicatedFromOption() throws Exception {
        AdminResource adminResource = createAdminResource();
        injectHttpServletRequest(adminResource);

        String jsonData = "{}";

        AtlasImportRequest mockRequest = mock(AtlasImportRequest.class);
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, "source-cluster");
        when(mockRequest.getOptions()).thenReturn((Map) options);

        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        when(mockResult.getOperationStatus()).thenReturn(AtlasImportResult.OperationStatus.SUCCESS);

        when(importService.run(any(AtlasImportRequest.class), anyString(), anyString(), anyString()))
                .thenReturn(mockResult);

        try (MockedStatic<AtlasAuthorizationUtils> mockedUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<AtlasType> mockedType = mockStatic(org.apache.atlas.type.AtlasType.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(org.apache.atlas.authorize.AtlasAdminAccessRequest.class), anyString())).then(invocation -> null);
            mockedUtils.when(AtlasAuthorizationUtils::getCurrentUserName).thenReturn("testuser");
            mockedUtils.when(() -> AtlasAuthorizationUtils.getRequestIpAddress(any())).thenReturn("127.0.0.1");
            mockedServlets.when(() -> Servlets.getHostName(any())).thenReturn("localhost");
            mockedType.when(() -> AtlasType.fromJson(jsonData, AtlasImportRequest.class)).thenReturn(mockRequest);

            AtlasImportResult result = adminResource.importFile(jsonData);

            assertNotNull(result);
            verify(importService).run(any(AtlasImportRequest.class), anyString(), anyString(), anyString());
        }
    }

    @Test
    public void testImportFileWithEmptyZipException() throws Exception {
        AdminResource adminResource = createAdminResource();
        injectHttpServletRequest(adminResource);

        String jsonData = "{}";

        AtlasImportRequest mockRequest = mock(AtlasImportRequest.class);
        Map<String, Object> options = new HashMap<>();
        options.put(AtlasImportRequest.OPTION_KEY_REPLICATED_FROM, "source-cluster");
        when(mockRequest.getOptions()).thenReturn((Map) options);

        when(importService.run(any(AtlasImportRequest.class), anyString(), anyString(), anyString()))
                .thenThrow(new AtlasBaseException(AtlasErrorCode.IMPORT_ATTEMPTING_EMPTY_ZIP, "Empty zip"));

        try (MockedStatic<AtlasAuthorizationUtils> mockedUtils = mockStatic(AtlasAuthorizationUtils.class);
                MockedStatic<AtlasType> mockedType = mockStatic(org.apache.atlas.type.AtlasType.class);
                MockedStatic<Servlets> mockedServlets = mockStatic(Servlets.class)) {
            mockedUtils.when(() -> AtlasAuthorizationUtils.verifyAccess(any(org.apache.atlas.authorize.AtlasAdminAccessRequest.class), anyString())).then(invocation -> null);
            mockedUtils.when(AtlasAuthorizationUtils::getCurrentUserName).thenReturn("testuser");
            mockedUtils.when(() -> AtlasAuthorizationUtils.getRequestIpAddress(any())).thenReturn("127.0.0.1");
            mockedServlets.when(() -> Servlets.getHostName(any())).thenReturn("localhost");
            mockedType.when(() -> AtlasType.fromJson(jsonData, AtlasImportRequest.class)).thenReturn(mockRequest);

            try {
                adminResource.importFile(jsonData);
                fail("Expected AtlasBaseException to be thrown");
            } catch (AtlasBaseException e) {
                assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.IMPORT_ATTEMPTING_EMPTY_ZIP);
            }
        }
    }

    @Test
    public void testAddToImportOperationAuditsWithObjectIds() throws Exception {
        AdminResource adminResource = createAdminResource();

        AtlasImportResult mockResult = mock(AtlasImportResult.class);
        AtlasExportResult mockExportResult = mock(AtlasExportResult.class);
        AtlasExportRequest mockExportRequest = mock(AtlasExportRequest.class);

        List<AtlasObjectId> objectIds = new ArrayList<>();
        AtlasObjectId objectId = mock(AtlasObjectId.class);
        when(objectId.getTypeName()).thenReturn("DataSet");
        objectIds.add(objectId);

        when(mockResult.getOperationStatus()).thenReturn(AtlasImportResult.OperationStatus.SUCCESS);
        when(mockResult.getExportResult()).thenReturn(mockExportResult);
        when(mockExportResult.getRequest()).thenReturn(mockExportRequest);
        when(mockExportRequest.getItemsToExport()).thenReturn(objectIds);

        // Use reflection to call the private method
        java.lang.reflect.Method method = AdminResource.class.getDeclaredMethod("addToImportOperationAudits", AtlasImportResult.class);
        method.setAccessible(true);
        method.invoke(adminResource, mockResult);

        verify(auditService).add(any(), anyString(), anyString(), anyLong());
    }

    @Test
    public void testAddToExportOperationAudits() throws Exception {
        AdminResource adminResource = createAdminResource();

        AtlasExportResult mockResult = mock(AtlasExportResult.class);
        AtlasExportRequest mockRequest = mock(AtlasExportRequest.class);

        List<AtlasObjectId> objectIds = new ArrayList<>();
        AtlasObjectId objectId = mock(AtlasObjectId.class);
        when(objectId.getTypeName()).thenReturn("Table");
        objectIds.add(objectId);

        Map<String, Object> options = new HashMap<>();
        options.put("test", "value");

        when(mockResult.getRequest()).thenReturn(mockRequest);
        when(mockResult.getOperationStatus()).thenReturn(AtlasExportResult.OperationStatus.SUCCESS);
        when(mockRequest.getItemsToExport()).thenReturn(objectIds);
        when(mockRequest.getOptions()).thenReturn(options);

        // Use reflection to call the private method
        java.lang.reflect.Method method = AdminResource.class.getDeclaredMethod("addToExportOperationAudits", boolean.class, AtlasExportResult.class);
        method.setAccessible(true);
        method.invoke(adminResource, true, mockResult);

        verify(auditService).add(any(), anyString(), anyString(), anyLong());
    }

    @Test
    public void testAddToExportOperationAuditsWithNullOptions() throws Exception {
        AdminResource adminResource = createAdminResource();

        AtlasExportResult mockResult = mock(AtlasExportResult.class);
        AtlasExportRequest mockRequest = mock(AtlasExportRequest.class);

        List<AtlasObjectId> objectIds = new ArrayList<>();
        AtlasObjectId objectId = mock(AtlasObjectId.class);
        objectIds.add(objectId);

        when(mockResult.getRequest()).thenReturn(mockRequest);
        when(mockRequest.getItemsToExport()).thenReturn(objectIds);
        when(mockRequest.getOptions()).thenReturn(null); // This should skip the audit

        // Use reflection to call the private method
        java.lang.reflect.Method method = AdminResource.class.getDeclaredMethod("addToExportOperationAudits", boolean.class, AtlasExportResult.class);
        method.setAccessible(true);
        method.invoke(adminResource, true, mockResult);

        // Should not call audit service when options is null
        verify(auditService, never()).add(any(), anyString(), anyString(), anyInt());
    }

    @Test
    public void testReleaseExportImportLock() throws Exception {
        AdminResource adminResource = createAdminResource();

        // First acquire the lock using reflection
        java.lang.reflect.Method acquireMethod = AdminResource.class.getDeclaredMethod("acquireExportImportLock", String.class);
        acquireMethod.setAccessible(true);
        acquireMethod.invoke(adminResource, "test");

        // Now release the lock using reflection
        java.lang.reflect.Method releaseMethod = AdminResource.class.getDeclaredMethod("releaseExportImportLock");
        releaseMethod.setAccessible(true);
        releaseMethod.invoke(adminResource);
    }
}
