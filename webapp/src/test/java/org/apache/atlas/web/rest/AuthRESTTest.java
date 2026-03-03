package org.apache.atlas.web.rest;

import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.policytransformer.CachePolicyTransformerImpl;
import org.apache.atlas.repository.audit.ESBasedAuditRepository;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AuthRESTTest {

    private static final int PAGE_SIZE = 100;

    private ESBasedAuditRepository auditRepository;
    private AuthREST authREST;

    @BeforeEach
    void setUp() {
        auditRepository = mock(ESBasedAuditRepository.class);
        authREST = new AuthREST(
                mock(CachePolicyTransformerImpl.class),
                auditRepository,
                mock(AtlasEntityStore.class));

        RequestContext.get().setUser("admin", null);
    }

    @AfterEach
    void tearDown() {
        RequestContext.clear();
    }

    // --- helpers ---

    private List<EntityAuditEventV2> invokeGetPolicyAuditLogs(long lastUpdatedTime) {
        return ReflectionTestUtils.invokeMethod(authREST, "getPolicyAuditLogs", "test-service", lastUpdatedTime);
    }

    private EntityAuditSearchResult resultWith(int count) {
        List<EntityAuditEventV2> events = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            events.add(new EntityAuditEventV2());
        }
        EntityAuditSearchResult result = new EntityAuditSearchResult();
        result.setEntityAudits(events);
        return result;
    }

    private EntityAuditSearchResult emptyResult() {
        EntityAuditSearchResult result = new EntityAuditSearchResult();
        result.setEntityAudits(Collections.emptyList());
        return result;
    }

    // --- pagination tests ---

    @Test
    void testPagination_noEvents_returnsEmptyList() throws AtlasBaseException {
        when(auditRepository.searchEvents(anyString())).thenReturn(emptyResult());

        List<EntityAuditEventV2> events = invokeGetPolicyAuditLogs(0L);

        assertTrue(events.isEmpty());
        verify(auditRepository, times(1)).searchEvents(anyString());
    }

    @Test
    void testPagination_partialFirstPage_stopsAfterOnePage() throws AtlasBaseException {
        when(auditRepository.searchEvents(anyString())).thenReturn(resultWith(50));

        List<EntityAuditEventV2> events = invokeGetPolicyAuditLogs(0L);

        assertEquals(50, events.size());
        verify(auditRepository, times(1)).searchEvents(anyString());
    }

    /**
     * Key regression test for the infinite-loop bug.
     *
     * When exactly PAGE_SIZE events exist, the old code compared the cumulative
     * list size to PAGE_SIZE:  events.size() == size  →  100 == 100  →  true forever.
     * The fixed code compares only the last batch, so the second (empty) response
     * correctly stops the loop.
     */
    @Test
    void testPagination_exactlyOneFullPage_stopsAfterTwoQueries() throws AtlasBaseException {
        when(auditRepository.searchEvents(anyString()))
                .thenReturn(resultWith(PAGE_SIZE))
                .thenReturn(emptyResult());

        List<EntityAuditEventV2> events = invokeGetPolicyAuditLogs(0L);

        assertEquals(PAGE_SIZE, events.size());
        // Must issue a second query to confirm no further results, then stop.
        verify(auditRepository, times(2)).searchEvents(anyString());
    }

    /**
     * Regression test: old code stopped after the 2nd page regardless of total count
     * (cumulative size 200 != 100 → false), so events on page 3+ were silently dropped.
     */
    @Test
    void testPagination_multipleFullPagesWithPartialLast_fetchesAllPages() throws AtlasBaseException {
        // 100 + 100 + 50 = 250 total events across 3 pages
        when(auditRepository.searchEvents(anyString()))
                .thenReturn(resultWith(PAGE_SIZE))
                .thenReturn(resultWith(PAGE_SIZE))
                .thenReturn(resultWith(50));

        List<EntityAuditEventV2> events = invokeGetPolicyAuditLogs(0L);

        assertEquals(250, events.size());
        verify(auditRepository, times(3)).searchEvents(anyString());
    }

    @Test
    void testPagination_threeFullPagesExact_stopsAfterFourQueries() throws AtlasBaseException {
        // Exactly 300 events: pages 1-3 return 100 each, page 4 returns empty
        when(auditRepository.searchEvents(anyString()))
                .thenReturn(resultWith(PAGE_SIZE))
                .thenReturn(resultWith(PAGE_SIZE))
                .thenReturn(resultWith(PAGE_SIZE))
                .thenReturn(emptyResult());

        List<EntityAuditEventV2> events = invokeGetPolicyAuditLogs(0L);

        assertEquals(300, events.size());
        verify(auditRepository, times(4)).searchEvents(anyString());
    }

    @Test
    void testPagination_nullResultFromRepository_returnsEmptyList() throws AtlasBaseException {
        when(auditRepository.searchEvents(anyString())).thenReturn(null);

        List<EntityAuditEventV2> events = invokeGetPolicyAuditLogs(0L);

        assertTrue(events.isEmpty());
        verify(auditRepository, times(1)).searchEvents(anyString());
    }

    @Test
    void testPagination_repositoryThrowsOnSecondPage_returnsFirstPageResults() throws AtlasBaseException {
        when(auditRepository.searchEvents(anyString()))
                .thenReturn(resultWith(PAGE_SIZE))
                .thenThrow(new AtlasBaseException("ES unavailable"));

        List<EntityAuditEventV2> events = invokeGetPolicyAuditLogs(0L);

        // Events collected before the exception should still be returned
        assertEquals(PAGE_SIZE, events.size());
    }

    @Test
    void testPagination_lastUpdatedTimeMinusOne_treatedAsZero() throws AtlasBaseException {
        when(auditRepository.searchEvents(anyString())).thenReturn(resultWith(10));

        List<EntityAuditEventV2> events = invokeGetPolicyAuditLogs(-1L);

        assertEquals(10, events.size());
        verify(auditRepository, times(1)).searchEvents(anyString());
    }
}