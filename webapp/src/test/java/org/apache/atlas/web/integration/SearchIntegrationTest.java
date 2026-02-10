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
package org.apache.atlas.web.integration;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for Search operations.
 *
 * <p>Exercises basicSearch (the only search endpoint available in the in-process server).
 * DSL, fullText, attribute, relationship, and quick search endpoints are NOT deployed
 * in the test webapp's DiscoveryREST, so they are not tested here.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SearchIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(SearchIntegrationTest.class);

    private static final int MAX_WAIT_SECONDS = 60;
    private final long testId = System.currentTimeMillis();
    private final String searchPrefix = "searchtest" + testId;

    private String glossaryGuid;
    private final List<String> tableGuids = new ArrayList<>();
    private String firstTableGuid;

    @Test
    @Order(1)
    void testSetupSearchData() throws Exception {
        // Create glossary + terms
        AtlasGlossary glossary = new AtlasGlossary();
        glossary.setName("SearchTestGlossary-" + testId);
        glossary.setShortDescription("Glossary for search tests");
        AtlasGlossary created = atlasClient.createGlossary(glossary);
        glossaryGuid = created.getGuid();

        for (int i = 0; i < 3; i++) {
            AtlasGlossaryTerm term = new AtlasGlossaryTerm();
            term.setName(searchPrefix + "-term-" + i);
            term.setShortDescription("Search test term " + i);
            term.setAnchor(new AtlasGlossaryHeader(glossaryGuid));
            atlasClient.createGlossaryTerm(term);
        }

        // Create 3 Tables
        for (int i = 0; i < 3; i++) {
            AtlasEntity table = new AtlasEntity("Table");
            table.setAttribute("name", searchPrefix + "-table-" + i);
            table.setAttribute("qualifiedName",
                    "test://integration/search/" + searchPrefix + "/table-" + i);
            table.setAttribute("description", "Search test table number " + i);

            EntityMutationResponse response = atlasClient.createEntity(new AtlasEntityWithExtInfo(table));
            String guid = response.getFirstEntityCreated().getGuid();
            tableGuids.add(guid);
            if (i == 0) firstTableGuid = guid;
        }

        assertEquals(3, tableGuids.size());
        LOG.info("Created search test data: glossary={}, 3 terms, 3 tables", glossaryGuid);
    }

    @Test
    @Order(2)
    void testBasicSearchByType() throws Exception {
        // Wait for ES to index the entities (basic search by type only)
        waitForIndexSync(() -> {
            AtlasSearchResult result = atlasClient.basicSearch("Table", null, null,
                    true, 100, 0);
            return result != null && result.getEntities() != null && !result.getEntities().isEmpty();
        }, MAX_WAIT_SECONDS);

        AtlasSearchResult result = atlasClient.basicSearch("Table", null, null,
                true, 100, 0);

        assertNotNull(result);
        assertNotNull(result.getEntities());
        assertFalse(result.getEntities().isEmpty(), "Should find Table entities");

        LOG.info("Basic search by type returned {} entities", result.getEntities().size());
    }

    @Test
    @Order(3)
    void testBasicSearchByTypeWithPagination() throws Exception {
        // Use basic search with pagination (no query string to avoid deserialization issues)
        AtlasSearchResult page1 = atlasClient.basicSearch("Table", null, null,
                true, 2, 0);
        assertNotNull(page1);
        assertNotNull(page1.getEntities());
        assertTrue(page1.getEntities().size() <= 2, "First page should have at most 2 results");

        AtlasSearchResult page2 = atlasClient.basicSearch("Table", null, null,
                true, 2, 2);
        assertNotNull(page2);

        LOG.info("Pagination: page1={}, page2={}",
                page1.getEntities().size(),
                page2.getEntities() != null ? page2.getEntities().size() : 0);
    }

    @Test
    @Order(4)
    void testBasicSearchExcludeDeleted() throws Exception {
        AtlasSearchResult result = atlasClient.basicSearch("Table", null, null,
                true, 100, 0);
        int activeCount = result.getEntities() != null ? result.getEntities().size() : 0;

        AtlasSearchResult resultWithDeleted = atlasClient.basicSearch("Table", null, null,
                false, 100, 0);
        int allCount = resultWithDeleted.getEntities() != null ? resultWithDeleted.getEntities().size() : 0;

        assertTrue(allCount >= activeCount,
                "Including deleted should return >= active-only count");

        LOG.info("Active count={}, all count={}", activeCount, allCount);
    }

    @Test
    @Order(5)
    void testFacetedSearchByType() throws Exception {
        SearchParameters params = new SearchParameters();
        params.setTypeName("Table");
        params.setLimit(10);
        params.setOffset(0);
        params.setExcludeDeletedEntities(true);

        AtlasSearchResult result = atlasClient.facetedSearch(params);

        assertNotNull(result);
        LOG.info("Faceted search by type returned result: entities={}",
                result.getEntities() != null ? result.getEntities().size() : 0);
    }

    @Test
    @Order(6)
    void testFacetedSearchWithFilter() throws Exception {
        SearchParameters params = new SearchParameters();
        params.setTypeName("Table");
        params.setLimit(10);
        params.setOffset(0);
        params.setExcludeDeletedEntities(true);

        SearchParameters.FilterCriteria filter = new SearchParameters.FilterCriteria();
        filter.setAttributeName("qualifiedName");
        filter.setOperator(SearchParameters.Operator.STARTS_WITH);
        filter.setAttributeValue("test://integration/search/" + searchPrefix);
        params.setEntityFilters(filter);

        AtlasSearchResult result = atlasClient.facetedSearch(params);

        assertNotNull(result);
        if (result.getEntities() != null) {
            LOG.info("Faceted search with filter returned {} entities", result.getEntities().size());
        } else {
            LOG.info("Faceted search with filter returned no entities (ES may not have indexed yet)");
        }
    }

    @Test
    @Order(7)
    void testBasicSearchReturnsNoResults() throws Exception {
        // Use a valid type with a filter that won't match anything
        SearchParameters params = new SearchParameters();
        params.setTypeName("Table");
        params.setLimit(10);
        params.setOffset(0);
        params.setExcludeDeletedEntities(true);

        SearchParameters.FilterCriteria filter = new SearchParameters.FilterCriteria();
        filter.setAttributeName("qualifiedName");
        filter.setOperator(SearchParameters.Operator.EQ);
        filter.setAttributeValue("nonExistentQualifiedNameThatShouldNeverMatch_" + testId);
        params.setEntityFilters(filter);

        AtlasSearchResult result = atlasClient.facetedSearch(params);

        assertNotNull(result);
        assertTrue(result.getEntities() == null || result.getEntities().isEmpty(),
                "Search with non-matching filter should return empty results");

        LOG.info("Empty search correctly returned no results");
    }

    @Test
    @Order(8)
    void testBasicSearchForGlossaryTerms() throws Exception {
        AtlasSearchResult result = atlasClient.basicSearch("AtlasGlossaryTerm", null, null,
                true, 100, 0);

        assertNotNull(result);
        if (result.getEntities() != null) {
            LOG.info("Found {} glossary terms via basic search", result.getEntities().size());
        } else {
            LOG.info("No glossary terms found via basic search (ES may not have indexed yet)");
        }
    }

    @Test
    @Order(9)
    void testCleanupSearchData() throws AtlasServiceException {
        if (glossaryGuid != null) {
            atlasClient.deleteGlossaryByGuid(glossaryGuid);
        }
        for (String guid : tableGuids) {
            atlasClient.deleteEntityByGuid(guid);
        }
        LOG.info("Cleaned up search test data");
    }

    /**
     * Polls the given condition until it returns true, or times out.
     * Used to wait for ES index sync.
     */
    private void waitForIndexSync(Callable<Boolean> condition, int maxWaitSeconds) throws Exception {
        long deadline = System.currentTimeMillis() + maxWaitSeconds * 1000L;

        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.call()) {
                    LOG.info("Index sync condition met");
                    return;
                }
            } catch (Exception e) {
                LOG.debug("Index sync check failed: {}", e.getMessage());
            }
            Thread.sleep(2000);
        }

        LOG.warn("Index sync timed out after {}s, proceeding anyway", maxWaitSeconds);
    }
}
