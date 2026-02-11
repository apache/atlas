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
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryBaseObject;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proof-of-concept integration test for Glossary CRUD operations.
 *
 * Runs Atlas in-process (no Docker image build needed) with testcontainers for
 * Cassandra, Elasticsearch, and Redis. Uses {@code AtlasClientV2} with basic auth.
 *
 * <p>Run with:
 * <pre>
 * mvn install -pl webapp -am -DskipTests -Drat.skip=true
 * mvn test -pl webapp -Dtest=GlossaryIntegrationTest -Drat.skip=true
 * </pre>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GlossaryIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(GlossaryIntegrationTest.class);

    private String glossaryGuid;
    private String termGuid;
    private String categoryGuid;

    @Test
    @Order(1)
    void testCreateGlossary() throws AtlasServiceException {
        AtlasGlossary glossary = new AtlasGlossary();
        glossary.setName("TestGlossary");
        glossary.setShortDescription("A test glossary for integration testing");

        AtlasGlossary created = atlasClient.createGlossary(glossary);

        assertNotNull(created, "Created glossary should not be null");
        assertNotNull(created.getGuid(), "Glossary GUID should not be null");
        assertEquals("TestGlossary", created.getName());
        assertEquals("A test glossary for integration testing", created.getShortDescription());

        glossaryGuid = created.getGuid();
        LOG.info("Created glossary: guid={}, name={}", glossaryGuid, created.getName());
    }

    @Test
    @Order(2)
    void testGetGlossary() throws AtlasServiceException {
        assertNotNull(glossaryGuid, "Glossary must be created first");

        AtlasGlossary fetched = atlasClient.getGlossaryByGuid(glossaryGuid);

        assertNotNull(fetched);
        assertEquals(glossaryGuid, fetched.getGuid());
        assertEquals("TestGlossary", fetched.getName());
        assertEquals("A test glossary for integration testing", fetched.getShortDescription());
    }

    @Test
    @Order(3)
    void testCreateGlossaryTerm() throws AtlasServiceException {
        assertNotNull(glossaryGuid, "Glossary must be created first");

        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setName("TestTerm");
        term.setShortDescription("A test term");
        term.setAnchor(new AtlasGlossaryHeader(glossaryGuid));

        AtlasGlossaryTerm created = atlasClient.createGlossaryTerm(term);

        assertNotNull(created, "Created term should not be null");
        assertNotNull(created.getGuid(), "Term GUID should not be null");
        assertEquals("TestTerm", created.getName());

        termGuid = created.getGuid();
        LOG.info("Created term: guid={}, name={}", termGuid, created.getName());
    }

    @Test
    @Order(4)
    void testCreateGlossaryCategory() throws AtlasServiceException {
        assertNotNull(glossaryGuid, "Glossary must be created first");

        AtlasGlossaryCategory category = new AtlasGlossaryCategory();
        category.setName("TestCategory");
        category.setShortDescription("A test category");
        category.setAnchor(new AtlasGlossaryHeader(glossaryGuid));

        AtlasGlossaryCategory created = atlasClient.createGlossaryCategory(category);

        assertNotNull(created, "Created category should not be null");
        assertNotNull(created.getGuid(), "Category GUID should not be null");
        assertEquals("TestCategory", created.getName());

        categoryGuid = created.getGuid();
        LOG.info("Created category: guid={}, name={}", categoryGuid, created.getName());
    }

    @Test
    @Order(5)
    void testUpdateGlossary() throws AtlasServiceException {
        assertNotNull(glossaryGuid, "Glossary must be created first");

        AtlasGlossary glossary = atlasClient.getGlossaryByGuid(glossaryGuid);
        glossary.setShortDescription("Updated description");
        removeLexorank(glossary);

        AtlasGlossary updated = atlasClient.updateGlossaryByGuid(glossaryGuid, glossary);

        assertNotNull(updated);
        assertEquals("Updated description", updated.getShortDescription());
        assertEquals("TestGlossary", updated.getName());
    }

    @Test
    @Order(6)
    void testGetGlossaryTerms() throws AtlasServiceException {
        assertNotNull(glossaryGuid, "Glossary must be created first");

        List<AtlasGlossaryTerm> terms = atlasClient.getGlossaryTerms(glossaryGuid, "name", 10, 0);

        assertNotNull(terms);
        assertFalse(terms.isEmpty(), "Should have at least one term");
        LOG.info("Found {} terms in glossary", terms.size());
    }

    @Test
    @Order(7)
    void testGetGlossaryCategories() throws AtlasServiceException {
        assertNotNull(glossaryGuid, "Glossary must be created first");

        List<AtlasGlossaryCategory> categories = atlasClient.getGlossaryCategories(
                glossaryGuid, "name", 10, 0);

        assertNotNull(categories);
        assertFalse(categories.isEmpty(), "Should have at least one category");
        LOG.info("Found {} categories in glossary", categories.size());
    }

    @Test
    @Order(8)
    void testUpdateGlossaryTerm() throws AtlasServiceException {
        assertNotNull(termGuid, "Term must be created first");

        AtlasGlossaryTerm term = atlasClient.getGlossaryTerm(termGuid);
        term.setShortDescription("Updated term description");
        removeLexorank(term);

        AtlasGlossaryTerm updated = atlasClient.updateGlossaryTermByGuid(termGuid, term);

        assertNotNull(updated);
        assertEquals("Updated term description", updated.getShortDescription());
    }

    @Test
    @Order(9)
    void testDeleteGlossaryTerm() throws AtlasServiceException {
        assertNotNull(termGuid, "Term must be created first");

        atlasClient.deleteGlossaryTermByGuid(termGuid);
        LOG.info("Deleted term: guid={}", termGuid);

        // Verify term is deleted by checking glossary terms list
        List<AtlasGlossaryTerm> terms = atlasClient.getGlossaryTerms(glossaryGuid, "name", 10, 0);
        boolean termExists = terms != null && terms.stream()
                .anyMatch(t -> termGuid.equals(t.getGuid()));
        assertFalse(termExists, "Term should no longer exist in glossary");
    }

    @Test
    @Order(10)
    void testDeleteGlossaryCategory() throws AtlasServiceException {
        assertNotNull(categoryGuid, "Category must be created first");

        atlasClient.deleteGlossaryCategoryByGuid(categoryGuid);
        LOG.info("Deleted category: guid={}", categoryGuid);
    }

    @Test
    @Order(11)
    void testDeleteGlossary() throws AtlasServiceException {
        assertNotNull(glossaryGuid, "Glossary must be created first");

        atlasClient.deleteGlossaryByGuid(glossaryGuid);
        LOG.info("Deleted glossary: guid={}", glossaryGuid);

        // Verify glossary is deleted
        assertThrows(AtlasServiceException.class,
                () -> atlasClient.getGlossaryByGuid(glossaryGuid),
                "Getting deleted glossary should throw");
    }

    /** Remove lexicographicalSortOrder from all attribute maps to avoid "Duplicate Lexorank" on update */
    private void removeLexorank(AtlasGlossaryBaseObject obj) {
        String key = "lexicographicalSortOrder";
        if (obj.getAdditionalAttributes() != null) {
            obj.getAdditionalAttributes().remove(key);
        }
        if (obj.getOtherAttributes() != null) {
            obj.getOtherAttributes().remove(key);
        }
    }
}
