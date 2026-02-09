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
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedCategoryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.glossary.relations.AtlasTermCategorizationHeader;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Extended Glossary integration tests covering category hierarchies,
 * term-entity assignment, and category-term relationships.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GlossaryExtendedIntegrationTest extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(GlossaryExtendedIntegrationTest.class);

    private final long testId = System.currentTimeMillis();

    private String glossaryGuid;
    private String parentCategoryGuid;
    private String childCategoryGuid;
    private String termGuid;
    private String entityGuid;
    private String assignmentRelationshipGuid;

    @Test
    @Order(1)
    void testSetupGlossary() throws AtlasServiceException {
        AtlasGlossary glossary = new AtlasGlossary();
        glossary.setName("ExtGlossary-" + testId);
        glossary.setShortDescription("Extended glossary test");

        AtlasGlossary created = atlasClient.createGlossary(glossary);
        glossaryGuid = created.getGuid();
        assertNotNull(glossaryGuid);

        // Create entity for term assignment
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setAttribute("name", "glossary-ext-table-" + testId);
        entity.setAttribute("qualifiedName", "test://integration/glossary-ext/table/" + testId);

        EntityMutationResponse resp = atlasClient.createEntity(new AtlasEntityWithExtInfo(entity));
        entityGuid = resp.getFirstEntityCreated().getGuid();

        LOG.info("Setup: glossary={}, entity={}", glossaryGuid, entityGuid);
    }

    @Test
    @Order(2)
    void testCreateCategoryHierarchy() throws AtlasServiceException {
        assertNotNull(glossaryGuid);

        // Parent category
        AtlasGlossaryCategory parent = new AtlasGlossaryCategory();
        parent.setName("ParentCategory-" + testId);
        parent.setShortDescription("Parent category");
        parent.setAnchor(new AtlasGlossaryHeader(glossaryGuid));

        AtlasGlossaryCategory createdParent = atlasClient.createGlossaryCategory(parent);
        parentCategoryGuid = createdParent.getGuid();
        assertNotNull(parentCategoryGuid);

        // Child category
        AtlasGlossaryCategory child = new AtlasGlossaryCategory();
        child.setName("ChildCategory-" + testId);
        child.setShortDescription("Child category");
        child.setAnchor(new AtlasGlossaryHeader(glossaryGuid));
        AtlasRelatedCategoryHeader parentHeader = new AtlasRelatedCategoryHeader();
        parentHeader.setCategoryGuid(parentCategoryGuid);
        child.setParentCategory(parentHeader);

        AtlasGlossaryCategory createdChild = atlasClient.createGlossaryCategory(child);
        childCategoryGuid = createdChild.getGuid();
        assertNotNull(childCategoryGuid);

        LOG.info("Created category hierarchy: parent={}, child={}", parentCategoryGuid, childCategoryGuid);
    }

    @Test
    @Order(3)
    void testGetCategoryChildren() throws AtlasServiceException {
        assertNotNull(parentCategoryGuid);

        AtlasGlossaryCategory parent = atlasClient.getGlossaryCategory(parentCategoryGuid);
        assertNotNull(parent);

        // Parent should have childrenCategories
        assertNotNull(parent.getChildrenCategories(), "Parent should have children categories");
        assertFalse(parent.getChildrenCategories().isEmpty(), "Parent should have at least one child");

        LOG.info("Parent category has {} children", parent.getChildrenCategories().size());
    }

    @Test
    @Order(4)
    void testCreateTermInCategory() throws AtlasServiceException {
        assertNotNull(glossaryGuid);
        assertNotNull(parentCategoryGuid);

        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setName("CategorizedTerm-" + testId);
        term.setShortDescription("Term in a category");
        term.setAnchor(new AtlasGlossaryHeader(glossaryGuid));

        // Assign to category
        Set<AtlasTermCategorizationHeader> categories = new HashSet<>();
        AtlasTermCategorizationHeader catHeader = new AtlasTermCategorizationHeader();
        catHeader.setCategoryGuid(parentCategoryGuid);
        categories.add(catHeader);
        term.setCategories(categories);

        AtlasGlossaryTerm created = atlasClient.createGlossaryTerm(term);
        termGuid = created.getGuid();
        assertNotNull(termGuid);

        LOG.info("Created term in category: termGuid={}, categoryGuid={}",
                termGuid, parentCategoryGuid);
    }

    @Test
    @Order(5)
    void testGetCategoryTerms() throws AtlasServiceException {
        assertNotNull(parentCategoryGuid);

        List<AtlasRelatedTermHeader> terms = atlasClient.getCategoryTerms(
                parentCategoryGuid, "name", 10, 0);

        assertNotNull(terms);
        assertFalse(terms.isEmpty(), "Category should have at least one term");

        LOG.info("Category has {} terms", terms.size());
    }

    @Test
    @Order(6)
    void testAssignTermToEntity() throws AtlasServiceException {
        assertNotNull(termGuid);
        assertNotNull(entityGuid);

        AtlasRelatedObjectId relatedObj = new AtlasRelatedObjectId();
        relatedObj.setGuid(entityGuid);
        relatedObj.setTypeName("Table");

        atlasClient.assignTermToEntities(termGuid, Collections.singletonList(relatedObj));

        LOG.info("Assigned term {} to entity {}", termGuid, entityGuid);
    }

    @Test
    @Order(7)
    void testGetEntitiesWithTerm() throws AtlasServiceException {
        assertNotNull(termGuid);

        // getEntitiesAssignedWithTerm may return List<LinkedHashMap> instead of List<AtlasRelatedObjectId>
        // due to Jackson deserialization, so we handle both cases
        List<?> rawEntities = atlasClient.getEntitiesAssignedWithTerm(
                termGuid, "name", 10, 0);

        assertNotNull(rawEntities);
        assertFalse(rawEntities.isEmpty(), "Term should have assigned entities");

        // Extract guid from either AtlasRelatedObjectId or LinkedHashMap
        boolean found = false;
        for (Object item : rawEntities) {
            String guid = null;
            String relGuid = null;
            if (item instanceof AtlasRelatedObjectId) {
                guid = ((AtlasRelatedObjectId) item).getGuid();
                relGuid = ((AtlasRelatedObjectId) item).getRelationshipGuid();
            } else if (item instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) item;
                guid = (String) map.get("guid");
                relGuid = (String) map.get("relationshipGuid");
            }
            if (entityGuid.equals(guid)) {
                found = true;
                assignmentRelationshipGuid = relGuid;
                break;
            }
        }
        assertTrue(found, "Assigned entity should be in the list");

        LOG.info("Term has {} assigned entities", rawEntities.size());
    }

    @Test
    @Order(8)
    void testDisassociateTermFromEntity() throws AtlasServiceException {
        assertNotNull(termGuid);
        assertNotNull(entityGuid);

        AtlasRelatedObjectId relatedObj = new AtlasRelatedObjectId();
        relatedObj.setGuid(entityGuid);
        relatedObj.setTypeName("Table");

        // Use the relationship guid we saved from the previous test
        if (assignmentRelationshipGuid == null) {
            // Fallback: fetch it again
            List<?> assigned = atlasClient.getEntitiesAssignedWithTerm(
                    termGuid, "name", 10, 0);
            for (Object item : assigned) {
                String guid = null;
                String relGuid = null;
                if (item instanceof AtlasRelatedObjectId) {
                    guid = ((AtlasRelatedObjectId) item).getGuid();
                    relGuid = ((AtlasRelatedObjectId) item).getRelationshipGuid();
                } else if (item instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> map = (Map<String, Object>) item;
                    guid = (String) map.get("guid");
                    relGuid = (String) map.get("relationshipGuid");
                }
                if (entityGuid.equals(guid)) {
                    assignmentRelationshipGuid = relGuid;
                    break;
                }
            }
        }

        relatedObj.setRelationshipGuid(assignmentRelationshipGuid);
        atlasClient.disassociateTermFromEntities(termGuid, Collections.singletonList(relatedObj));

        LOG.info("Disassociated term from entity");
    }

    @Test
    @Order(9)
    void testCleanup() throws AtlasServiceException {
        // Delete entity first (before glossary, since term may still reference it)
        if (entityGuid != null) {
            atlasClient.deleteEntityByGuid(entityGuid);
        }
        if (glossaryGuid != null) {
            atlasClient.deleteGlossaryByGuid(glossaryGuid);
        }
        LOG.info("Cleaned up glossary extended test data");
    }
}
