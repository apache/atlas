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
package org.apache.atlas.glossary;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.glossary.relations.AtlasRelatedCategoryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GlossaryCategoryUtils extends GlossaryUtils {
    private static final Logger  LOG           = LoggerFactory.getLogger(GlossaryCategoryUtils.class);
    private static final boolean DEBUG_ENABLED = LOG.isDebugEnabled();

    protected GlossaryCategoryUtils(AtlasRelationshipStore relationshipStore, AtlasTypeRegistry typeRegistry, DataAccess dataAccess) {
        super(relationshipStore, typeRegistry, dataAccess);
    }

    public void processCategoryRelations(AtlasGlossaryCategory updatedCategory, AtlasGlossaryCategory existing, RelationshipOperation op) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryCategoryUtils.processCategoryRelations({}, {}, {})", updatedCategory, existing, op);
        }
        processCategoryAnchor(updatedCategory, existing, op);
        processParentCategory(updatedCategory, existing, op);
        processCategoryChildren(updatedCategory, existing, op);
        processAssociatedTerms(updatedCategory, existing, op);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryCategoryUtils.processCategoryRelations()");
        }
    }

    private void processCategoryAnchor(AtlasGlossaryCategory updatedCategory, AtlasGlossaryCategory existing, RelationshipOperation op) throws AtlasBaseException {
        if (Objects.isNull(updatedCategory.getAnchor()) && op != RelationshipOperation.DELETE) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ANCHOR);
        }

        switch (op) {
            case CREATE:
                if (DEBUG_ENABLED) {
                    LOG.debug("Creating new category anchor, category = {}, glossary = {}", existing.getGuid(), updatedCategory.getAnchor().getDisplayText());
                }
                String anchorGlossaryGuid = updatedCategory.getAnchor().getGlossaryGuid();
                createRelationship(defineCategoryAnchorRelation(anchorGlossaryGuid, existing.getGuid()));
                break;
            case UPDATE:
                if (!Objects.equals(updatedCategory.getAnchor(), existing.getAnchor())) {
                    if (Objects.isNull(updatedCategory.getAnchor().getGlossaryGuid())) {
                        throw new AtlasBaseException(AtlasErrorCode.INVALID_NEW_ANCHOR_GUID);
                    }

                    if (DEBUG_ENABLED) {
                        LOG.debug("Updating category anchor, category = {}, currAnchor = {}, newAnchor = {}", existing.getGuid(),
                                  existing.getAnchor().getDisplayText(), updatedCategory.getAnchor().getDisplayText());
                    }
                    relationshipStore.deleteById(existing.getAnchor().getRelationGuid());
                    createRelationship(defineCategoryAnchorRelation(updatedCategory.getAnchor().getGlossaryGuid(), existing.getGuid()));
                }
                break;
            case DELETE:
                if (Objects.nonNull(existing.getAnchor())) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Deleting category anchor");
                    }
                    relationshipStore.deleteById(existing.getAnchor().getRelationGuid());
                }
                break;
        }
    }

    private void processParentCategory(AtlasGlossaryCategory updatedCategory, AtlasGlossaryCategory existing, RelationshipOperation op) throws AtlasBaseException {
        AtlasRelatedCategoryHeader newParent      = updatedCategory.getParentCategory();
        AtlasRelatedCategoryHeader existingParent = existing.getParentCategory();
        switch (op) {
            case CREATE:
                if (Objects.nonNull(newParent)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating new parent, category = {}, parent = {}", existing.getGuid(), newParent.getDisplayText());
                    }
                    createRelationship(defineCategoryHierarchyLink(newParent, existing.getGuid()));
                }
                break;
            case UPDATE:
                if (Objects.equals(newParent, existingParent)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("No change to parent");
                    }
                    break;
                }

                if (Objects.isNull(existingParent)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating new parent, category = {}, parent = {}", existing.getGuid(), newParent.getDisplayText());
                    }
                    createRelationship(defineCategoryHierarchyLink(newParent, existing.getGuid()));
                } else if (Objects.isNull(newParent)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Removing category parent, category = {}, parent = {}", existing.getGuid(), existingParent.getDisplayText());
                    }
                    relationshipStore.deleteById(existingParent.getRelationGuid());
                } else {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Updating category parent, category = {}, currParent = {}, newParent = {}", existing.getGuid(), existingParent.getDisplayText(), newParent.getDisplayText());
                    }
                    AtlasRelationship parentRelationship = relationshipStore.getById(existingParent.getRelationGuid());
                    if (existingParent.getCategoryGuid().equals(newParent.getCategoryGuid())) {
                        updateRelationshipAttributes(parentRelationship, newParent);
                        relationshipStore.update(parentRelationship);
                    } else {
                        // Delete link to existing parent and link to new parent
                        relationshipStore.deleteById(parentRelationship.getGuid());
                        createRelationship(defineCategoryHierarchyLink(newParent, existing.getGuid()));
                    }
                }
                break;
            case DELETE:
                if (Objects.nonNull(existingParent)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Removing category parent, category = {}, parent = {}", existing.getGuid(), existingParent.getDisplayText());
                    }
                    relationshipStore.deleteById(existingParent.getRelationGuid());
                }
                break;
        }
    }

    private void processAssociatedTerms(AtlasGlossaryCategory updatedCategory, AtlasGlossaryCategory existing, RelationshipOperation op) throws AtlasBaseException {
        Map<String, AtlasRelatedTermHeader> newTerms      = getTerms(updatedCategory);
        Map<String, AtlasRelatedTermHeader> existingTerms = getTerms(existing);

        switch (op) {
            case CREATE:
                if (DEBUG_ENABLED) {
                    LOG.debug("Creating term relation with category = {}, terms = {}", existing.getDisplayName(),
                              Objects.nonNull(newTerms) ? newTerms.size() : "none");
                }
                createTermCategorizationRelationships(existing, newTerms.values());
                break;
            case UPDATE:
                if (MapUtils.isEmpty(existingTerms)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating term relation with category = {}, terms = {}", existing.getDisplayName(),
                                  Objects.nonNull(newTerms) ? newTerms.size() : "none");
                    }
                    createTermCategorizationRelationships(existing, newTerms.values());
                    break;
                }

                if (MapUtils.isEmpty(newTerms)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Deleting term relation with category = {}, terms = {}", existing.getDisplayName(), existingTerms.size());
                    }
                    deleteTermCategorizationRelationships(existing, existingTerms.values());
                    break;
                }

                Set<AtlasRelatedTermHeader> toCreate = newTerms
                                                               .values()
                                                               .stream()
                                                               .filter(t -> !existingTerms.containsKey(t.getTermGuid()))
                                                               .collect(Collectors.toSet());
                createTermCategorizationRelationships(existing, toCreate);

                Set<AtlasRelatedTermHeader> toUpdate = newTerms
                                                               .values()
                                                               .stream()
                                                               .filter(t -> updatedExistingTermRelation(existingTerms, t))
                                                               .collect(Collectors.toSet());
                updateTermCategorizationRelationships(existing, toUpdate);

                Set<AtlasRelatedTermHeader> toDelete = existingTerms
                                                               .values()
                                                               .stream()
                                                               .filter(t -> !newTerms.containsKey(t.getTermGuid()))
                                                               .collect(Collectors.toSet());
                deleteTermCategorizationRelationships(existing, toDelete);
                break;
            case DELETE:
                deleteTermCategorizationRelationships(existing, existingTerms.values());
                break;
        }
    }

    private boolean updatedExistingTermRelation(Map<String, AtlasRelatedTermHeader> existingTerms, AtlasRelatedTermHeader term) {
        return Objects.nonNull(term.getRelationGuid()) && !existingTerms.get(term.getTermGuid()).equals(term);
    }

    private Map<String, AtlasRelatedTermHeader> getTerms(final AtlasGlossaryCategory category) {
        if (Objects.nonNull(category.getTerms())) {
            Map<String, AtlasRelatedTermHeader> map = new HashMap<>();
            for (AtlasRelatedTermHeader t : category.getTerms()) {
                AtlasRelatedTermHeader header = map.get(t.getTermGuid());
                if (header == null) {
                    map.put(t.getTermGuid(), t);
                } else if (StringUtils.isEmpty(header.getRelationGuid()) && StringUtils.isNotEmpty(t.getRelationGuid())) {
                    map.put(t.getTermGuid(), t);
                }
            }
            return map;
        }
        else return Collections.emptyMap();
    }

    private void createTermCategorizationRelationships(AtlasGlossaryCategory existing, Collection<AtlasRelatedTermHeader> terms) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(terms)) {
            Set<AtlasRelatedTermHeader> existingTerms = existing.getTerms();
            for (AtlasRelatedTermHeader term : terms) {
                if (Objects.isNull(term.getTermGuid())) {
                    throw new AtlasBaseException(AtlasErrorCode.MISSING_TERM_ID_FOR_CATEGORIZATION);
                } else {
                    if (Objects.nonNull(existingTerms) && existingTerms.contains(term)) {
                        if (DEBUG_ENABLED) {
                            LOG.debug("Skipping existing term guid={}", term.getTermGuid());
                        }
                        continue;
                    }
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating relation between category = {} and term = {}", existing.getGuid(), term.getDisplayText());
                    }
                    createRelationship(defineCategorizedTerm(existing.getGuid(), term));
                }
            }
        }
    }

    private void updateTermCategorizationRelationships(AtlasGlossaryCategory existing, Collection<AtlasRelatedTermHeader> terms) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(terms)) {
            for (AtlasRelatedTermHeader term : terms) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Updating term relation with category = {}, term = {}", existing.getDisplayName(), term.getDisplayText());
                }
                AtlasRelationship relationship = relationshipStore.getById(term.getRelationGuid());
                updateRelationshipAttributes(relationship, term);
                relationshipStore.update(relationship);
            }
        }
    }

    private void deleteTermCategorizationRelationships(AtlasGlossaryCategory existing, Collection<AtlasRelatedTermHeader> terms) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(terms)) {
            for (AtlasRelatedTermHeader term : terms) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Creating term relation with category = {}, terms = {}", existing.getDisplayName(), term.getDisplayText());
                }
                relationshipStore.deleteById(term.getRelationGuid());
            }
        }
    }

    private void processCategoryChildren(AtlasGlossaryCategory updatedCategory, AtlasGlossaryCategory existing, RelationshipOperation op) throws AtlasBaseException {
        Map<String, AtlasRelatedCategoryHeader> newChildren      = getChildren(updatedCategory);
        Map<String, AtlasRelatedCategoryHeader> existingChildren = getChildren(existing);
        switch (op) {
            case CREATE:
                if (DEBUG_ENABLED) {
                    LOG.debug("Creating new children, category = {}, children = {}", existing.getDisplayName(),
                              Objects.nonNull(newChildren) ? newChildren.size() : "none");
                }
                createCategoryRelationships(existing, newChildren.values());
                break;
            case UPDATE:
                // Create new children
                if (MapUtils.isEmpty(existingChildren)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating new children, category = {}, children = {}", existing.getDisplayName(),
                                  Objects.nonNull(newChildren) ? newChildren.size() : "none");
                    }
                    createCategoryRelationships(existing, newChildren.values());
                    break;
                }
                // Delete current children
                if (MapUtils.isEmpty(newChildren)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Deleting children, category = {}, children = {}", existing.getDisplayName(), existingChildren.size());
                    }
                    deleteCategoryRelationships(existing, existingChildren.values());
                    break;
                }

                Set<AtlasRelatedCategoryHeader> toCreate = newChildren
                                                                   .values()
                                                                   .stream()
                                                                   .filter(c -> !existingChildren.containsKey(c.getCategoryGuid()))
                                                                   .collect(Collectors.toSet());
                createCategoryRelationships(existing, toCreate);

                Set<AtlasRelatedCategoryHeader> toUpdate = newChildren
                                                                   .values()
                                                                   .stream()
                                                                   .filter(c -> updatedExistingCategoryRelation(existingChildren, c))
                                                                   .collect(Collectors.toSet());
                updateCategoryRelationships(existing, toUpdate);

                Set<AtlasRelatedCategoryHeader> toDelete = existingChildren
                                                                   .values()
                                                                   .stream()
                                                                   .filter(c -> !newChildren.containsKey(c.getCategoryGuid()))
                                                                   .collect(Collectors.toSet());
                deleteCategoryRelationships(existing, toDelete);
                break;
            case DELETE:
                deleteCategoryRelationships(existing, existingChildren.values());
                break;
        }
    }

    private boolean updatedExistingCategoryRelation(Map<String, AtlasRelatedCategoryHeader> existingChildren, AtlasRelatedCategoryHeader header) {
        return Objects.nonNull(header.getRelationGuid()) && !header.equals(existingChildren.get(header.getCategoryGuid()));
    }

    private Map<String, AtlasRelatedCategoryHeader> getChildren(final AtlasGlossaryCategory category) {
        if (Objects.nonNull(category.getChildrenCategories())) {
            Map<String, AtlasRelatedCategoryHeader> map = new HashMap<>();
            for (AtlasRelatedCategoryHeader c : category.getChildrenCategories()) {
                AtlasRelatedCategoryHeader header = map.get(c.getCategoryGuid());
                if (header == null || (StringUtils.isEmpty(header.getRelationGuid()) && StringUtils.isNotEmpty(c.getRelationGuid()))) {
                    map.put(c.getCategoryGuid(), c);
                }
            }
            return map;
        }
        else return Collections.emptyMap();
    }

    private void createCategoryRelationships(AtlasGlossaryCategory existing, Collection<AtlasRelatedCategoryHeader> newChildren) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(newChildren)) {
            Set<AtlasRelatedCategoryHeader> existingChildren = existing.getChildrenCategories();
            for (AtlasRelatedCategoryHeader child : newChildren) {
                if (Objects.nonNull(existingChildren) && existingChildren.contains(child)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Skipping existing child relation for category guid = {}", child.getCategoryGuid());
                    }
                    continue;
                }

                if (DEBUG_ENABLED) {
                    LOG.debug("Loading the child category to perform glossary check");
                }

                AtlasGlossaryCategory childCategory = new AtlasGlossaryCategory();
                childCategory.setGuid(child.getCategoryGuid());
                childCategory = dataAccess.load(childCategory);

                if (StringUtils.equals(existing.getAnchor().getGlossaryGuid(), childCategory.getAnchor().getGlossaryGuid())) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating new child, category = {}, child = {}", existing.getDisplayName(), child.getDisplayText());
                    }
                    createRelationship(defineCategoryHierarchyLink(existing.getGuid(), child));
                } else {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_CHILD_CATEGORY_DIFFERENT_GLOSSARY, child.getCategoryGuid());
                }

            }
        }
    }

    private void updateCategoryRelationships(AtlasGlossaryCategory existing, Collection<AtlasRelatedCategoryHeader> toUpdate) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(toUpdate)) {
            for (AtlasRelatedCategoryHeader categoryHeader : toUpdate) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Updating child, category = {}, child = {}", existing.getDisplayName(), categoryHeader.getDisplayText());
                }
                AtlasRelationship childRelationship = relationshipStore.getById(categoryHeader.getRelationGuid());
                updateRelationshipAttributes(childRelationship, categoryHeader);
                relationshipStore.update(childRelationship);
            }
        }
    }

    private void deleteCategoryRelationships(AtlasGlossaryCategory existing, Collection<AtlasRelatedCategoryHeader> existingChildren) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(existingChildren)) {
            for (AtlasRelatedCategoryHeader child : existingChildren) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Deleting child, category = {}, child = {}", existing.getDisplayName(), child.getDisplayText());
                }
                relationshipStore.deleteById(child.getRelationGuid());
            }
        }
    }

    private AtlasRelationship defineCategoryAnchorRelation(String glossaryGuid, String categoryGuid) {
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(CATEGORY_ANCHOR);
        AtlasStruct           defaultAttrs     = relationshipType.createDefaultValue();

        return new AtlasRelationship(CATEGORY_ANCHOR, new AtlasObjectId(glossaryGuid), new AtlasObjectId(categoryGuid), defaultAttrs.getAttributes());
    }

    private AtlasRelationship defineCategoryHierarchyLink(String parentCategoryGuid, AtlasRelatedCategoryHeader childCategory) {
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(CATEGORY_HIERARCHY);
        AtlasStruct           defaultAttrs     = relationshipType.createDefaultValue();

        AtlasRelationship relationship = new AtlasRelationship(CATEGORY_HIERARCHY, new AtlasObjectId(parentCategoryGuid), new AtlasObjectId(childCategory.getCategoryGuid()), defaultAttrs.getAttributes());
        updateRelationshipAttributes(relationship, childCategory);
        return relationship;
    }

    private AtlasRelationship defineCategoryHierarchyLink(AtlasRelatedCategoryHeader parentCategory, String childGuid) {
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(CATEGORY_HIERARCHY);
        AtlasStruct           defaultAttrs     = relationshipType.createDefaultValue();

        AtlasRelationship relationship = new AtlasRelationship(CATEGORY_HIERARCHY, new AtlasObjectId(parentCategory.getCategoryGuid()), new AtlasObjectId(childGuid), defaultAttrs.getAttributes());
        updateRelationshipAttributes(relationship, parentCategory);
        return relationship;
    }

    private AtlasRelationship defineCategorizedTerm(String categoryGuid, AtlasRelatedTermHeader relatedTermId) {
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(TERM_CATEGORIZATION);
        AtlasStruct           defaultAttrs     = relationshipType.createDefaultValue();

        AtlasRelationship relationship = new AtlasRelationship(TERM_CATEGORIZATION, new AtlasObjectId(categoryGuid), new AtlasObjectId(relatedTermId.getTermGuid()), defaultAttrs.getAttributes());
        updateRelationshipAttributes(relationship, relatedTermId);
        return relationship;
    }

    private void updateRelationshipAttributes(AtlasRelationship relationship, AtlasRelatedCategoryHeader relatedCategoryHeader) {
        if (Objects.nonNull(relationship)) {
            relationship.setAttribute("description", relatedCategoryHeader.getDescription());
        }
    }

}
