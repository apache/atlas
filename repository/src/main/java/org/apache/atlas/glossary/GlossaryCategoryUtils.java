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
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GlossaryCategoryUtils extends GlossaryUtils {
    private static final Logger  LOG           = LoggerFactory.getLogger(GlossaryCategoryUtils.class);
    private static final boolean DEBUG_ENABLED = LOG.isDebugEnabled();

    protected GlossaryCategoryUtils(AtlasRelationshipStore relationshipStore, AtlasTypeRegistry typeRegistry) {
        super(relationshipStore, typeRegistry);
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

    private void processParentCategory(AtlasGlossaryCategory newObj, AtlasGlossaryCategory existing, RelationshipOperation op) throws AtlasBaseException {
        AtlasRelatedCategoryHeader newParent      = newObj.getParentCategory();
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

    private void processAssociatedTerms(AtlasGlossaryCategory newObj, AtlasGlossaryCategory existing, RelationshipOperation op) throws AtlasBaseException {
        Set<AtlasRelatedTermHeader> newTerms      = newObj.getTerms();
        Set<AtlasRelatedTermHeader> existingTerms = existing.getTerms();

        switch (op) {
            case CREATE:
                if (DEBUG_ENABLED) {
                    LOG.debug("Creating term relation with category = {}, terms = {}", existing.getDisplayName(),
                              Objects.nonNull(newTerms) ? newTerms.size() : "none");
                }
                createTermCategorizationRelationships(existing, newTerms);
                break;
            case UPDATE:
                if (CollectionUtils.isEmpty(existingTerms)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating term relation with category = {}, terms = {}", existing.getDisplayName(),
                                  Objects.nonNull(newTerms) ? newTerms.size() : "none");
                    }
                    createTermCategorizationRelationships(existing, newTerms);
                    break;
                }

                if (CollectionUtils.isEmpty(newTerms)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Deleting term relation with category = {}, terms = {}", existing.getDisplayName(), existingTerms.size());
                    }
                    deleteTermCategorizationRelationships(existing, existingTerms);
                    break;
                }

                Set<AtlasRelatedTermHeader> toCreate = newTerms
                                                               .stream()
                                                               .filter(c -> Objects.isNull(c.getRelationGuid()))
                                                               .collect(Collectors.toSet());
                createTermCategorizationRelationships(existing, toCreate);

                Set<AtlasRelatedTermHeader> toUpdate = newTerms
                                                               .stream()
                                                               .filter(c -> Objects.nonNull(c.getRelationGuid()) && existingTerms.contains(c))
                                                               .collect(Collectors.toSet());
                updateTermCategorizationRelationships(existing, toUpdate);

                Set<AtlasRelatedTermHeader> toDelete = existingTerms
                                                               .stream()
                                                               .filter(c -> !toCreate.contains(c) && !toUpdate.contains(c))
                                                               .collect(Collectors.toSet());
                deleteTermCategorizationRelationships(existing, toDelete);
                break;
            case DELETE:
                deleteTermCategorizationRelationships(existing, existingTerms);
                break;
        }
    }

    private void createTermCategorizationRelationships(AtlasGlossaryCategory existing, Set<AtlasRelatedTermHeader> terms) throws AtlasBaseException {
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

    private void updateTermCategorizationRelationships(AtlasGlossaryCategory existing, Set<AtlasRelatedTermHeader> terms) throws AtlasBaseException {
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

    private void deleteTermCategorizationRelationships(AtlasGlossaryCategory existing, Set<AtlasRelatedTermHeader> terms) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(terms)) {
            for (AtlasRelatedTermHeader term : terms) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Creating term relation with category = {}, terms = {}", existing.getDisplayName(), term.getDisplayText());
                }
                relationshipStore.deleteById(term.getRelationGuid());
            }
        }
    }

    private void processCategoryChildren(AtlasGlossaryCategory newObj, AtlasGlossaryCategory existing, RelationshipOperation op) throws AtlasBaseException {
        Set<AtlasRelatedCategoryHeader> newChildren      = newObj.getChildrenCategories();
        Set<AtlasRelatedCategoryHeader> existingChildren = existing.getChildrenCategories();
        switch (op) {
            case CREATE:
                if (DEBUG_ENABLED) {
                    LOG.debug("Creating new children, category = {}, children = {}", existing.getDisplayName(),
                              Objects.nonNull(newChildren) ? newChildren.size() : "none");
                }
                createCategoryRelationships(existing, newChildren);
                break;
            case UPDATE:
                // Create new children
                if (CollectionUtils.isEmpty(existingChildren)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating new children, category = {}, children = {}", existing.getDisplayName(),
                                  Objects.nonNull(newChildren) ? newChildren.size() : "none");
                    }
                    createCategoryRelationships(existing, newChildren);
                    break;
                }
                // Delete current children
                if (CollectionUtils.isEmpty(newChildren)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Deleting children, category = {}, children = {}", existing.getDisplayName(), existingChildren.size());
                    }
                    deleteCategoryRelationships(existing, existingChildren);
                    break;
                }

                Set<AtlasRelatedCategoryHeader> toCreate = newChildren
                                                                   .stream()
                                                                   .filter(c -> Objects.isNull(c.getRelationGuid()))
                                                                   .collect(Collectors.toSet());
                createCategoryRelationships(existing, toCreate);

                Set<AtlasRelatedCategoryHeader> toUpdate = newChildren
                                                                   .stream()
                                                                   .filter(c -> Objects.nonNull(c.getRelationGuid()) && existingChildren.contains(c))
                                                                   .collect(Collectors.toSet());
                updateCategoryRelationships(existing, toUpdate);

                Set<AtlasRelatedCategoryHeader> toDelete = existingChildren
                                                                   .stream()
                                                                   .filter(c -> !toCreate.contains(c) && !toUpdate.contains(c))
                                                                   .collect(Collectors.toSet());
                deleteCategoryRelationships(existing, toDelete);
                break;
            case DELETE:
                deleteCategoryRelationships(existing, existingChildren);
                break;
        }
    }

    private void createCategoryRelationships(AtlasGlossaryCategory existing, Set<AtlasRelatedCategoryHeader> newChildren) throws AtlasBaseException {
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
                    LOG.debug("Creating new child, category = {}, child = {}", existing.getDisplayName(), child.getDisplayText());
                }
                createRelationship(defineCategoryHierarchyLink(existing.getGuid(), child));
            }
        }
    }

    private void updateCategoryRelationships(AtlasGlossaryCategory existing, Set<AtlasRelatedCategoryHeader> toUpdate) throws AtlasBaseException {
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

    private void deleteCategoryRelationships(AtlasGlossaryCategory existing, Set<AtlasRelatedCategoryHeader> existingChildren) throws AtlasBaseException {
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
