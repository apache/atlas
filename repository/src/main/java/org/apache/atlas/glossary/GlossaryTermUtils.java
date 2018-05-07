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
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.glossary.relations.AtlasTermCategorizationHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class GlossaryTermUtils extends GlossaryUtils {
    private static final Logger  LOG           = LoggerFactory.getLogger(GlossaryTermUtils.class);
    private static final boolean DEBUG_ENABLED = LOG.isDebugEnabled();

    protected GlossaryTermUtils(AtlasRelationshipStore relationshipStore, AtlasTypeRegistry typeRegistry, DataAccess dataAccess) {
        super(relationshipStore, typeRegistry, dataAccess);
    }

    public void processTermRelations(AtlasGlossaryTerm updatedTerm, AtlasGlossaryTerm existing, RelationshipOperation op) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryTermUtils.processTermRelations({}, {}, {})", updatedTerm, existing, op);
        }

        processTermAnchor(updatedTerm, existing, op);
        processRelatedTerms(updatedTerm, existing, op);
        processAssociatedCategories(updatedTerm, existing, op);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryTermUtils.processTermRelations()");
        }
    }

    public void processTermAssignments(AtlasGlossaryTerm glossaryTerm, Collection<AtlasRelatedObjectId> relatedObjectIds) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryTermUtils.processTermAssignments({}, {})", glossaryTerm, relatedObjectIds);
        }

        Objects.requireNonNull(glossaryTerm);

        Set<AtlasRelatedObjectId> assignedEntities = glossaryTerm.getAssignedEntities();
        for (AtlasRelatedObjectId objectId : relatedObjectIds) {
            if (CollectionUtils.isNotEmpty(assignedEntities) && assignedEntities.contains(objectId)) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Skipping already assigned entity {}", objectId);
                    continue;
                }
            }

            if (DEBUG_ENABLED) {
                LOG.debug("Assigning term guid={}, to entity guid = {}", glossaryTerm.getGuid(), objectId.getGuid());
            }
            createRelationship(defineTermAssignment(glossaryTerm.getGuid(), objectId));
        }

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryTermUtils.processTermAssignments()");
        }
    }

    public void processTermDissociation(AtlasGlossaryTerm glossaryTerm, Collection<AtlasRelatedObjectId> relatedObjectIds) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryTermUtils.processTermDissociation({}, {}, {})", glossaryTerm.getGuid(), relatedObjectIds, glossaryTerm);
        }

        Objects.requireNonNull(glossaryTerm);
        if (CollectionUtils.isNotEmpty(relatedObjectIds)) {
            for (AtlasRelatedObjectId relatedObjectId : relatedObjectIds) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Removing term guid={}, from entity guid = {}", glossaryTerm.getGuid(), relatedObjectId.getGuid());
                }
                if (Objects.isNull(relatedObjectId.getRelationshipGuid())) {
                    throw new AtlasBaseException(AtlasErrorCode.TERM_DISSOCIATION_MISSING_RELATION_GUID);
                }
                relationshipStore.deleteById(relatedObjectId.getRelationshipGuid());
            }
        }

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryTermUtils.processTermDissociation()");
        }
    }

    private void processTermAnchor(AtlasGlossaryTerm updatedTerm, AtlasGlossaryTerm existing, RelationshipOperation op) throws AtlasBaseException {
        AtlasGlossaryHeader existingAnchor    = existing.getAnchor();
        AtlasGlossaryHeader updatedTermAnchor = updatedTerm.getAnchor();

        switch (op) {
            case CREATE:
                if (Objects.isNull(updatedTermAnchor.getGlossaryGuid())) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_NEW_ANCHOR_GUID);
                } else {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating relation between glossary = {} and term = {}", updatedTermAnchor.getGlossaryGuid(), existing.getDisplayName());
                    }
                    createRelationship(defineTermAnchorRelation(updatedTermAnchor.getGlossaryGuid(), existing.getGuid()));
                }
                break;
            case UPDATE:
                if (!Objects.equals(updatedTermAnchor, existingAnchor)) {
                    if (Objects.isNull(updatedTermAnchor.getGlossaryGuid())) {
                        throw new AtlasBaseException(AtlasErrorCode.INVALID_NEW_ANCHOR_GUID);
                    }

                    if (DEBUG_ENABLED) {
                        LOG.debug("Updating relation between glossary = {} and term = {}", updatedTermAnchor.getGlossaryGuid(), existing.getDisplayName());
                    }
                    relationshipStore.deleteById(existingAnchor.getRelationGuid());
                    createRelationship(defineTermAnchorRelation(updatedTermAnchor.getGlossaryGuid(), existing.getGuid()));
                }
                break;
            case DELETE:
                if (Objects.nonNull(existingAnchor)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Deleting term anchor");
                    }
                    relationshipStore.deleteById(existingAnchor.getRelationGuid());
                }
                break;
        }
    }

    private void processRelatedTerms(AtlasGlossaryTerm updatedTerm, AtlasGlossaryTerm existing, RelationshipOperation op) throws AtlasBaseException {
        Map<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> newRelatedTerms      = updatedTerm.getRelatedTerms();
        Map<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> existingRelatedTerms = existing.getRelatedTerms();
        switch (op) {
            case CREATE:
                for (Map.Entry<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> entry : newRelatedTerms.entrySet()) {
                    AtlasGlossaryTerm.Relation  relation = entry.getKey();
                    Set<AtlasRelatedTermHeader> terms    = entry.getValue();
                    if (Objects.nonNull(terms)) {
                        if (DEBUG_ENABLED) {
                            LOG.debug("{} relation {} for term = {}", op, relation, existing.getGuid());
                            LOG.debug("Related Term count = {}", terms.size());
                        }
                        createTermRelationships(existing, relation, terms);
                    }
                }
                break;
            case UPDATE:
                for (AtlasGlossaryTerm.Relation relation : AtlasGlossaryTerm.Relation.values()) {
                    Map<String, AtlasRelatedTermHeader> existingTermHeaders = getRelatedTermHeaders(existingRelatedTerms, relation);
                    Map<String, AtlasRelatedTermHeader> newTermHeaders      = getRelatedTermHeaders(newRelatedTerms, relation);

                    // No existing term relations, create all
                    if (MapUtils.isEmpty(existingTermHeaders)) {
                        if (DEBUG_ENABLED) {
                            LOG.debug("Creating new term relations, relation = {}, terms = {}", relation,
                                      Objects.nonNull(newTermHeaders) ? newTermHeaders.size() : "none");
                        }
                        createTermRelationships(existing, relation, newTermHeaders.values());
                        continue;
                    }

                    // Existing term relations but nothing in updated object, remove all
                    if (MapUtils.isEmpty(newTermHeaders)) {
                        if (DEBUG_ENABLED) {
                            LOG.debug("Deleting existing term relations, relation = {}, terms = {}", relation, existingTermHeaders.size());
                        }
                        deleteTermRelationships(relation, existingTermHeaders.values());
                        continue;
                    }
                    // Determine what to update, delete or create
                    Set<AtlasRelatedTermHeader> toCreate = newTermHeaders
                                                                   .values()
                                                                   .stream()
                                                                   .filter(t -> !existingTermHeaders.containsKey(t.getTermGuid()))
                                                                   .collect(Collectors.toSet());
                    Set<AtlasRelatedTermHeader> toUpdate = newTermHeaders
                                                                   .values()
                                                                   .stream()
                                                                   .filter(t -> updatedExistingTermRelation(existingTermHeaders, t))
                                                                   .collect(Collectors.toSet());
                    Set<AtlasRelatedTermHeader> toDelete = existingTermHeaders
                                                                   .values()
                                                                   .stream()
                                                                   .filter(t -> !newTermHeaders.containsKey(t.getTermGuid()))
                                                                   .collect(Collectors.toSet());

                    createTermRelationships(existing, relation, toCreate);
                    updateTermRelationships(relation, toUpdate);
                    deleteTermRelationships(relation, toDelete);

                }
                break;
            case DELETE:
                for (AtlasGlossaryTerm.Relation relation : AtlasGlossaryTerm.Relation.values()) {
                    // No existing term relations, create all
                    Set<AtlasRelatedTermHeader> existingTermHeaders = existingRelatedTerms.get(relation);
                    deleteTermRelationships(relation, existingTermHeaders);
                }
                break;
        }
    }

    private Map<String, AtlasRelatedTermHeader> getRelatedTermHeaders(Map<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> relatedTerms, AtlasGlossaryTerm.Relation relation) {
        return Objects.nonNull(relatedTerms.get(relation)) ?
                       relatedTerms.get(relation)
                                   .stream()
                                   .collect(Collectors.toMap(AtlasRelatedTermHeader::getTermGuid, t -> t)) :
                       Collections.EMPTY_MAP;
    }

    private boolean updatedExistingTermRelation(Map<String, AtlasRelatedTermHeader> existingTermHeaders, AtlasRelatedTermHeader header) {
        return Objects.nonNull(header.getRelationGuid()) && !header.equals(existingTermHeaders.get(header.getTermGuid()));
    }

    private void processAssociatedCategories(AtlasGlossaryTerm updatedTerm, AtlasGlossaryTerm existing, RelationshipOperation op) throws AtlasBaseException {
        Map<String, AtlasTermCategorizationHeader> newCategories      = getAssociatedCategories(updatedTerm);
        Map<String, AtlasTermCategorizationHeader> existingCategories = getAssociatedCategories(existing);
        switch (op) {
            case CREATE:
                if (Objects.nonNull(newCategories)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating new term categorization, term = {}, categories = {}", existing.getGuid(), newCategories.size());
                    }
                    createTermCategorizationRelationships(existing, newCategories.values());
                }
                break;
            case UPDATE:
                // If no existing categories are present then create all existing ones
                if (MapUtils.isEmpty(existingCategories)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating new term categorization, term = {}, categories = {}", existing.getGuid(),
                                  Objects.nonNull(newCategories) ? newCategories.size() : "none");
                    }
                    createTermCategorizationRelationships(existing, newCategories.values());
                    break;
                }

                // If no new categories are present then delete all existing ones
                if (MapUtils.isEmpty(newCategories)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Deleting term categorization, term = {}, categories = {}", existing.getGuid(), existingCategories.size());
                    }
                    deleteCategorizationRelationship(existingCategories.values());
                    break;
                }

                Set<AtlasTermCategorizationHeader> toCreate = newCategories
                                                                      .values()
                                                                      .stream()
                                                                      .filter(c -> !existingCategories.containsKey(c.getCategoryGuid()))
                                                                      .collect(Collectors.toSet());
                createTermCategorizationRelationships(existing, toCreate);
                Set<AtlasTermCategorizationHeader> toUpdate = newCategories
                                                                      .values()
                                                                      .stream()
                                                                      .filter(c -> updatedExistingCategorizationRelation(existingCategories, c))
                                                                      .collect(Collectors.toSet());
                updateTermCategorizationRelationships(existing, toUpdate);
                Set<AtlasTermCategorizationHeader> toDelete = existingCategories
                                                                      .values()
                                                                      .stream()
                                                                      .filter(c -> !newCategories.containsKey(c.getCategoryGuid()))
                                                                      .collect(Collectors.toSet());
                deleteCategorizationRelationship(toDelete);
                break;
            case DELETE:
                deleteCategorizationRelationship(existingCategories.values());
                break;
        }
    }

    private boolean updatedExistingCategorizationRelation(Map<String, AtlasTermCategorizationHeader> existingCategories, AtlasTermCategorizationHeader header) {
        return Objects.nonNull(header.getRelationGuid()) && !header.equals(existingCategories.get(header.getCategoryGuid()));
    }

    private Map<String, AtlasTermCategorizationHeader> getAssociatedCategories(final AtlasGlossaryTerm term) {
        return Objects.nonNull(term.getCategories()) ?
                       term.getCategories()
                           .stream()
                           .collect(Collectors.toMap(AtlasTermCategorizationHeader::getCategoryGuid, c -> c)) :
                       Collections.EMPTY_MAP;
    }

    private void createTermCategorizationRelationships(AtlasGlossaryTerm existing, Collection<AtlasTermCategorizationHeader> categories) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(categories)) {
            Set<AtlasTermCategorizationHeader> existingCategories = existing.getCategories();
            for (AtlasTermCategorizationHeader categorizationHeader : categories) {
                if (Objects.nonNull(existingCategories) && existingCategories.contains(categorizationHeader)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Skipping existing category guid={}", categorizationHeader.getCategoryGuid());
                    }
                    continue;
                }
                if (DEBUG_ENABLED) {
                    LOG.debug("Creating relation between term = {} and category = {}", existing.getGuid(), categorizationHeader.getDisplayText());
                }
                createRelationship(defineCategorizedTerm(categorizationHeader, existing.getGuid()));
            }
        }
    }

    private void updateTermCategorizationRelationships(AtlasGlossaryTerm existing, Collection<AtlasTermCategorizationHeader> toUpdate) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(toUpdate)) {
            for (AtlasTermCategorizationHeader categorizationHeader : toUpdate) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Updating relation between term = {} and category = {}", existing.getGuid(), categorizationHeader.getDisplayText());
                }
                AtlasRelationship relationship = relationshipStore.getById(categorizationHeader.getRelationGuid());
                updateRelationshipAttributes(relationship, categorizationHeader);
                relationshipStore.update(relationship);
            }
        }
    }

    private void deleteCategorizationRelationship(Collection<AtlasTermCategorizationHeader> existingCategories) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(existingCategories)) {
            for (AtlasTermCategorizationHeader categorizationHeader : existingCategories) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Deleting relation guid = {}, text = {}", categorizationHeader.getRelationGuid(), categorizationHeader.getDisplayText());
                }
                relationshipStore.deleteById(categorizationHeader.getRelationGuid());
            }
        }
    }

    private void createTermRelationships(AtlasGlossaryTerm existing, AtlasGlossaryTerm.Relation relation, Collection<AtlasRelatedTermHeader> terms) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(terms)) {
            Map<String, AtlasRelatedTermHeader> existingRelations;
            if (Objects.nonNull(existing.getRelatedTerms()) && Objects.nonNull(existing.getRelatedTerms().get(relation))) {
                existingRelations = existing.getRelatedTerms().get(relation).stream().collect(Collectors.toMap(AtlasRelatedTermHeader::getTermGuid, t -> t));
            } else {
                existingRelations = Collections.emptyMap();
            }
            for (AtlasRelatedTermHeader term : terms) {
                if (Objects.nonNull(existingRelations) && existingRelations.containsKey(term.getTermGuid())) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Skipping existing term relation termGuid={}", term.getTermGuid());
                    }
                    continue;
                }

                if (existing.getGuid().equals(term.getTermGuid())) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_TERM_RELATION_TO_SELF);
                }

                if (DEBUG_ENABLED) {
                    LOG.debug("Creating new term relation = {}, terms = {}", relation, term.getDisplayText());
                }

                createRelationship(defineTermRelation(relation, existing.getGuid(), term));
            }
        }
    }

    private void updateTermRelationships(AtlasGlossaryTerm.Relation relation, Collection<AtlasRelatedTermHeader> terms) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(terms)) {
            for (AtlasRelatedTermHeader term : terms) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Updating term relation = {}, terms = {}", relation, term.getDisplayText());
                }
                AtlasRelationship relationship = relationshipStore.getById(term.getRelationGuid());
                updateRelationshipAttributes(relationship, term);
                relationshipStore.update(relationship);
            }
        }
    }

    private void deleteTermRelationships(AtlasGlossaryTerm.Relation relation, Collection<AtlasRelatedTermHeader> terms) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(terms)) {
            for (AtlasRelatedTermHeader termHeader : terms) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Deleting term relation = {}, terms = {}", relation, termHeader.getDisplayText());
                }
                relationshipStore.deleteById(termHeader.getRelationGuid());
            }
        }
    }

    private AtlasRelationship defineTermAnchorRelation(String glossaryGuid, String termGuid) {
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(TERM_ANCHOR);
        AtlasStruct           defaultAttrs     = relationshipType.createDefaultValue();

        return new AtlasRelationship(TERM_ANCHOR, new AtlasObjectId(glossaryGuid), new AtlasObjectId(termGuid), defaultAttrs.getAttributes());
    }

    private AtlasRelationship defineTermRelation(AtlasGlossaryTerm.Relation relation, String end1TermGuid, AtlasRelatedTermHeader end2RelatedTerm) {
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relation.getName());
        AtlasStruct           defaultAttrs     = relationshipType.createDefaultValue();

        AtlasRelationship relationship;
        // End1 and End2 ObjectIds depend on the attribute
        if (relation.isEnd2Attr()) {
            relationship = new AtlasRelationship(relation.getName(), new AtlasObjectId(end2RelatedTerm.getTermGuid()), new AtlasObjectId(end1TermGuid), defaultAttrs.getAttributes());
        } else {
            relationship = new AtlasRelationship(relation.getName(), new AtlasObjectId(end1TermGuid), new AtlasObjectId(end2RelatedTerm.getTermGuid()), defaultAttrs.getAttributes());
        }

        updateRelationshipAttributes(relationship, end2RelatedTerm);
        return relationship;
    }

    private AtlasRelationship defineCategorizedTerm(AtlasTermCategorizationHeader relatedCategoryId, String termId) {
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(TERM_CATEGORIZATION);
        AtlasStruct           defaultAttrs     = relationshipType.createDefaultValue();

        AtlasRelationship relationship = new AtlasRelationship(TERM_CATEGORIZATION, new AtlasObjectId(relatedCategoryId.getCategoryGuid()), new AtlasObjectId(termId), defaultAttrs.getAttributes());
        updateRelationshipAttributes(relationship, relatedCategoryId);
        return relationship;
    }

    private AtlasRelationship defineTermAssignment(String termGuid, AtlasRelatedObjectId relatedObjectId) {
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(TERM_ASSIGNMENT);
        AtlasStruct           defaultAttrs     = relationshipType.createDefaultValue();

        AtlasObjectId     end1         = new AtlasObjectId(termGuid);
        AtlasRelationship relationship = new AtlasRelationship(TERM_ASSIGNMENT, end1, relatedObjectId, defaultAttrs.getAttributes());
        updateRelationshipAttributes(relationship, relatedObjectId);
        return relationship;
    }

    private void updateRelationshipAttributes(AtlasRelationship relationship, AtlasTermCategorizationHeader categorizationHeader) {
        if (Objects.nonNull(relationship)) {
            relationship.setAttribute(TERM_RELATION_ATTR_DESCRIPTION, categorizationHeader.getDescription());
            if (Objects.nonNull(categorizationHeader.getStatus())) {
                relationship.setAttribute(TERM_RELATION_ATTR_STATUS, categorizationHeader.getStatus().name());
            }
        }
    }

    private void updateRelationshipAttributes(AtlasRelationship relationship, AtlasRelatedObjectId relatedObjectId) {
        AtlasStruct relationshipAttributes = relatedObjectId.getRelationshipAttributes();
        if (Objects.nonNull(relationshipAttributes)) {
            for (Map.Entry<String, Object> attrEntry : relationshipAttributes.getAttributes().entrySet()) {
                relationship.setAttribute(attrEntry.getKey(), attrEntry.getValue());
            }
        }
    }

}
