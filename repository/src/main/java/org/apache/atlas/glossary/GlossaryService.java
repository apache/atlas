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
import org.apache.atlas.SortOrder;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.relations.AtlasRelatedCategoryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.glossary.relations.AtlasTermCategorizationHeader;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.repository.ogm.DataAccess;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class GlossaryService {
    private static final Logger  LOG           = LoggerFactory.getLogger(GlossaryService.class);
    private static final boolean DEBUG_ENABLED = LOG.isDebugEnabled();

    private static final String ATLAS_GLOSSARY_PREFIX = "__AtlasGlossary";
    // Relation name constants
    private static final String TERM_ANCHOR           = ATLAS_GLOSSARY_PREFIX + "TermAnchor";
    private static final String CATEGORY_ANCHOR       = ATLAS_GLOSSARY_PREFIX + "CategoryAnchor";
    private static final String CATEGORY_HIERARCHY    = ATLAS_GLOSSARY_PREFIX + "CategoryHierarchyLink";
    private static final String TERM_CATEGORIZATION   = ATLAS_GLOSSARY_PREFIX + "TermCategorization";
    private static final String TERM_ASSIGNMENT       = ATLAS_GLOSSARY_PREFIX + "SemanticAssignment";

    private final DataAccess             dataAccess;
    private final AtlasRelationshipStore relationshipStore;

    @Inject
    public GlossaryService(DataAccess dataAccess, final AtlasRelationshipStore relationshipStore) {
        this.dataAccess = dataAccess;
        this.relationshipStore = relationshipStore;
    }


    /**
     * List all glossaries
     *
     * @param limit     page size - no paging by default
     * @param offset    offset for pagination
     * @param sortOrder ASC (default) or DESC
     * @return List of all glossaries
     * @throws AtlasBaseException
     */
    public List<AtlasGlossary> getGlossaries(int limit, int offset, SortOrder sortOrder) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.getGlossaries({}, {}, {})", limit, offset, sortOrder);
        }

        List<String>     glossaryGuids    = AtlasGraphUtilsV1.findEntityGUIDsByType(ATLAS_GLOSSARY_PREFIX, sortOrder);
        PaginationHelper paginationHelper = new PaginationHelper<>(glossaryGuids, offset, limit);

        List<AtlasGlossary> ret;
        List<String>        guidsToLoad = paginationHelper.getPaginatedList();
        if (CollectionUtils.isNotEmpty(guidsToLoad)) {
            ret = guidsToLoad.stream().map(GlossaryService::getGlossarySkeleton).collect(Collectors.toList());
            Iterable<AtlasGlossary> glossaries = dataAccess.load(ret);
            ret.clear();

            glossaries.forEach(ret::add);
        } else {
            ret = Collections.emptyList();
        }

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.getGlossaries() : {}", ret);
        }
        return ret;
    }

    /**
     * Create a glossary
     *
     * @param atlasGlossary Glossary specification to be created
     * @return Glossary definition
     * @throws AtlasBaseException
     */
    public AtlasGlossary createGlossary(AtlasGlossary atlasGlossary) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.createGlossary({})", atlasGlossary);
        }

        if (Objects.isNull(atlasGlossary)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Glossary definition missing");
        }
        if (Objects.isNull(atlasGlossary.getQualifiedName())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Glossary qualifiedName is mandatory");

        }
        AtlasGlossary saved = dataAccess.save(atlasGlossary);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.createGlossary() : {}", saved);
        }
        return saved;
    }

    /**
     * Get specific glossary
     *
     * @param glossaryGuid unique identifier
     * @return Glossary corresponding to specified glossaryGuid
     * @throws AtlasBaseException
     */
    public AtlasGlossary getGlossary(String glossaryGuid) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.getGlossary({})", glossaryGuid);
        }

        if (Objects.isNull(glossaryGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "glossaryGuid is null/empty");
        }

        AtlasGlossary atlasGlossary = getGlossarySkeleton(glossaryGuid);
        AtlasGlossary ret           = dataAccess.load(atlasGlossary);

        setDisplayTextForRelations(ret);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.getGlossary() : {}", ret);
        }
        return ret;
    }

    /**
     * Get specific glossary
     *
     * @param glossaryGuid unique identifier
     * @return Glossary corresponding to specified glossaryGuid
     * @throws AtlasBaseException
     */
    public AtlasGlossary.AtlasGlossaryExtInfo getDetailedGlossary(String glossaryGuid) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.getGlossary({})", glossaryGuid);
        }

        if (Objects.isNull(glossaryGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "glossaryGuid is null/empty");
        }

        AtlasGlossary atlasGlossary = getGlossarySkeleton(glossaryGuid);
        AtlasGlossary glossary      = dataAccess.load(atlasGlossary);

        AtlasGlossary.AtlasGlossaryExtInfo ret = new AtlasGlossary.AtlasGlossaryExtInfo(glossary);

        // Load all linked terms
        if (CollectionUtils.isNotEmpty(ret.getTerms())) {
            List<AtlasGlossaryTerm> termsToLoad = ret.getTerms()
                                                     .stream()
                                                     .map(id -> getAtlasGlossaryTermSkeleton(id.getTermGuid()))
                                                     .collect(Collectors.toList());
            Iterable<AtlasGlossaryTerm> glossaryTerms = dataAccess.load(termsToLoad);
            glossaryTerms.forEach(ret::addTermInfo);
        }

        // Load all linked categories
        if (CollectionUtils.isNotEmpty(ret.getCategories())) {
            List<AtlasGlossaryCategory> categoriesToLoad = ret.getCategories()
                                                              .stream()
                                                              .map(id -> getAtlasGlossaryCategorySkeleton(id.getCategoryGuid()))
                                                              .collect(Collectors.toList());
            Iterable<AtlasGlossaryCategory> glossaryCategories = dataAccess.load(categoriesToLoad);
            glossaryCategories.forEach(ret::addCategoryInfo);
        }

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.getGlossary() : {}", ret);
        }
        return ret;
    }

    public AtlasGlossary updateGlossary(AtlasGlossary atlasGlossary) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.updateGlossary({})", atlasGlossary);
        }
        if (Objects.isNull(atlasGlossary)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Glossary is null/empty");
        }

        AtlasGlossary ret = dataAccess.load(atlasGlossary);

        if (!ret.equals(atlasGlossary)) {
            atlasGlossary.setGuid(ret.getGuid());
            atlasGlossary.setQualifiedName(ret.getQualifiedName());

            ret = dataAccess.save(atlasGlossary);
            setDisplayTextForRelations(ret);
        }

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.updateGlossary() : {}", ret);
        }
        return ret;
    }

    public void deleteGlossary(String glossaryGuid) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.deleteGlossary({})", glossaryGuid);
        }
        if (Objects.isNull(glossaryGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "glossaryGuid is null/empty");
        }


        // FIXME: When deleting all other related entities, the new edge label (r:<Relation>) is failing the delete calls
        dataAccess.delete(glossaryGuid);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.deleteGlossary()");
        }
    }

    /*
     * GlossaryTerms related operations
     * */
    public AtlasGlossaryTerm getTerm(String termGuid) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.getTerm({})", termGuid);
        }
        if (Objects.isNull(termGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "termGuid is null/empty");
        }


        AtlasGlossaryTerm atlasGlossary = getAtlasGlossaryTermSkeleton(termGuid);
        AtlasGlossaryTerm ret           = dataAccess.load(atlasGlossary);

        setDisplayTextForRelations(ret);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.getTerm() : {}", ret);
        }
        return ret;
    }

    public AtlasGlossaryTerm createTerm(AtlasGlossaryTerm glossaryTerm) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.create({})", glossaryTerm);
        }
        if (Objects.isNull(glossaryTerm)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "GlossaryTerm definition missing");
        }
        if (Objects.isNull(glossaryTerm.getQualifiedName())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "GlossaryTerm qualifiedName is mandatory");
        }

        AtlasGlossaryTerm saved = dataAccess.save(glossaryTerm);

        // TODO: Create all term relations
        processTermAnchor(glossaryTerm, saved);
        processRelatedTerms(glossaryTerm, saved);
        processAssociatedCategories(glossaryTerm, saved);

        saved = dataAccess.load(glossaryTerm);
        setDisplayTextForRelations(saved);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.create() : {}", saved);
        }
        return saved;
    }

    public List<AtlasGlossaryTerm> createTerms(List<AtlasGlossaryTerm> glossaryTerm) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.create({})", glossaryTerm);
        }

        if (Objects.isNull(glossaryTerm)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "glossaryTerm(s) is null/empty");
        }


        List<AtlasGlossaryTerm> ret = new ArrayList<>();
        for (AtlasGlossaryTerm atlasGlossaryTerm : glossaryTerm) {
            ret.add(createTerm(atlasGlossaryTerm));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== GlossaryService.createTerms() : {}", ret);
        }

        return ret;
    }

    public AtlasGlossaryTerm updateTerm(AtlasGlossaryTerm atlasGlossaryTerm) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.updateTerm({})", atlasGlossaryTerm);
        }
        if (Objects.isNull(atlasGlossaryTerm)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "atlasGlossaryTerm is null/empty");
        }


        AtlasGlossaryTerm existing = dataAccess.load(atlasGlossaryTerm);
        AtlasGlossaryTerm updated  = atlasGlossaryTerm;
        if (!existing.equals(updated)) {
            try {
                updated.setGuid(existing.getGuid());
                updated.setQualifiedName(existing.getQualifiedName());

                atlasGlossaryTerm = dataAccess.save(updated);
            } catch (AtlasBaseException e) {
                LOG.debug("Glossary term had no immediate attr updates. Exception: {}", e.getMessage());
            } finally {
                // TODO: Manage remaining term relations
                processRelations(atlasGlossaryTerm, existing);
            }

        }

        updated = dataAccess.load(atlasGlossaryTerm);
        setDisplayTextForRelations(updated);
        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.updateTerm() : {}", updated);
        }
        return updated;
    }

    public void deleteTerm(String termGuid) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.deleteTerm({})", termGuid);
        }
        if (Objects.isNull(termGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "termGuid is null/empty");
        }


        dataAccess.delete(termGuid);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.deleteTerm()");
        }
    }

    public void assignTermToEntities(String termGuid, Collection<AtlasEntityHeader> entityHeaders) throws AtlasBaseException {
        AtlasGlossaryTerm glossaryTerm = dataAccess.load(getAtlasGlossaryTermSkeleton(termGuid));

        if (Objects.nonNull(glossaryTerm)) {
            Set<AtlasEntityHeader> assignedEntities = glossaryTerm.getAssignedEntities();
            for (AtlasEntityHeader entityHeader : entityHeaders) {
                if (CollectionUtils.isNotEmpty(assignedEntities) && assignedEntities.contains(entityHeader)) continue;
                if (DEBUG_ENABLED) {
                    LOG.debug("Assigning term guid={}, to entity guid = {}", termGuid, entityHeader.getGuid());
                }
                createRelationship(defineTermAssignment(termGuid, entityHeader));
            }
        }
    }

    public void removeTermFromEntities(String termGuid, Collection<AtlasEntityHeader> entityHeaders) throws AtlasBaseException {
        AtlasGlossaryTerm glossaryTerm = dataAccess.load(getAtlasGlossaryTermSkeleton(termGuid));

        if (Objects.nonNull(glossaryTerm)) {
            for (AtlasEntityHeader entityHeader : entityHeaders) {
                if (DEBUG_ENABLED) {
                    LOG.debug("Removing term guid={}, from entity guid = {}", termGuid, entityHeader.getGuid());
                }
                Object relationGuid = entityHeader.getAttribute("relationGuid");
                if (Objects.isNull(relationGuid)) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "EntityHeader is missing mandatory attribute relation guid");
                }
                relationshipStore.deleteById((String) relationGuid);
            }
        }
    }

    /*
     * GlossaryCategory related operations
     * */
    public AtlasGlossaryCategory getCategory(String categoryGuid) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.getCategory({})", categoryGuid);
        }
        if (Objects.isNull(categoryGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "categoryGuid is null/empty");
        }


        AtlasGlossaryCategory atlasGlossary = getAtlasGlossaryCategorySkeleton(categoryGuid);
        AtlasGlossaryCategory ret           = dataAccess.load(atlasGlossary);

        setDisplayTextForRelations(ret);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.getCategory() : {}", ret);
        }
        return ret;
    }

    public AtlasGlossaryCategory createCategory(AtlasGlossaryCategory glossaryCategory) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.createCategory({})", glossaryCategory);
        }

        if (Objects.isNull(glossaryCategory)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "GlossaryCategory definition missing");
        }
        if (Objects.isNull(glossaryCategory.getQualifiedName())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "GlossaryCategory qualifiedName is mandatory");
        }

        AtlasGlossaryCategory saved = dataAccess.save(glossaryCategory);

        // Attempt relation creation
        if (Objects.nonNull(glossaryCategory.getAnchor())) {
            processCategoryAnchor(glossaryCategory, saved);
        }

        if (Objects.nonNull(glossaryCategory.getParentCategory())) {
            processParentCategory(glossaryCategory, saved);
        }

        if (CollectionUtils.isNotEmpty(glossaryCategory.getChildrenCategories())) {
            processCategoryChildren(glossaryCategory, saved);
        }

        saved = dataAccess.load(glossaryCategory);

        setDisplayTextForRelations(glossaryCategory);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.createCategory() : {}", saved);
        }

        return saved;
    }

    public List<AtlasGlossaryCategory> createCategories(List<AtlasGlossaryCategory> glossaryCategory) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.createCategories({})", glossaryCategory);
        }
        if (Objects.isNull(glossaryCategory)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "glossaryCategory is null/empty");
        }


        List<AtlasGlossaryCategory> ret = new ArrayList<>();
        for (AtlasGlossaryCategory category : glossaryCategory) {
            ret.add(createCategory(category));
        }
        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.createCategories() : {}", ret);
        }
        return ret;
    }

    public AtlasGlossaryCategory updateCategory(AtlasGlossaryCategory glossaryCategory) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.updateCategory({})", glossaryCategory);
        }
        if (Objects.isNull(glossaryCategory)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "GlossaryCategory is null/empty");
        }

        AtlasGlossaryCategory existing = dataAccess.load(glossaryCategory);
        AtlasGlossaryCategory ret      = glossaryCategory;

        if (!existing.equals(glossaryCategory)) {
            try {
                glossaryCategory.setGuid(existing.getGuid());
                glossaryCategory.setQualifiedName(existing.getQualifiedName());

                ret = dataAccess.save(glossaryCategory);
            } catch (AtlasBaseException e) {
                LOG.debug("No immediate attribute update. Exception: {}", e.getMessage());
            } finally {
                processRelations(glossaryCategory, existing);
            }
        }

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.updateCategory() : {}", ret);
        }
        ret = dataAccess.load(glossaryCategory);

        setDisplayTextForRelations(glossaryCategory);

        return ret;
    }

    public void deleteCategory(String categoryGuid) throws AtlasBaseException {
        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.deleteCategory({})", categoryGuid);
        }
        if (Objects.isNull(categoryGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Category guid is null/empty");
        }

        dataAccess.delete(categoryGuid);

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.deleteCategory()");
        }

    }

    public List<AtlasGlossaryTerm> getGlossaryTerms(String glossaryGuid, int offset, int limit, SortOrder sortOrder) throws AtlasBaseException {
        if (Objects.isNull(glossaryGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "glossaryGuid is null/empty");
        }

        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.getGlossaryTerms({}, {}, {}, {})", glossaryGuid, offset, limit, sortOrder);
        }

        AtlasGlossary glossary = getGlossary(glossaryGuid);

        List<AtlasGlossaryTerm> ret;
        if (CollectionUtils.isNotEmpty(glossary.getTerms())) {
            List<AtlasGlossaryTerm> toLoad = glossary.getTerms().stream()
                                                     .map(t -> getAtlasGlossaryTermSkeleton(t.getTermGuid()))
                                                     .collect(Collectors.toList());
            Iterable<AtlasGlossaryTerm> terms = dataAccess.load(toLoad);
            toLoad.clear();
            terms.forEach(toLoad::add);
            if (sortOrder != null) {
                toLoad.sort((o1, o2) -> sortOrder == SortOrder.ASCENDING ?
                                                o1.getDisplayName().compareTo(o2.getDisplayName()) :
                                                o2.getDisplayName().compareTo(o1.getDisplayName()));
            }
            ret = new PaginationHelper<>(toLoad, offset, limit).getPaginatedList();
        } else {
            ret = Collections.emptyList();
        }

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.getGlossaryTerms() : {}", ret);
        }

        return ret;
    }

    public List<AtlasRelatedCategoryHeader> getGlossaryCategories(String glossaryGuid, int offset, int limit, SortOrder sortOrder) throws AtlasBaseException {
        if (Objects.isNull(glossaryGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "glossaryGuid is null/empty");
        }

        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.getGlossaryCategories({}, {}, {}, {})", glossaryGuid, offset, limit, sortOrder);
        }

        AtlasGlossary glossary = getGlossary(glossaryGuid);

        List<AtlasRelatedCategoryHeader> ret;

        List<AtlasRelatedCategoryHeader> categories = new ArrayList<>(glossary.getCategories());
        if (CollectionUtils.isNotEmpty(categories)) {
            if (sortOrder != null) {
                categories.sort((o1, o2) -> sortOrder == SortOrder.ASCENDING ?
                               o1.getDisplayText().compareTo(o2.getDisplayText()) :
                               o2.getDisplayText().compareTo(o1.getDisplayText()));
            }
            ret = new PaginationHelper<>(categories, offset, limit).getPaginatedList();
        } else {
            ret = Collections.emptyList();
        }

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.getGlossaryCategories() : {}", ret);
        }

        return ret;
    }

    public List<AtlasRelatedTermHeader> getCategoryTerms(String categoryGuid, int offset, int limit, SortOrder sortOrder) throws AtlasBaseException {
        if (Objects.isNull(categoryGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "categoryGuid is null/empty");
        }

        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.getCategoryTerms({}, {}, {}, {})", categoryGuid, offset, limit, sortOrder);
        }
        AtlasGlossaryCategory glossaryCategory = getCategory(categoryGuid);

        List<AtlasRelatedTermHeader> ret;
        List<AtlasRelatedTermHeader> terms = new ArrayList<>(glossaryCategory.getTerms());
        if (CollectionUtils.isNotEmpty(glossaryCategory.getTerms())) {
            if (sortOrder != null) {
                terms.sort((o1, o2) -> sortOrder == SortOrder.ASCENDING ?
                                                    o1.getDisplayText().compareTo(o2.getDisplayText()) :
                                                    o2.getDisplayText().compareTo(o1.getDisplayText()));
            }
            ret = new PaginationHelper<>(terms, offset, limit).getPaginatedList();
        } else {
            ret = Collections.emptyList();
        }

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.getCategoryTerms() : {}", ret);
        }
        return ret;
    }

    public Map<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> getRelatedTerms(String termGuid, int offset, int limit, SortOrder sortOrder) throws AtlasBaseException {
        if (Objects.isNull(termGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "termGuid is null/empty");
        }

        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.getRelatedTerms({}, {}, {}, {})", termGuid, offset, limit, sortOrder);
        }

        AtlasGlossaryTerm                                            glossaryTerm = getTerm(termGuid);
        Map<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> ret;
        if (glossaryTerm.hasTerms()) {
            ret = glossaryTerm.getRelatedTerms();
        } else {
            ret = Collections.emptyMap();
        }

        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.getRelatedTerms() : {}", ret);
        }

        return ret;
    }

    public Map<String, List<AtlasRelatedCategoryHeader>> getRelatedCategories(String categoryGuid, int offset, int limit, SortOrder sortOrder) throws AtlasBaseException {
        if (Objects.isNull(categoryGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "categoryGuid is null/empty");
        }

        if (DEBUG_ENABLED) {
            LOG.debug("==> GlossaryService.getRelatedCategories({}, {}, {}, {})", categoryGuid, offset, limit, sortOrder);
        }

        AtlasGlossaryCategory glossaryCategory = getCategory(categoryGuid);

        Map<String, List<AtlasRelatedCategoryHeader>> ret = new HashMap<>();
        if (glossaryCategory.getParentCategory() != null) {
            ret.put("parent", new ArrayList<AtlasRelatedCategoryHeader>() {{
                add(glossaryCategory.getParentCategory());
            }});
        }
        if (CollectionUtils.isNotEmpty(glossaryCategory.getChildrenCategories())) {
            ret.put("children", new ArrayList<>(glossaryCategory.getChildrenCategories()));
        }


        if (DEBUG_ENABLED) {
            LOG.debug("<== GlossaryService.getRelatedCategories() : {}", ret);
        }

        return ret;
    }

    public List<AtlasEntityHeader> getAssignedEntities(final String termGuid, int offset, int limit, SortOrder sortOrder) throws AtlasBaseException {
        if (Objects.isNull(termGuid)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "termGuid is null/empty");
        }

        AtlasGlossaryTerm      glossaryTerm     = dataAccess.load(getAtlasGlossaryTermSkeleton(termGuid));
        Set<AtlasEntityHeader> assignedEntities = glossaryTerm.getAssignedEntities();

        List<AtlasEntityHeader> ret;
        if (CollectionUtils.isNotEmpty(assignedEntities)) {
            ret = new ArrayList<>(assignedEntities);
            if (sortOrder != null) {
                ret.sort((o1, o2) -> sortOrder == SortOrder.ASCENDING ?
                                             o1.getDisplayText().compareTo(o2.getDisplayText()) :
                                             o2.getDisplayText().compareTo(o1.getDisplayText()));
            }
            ret = new PaginationHelper<>(assignedEntities, offset, limit).getPaginatedList();
        } else {
            ret = Collections.emptyList();
        }

        return ret;
    }

    private static AtlasGlossary getGlossarySkeleton(String glossaryGuid) {
        AtlasGlossary glossary = new AtlasGlossary();
        glossary.setGuid(glossaryGuid);
        return glossary;
    }

    private void processAssignedEntities(final AtlasGlossaryTerm newObj, final AtlasGlossaryTerm existing) throws AtlasBaseException {
        if (newObj.equals(existing)) return;

        if (CollectionUtils.isNotEmpty(newObj.getAssignedEntities())) {
            for (AtlasEntityHeader entityHeader : newObj.getAssignedEntities()) {
                createRelationship(defineTermAssignment(existing.getGuid(), entityHeader));
            }
        }
    }

    private void setDisplayTextForRelations(final AtlasGlossary ret) throws AtlasBaseException {
        if (Objects.nonNull(ret.getTerms())) {
            setDisplayNameForTerms(ret.getTerms());
        }

        if (Objects.nonNull(ret.getCategories())) {
            setDisplayNameForRelatedCategories(ret.getCategories());
        }
    }

    private void setDisplayTextForRelations(final AtlasGlossaryTerm ret) throws AtlasBaseException {
        if (Objects.nonNull(ret.getCategories())) {
            setDisplayNameForTermCategories(ret.getCategories());
        }
        if (Objects.nonNull(ret.getRelatedTerms())) {
            for (Map.Entry<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> entry : ret.getRelatedTerms().entrySet()) {
                setDisplayNameForTerms(entry.getValue());
            }
        }
    }

    private void setDisplayTextForRelations(final AtlasGlossaryCategory glossaryCategory) throws AtlasBaseException {
        if (Objects.nonNull(glossaryCategory.getChildrenCategories())) {
            setDisplayNameForRelatedCategories(glossaryCategory.getChildrenCategories());
        }
        if (Objects.nonNull(glossaryCategory.getTerms())) {
            setDisplayNameForTerms(glossaryCategory.getTerms());
        }
    }

    private void processRelations(final AtlasGlossaryTerm newObj, final AtlasGlossaryTerm existing) throws AtlasBaseException {
        boolean hasRelatedTerms     = newObj.hasTerms();
        boolean hasTermAnchor       = Objects.nonNull(newObj.getAnchor());
        boolean hasCategories       = Objects.nonNull(newObj.getCategories());

        if (hasTermAnchor) {
            processTermAnchor(newObj, existing);
        }
        if (hasRelatedTerms) {
            processRelatedTerms(newObj, existing);
        }
        if (hasCategories) {
            processAssociatedCategories(newObj, existing);
        }
    }

    private void processRelations(final AtlasGlossaryCategory newObj, final AtlasGlossaryCategory existing) throws AtlasBaseException {
        boolean hasParent   = Objects.nonNull(newObj.getParentCategory());
        boolean hasChildren = Objects.nonNull(newObj.getChildrenCategories());
        boolean hasAnchor   = Objects.nonNull(newObj.getAnchor());
        boolean hasTerms    = Objects.nonNull(newObj.getTerms());

        if (hasAnchor) {
            processCategoryAnchor(newObj, existing);
        }
        if (hasParent) {
            processParentCategory(newObj, existing);
        }
        if (hasChildren) {
            processCategoryChildren(newObj, existing);
        }
        if (hasTerms) {
            processAssociatedTerms(newObj, existing);
        }
    }

    private void setDisplayNameForTermCategories(final Set<AtlasTermCategorizationHeader> categorizationHeaders) throws AtlasBaseException {
        List<AtlasGlossaryCategory> categories = categorizationHeaders
                                                         .stream()
                                                         .map(id -> getAtlasGlossaryCategorySkeleton(id.getCategoryGuid()))
                                                         .collect(Collectors.toList());
        Map<String, AtlasGlossaryCategory> categoryMap = new HashMap<>();
        dataAccess.load(categories).forEach(c -> categoryMap.put(c.getGuid(), c));
        categorizationHeaders.forEach(c -> c.setDisplayText(categoryMap.get(c.getCategoryGuid()).getDisplayName()));
    }

    private void setDisplayNameForRelatedCategories(final Set<AtlasRelatedCategoryHeader> categoryHeaders) throws AtlasBaseException {
        List<AtlasGlossaryCategory> categories = categoryHeaders
                                                    .stream()
                                                    .map(id -> getAtlasGlossaryCategorySkeleton(id.getCategoryGuid()))
                                                    .collect(Collectors.toList());
        Map<String, AtlasGlossaryCategory> categoryMap = new HashMap<>();
        dataAccess.load(categories).forEach(c -> categoryMap.put(c.getGuid(), c));
        categoryHeaders.forEach(c -> c.setDisplayText(categoryMap.get(c.getCategoryGuid()).getDisplayName()));
    }

    private void setDisplayNameForTerms(final Set<AtlasRelatedTermHeader> termHeaders) throws AtlasBaseException {
        List<AtlasGlossaryTerm> terms = termHeaders
                                                .stream()
                                                .map(id -> getAtlasGlossaryTermSkeleton(id.getTermGuid()))
                                                .collect(Collectors.toList());
        Map<String, AtlasGlossaryTerm> termMap = new HashMap<>();
        dataAccess.load(terms).iterator().forEachRemaining(t -> termMap.put(t.getGuid(), t));

        termHeaders.forEach(t -> t.setDisplayText(termMap.get(t.getTermGuid()).getDisplayName()));
    }

    private void processAssociatedCategories(final AtlasGlossaryTerm newObj, final AtlasGlossaryTerm existing) throws AtlasBaseException {
        if (newObj.equals(existing)) return;

        Set<AtlasTermCategorizationHeader> categories = newObj.getCategories();
        if (Objects.nonNull(categories)) {
            Set<AtlasTermCategorizationHeader> existingCategories = existing.getCategories();
            for (AtlasTermCategorizationHeader category : categories) {
                if (Objects.isNull(category.getCategoryGuid())) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Linked category guid can't be empty");
                } else {
                    if (Objects.nonNull(existingCategories) && existingCategories.contains(category)) {
                        if (DEBUG_ENABLED) {
                            LOG.debug("Skipping linked category {}", category.getCategoryGuid());
                        }
                        continue;
                    }
                    if (DEBUG_ENABLED) {
                        LOG.debug("Creating relation between term = {} and category = {}", existing.getGuid(), category.getCategoryGuid());
                    }
                    createRelationship(defineCategorizedTerm(category, existing.getGuid()));
                }
            }
        }
    }

    private void processAssociatedTerms(final AtlasGlossaryCategory glossaryCategory, final AtlasGlossaryCategory existing) throws AtlasBaseException {
        if (Objects.equals(glossaryCategory.getTerms(), existing.getTerms())) return;

        for (AtlasRelatedTermHeader linkedTerm : glossaryCategory.getTerms()) {
            if (Objects.isNull(linkedTerm.getTermGuid())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Linked term guid can't be empty");
            } else {
                // Don't process existing child relation
                Set<AtlasRelatedTermHeader> existingTerms = existing.getTerms();
                if (Objects.nonNull(existingTerms) && existingTerms.contains(linkedTerm)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Skipping linked term {}", linkedTerm.getTermGuid());
                    }
                    continue;
                }

                if (DEBUG_ENABLED) {
                    LOG.debug("Creating relation between category = {} and term = {}", existing.getGuid(), linkedTerm.getTermGuid());
                }
                // TODO: Accept the relationship attributes as well
                createRelationship(defineCategorizedTerm(existing.getGuid(), linkedTerm));
            }
        }
    }

    private void processTermAnchor(final AtlasGlossaryTerm glossaryTerm, final AtlasGlossaryTerm saved) throws AtlasBaseException {
        if (Objects.isNull(glossaryTerm.getAnchor())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "glossaryTerm anchor mandatory attribute");
        }

        if (Objects.equals(glossaryTerm.getAnchor(), saved.getAnchor())) return;

        if (Objects.isNull(glossaryTerm.getAnchor().getGlossaryGuid())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Anchor guid can't be empty");
        } else {
            if (DEBUG_ENABLED) {
                LOG.debug("Creating relation between glossary = {} and term = {}", glossaryTerm.getAnchor().getGlossaryGuid(), saved.getGuid());
            }
            createRelationship(defineTermAnchorRelation(glossaryTerm.getAnchor().getGlossaryGuid(), glossaryTerm.getGuid()));
        }
    }

    private void processCategoryAnchor(final AtlasGlossaryCategory newObj, final AtlasGlossaryCategory existing) throws AtlasBaseException {
        if (Objects.isNull(newObj.getAnchor())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "glossaryCategory anchor mandatory attribute");
        }

        // Don't process anchor if no change
        if (Objects.equals(newObj.getAnchor(), existing.getAnchor())) return;

        if (Objects.isNull(newObj.getAnchor().getGlossaryGuid())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Category anchor guid can't be empty");
        } else {
            if (DEBUG_ENABLED) {
                LOG.debug("Creating relation between glossary = {} and category = {}", newObj.getAnchor().getGlossaryGuid(), existing.getGuid());
            }
            createRelationship(defineCategoryAnchorRelation(newObj.getAnchor().getGlossaryGuid(), existing.getGuid()));
        }
    }

    private void processRelatedTerms(final AtlasGlossaryTerm incomingObj, final AtlasGlossaryTerm savedObj) throws AtlasBaseException {
        if (incomingObj.hasTerms()) {
            for (Map.Entry<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> entry : incomingObj.getRelatedTerms().entrySet()) {
                AtlasGlossaryTerm.Relation  relation = entry.getKey();
                Set<AtlasRelatedTermHeader> terms    = entry.getValue();
                if (DEBUG_ENABLED) {
                    LOG.debug("Creating relation {}", relation);
                    LOG.debug("Related Term count = {}", terms.size());
                }
                if (Objects.nonNull(terms)) {
                    for (AtlasRelatedTermHeader atlasGlossaryTerm : terms) {
                        createRelationship(defineTermRelation(relation.getRelationName(), savedObj.getGuid(), atlasGlossaryTerm));
                    }
                }
            }
        }
        // TODO: Process other term relations as well
    }

    private void processCategoryChildren(final AtlasGlossaryCategory newObj, final AtlasGlossaryCategory existing) throws AtlasBaseException {
        for (AtlasRelatedCategoryHeader childCategory : newObj.getChildrenCategories()) {
            if (Objects.isNull(childCategory.getCategoryGuid())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Child category guid can't be empty");
            } else {
                // Don't process existing child relation
                Set<AtlasRelatedCategoryHeader> existingChildren = existing.getChildrenCategories();
                if (Objects.nonNull(existingChildren) && existingChildren.contains(childCategory)) {
                    if (DEBUG_ENABLED) {
                        LOG.debug("Skipping category child {}", childCategory.getCategoryGuid());
                    }
                    continue;
                }

                if (DEBUG_ENABLED) {
                    LOG.debug("Creating relation between glossary = {} and term = {}", existing.getGuid(), childCategory.getCategoryGuid());
                }
                // TODO: Accept the relationship attributes as well
                createRelationship(defineCategoryHierarchyLink(existing.getGuid(), childCategory));
            }
        }
    }

    private void processParentCategory(final AtlasGlossaryCategory newObj, final AtlasGlossaryCategory existing) throws AtlasBaseException {
        // Don't process unchanged parent
        if (Objects.equals(newObj.getParentCategory(), existing.getParentCategory())) return;

        if (Objects.isNull(newObj.getParentCategory().getCategoryGuid())) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Parent category guid can't be empty");
        } else {
            if (DEBUG_ENABLED) {
                LOG.debug("Creating category hierarchy b/w parent = {} and child = {}", newObj.getParentCategory().getCategoryGuid(), existing.getGuid());
            }
            createRelationship(defineCategoryHierarchyLink(newObj.getParentCategory(), newObj.getGuid()));
        }
    }

    private Map<String, List<AtlasGlossaryTerm>> loadTerms(final Map<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> relatedTerms,
                                                           final int offset,
                                                           final int limit,
                                                           final SortOrder sortOrder) throws AtlasBaseException {
        Map<String, List<AtlasGlossaryTerm>> ret = new HashMap<>();

        for (Map.Entry<AtlasGlossaryTerm.Relation, Set<AtlasRelatedTermHeader>> entry : relatedTerms.entrySet()) {
            ret.put(entry.getKey().getRelationAttrName(), loadTerms(entry.getValue(), offset, limit, sortOrder));
        }

        return ret;
    }

    private List<AtlasGlossaryTerm> loadTerms(final Set<AtlasRelatedTermHeader> terms,
                                              final int offset,
                                              final int limit,
                                              final SortOrder sortOrder) throws AtlasBaseException {
        return loadTerms(new ArrayList<>(terms), offset, limit, sortOrder);
    }

    private List<AtlasGlossaryTerm> loadTerms(final List<AtlasRelatedTermHeader> terms,
                                              final int offset,
                                              final int limit,
                                              final SortOrder sortOrder) throws AtlasBaseException {
        Objects.requireNonNull(terms);
        List<AtlasGlossaryTerm> ret;

        ret = terms.stream().map(id -> getAtlasGlossaryTermSkeleton(id.getTermGuid())).collect(Collectors.toList());
        Iterable<AtlasGlossaryTerm> loadedTerms = dataAccess.load(ret);
        ret.clear();
        loadedTerms.forEach(ret::add);
        // Sort only when needed
        if (sortOrder != null) {
            ret.sort((o1, o2) -> sortOrder == SortOrder.ASCENDING ?
                                         o1.getDisplayName().compareTo(o2.getDisplayName()) :
                                         o2.getDisplayName().compareTo(o1.getDisplayName()));
        }
        return new PaginationHelper<>(ret, offset, limit).getPaginatedList();
    }

    private List<AtlasGlossaryCategory> loadCategories(final Set<AtlasRelatedCategoryHeader> categories,
                                                       final int offset,
                                                       final int limit,
                                                       final SortOrder sortOrder) throws AtlasBaseException {
        return loadCategories(new ArrayList<>(categories), offset, limit, sortOrder);
    }

    private List<AtlasGlossaryCategory> loadCategories(final List<AtlasRelatedCategoryHeader> categories,
                                                       final int offset,
                                                       final int limit,
                                                       final SortOrder sortOrder) throws AtlasBaseException {
        Objects.requireNonNull(categories);
        List<AtlasGlossaryCategory> ret = categories.stream()
                                                    .map(id -> getAtlasGlossaryCategorySkeleton(id.getCategoryGuid()))
                                                    .collect(Collectors.toList());
        Iterable<AtlasGlossaryCategory> loadedCategories = dataAccess.load(ret);
        ret.clear();
        loadedCategories.forEach(ret::add);

        // Sort only when needed
        if (sortOrder != null) {
            ret.sort((o1, o2) -> sortOrder == SortOrder.ASCENDING ?
                                         o1.getDisplayName().compareTo(o2.getDisplayName()) :
                                         o2.getDisplayName().compareTo(o1.getDisplayName()));
        }

        return new PaginationHelper<>(ret, offset, limit).getPaginatedList();
    }

    private AtlasGlossaryTerm getAtlasGlossaryTermSkeleton(final String termGuid) {
        AtlasGlossaryTerm glossaryTerm = new AtlasGlossaryTerm();
        glossaryTerm.setGuid(termGuid);
        return glossaryTerm;
    }

    private AtlasGlossaryCategory getAtlasGlossaryCategorySkeleton(final String categoryGuid) {
        AtlasGlossaryCategory glossaryCategory = new AtlasGlossaryCategory();
        glossaryCategory.setGuid(categoryGuid);
        return glossaryCategory;
    }

    private void createRelationship(AtlasRelationship relationship) throws AtlasBaseException {
        try {
            relationshipStore.create(relationship);
        } catch (AtlasBaseException e) {
            if (!e.getAtlasErrorCode().equals(AtlasErrorCode.RELATIONSHIP_ALREADY_EXISTS)) {
                throw e;
            }
        }
    }

    private AtlasRelationship defineTermAnchorRelation(String glossaryGuid, String termGuid) {
        return new AtlasRelationship(TERM_ANCHOR, new AtlasObjectId(glossaryGuid), new AtlasObjectId(termGuid));
    }

    private AtlasRelationship defineCategoryAnchorRelation(String glossaryGuid, String categoryGuid) {
        return new AtlasRelationship(CATEGORY_ANCHOR, new AtlasObjectId(glossaryGuid), new AtlasObjectId(categoryGuid));
    }

    private AtlasRelationship defineCategoryHierarchyLink(String parentCategoryGuid, AtlasRelatedCategoryHeader childCategory) {
        AtlasRelationship relationship = new AtlasRelationship(CATEGORY_HIERARCHY, new AtlasObjectId(parentCategoryGuid), new AtlasObjectId(childCategory.getCategoryGuid()));
        relationship.setAttribute("description", childCategory.getDescription());
        return relationship;
    }

    private AtlasRelationship defineCategoryHierarchyLink(final AtlasRelatedCategoryHeader parentCategory, final String childGuid) {
        AtlasRelationship relationship = new AtlasRelationship(CATEGORY_HIERARCHY, new AtlasObjectId(parentCategory.getCategoryGuid()), new AtlasObjectId(childGuid));
        relationship.setAttribute("description", parentCategory.getDescription());
        return relationship;
    }

    private AtlasRelationship defineCategorizedTerm(String categoryGuid, AtlasRelatedTermHeader relatedTermId) {
        AtlasRelationship relationship = new AtlasRelationship(TERM_CATEGORIZATION, new AtlasObjectId(categoryGuid), new AtlasObjectId(relatedTermId.getTermGuid()));

        relationship.setAttribute("expression", relatedTermId.getExpression());
        relationship.setAttribute("description", relatedTermId.getDescription());
        relationship.setAttribute("steward", relatedTermId.getSteward());
        relationship.setAttribute("source", relatedTermId.getSource());
        if (Objects.nonNull(relatedTermId.getStatus())) {
            relationship.setAttribute("status", relatedTermId.getStatus().name());
        }

        return relationship;
    }

    private AtlasRelationship defineCategorizedTerm(AtlasTermCategorizationHeader relatedCategoryId, String termId) {
        AtlasRelationship relationship = new AtlasRelationship(TERM_CATEGORIZATION, new AtlasObjectId(relatedCategoryId.getCategoryGuid()), new AtlasObjectId(termId));

        relationship.setAttribute("description", relatedCategoryId.getDescription());
        if (Objects.nonNull(relatedCategoryId.getStatus())) {
            relationship.setAttribute("status", relatedCategoryId.getStatus().name());
        }

        return relationship;
    }

    private AtlasRelationship defineTermRelation(String relation, String end1TermGuid, AtlasRelatedTermHeader end2RelatedTerm) {
        AtlasRelationship relationship = new AtlasRelationship(relation, new AtlasObjectId(end1TermGuid), new AtlasObjectId(end2RelatedTerm.getTermGuid()));

        relationship.setAttribute("expression", end2RelatedTerm.getExpression());
        relationship.setAttribute("description", end2RelatedTerm.getDescription());
        relationship.setAttribute("steward", end2RelatedTerm.getSteward());
        relationship.setAttribute("source", end2RelatedTerm.getSource());
        if (Objects.nonNull(end2RelatedTerm.getStatus())) {
            relationship.setAttribute("status", end2RelatedTerm.getStatus().name());
        }

        return relationship;
    }

    private AtlasRelationship defineTermAssignment(String termGuid, AtlasEntityHeader entityHeader) {
        return new AtlasRelationship(TERM_ASSIGNMENT, new AtlasObjectId(termGuid), new AtlasObjectId(entityHeader.getGuid()));
    }

    static class PaginationHelper<T> {
        private int     pageStart;
        private int     pageEnd;
        private int     maxSize;
        private List<T> items;

        PaginationHelper(Collection<T> items, int offset, int limit) {
            Objects.requireNonNull(items, "items can't be empty/null");
            this.items = new ArrayList<>(items);
            this.maxSize = items.size();

            // If limit is negative then limit is effectively the maxSize, else the smaller one out of limit and maxSize
            int adjustedLimit = limit < 0 ? maxSize : Integer.min(maxSize, limit);
            // Page starting can only be between zero and adjustedLimit
            pageStart = Integer.max(0, offset);
            // Page end can't exceed the maxSize
            pageEnd = Integer.min(adjustedLimit + pageStart, maxSize);
        }

        List<T> getPaginatedList() {
            List<T> ret;
            if (isValidOffset()) {
                if (isPagingNeeded()) {
                    ret = items.subList(pageStart, pageEnd);
                } else {
                    ret = items;
                }
            } else {
                ret = Collections.emptyList();
            }

            return ret;
        }

        private boolean isPagingNeeded() {
            return !(pageStart == 0 && pageEnd == maxSize) && pageStart <= pageEnd;
        }

        private boolean isValidOffset() {
            return pageStart <= maxSize;
        }
    }
}
