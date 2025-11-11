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
package org.apache.atlas.repository.store.graph.v2.preprocessor.glossary;


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphMapper;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.ATLAS_GLOSSARY_ENTITY_TYPE;
import static org.apache.atlas.repository.Constants.CATEGORY_PARENT_EDGE_LABEL;
import static org.apache.atlas.repository.Constants.CATEGORY_TERMS_EDGE_LABEL;
import static org.apache.atlas.repository.Constants.GUID_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.UNIQUE_QUALIFIED_NAME;
import static org.apache.atlas.repository.graph.GraphHelper.getActiveChildrenVertices;
import static org.apache.atlas.repository.graph.GraphHelper.getActiveParentVertices;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTaskFactory.UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE;
import static org.apache.atlas.repository.util.AtlasEntityUtils.mapOf;
import static org.apache.atlas.type.Constants.*;

public class CategoryPreProcessor extends AbstractGlossaryPreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CategoryPreProcessor.class);

    private AtlasEntityHeader anchor;
    private AtlasEntityHeader parentCategory;
    private EntityGraphMapper entityGraphMapper;
    private EntityMutationContext context;

    public CategoryPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever,
                                AtlasGraph graph, TaskManagement taskManagement, EntityGraphMapper entityGraphMapper) {
        super(typeRegistry, entityRetriever, graph, taskManagement);
        this.entityGraphMapper = entityGraphMapper;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("CategoryPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

        this.context = context;

        AtlasEntity entity = (AtlasEntity) entityStruct;
        AtlasVertex vertex = context.getVertex(entity.getGuid());

        setAnchorAndParent(entity, context);

        switch (operation) {
            case CREATE:
                processCreateCategory(entity, vertex);
                break;
            case UPDATE:
                processUpdateCategory(entity, vertex);
                break;
        }
    }

    private void processCreateCategory(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processCreateCategory");
        String catName = (String) entity.getAttribute(NAME);
        String parentQname = null;

        if (StringUtils.isEmpty(catName) || isNameInvalid(catName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        String glossaryQualifiedName = (String) anchor.getAttribute(QUALIFIED_NAME);
        categoryExists(catName, glossaryQualifiedName);
        validateParent(glossaryQualifiedName);

        if (parentCategory != null) {
            parentQname = (String) parentCategory.getAttribute(QUALIFIED_NAME);
        }
        String lexicographicalSortOrder = (String) entity.getAttribute(LEXICOGRAPHICAL_SORT_ORDER);
        if(StringUtils.isEmpty(lexicographicalSortOrder)){
            assignNewLexicographicalSortOrder(entity,glossaryQualifiedName, parentQname, this.discovery);
        } else {
            isValidLexoRank(lexicographicalSortOrder, glossaryQualifiedName, parentQname, this.discovery);
        }

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(vertex));
        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(entity)),
                "create entity: type=", entity.getTypeName());

        validateChildren(entity, null);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateCategory(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateCategory");
        String catName = (String) entity.getAttribute(NAME);
        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        if (StringUtils.isEmpty(catName) || isNameInvalid(catName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        AtlasEntity storedCategory = entityRetriever.toAtlasEntity(vertex);
        AtlasRelatedObjectId currentGlossary = (AtlasRelatedObjectId) storedCategory.getRelationshipAttribute(ANCHOR);
        AtlasEntityHeader currentGlossaryHeader = entityRetriever.toAtlasEntityHeader(currentGlossary.getGuid());
        String currentGlossaryQualifiedName = (String) currentGlossaryHeader.getAttribute(QUALIFIED_NAME);

        String newGlossaryQualifiedName = (String) anchor.getAttribute(QUALIFIED_NAME);

        String lexicographicalSortOrder = (String) entity.getAttribute(LEXICOGRAPHICAL_SORT_ORDER);
        String parentQname = "";
        if(Objects.nonNull(parentCategory)) {
            parentQname = (String) parentCategory.getAttribute(QUALIFIED_NAME);
        }
        if(StringUtils.isNotEmpty(lexicographicalSortOrder)) {
            isValidLexoRank(lexicographicalSortOrder, newGlossaryQualifiedName, parentQname, this.discovery);
        } else {
            entity.removeAttribute(LEXICOGRAPHICAL_SORT_ORDER);
        }

        if (!currentGlossaryQualifiedName.equals(newGlossaryQualifiedName)){
            //Auth check
            isAuthorized(currentGlossaryHeader, anchor);

            processMoveCategoryToAnotherGlossary(entity, vertex, currentGlossaryQualifiedName, newGlossaryQualifiedName, vertexQnName);

        } else {
            String vertexName = vertex.getProperty(NAME, String.class);
            if (!vertexName.equals(catName)) {
                categoryExists(catName, newGlossaryQualifiedName);
            }
            validateChildren(entity, storedCategory);
            validateParent(newGlossaryQualifiedName);

            entity.setAttribute(QUALIFIED_NAME, vertexQnName);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processMoveCategoryToAnotherGlossary(AtlasEntity category,
                                                      AtlasVertex categoryVertex,
                                                      String sourceGlossaryQualifiedName,
                                                      String targetGlossaryQualifiedName,
                                                      String currentCategoryQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("processMoveCategoryToAnotherGlossary");

        try {
            if (category.hasRelationshipAttribute(CATEGORY_CHILDREN) || category.hasRelationshipAttribute(CATEGORY_TERMS)) {
                throw new AtlasBaseException(BAD_REQUEST, String.format("Please do not pass relationship attributes [%s, %s] while moving Category to different Glossary",
                        CATEGORY_CHILDREN, CATEGORY_TERMS));
            }

            String categoryName = (String) category.getAttribute(NAME);

            LOG.info("Moving category {} to Glossary {}", categoryName, targetGlossaryQualifiedName);

            categoryExists(categoryName , targetGlossaryQualifiedName);
            validateParentForGlossaryChange(category, categoryVertex, targetGlossaryQualifiedName);

            String updatedQualifiedName = currentCategoryQualifiedName.replace(sourceGlossaryQualifiedName, targetGlossaryQualifiedName);

            category.setAttribute(QUALIFIED_NAME, updatedQualifiedName);

            moveChildrenToAnotherGlossary(categoryVertex, null, sourceGlossaryQualifiedName, targetGlossaryQualifiedName);

            LOG.info("Moved category {} to Glossary {}", categoryName, targetGlossaryQualifiedName);

        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void moveChildrenToAnotherGlossary(AtlasVertex childCategoryVertex,
                                               String parentCategoryQualifiedName,
                                               String sourceGlossaryQualifiedName,
                                               String targetGlossaryQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("moveChildrenToAnotherGlossary");


        try {
            LOG.info("Moving child category {} to Glossary {}", childCategoryVertex.getProperty(NAME, String.class), targetGlossaryQualifiedName);
            Map<String, Object> updatedAttributes = new HashMap<>();

            String currentCategoryQualifiedName = childCategoryVertex.getProperty(QUALIFIED_NAME, String.class);
            String updatedQualifiedName = currentCategoryQualifiedName.replace(sourceGlossaryQualifiedName, targetGlossaryQualifiedName);

            // Change category qualifiedName
            childCategoryVertex.setProperty(QUALIFIED_NAME, updatedQualifiedName);
            updatedAttributes.put(QUALIFIED_NAME, updatedQualifiedName);

            // Change category __u_qualifiedName
            childCategoryVertex.setProperty(UNIQUE_QUALIFIED_NAME, updatedQualifiedName);

            //change __glossary, __parentCategory
            childCategoryVertex.setProperty(GLOSSARY_PROPERTY_KEY, targetGlossaryQualifiedName);
            childCategoryVertex.setProperty(CATEGORIES_PARENT_PROPERTY_KEY, parentCategoryQualifiedName);

            // update glossary relationship
            updateGlossaryRelationship(childCategoryVertex, GLOSSARY_CATEGORY_REL_TYPE);

            //update system properties
            GraphHelper.setModifiedByAsString(childCategoryVertex, RequestContext.get().getUser());
            GraphHelper.setModifiedTime(childCategoryVertex, System.currentTimeMillis());

            // move terms to target Glossary
            Iterator<AtlasVertex> terms = getActiveChildrenVertices(childCategoryVertex, CATEGORY_TERMS_EDGE_LABEL);

            while (terms.hasNext()) {
                AtlasVertex termVertex = terms.next();
                moveChildTermToAnotherGlossary(termVertex, updatedQualifiedName, sourceGlossaryQualifiedName, targetGlossaryQualifiedName);
            }

            // Get all children categories of current category
            Iterator<AtlasVertex> childCategories = getActiveChildrenVertices(childCategoryVertex, CATEGORY_PARENT_EDGE_LABEL);

            while (childCategories.hasNext()) {
                AtlasVertex childVertex = childCategories.next();
                moveChildrenToAnotherGlossary(childVertex, updatedQualifiedName, sourceGlossaryQualifiedName, targetGlossaryQualifiedName);
            }

            recordUpdatedChildEntities(childCategoryVertex, updatedAttributes);

            LOG.info("Moved child category {} to Glossary {}", childCategoryVertex.getProperty(NAME, String.class), targetGlossaryQualifiedName);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public void moveChildTermToAnotherGlossary(AtlasVertex termVertex,
                                               String parentCategoryQualifiedName,
                                               String sourceGlossaryQualifiedName,
                                               String targetGlossaryQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("moveChildTermToAnotherGlossary");

        try {
            Map<String, Object> updatedAttributes = new HashMap<>();

            String termName = termVertex.getProperty(NAME, String.class);
            String termGuid = termVertex.getProperty(GUID_PROPERTY_KEY, String.class);

            LOG.info("Moving child term {} to Glossary {}", termName, targetGlossaryQualifiedName);

            //check duplicate term name
            termExists(termName, targetGlossaryQualifiedName);

            String currentTermQualifiedName = termVertex.getProperty(QUALIFIED_NAME, String.class);
            String updatedTermQualifiedName = currentTermQualifiedName.replace(sourceGlossaryQualifiedName, targetGlossaryQualifiedName);

            //qualifiedName
            termVertex.setProperty(QUALIFIED_NAME, updatedTermQualifiedName);
            updatedAttributes.put(QUALIFIED_NAME, updatedTermQualifiedName);

            // Change termVertex __u_qualifiedName
            termVertex.setProperty(UNIQUE_QUALIFIED_NAME, updatedTermQualifiedName);

            // __glossary, __categories
            termVertex.setProperty(GLOSSARY_PROPERTY_KEY, targetGlossaryQualifiedName);
            termVertex.removeProperty(CATEGORIES_PROPERTY_KEY);
            termVertex.setProperty(CATEGORIES_PROPERTY_KEY, parentCategoryQualifiedName);

            // update glossary relationship
            updateGlossaryRelationship(termVertex, GLOSSARY_TERM_REL_TYPE);

            //update system properties
            GraphHelper.setModifiedByAsString(termVertex, RequestContext.get().getUser());
            GraphHelper.setModifiedTime(termVertex, System.currentTimeMillis());

            if (checkEntityTermAssociation(currentTermQualifiedName)) {
                if (taskManagement != null && DEFERRED_ACTION_ENABLED) {
                    createAndQueueTask(UPDATE_ENTITY_MEANINGS_ON_TERM_UPDATE, termName, termName, currentTermQualifiedName, updatedTermQualifiedName, termVertex);
                } else {
                    updateMeaningsAttributesInEntitiesOnTermUpdate(termName, termName, currentTermQualifiedName, updatedTermQualifiedName, termGuid);
                }
            }

            recordUpdatedChildEntities(termVertex, updatedAttributes);

            LOG.info("Moved child term {} to Glossary {}", termName, targetGlossaryQualifiedName);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    private void validateParentForGlossaryChange(AtlasEntity category,
                                                 AtlasVertex categoryVertex,
                                                 String targetGlossaryQualifiedName) throws AtlasBaseException {

        if (!category.hasRelationshipAttribute(CATEGORY_PARENT)) {
            // parentCategory not present in payload, check in store
            Iterator<AtlasVertex> parentItr = getActiveParentVertices(categoryVertex, CATEGORY_PARENT_EDGE_LABEL);

            if (parentItr.hasNext()) {
                AtlasVertex parentCategory = parentItr.next();
                String parentCategoryQualifiedName = parentCategory.getProperty(QUALIFIED_NAME, String.class);

                if (!parentCategoryQualifiedName.endsWith(targetGlossaryQualifiedName)){
                    throw new AtlasBaseException(AtlasErrorCode.CATEGORY_PARENT_FROM_OTHER_GLOSSARY);
                }
            }
        } else {
            validateParent(targetGlossaryQualifiedName);
        }
    }

    private void categoryExists(String categoryName, String glossaryQualifiedName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("categoryExists");

        boolean exists = false;
        try {
            List mustClauseList = new ArrayList();
            mustClauseList.add(mapOf("term", mapOf("__glossary", glossaryQualifiedName)));
            mustClauseList.add(mapOf("term", mapOf("__typeName.keyword", ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE)));
            mustClauseList.add(mapOf("term", mapOf("__state", "ACTIVE")));
            mustClauseList.add(mapOf("term", mapOf("name.keyword", categoryName)));


            Map<String, Object> bool = new HashMap<>();
            if (parentCategory != null) {
                String parentQname = (String) parentCategory.getAttribute(QUALIFIED_NAME);
                mustClauseList.add(mapOf("term", mapOf("__parentCategory", parentQname)));
            } else {
                List mustNotClauseList = new ArrayList();
                mustNotClauseList.add(mapOf("exists", mapOf("field", "__parentCategory")));
                bool.put("must_not", mustNotClauseList);
            }

            bool.put("must", mustClauseList);

            Map<String, Object> dsl = mapOf("query", mapOf("bool", bool));

            List<AtlasEntityHeader> categories = indexSearchPaginated(dsl, null, this.discovery);

            if (CollectionUtils.isNotEmpty(categories)) {
                for (AtlasEntityHeader category : categories) {
                    String name = (String) category.getAttribute(NAME);
                    if (categoryName.equals(name)) {
                        exists = true;
                        break;
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }

        if (exists) {
            throw new AtlasBaseException(AtlasErrorCode.GLOSSARY_CATEGORY_ALREADY_EXISTS, categoryName);
        }
    }

    private void validateParent(String glossaryQualifiedName) throws AtlasBaseException {
        // in case parent category is present, ensure it belongs to same Glossary

        if (parentCategory != null) {
            String newParentGlossaryQualifiedName = (String) parentCategory.getAttribute(QUALIFIED_NAME);

            if (!newParentGlossaryQualifiedName.endsWith(glossaryQualifiedName)){
                throw new AtlasBaseException(AtlasErrorCode.CATEGORY_PARENT_FROM_OTHER_GLOSSARY);
            }
        }
    }

    private void validateChildren(AtlasEntity entity, AtlasEntity storedCategory) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("CategoryPreProcessor.validateChildren");
        // in case new child is being added, ensure it belongs to same Glossary

        try {
            if (entity.hasRelationshipAttribute(CATEGORY_CHILDREN) && entity.getRelationshipAttribute(CATEGORY_CHILDREN) != null) {
                List<AtlasObjectId> children = (List<AtlasObjectId>) entity.getRelationshipAttribute(CATEGORY_CHILDREN);

                if (CollectionUtils.isNotEmpty(children)) {
                    Set<String> existingChildrenGuids = new HashSet<>();

                    if (storedCategory != null &&
                            storedCategory.hasRelationshipAttribute(CATEGORY_CHILDREN) &&
                            storedCategory.getRelationshipAttribute(CATEGORY_CHILDREN) != null) {
                        List<AtlasObjectId> existingChildren = (List<AtlasObjectId>) storedCategory.getRelationshipAttribute(CATEGORY_CHILDREN);

                        existingChildrenGuids = existingChildren.stream().map(x -> x.getGuid()).collect(Collectors.toSet());
                    }

                    for (AtlasObjectId child : children) {
                        if (!existingChildrenGuids.contains(child.getGuid())) {
                            AtlasEntity newChild = entityRetriever.toAtlasEntity(child.getGuid());
                            AtlasRelatedObjectId newAnchor = (AtlasRelatedObjectId) newChild.getRelationshipAttribute(ANCHOR);

                            if (newAnchor != null && !newAnchor.getGuid().equals(anchor.getGuid())){
                                throw new AtlasBaseException(AtlasErrorCode.CATEGORY_PARENT_FROM_OTHER_GLOSSARY);
                            }
                        }
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void setAnchorAndParent(AtlasEntity entity, EntityMutationContext context) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("CategoryPreProcessor.setAnchorAndParent");
        if (anchor == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(ANCHOR);

            if (StringUtils.isNotEmpty(objectId.getGuid())) {
                AtlasVertex vertex = context.getVertex(objectId.getGuid());

                if (vertex == null) {
                    anchor = entityRetriever.toAtlasEntityHeader(objectId.getGuid());
                } else {
                    anchor = entityRetriever.toAtlasEntityHeader(vertex);
                }

            } else if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                    StringUtils.isNotEmpty( (String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                anchor = new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

            }
        }

        if (parentCategory == null) {
            AtlasObjectId objectId = (AtlasObjectId) entity.getRelationshipAttribute(CATEGORY_PARENT);

            if (objectId != null) {
                if (StringUtils.isNotEmpty(objectId.getGuid())) {
                    AtlasVertex vertex = context.getVertex(objectId.getGuid());

                    if (vertex == null) {
                        parentCategory = entityRetriever.toAtlasEntityHeader(objectId.getGuid());
                    } else {
                        parentCategory = entityRetriever.toAtlasEntityHeader(vertex);
                    }

                } else if (MapUtils.isNotEmpty(objectId.getUniqueAttributes()) &&
                        StringUtils.isNotEmpty( (String) objectId.getUniqueAttributes().get(QUALIFIED_NAME))) {
                    parentCategory = new AtlasEntityHeader(objectId.getTypeName(), objectId.getUniqueAttributes());

                }
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void updateGlossaryRelationship(AtlasVertex entityVertex, String relationshipType) throws AtlasBaseException {
        AtlasObjectId glossaryObjectId = new AtlasObjectId(anchor.getGuid(), ATLAS_GLOSSARY_ENTITY_TYPE);

        String typeName = getTypeName(entityVertex);
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
        AtlasStructType.AtlasAttribute attribute = entityType.getRelationshipAttribute(ANCHOR, relationshipType);

        entityGraphMapper.mapGlossaryRelationshipAttribute(attribute, glossaryObjectId, entityVertex, context);
    }

    private String createQualifiedName(AtlasVertex vertex) {

        if (vertex != null) {
            String catQName = vertex.getProperty(QUALIFIED_NAME, String.class);
            if (StringUtils.isNotEmpty(catQName)) {
                return catQName;
            }
        }

        return getUUID() + "@" + anchor.getAttribute(QUALIFIED_NAME);
    }

}
