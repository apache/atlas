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
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutations;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.EntityMutationContext;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.atlas.repository.Constants.NAME;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;

public class CategoryPreProcessor implements PreProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(CategoryPreProcessor.class);

    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;

    private AtlasEntityHeader anchor;
    private AtlasEntityHeader parentCategory;

    public CategoryPreProcessor(AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityRetriever) {
        this.entityRetriever = entityRetriever;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void processAttributes(AtlasStruct entityStruct, EntityMutationContext context,
                                  EntityMutations.EntityOperation operation) throws AtlasBaseException {
        //Handle name & qualifiedName
        if (LOG.isDebugEnabled()) {
            LOG.debug("CategoryPreProcessor.processAttributes: pre processing {}, {}",
                    entityStruct.getAttribute(QUALIFIED_NAME), operation);
        }

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

        if (StringUtils.isEmpty(catName) || isNameInvalid(catName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        if (parentCategory != null) {
            AtlasEntity newParent = entityRetriever.toAtlasEntity(parentCategory.getGuid());
            AtlasRelatedObjectId newAnchor = (AtlasRelatedObjectId) newParent.getRelationshipAttribute(ANCHOR);

            if (newAnchor != null && !newAnchor.getGuid().equals(anchor.getGuid())){
                throw new AtlasBaseException(AtlasErrorCode.CATEGORY_PARENT_FROM_OTHER_GLOSSARY);
            }
        }

        validateChildren(entity, null);

        entity.setAttribute(QUALIFIED_NAME, createQualifiedName(vertex));
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void processUpdateCategory(AtlasEntity entity, AtlasVertex vertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("processUpdateCategory");
        String catName = (String) entity.getAttribute(NAME);
        String vertexQnName = vertex.getProperty(QUALIFIED_NAME, String.class);

        if (StringUtils.isEmpty(catName) || isNameInvalid(catName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_DISPLAY_NAME);
        }

        AtlasEntity storeObject = entityRetriever.toAtlasEntity(vertex);
        AtlasRelatedObjectId existingAnchor = (AtlasRelatedObjectId) storeObject.getRelationshipAttribute(ANCHOR);
        if (existingAnchor != null && !existingAnchor.getGuid().equals(anchor.getGuid())){
            throw new AtlasBaseException(AtlasErrorCode.ACHOR_UPDATION_NOT_SUPPORTED);
        }

        if (parentCategory != null) {
            AtlasEntity newParent = entityRetriever.toAtlasEntity(parentCategory.getGuid());
            AtlasRelatedObjectId newAnchor = (AtlasRelatedObjectId) newParent.getRelationshipAttribute(ANCHOR);

            if (newAnchor != null && !newAnchor.getGuid().equals(existingAnchor.getGuid())){
                throw new AtlasBaseException(AtlasErrorCode.CATEGORY_PARENT_FROM_OTHER_GLOSSARY);
            }
        }

        validateChildren(entity, storeObject);

        entity.setAttribute(QUALIFIED_NAME, vertexQnName);
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void validateChildren(AtlasEntity entity, AtlasEntity storeObject) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("CategoryPreProcessor.validateChildren");
        List<AtlasObjectId> existingChildren = new ArrayList<>();
        if (storeObject != null) {
            existingChildren = (List<AtlasObjectId>) storeObject.getRelationshipAttribute(CATEGORY_CHILDREN);
        }
        Set<String> existingChildrenGuids = existingChildren.stream().map(x -> x.getGuid()).collect(Collectors.toSet());

        List<AtlasObjectId> children = (List<AtlasObjectId>) entity.getRelationshipAttribute(CATEGORY_CHILDREN);

        if (CollectionUtils.isNotEmpty(children)) {
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
        RequestContext.get().endMetricRecord(metricRecorder);
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
