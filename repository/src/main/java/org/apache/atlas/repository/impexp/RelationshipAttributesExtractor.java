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
package org.apache.atlas.repository.impexp;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.impexp.ExportService.TraversalDirection;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.apache.atlas.repository.impexp.ExportService.ExportContext;
import static org.apache.atlas.repository.impexp.ExportService.TraversalDirection.BOTH;
import static org.apache.atlas.repository.impexp.ExportService.TraversalDirection.INWARD;
import static org.apache.atlas.repository.impexp.ExportService.TraversalDirection.OUTWARD;
import static org.apache.atlas.repository.impexp.ExportService.TraversalDirection.UNKNOWN;

public class RelationshipAttributesExtractor implements ExtractStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(RelationshipAttributesExtractor.class);

    private static final String DISPLAY_NAME_PROPERTY = "Asset.__s_name";
    private static final String RELATIONSHIP_GUID_PROPERTY = "_r__guid";
    private static final String QUALIFIED_NAME_PROPERTY = "Referenceable.qualifiedName";

    private final AtlasTypeRegistry typeRegistry;

    public RelationshipAttributesExtractor(AtlasTypeRegistry typeRegistry) {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void connectedFetch(AtlasEntity entity, ExportContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> connectedFetch({}): guidsToProcess {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size());
        }

        ExportService.TraversalDirection direction = context.guidDirection.get(entity.getGuid());

        if (direction == null || direction == UNKNOWN) {
            addToBeProcessed(entity, context, OUTWARD, INWARD);
        } else {
            if (isLineageType(entity.getTypeName())) {
                direction = OUTWARD;
            }
            addToBeProcessed(entity, context, direction);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("==> connectedFetch({}): guidsToProcess {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size());
        }
    }

    @Override
    public void fullFetch(AtlasEntity entity, ExportContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> fullFetch({}): guidsToProcess {}", AtlasTypeUtil.getAtlasObjectId(entity), context.guidsToProcess.size());
        }

        List<AtlasRelatedObjectId> atlasRelatedObjectIdList = getRelatedObjectIds(entity);

        for (AtlasRelatedObjectId ar : atlasRelatedObjectIdList) {
            boolean isLineage = isLineageType(ar.getTypeName());

            if (context.skipLineage && isLineage) {
                continue;
            }
            context.addToBeProcessed(isLineage, ar.getGuid(), BOTH);
        }

        LOG.debug("<== fullFetch({}): guidsToProcess {}", entity.getGuid(), context.guidsToProcess.size());
    }

    @Override
    public void close() {
    }

    private void addToBeProcessed(AtlasEntity entity, ExportContext context, TraversalDirection... directions) {
        if (directions == null || directions.length == 0) {
            return;
        }

        boolean                    isLineageEntity  = isLineageType(entity.getTypeName());
        List<AtlasRelatedObjectId> relatedObjectIds = getRelatedObjectIds(entity);

        for (TraversalDirection direction : directions) {
            for (AtlasRelatedObjectId id : relatedObjectIds) {
                String             guid             = id.getGuid();
                TraversalDirection currentDirection = context.guidDirection.get(guid);
                boolean            isLineageId      = isLineageType(id.getTypeName());
                TraversalDirection edgeDirection    = getRelationshipEdgeDirection(id, entity.getTypeName());

                if (context.skipLineage && isLineageId) {
                    continue;
                }

                if (!isLineageEntity && direction != edgeDirection || isLineageEntity && direction == edgeDirection) {
                    continue;
                }

                if (currentDirection == null) {
                    context.addToBeProcessed(isLineageId, guid, direction);
                } else if (currentDirection == OUTWARD && direction == INWARD) {
                    context.guidsProcessed.remove(guid);
                    context.addToBeProcessed(isLineageId, guid, direction);
                }
            }
        }
    }

    private TraversalDirection getRelationshipEdgeDirection(AtlasRelatedObjectId relatedObjectId, String entityTypeName) {
        boolean isOutEdge = typeRegistry.getRelationshipDefByName(relatedObjectId.getRelationshipType()).getEndDef1().getType().equals(entityTypeName);

        return isOutEdge ? OUTWARD : INWARD;
    }

    private boolean isLineageType(String typeName) {
        AtlasEntityDef entityDef = typeRegistry.getEntityDefByName(typeName);

        return entityDef.getSuperTypes().contains(AtlasBaseTypeDef.ATLAS_TYPE_PROCESS);
    }

    private List<AtlasRelatedObjectId> getRelatedObjectIds(AtlasEntity entity) {
        List<AtlasRelatedObjectId> relatedObjectIds = new ArrayList<>();

        for (Object o : entity.getRelationshipAttributes().values()) {
            if (o instanceof AtlasRelatedObjectId) {
                relatedObjectIds.add((AtlasRelatedObjectId) o);
            } else if (o instanceof Collection) {
                relatedObjectIds.addAll((List<AtlasRelatedObjectId>) o);
            }
        }

        String typeName = entity.getTypeName();
        List<String> relatedTypes = checkForRelationshipTypes(typeName);
        if (CollectionUtils.isNotEmpty(relatedTypes)) {
            String alreadyPopulatedType = checkAlreadyPopulatedRelationshipType(entity);
            relatedTypes.remove(alreadyPopulatedType);
        }
        String entityGuid = entity.getGuid();
        List<AtlasRelatedObjectId> relatedObjectIdList = populateRelatedObjectIds(typeName, entityGuid, relatedTypes);
        relatedObjectIds.addAll(relatedObjectIdList);
        return relatedObjectIds;
    }

    private String checkAlreadyPopulatedRelationshipType(AtlasEntity entity) {
        String typeName = entity.getTypeName();
        String relationshipAttr;
        try {
            relationshipAttr = ApplicationProperties.get().getString("atlas.relationship.attr.for." + typeName);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
        Object o = entity.getRelationshipAttribute(relationshipAttr);
        String relatedTypeName = null;
        if (o instanceof AtlasRelatedObjectId) {
            AtlasRelatedObjectId relatedObjectId = (AtlasRelatedObjectId) o;
            relatedTypeName = relatedObjectId.getTypeName();
        } else if (o instanceof Collection) {
            List<AtlasRelatedObjectId> relatedObjectIds = (List<AtlasRelatedObjectId>) o;
            if (!CollectionUtils.isEmpty(relatedObjectIds)) {
                relatedTypeName = relatedObjectIds.get(0).getTypeName();
            }
        }
        return relatedTypeName;
    }

    private List<String> checkForRelationshipTypes(String typeName) {
        List<String> relatedTypeList = null;
        try {
            String[] relatedTypes = ApplicationProperties.get().getStringArray("atlas.relationship.types.for." + typeName);
            if (relatedTypes != null && relatedTypes.length > 0) {
                relatedTypeList = new ArrayList<>();
                for (String relatedType : relatedTypes) {
                    relatedTypeList.add(relatedType);
                }
            }
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
        return relatedTypeList;
    }

    private List<AtlasRelatedObjectId> populateRelatedObjectIds(String typeName, String entityGuid, List<String> relatedTypes) {
        List<AtlasRelatedObjectId> relatedObjectIds = new ArrayList<>();
        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(entityGuid);
        if (CollectionUtils.isEmpty(relatedTypes)) {
            return relatedObjectIds;
        }
        for (String relatedType : relatedTypes) {
            String edgeLabel;
            try {
                edgeLabel = ApplicationProperties.get().getString("atlas.relationship.edge.label.between." + typeName + "." + relatedType);
            } catch (AtlasException e) {
                throw new RuntimeException(e);
            }
            Iterator<AtlasEdge> edges = GraphHelper.getIncomingEdgesByLabel(vertex, edgeLabel);
            while (edges.hasNext()) {
                AtlasEdge relationshipEdge = edges.next();
                AtlasVertex relationshipVertex = relationshipEdge.getOutVertex();
                AtlasRelatedObjectId relatedObjectId = new AtlasRelatedObjectId();
                AtlasEntity.Status vertexStatus = GraphHelper.getStatus(relationshipVertex);
                relatedObjectId.setEntityStatus(vertexStatus);
                String displayText = relationshipVertex.getProperty(DISPLAY_NAME_PROPERTY, String.class);
                relatedObjectId.setDisplayText(displayText);
                relatedObjectId.setRelationshipType(GraphHelper.getTypeName(relationshipEdge));
                String relationshipGuid = relationshipEdge.getProperty(RELATIONSHIP_GUID_PROPERTY, String.class);
                relatedObjectId.setRelationshipGuid(relationshipGuid);
                AtlasRelationship.Status edgeStatus = GraphHelper.getEdgeStatus(relationshipEdge);
                relatedObjectId.setRelationshipStatus(edgeStatus);
                AtlasStruct relationshipAttributes = new AtlasStruct();
                relationshipAttributes.setTypeName(GraphHelper.getTypeName(relationshipEdge));
                relatedObjectId.setRelationshipAttributes(relationshipAttributes);
                String qualifiedName = relationshipVertex.getProperty(QUALIFIED_NAME_PROPERTY, String.class);
                relatedObjectId.setQualifiedName(qualifiedName);
                relatedObjectId.setGuid(GraphHelper.getGuid(relationshipVertex));
                relatedObjectId.setTypeName(GraphHelper.getTypeName(relationshipVertex));
                relatedObjectIds.add(relatedObjectId);
            }
        }
        return relatedObjectIds;
    }
}
