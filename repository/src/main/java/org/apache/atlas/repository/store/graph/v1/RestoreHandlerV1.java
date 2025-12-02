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
package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.AtlasRelationshipStoreV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.*;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections.iterators.IteratorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;

import static org.apache.atlas.model.TypeCategory.*;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.*;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.getIdFromEdge;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.isReference;
import static org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessorUtils.*;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.OUT;

@Singleton
@Component
public class RestoreHandlerV1 {
    public static final Logger LOG = LoggerFactory.getLogger(RestoreHandlerV1.class);
    protected final GraphHelper graphHelper;
    private final AtlasTypeRegistry typeRegistry;
    private final EntityGraphRetriever entityRetriever;

    @Inject
    public RestoreHandlerV1(AtlasGraph graph, AtlasTypeRegistry typeRegistry, EntityGraphRetriever entityGraphRetriever) {
        this.graphHelper = new GraphHelper(graph);
        this.typeRegistry = typeRegistry;
        this.entityRetriever = entityGraphRetriever;
    }

    private void restoreEdge(AtlasEdge edge) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RestoreHandlerV1.restoreEdge({})", string(edge));
        }

        if (isClassificationEdge(edge)) {
            AtlasVertex classificationVertex = edge.getInVertex();
            AtlasGraphUtilsV2.setEncodedProperty(classificationVertex, CLASSIFICATION_ENTITY_STATUS, ACTIVE.name());
        }

        if (AtlasGraphUtilsV2.getState(edge) == DELETED) {
            AtlasGraphUtilsV2.setEncodedProperty(edge, STATE_PROPERTY_KEY, ACTIVE.name());
            AtlasGraphUtilsV2.setEncodedProperty(edge, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
            AtlasGraphUtilsV2.setEncodedProperty(edge, MODIFIED_BY_KEY, RequestContext.get().getUser());
        }

        if (isRelationshipEdge(edge))
            AtlasRelationshipStoreV2.recordRelationshipMutation(AtlasRelationshipStoreV2.RelationshipMutation.RELATIONSHIP_RESTORE, edge, entityRetriever);
    }


    public void restoreEntities(Collection<AtlasVertex> instanceVertices) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("RestoreHandlerV1.restoreEntities");

        try {
            final RequestContext requestContext = RequestContext.get();
            final Set<AtlasVertex> restoreCandidateVertices = new HashSet<>();

            for (AtlasVertex instanceVertex : instanceVertices) {
                final String guid = AtlasGraphUtilsV2.getIdFromVertex(instanceVertex);

                if (skipVertexForRestore(instanceVertex)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping restoring of entity={} as it is already active", guid);
                    }
                    continue;
                }

                String typeName = AtlasGraphUtilsV2.getTypeName(instanceVertex);
                if (typeName.equals(DATA_DOMAIN_ENTITY_TYPE) || typeName.equals(DATA_PRODUCT_ENTITY_TYPE)) {
                    if (!canRestoreEntity(typeName, instanceVertex)) {
                        throw new AtlasBaseException(AtlasErrorCode.OPERATION_NOT_SUPPORTED, "Cannot restore " + typeName + " with guid " + guid + " because it has no parent domain relationship");
                    }
                }

                // Record all restoring candidate entities in RequestContext
                // and gather restoring candidate vertices.
                for (VertexInfo vertexInfo : getOwnedVertices(instanceVertex)) {
                    requestContext.recordEntityRestore(vertexInfo.getEntity(), vertexInfo.getVertex());
                    restoreCandidateVertices.add(vertexInfo.getVertex());
                }
            }

            // Restore traits and vertices.
            for (AtlasVertex restoreCandidateVertex : restoreCandidateVertices) {
                restoreAllClassifications(restoreCandidateVertex);
                restoreTypeVertex(restoreCandidateVertex);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private boolean canRestoreEntity(String typeName, AtlasVertex instanceVertex) throws AtlasBaseException {
        AtlasEntity entity = entityRetriever.toAtlasEntity(instanceVertex);
        boolean flag = true;

        if (typeName.equals(DATA_DOMAIN_ENTITY_TYPE)) {
            boolean noParentRel = entity.getRelationshipAttribute(PARENT_DOMAIN_REL_TYPE) == null;
            if (noParentRel) {
                // To ensure super domains can be restored
                String superDomainQualifiedName = instanceVertex.getProperty(SUPER_DOMAIN_QN_ATTR, String.class);
                String parentQualifiedName = instanceVertex.getProperty(PARENT_DOMAIN_QN_ATTR, String.class);

                boolean isSuperDomain = superDomainQualifiedName == null && parentQualifiedName == null;
                flag = isSuperDomain;
            }
        } else if (typeName.equals(DATA_PRODUCT_ENTITY_TYPE)) {
            flag = entity.getRelationshipAttribute(DATA_DOMAIN_REL_TYPE) != null;
        }
        return flag;
    }

    private void restoreEdgeBetweenVertices(AtlasVertex outVertex, AtlasVertex inVertex, AtlasStructType.AtlasAttribute attribute) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("RestoreHandlerV1.restoreEdgeBetweenVertices");

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Removing edge from {} to {} with attribute name {}", string(outVertex), string(inVertex), attribute.getName());
            }


            String edgeLabel = attribute.getRelationshipEdgeLabel();
            AtlasEdge edge = null;
            AtlasStructDef.AtlasAttributeDef attrDef = attribute.getAttributeDef();
            AtlasType attrType = attribute.getAttributeType();

            switch (attrType.getTypeCategory()) {
                case OBJECT_ID_TYPE: {
                    if (attrDef.getIsOptional()) {
                        edge = graphHelper.getEdgeForLabel(outVertex, edgeLabel);
                    }
                }
                break;
                case ARRAY: {
                    //If its array attribute, find the right edge between the two vertices and update array property
                    List<AtlasEdge> elementEdges = getCollectionElementsUsingRelationship(outVertex, attribute);
                    if (elementEdges != null) {
                        elementEdges = new ArrayList<>(elementEdges);

                        for (AtlasEdge elementEdge : elementEdges) {
                            if (elementEdge == null) {
                                continue;
                            }

                            AtlasVertex elementVertex = elementEdge.getInVertex();
                            if (elementVertex.equals(inVertex)) {
                                edge = elementEdge;
                            }
                        }
                    }
                }
                break;
                case MAP: {
                    List<AtlasEdge> mapEdges = getMapValuesUsingRelationship(outVertex, attribute);
                    if (mapEdges != null) {
                        mapEdges = new ArrayList<>(mapEdges);
                        for (AtlasEdge mapEdge : mapEdges) {
                            if (mapEdge != null) {
                                AtlasVertex mapVertex = mapEdge.getInVertex();
                                if (mapVertex.getId().toString().equals(inVertex.getId().toString())) {
                                    edge = mapEdge;
                                }
                            }
                        }
                    }
                }
                break;
                case STRUCT:
                case CLASSIFICATION:
                    break;
                default:
                    throw new IllegalStateException("There can't be an edge from " + getVertexDetails(outVertex) + " to " + getVertexDetails(inVertex) + " with attribute name " + attribute.getName() + " which is not class/array/map attribute. found " + attrType.getTypeCategory().name());
            }

            if (edge != null) {
                restoreEdge(edge);

                final RequestContext requestContext = RequestContext.get();
                final String outId = getGuid(outVertex);

                if (!requestContext.isUpdatedEntity(outId)) {
                    AtlasGraphUtilsV2.setEncodedProperty(outVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, requestContext.getRequestTime());
                    AtlasGraphUtilsV2.setEncodedProperty(outVertex, MODIFIED_BY_KEY, requestContext.getUser());

                    requestContext.recordEntityUpdate(entityRetriever.toAtlasEntityHeader(outVertex));
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }


    private Collection<VertexInfo> getOwnedVertices(AtlasVertex entityVertex) throws AtlasBaseException {
        final Map<String, VertexInfo> vertexInfoMap = new HashMap<>();
        final Stack<AtlasVertex> vertices = new Stack<>();

        vertices.push(entityVertex);

        while (vertices.size() > 0) {
            AtlasVertex vertex = vertices.pop();
            AtlasEntity.Status state = AtlasGraphUtilsV2.getState(vertex);

            if (state != DELETED) {
                continue;
            }

            String guid = getGuid(vertex);

            if (vertexInfoMap.containsKey(guid)) {
                continue;
            }

            AtlasEntityHeader entity = entityRetriever.toAtlasEntityHeader(vertex);
            String typeName = entity.getTypeName();
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, ENTITY.name(), typeName);
            }

            vertexInfoMap.put(guid, new VertexInfo(entity, vertex));

            for (AtlasStructType.AtlasAttribute attributeInfo : entityType.getOwnedRefAttributes()) {
                String edgeLabel = attributeInfo.getRelationshipEdgeLabel();
                AtlasType attrType = attributeInfo.getAttributeType();
                TypeCategory typeCategory = attrType.getTypeCategory();

                if (typeCategory == OBJECT_ID_TYPE) {
                    if (attributeInfo.getAttributeDef().isSoftReferenced()) {
                        String softRefVal = vertex.getProperty(attributeInfo.getVertexPropertyName(), String.class);
                        AtlasObjectId refObjId = AtlasEntityUtil.parseSoftRefValue(softRefVal);
                        AtlasVertex refVertex = refObjId != null ? AtlasGraphUtilsV2.findByGuid(this.graphHelper.getGraph(), refObjId.getGuid()) : null;

                        if (refVertex != null) {
                            vertices.push(refVertex);
                        }
                    } else {
                        AtlasEdge edge = graphHelper.getEdgeForLabel(vertex, edgeLabel);

                        if (edge == null || (AtlasGraphUtilsV2.getState(edge) != DELETED)) {
                            continue;
                        }

                        vertices.push(edge.getInVertex());
                    }
                } else if (typeCategory == ARRAY || typeCategory == MAP) {
                    TypeCategory elementType = null;

                    if (typeCategory == ARRAY) {
                        elementType = ((AtlasArrayType) attrType).getElementType().getTypeCategory();
                    } else if (typeCategory == MAP) {
                        elementType = ((AtlasMapType) attrType).getValueType().getTypeCategory();
                    }

                    if (elementType != OBJECT_ID_TYPE) {
                        continue;
                    }

                    if (attributeInfo.getAttributeDef().isSoftReferenced()) {
                        if (typeCategory == ARRAY) {
                            List softRefVal = vertex.getListProperty(attributeInfo.getVertexPropertyName(), List.class);
                            List<AtlasObjectId> refObjIds = AtlasEntityUtil.parseSoftRefValue(softRefVal);

                            if (CollectionUtils.isNotEmpty(refObjIds)) {
                                for (AtlasObjectId refObjId : refObjIds) {
                                    AtlasVertex refVertex = AtlasGraphUtilsV2.findByGuid(this.graphHelper.getGraph(), refObjId.getGuid());

                                    if (refVertex != null) {
                                        vertices.push(refVertex);
                                    }
                                }
                            }
                        } else if (typeCategory == MAP) {
                            Map softRefVal = vertex.getProperty(attributeInfo.getVertexPropertyName(), Map.class);
                            Map<String, AtlasObjectId> refObjIds = AtlasEntityUtil.parseSoftRefValue(softRefVal);

                            if (MapUtils.isNotEmpty(refObjIds)) {
                                for (AtlasObjectId refObjId : refObjIds.values()) {
                                    AtlasVertex refVertex = AtlasGraphUtilsV2.findByGuid(this.graphHelper.getGraph(), refObjId.getGuid());

                                    if (refVertex != null) {
                                        vertices.push(refVertex);
                                    }
                                }
                            }
                        }

                    } else {
                        List<AtlasEdge> edges = getCollectionElementsUsingRelationship(vertex, attributeInfo);

                        if (CollectionUtils.isNotEmpty(edges)) {
                            for (AtlasEdge edge : edges) {
                                if (edge == null || (AtlasGraphUtilsV2.getState(edge) != DELETED)) {
                                    continue;
                                }

                                vertices.push(edge.getInVertex());
                            }
                        }
                    }
                }
            }
        }
        return vertexInfoMap.values();
    }

    private boolean skipVertexForRestore(AtlasVertex vertex) {
        boolean ret = true;

        if (vertex != null) {
            try {
                final RequestContext reqContext = RequestContext.get();
                final String guid = AtlasGraphUtilsV2.getIdFromVertex(vertex);
                if (guid != null && !reqContext.isRestoredEntity(guid)) {
                    final AtlasEntity.Status vertexState = AtlasGraphUtilsV2.getState(vertex);
                    ret = vertexState != DELETED;
                }
            } catch (IllegalStateException excp) {
                LOG.warn("skipVertexForRestore(): failed guid/state for the vertex", excp);
            }
        }

        return ret;
    }

    private void restoreEdgeReference(AtlasVertex outVertex, String edgeLabel, TypeCategory typeCategory, boolean isOwned) throws AtlasBaseException {
        AtlasEdge edge = graphHelper.getEdgeForLabel(outVertex, edgeLabel);

        if (edge != null) {
            restoreEdgeReference(edge, typeCategory, isOwned, outVertex);
        }
    }

    private void restoreEdgeReference(AtlasEdge edge, TypeCategory typeCategory, boolean isOwned, AtlasVertex vertex) throws AtlasBaseException {

        restoreEdgeReference(edge, typeCategory, isOwned, OUT, vertex);
    }


    private void restoreRelationships(Collection<AtlasEdge> edges) throws AtlasBaseException {
        for (AtlasEdge edge : edges) {
            boolean needToSkip = (AtlasGraphUtilsV2.getState(edge) == ACTIVE);

            if (needToSkip) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Skipping restoring of edge={} as it is already active", getIdFromEdge(edge));
                }
                continue;
            }

            restoreEdge(edge);
        }
    }

    private void restoreRelationship(AtlasEdge edge) throws AtlasBaseException {
        restoreRelationships(Collections.singleton(edge));
    }

    private AtlasStructType.AtlasAttribute getAttributeForEdge(AtlasEdge edge) throws AtlasBaseException {
        String labelWithoutPrefix        = edge.getLabel().substring(EDGE_LABEL_PREFIX.length());
        AtlasType       parentType       = typeRegistry.getType(AtlasGraphUtilsV2.getTypeName(edge.getOutVertex()));
        AtlasStructType parentStructType = (AtlasStructType) parentType;
        AtlasStructType.AtlasAttribute attribute = parentStructType.getAttribute(labelWithoutPrefix);
        if (attribute == null) {
            String[] tokenizedLabel = labelWithoutPrefix.split("\\.");
            if (tokenizedLabel.length == 2) {
                String attributeName = tokenizedLabel[1];
                attribute = parentStructType.getAttribute(attributeName);
            }
        }
        return attribute;
    }

    protected void restoreVertex(AtlasVertex instanceVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("RestoreHandlerV1.restoreVertex");
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Setting the external references to {} to null(removing edges)", string(instanceVertex));
            }
            Iterable<AtlasEdge> incomingEdges;

            // The restoration logic now unconditionally excludes PROCESS_EDGE_LABELS (process inputs/outputs),
            // removing any previous conditional behavior based on isSkipProcessEdgeRestoration().
            // This means process edges will never be restored in this method, while non-process edges
            // (like glossary term edges and other relationship edges) will be restored.
            // Note: The configuration flag ATLAS_RELATIONSHIP_SKIP_PROCESS_EDGE_RESTORATION is defined
            // but not actually used to control this behavior.
           incomingEdges = instanceVertex.getInEdges(PROCESS_EDGE_LABELS);

            for (AtlasEdge edge : incomingEdges) {
                AtlasEntity.Status edgeStatus = getStatus(edge);
                boolean isProceed = edgeStatus == DELETED;

                if (isProceed) {
                    if (isRelationshipEdge(edge)) {
                        restoreRelationship(edge);
                    } else {
                        AtlasVertex outVertex = edge.getOutVertex();
                        AtlasVertex inVertex = edge.getInVertex();
                        AtlasStructType.AtlasAttribute attribute = getAttributeForEdge(edge);

                        restoreEdgeBetweenVertices(outVertex, inVertex, attribute);
                    }
                }
            }

            _restoreVertex(instanceVertex);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void _restoreVertex(AtlasVertex instanceVertex) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RestoreHandlerV1._restoreVertex({})", string(instanceVertex));
        }

        AtlasEntity.Status state = AtlasGraphUtilsV2.getState(instanceVertex);

        if (state == DELETED) {
            AtlasGraphUtilsV2.setEncodedProperty(instanceVertex, STATE_PROPERTY_KEY, ACTIVE.name());
            AtlasGraphUtilsV2.setEncodedProperty(instanceVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
            AtlasGraphUtilsV2.setEncodedProperty(instanceVertex, MODIFIED_BY_KEY, RequestContext.get().getUser());

        }
    }

    private void restoreTypeVertex(AtlasVertex instanceVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("RestoreHandlerV1.restoreTypeVertex");

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Restoring {}", string(instanceVertex));
            }

            String typeName = getTypeName(instanceVertex);
            AtlasType parentType = typeRegistry.getType(typeName);

            if (parentType instanceof AtlasStructType) {
                AtlasStructType structType = (AtlasStructType) parentType;
                boolean isEntityType = (parentType instanceof AtlasEntityType);

                for (AtlasStructType.AtlasAttribute attributeInfo : structType.getAllAttributes().values()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Deleting attribute {} for {}", attributeInfo.getName(), string(instanceVertex));
                    }

                    boolean isOwned = isEntityType && attributeInfo.isOwnedRef();
                    AtlasType attrType = attributeInfo.getAttributeType();
                    String edgeLabel = attributeInfo.getRelationshipEdgeLabel();

                    switch (attrType.getTypeCategory()) {
                        case OBJECT_ID_TYPE:
                            //If its class attribute, restore the reference
                            restoreEdgeReference(instanceVertex, edgeLabel, attrType.getTypeCategory(), isOwned);
                            break;

                        case STRUCT:
                            //If its struct attribute, restore the reference
                            restoreEdgeReference(instanceVertex, edgeLabel, attrType.getTypeCategory(), false);
                            break;

                        case ARRAY:
                            //For array attribute, if the element is struct/class, restore all the references
                            AtlasArrayType arrType = (AtlasArrayType) attrType;
                            AtlasType elemType = arrType.getElementType();

                            if (isReference(elemType.getTypeCategory())) {
                                List<AtlasEdge> edges = getCollectionElementsUsingRelationship(instanceVertex, attributeInfo);

                                if (CollectionUtils.isNotEmpty(edges)) {
                                    for (AtlasEdge edge : edges) {
                                        restoreEdgeReference(edge, elemType.getTypeCategory(), isOwned, instanceVertex);
                                    }
                                }
                            }
                            break;

                        case MAP:
                            //For map attribute, if the value type is struct/class, restore all the references
                            AtlasMapType mapType = (AtlasMapType) attrType;
                            TypeCategory valueTypeCategory = mapType.getValueType().getTypeCategory();

                            if (isReference(valueTypeCategory)) {
                                List<AtlasEdge> edges = getMapValuesUsingRelationship(instanceVertex, attributeInfo);

                                for (AtlasEdge edge : edges) {
                                    restoreEdgeReference(edge, valueTypeCategory, isOwned, instanceVertex);
                                }
                            }
                            break;

                        case PRIMITIVE:
//                        if (attributeInfo.getVertexUniquePropertyName() != null) {
//                            Object property = instanceVertex.getProperty(
//                                    attributeInfo.getVertexPropertyName(),
//                                    Object.class
//                            );
//                            instanceVertex.setProperty(attributeInfo.getVertexUniquePropertyName(), property);
//                        }
                            break;
                    }
                }
            }
            restoreVertex(instanceVertex);
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void restoreClassificationVertex(AtlasVertex classificationVertex) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("RestoreHandlerV1.restoreClassificationVertex");

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Restoring classification vertex", string(classificationVertex));
            }

            // restore classification vertex only if it has no more entity references (direct or propagated)
            if (!hasEntityReferences(classificationVertex)) {
                _restoreVertex(classificationVertex);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    private void restoreTypeVertex(AtlasVertex instanceVertex, TypeCategory typeCategory) throws AtlasBaseException {
        switch (typeCategory) {
            case STRUCT:
                restoreTypeVertex(instanceVertex);
                break;

            case CLASSIFICATION:
                restoreClassificationVertex(instanceVertex);
                break;

            case ENTITY:
            case OBJECT_ID_TYPE:
                restoreEntities(Collections.singletonList(instanceVertex));
                break;

            default:
                throw new IllegalStateException("Type category " + typeCategory + " not handled");
        }
    }

    private void restoreEdgeReference(AtlasEdge edge, TypeCategory typeCategory, boolean isOwned,
                                      AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection relationshipDirection, AtlasVertex entityVertex) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Restoring {}", string(edge));
        }

        if (typeCategory == STRUCT || typeCategory == CLASSIFICATION || (typeCategory == OBJECT_ID_TYPE && isOwned)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Processing restore for typeCategory={}, isOwned={}", typeCategory, isOwned);
            }
            //If the vertex is of type struct and classification, restore the edge and then the reference vertex.
            //If the vertex is of type class, and its composite attribute, this reference vertex' lifecycle is controlled
            //through this restore, hence restore the edge and the reference vertex.
            AtlasVertex vertexForRestore = edge.getInVertex();

            restoreEdge(edge);
            restoreTypeVertex(vertexForRestore, typeCategory);
        } else {
            //If the vertex is of type class, and its not a composite attributes, the reference AtlasVertex' lifecycle is not controlled
            //through this restore. Hence just restore the reference edge. Leave the reference AtlasVertex as is

            if (GraphHelper.isRelationshipEdge(edge)) {
                restoreEdge(edge);

                AtlasVertex referencedVertex = entityRetriever.getReferencedEntityVertex(edge, relationshipDirection, entityVertex);

                if (referencedVertex != null) {
                    RequestContext requestContext = RequestContext.get();

                    if (!requestContext.isUpdatedEntity(getGuid(referencedVertex))) {
                        AtlasGraphUtilsV2.setEncodedProperty(referencedVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, requestContext.getRequestTime());
                        AtlasGraphUtilsV2.setEncodedProperty(referencedVertex, MODIFIED_BY_KEY, requestContext.getUser());

                        requestContext.recordEntityUpdate(entityRetriever.toAtlasEntityHeader(referencedVertex));
                    }
                }
            } else {
                //legacy case - not a relationship edge
                //If restoring just the edge, reverse attribute should be updated for any references
                //For example, for the department type system, if the person's manager edge is restored, subordinates of manager should be updated
                // TODO - restoreEdge(edge) with its inverse refs
            }
        }
    }

    private void restoreAllClassifications(AtlasVertex instanceVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("RestoreHandlerV1.restoreAllClassifications");
        try
        {
            List<AtlasEdge> classificationEdges = getAllClassificationEdges(instanceVertex);
            for (AtlasEdge edge : classificationEdges) {
                restoreEdgeReference(edge, CLASSIFICATION, false, instanceVertex);
            }
        } finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public boolean isRelationshipEdge(AtlasEdge edge) {
        boolean ret = false;

        if (edge != null) {
            String outVertexType = getTypeName(edge.getOutVertex());
            String inVertexType  = getTypeName(edge.getInVertex());

            ret = GraphHelper.isRelationshipEdge(edge) || edge.getPropertyKeys().contains(RELATIONSHIP_GUID_PROPERTY_KEY) ||
                    (typeRegistry.getEntityTypeByName(outVertexType) != null && typeRegistry.getEntityTypeByName(inVertexType) != null);
        }

        return ret;
    }
}