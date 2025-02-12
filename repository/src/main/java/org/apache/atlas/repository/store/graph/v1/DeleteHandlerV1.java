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
package org.apache.atlas.repository.store.graph.v1;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.DeleteType;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.tasks.TaskSearchResult;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.AtlasRelationshipStoreV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.store.graph.v2.tasks.ClassificationTask;
import org.apache.atlas.repository.store.graph.v2.tasks.TaskUtil;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.*;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.atlas.AtlasClient.DATA_SET_SUPER_TYPE;
import static org.apache.atlas.AtlasClient.PROCESS_SUPER_TYPE;
import static org.apache.atlas.model.TypeCategory.*;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.model.instance.AtlasEntity.Status.PURGED;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.ONE_TO_TWO;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.*;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_PROPAGATION_ADD;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_PROPAGATION_DELETE;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_REFRESH_PROPAGATION;
import static org.apache.atlas.type.AtlasStructType.AtlasAttribute.AtlasRelationshipEdgeDirection.OUT;
import static org.apache.atlas.type.Constants.HAS_LINEAGE;
import static org.apache.atlas.type.Constants.PENDING_TASKS_PROPERTY_KEY;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.getState;

public abstract class DeleteHandlerV1 {
    public static final Logger  LOG = LoggerFactory.getLogger(DeleteHandlerV1.class);

    static final boolean        DEFERRED_ACTION_ENABLED        = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();
    static final     int        PENDING_TASK_QUERY_SIZE_LIMIT  = 20;

    protected final GraphHelper          graphHelper;
    private   final AtlasTypeRegistry    typeRegistry;
    protected   final EntityGraphRetriever entityRetriever;
    private   final boolean              shouldUpdateInverseReferences;
    private   final boolean              softDelete;
    private   final TaskManagement       taskManagement;
    private   final AtlasGraph           graph;
    private   final TaskUtil             taskUtil;

    public DeleteHandlerV1(AtlasGraph graph, AtlasTypeRegistry typeRegistry, boolean shouldUpdateInverseReference, boolean softDelete, TaskManagement taskManagement) {
        this.typeRegistry                  = typeRegistry;
        this.graphHelper                   = new GraphHelper(graph);
        this.entityRetriever               = new EntityGraphRetriever(graph, typeRegistry);
        this.shouldUpdateInverseReferences = shouldUpdateInverseReference;
        this.softDelete                    = softDelete;
        this.taskManagement                = taskManagement;
        this.graph                         = graph;
        this.taskUtil                      = new TaskUtil(graph);
    }

    /**
     * Deletes the specified entity vertices.
     * Deletes any traits, composite entities, and structs owned by each entity.
     * Also deletes all the references from/to the entity.
     *
     * @param instanceVertices
     * @throws AtlasException
     */
    public void deleteEntities(Collection<AtlasVertex> instanceVertices) throws AtlasBaseException {
        final RequestContext   requestContext            = RequestContext.get();
        final Set<AtlasVertex> deletionCandidateVertices = new HashSet<>();

        for (AtlasVertex instanceVertex : instanceVertices) {
            final String             guid  = AtlasGraphUtilsV2.getIdFromVertex(instanceVertex);

            if (skipVertexForDelete(instanceVertex)) {
                if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping deletion of entity={} as it is already deleted", guid);
                }
                continue;
            }

            // Record all deletion candidate entities in RequestContext
            // and gather deletion candidate vertices.
            for (GraphHelper.VertexInfo vertexInfo : getOwnedVertices(instanceVertex)) {
                AtlasEntityHeader entityHeader = vertexInfo.getEntity();

                if (requestContext.isPurgeRequested()) {
                    entityHeader.setClassifications(entityRetriever.getAllClassifications(vertexInfo.getVertex()));
                }

                requestContext.recordEntityDelete(entityHeader);
                deletionCandidateVertices.add(vertexInfo.getVertex());
            }
        }

        // Delete traits and vertices.
        for (AtlasVertex deletionCandidateVertex : deletionCandidateVertices) {
            RequestContext.get().getDeletedEdgesIds().clear();

            deleteAllClassifications(deletionCandidateVertex);
            deleteTypeVertex(deletionCandidateVertex, isInternalType(deletionCandidateVertex));

            if (DEFERRED_ACTION_ENABLED) {
                Set<String> deletedEdgeIds = RequestContext.get().getDeletedEdgesIds();
                for (String deletedEdgeId : deletedEdgeIds) {
                    AtlasEdge edge = graph.getEdge(deletedEdgeId);
                    createAndQueueClassificationRefreshPropagationTask(edge);
                }
            }
        }
    }

    /**
     * Delete the specified relationship edge.
     *
     * @param edge
     * @throws AtlasBaseException
     */
    public void deleteRelationship(AtlasEdge edge) throws AtlasBaseException {
        deleteRelationships(Collections.singleton(edge), false);
    }

    /**
     * Deletes the specified relationship edges.
     *
     * @param edges
     * @param forceDelete
     * @throws AtlasBaseException
     */
    public void deleteRelationships(Collection<AtlasEdge> edges, final boolean forceDelete) throws AtlasBaseException {
        final boolean isPurgeRequested = RequestContext.get().isPurgeRequested();

        for (AtlasEdge edge : edges) {
            boolean isInternal = isInternalType(edge.getInVertex()) && isInternalType(edge.getOutVertex());
            boolean needToSkip = !isInternal && (!isPurgeRequested && DELETED.equals(getState(edge)));

            if (needToSkip) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Skipping deletion of edge={} as it is already deleted", getIdFromEdge(edge));
                }
                continue;
            }
            deleteEdge(edge, isInternal || forceDelete || isCustomRelationship(edge));
        }
    }

    /**
     * Get the GUIDs and vertices for all composite entities owned/contained by the specified root entity AtlasVertex.
     * The graph is traversed from the root entity through to the leaf nodes of the containment graph.
     *
     * @param entityVertex the root entity vertex
     * @return set of VertexInfo for all composite entities
     * @throws AtlasException
     */
    public Collection<GraphHelper.VertexInfo> getOwnedVertices(AtlasVertex entityVertex) throws AtlasBaseException {
        final Map<String, GraphHelper.VertexInfo> vertexInfoMap    = new HashMap<>();
        final Stack<AtlasVertex>                  vertices         = new Stack<>();
        final boolean                             isPurgeRequested = RequestContext.get().isPurgeRequested();

        vertices.push(entityVertex);

        while (vertices.size() > 0) {
            AtlasVertex        vertex = vertices.pop();
            AtlasEntity.Status state  = getState(vertex);

            //If the vertex marked for deletion, if we are not purging, skip it
            if (!isPurgeRequested && DELETED.equals(state)) {
                continue;
            }

            String guid = GraphHelper.getGuid(vertex);

            if (vertexInfoMap.containsKey(guid)) {
                continue;
            }

            String typeName = GraphHelper.getTypeName(vertex);
            AtlasEntityType   entityType = typeRegistry.getEntityTypeByName(typeName);

            Set<String> attributes = entityType.getAllAttributes().values().stream()
                    .filter(x -> x.getAttributeDef().getIncludeInNotification())
                    .map(x -> x.getAttributeDef().getName()).collect(Collectors.toSet());

            AtlasEntityHeader entity     = entityRetriever.toAtlasEntityHeader(vertex, attributes);

            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), typeName);
            }

            vertexInfoMap.put(guid, new GraphHelper.VertexInfo(entity, vertex));

            for (AtlasStructType.AtlasAttribute attributeInfo : entityType.getOwnedRefAttributes()) {
                String       edgeLabel    = attributeInfo.getRelationshipEdgeLabel();
                AtlasType    attrType     = attributeInfo.getAttributeType();
                TypeCategory typeCategory = attrType.getTypeCategory();

                if (typeCategory == OBJECT_ID_TYPE) {
                    if (attributeInfo.getAttributeDef().isSoftReferenced()) {
                        String        softRefVal = vertex.getProperty(attributeInfo.getVertexPropertyName(), String.class);
                        AtlasObjectId refObjId   = AtlasEntityUtil.parseSoftRefValue(softRefVal);
                        AtlasVertex   refVertex  = refObjId != null ? AtlasGraphUtilsV2.findByGuid(this.graphHelper.getGraph(), refObjId.getGuid()) : null;
                        if (refObjId.getGuid() == null) {
                            LOG.warn("OBJECT_ID_TYPE type category - null guid passed in findByGuid!");
                        }
                        if (refVertex != null) {
                            vertices.push(refVertex);
                        }
                    } else {
                        AtlasEdge edge = graphHelper.getEdgeForLabel(vertex, edgeLabel);

                        if (edge == null || (!isPurgeRequested && DELETED.equals(getState(edge)))) {
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
                            List                softRefVal = vertex.getListProperty(attributeInfo.getVertexPropertyName(), List.class);
                            List<AtlasObjectId> refObjIds  = AtlasEntityUtil.parseSoftRefValue(softRefVal);

                            if (CollectionUtils.isNotEmpty(refObjIds)) {
                                for (AtlasObjectId refObjId : refObjIds) {
                                    AtlasVertex refVertex = AtlasGraphUtilsV2.findByGuid(this.graphHelper.getGraph(), refObjId.getGuid());
                                    if (refObjId.getGuid() == null) {
                                        LOG.warn("ARRAY type category - null guid passed in findByGuid!");
                                    }
                                    if (refVertex != null) {
                                        vertices.push(refVertex);
                                    }
                                }
                            }
                        } else if (typeCategory == MAP) {
                            Map                        softRefVal = vertex.getProperty(attributeInfo.getVertexPropertyName(), Map.class);
                            Map<String, AtlasObjectId> refObjIds  = AtlasEntityUtil.parseSoftRefValue(softRefVal);

                            if (MapUtils.isNotEmpty(refObjIds)) {
                                for (AtlasObjectId refObjId : refObjIds.values()) {
                                    AtlasVertex refVertex = AtlasGraphUtilsV2.findByGuid(this.graphHelper.getGraph(), refObjId.getGuid());
                                    if (refObjId.getGuid() == null) {
                                        LOG.warn("MAP type category - null guid passed in findByGuid!");
                                    }

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
                                if (edge == null || (!isPurgeRequested && DELETED.equals(getState(edge)))) {
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

    /**
     * Force delete is used to remove struct/trait in case of entity updates
     * @param edge
     * @param typeCategory
     * @param isOwned
     * @param forceDeleteStructTrait
     * @return returns true if the edge reference is hard deleted
     * @throws AtlasException
     */
    public boolean deleteEdgeReference(AtlasEdge edge, TypeCategory typeCategory, boolean isOwned,
                                       boolean forceDeleteStructTrait, AtlasVertex vertex) throws AtlasBaseException {
        // default edge direction is outward
        return deleteEdgeReference(edge, typeCategory, isOwned, forceDeleteStructTrait, OUT, vertex);
    }

    public boolean deleteEdgeReference(AtlasEdge edge, TypeCategory typeCategory, boolean isOwned, boolean forceDeleteStructTrait,
                                       AtlasRelationshipEdgeDirection relationshipDirection, AtlasVertex entityVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deleteEdgeReference");
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Deleting {}, force = {}", string(edge), forceDeleteStructTrait);
            }

            boolean isInternalType = isInternalType(entityVertex);
            boolean forceDelete    = (typeCategory == STRUCT || typeCategory == CLASSIFICATION) && (forceDeleteStructTrait || isInternalType);

            if (LOG.isDebugEnabled()) {
                LOG.debug("isInternal = {}, forceDelete = {}", isInternalType, forceDelete);
            }

            if (typeCategory == STRUCT || typeCategory == CLASSIFICATION || (typeCategory == OBJECT_ID_TYPE && isOwned)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Processing delete for typeCategory={}, isOwned={}", typeCategory, isOwned);
                }
                //If the vertex is of type struct delete the edge and then the reference vertex as the vertex is not shared by any other entities.
                //If the vertex is of type classification, delete the edge and then the reference vertex only if the vertex is not shared by any other propagated entities.
                //If the vertex is of type class, and its composite attribute, this reference vertex' lifecycle is controlled
                //through this delete, hence delete the edge and the reference vertex.
                AtlasVertex vertexForDelete = edge.getInVertex();

                //If deleting the edge and then the in vertex, reverse attribute shouldn't be updated
                deleteEdge(edge, false, forceDelete);
                try {
                    deleteTypeVertex(vertexForDelete, typeCategory, forceDelete);
                }
                catch (IllegalStateException | AtlasBaseException e){
                    e.printStackTrace();
                }
            } else {
                //If the vertex is of type class, and its not a composite attributes, the reference AtlasVertex' lifecycle is not controlled
                //through this delete. Hence just remove the reference edge. Leave the reference AtlasVertex as is

                // for relationship edges, inverse vertex's relationship attribute doesn't need to be updated.
                // only delete the reference relationship edge
                if (GraphHelper.isRelationshipEdge(edge)) {
                    deleteEdge(edge, isInternalType || isCustomRelationship(edge) || isHardDeleteProductRelationship(edge));

                    AtlasVertex referencedVertex = entityRetriever.getReferencedEntityVertex(edge, relationshipDirection, entityVertex);

                    if (referencedVertex != null) {
                        RequestContext requestContext = RequestContext.get();

                        if (!requestContext.isUpdatedEntity(GraphHelper.getGuid(referencedVertex))) {
                            AtlasGraphUtilsV2.setEncodedProperty(referencedVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, requestContext.getRequestTime());
                            AtlasGraphUtilsV2.setEncodedProperty(referencedVertex, MODIFIED_BY_KEY, requestContext.getUser());

                            requestContext.recordEntityUpdate(entityRetriever.toAtlasEntityHeader(referencedVertex));
                        }
                    }
                } else {
                    //legacy case - not a relationship edge
                    //If deleting just the edge, reverse attribute should be updated for any references
                    //For example, for the department type system, if the person's manager edge is deleted, subordinates of manager should be updated
                    deleteEdge(edge, true, isInternalType || isCustomRelationship(edge) || isHardDeleteProductRelationship(edge));
                }
            }

            return !softDelete || forceDelete || isCustomRelationship(edge);
        }
        finally {
            RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    public void addTagPropagation(AtlasEdge edge, PropagateTags propagateTags) throws AtlasBaseException {
        if (edge == null) {
            return;
        }

        AtlasVertex outVertex = edge.getOutVertex();
        AtlasVertex inVertex  = edge.getInVertex();

        if (propagateTags == ONE_TO_TWO || propagateTags == PropagateTags.BOTH) {
            addTagPropagation(outVertex, inVertex, edge);
        }

        if (propagateTags == PropagateTags.TWO_TO_ONE || propagateTags == PropagateTags.BOTH) {
            addTagPropagation(inVertex, outVertex, edge);
        }
    }

    private void addTagPropagation(AtlasVertex fromVertex, AtlasVertex toVertex, AtlasEdge edge) throws AtlasBaseException {
        final List<AtlasVertex> classificationVertices = getPropagationEnabledClassificationVertices(fromVertex);
        String                  relationshipGuid       = getRelationshipGuid(edge);

        if (taskManagement != null && DEFERRED_ACTION_ENABLED) {
            for (AtlasVertex classificationVertex : classificationVertices) {
                createAndQueueTask(CLASSIFICATION_PROPAGATION_ADD, toVertex, classificationVertex.getIdForDisplay(), getTypeName(classificationVertex), relationshipGuid);
            }
        } else {
            final List<AtlasVertex> propagatedEntityVertices = CollectionUtils.isNotEmpty(classificationVertices) ? entityRetriever.getIncludedImpactedVerticesV2(toVertex, relationshipGuid) : null;

            if (CollectionUtils.isNotEmpty(propagatedEntityVertices)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Propagate {} tags: from {} entity to {} entities", classificationVertices.size(), getTypeName(fromVertex), propagatedEntityVertices.size());
                }

                for (AtlasVertex classificationVertex : classificationVertices) {
                    addTagPropagation(classificationVertex, propagatedEntityVertices);
                }
            }
        }
    }

    public List<AtlasVertex> addTagPropagation(AtlasVertex classificationVertex, List<AtlasVertex> propagatedEntityVertices) throws AtlasBaseException {
        List<AtlasVertex> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(propagatedEntityVertices) && classificationVertex != null) {
            String                  classificationName     = getTypeName(classificationVertex);
            AtlasClassificationType classificationType     = typeRegistry.getClassificationTypeByName(classificationName);
            AtlasVertex             associatedEntityVertex = getAssociatedEntityVertex(classificationVertex);

            for (AtlasVertex propagatedEntityVertex : propagatedEntityVertices) {
                if (getClassificationEdge(propagatedEntityVertex, classificationVertex) != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(" --> Classification edge already exists from [{}] --> [{}][{}] using edge label: [{}]",
                                getTypeName(propagatedEntityVertex), getTypeName(classificationVertex), getTypeName(associatedEntityVertex), classificationName);
                    }

                    continue;
                }
                if (getPropagatedClassificationEdge(propagatedEntityVertex, classificationVertex) != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(" --> Propagated classification edge already exists from [{}] --> [{}][{}] using edge label: [{}]",
                                getTypeName(propagatedEntityVertex), getTypeName(classificationVertex), getTypeName(associatedEntityVertex), CLASSIFICATION_LABEL);
                    }

                    continue;
                }
                AtlasPerfMetrics.MetricRecorder countMetricRecorder = RequestContext.get().startMetricRecord("countPropagations");

                String          entityTypeName = getTypeName(propagatedEntityVertex);
                AtlasEntityType entityType     = typeRegistry.getEntityTypeByName(entityTypeName);
                String          entityGuid     = getGuid(propagatedEntityVertex);

                if (!classificationType.canApplyToEntityType(entityType)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(" --> Not creating propagated classification edge from [{}] --> [{}][{}], classification is not applicable for entity type",
                                getTypeName(propagatedEntityVertex), getTypeName(classificationVertex), getTypeName(associatedEntityVertex));
                    }

                    continue;
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug(" --> Adding propagated classification: [{}] to {} ({}) using edge label: [{}]", classificationName, getTypeName(propagatedEntityVertex),
                            GraphHelper.getGuid(propagatedEntityVertex), CLASSIFICATION_LABEL);
                }

                ret.add(propagatedEntityVertex);

                graphHelper.addClassificationEdge(propagatedEntityVertex, classificationVertex, true);

                addToPropagatedClassificationNames(propagatedEntityVertex, classificationName);

                // record add propagation details to send notifications at the end
                RequestContext      context        = RequestContext.get();
                AtlasClassification classification = entityRetriever.toAtlasClassification(classificationVertex);

                context.recordAddedPropagation(entityGuid, classification);
                RequestContext.get().endMetricRecord(countMetricRecorder);
            }
        }
        return ret;
    }

    public void authorizeRemoveRelation(AtlasEdge edge) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("authoriseRemoveRelation");
        if(!RequestContext.get().isAuthorisedRemoveRelation()) {
            if (isRequestFromWorkFlow()) {
                RequestContext.get().setAuthorisedRemoveRelation(true);
            }
            AtlasEntityHeader end1Entity, end2Entity;
            String relationShipType = getTypeName(edge);
            AtlasRelationshipDef relationshipDef = typeRegistry.getRelationshipDefByName(relationShipType);
            if (relationshipDef == null) {
                return;
            }

            end1Entity = entityRetriever.toAtlasEntityHeaderWithClassifications(edge.getOutVertex());
            end2Entity = entityRetriever.toAtlasEntityHeaderWithClassifications(edge.getInVertex());

            AtlasAuthorizationUtils.verifyAccess(new AtlasRelationshipAccessRequest(typeRegistry, AtlasPrivilege.RELATIONSHIP_REMOVE, relationShipType, end1Entity, end2Entity));
        }
        RequestContext.get().endMetricRecord(metric);
    }


    public Map<AtlasVertex, List<AtlasVertex>> removeTagPropagation(AtlasEdge edge) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("removeTagPropagationEdge");

        Map<AtlasVertex, List<AtlasVertex>> removePropagationsMap = getRemovePropagationMap(edge);

        if (removePropagationsMap == null) {
            return null;
        }

        boolean isTermEntityEdge = isTermEntityEdge(edge);

        for (AtlasVertex classificationVertex : removePropagationsMap.keySet()) {
            boolean removePropagations = getRemovePropagations(classificationVertex);

            if (isTermEntityEdge || removePropagations) {
                removeTagPropagation(classificationVertex, removePropagationsMap.get(classificationVertex));
            }
        }

        RequestContext.get().endMetricRecord(metric);
        return removePropagationsMap;
    }

    public Map<AtlasVertex, List<AtlasVertex>> getRemovePropagationMap(AtlasEdge edge) throws AtlasBaseException{

        if (edge == null || !isRelationshipEdge(edge)) {
            return null;
        }

        List<AtlasVertex> currentClassificationVertices = getPropagatableClassifications(edge);

        Map<AtlasVertex, List<AtlasVertex>> classificationMapWithEdge     = entityRetriever.getClassificationPropagatedEntitiesMapping(currentClassificationVertices);
        Map<AtlasVertex, List<AtlasVertex>> classificationMapWithOutEdge  = entityRetriever.getClassificationPropagatedEntitiesMapping(currentClassificationVertices, getRelationshipGuid(edge));
        Map<AtlasVertex, List<AtlasVertex>> removePropagationsMap         = new HashMap<>();

        if (MapUtils.isNotEmpty(classificationMapWithEdge) && MapUtils.isEmpty(classificationMapWithOutEdge)) {
            removePropagationsMap.putAll(classificationMapWithEdge);
        } else {
            for (AtlasVertex classificationVertex : classificationMapWithOutEdge.keySet()) {

                List<AtlasVertex> currentPropagatingEntities = classificationMapWithEdge.getOrDefault(classificationVertex, Collections.emptyList());
                List<AtlasVertex> updatedPropagatingEntities = classificationMapWithOutEdge.getOrDefault(classificationVertex, Collections.emptyList());
                List<AtlasVertex> entitiesRemoved            = (List<AtlasVertex>) CollectionUtils.subtract(currentPropagatingEntities, updatedPropagatingEntities);

                if (CollectionUtils.isNotEmpty(entitiesRemoved)) {
                    removePropagationsMap.put(classificationVertex, entitiesRemoved);
                }
            }
        }
        return  removePropagationsMap;
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

    public List<AtlasVertex> removeTagPropagation(AtlasClassification classification, List<AtlasEdge> propagatedEdges) throws AtlasBaseException {
        List<AtlasVertex> ret = new ArrayList<>();

        for (AtlasEdge propagatedEdge : propagatedEdges) {
            AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("removeTagPropagationEdges");
            AtlasVertex entityVertex = propagatedEdge.getOutVertex();

            ret.add(entityVertex);

            // record remove propagation details to send notifications inline
            RequestContext.get().recordRemovedPropagation(getGuid(entityVertex), classification);

            deletePropagatedEdge(propagatedEdge);

            RequestContext.get().endMetricRecord(metric);
        }

        return ret;
    }

    public List<AtlasVertex> removeTagPropagation(AtlasVertex classificationVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("removeTagPropagationVertex");
        List<AtlasVertex> ret = new ArrayList<>();

        if (classificationVertex != null) {
            List<AtlasEdge> propagatedEdges = getPropagatedEdges(classificationVertex);

            if (CollectionUtils.isNotEmpty(propagatedEdges)) {
                AtlasClassification classification = entityRetriever.toAtlasClassification(classificationVertex);

                for (AtlasEdge propagatedEdge : propagatedEdges) {
                    AtlasVertex entityVertex = propagatedEdge.getOutVertex();

                    ret.add(entityVertex);

                    // record remove propagation details to send notifications at the end
                    RequestContext.get().recordRemovedPropagation(getGuid(entityVertex), classification);

                    deletePropagatedEdge(propagatedEdge);
                }
            }
        }
        RequestContext.get().endMetricRecord(metric);
        return ret;
    }

    public List<AtlasVertex> removeTagPropagation(AtlasVertex classificationVertex, List<AtlasVertex> entityVertices) throws AtlasBaseException {
        List<AtlasVertex> ret = new ArrayList<>();
        if (classificationVertex != null && CollectionUtils.isNotEmpty(entityVertices)) {
            AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("removeTagPropagationVertices");
            String              classificationName = getClassificationName(classificationVertex);
            AtlasClassification classification     = entityRetriever.toAtlasClassification(classificationVertex);
            String              entityGuid         = getClassificationEntityGuid(classificationVertex);
            RequestContext      context            = RequestContext.get();

            for (AtlasVertex entityVertex : entityVertices) {
                if(!entityVertex.exists()) {
                    continue;
                }
                AtlasEdge propagatedEdge = getPropagatedClassificationEdge(entityVertex, classificationName, entityGuid);

                if (propagatedEdge != null) {
                    deletePropagatedEdge(propagatedEdge);

                    ret.add(entityVertex);

                    // record remove propagation details to send notifications at the end
                    context.recordRemovedPropagation(getGuid(entityVertex), classification);
                }
            }
            RequestContext.get().endMetricRecord(metric);
        }
        return ret;
    }

    public void deletePropagatedClassification(AtlasVertex entityVertex, String classificationName, String associatedEntityGuid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deletePropagatedClassification");
        AtlasEdge propagatedEdge = getPropagatedClassificationEdge(entityVertex, classificationName, associatedEntityGuid);

        if (propagatedEdge == null) {
            throw new AtlasBaseException(AtlasErrorCode.PROPAGATED_CLASSIFICATION_NOT_ASSOCIATED_WITH_ENTITY, classificationName, associatedEntityGuid, getGuid(entityVertex));
        }

        AtlasVertex classificationVertex = propagatedEdge.getInVertex();

        // do not remove propagated classification with ACTIVE associated entity
        if (getClassificationEntityStatus(classificationVertex) == ACTIVE) {
            throw new AtlasBaseException(AtlasErrorCode.PROPAGATED_CLASSIFICATION_REMOVAL_NOT_SUPPORTED, classificationName, associatedEntityGuid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing propagated classification: [{} - associatedEntityGuid: {}] from: [{}][{}] with edge label: [{}]",
                    classificationName, associatedEntityGuid, getTypeName(entityVertex), getGuid(entityVertex), CLASSIFICATION_LABEL);
        }

        AtlasClassification classification = entityRetriever.toAtlasClassification(classificationVertex);

        // delete classification edge
        deletePropagatedEdge(propagatedEdge);

        // delete classification vertex
        deleteClassificationVertex(classificationVertex, true);

        // record remove propagation details to send notifications at the end
        RequestContext.get().recordRemovedPropagation(getGuid(entityVertex), classification);

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    public void deletePropagatedEdge(AtlasEdge edge) throws AtlasBaseException {
        String      classificationName = AtlasGraphUtilsV2.getEncodedProperty(edge, CLASSIFICATION_EDGE_NAME_PROPERTY_KEY, String.class);
        AtlasVertex entityVertex       = edge.getOutVertex();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing propagated classification: [{}] from: [{}][{}] with edge label: [{}]", classificationName,
                    getTypeName(entityVertex), GraphHelper.getGuid(entityVertex), CLASSIFICATION_LABEL);
        }

        removeFromPropagatedClassificationNames(entityVertex, classificationName);

        deleteEdge(edge, true);

        updateModificationMetadata(entityVertex);
    }

    public void deleteEdgeReference(AtlasVertex outVertex, String edgeLabel, TypeCategory typeCategory, boolean isOwned) throws AtlasBaseException {
        AtlasEdge edge = graphHelper.getEdgeForLabel(outVertex, edgeLabel);

        if (edge != null) {
            deleteEdgeReference(edge, typeCategory, isOwned, false, outVertex);
        }
    }

    protected void deleteEdge(AtlasEdge edge, boolean updateInverseAttribute, boolean force) throws AtlasBaseException {
        //update inverse attribute
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("deleteEdge");

        try {
            if (updateInverseAttribute) {
                String labelWithoutPrefix = edge.getLabel().substring(GraphHelper.EDGE_LABEL_PREFIX.length());
                AtlasType      parentType = typeRegistry.getType(AtlasGraphUtilsV2.getTypeName(edge.getOutVertex()));

                if (parentType instanceof AtlasEntityType) {
                    AtlasEntityType                parentEntityType = (AtlasEntityType) parentType;
                    AtlasStructType.AtlasAttribute attribute        = parentEntityType.getAttribute(labelWithoutPrefix);

                    if (attribute == null) {
                        attribute = parentEntityType.getRelationshipAttribute(labelWithoutPrefix, AtlasGraphUtilsV2.getTypeName(edge));
                    }

                    if (attribute != null && attribute.getInverseRefAttribute() != null) {
                        deleteEdgeBetweenVertices(edge.getInVertex(), edge.getOutVertex(), attribute.getInverseRefAttribute());
                    }
                }
            }

            if (isClassificationEdge(edge)) {
                AtlasVertex classificationVertex = edge.getInVertex();

                AtlasGraphUtilsV2.setEncodedProperty(classificationVertex, CLASSIFICATION_ENTITY_STATUS,
                        RequestContext.get().getDeleteType() == DeleteType.HARD ? PURGED.name() : DELETED.name());
            }

            deleteEdge(edge, force);
        } finally {
        RequestContext.get().endMetricRecord(metricRecorder);
        }
    }

    protected void deleteTypeVertex(AtlasVertex instanceVertex, TypeCategory typeCategory, boolean force) throws AtlasBaseException {
        switch (typeCategory) {
            case STRUCT:
                deleteTypeVertex(instanceVertex, force);
                break;

            case CLASSIFICATION:
                deleteClassificationVertex(instanceVertex, force);
                break;

            case ENTITY:
            case OBJECT_ID_TYPE:
                deleteEntities(Collections.singletonList(instanceVertex));
                break;

            default:
                throw new IllegalStateException("Type category " + typeCategory + " not handled");
        }
    }

    /**
     * Deleting any type vertex. Goes over the complex attributes and removes the references
     * @param instanceVertex
     * @throws AtlasException
     */
    protected void deleteTypeVertex(AtlasVertex instanceVertex, boolean force) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting {}, force={}", string(instanceVertex), force);
        }

        String    typeName   = GraphHelper.getTypeName(instanceVertex);

        if (StringUtils.isNotEmpty(typeName)) {
            AtlasType parentType = typeRegistry.getType(typeName);

            if (parentType instanceof AtlasStructType) {
                AtlasStructType structType   = (AtlasStructType) parentType;
                boolean         isEntityType = (parentType instanceof AtlasEntityType);

                for (AtlasStructType.AtlasAttribute attributeInfo : structType.getAllAttributes().values()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Deleting attribute {} for {}", attributeInfo.getName(), string(instanceVertex));
                    }

                    boolean   isOwned   = isEntityType && attributeInfo.isOwnedRef();
                    AtlasType attrType  = attributeInfo.getAttributeType();
                    String    edgeLabel = attributeInfo.getRelationshipEdgeLabel();

                    switch (attrType.getTypeCategory()) {
                        case OBJECT_ID_TYPE:
                            //If its class attribute, delete the reference
                            deleteEdgeReference(instanceVertex, edgeLabel, attrType.getTypeCategory(), isOwned);
                            break;

                        case STRUCT:
                            //If its struct attribute, delete the reference
                            deleteEdgeReference(instanceVertex, edgeLabel, attrType.getTypeCategory(), false);
                            break;

                        case ARRAY:
                            //For array attribute, if the element is struct/class, delete all the references
                            AtlasArrayType arrType  = (AtlasArrayType) attrType;
                            AtlasType      elemType = arrType.getElementType();

                            if (isReference(elemType.getTypeCategory())) {
                                List<AtlasEdge> edges = getActiveCollectionElementsUsingRelationship(instanceVertex, attributeInfo);

                                if (CollectionUtils.isNotEmpty(edges)) {
                                    for (AtlasEdge edge : edges) {
                                        deleteEdgeReference(edge, elemType.getTypeCategory(), isOwned, false, instanceVertex);
                                    }
                                }
                            }
                            break;

                        case MAP:
                            //For map attribute, if the value type is struct/class, delete all the references
                            AtlasMapType mapType           = (AtlasMapType) attrType;
                            TypeCategory valueTypeCategory = mapType.getValueType().getTypeCategory();

                            if (isReference(valueTypeCategory)) {
                                List<AtlasEdge> edges = getMapValuesUsingRelationship(instanceVertex, attributeInfo);

                                for (AtlasEdge edge : edges) {
                                    deleteEdgeReference(edge, valueTypeCategory, isOwned, false, instanceVertex);
                                }
                            }
                            break;

                        case PRIMITIVE:
                            // This is different from upstream atlas.
                            // Here we are not deleting the unique property thus users can only restore after deleting an entity.
                            if (attributeInfo.getVertexUniquePropertyName() != null && force) {
                                instanceVertex.removeProperty(attributeInfo.getVertexUniquePropertyName());
                            }
                            break;
                    }
                }
            }
        } else {
            try {
                LOG.error("typeName not found for the vertex {}", instanceVertex.getIdForDisplay());
            } catch (Exception e) {
                LOG.error("Error while writing error log");
                e.printStackTrace();
            }
        }

        deleteVertex(instanceVertex, force);
    }

    protected AtlasAttribute getAttributeForEdge(AtlasEdge edge) throws AtlasBaseException {
        String labelWithoutPrefix        = edge.getLabel().substring(GraphHelper.EDGE_LABEL_PREFIX.length());
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

    protected abstract void _deleteVertex(AtlasVertex instanceVertex, boolean force);

    protected abstract void deleteEdge(AtlasEdge edge, boolean force) throws AtlasBaseException;

    /**
     * Deletes the edge between outvertex and inVertex. The edge is for attribute attributeName of outVertex
     * @param outVertex
     * @param inVertex
     * @param attribute
     * @throws AtlasException
     */
    protected void deleteEdgeBetweenVertices(AtlasVertex outVertex, AtlasVertex inVertex, AtlasAttribute attribute) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing edge from {} to {} with attribute name {}", string(outVertex), string(inVertex), attribute.getName());
        }

        if (skipVertexForDelete(outVertex)) {
            return;
        }

        AtlasStructType   parentType   = (AtlasStructType) typeRegistry.getType(GraphHelper.getTypeName(outVertex));
        String            propertyName = getQualifiedAttributePropertyKey(parentType, attribute.getName());
        String            edgeLabel    = attribute.getRelationshipEdgeLabel();
        AtlasEdge         edge         = null;
        AtlasAttributeDef attrDef      = attribute.getAttributeDef();
        AtlasType         attrType     = attribute.getAttributeType();

        switch (attrType.getTypeCategory()) {
            case OBJECT_ID_TYPE: {
                //If its class attribute, its the only edge between two vertices
                if (attrDef.getIsOptional()) {
                    edge = graphHelper.getEdgeForLabel(outVertex, edgeLabel);

                    if (shouldUpdateInverseReferences) {
                        AtlasGraphUtilsV2.setEncodedProperty(outVertex, propertyName, null);
                    }
                } else {
                    // Cannot unset a required attribute.
                    throw new AtlasBaseException("Cannot unset required attribute " + propertyName + " on " + GraphHelper.vertexString(outVertex) + " edge = " + edgeLabel);
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

                            //TODO element.size includes deleted items as well. should exclude
                            if (!attrDef.getIsOptional() && elementEdges.size() <= attrDef.getValuesMinCount()) {
                                // Deleting this edge would violate the attribute's lower bound.
                                throw new AtlasBaseException("Cannot remove array element from required attribute " + propertyName + " on " + GraphHelper.getVertexDetails(outVertex) + " " + GraphHelper.getEdgeDetails(elementEdge));
                            }
                        }
                    }
                }
            }
            break;

            case MAP: {
                //If its map attribute, find the right edge between two vertices and update map property
                List<AtlasEdge> mapEdges = getMapValuesUsingRelationship(outVertex, attribute);

                if (mapEdges != null) {
                    mapEdges = new ArrayList<>(mapEdges);

                    for (AtlasEdge mapEdge : mapEdges) {
                        if (mapEdge != null) {
                            AtlasVertex mapVertex = mapEdge.getInVertex();

                            if (mapVertex.getId().toString().equals(inVertex.getId().toString())) {
                                //TODO keys.size includes deleted items as well. should exclude
                                if (attrDef.getIsOptional() || mapEdges.size() > attrDef.getValuesMinCount()) {
                                    edge = mapEdge;
                                } else {
                                    // Deleting this entry would violate the attribute's lower bound.
                                    throw new AtlasBaseException("Cannot remove map entry " + propertyName + " from required attribute " + propertyName + " on " + GraphHelper.getVertexDetails(outVertex) + " " + GraphHelper.getEdgeDetails(mapEdge));
                                }
                                break;
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
                throw new IllegalStateException("There can't be an edge from " + GraphHelper.getVertexDetails(outVertex) + " to " + GraphHelper.getVertexDetails(inVertex) + " with attribute name " + attribute.getName() + " which is not class/array/map attribute. found " + attrType.getTypeCategory().name());
        }

        if (edge != null) {
            boolean isInternal = isInternalType(inVertex) && isInternalType(outVertex);
            deleteEdge(edge, isInternal || isCustomRelationship(edge) || isHardDeleteProductRelationship(edge));

            final RequestContext requestContext = RequestContext.get();
            final String         outId          = GraphHelper.getGuid(outVertex);

            if (! requestContext.isUpdatedEntity(outId)) {
                AtlasGraphUtilsV2.setEncodedProperty(outVertex, MODIFICATION_TIMESTAMP_PROPERTY_KEY, requestContext.getRequestTime());
                AtlasGraphUtilsV2.setEncodedProperty(outVertex, MODIFIED_BY_KEY, requestContext.getUser());

                requestContext.recordEntityUpdate(entityRetriever.toAtlasEntityHeader(outVertex));
            }
        }
    }

    protected void deleteVertex(AtlasVertex instanceVertex, boolean force) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting the external references to {} to null(removing edges)", string(instanceVertex));
        }

        // Delete external references to this vertex - incoming edges from lineage or glossary term edges
        final Iterable<AtlasEdge> incomingEdges    = instanceVertex.getEdges(AtlasEdgeDirection.IN);
        final Iterable<AtlasEdge> outgoingEdges    = instanceVertex.getEdges(AtlasEdgeDirection.OUT);
        final boolean             isPurgeRequested = RequestContext.get().isPurgeRequested();

        if (RequestContext.get().getDeleteType().equals(DeleteType.HARD) || RequestContext.get().getDeleteType().equals(DeleteType.PURGE)) {
            for (AtlasEdge edge : outgoingEdges) {
                if (isRelationshipEdge(edge))
                    AtlasRelationshipStoreV2.recordRelationshipMutation(AtlasRelationshipStoreV2.RelationshipMutation.RELATIONSHIP_HARD_DELETE, edge, entityRetriever);
            }
        }

        for (AtlasEdge edge : incomingEdges) {
            AtlasEntity.Status edgeStatus = getStatus(edge);
            boolean            isProceed   = edgeStatus == (isPurgeRequested ? DELETED : ACTIVE);

            if (isProceed) {
                if (isRelationshipEdge(edge)) {
                    deleteRelationship(edge);
                } else {
                    AtlasVertex    outVertex = edge.getOutVertex();

                    if (!isDeletedEntity(outVertex)) {
                        AtlasVertex inVertex = edge.getInVertex();
                        AtlasAttribute attribute = getAttributeForEdge(edge);

                        deleteEdgeBetweenVertices(outVertex, inVertex, attribute);
                    }
                }
            }
        }

        _deleteVertex(instanceVertex, force);
    }

    private boolean isDeletedEntity(AtlasVertex entityVertex) {
        boolean            ret      = false;
        String             outGuid  = GraphHelper.getGuid(entityVertex);
        AtlasEntity.Status outState = GraphHelper.getStatus(entityVertex);

        //If the reference vertex is marked for deletion, skip updating the reference
        if (outState == AtlasEntity.Status.DELETED || (outGuid != null && RequestContext.get().isDeletedEntity(outGuid))) {
            ret = true;
        }

        return ret;
    }

    public void deleteClassificationVertex(AtlasVertex classificationVertex, boolean force) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting classification vertex", string(classificationVertex));
        }

        // delete classification vertex only if it has no more entity references (direct or propagated)
        if (!hasEntityReferences(classificationVertex)) {
            _deleteVertex(classificationVertex, force);
        }
    }

    private boolean isInternalType(final AtlasVertex instanceVertex) {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(GraphHelper.getTypeName(instanceVertex));
        return Objects.nonNull(entityType) && entityType.isInternalType();
    }

    private boolean isCustomRelationship(final AtlasEdge edge) {
        return edge.getLabel().equals(UD_RELATIONSHIP_EDGE_LABEL);
    }

    private boolean isHardDeleteProductRelationship(final AtlasEdge edge) {
        return EDGE_LABELS_FOR_HARD_DELETION.contains(edge.getLabel());
    }

    private void addToPropagatedClassificationNames(AtlasVertex entityVertex, String classificationName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding property {} = \"{}\" to vertex {}", PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, classificationName, string(entityVertex));
        }
        entityVertex.addListProperty(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, classificationName);

        entityVertex.setProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, getDelimitedPropagatedClassificationNames(entityVertex, classificationName));
    }

    public void removeFromPropagatedClassificationNames(AtlasVertex entityVertex, String classificationName) {
        if (entityVertex != null && StringUtils.isNotEmpty(classificationName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Removing from property: {} value: {} in vertex: {}", PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, classificationName, string(entityVertex));
            }

            entityVertex.removePropertyValue(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY, classificationName);

            List<String> propagatedTraitNames = getPropagatedTraitNames(entityVertex);

            if (CollectionUtils.isNotEmpty(propagatedTraitNames)) {
                propagatedTraitNames.remove(classificationName);

                String propClsName = CLASSIFICATION_NAME_DELIMITER + StringUtils.join(propagatedTraitNames, CLASSIFICATION_NAME_DELIMITER) + CLASSIFICATION_NAME_DELIMITER;

                entityVertex.setProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, propClsName);
            }
        }
    }

    private String getDelimitedPropagatedClassificationNames(AtlasVertex entityVertex, String classificationName) {
        String ret = entityVertex.getProperty(PROPAGATED_CLASSIFICATION_NAMES_KEY, String.class);

        if (StringUtils.isEmpty(ret)) {
            ret = CLASSIFICATION_NAME_DELIMITER + classificationName + CLASSIFICATION_NAME_DELIMITER;
        } else {
            ret = ret + classificationName + CLASSIFICATION_NAME_DELIMITER;
        }

        return ret;
    }

    /**
     * Delete all associated classifications from the specified entity vertex.
     * @param instanceVertex
     * @throws AtlasException
     */
    private void deleteAllClassifications(AtlasVertex instanceVertex) throws AtlasBaseException {
        // If instance is deleted no need to operate classification deleted
        if (!ACTIVE.equals(getState(instanceVertex)))
            return;

        List<AtlasEdge> classificationEdges = getAllClassificationEdges(instanceVertex);

        for (AtlasEdge edge : classificationEdges) {
            AtlasVertex classificationVertex = edge.getInVertex();
            boolean     isClassificationEdge = isClassificationEdge(edge);
            boolean     removePropagations   = getRemovePropagations(classificationVertex);

            if (isClassificationEdge && removePropagations) {
                if (taskManagement != null && DEFERRED_ACTION_ENABLED) {
                    createAndQueueTask(CLASSIFICATION_PROPAGATION_DELETE, instanceVertex, classificationVertex.getIdForDisplay(),getTypeName(classificationVertex) , null);
                } else {
                    removeTagPropagation(classificationVertex);
                }
            }

            deleteEdgeReference(edge, CLASSIFICATION, false, false, instanceVertex);
        }
    }

    private boolean skipVertexForDelete(AtlasVertex vertex) {
        boolean ret = true;

        if(vertex != null) {
            try {
                final RequestContext reqContext = RequestContext.get();
                final String guid = AtlasGraphUtilsV2.getIdFromVertex(vertex);

                if(guid != null && !reqContext.isDeletedEntity(guid)) {
                    final AtlasEntity.Status vertexState = getState(vertex);
                    if (reqContext.isPurgeRequested()) {
                        ret = false; // Delete all ACTIVE or DELETED assets in PURGING
                    } else {
                        ret = vertexState == DELETED; // skip deleting DELETED vertices
                    }
                }
            } catch (IllegalStateException excp) {
                LOG.warn("skipVertexForDelete(): failed guid/state for the vertex", excp);
            }
        }

        return ret;
    }

    public void updateTagPropagations(AtlasEdge edge, AtlasRelationship relationship) throws AtlasBaseException {
        PropagateTags oldTagPropagation = getPropagateTags(edge);
        PropagateTags newTagPropagation = relationship.getPropagateTags();

        if (newTagPropagation != oldTagPropagation) {
            List<AtlasVertex>                   currentClassificationVertices = getPropagatableClassifications(edge);
            Map<AtlasVertex, List<AtlasVertex>> currentClassificationsMap     = entityRetriever.getClassificationPropagatedEntitiesMapping(currentClassificationVertices);

            // Update propagation edge
            AtlasGraphUtilsV2.setEncodedProperty(edge, RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, newTagPropagation.name());

            List<AtlasVertex>                   updatedClassificationVertices = getPropagatableClassifications(edge);
            List<AtlasVertex>                   classificationVerticesUnion   = (List<AtlasVertex>) CollectionUtils.union(currentClassificationVertices, updatedClassificationVertices);
            Map<AtlasVertex, List<AtlasVertex>> updatedClassificationsMap     = entityRetriever.getClassificationPropagatedEntitiesMapping(classificationVerticesUnion);

            // compute add/remove propagations list
            Map<AtlasVertex, List<AtlasVertex>> addPropagationsMap    = new HashMap<>();
            Map<AtlasVertex, List<AtlasVertex>> removePropagationsMap = new HashMap<>();

            if (MapUtils.isEmpty(currentClassificationsMap) && MapUtils.isNotEmpty(updatedClassificationsMap)) {
                addPropagationsMap.putAll(updatedClassificationsMap);

            } else if (MapUtils.isNotEmpty(currentClassificationsMap) && MapUtils.isEmpty(updatedClassificationsMap)) {
                removePropagationsMap.putAll(currentClassificationsMap);

            } else {
                for (AtlasVertex classificationVertex : updatedClassificationsMap.keySet()) {
                    List<AtlasVertex> currentPropagatingEntities = currentClassificationsMap.containsKey(classificationVertex) ? currentClassificationsMap.get(classificationVertex) : Collections.emptyList();
                    List<AtlasVertex> updatedPropagatingEntities = updatedClassificationsMap.containsKey(classificationVertex) ? updatedClassificationsMap.get(classificationVertex) : Collections.emptyList();
                    List<AtlasVertex> entitiesAdded              = (List<AtlasVertex>) CollectionUtils.subtract(updatedPropagatingEntities, currentPropagatingEntities);
                    List<AtlasVertex> entitiesRemoved            = (List<AtlasVertex>) CollectionUtils.subtract(currentPropagatingEntities, updatedPropagatingEntities);

                    if (CollectionUtils.isNotEmpty(entitiesAdded)) {
                        addPropagationsMap.put(classificationVertex, entitiesAdded);
                    }

                    if (CollectionUtils.isNotEmpty(entitiesRemoved)) {
                        removePropagationsMap.put(classificationVertex, entitiesRemoved);
                    }
                }
            }

            for (AtlasVertex classificationVertex : addPropagationsMap.keySet()) {
                List<AtlasVertex> entitiesToAddPropagation = addPropagationsMap.get(classificationVertex);

                addTagPropagation(classificationVertex, entitiesToAddPropagation);
            }

            for (AtlasVertex classificationVertex : removePropagationsMap.keySet()) {
                List<AtlasVertex> entitiesToRemovePropagation = removePropagationsMap.get(classificationVertex);

                removeTagPropagation(classificationVertex, entitiesToRemovePropagation);
            }
        } else {
            // update blocked propagated classifications only if there is no change is tag propagation (don't update both)
            handleBlockedClassifications(edge, relationship.getBlockedPropagatedClassifications());
        }
    }

    public void handleBlockedClassifications(AtlasEdge edge, Set<AtlasClassification> blockedClassifications) throws AtlasBaseException {
        if (blockedClassifications != null) {
            List<AtlasVertex> propagatableClassifications  = getPropagatableClassifications(edge);
            List<String>      currBlockedClassificationIds = getBlockedClassificationIds(edge);
            List<AtlasVertex> currBlockedClassifications   = getVerticesForIds(propagatableClassifications, currBlockedClassificationIds);
            List<AtlasVertex> classificationsToBlock       = new ArrayList<>();
            List<String>      classificationIdsToBlock     = new ArrayList<>();

            for (AtlasClassification blockedClassification : blockedClassifications) {
                AtlasVertex classificationVertex = validateBlockedPropagatedClassification(propagatableClassifications, blockedClassification);

                if (classificationVertex != null) {
                    classificationsToBlock.add(classificationVertex);
                    classificationIdsToBlock.add(classificationVertex.getIdForDisplay());
                }
            }

            setBlockedClassificationIds(edge, classificationIdsToBlock);

            List<AtlasVertex> propagationChangedClassifications = (List<AtlasVertex>) CollectionUtils.disjunction(classificationsToBlock, currBlockedClassifications);

            for (AtlasVertex classificationVertex : propagationChangedClassifications) {
                List<AtlasVertex> propagationsToRemove = new ArrayList<>();
                List<AtlasVertex> propagationsToAdd    = new ArrayList<>();

                entityRetriever.evaluateClassificationPropagation(classificationVertex, propagationsToAdd, propagationsToRemove);

                if (CollectionUtils.isNotEmpty(propagationsToAdd)) {
                    addTagPropagation(classificationVertex, propagationsToAdd);
                }

                if (CollectionUtils.isNotEmpty(propagationsToRemove)) {
                    removeTagPropagation(classificationVertex, propagationsToRemove);
                }
            }
        }
    }

    private List<AtlasVertex> getVerticesForIds(List<AtlasVertex> vertices, List<String> vertexIds) {
        List<AtlasVertex> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(vertexIds)) {
            for (AtlasVertex vertex : vertices) {
                String vertexId = vertex.getIdForDisplay();

                if (vertexIds.contains(vertexId)) {
                    ret.add(vertex);
                }
            }
        }

        return ret;
    }

    // propagated classifications should contain blocked propagated classification
    private AtlasVertex validateBlockedPropagatedClassification(List<AtlasVertex> classificationVertices, AtlasClassification classification) {
        AtlasVertex ret = null;

        for (AtlasVertex vertex : classificationVertices) {
            String classificationName = getClassificationName(vertex);
            String entityGuid         = getClassificationEntityGuid(vertex);

            if (classificationName.equals(classification.getTypeName()) && entityGuid.equals(classification.getEntityGuid())) {
                ret = vertex;
                break;
            }
        }

        return ret;
    }

    private void setBlockedClassificationIds(AtlasEdge edge, List<String> classificationIds) {
        if (edge != null) {
            if (classificationIds.isEmpty()) {
                edge.removeProperty(org.apache.atlas.repository.Constants.RELATIONSHIPTYPE_BLOCKED_PROPAGATED_CLASSIFICATIONS_KEY);
            } else {
                edge.setListProperty(org.apache.atlas.repository.Constants.RELATIONSHIPTYPE_BLOCKED_PROPAGATED_CLASSIFICATIONS_KEY, classificationIds);
            }
        }
    }
    public void createAndQueueTaskWithoutCheck(String taskType, AtlasVertex entityVertex, String classificationVertexId, String classificationTypeName, String relationshipGuid) throws AtlasBaseException {
        String              currentUser = RequestContext.getCurrentUser();
        String              entityGuid  = GraphHelper.getGuid(entityVertex);
        Map<String, Object> taskParams  = ClassificationTask.toParameters(entityGuid, classificationVertexId, relationshipGuid);
        AtlasTask           task        = taskManagement.createTask(taskType, currentUser, taskParams, classificationVertexId, classificationTypeName, entityGuid);

        AtlasGraphUtilsV2.addEncodedProperty(entityVertex, PENDING_TASKS_PROPERTY_KEY, task.getGuid());

        RequestContext.get().queueTask(task);
    }

    public void createAndQueueTask(String taskType, AtlasVertex entityVertex, String classificationVertexId, String classificationTypeName, String relationshipGuid) throws AtlasBaseException {
        if (!CLASSIFICATION_PROPAGATION_DELETE.equals(taskType) && skipClassificationTaskCreation(classificationVertexId)) {
            LOG.info("Task is already scheduled for classification id {}, no need to schedule task for vertex {}", classificationVertexId, entityVertex.getIdForDisplay());
            return;
        }

        createAndQueueTaskWithoutCheck(taskType, entityVertex, classificationVertexId, classificationTypeName, relationshipGuid);
    }

    public void createAndQueueTaskWithoutCheck(String taskType, AtlasVertex entityVertex, String classificationVertexId,String classificationTypeName, String relationshipGuid, Boolean currentRestrictPropagationThroughLineage,Boolean currentRestrictPropogationThroughHierarchy) throws AtlasBaseException {
        String              currentUser = RequestContext.getCurrentUser();
        String              entityGuid  = GraphHelper.getGuid(entityVertex);
        Map<String, Object> taskParams  = ClassificationTask.toParameters(entityGuid, classificationVertexId, relationshipGuid, currentRestrictPropagationThroughLineage,currentRestrictPropogationThroughHierarchy);
        AtlasTask           task        = taskManagement.createTask(taskType, currentUser, taskParams, classificationVertexId, classificationTypeName, entityGuid);

        AtlasGraphUtilsV2.addEncodedProperty(entityVertex, PENDING_TASKS_PROPERTY_KEY, task.getGuid());

        RequestContext.get().queueTask(task);
    }

    public void createAndQueueTask(String taskType, AtlasEdge relationshipEdge, AtlasRelationship relationship) {
        String              currentUser        = RequestContext.getCurrentUser();
        String              relationshipEdgeId = relationshipEdge.getIdForDisplay();
        Map<String, Object> taskParams         = ClassificationTask.toParameters(relationshipEdgeId, relationship);

        AtlasTask           task               = taskManagement.createTask(taskType, currentUser, taskParams);

        AtlasGraphUtilsV2.addItemToListProperty(relationshipEdge, EDGE_PENDING_TASKS_PROPERTY_KEY, task.getGuid());

        RequestContext.get().queueTask(task);
    }

    public void createAndQueueClassificationRefreshPropagationTask(AtlasEdge edge) throws AtlasBaseException{

        if (taskManagement==null) {
            LOG.warn("Task management is null, can't schedule task now");
            return;
        }

        String      currentUser         = RequestContext.getCurrentUser();
        boolean     isRelationshipEdge  = isRelationshipEdge(edge);
        boolean     isTermEntityEdge    = GraphHelper.isTermEntityEdge(edge);

        if (edge == null || !isRelationshipEdge) {
            LOG.warn("Edge is null or it is not relationship edge, can't schedule task now");
            return;
        }


        AtlasVertex referenceVertex = GraphHelper.getPropagatingVertex(edge);
        if(referenceVertex == null) {
            return;
        }

        List<AtlasVertex> currentClassificationVertices = GraphHelper.getPropagatableClassifications(edge);
        for (AtlasVertex currentClassificationVertex : currentClassificationVertices) {
            String currentClassificationId = currentClassificationVertex.getIdForDisplay();
            String classificationTypeName      = getTypeName(currentClassificationVertex);
            boolean removePropagationOnEntityDelete = GraphHelper.getRemovePropagations(currentClassificationVertex);

            if (!(isTermEntityEdge || removePropagationOnEntityDelete)) {
                LOG.debug("This edge is not term edge or remove propagation isn't enabled");
                continue;
            }

            if(skipClassificationTaskCreation(currentClassificationId)) {
                LOG.info("Task is already scheduled for classification id {}, no need to schedule task for edge {}", currentClassificationId, edge.getIdForDisplay());
                continue;
            }

            Map<String, Object> taskParams = ClassificationTask.toParameters(currentClassificationVertex.getIdForDisplay());
            AtlasTask task  =  taskManagement.createTask(CLASSIFICATION_REFRESH_PROPAGATION, currentUser, taskParams, currentClassificationId, classificationTypeName,GraphHelper.getGuid(referenceVertex));

            RequestContext.get().queueTask(task);
        }

    }

    private boolean skipClassificationTaskCreation(String classificationId) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("skipClassificationTaskCreation");
        /*
        If any of,
        1. CLASSIFICATION_PROPAGATION_DELETE
        2. CLASSIFICATION_REFRESH_PROPAGATION task scheduled already
        skip classification task creation
         */
        try {

            List<String> taskTypes = Arrays.asList(CLASSIFICATION_REFRESH_PROPAGATION, CLASSIFICATION_PROPAGATION_DELETE);
            List<AtlasTask> tasksInRequestContext = RequestContext.get().getQueuedTasks();
            if (
                    tasksInRequestContext != null &&
                    tasksInRequestContext.stream().filter(Objects::nonNull)
                    .anyMatch(task -> task.getClassificationId().equals(classificationId)
                            && taskTypes.contains(task.getType()) && task.getStatus().equals(AtlasTask.Status.PENDING))
            ) {
                return true;
            }

            TaskSearchResult taskSearchResult = taskUtil.findPendingTasksByClassificationId(0, PENDING_TASK_QUERY_SIZE_LIMIT,
                    classificationId, taskTypes , new ArrayList<>());

            List<AtlasTask> pendingTasks = taskSearchResult.getTasks();
            if(CollectionUtils.isEmpty(pendingTasks)) {
                return false;
            }

            List<AtlasTask> pendingRefreshPropagationTasks = pendingTasks.stream()
                    .filter(task -> CLASSIFICATION_REFRESH_PROPAGATION.equals(task.getType()))
                    .collect(Collectors.toList());

            // Ideally there should be only refresh propagation task
            if (pendingRefreshPropagationTasks.size() > 1) {
                LOG.warn("More than one {} task found for classification id {}", CLASSIFICATION_REFRESH_PROPAGATION, classificationId);
            }

            // if any task have status as PENDING, then skip task creation
            if (
                    pendingTasks.stream()
                    .filter(Objects::nonNull)
                    .anyMatch(task -> task.getClassificationId().equals(classificationId)
                            && taskTypes.contains(task.getType()) && task.getStatus().equals(AtlasTask.Status.PENDING))
            ) {
                return true;
            } else {
                LOG.warn("There is inconsistency " +
                        "in task queue, there are no pending tasks for classification id {} but there are tasks in queue", classificationId);
            }
        } catch (AtlasBaseException e) {
            LOG.error("Error while checking if classification task creation is required for classification id {}", classificationId, e);
            throw e;
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }

        return false;
    }


    public void removeHasLineageOnDelete(Collection<AtlasVertex> vertices) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("removeHasLineageOnDelete");

        if (RequestContext.get().skipHasLineageCalculation()) {
            return;
        }

        for (AtlasVertex vertexToBeDeleted : vertices) {
            if (ACTIVE.equals(getStatus(vertexToBeDeleted))) {
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(getTypeName(vertexToBeDeleted));
                boolean isProcess = entityType.getTypeAndAllSuperTypes().contains(PROCESS_SUPER_TYPE);
                boolean isCatalog = entityType.getTypeAndAllSuperTypes().contains(DATA_SET_SUPER_TYPE);

                if (isCatalog || isProcess) {

                    Iterator<AtlasEdge> edgeIterator = vertexToBeDeleted.getEdges(AtlasEdgeDirection.BOTH, PROCESS_EDGE_LABELS).iterator();

                    Set<AtlasEdge> edgesToBeDeleted = new HashSet<>();

                    while (edgeIterator.hasNext()) {
                        AtlasEdge edge = edgeIterator.next();
                        if (ACTIVE.equals(getStatus(edge))) {
                            edgesToBeDeleted.add(edge);
                        }
                    }

                    resetHasLineageOnInputOutputDelete(edgesToBeDeleted, vertexToBeDeleted);
                }
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }


    public void resetHasLineageOnInputOutputDelete(Collection<AtlasEdge> removedEdges, AtlasVertex deletedVertex) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("resetHasLineageOnInputOutputDelete");

        for (AtlasEdge atlasEdge : removedEdges) {

            boolean isOutputEdge = PROCESS_OUTPUTS.equals(atlasEdge.getLabel());

            AtlasVertex assetVertex = atlasEdge.getInVertex();
            String assetEdgeLabel = getLabel(getGuid(assetVertex), atlasEdge.getLabel());

            boolean assetLabelPairAlreadyProcessed = RequestContext.get().isEdgeLabelAlreadyProcessed(assetEdgeLabel);

            if (!assetLabelPairAlreadyProcessed) {
                RequestContext.get().addEdgeLabel(assetEdgeLabel);
                if (getStatus(assetVertex) == ACTIVE && !assetVertex.equals(deletedVertex)) {
                    updateAssetHasLineageStatus(assetVertex, atlasEdge, removedEdges);
                }
            }

            AtlasVertex processVertex = atlasEdge.getOutVertex();
            String processId = getGuid(processVertex);
            String edgeLabel = isOutputEdge ? PROCESS_OUTPUTS : PROCESS_INPUTS;
            String processEdgeLabel = getLabel(processId, edgeLabel);
            boolean processLabelPairAlreadyProcessed = RequestContext.get().isEdgeLabelAlreadyProcessed(processEdgeLabel);

            if (processLabelPairAlreadyProcessed) {
                continue;
            }

            RequestContext.get().addEdgeLabel(processEdgeLabel);

                if (getStatus(processVertex) == ACTIVE && !processVertex.equals(deletedVertex)) {
                Iterator<AtlasEdge> edgeIterator = GraphHelper.getActiveEdges(processVertex, edgeLabel, AtlasEdgeDirection.BOTH);

                boolean activeEdgeFound = false;

                while (edgeIterator.hasNext()) {
                    AtlasEdge edge = edgeIterator.next();
                    if (!removedEdges.contains(edge)) {
                        AtlasVertex relatedAssetVertex = edge.getInVertex();

                        if (getStatus(relatedAssetVertex) == ACTIVE) {
                            activeEdgeFound = true;
                            break;
                        }
                    }
                }

                if (!activeEdgeFound) {
                    AtlasGraphUtilsV2.setEncodedProperty(processVertex, HAS_LINEAGE, false);

                    String oppositeEdgeLabel = isOutputEdge ? PROCESS_INPUTS : PROCESS_OUTPUTS;

                    processEdgeLabel = getLabel(processId, oppositeEdgeLabel);
                    processLabelPairAlreadyProcessed = RequestContext.get().isEdgeLabelAlreadyProcessed(processEdgeLabel);

                    if (processLabelPairAlreadyProcessed) {
                        continue;
                    }
                    RequestContext.get().addEdgeLabel(processEdgeLabel);

                    Iterator<AtlasEdge> processEdgeIterator = GraphHelper.getActiveEdges(processVertex, oppositeEdgeLabel, AtlasEdgeDirection.BOTH);

                    while (processEdgeIterator.hasNext()) {
                        AtlasEdge edge = processEdgeIterator.next();

                        if (!removedEdges.contains(edge)) {
                            AtlasVertex relatedAssetVertex = edge.getInVertex();
                            updateAssetHasLineageStatus(relatedAssetVertex, edge, removedEdges);
                        }
                    }
                }
            }
        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }
    private boolean isRequestFromWorkFlow() {
        String workflowID = RequestContext.get().getRequestContextHeaders().getOrDefault("x-atlan-agent-workflow-id", "");
        boolean isWorkFlowRequest = !workflowID.isEmpty();
        if(isWorkFlowRequest){
            LOG.info("Authorised one time request for workflow with id : {} ", workflowID);
        }
        return isWorkFlowRequest;
    }

    private String getLabel(String guid, String label){
       return  guid + ":" + label;
    }

    private void updateAssetHasLineageStatus(AtlasVertex assetVertex, AtlasEdge currentEdge, Collection<AtlasEdge> removedEdges) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("updateAssetHasLineageStatus");

        removedEdges.forEach(edge -> RequestContext.get().addToDeletedEdgesIdsForResetHasLineage(edge.getIdForDisplay()));

        Iterator<AtlasEdge> edgeIterator = assetVertex.query()
                .direction(AtlasEdgeDirection.BOTH)
                .label(PROCESS_EDGE_LABELS)
                .has(STATE_PROPERTY_KEY, ACTIVE.name())
                .edges()
                .iterator();

        int processHasLineageCount = 0;
        while (edgeIterator.hasNext()) {
            AtlasEdge edge = edgeIterator.next();
            if (!RequestContext.get().getDeletedEdgesIdsForResetHasLineage().contains(edge.getIdForDisplay()) && !currentEdge.equals(edge)) {
                AtlasVertex relatedProcessVertex = edge.getOutVertex();
                boolean processHasLineage = getEntityHasLineage(relatedProcessVertex);
                if (processHasLineage) {
                    processHasLineageCount++;
                    break;
                }
            }
        }

        if (processHasLineageCount == 0) {
            AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE, false);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

}
