
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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasRelationshipAccessRequest;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasRelationship.AtlasRelationshipWithExtInfo;
import org.apache.atlas.model.instance.RelationshipMutationContext;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType;
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.model.typedef.AtlasRelationshipEndDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.JanusUtils;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.AtlasConfiguration.NOTIFICATION_RELATIONSHIPS_ENABLED;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.BOTH;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.NONE;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.ONE_TO_TWO;
import static org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags.TWO_TO_ONE;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.getTypeName;
import static org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2.*;
import static org.apache.atlas.repository.store.graph.v2.tasks.ClassificationPropagateTaskFactory.CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE;

@Component
public class AtlasRelationshipStoreV2 implements AtlasRelationshipStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipStoreV2.class);
    private static final Long DEFAULT_RELATIONSHIP_VERSION = 0L;

    private final AtlasGraph graph;
    private boolean notificationsEnabled    = NOTIFICATION_RELATIONSHIPS_ENABLED.getBoolean();
    private boolean DEFERRED_ACTION_ENABLED = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();

    private final AtlasTypeRegistry         typeRegistry;
    private final EntityGraphRetriever      entityRetriever;
    private final DeleteHandlerDelegate     deleteDelegate;
    private final GraphHelper               graphHelper;
    private final IAtlasEntityChangeNotifier entityChangeNotifier;

    private static final String RELATIONSHIP_DEF_MAP_KEY = "relationshipDef";
    private static final String END_1_CARDINALITY_KEY = "end1Cardinality";
    private static final String END_1_NAME_KEY = "end1Name";
    private static final String IS_END_1_CONTAINER_KEY = "IsEnd1Container";
    private static final String END_2_CARDINALITY_KEY = "end2Cardinality";
    private static final String END_2_NAME_KEY = "end2Name";
    private static final String IS_END_2_CONTAINER_KEY = "IsEnd2Container";

    private static final String END_1_DOC_ID_KEY = "end1DocId";
    private static final String END_2_DOC_ID_KEY = "end2DocId";
    private static final String ES_DOC_ID_MAP_KEY = "esDocIdMap";

    private static final String UD_RELATIONSHIP_TYPE_NAME = "UserDefRelationship";

    private static final List<String> ALLOWED_RELATIONSHIP_TYPES_FOR_ES_FILTERING = Arrays.asList(AtlasConfiguration.SUPPORTED_RELATIONSHIP_EVENTS.getStringArray());

    private static Set<String> EXCLUDE_MUTATION_REL_TYPE_NAMES = new HashSet<String>() {{
        add(REL_DOMAIN_TO_DOMAINS);
        add(REL_DOMAIN_TO_PRODUCTS);
        add(REL_DOMAIN_TO_STAKEHOLDERS);
        add(REL_STAKEHOLDER_TITLE_TO_STAKEHOLDERS);
        add(REL_POLICY_TO_ACCESS_CONTROL);
        add(REL_DATA_PRODUCT_TO_OUTPUT_PORTS);
        add(REL_DATA_PRODUCT_TO_INPUT_PORTS);
    }};

    public enum RelationshipMutation {
        RELATIONSHIP_CREATE,
        RELATIONSHIP_UPDATE,
        RELATIONSHIP_RESTORE,
        RELATIONSHIP_SOFT_DELETE,
        RELATIONSHIP_HARD_DELETE
    }

    @Inject
    public AtlasRelationshipStoreV2(AtlasGraph graph, AtlasTypeRegistry typeRegistry, DeleteHandlerDelegate deleteDelegate,
                                    IAtlasEntityChangeNotifier entityChangeNotifier, EntityGraphRetriever entityRetriever) {
        this.graph                = graph;
        this.typeRegistry         = typeRegistry;
        this.graphHelper          = new GraphHelper(graph);
        this.entityRetriever      = entityRetriever;
        this.deleteDelegate       = deleteDelegate;
        this.entityChangeNotifier = entityChangeNotifier;
    }

    @Override
    @GraphTransaction
    public AtlasRelationship create(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> create({})", relationship);
        }

        validateRelationshipType(relationship.getTypeName());

        AtlasVertex end1Vertex = getVertexFromEndPoint(relationship.getEnd1());
        AtlasVertex end2Vertex = getVertexFromEndPoint(relationship.getEnd2());

        if (relationship.getTypeName().equals(UD_RELATIONSHIP_TYPE_NAME)) {
            EntityGraphMapper.validateCustomRelationship(end1Vertex, end2Vertex);
        }

        AtlasEdge edge = createRelationship(end1Vertex, end2Vertex, relationship);

        AtlasRelationship ret = edge != null ? entityRetriever.mapEdgeToAtlasRelationship(edge) : null;
        recordRelationshipMutation(RelationshipMutation.RELATIONSHIP_CREATE, edge, entityRetriever);
        onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== create({}): {}", relationship, ret);
        }
        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasRelationship update(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> update({})", relationship);
        }

        String guid = relationship.getGuid();

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND, guid);
        }

        AtlasEdge   edge       = graphHelper.getEdgeForGUID(guid);
        String      edgeType   = AtlasGraphUtilsV2.getTypeName(edge);
        AtlasVertex end1Vertex = edge.getOutVertex();
        AtlasVertex end2Vertex = edge.getInVertex();

        validateRelationshipType(edgeType);

        // update shouldn't change endType
        if (StringUtils.isNotEmpty(relationship.getTypeName()) && !StringUtils.equalsIgnoreCase(edgeType, relationship.getTypeName())) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_UPDATE_TYPE_CHANGE_NOT_ALLOWED, guid, edgeType, relationship.getTypeName());
        }

        // update shouldn't change ends
        if (relationship.getEnd1() != null) {
            String updatedEnd1Guid = relationship.getEnd1().getGuid();

            if (updatedEnd1Guid == null) {
                AtlasVertex updatedEnd1Vertex = getVertexFromEndPoint(relationship.getEnd1());

                updatedEnd1Guid = updatedEnd1Vertex == null ? null : AtlasGraphUtilsV2.getIdFromVertex(updatedEnd1Vertex);
            }

            if (updatedEnd1Guid != null) {
                String end1Guid = AtlasGraphUtilsV2.getIdFromVertex(end1Vertex);

                if (!StringUtils.equalsIgnoreCase(relationship.getEnd1().getGuid(), end1Guid)) {
                    throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_UPDATE_END_CHANGE_NOT_ALLOWED, edgeType, guid, end1Guid, relationship.getEnd1().getGuid());
                }
            }
        }

        // update shouldn't change ends
        if (relationship.getEnd2() != null) {
            String updatedEnd2Guid = relationship.getEnd2().getGuid();

            if (updatedEnd2Guid == null) {
                AtlasVertex updatedEnd2Vertex = getVertexFromEndPoint(relationship.getEnd2());

                updatedEnd2Guid = updatedEnd2Vertex == null ? null : AtlasGraphUtilsV2.getIdFromVertex(updatedEnd2Vertex);
            }

            if (updatedEnd2Guid != null) {
                String end2Guid = AtlasGraphUtilsV2.getIdFromVertex(end2Vertex);

                if (!StringUtils.equalsIgnoreCase(relationship.getEnd2().getGuid(), end2Guid)) {
                    throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_UPDATE_END_CHANGE_NOT_ALLOWED, AtlasGraphUtilsV2.getTypeName(edge), guid, end2Guid, relationship.getEnd2().getGuid());
                }
            }
        }

        boolean relationshipTypeNotExists = false;
        if (StringUtils.isEmpty(relationship.getTypeName())) {
            relationship.setTypeName(edgeType);
            relationshipTypeNotExists = true;
        }
        validateRelationship(end1Vertex, end2Vertex, relationship);

        if (relationshipTypeNotExists) {
            relationship.setTypeName(null);
        }

        AtlasRelationship ret = updateRelationship(edge, relationship);
        recordRelationshipMutation(RelationshipMutation.RELATIONSHIP_UPDATE, edge, entityRetriever);
        onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== update({}): {}", relationship, ret);
        }
        return ret;
    }

    @Override
    @GraphTransaction
    public List<AtlasRelationship> createOrUpdate(List<AtlasRelationship> relationships) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createOrUpdate({})", relationships);
        }
        List<AtlasRelationship> ret = new ArrayList<>();

        for (AtlasRelationship relationship : relationships) {
            AtlasVertex end1Vertex = getVertexFromEndPoint(relationship.getEnd1());
            AtlasVertex end2Vertex = getVertexFromEndPoint(relationship.getEnd2());

            if (end1Vertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_END_VERTEX_NOT_FOUND, relationship.getTypeName(),
                        relationship.getEnd1().getGuid(), relationship.getEnd1().getTypeName());
            }

            if (end2Vertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIPDEF_END_VERTEX_NOT_FOUND, relationship.getTypeName(),
                        relationship.getEnd2().getGuid(), relationship.getEnd2().getTypeName());
            }

            String relationshipLabel = getRelationshipEdgeLabel(end1Vertex, end2Vertex, relationship.getTypeName());

            AtlasEdge existingEdge = getRelationshipEdge(end1Vertex, end2Vertex, relationshipLabel);

            if (existingEdge == null) {
                ret.add(create(relationship));
            } else {
                ret.add(update(relationship));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== createOrUpdate({}): {}", relationships, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasRelationship getById(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getById({})", guid);
        }

        AtlasEdge         edge = graphHelper.getEdgeForGUID(guid);
        AtlasRelationship ret  = entityRetriever.mapEdgeToAtlasRelationship(edge);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getById({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasRelationshipWithExtInfo getExtInfoById(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getExtInfoById({})", guid);
        }

        AtlasEdge                    edge = graphHelper.getEdgeForGUID(guid);
        AtlasRelationshipWithExtInfo ret  = entityRetriever.mapEdgeToAtlasRelationshipWithExtInfo(edge);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getExtInfoById({}): {}", guid, ret);
        }

        return ret;
    }


    @Override
    @GraphTransaction
    public void deleteByIds(List<String> guids) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> deleteByIds({}})", guids.size());
        }

        Set<AtlasRelationship> deletedRelationships = new HashSet<>();
        List<AtlasEdge> edgesToDelete = new ArrayList<>();

        for (String guid : guids) {
            AtlasEdge edge = graphHelper.getEdgeForGUID(guid);

            if (edge == null) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND, guid);
            }

            if (getState(edge) == DELETED) {
                throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_ALREADY_DELETED, guid);
            }

            validateRelationshipType(getTypeName(edge));

            edgesToDelete.add(edge);
            AtlasRelationship relationshipToDelete = entityRetriever.mapEdgeToAtlasRelationship(edge);
            deletedRelationships.add(relationshipToDelete);
        }

        deleteDelegate.getHandler().resetHasLineageOnInputOutputDelete(edgesToDelete,null);
        deleteDelegate.getHandler().deleteRelationships(edgesToDelete, false);

        if (DEFERRED_ACTION_ENABLED) {
            Set<String> deletedEdgeIds = RequestContext.get().getDeletedEdgesIds();
            for (String deletedEdgeId : deletedEdgeIds) {
                AtlasEdge deletedEdge = graph.getEdge(deletedEdgeId);
                deleteDelegate.getHandler().createAndQueueClassificationRefreshPropagationTask(deletedEdge);
            }
        }

        onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== deleteByIds({}):", guids.size());
        }
    }

    @Override
    @GraphTransaction
    public void deleteById(String guid) throws AtlasBaseException {
        deleteById(guid, false);
    }

    @Override
    @GraphTransaction
    public void deleteById(String guid, boolean forceDelete) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> deleteById({}, {})", guid, forceDelete);
        }

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_CRUD_INVALID_PARAMS, " empty/null guid");
        }

        AtlasEdge edge = graphHelper.getEdgeForGUID(guid);
        if (edge == null) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND, guid);
        }

        if (getState(edge) == DELETED) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_ALREADY_DELETED, guid);
        }

        validateRelationshipType(getTypeName(edge));

        deleteDelegate.getHandler().resetHasLineageOnInputOutputDelete(Collections.singleton(edge), null);
        deleteDelegate.getHandler().deleteRelationships(Collections.singleton(edge), forceDelete);

        if (DEFERRED_ACTION_ENABLED) {
            Set<String> deletedEdgeIds = RequestContext.get().getDeletedEdgesIds();
            for (String deletedEdgeId : deletedEdgeIds) {
                AtlasEdge deletedEdge = graph.getEdge(deletedEdgeId);
                deleteDelegate.getHandler().createAndQueueClassificationRefreshPropagationTask(deletedEdge);
            }
        }

        onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== deleteById({}): {}", guid);
        }
    }

    @Override
    public AtlasEdge getOrCreate(AtlasVertex end1Vertex, AtlasVertex end2Vertex, AtlasRelationship relationship, boolean skipAuth) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("entityStoreV2.getOrCreate");
        AtlasEdge ret = getRelationship(end1Vertex, end2Vertex, relationship);

        if (ret == null) {
            ret = createRelationship(end1Vertex, end2Vertex, relationship, false, skipAuth);
            recordRelationshipMutation(RelationshipMutation.RELATIONSHIP_CREATE, ret, entityRetriever);
        }

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }

    @Override
    public AtlasEdge getRelationship(AtlasVertex fromVertex, AtlasVertex toVertex, AtlasRelationship relationship) throws AtlasBaseException {
        String relationshipLabel = getRelationshipEdgeLabel(fromVertex, toVertex, relationship.getTypeName());

        return getRelationshipEdge(fromVertex, toVertex, relationshipLabel);
    }

    @Override
    public AtlasRelationship getOrCreate(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getOrCreate({})", relationship);
        }

        AtlasVertex       end1Vertex = getVertexFromEndPoint(relationship.getEnd1());
        AtlasVertex       end2Vertex = getVertexFromEndPoint(relationship.getEnd2());
        AtlasRelationship ret        = null;

        // check if relationship exists
        AtlasEdge relationshipEdge = getRelationship(end1Vertex, end2Vertex, relationship);
        boolean isRelationshipCreated = false;
        if (relationshipEdge == null) {
            relationshipEdge = createRelationship(end1Vertex, end2Vertex, relationship, false, false);
            isRelationshipCreated = true;
        }

        if (relationshipEdge != null){
            ret = entityRetriever.mapEdgeToAtlasRelationship(relationshipEdge);
        }

        if (isRelationshipCreated) {
            recordRelationshipMutation(RelationshipMutation.RELATIONSHIP_CREATE, relationshipEdge, entityRetriever);
            onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getOrCreate({}): {}", relationship, ret);
        }
        return ret;
    }

    private AtlasEdge createRelationship(AtlasVertex end1Vertex, AtlasVertex end2Vertex, AtlasRelationship relationship) throws AtlasBaseException {
        return createRelationship(end1Vertex, end2Vertex, relationship, true, false);
    }

    private AtlasEdge createRelationship(AtlasVertex end1Vertex, AtlasVertex end2Vertex, AtlasRelationship relationship, boolean existingRelationshipCheck, boolean skipAuth) throws AtlasBaseException {
        AtlasEdge ret;
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("createRelationship");

        try {
            validateRelationship(end1Vertex, end2Vertex, relationship);

            String relationshipLabel = getRelationshipEdgeLabel(end1Vertex, end2Vertex, relationship.getTypeName());

            if (existingRelationshipCheck) {
                ret = getRelationshipEdge(end1Vertex, end2Vertex, relationshipLabel);

                if (ret != null) {
                    throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_ALREADY_EXISTS, relationship.getTypeName(),
                                                 AtlasGraphUtilsV2.getIdFromVertex(end1Vertex), AtlasGraphUtilsV2.getIdFromVertex(end2Vertex));
                }
            }

            AtlasRelationshipType relationType = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());


            AtlasEntityHeader end1Entity = entityRetriever.toAtlasEntityHeaderWithClassifications(end1Vertex);
            AtlasEntityHeader end2Entity = entityRetriever.toAtlasEntityHeaderWithClassifications(end2Vertex);

            if(!skipAuth){
                AtlasAuthorizationUtils.verifyAccess(new AtlasRelationshipAccessRequest(typeRegistry, AtlasPrivilege.RELATIONSHIP_ADD,
                        relationship.getTypeName(), end1Entity, end2Entity));
            }


            if (existingRelationshipCheck) {
                ret = graphHelper.getOrCreateEdge(end1Vertex, end2Vertex, relationshipLabel);
            } else {
                ret = graphHelper.addEdge(end1Vertex, end2Vertex, relationshipLabel);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Created relationship edge from [{}] --> [{}] using edge label: [{}]", getTypeName(end1Vertex), getTypeName(end2Vertex), relationshipLabel);
                }
            }

            // map additional properties to relationship edge
            if (ret != null) {
                // Accept a valid (assigned) guid from the supplied relationship, or generate one.
                String        relationshipGuid = relationship.getGuid();
                PropagateTags tagPropagation   = getRelationshipTagPropagation(end1Vertex, end2Vertex, relationship);
                final String  guid             = AtlasTypeUtil.isAssignedGuid(relationshipGuid) ? relationshipGuid : UUID.randomUUID().toString();

                AtlasGraphUtilsV2.setEncodedProperty(ret, ENTITY_TYPE_PROPERTY_KEY, relationship.getTypeName());
                AtlasGraphUtilsV2.setEncodedProperty(ret, RELATIONSHIP_GUID_PROPERTY_KEY, guid);
                AtlasGraphUtilsV2.setEncodedProperty(ret, HOME_ID_KEY, relationship.getHomeId());
                AtlasGraphUtilsV2.setEncodedProperty(ret, VERSION_PROPERTY_KEY, getRelationshipVersion(relationship));
                AtlasGraphUtilsV2.setEncodedProperty(ret, PROVENANCE_TYPE_KEY, relationship.getProvenanceType());
                AtlasGraphUtilsV2.setEncodedProperty(ret, RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, tagPropagation.name());

                // blocked propagated classifications
                deleteDelegate.getHandler().handleBlockedClassifications(ret, relationship.getBlockedPropagatedClassifications());

                // propagate tags
                deleteDelegate.getHandler().addTagPropagation(ret, tagPropagation);
            }

            if (MapUtils.isNotEmpty(relationType.getAllAttributes())) {
                for (AtlasAttribute attr : relationType.getAllAttributes().values()) {
                    String attrName           = attr.getName();
                    String attrVertexProperty = attr.getVertexPropertyName();
                    Object attrValue          = relationship.getAttribute(attrName);

                    AtlasGraphUtilsV2.setEncodedProperty(ret, attrVertexProperty, attrValue);
                }
            }
        } catch (RepositoryException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }

    private AtlasRelationship updateRelationship(AtlasEdge relationshipEdge, AtlasRelationship relationship) throws AtlasBaseException {
        AtlasRelationshipType relationType = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());
        AtlasVertex           end1Vertex   = relationshipEdge.getOutVertex();
        AtlasVertex           end2Vertex   = relationshipEdge.getInVertex();
        AtlasEntityHeader     end1Entity   = entityRetriever.toAtlasEntityHeaderWithClassifications(end1Vertex);
        AtlasEntityHeader     end2Entity   = entityRetriever.toAtlasEntityHeaderWithClassifications(end2Vertex);

        AtlasAuthorizationUtils.verifyAccess(new AtlasRelationshipAccessRequest(typeRegistry, AtlasPrivilege.RELATIONSHIP_UPDATE, relationship.getTypeName(), end1Entity, end2Entity));

        updateTagPropagations(relationshipEdge, relationship);

        if (MapUtils.isNotEmpty(relationType.getAllAttributes())) {
            for (AtlasAttribute attr : relationType.getAllAttributes().values()) {
                String attrName           = attr.getName();
                String attrVertexProperty = attr.getVertexPropertyName();

                if (relationship.hasAttribute(attrName)) {
                    AtlasGraphUtilsV2.setEncodedProperty(relationshipEdge, attrVertexProperty, relationship.getAttribute(attrName));
                } else {
                    String defaultValue = attr.getAttributeDef().getDefaultValue();

                    if (StringUtils.isNotEmpty(defaultValue)) {
                        Object attrValue = attr.getAttributeType().createDefaultValue(defaultValue);

                        if (attrValue != null) {
                            AtlasGraphUtilsV2.setEncodedProperty(relationshipEdge, attrVertexProperty, attrValue);
                        }
                    }
                }
            }
        }

        return entityRetriever.mapEdgeToAtlasRelationship(relationshipEdge);
    }

    private void updateTagPropagations(AtlasEdge relationshipEdge, AtlasRelationship relationship) throws AtlasBaseException {
        if (DEFERRED_ACTION_ENABLED) {
            createAndQueueTask(CLASSIFICATION_PROPAGATION_RELATIONSHIP_UPDATE, relationshipEdge, relationship);
        } else {
            deleteDelegate.getHandler().updateTagPropagations(relationshipEdge, relationship);
        }
    }

    private void validateRelationship(AtlasRelationship relationship) throws AtlasBaseException {
        if (relationship == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "AtlasRelationship is null");
        }

        String                relationshipName = relationship.getTypeName();
        String                end1TypeName     = getTypeNameFromObjectId(relationship.getEnd1());
        String                end2TypeName     = getTypeNameFromObjectId(relationship.getEnd2());
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipName);

        if (relationshipType == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "unknown relationship type'" + relationshipName + "'");
        }

        if (relationship.getEnd1() == null || relationship.getEnd2() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "end1/end2 is null");
        }

        boolean validEndTypes = false;

        if (relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end1TypeName)) {
            validEndTypes = relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end2TypeName);
        } else if (relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end1TypeName)) {
            validEndTypes = relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end2TypeName);
        }

        if (!validEndTypes) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_RELATIONSHIP_END_TYPE, relationshipName, relationshipType.getEnd2Type().getTypeName(), end1TypeName);
        }

        validateEnds(relationship);

        validateAndNormalize(relationship);
    }

    private void validateRelationship(AtlasVertex end1Vertex, AtlasVertex end2Vertex,  AtlasRelationship relationship) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateRelationship");
        String relationshipName = relationship.getTypeName();

        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipName);

        if (relationshipType == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "unknown relationship type'" + relationshipName + "'");
        }

        if (end1Vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_END_IS_NULL, relationshipType.getEnd1Type().getTypeName());
        }

        if (end2Vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_END_IS_NULL, relationshipType.getEnd2Type().getTypeName());
        }

        String                end1TypeName     = AtlasGraphUtilsV2.getTypeName(end1Vertex);
        String                end2TypeName     = AtlasGraphUtilsV2.getTypeName(end2Vertex);

        boolean validEndTypes = false;

        if (relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end1TypeName)) {
            validEndTypes = relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end2TypeName);
        } else if (relationshipType.getEnd2Type().isTypeOrSuperTypeOf(end1TypeName)) {
            validEndTypes = relationshipType.getEnd1Type().isTypeOrSuperTypeOf(end2TypeName);
        }

        if (!validEndTypes) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_RELATIONSHIP_END_TYPE, relationshipName, relationshipType.getEnd2Type().getTypeName(), end1TypeName);
        }

        PropagateTags typePropagation = relationshipType.getRelationshipDef().getPropagateTags();
        PropagateTags edgePropagation = relationship.getPropagateTags();

        if (typePropagation == null) {
            typePropagation = NONE;
        }

        if (edgePropagation == null) {
            edgePropagation = NONE;
        }

        /*
          +-------------+----------------------------------------+
          |     type    |                edge                    |
          +-------------+-------+------------+------------+------+
          |             | NONE  | ONE_TO_TWO | TWO_TO_ONE | BOTH |
          |-------------+-------+------------+------------+------|
          | NONE        |   Y   |     N      |      N     |   N  |
          | ONE_TO_TWO  |   Y   |     Y      |      N     |   N  |
          | TWO_TO_ONE  |   Y   |     N      |      Y     |   N  |
          | BOTH        |   Y   |     Y      |      Y     |   Y  |
          +-------------+-------+------------+------------+------+
         */

        if (edgePropagation != NONE && typePropagation != BOTH && edgePropagation != typePropagation) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PROPAGATION_TYPE, edgePropagation.toString(), relationshipName, typePropagation.toString());
        }

        List<String>      messages     = new ArrayList<>();

        relationshipType.validateValue(relationship, relationshipName, messages);

        if (!messages.isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_CRUD_INVALID_PARAMS, messages);
        }

        relationshipType.getNormalizedValue(relationship);

        Map<String, Object> relAttrs = relationship.getAttributes();
        EntityGraphMapper.validateCustomRelationshipAttributeValueCase(relAttrs);

        RequestContext.get().endMetricRecord(recorder);
    }


    /**
     * Validate the ends of the passed relationship
     * @param relationship
     * @throws AtlasBaseException
     */
    private void validateEnds(AtlasRelationship relationship) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("validateEnds entry relationship:" + relationship);
        }
        List<AtlasObjectId>           ends                 = new ArrayList<>();
        List<AtlasRelationshipEndDef> endDefs              = new ArrayList<>();
        String                        relationshipTypeName = relationship.getTypeName();
        AtlasRelationshipDef          relationshipDef      = typeRegistry.getRelationshipDefByName(relationshipTypeName);

        ends.add(relationship.getEnd1());
        ends.add(relationship.getEnd2());
        endDefs.add(relationshipDef.getEndDef1());
        endDefs.add(relationshipDef.getEndDef2());

        for (int i = 0; i < ends.size(); i++) {
            AtlasObjectId       end              = ends.get(i);
            String              guid             = end.getGuid();
            String              typeName         = end.getTypeName();
            Map<String, Object> uniqueAttributes = end.getUniqueAttributes();
            AtlasVertex         endVertex        = AtlasGraphUtilsV2.findByGuid(this.graph, guid);

            if (!AtlasTypeUtil.isValidGuid(guid) || endVertex == null) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);

            } else if (MapUtils.isNotEmpty(uniqueAttributes)) {
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

                if (AtlasGraphUtilsV2.findByUniqueAttributes(this.graph, entityType, uniqueAttributes) == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, typeName, uniqueAttributes.toString());
                }
            } else {
                // check whether the guid is the correct type
                String vertexTypeName = endVertex.getProperty(Constants.TYPE_NAME_PROPERTY_KEY, String.class);

                if (!Objects.equals(vertexTypeName, typeName)) {
                    String attrName = endDefs.get(i).getName();

                    throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_INVALID_ENDTYPE, attrName, guid, vertexTypeName, typeName);
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("validateEnds exit successfully validated relationship:" + relationship);
        }
    }

    private void validateAndNormalize(AtlasRelationship relationship) throws AtlasBaseException {
        List<String> messages = new ArrayList<>();

        if (! AtlasTypeUtil.isValidGuid(relationship.getGuid())) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_GUID_NOT_FOUND, relationship.getGuid());
        }

        AtlasRelationshipType type = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());

        if (type == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.RELATIONSHIP.name(), relationship.getTypeName());
        }

        type.validateValue(relationship, relationship.getTypeName(), messages);

        if (!messages.isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_CRUD_INVALID_PARAMS, messages);
        }

        type.getNormalizedValue(relationship);
    }

    public AtlasEdge getRelationshipEdge(AtlasVertex fromVertex, AtlasVertex toVertex, String relationshipLabel) {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getRelationshipEdge");

        AtlasEdge ret = null;

        ret = graph.getEdgeBetweenVertices(fromVertex, toVertex, relationshipLabel);

        RequestContext.get().endMetricRecord(metric);
        return ret;
    }

    private Long getRelationshipVersion(AtlasRelationship relationship) {
        Long ret = relationship != null ? relationship.getVersion() : null;

        return (ret != null) ? ret : DEFAULT_RELATIONSHIP_VERSION;
    }

    public AtlasVertex getVertexFromEndPoint(AtlasObjectId endPoint) {
        AtlasVertex ret = null;

        if (StringUtils.isNotEmpty(endPoint.getGuid())) {
            ret = AtlasGraphUtilsV2.findByGuid(this.graph, endPoint.getGuid());
        } else if (StringUtils.isNotEmpty(endPoint.getTypeName()) && MapUtils.isNotEmpty(endPoint.getUniqueAttributes())) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(endPoint.getTypeName());

            ret = AtlasGraphUtilsV2.findByUniqueAttributes(this.graph, entityType, endPoint.getUniqueAttributes());
        }

        return ret;
    }

    private PropagateTags getRelationshipTagPropagation(AtlasVertex fromVertex, AtlasVertex toVertex, AtlasRelationship relationship) {
        AtlasRelationshipType   relationshipType = typeRegistry.getRelationshipTypeByName(relationship.getTypeName());
        AtlasRelationshipEndDef endDef1          = relationshipType.getRelationshipDef().getEndDef1();
        AtlasRelationshipEndDef endDef2          = relationshipType.getRelationshipDef().getEndDef2();
        Set<String>             fromVertexTypes  = getTypeAndAllSuperTypes(getTypeName(fromVertex));
        Set<String>             toVertexTypes    = getTypeAndAllSuperTypes(getTypeName(toVertex));
        PropagateTags           ret              = relationshipType.getRelationshipDef().getPropagateTags();

        // relationshipDef is defined as end1 (hive_db) and end2 (hive_table) and tagPropagation = ONE_TO_TWO
        // relationship edge exists from [hive_table --> hive_db]
        // swap the tagPropagation property for such cases.
        if (fromVertexTypes.contains(endDef2.getType()) && toVertexTypes.contains(endDef1.getType())) {
            if (ret == ONE_TO_TWO) {
                ret = TWO_TO_ONE;
            } else if (ret == TWO_TO_ONE) {
                ret = ONE_TO_TWO;
            }
        }

        return ret;
    }

    public String getRelationshipEdgeLabel(AtlasVertex fromVertex, AtlasVertex toVertex, String relationshipTypeName) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("entityStoreV2.getRelationshipEdgeLabel");
        if (LOG.isDebugEnabled()) {
            LOG.debug("getRelationshipEdgeLabel({})", relationshipTypeName);
        }

        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipTypeName);

        if (relationshipType == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_VALUE, "unknown relationship type'" + relationshipTypeName + "'");
        }

        if (fromVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_END_IS_NULL, relationshipType.getEnd1Type().getTypeName());
        }

        if (toVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.RELATIONSHIP_END_IS_NULL, relationshipType.getEnd2Type().getTypeName());
        }

        String                  ret             = relationshipType.getRelationshipLabel();
        AtlasRelationshipEndDef endDef1         = relationshipType.getRelationshipDef().getEndDef1();
        AtlasRelationshipEndDef endDef2         = relationshipType.getRelationshipDef().getEndDef2();
        Set<String>             fromVertexTypes = getTypeAndAllSuperTypes(AtlasGraphUtilsV2.getTypeName(fromVertex));
        Set<String>             toVertexTypes   = getTypeAndAllSuperTypes(AtlasGraphUtilsV2.getTypeName(toVertex));
        AtlasAttribute          attribute       = null;

        // validate entity type and all its supertypes contains relationshipDefs end type
        // e.g. [hive_process -> hive_table] -> [Process -> DataSet]
        if (fromVertexTypes.contains(endDef1.getType()) && toVertexTypes.contains(endDef2.getType())) {
            String attributeName = endDef1.getName();

            attribute = relationshipType.getEnd1Type().getRelationshipAttribute(attributeName, relationshipTypeName);
        } else if (fromVertexTypes.contains(endDef2.getType()) && toVertexTypes.contains(endDef1.getType())) {
            String attributeName = endDef2.getName();

            attribute = relationshipType.getEnd2Type().getRelationshipAttribute(attributeName, relationshipTypeName);
        }

        if (attribute != null) {
            ret = attribute.getRelationshipEdgeLabel();
        }

        RequestContext.get().endMetricRecord(recorder);
        return ret;
    }

    public Set<String> getTypeAndAllSuperTypes(String entityTypeName) {
        AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entityTypeName);

        return (entityType != null) ? entityType.getTypeAndAllSuperTypes() : new HashSet<String>();
    }

    private String getTypeNameFromObjectId(AtlasObjectId objectId) {
        String typeName = objectId.getTypeName();

        if (StringUtils.isBlank(typeName)) {
            typeName = AtlasGraphUtilsV2.getTypeNameFromGuid(this.graph, objectId.getGuid());
        }

        return typeName;
    }

    /**
     * Check whether this vertex has a relationship associated with this relationship type.
     * @param vertex
     * @param relationshipTypeName
     * @return true if found an edge with this relationship type in.
     */
    private boolean vertexHasRelationshipWithType(AtlasVertex vertex, String relationshipTypeName) {
        String relationshipEdgeLabel = getRelationshipEdgeLabel(getTypeName(vertex), relationshipTypeName);
        Iterator<AtlasEdge> iter     = graphHelper.getAdjacentEdgesByLabel(vertex, AtlasEdgeDirection.BOTH, relationshipEdgeLabel);

        return (iter != null) ? iter.hasNext() : false;
    }

    private String getRelationshipEdgeLabel(String typeName, String relationshipTypeName) {
        AtlasRelationshipType relationshipType = typeRegistry.getRelationshipTypeByName(relationshipTypeName);
        AtlasRelationshipDef  relationshipDef  = relationshipType.getRelationshipDef();
        AtlasEntityType       end1Type         = relationshipType.getEnd1Type();
        AtlasEntityType       end2Type         = relationshipType.getEnd2Type();
        Set<String>           vertexTypes      = getTypeAndAllSuperTypes(typeName);
        AtlasAttribute        attribute        = null;

        if (vertexTypes.contains(end1Type.getTypeName())) {
            String attributeName = relationshipDef.getEndDef1().getName();

            attribute = (attributeName != null) ? end1Type.getAttribute(attributeName) : null;
        } else if (vertexTypes.contains(end2Type.getTypeName())) {
            String attributeName = relationshipDef.getEndDef2().getName();

            attribute = (attributeName != null) ? end2Type.getAttribute(attributeName) : null;
        }

        return (attribute != null) ? attribute.getRelationshipEdgeLabel() : null;
    }

    public void onRelationshipsMutated(Map<String, Set<AtlasRelationship>> relationshipsMutationMap) throws AtlasBaseException {
        entityChangeNotifier.notifyPropagatedEntities();
        RelationshipMutationContext relationshipMutationContext = getRelationshipMutationContext(relationshipsMutationMap);
        if (notificationsEnabled) {
            entityChangeNotifier.notifyRelationshipMutation(relationshipMutationContext.getCreatedRelationships(), OperationType.RELATIONSHIP_CREATE);
            entityChangeNotifier.notifyRelationshipMutation(relationshipMutationContext.getUpdatedRelationships(), OperationType.RELATIONSHIP_UPDATE);
            entityChangeNotifier.notifyRelationshipMutation(relationshipMutationContext.getDeletedRelationships(), OperationType.RELATIONSHIP_DELETE);
        }
    }

    private RelationshipMutationContext getRelationshipMutationContext(Map<String, Set<AtlasRelationship>> relationshipsMutationMap) {
        final List<AtlasRelationship> createdRelationships = new ArrayList<>();
        final List<AtlasRelationship> deletedRelationships = new ArrayList<>();
        final List<AtlasRelationship> updatedRelationships = new ArrayList<>();

        relationshipsMutationMap.keySet().forEach((relationshipMutation) -> {
            final Set<AtlasRelationship> relationships = relationshipsMutationMap.getOrDefault(relationshipMutation, new HashSet<>());
            this.addRelationshipMetadataForNotificationEvent(relationships);
            if (RelationshipMutation.RELATIONSHIP_CREATE.name().equals(relationshipMutation)) {
                createdRelationships.addAll(relationships);
            } else if (RelationshipMutation.RELATIONSHIP_SOFT_DELETE.name().equals(relationshipMutation) || RelationshipMutation.RELATIONSHIP_HARD_DELETE.name().equals(relationshipMutation)) {
                deletedRelationships.addAll(relationships);
            } else if (RelationshipMutation.RELATIONSHIP_UPDATE.name().equals(relationshipMutation) || RelationshipMutation.RELATIONSHIP_RESTORE.name().equals(relationshipMutation)) {
                updatedRelationships.addAll(relationships);
            }
        });
        return RelationshipMutationContext.getInstance(createdRelationships, updatedRelationships, deletedRelationships);
    }

    public static void recordRelationshipMutation(RelationshipMutation relationshipMutation, AtlasEdge edge, EntityGraphRetriever entityRetriever) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("recordRelationshipMutation");
        if (Objects.isNull(edge))
            throw new IllegalStateException("edge cannot be null");
        final AtlasRelationship relationship = entityRetriever.mapEdgeToAtlasRelationship(edge);
        if (relationshipMutation.equals(RelationshipMutation.RELATIONSHIP_HARD_DELETE))
            relationship.setStatus(AtlasRelationship.Status.PURGED);
        AtlasRelationshipStoreV2.setEdgeVertexIdsInContext(edge);
        RequestContext.get().saveRelationshipsMutationContext(relationshipMutation.name(), relationship);

        RequestContext.get().endMetricRecord(recorder);
    }

    private void addRelationshipMetadataForNotificationEvent(Set<AtlasRelationship> relationships) {
        for (AtlasRelationship r : relationships) {
            final Map<String, Object> relationshipDef = buildRelationshipDefMap(r);
            r.setRelationshipDef(relationshipDef);
            final Map<String, String> relationshipEndToESDocIdMap = builsESDocIdMapping(r);
            r.setRelationshipEndToESDocIdMap(relationshipEndToESDocIdMap);
        }
    }

    private static Map<String, String> builsESDocIdMapping(AtlasRelationship r) {

        final Map<String, String> esDocIdMapping = new HashMap<>();

        if(r == null || r.getEnd1() == null || r.getEnd2() == null) {
            LOG.warn(" AtlasRelationship is null {} " , r);
            return esDocIdMapping;
        }

        final Map<AtlasObjectId, String> relationshipEndToVertexIdMap = RequestContext.get().getRelationshipEndToVertexIdMap();
        String end1DocId = "";
        String end2DocId = "";

            for (AtlasObjectId atlasObjectId : relationshipEndToVertexIdMap.keySet()) {
                    if(atlasObjectId == null) {
                        LOG.warn(" atlasObjectId is null.");
                        return esDocIdMapping;
                    }

                    final String docId = AtlasGraphUtilsV2.getDocIdForVertexId(relationshipEndToVertexIdMap.get(atlasObjectId));
                    String guid = atlasObjectId.getGuid();
                    AtlasObjectId end1 = r.getEnd1();
                    AtlasObjectId end2 = r.getEnd2();

                    if(guid == null) {
                        LOG.warn(" atlasObjectId.getGuid() is null. atlasObjectId {}" , atlasObjectId);
                        return esDocIdMapping;
                    }

                    if (guid.equals(end1.getGuid())) {
                        end1DocId = docId;
                    } else if (guid.equals(end2.getGuid())) {
                        end2DocId = docId;
                    }

            }

        esDocIdMapping.put(END_1_DOC_ID_KEY, end1DocId);
        esDocIdMapping.put(END_2_DOC_ID_KEY, end2DocId);
        return esDocIdMapping;
    }

    private Map<String, Object> buildRelationshipDefMap(AtlasRelationship relationship) {
        final AtlasRelationshipDef relationshipDef = typeRegistry.getRelationshipDefByName(relationship.getTypeName());
        Map<String, Object> relationshipDefMap = new HashMap<>();
        relationshipDefMap.put(END_1_CARDINALITY_KEY, relationshipDef.getEndDef1().getCardinality().toString());
        relationshipDefMap.put(END_1_NAME_KEY, relationshipDef.getEndDef1().getName());
        relationshipDefMap.put(IS_END_1_CONTAINER_KEY, relationshipDef.getEndDef1().getIsContainer());

        relationshipDefMap.put(END_2_CARDINALITY_KEY, relationshipDef.getEndDef2().getCardinality().toString());
        relationshipDefMap.put(END_2_NAME_KEY, relationshipDef.getEndDef2().getName());
        relationshipDefMap.put(IS_END_2_CONTAINER_KEY, relationshipDef.getEndDef2().getIsContainer());
        return relationshipDefMap;
    }

    private void createAndQueueTask(String taskType, AtlasEdge relationshipEdge, AtlasRelationship relationship) {
        deleteDelegate.getHandler().createAndQueueTask(taskType, relationshipEdge, relationship);
    }

    private static void setEdgeVertexIdsInContext(AtlasEdge edge) {
        RequestContext.get().addRelationshipEndToVertexIdMapping(GraphHelper.getAtlasObjectIdForOutVertex(edge), edge.getOutVertex().getIdForDisplay());
        RequestContext.get().addRelationshipEndToVertexIdMapping(GraphHelper.getAtlasObjectIdForInVertex(edge), edge.getInVertex().getIdForDisplay());
    }

    private static void validateRelationshipType(String relationshipTypeName) throws AtlasBaseException {
        if (EXCLUDE_MUTATION_REL_TYPE_NAMES.contains(relationshipTypeName)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST,
                    String.format("Mutating relationship of type %s is not supported via relationship APIs, please use entity APIs", relationshipTypeName));
        }
    }
}