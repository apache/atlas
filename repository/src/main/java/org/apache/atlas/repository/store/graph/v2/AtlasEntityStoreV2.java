
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


import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.*;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.authorize.*;
import org.apache.atlas.authorize.AtlasEntityAccessRequest.AtlasEntityAccessRequestBuilder;
import org.apache.atlas.bulkimport.BulkImportResponse;
import org.apache.atlas.bulkimport.BulkImportResponse.ImportInfo;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.featureflag.FeatureFlagStore;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.patches.PatchContext;
import org.apache.atlas.repository.patches.ReIndexPatch;
import org.apache.atlas.repository.store.aliasstore.ESAliasStore;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.AtlasRelationshipStore;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerDelegate;
import org.apache.atlas.repository.store.graph.v1.RestoreHandlerV1;
import org.apache.atlas.repository.store.graph.v2.AtlasEntityComparator.AtlasEntityDiffResult;
import org.apache.atlas.repository.store.graph.v2.preprocessor.AssetPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.AuthPolicyPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.ConnectionPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.PreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol.PersonaPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol.PurposePreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.accesscontrol.StakeholderPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.contract.ContractPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh.DataDomainPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh.DataProductPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.datamesh.StakeholderTitlePreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.CategoryPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.GlossaryPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.glossary.TermPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.resource.LinkPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.resource.ReadmePreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.sql.QueryCollectionPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.sql.QueryFolderPreProcessor;
import org.apache.atlas.repository.store.graph.v2.preprocessor.sql.QueryPreProcessor;
import org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTask;
import org.apache.atlas.tasks.TaskManagement;
import org.apache.atlas.type.*;
import org.apache.atlas.type.AtlasBusinessMetadataType.AtlasBusinessAttribute;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.util.FileUtils;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfMetrics.MetricRecorder;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.lang.Boolean.FALSE;
import static org.apache.atlas.AtlasConfiguration.STORE_DIFFERENTIAL_AUDITS;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.authorize.AtlasPrivilege.*;
import static org.apache.atlas.bulkimport.BulkImportResponse.ImportStatus.FAILED;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.*;
import static org.apache.atlas.repository.Constants.IS_INCOMPLETE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.STATE_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.graph.GraphHelper.*;
import static org.apache.atlas.repository.store.graph.v2.EntityGraphMapper.validateLabels;
import static org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTaskFactory.UPDATE_ENTITY_MEANINGS_ON_TERM_HARD_DELETE;
import static org.apache.atlas.repository.store.graph.v2.tasks.MeaningsTaskFactory.UPDATE_ENTITY_MEANINGS_ON_TERM_SOFT_DELETE;
import static org.apache.atlas.repository.util.AccessControlUtils.REL_ATTR_POLICIES;
import static org.apache.atlas.type.Constants.*;



@Component
public class AtlasEntityStoreV2 implements AtlasEntityStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV2.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("store.EntityStore");

    static final boolean DEFERRED_ACTION_ENABLED = AtlasConfiguration.TASKS_USE_ENABLED.getBoolean();

    private static final String ATTR_MEANINGS = "meanings";

    private final AtlasGraph                graph;
    private final DeleteHandlerDelegate     deleteDelegate;
    private final RestoreHandlerV1          restoreHandlerV1;
    private final AtlasTypeRegistry         typeRegistry;
    private final IAtlasEntityChangeNotifier entityChangeNotifier;
    private final EntityGraphMapper          entityGraphMapper;
    private final EntityGraphRetriever       entityRetriever;
    private       boolean                    storeDifferentialAudits;
    private final GraphHelper                graphHelper;
    private final TaskManagement             taskManagement;
    private EntityDiscoveryService discovery;
    private final AtlasRelationshipStore atlasRelationshipStore;
    private final FeatureFlagStore featureFlagStore;

    private final ESAliasStore esAliasStore;
    private final IAtlasMinimalChangeNotifier atlasAlternateChangeNotifier;

    @Inject
    public AtlasEntityStoreV2(AtlasGraph graph, DeleteHandlerDelegate deleteDelegate, RestoreHandlerV1 restoreHandlerV1, AtlasTypeRegistry typeRegistry,
                              IAtlasEntityChangeNotifier entityChangeNotifier, EntityGraphMapper entityGraphMapper, TaskManagement taskManagement,
                              AtlasRelationshipStore atlasRelationshipStore, FeatureFlagStore featureFlagStore,
                              IAtlasMinimalChangeNotifier atlasAlternateChangeNotifier) {

        this.graph                = graph;
        this.deleteDelegate       = deleteDelegate;
        this.restoreHandlerV1     = restoreHandlerV1;
        this.typeRegistry         = typeRegistry;
        this.entityChangeNotifier = entityChangeNotifier;
        this.entityGraphMapper    = entityGraphMapper;
        this.entityRetriever      = new EntityGraphRetriever(graph, typeRegistry);
        this.storeDifferentialAudits = STORE_DIFFERENTIAL_AUDITS.getBoolean();
        this.graphHelper          = new GraphHelper(graph);
        this.taskManagement = taskManagement;
        this.atlasRelationshipStore = atlasRelationshipStore;
        this.featureFlagStore = featureFlagStore;
        this.esAliasStore = new ESAliasStore(graph, entityRetriever);
        this.atlasAlternateChangeNotifier = atlasAlternateChangeNotifier;
        try {
            this.discovery = new EntityDiscoveryService(typeRegistry, graph, null, null, null, null);
        } catch (AtlasException e) {
            e.printStackTrace();
        }

    }

    @VisibleForTesting
    public void setStoreDifferentialAudits(boolean val) {
        this.storeDifferentialAudits = val;
    }

    @Override
    @GraphTransaction
    public List<String> getEntityGUIDS(final String typename) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getEntityGUIDS({})", typename);
        }

        if (StringUtils.isEmpty(typename) || !typeRegistry.isRegisteredType(typename)) {
            throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME);
        }

        List<String> ret = AtlasGraphUtilsV2.findEntityGUIDsByType(graph, typename);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getEntityGUIDS({})", typename);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityWithExtInfo getById(String guid) throws AtlasBaseException {
        return getById(guid, false, false);
    }

    @Override
    @GraphTransaction
    public AtlasEntityWithExtInfo getById(final String guid, final boolean isMinExtInfo, boolean ignoreRelationships) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getById({}, {})", guid, isMinExtInfo);
        }

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(graph, typeRegistry, ignoreRelationships);

        AtlasEntityWithExtInfo ret = entityRetriever.toAtlasEntityWithExtInfo(guid, isMinExtInfo);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, new AtlasEntityHeader(ret.getEntity())), "read entity: guid=", guid);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getById({}, {}): {}", guid, isMinExtInfo, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityWithExtInfo getByIdWithoutAuthorization(final String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getByIdWithoutAuthorization({})", guid);
        }

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(graph, typeRegistry, true);

        AtlasEntityWithExtInfo ret = entityRetriever.toAtlasEntityWithExtInfo(guid, true);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getByIdWithoutAuthorization({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityHeader getHeaderById(final String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getHeaderById({})", guid);
        }

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(graph, typeRegistry);

        AtlasEntityHeader ret = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, ret), "read entity: guid=", guid);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getHeaderById({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntitiesWithExtInfo getByIds(List<String> guids) throws AtlasBaseException {
        return getByIds(guids, false, false);
    }

    @Override
    @GraphTransaction
    public AtlasEntitiesWithExtInfo getByIds(List<String> guids, boolean isMinExtInfo, boolean ignoreRelationships) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getByIds({}, {})", guids, isMinExtInfo);
        }

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(graph, typeRegistry, ignoreRelationships);

        AtlasEntitiesWithExtInfo ret = entityRetriever.toAtlasEntitiesWithExtInfo(guids, isMinExtInfo);

        if(ret != null){
            for(String guid : guids) {
                AtlasEntity entity = ret.getEntity(guid);
                try {
                    AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, new AtlasEntityHeader(entity)), "read entity: guid=", guid);
                } catch (AtlasBaseException e) {
                    if (RequestContext.get().isSkipFailedEntities()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("getByIds(): ignoring failure for entity {}: error code={}, message={}", guid, e.getAtlasErrorCode(), e.getMessage());
                        }

                        //Remove from referred entities
                        ret.removeEntity(guid);
                        //Remove from entities
                        ret.removeEntity(entity);

                        continue;
                    }

                    throw e;
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getByIds({}, {}): {}", guids, isMinExtInfo, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntitiesWithExtInfo getEntitiesByUniqueAttributes(AtlasEntityType entityType, List<Map<String, Object>> uniqueAttributes , boolean isMinExtInfo, boolean ignoreRelationships) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getEntitiesByUniqueAttributes({}, {})", entityType.getTypeName(), uniqueAttributes);
        }

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(graph, typeRegistry, ignoreRelationships);

        AtlasEntitiesWithExtInfo ret = entityRetriever.getEntitiesByUniqueAttributes(entityType.getTypeName(), uniqueAttributes, isMinExtInfo);

        if (ret != null && ret.getEntities() != null) {
            for (AtlasEntity entity : ret.getEntities()) {
                AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, new AtlasEntityHeader(entity)), "read entity: typeName=", entityType.getTypeName(), ", guid=", entity.getGuid());
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getEntitiesByUniqueAttributes({}, {}): {}", entityType.getTypeName(), uniqueAttributes, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityWithExtInfo getByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes)
            throws AtlasBaseException {
        return getByUniqueAttributes(entityType, uniqAttributes, false, false);
    }

    @Override
    @GraphTransaction
    public AtlasEntityWithExtInfo getByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes, boolean isMinExtInfo, boolean ignoreRelationships) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getByUniqueAttribute({}, {})", entityType.getTypeName(), uniqAttributes);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.getVertexByUniqueAttributes(graph, entityType, uniqAttributes);

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(graph, typeRegistry, ignoreRelationships);

        AtlasEntityWithExtInfo ret = entityRetriever.toAtlasEntityWithExtInfo(entityVertex, isMinExtInfo);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, entityType.getTypeName(),
                    uniqAttributes.toString());
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, new AtlasEntityHeader(ret.getEntity())), "read entity: typeName=", entityType.getTypeName(), ", uniqueAttributes=", uniqAttributes);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getByUniqueAttribute({}, {}): {}", entityType.getTypeName(), uniqAttributes, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityHeader getAtlasEntityHeaderWithoutAuthorization(String guid, String qualifiedName, String typeName) throws AtlasBaseException {
        return extractEntityHeader( guid,  qualifiedName,  typeName);
    }

    @Override
    @GraphTransaction
    public AtlasEntityHeader getEntityHeaderByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getEntityHeaderByUniqueAttributes({}, {})", entityType.getTypeName(), uniqAttributes);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.getVertexByUniqueAttributes(graph, entityType, uniqAttributes);

        EntityGraphRetriever entityRetriever = new EntityGraphRetriever(graph, typeRegistry);

        AtlasEntityHeader ret = entityRetriever.toAtlasEntityHeader(entityVertex);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, entityType.getTypeName(),
                    uniqAttributes.toString());
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, ret), "read entity: typeName=", entityType.getTypeName(), ", uniqueAttributes=", uniqAttributes);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getEntityHeaderByUniqueAttributes({}, {}): {}", entityType.getTypeName(), uniqAttributes, ret);
        }

        return ret;
    }

    /**
     * Check state of entities in the store
     * @param request AtlasCheckStateRequest
     * @return AtlasCheckStateResult
     * @throws AtlasBaseException
     */
    @Override
    @GraphTransaction
    public AtlasCheckStateResult checkState(AtlasCheckStateRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> checkState({})", request);
        }

        EntityStateChecker entityStateChecker = new EntityStateChecker(graph, typeRegistry);

        AtlasCheckStateResult ret = entityStateChecker.checkState(request);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== checkState({}, {})", request, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse createOrUpdate(EntityStream entityStream, boolean isPartialUpdate) throws AtlasBaseException {
        return createOrUpdate(entityStream, isPartialUpdate, false, false);
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse createOrUpdate(EntityStream entityStream,  boolean replaceClassifications,
                                                 boolean replaceBusinessAttributes, boolean isOverwriteBusinessAttributes) throws AtlasBaseException {
        return createOrUpdate(entityStream, false, replaceClassifications, replaceBusinessAttributes, isOverwriteBusinessAttributes);
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse createOrUpdateGlossary(EntityStream entityStream, boolean isPartialUpdate, boolean replaceClassification) throws AtlasBaseException {
        return createOrUpdate(entityStream, isPartialUpdate, true, false);
    }

    @Override
    @GraphTransaction(logRollback = false)
    public EntityMutationResponse createOrUpdateForImport(EntityStream entityStream) throws AtlasBaseException {
        return createOrUpdate(entityStream, false, true, true, false);
    }

    @Override
    public EntityMutationResponse createOrUpdateForImportNoCommit(EntityStream entityStream) throws AtlasBaseException {
        return createOrUpdate(entityStream, false, true, true, false);
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse updateEntity(AtlasObjectId objectId, AtlasEntityWithExtInfo updatedEntityInfo, boolean isPartialUpdate) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> updateEntity({}, {}, {})", objectId, updatedEntityInfo, isPartialUpdate);
        }

        if (objectId == null || updatedEntityInfo == null || updatedEntityInfo.getEntity() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "null entity-id/entity");
        }

        final String guid;

        if (AtlasTypeUtil.isAssignedGuid(objectId.getGuid())) {
            guid = objectId.getGuid();
        } else {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(objectId.getTypeName());

            if (entityType == null) {
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, objectId.getTypeName());
            }

            guid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(graph, typeRegistry.getEntityTypeByName(objectId.getTypeName()), objectId.getUniqueAttributes());
        }

        AtlasEntity entity = updatedEntityInfo.getEntity();

        entity.setGuid(guid);

        return createOrUpdate(new AtlasEntityStream(updatedEntityInfo), isPartialUpdate, false, false, false);
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse updateByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes,
                                                           AtlasEntityWithExtInfo updatedEntityInfo) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> updateByUniqueAttributes({}, {})", entityType.getTypeName(), uniqAttributes);
        }

        if (updatedEntityInfo == null || updatedEntityInfo.getEntity() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no entity to update.");
        }

        String      guid   = AtlasGraphUtilsV2.getGuidByUniqueAttributes(graph, entityType, uniqAttributes);
        AtlasEntity entity = updatedEntityInfo.getEntity();

        entity.setGuid(guid);

        AtlasAuthorizationUtils.verifyUpdateEntityAccess(typeRegistry, new AtlasEntityHeader(entity), "update entity ByUniqueAttributes");

        return createOrUpdate(new AtlasEntityStream(updatedEntityInfo), true, false, false, false);
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse updateEntityAttributeByGuid(String guid, String attrName, Object attrValue)
            throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> updateEntityAttributeByGuid({}, {}, {})", guid, attrName, attrValue);
        }

        AtlasEntityHeader entity     = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);
        AtlasEntityType   entityType = (AtlasEntityType) typeRegistry.getType(entity.getTypeName());
        AtlasAttribute    attr       = entityType.getAttribute(attrName);

        AtlasAuthorizationUtils.verifyUpdateEntityAccess(typeRegistry, entity, "update entity ByUniqueAttributes : guid=" + guid);

        if (attr == null) {
            attr = entityType.getRelationshipAttribute(attrName, AtlasEntityUtil.getRelationshipType(attrValue));

            if (attr == null) {
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_ATTRIBUTE, attrName, entity.getTypeName());
            }
        }

        AtlasType   attrType     = attr.getAttributeType();
        AtlasEntity updateEntity = new AtlasEntity();

        updateEntity.setGuid(guid);
        updateEntity.setTypeName(entity.getTypeName());

        switch (attrType.getTypeCategory()) {
            case PRIMITIVE:
            case ARRAY:
            case ENUM:
            case MAP:
                updateEntity.setAttribute(attrName, attrValue);
                break;
            case OBJECT_ID_TYPE:
                AtlasObjectId objId;

                if (attrValue instanceof String) {
                    objId = new AtlasObjectId((String) attrValue, attr.getAttributeDef().getTypeName());
                } else {
                    objId = (AtlasObjectId) attrType.getNormalizedValue(attrValue);
                }

                updateEntity.setAttribute(attrName, objId);
                break;

            default:
                throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_UPDATE_NOT_SUPPORTED, attrName, attrType.getTypeName());
        }

        return createOrUpdate(new AtlasEntityStream(updateEntity), true, false, false, false);
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse deleteById(final String guid) throws AtlasBaseException {
        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        Collection<AtlasVertex> deletionCandidates = new ArrayList<>();
        AtlasVertex             vertex             = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (vertex != null) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);

            AtlasAuthorizationUtils.verifyDeleteEntityAccess(typeRegistry, entityHeader, "delete entity: guid=" + guid);

            deletionCandidates.add(vertex);
        } else {
            if (LOG.isDebugEnabled()) {
                // Entity does not exist - treat as non-error, since the caller
                // wanted to delete the entity and it's already gone.
                LOG.debug("Deletion request ignored for non-existent entity with guid " + guid);
            }
        }

        EntityMutationResponse ret = deleteVertices(deletionCandidates);

        if(ret.getDeletedEntities()!=null)
            processTermEntityDeletion(ret.getDeletedEntities());

        // Notify the change listeners
        entityChangeNotifier.onEntitiesMutated(ret, false);
        atlasRelationshipStore.onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());
        return ret;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse deleteByIds(final List<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(guids)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }

        Collection<AtlasVertex> deletionCandidates = new ArrayList<>();

        for (String guid : guids) {
            AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

            if (vertex == null) {
                if (LOG.isDebugEnabled()) {
                    // Entity does not exist - treat as non-error, since the caller
                    // wanted to delete the entity and it's already gone.
                    LOG.debug("Deletion request ignored for non-existent entity with guid " + guid);
                }

                continue;
            }

            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);

            AtlasAuthorizationUtils.verifyDeleteEntityAccess(typeRegistry, entityHeader, "delete entity: guid=" + guid);

            deletionCandidates.add(vertex);
        }

        if (deletionCandidates.isEmpty()) {
            LOG.info("No deletion candidate entities were found for guids %s", guids);
        }

        EntityMutationResponse ret = deleteVertices(deletionCandidates);

        if(ret.getDeletedEntities() != null)
            processTermEntityDeletion(ret.getDeletedEntities());

        // Notify the change listeners
        entityChangeNotifier.onEntitiesMutated(ret, false);
        atlasRelationshipStore.onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());
        return ret;
    }


    @Override
    @GraphTransaction
    public EntityMutationResponse restoreByIds(final List<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(guids)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }

        Collection<AtlasVertex> restoreCandidates = new ArrayList<>();

        for (String guid : guids) {
            AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

            if (vertex == null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Restore request ignored for non-existent entity with guid " + guid);
                }

                continue;
            }

            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);

            AtlasAuthorizationUtils.verifyDeleteEntityAccess(typeRegistry, entityHeader, "delete entity: guid=" + guid);

            restoreCandidates.add(vertex);
        }

        if (restoreCandidates.isEmpty()) {
            LOG.info("No restore candidate entities were found for guids %s", guids);
        }

        EntityMutationResponse ret = restoreVertices(restoreCandidates);

        // Notify the change listeners
        entityChangeNotifier.onEntitiesMutated(ret, false);
        atlasRelationshipStore.onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());
        return ret;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse purgeByIds(Set<String> guids) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(guids)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.ADMIN_PURGE), "purge entity: guids=", guids);
        Collection<AtlasVertex> purgeCandidates = new ArrayList<>();

        for (String guid : guids) {
            AtlasVertex vertex = AtlasGraphUtilsV2.findDeletedByGuid(graph, guid);

            if (vertex == null) {
                // Entity does not exist - treat as non-error, since the caller
                // wanted to delete the entity and it's already gone.
                LOG.warn("Purge request ignored for non-existent/active entity with guid " + guid);

                continue;
            }
            this.recordRelationshipsToBePurged(vertex);
            purgeCandidates.add(vertex);
        }

        if (purgeCandidates.isEmpty()) {
            LOG.info("No purge candidate entities were found for guids: " + guids + " which is already deleted");
        }

        EntityMutationResponse ret = purgeVertices(purgeCandidates);

        // Notify the change listeners
        entityChangeNotifier.onEntitiesMutated(ret, false);
        atlasRelationshipStore.onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());
        return ret;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse deleteByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes) throws AtlasBaseException {
        if (MapUtils.isEmpty(uniqAttributes)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, uniqAttributes.toString());
        }

        Collection<AtlasVertex> deletionCandidates = new ArrayList<>();
        AtlasVertex             vertex             = AtlasGraphUtilsV2.findByUniqueAttributes(graph, entityType, uniqAttributes);

        if (vertex != null) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);

            AtlasAuthorizationUtils.verifyDeleteEntityAccess(typeRegistry, entityHeader,
                    "delete entity: typeName=" + entityType.getTypeName() + ", uniqueAttributes=" + uniqAttributes);

            deletionCandidates.add(vertex);
        } else {
            if (LOG.isDebugEnabled()) {
                // Entity does not exist - treat as non-error, since the caller
                // wanted to delete the entity and it's already gone.
                LOG.debug("Deletion request ignored for non-existent entity with uniqueAttributes " + uniqAttributes);
            }
        }

        EntityMutationResponse ret = deleteVertices(deletionCandidates);

        if(ret.getDeletedEntities()!=null)
            processTermEntityDeletion(ret.getDeletedEntities());

        // Notify the change listeners
        entityChangeNotifier.onEntitiesMutated(ret, false);
        atlasRelationshipStore.onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());
        return ret;
    }

    private AtlasEntityHeader getAtlasEntityHeader(String entityGuid, String entityId, String entityType) throws AtlasBaseException {
        // Metric logs
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getAtlasEntityHeader");
        AtlasEntityHeader entityHeader = null;
        String cacheKey = generateCacheKey(entityGuid, entityId, entityType);
        entityHeader = RequestContext.get().getCachedEntityHeader(cacheKey);
        if(Objects.nonNull(entityHeader)){
            return entityHeader;
        }
        if (StringUtils.isNotEmpty(entityGuid)) {
            AtlasEntityWithExtInfo ret = getByIdWithoutAuthorization(entityGuid);
            entityHeader = new AtlasEntityHeader(ret.getEntity());
        } else if (StringUtils.isNotEmpty(entityId) && StringUtils.isNotEmpty(entityType)) {
            try {
                entityHeader = getAtlasEntityHeaderWithoutAuthorization(null, entityId, entityType);
            } catch (AtlasBaseException abe) {
                if (abe.getAtlasErrorCode() == AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND) {
                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put(QUALIFIED_NAME, entityId);
                    entityHeader = new AtlasEntityHeader(entityType, attributes);
                }
            }
        } else {
            throw new AtlasBaseException(BAD_REQUEST, "requires entityGuid or typeName and qualifiedName for entity authorization");
        }
        RequestContext.get().setEntityHeaderCache(cacheKey, entityHeader);
        RequestContext.get().endMetricRecord(metric);
        return entityHeader;
    }

    @Override
    public List<AtlasEvaluatePolicyResponse> evaluatePolicies(List<AtlasEvaluatePolicyRequest> entities) throws AtlasBaseException {
        List<AtlasEvaluatePolicyResponse> response = new ArrayList<>();
        HashMap<String, AtlasEntityHeader> atlasEntityHeaderCache = new HashMap<>();
        for (AtlasEvaluatePolicyRequest entity : entities) {
            String action = entity.getAction();

            if (action == null) {
                throw new AtlasBaseException(BAD_REQUEST, "action is null");
            }
            AtlasEntityHeader entityHeader = null;

            if (ENTITY_READ.name().equals(action) || ENTITY_CREATE.name().equals(action) || ENTITY_UPDATE.name().equals(action)
                    || ENTITY_DELETE.name().equals(action) || ENTITY_UPDATE_BUSINESS_METADATA.name().equals(action)) {

                try {
                    entityHeader = getAtlasEntityHeader(entity.getEntityGuid(), entity.getEntityId(), entity.getTypeName());
                    AtlasEntityAccessRequest.AtlasEntityAccessRequestBuilder requestBuilder = new AtlasEntityAccessRequest.AtlasEntityAccessRequestBuilder(typeRegistry, AtlasPrivilege.valueOf(entity.getAction()), entityHeader);
                    if (entity.getBusinessMetadata() != null) {
                        requestBuilder.setBusinessMetadata(entity.getBusinessMetadata());
                    }

                    AtlasEntityAccessRequest entityAccessRequest = requestBuilder.build();

                    AtlasAuthorizationUtils.verifyAccess(entityAccessRequest, entity.getAction() + "guid=" + entity.getEntityGuid());
                    response.add(new AtlasEvaluatePolicyResponse(entity.getTypeName(), entity.getEntityGuid(), entity.getAction(), entity.getEntityId(), true, null , entity.getBusinessMetadata()));
                } catch (AtlasBaseException e) {
                    AtlasErrorCode code = e.getAtlasErrorCode();
                    String errorCode = code.getErrorCode();
                    response.add(new AtlasEvaluatePolicyResponse(entity.getTypeName(), entity.getEntityGuid(), entity.getAction(), entity.getEntityId(), false, errorCode, entity.getBusinessMetadata()));
                }

            } else if (ENTITY_REMOVE_CLASSIFICATION.name().equals(action) || ENTITY_ADD_CLASSIFICATION.name().equals(action) || ENTITY_UPDATE_CLASSIFICATION.name().equals(action)) {

                if (entity.getClassification() == null) {
                    throw new AtlasBaseException(BAD_REQUEST, "classification needed for " + action + " authorization");
                }
                try {
                    entityHeader = getAtlasEntityHeader(entity.getEntityGuid(), entity.getEntityId(), entity.getTypeName());

                    AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.valueOf(entity.getAction()), entityHeader, new AtlasClassification(entity.getClassification())));
                    response.add(new AtlasEvaluatePolicyResponse(entity.getTypeName(), entity.getEntityGuid(), entity.getAction(), entity.getEntityId(), entity.getClassification(), true, null));

                } catch (AtlasBaseException e) {
                    AtlasErrorCode code = e.getAtlasErrorCode();
                    String errorCode = code.getErrorCode();
                    response.add(new AtlasEvaluatePolicyResponse(entity.getTypeName(), entity.getEntityGuid(), entity.getAction(), entity.getEntityId(), entity.getClassification(), false, errorCode));
                }

            }    else if (RELATIONSHIP_ADD.name().equals(action) || RELATIONSHIP_REMOVE.name().equals(action) || RELATIONSHIP_UPDATE.name().equals(action)) {

                if (entity.getRelationShipTypeName() == null) {
                    throw new AtlasBaseException(BAD_REQUEST, "RelationShip TypeName needed for " + action + " authorization");
                }

                try {
                    AtlasEntityHeader end1Entity = getAtlasEntityHeader(entity.getEntityGuidEnd1(), entity.getEntityIdEnd1(), entity.getEntityTypeEnd1());

                    AtlasEntityHeader end2Entity = getAtlasEntityHeader(entity.getEntityGuidEnd2(), entity.getEntityIdEnd2(), entity.getEntityTypeEnd2());

                    AtlasAuthorizationUtils.verifyAccess(new AtlasRelationshipAccessRequest(typeRegistry, AtlasPrivilege.valueOf(action), entity.getRelationShipTypeName(), end1Entity, end2Entity));
                    response.add(new AtlasEvaluatePolicyResponse(action, entity.getRelationShipTypeName(), entity.getEntityTypeEnd1(), entity.getEntityGuidEnd1(), entity.getEntityIdEnd1(), entity.getEntityTypeEnd2(), entity.getEntityGuidEnd2(), entity.getEntityIdEnd2(), true, null));
                } catch (AtlasBaseException e) {
                    AtlasErrorCode code = e.getAtlasErrorCode();
                    String errorCode = code.getErrorCode();
                    response.add(new AtlasEvaluatePolicyResponse(action, entity.getRelationShipTypeName(), entity.getEntityTypeEnd1(), entity.getEntityGuidEnd1(), entity.getEntityIdEnd1(), entity.getEntityTypeEnd2(), entity.getEntityGuidEnd2(), entity.getEntityIdEnd2(), false, errorCode));
                }
            }
        }
        return response;
    }

    private String generateCacheKey(String guid, String id, String typeName) {
        return (guid != null ? guid : "") + "|" + (id != null ? id : "") + "|" + (typeName != null ? typeName : "");
    }



    @Override
    @GraphTransaction
    public EntityMutationResponse deleteByUniqueAttributes(List<AtlasObjectId> objectIds) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(objectIds)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS);
        }

        EntityMutationResponse ret = new EntityMutationResponse();
        Collection<AtlasVertex> deletionCandidates = new ArrayList<>();
        try {
            for (AtlasObjectId objectId : objectIds) {
                if (StringUtils.isEmpty(objectId.getTypeName())) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "typeName not specified");
                }

                if (MapUtils.isEmpty(objectId.getUniqueAttributes())) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "uniqueAttributes not specified");
                }

                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(objectId.getTypeName());

                if (entityType == null) {
                    throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), objectId.getTypeName());
                }

                AtlasVertex vertex = AtlasGraphUtilsV2.findByUniqueAttributes(graph, entityType, objectId.getUniqueAttributes());

                if (vertex != null) {
                    AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);

                    AtlasAuthorizationUtils.verifyDeleteEntityAccess(typeRegistry, entityHeader,
                            "delete entity: typeName=" + entityType.getTypeName() + ", uniqueAttributes=" + objectId.getUniqueAttributes());

                    deletionCandidates.add(vertex);
                } else {
                    if (LOG.isDebugEnabled()) {
                        // Entity does not exist - treat as non-error, since the caller
                        // wanted to delete the entity and it's already gone.
                        LOG.debug("Deletion request ignored for non-existent entity with uniqueAttributes " + objectId.getUniqueAttributes());
                    }
                }
            }

            ret = deleteVertices(deletionCandidates);

            if (ret.getDeletedEntities() != null)
                processTermEntityDeletion(ret.getDeletedEntities());
            // Notify the change listeners
            entityChangeNotifier.onEntitiesMutated(ret, false);
            atlasRelationshipStore.onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());

        } catch (Exception e) {
            LOG.error("Failed to delete objects:{}", objectIds.stream().map(AtlasObjectId::getUniqueAttributes).collect(Collectors.toList()), e);
            throw new AtlasBaseException(e);
        }
        return ret;
    }

    private void processTermEntityDeletion(List<AtlasEntityHeader> deletedEntities) throws AtlasBaseException{
        for(AtlasEntityHeader entity:deletedEntities){
            if(ATLAS_GLOSSARY_TERM_ENTITY_TYPE.equals(entity.getTypeName())){

                String termQualifiedName    = entity.getAttribute(QUALIFIED_NAME).toString();
                String termName             = entity.getAttribute(NAME).toString();
                String guid                 = entity.getGuid();
                Boolean isHardDelete        = DeleteType.HARD.name().equals(entity.getDeleteHandler());

                if(checkEntityTermAssociation(termQualifiedName)){
                    if(DEFERRED_ACTION_ENABLED && taskManagement!=null){
                        createAndQueueTask(termName, termQualifiedName, guid, isHardDelete);
                    }else{
                        updateMeaningsNamesInEntitiesOnTermDelete(termName, termQualifiedName, guid);
                    }
                }
            }
        }
    }

    private boolean checkEntityTermAssociation(String termQName) throws AtlasBaseException{
        List<AtlasEntityHeader> entityHeader;

        try {
            entityHeader = discovery.searchUsingTermQualifiedName(0, 1, termQName,null, null);
        } catch (AtlasBaseException e) {
            throw e;
        }
        Boolean hasEntityAssociation = entityHeader != null ? true : false;

        return hasEntityAssociation;
    }

    public void updateMeaningsNamesInEntitiesOnTermDelete(String termName, String termQName, String termGuid) throws AtlasBaseException {
        int from = 0;

        Set<String> attributes = new HashSet<String>(){{
            add(ATTR_MEANINGS);
        }};
        Set<String> relationAttributes = new HashSet<String>(){{
            add(STATE_PROPERTY_KEY);
            add(NAME);
        }};

        while (true) {
            List<AtlasEntityHeader> entityHeaders = discovery.searchUsingTermQualifiedName(from, ELASTICSEARCH_PAGINATION_SIZE,
                    termQName, attributes, relationAttributes);

            if (entityHeaders == null)
                break;

            for (AtlasEntityHeader entityHeader : entityHeaders) {
                List<AtlasObjectId> meanings = (List<AtlasObjectId>) entityHeader.getAttribute(ATTR_MEANINGS);

                String updatedMeaningsText = meanings.stream()
                        .filter(x -> !termGuid.equals(x.getGuid()))
                        .filter(x -> ACTIVE.name().equals(x.getAttributes().get(STATE_PROPERTY_KEY)))
                        .map(x -> x.getAttributes().get(NAME).toString())
                        .collect(Collectors.joining(","));


                AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(entityHeader.getGuid());
                AtlasGraphUtilsV2.removeItemFromListPropertyValue(entityVertex, MEANINGS_PROPERTY_KEY, termQName);
                AtlasGraphUtilsV2.setEncodedProperty(entityVertex, MEANINGS_TEXT_PROPERTY_KEY, updatedMeaningsText);
                AtlasGraphUtilsV2.removeItemFromListPropertyValue(entityVertex, MEANING_NAMES_PROPERTY_KEY, termName);
            }
            from += ELASTICSEARCH_PAGINATION_SIZE;

            if (entityHeaders.size() < ELASTICSEARCH_PAGINATION_SIZE)
                break;
        }

    }

    public void createAndQueueTask(String termName, String termQName, String termGuid, Boolean isHardDelete){
        String taskType = isHardDelete ? UPDATE_ENTITY_MEANINGS_ON_TERM_HARD_DELETE : UPDATE_ENTITY_MEANINGS_ON_TERM_SOFT_DELETE;
        String currentUser = RequestContext.getCurrentUser();
        Map<String, Object> taskParams = MeaningsTask.toParameters(termName, termQName, termGuid);
        AtlasTask task = taskManagement.createTask(taskType, currentUser, taskParams);

        if(!isHardDelete){
            AtlasVertex termVertex = AtlasGraphUtilsV2.findByGuid(termGuid);
            AtlasGraphUtilsV2.addEncodedProperty(termVertex, PENDING_TASKS_PROPERTY_KEY, task.getGuid());
        }

        RequestContext.get().queueTask(task);
    }


    @Override
    @GraphTransaction
    public String getGuidByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes) throws AtlasBaseException{
        return AtlasGraphUtilsV2.getGuidByUniqueAttributes(graph, entityType, uniqAttributes);
    }

    @Override
    @GraphTransaction
    public void repairClassificationMappings(final String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> repairClassificationMappings({})", guid);
        }

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        entityGraphMapper.repairClassificationMappings(entityVertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== repairClassificationMappings({})", guid);
        }
    }

    @Override
    @GraphTransaction
    public void addClassifications(final String guid, final List<AtlasClassification> classifications) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding classifications={} to entity={}", classifications, guid);
        }

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }

        if (CollectionUtils.isEmpty(classifications)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "classifications(s) not specified");
        }

        GraphTransactionInterceptor.lockObjectAndReleasePostCommit(guid);

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(entityVertex);

        for (AtlasClassification classification : classifications) {
            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_ADD_CLASSIFICATION, entityHeader, classification),
                                                 "add classification: guid=", guid, ", classification=", classification.getTypeName());
        }

        EntityMutationContext context = new EntityMutationContext();

        context.cacheEntity(guid, entityVertex, typeRegistry.getEntityTypeByName(entityHeader.getTypeName()));


        for (AtlasClassification classification : classifications) {
            validateAndNormalize(classification);
        }

        // validate if entity, not already associated with classifications
        validateEntityAssociations(guid, classifications);

        entityGraphMapper.addClassifications(context, guid, classifications);
    }

    @Override
    @GraphTransaction
    public void updateClassifications(String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating classifications={} for entity={}", classifications, guid);
        }

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            AtlasPerfTracer.getPerfTracer(PERF_LOG, "AtlasEntityStoreV2.updateClassification()");
        }

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid not specified");
        }

        if (CollectionUtils.isEmpty(classifications)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "classifications(s) not specified");
        }

        GraphTransactionInterceptor.lockObjectAndReleasePostCommit(guid);

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(entityVertex);

        for (AtlasClassification classification : classifications) {
            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE_CLASSIFICATION, entityHeader, classification), "update classification: guid=", guid, ", classification=", classification.getTypeName());
        }

        EntityMutationContext context = new EntityMutationContext();

        context.cacheEntity(guid, entityVertex, typeRegistry.getEntityTypeByName(entityHeader.getTypeName()));


        for (AtlasClassification classification : classifications) {
            validateAndNormalize(classification);
        }

        entityGraphMapper.updateClassifications(context, guid, classifications);

        AtlasPerfTracer.log(perf);
    }

    @Override
    @GraphTransaction
    public void addClassification(final List<String> guids, final AtlasClassification classification) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding classification={} to entities={}", classification, guids);
        }

        if (CollectionUtils.isEmpty(guids)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }

        if (classification == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "classification not specified");
        }

        validateAndNormalize(classification);

        EntityMutationContext     context         = new EntityMutationContext();
        List<AtlasClassification> classifications = Collections.singletonList(classification);
        List<String>              validGuids      =  new ArrayList<>();

        GraphTransactionInterceptor.lockObjectAndReleasePostCommit(guids);

        for (String guid : guids) {
            try {
                AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

                if (entityVertex == null) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
                }

                AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(entityVertex);

                AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_ADD_CLASSIFICATION, entityHeader, classification),
                        "add classification: guid=", guid, ", classification=", classification.getTypeName());

                validateEntityAssociations(guid, classifications);

                validGuids.add(guid);
                context.cacheEntity(guid, entityVertex, typeRegistry.getEntityTypeByName(entityHeader.getTypeName()));
            } catch (AtlasBaseException abe) {
                if (RequestContext.get().isSkipFailedEntities()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("addClassification(): ignoring failure for entity {}: error code={}, message={}", guid, abe.getAtlasErrorCode(), abe.getMessage());
                    }

                    continue;
                }

                throw abe;
            }
        }

        for (String guid : validGuids) {
            entityGraphMapper.addClassifications(context, guid, classifications);
        }
    }

    @Override
    @GraphTransaction
    public void deleteClassification(final String guid, final String classificationName) throws AtlasBaseException {
        deleteClassification(guid, classificationName, null);
    }

    @Override
    @GraphTransaction
    public void deleteClassification(final String guid, final String classificationName, final String associatedEntityGuid) throws AtlasBaseException {
        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }
        if (StringUtils.isEmpty(classificationName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "classifications not specified");
        }

        GraphTransactionInterceptor.lockObjectAndReleasePostCommit(guid);

        AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);

        // verify authorization only for removal of directly associated classification and not propagated one.
        if (StringUtils.isEmpty(associatedEntityGuid) || guid.equals(associatedEntityGuid)) {
            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_REMOVE_CLASSIFICATION,
                            entityHeader, new AtlasClassification(classificationName)),
                    "remove classification: guid=", guid, ", classification=", classificationName);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting classification={} from entity={}", classificationName, guid);
        }


        entityGraphMapper.deleteClassification(guid, classificationName, associatedEntityGuid);
    }


    @GraphTransaction
    public List<AtlasClassification> retrieveClassifications(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Retriving classifications for entity={}", guid);
        }

        AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);

        return entityHeader.getClassifications();
    }


    @Override
    @GraphTransaction
    public List<AtlasClassification> getClassifications(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting classifications for entity={}", guid);
        }

        AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, entityHeader), "get classifications: guid=", guid);

        return entityHeader.getClassifications();
    }

    @Override
    @GraphTransaction
    public AtlasClassification getClassification(String guid, String classificationName) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Getting classifications for entities={}", guid);
        }

        AtlasClassification ret          = null;
        AtlasEntityHeader   entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);

        if (CollectionUtils.isNotEmpty(entityHeader.getClassifications())) {
            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, entityHeader), "get classification: guid=", guid, ", classification=", classificationName);

            for (AtlasClassification classification : entityHeader.getClassifications()) {
                if (!StringUtils.equalsIgnoreCase(classification.getTypeName(), classificationName)) {
                    continue;
                }

                if (StringUtils.isEmpty(classification.getEntityGuid()) || StringUtils.equalsIgnoreCase(classification.getEntityGuid(), guid)) {
                    ret = classification;
                    break;
                } else if (ret == null) {
                    ret = classification;
                }
            }
        }

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_FOUND, classificationName);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public void addOrUpdateBusinessAttributesByDisplayName(String guid, Map<String, Map<String, Object>> businessAttrbutes, boolean isOverwrite) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addOrUpdateBusinessAttributesByDisplayName(guid={}, businessAttributes={}, isOverwrite={})", guid, businessAttrbutes, isOverwrite);
        }

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "guid is null/empty");
        }

        if (MapUtils.isEmpty(businessAttrbutes)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "businessAttributes is null/empty");
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        String                           typeName                            = getTypeName(entityVertex);
        AtlasEntityType                  entityType                          = typeRegistry.getEntityTypeByName(typeName);
        Map<String, Map<String, AtlasBusinessAttribute>> entityBMs           = entityType.getBusinessAttributes();
        Map<String, Map<String, Object>> finalBMAttributes                   = new HashMap<>();

        MetricRecorder metric = RequestContext.get().startMetricRecord("preProcessDisplayNames");
        for (Map.Entry<String, Map<String, Object>> bm : businessAttrbutes.entrySet()) {
            Map<String, AtlasBusinessAttribute> enitytBM = entityBMs.get(bm.getKey());

            AtlasBusinessMetadataType bmType = null;
            if (enitytBM == null) {
                //search BM type by displayName
                try {
                    bmType = typeRegistry.getBusinessMetadataTypeByDisplayName(bm.getKey());
                } catch (NoSuchElementException nse) {
                    throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, String.format("No BM type found with displayName %s", bm.getKey()));
                }
            } else {
                bmType = typeRegistry.getBusinessMetadataTypeByName(bm.getKey());
            }

            //check & validate attributes
            Map <String, Object> attributes = new HashMap<>();
            for (Map.Entry<String, Object> incomingAttrs : bm.getValue().entrySet()) {
                AtlasAttribute atlasAttribute = bmType.getAllAttributes().get(incomingAttrs.getKey());

                if (atlasAttribute == null) { //attribute is having displayName find attribute name
                    try {
                        atlasAttribute = bmType.getAllAttributes().values().stream().filter(x -> incomingAttrs.getKey().equals(x.getAttributeDef().getDisplayName())).findFirst().get();
                    } catch (NoSuchElementException nse) {
                        String message = String.format("No attribute found with displayName %s for BM attribute %s",
                                incomingAttrs.getKey(), bmType.getTypeName());
                        throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, message);
                    }
                }
                attributes.put(atlasAttribute.getAttributeDef().getName(), bm.getValue().get(incomingAttrs.getKey()));
            }
            finalBMAttributes.put(bmType.getTypeName(), attributes);
        }
        RequestContext.get().endMetricRecord(metric);

        addOrUpdateBusinessAttributes(guid, finalBMAttributes, isOverwrite);
    }

    @Override
    @GraphTransaction
    public void addOrUpdateBusinessAttributes(String guid, Map<String, Map<String, Object>> businessAttrbutes, boolean isOverwrite) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {

            LOG.debug("==> addOrUpdateBusinessAttributes(guid={}, businessAttributes={}, isOverwrite={})", guid, businessAttrbutes, isOverwrite);
        }

        entityGraphMapper.addOrUpdateBusinessAttributes(guid, businessAttrbutes, isOverwrite);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== addOrUpdateBusinessAttributes(guid={}, businessAttributes={}, isOverwrite={})", guid, businessAttrbutes, isOverwrite);
        }
    }

    @Override
    @GraphTransaction
    public void removeBusinessAttributes(String guid, Map<String, Map<String, Object>> businessAttributes) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> removeBusinessAttributes(guid={}, businessAttributes={})", guid, businessAttributes);
        }

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "guid is null/empty");
        }

        if (MapUtils.isEmpty(businessAttributes)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "businessAttributes is null/empty");
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        String                          typeName       = getTypeName(entityVertex);
        AtlasEntityType                 entityType     = typeRegistry.getEntityTypeByName(typeName);

        entityGraphMapper.removeBusinessAttributes(entityVertex, entityType, businessAttributes);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== removeBusinessAttributes(guid={}, businessAttributes={})", guid, businessAttributes);
        }
    }

    @Override
    @GraphTransaction
    public void setLabels(String guid, Set<String> labels) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> setLabels()");
        }

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "guid is null/empty");
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        validateLabels(labels);

        AtlasEntityHeader entityHeader  = entityRetriever.toAtlasEntityHeaderWithClassifications(entityVertex);
        Set<String>       addedLabels   = Collections.emptySet();
        Set<String>       removedLabels = Collections.emptySet();

        if (CollectionUtils.isEmpty(entityHeader.getLabels())) {
            addedLabels = labels;
        } else if (CollectionUtils.isEmpty(labels)) {
            removedLabels = entityHeader.getLabels();
        } else {
            addedLabels   = new HashSet<String>(CollectionUtils.subtract(labels, entityHeader.getLabels()));
            removedLabels = new HashSet<String>(CollectionUtils.subtract(entityHeader.getLabels(), labels));
        }

        if (addedLabels != null) {
            AtlasEntityAccessRequestBuilder requestBuilder = new AtlasEntityAccessRequestBuilder(typeRegistry, AtlasPrivilege.ENTITY_ADD_LABEL, entityHeader);

            for (String label : addedLabels) {
                requestBuilder.setLabel(label);

                AtlasAuthorizationUtils.verifyAccess(requestBuilder.build(), "add label: guid=", guid, ", label=", label);
            }
        }

        if (removedLabels != null) {
            AtlasEntityAccessRequestBuilder requestBuilder = new AtlasEntityAccessRequestBuilder(typeRegistry, AtlasPrivilege.ENTITY_REMOVE_LABEL, entityHeader);

            for (String label : removedLabels) {
                requestBuilder.setLabel(label);

                AtlasAuthorizationUtils.verifyAccess(requestBuilder.build(), "remove label: guid=", guid, ", label=", label);
            }
        }

        entityGraphMapper.setLabels(entityVertex, labels);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== setLabels()");
        }
    }

    @Override
    @GraphTransaction
    public void removeLabels(String guid, Set<String> labels) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> removeLabels()");
        }

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "guid is null/empty");
        }

        if (CollectionUtils.isEmpty(labels)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "labels is null/empty");
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        AtlasEntityHeader               entityHeader   = entityRetriever.toAtlasEntityHeaderWithClassifications(entityVertex);
        AtlasEntityAccessRequestBuilder requestBuilder = new AtlasEntityAccessRequestBuilder(typeRegistry, AtlasPrivilege.ENTITY_REMOVE_LABEL, entityHeader);

        for (String label : labels) {
            requestBuilder.setLabel(label);

            AtlasAuthorizationUtils.verifyAccess(requestBuilder.build(), "remove label: guid=", guid, ", label=", label);
        }

        validateLabels(labels);

        entityGraphMapper.removeLabels(entityVertex, labels);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== removeLabels()");
        }
    }

    @Override
    @GraphTransaction
    public void addLabels(String guid, Set<String> labels) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> addLabels()");
        }

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "guid is null/empty");
        }

        if (CollectionUtils.isEmpty(labels)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "labels is null/empty");
        }

        AtlasVertex entityVertex = AtlasGraphUtilsV2.findByGuid(graph, guid);

        if (entityVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        AtlasEntityHeader               entityHeader   = entityRetriever.toAtlasEntityHeaderWithClassifications(entityVertex);
        AtlasEntityAccessRequestBuilder requestBuilder = new AtlasEntityAccessRequestBuilder(typeRegistry, AtlasPrivilege.ENTITY_ADD_LABEL, entityHeader);

        for (String label : labels) {
            requestBuilder.setLabel(label);

            AtlasAuthorizationUtils.verifyAccess(requestBuilder.build(), "add/update label: guid=", guid, ", label=", label);
        }

        validateLabels(labels);

        entityGraphMapper.addLabels(entityVertex, labels);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== addLabels()");
        }
    }

    private EntityMutationResponse createOrUpdate(EntityStream entityStream, boolean isPartialUpdate, boolean replaceClassifications, boolean replaceBusinessAttributes, boolean isOverwriteBusinessAttribute) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> createOrUpdate()");
        }

        if (entityStream == null || !entityStream.hasNext()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "no entities to create/update.");
        }

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "createOrUpdate()");
        }

        MetricRecorder metric = RequestContext.get().startMetricRecord("createOrUpdate");

        try {
            final EntityMutationContext context = preCreateOrUpdate(entityStream, entityGraphMapper, isPartialUpdate);

            // Check if authorized to create entities
            if (!RequestContext.get().isImportInProgress() && !RequestContext.get().isSkipAuthorizationCheck()) {
                for (AtlasEntity entity : context.getCreatedEntities()) {
                    if (!PreProcessor.skipInitialAuthCheckTypes.contains(entity.getTypeName())) {
                        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(entity)),
                                "create entity: type=", entity.getTypeName());
                    }
                }
            }
            // for existing entities, skip update if incoming entity doesn't have any change
            if (CollectionUtils.isNotEmpty(context.getUpdatedEntities())) {
                MetricRecorder checkForUnchangedEntities = RequestContext.get().startMetricRecord("checkForUnchangedEntities");

                List<AtlasEntity>     entitiesToSkipUpdate = new ArrayList<>();
                AtlasEntityComparator entityComparator     = new AtlasEntityComparator(typeRegistry, entityRetriever, context.getGuidAssignments(), !replaceClassifications, !replaceBusinessAttributes);
                RequestContext        reqContext           = RequestContext.get();

                for (AtlasEntity entity : context.getUpdatedEntities()) {
                    if (entity.getStatus() == AtlasEntity.Status.DELETED) {// entity status could be updated during import
                        continue;
                    }

                    AtlasVertex           storedVertex = context.getVertex(entity.getGuid());
                    AtlasEntityDiffResult diffResult   = entityComparator.getDiffResult(entity, storedVertex, !storeDifferentialAudits);

                    if (diffResult.hasDifference()) {
                        if (storeDifferentialAudits) {
                            diffResult.getDiffEntity().setGuid(entity.getGuid());
                            reqContext.cacheDifferentialEntity(diffResult.getDiffEntity());
                        }

                        if (diffResult.hasDifferenceOnlyInCustomAttributes()) {
                            reqContext.recordEntityWithCustomAttributeUpdate(entity.getGuid());
                        }

                        if (diffResult.hasDifferenceOnlyInBusinessAttributes()) {
                            reqContext.recordEntityWithBusinessAttributeUpdate(entity.getGuid());
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("skipping unchanged entity: {}", entity);
                        }

                        entitiesToSkipUpdate.add(entity);
                        reqContext.recordEntityToSkip(entity.getGuid());
                    }
                }

                if (entitiesToSkipUpdate.size() > 0) {
                    // remove entitiesToSkipUpdate from EntityMutationContext
                    context.getUpdatedEntities().removeAll(entitiesToSkipUpdate);
                }

                // Check if authorized to update entities
                if (!reqContext.isImportInProgress()) {
                    for (AtlasEntity entity : context.getUpdatedEntities()) {
                        AtlasEntityHeader entityHeaderWithClassifications = entityRetriever.toAtlasEntityHeaderWithClassifications(entity.getGuid());
                        AtlasEntityHeader entityHeader = new AtlasEntityHeader(entity);

                        if(CollectionUtils.isNotEmpty(entityHeaderWithClassifications.getClassifications())) {
                            entityHeader.setClassifications(entityHeaderWithClassifications.getClassifications());
                        }

                        AtlasEntity diffEntity = reqContext.getDifferentialEntity(entity.getGuid());
                        boolean skipAuthBaseConditions = diffEntity != null && MapUtils.isEmpty(diffEntity.getCustomAttributes()) && MapUtils.isEmpty(diffEntity.getBusinessAttributes()) && CollectionUtils.isEmpty(diffEntity.getClassifications()) && CollectionUtils.isEmpty(diffEntity.getLabels());
                        boolean skipAuthMeaningsUpdate = diffEntity != null && MapUtils.isNotEmpty(diffEntity.getRelationshipAttributes()) && diffEntity.getRelationshipAttributes().containsKey("meanings") && diffEntity.getRelationshipAttributes().size() == 1 && MapUtils.isEmpty(diffEntity.getAttributes());
                        boolean skipAuthStarredDetailsUpdate = diffEntity != null && MapUtils.isEmpty(diffEntity.getRelationshipAttributes()) && MapUtils.isNotEmpty(diffEntity.getAttributes()) && diffEntity.getAttributes().size() == 3 && diffEntity.getAttributes().containsKey(ATTR_STARRED_BY) && diffEntity.getAttributes().containsKey(ATTR_STARRED_COUNT) && diffEntity.getAttributes().containsKey(ATTR_STARRED_DETAILS_LIST);
                        if (skipAuthBaseConditions && (skipAuthMeaningsUpdate || skipAuthStarredDetailsUpdate)) {
                            //do nothing, only diff is relationshipAttributes.meanings or starred, allow update
                        } else {
                            AtlasAuthorizationUtils.verifyUpdateEntityAccess(typeRegistry, entityHeader,"update entity: type=" + entity.getTypeName());
                        }
                    }
                }

                reqContext.endMetricRecord(checkForUnchangedEntities);
            }

            executePreProcessor(context);

            // Updating hierarchy after preprocessor is executed so that qualifiedName update during preprocessor is considered
            for (AtlasEntity entity : context.getCreatedEntities()) {
                createQualifiedNameHierarchyField(entity, context.getVertex(entity.getGuid()));
            }

            for (Map.Entry<String, AtlasEntity> entry : RequestContext.get().getDifferentialEntitiesMap().entrySet()) {
                if (entry.getValue().hasAttribute(QUALIFIED_NAME)) {
                    createQualifiedNameHierarchyField(entry.getValue(), context.getVertex(entry.getKey()));
                }
            }


            EntityMutationResponse ret = entityGraphMapper.mapAttributesAndClassifications(context, isPartialUpdate,
                    replaceClassifications, replaceBusinessAttributes, isOverwriteBusinessAttribute);

            ret.setGuidAssignments(context.getGuidAssignments());


            // Notify the change listeners
            entityChangeNotifier.onEntitiesMutated(ret, RequestContext.get().isImportInProgress());
            atlasRelationshipStore.onRelationshipsMutated(RequestContext.get().getRelationshipMutationMap());
            if (LOG.isDebugEnabled()) {
                LOG.debug("<== createOrUpdate()");
            }

            return ret;
        } finally {
            RequestContext.get().endMetricRecord(metric);

            AtlasPerfTracer.log(perf);
        }
    }

    private void executePreProcessor(EntityMutationContext context) throws AtlasBaseException {
        AtlasEntityType entityType;
        List<PreProcessor> preProcessors;

        List<AtlasEntity> copyOfCreated = new ArrayList<>(context.getCreatedEntities());
        for (AtlasEntity entity : copyOfCreated) {
            entityType = context.getType(entity.getGuid());
            preProcessors = getPreProcessor(entityType.getTypeName());
            for(PreProcessor processor : preProcessors){
                processor.processAttributes(entity, context, CREATE);
            }
        }

        List<AtlasEntity> copyOfUpdated = new ArrayList<>(context.getUpdatedEntities());
        for (AtlasEntity entity: copyOfUpdated) {
            entityType = context.getType(entity.getGuid());
            preProcessors = getPreProcessor(entityType.getTypeName());
            for(PreProcessor processor : preProcessors){
                processor.processAttributes(entity, context, UPDATE);
            }
        }
    }

    private EntityMutationContext preCreateOrUpdate(EntityStream entityStream, EntityGraphMapper entityGraphMapper, boolean isPartialUpdate) throws AtlasBaseException {
        MetricRecorder metric = RequestContext.get().startMetricRecord("preCreateOrUpdate");

        EntityGraphDiscovery        graphDiscoverer  = new AtlasEntityGraphDiscoveryV2(graph, typeRegistry, entityStream, entityGraphMapper);
        EntityGraphDiscoveryContext discoveryContext = graphDiscoverer.discoverEntities();
        EntityMutationContext       context          = new EntityMutationContext(discoveryContext);
        RequestContext              requestContext   = RequestContext.get();

        Map<String, String> referencedGuids = discoveryContext.getReferencedGuids();
        for (Map.Entry<String, String> element : referencedGuids.entrySet()) {
            String guid = element.getKey();
            AtlasEntity entity = entityStream.getByGuid(guid);

            if (entity != null) { // entity would be null if guid is not in the stream but referenced by an entity in the stream
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

                if (entityType == null) {
                    throw new AtlasBaseException(element.getValue(), AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), entity.getTypeName());
                }

                compactAttributes(entity, entityType);
                flushAutoUpdateAttributes(entity, entityType);

                AtlasVertex vertex = getResolvedEntityVertex(discoveryContext, entity);

                autoUpdateStarredDetailsAttributes(entity, vertex);

                try {
                    if (vertex != null) {
                        if (!isPartialUpdate) {
                            graphDiscoverer.validateAndNormalize(entity);

                            // change entity 'isInComplete' to 'false' during full update
                            if (isEntityIncomplete(vertex)) {
                                vertex.removeProperty(IS_INCOMPLETE_PROPERTY_KEY);

                                entity.setIsIncomplete(FALSE);
                            }
                        } else {
                            graphDiscoverer.validateAndNormalizeForUpdate(entity);
                        }

                        String guidVertex = AtlasGraphUtilsV2.getIdFromVertex(vertex);

                        if (!StringUtils.equals(guidVertex, guid)) { // if entity was found by unique attribute
                            entity.setGuid(guidVertex);

                            requestContext.recordEntityGuidUpdate(entity, guid);
                        }

                        context.addUpdated(guid, entity, entityType, vertex);

                    } else {
                        graphDiscoverer.validateAndNormalize(entity);

                        //Create vertices which do not exist in the repository
                        if (RequestContext.get().isImportInProgress() && AtlasTypeUtil.isAssignedGuid(entity.getGuid())) {
                            vertex = entityGraphMapper.createVertexWithGuid(entity, entity.getGuid());
                        } else {
                            vertex = entityGraphMapper.createVertex(entity);
                        }

                        discoveryContext.addResolvedGuid(guid, vertex);

                        discoveryContext.addResolvedIdByUniqAttribs(getAtlasObjectId(entity), vertex);

                        String generatedGuid = AtlasGraphUtilsV2.getIdFromVertex(vertex);

                        entity.setGuid(generatedGuid);

                        requestContext.recordEntityGuidUpdate(entity, guid);

                        context.addCreated(guid, entity, entityType, vertex);
                    }

                } catch (AtlasBaseException exception) {
                    exception.setEntityGuid(element.getValue());
                    throw exception;
                }


                String entityStateValue = (String) entity.getAttribute(STATE_PROPERTY_KEY);
                String entityStatusValue = entity.getStatus() != null ? entity.getStatus().toString() : null;
                String entityActiveKey = Status.ACTIVE.toString();
                boolean isRestoreRequested = ((StringUtils.isNotEmpty(entityStateValue) && entityStateValue.equals(entityActiveKey)) || (StringUtils.isNotEmpty(entityStatusValue) && entityStatusValue.equals(entityActiveKey)));

                if (discoveryContext.isAppendRelationshipAttributeVisited() && MapUtils.isNotEmpty(entity.getAppendRelationshipAttributes())) {
                    context.setUpdatedWithRelationshipAttributes(entity);
                }

                if (discoveryContext.isRemoveRelationshipAttributeVisited() && MapUtils.isNotEmpty(entity.getRemoveRelationshipAttributes())) {
                    context.setUpdatedWithRemoveRelationshipAttributes(entity);
                }

                if (isRestoreRequested) {
                    Status currStatus = AtlasGraphUtilsV2.getState(vertex);
                    if (currStatus == Status.DELETED) {
                        context.addEntityToRestore(vertex);
                    }
                }

                // during import, update the system attributes
                if (RequestContext.get().isImportInProgress()) {
                    Status newStatus = entity.getStatus();

                    if (newStatus != null) {
                        Status currStatus = AtlasGraphUtilsV2.getState(vertex);

                        if (currStatus == Status.ACTIVE && newStatus == Status.DELETED) {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("entity-delete via import - guid={}", guid);
                            }

                            context.addEntityToDelete(vertex);
                        } else if (currStatus == Status.DELETED && newStatus == Status.ACTIVE) {
                            LOG.warn("Import is attempting to activate deleted entity (guid={}).", guid);
                            entityGraphMapper.importActivateEntity(vertex, entity);
                            context.addCreated(guid, entity, entityType, vertex);
                        }
                    }

                    entityGraphMapper.updateSystemAttributes(vertex, entity);
                }
            }
        }

        RequestContext.get().endMetricRecord(metric);

        return context;
    }

    private void autoUpdateStarredDetailsAttributes(AtlasEntity entity, AtlasVertex vertex) {

        MetricRecorder metric = RequestContext.get().startMetricRecord("autoUpdateStarredDetailsAttributes");

        Boolean starEntityForUser = entity.getStarred();

        if (starEntityForUser != null) {

            long requestTime = RequestContext.get().getRequestTime();
            String requestUser = RequestContext.get().getUser();

            Set<String> starredBy = new HashSet<>();
            Set<AtlasStruct> starredDetailsList = new HashSet<>();
            int starredCount = 0;

            if (vertex != null) {
                Set<String> vertexStarredBy = vertex.getMultiValuedSetProperty(ATTR_STARRED_BY, String.class);
                if (vertexStarredBy != null) {
                    starredBy = vertexStarredBy;
                }

                Iterable<AtlasEdge> starredDetailsEdges = vertex.getEdges(AtlasEdgeDirection.OUT, "__" + ATTR_STARRED_DETAILS_LIST);
                for (AtlasEdge starredDetailsEdge : starredDetailsEdges) {
                    AtlasVertex starredDetailsVertex = starredDetailsEdge.getInVertex();
                    String assetStarredBy = starredDetailsVertex.getProperty(ATTR_ASSET_STARRED_BY, String.class);
                    Long assetStarredAt = starredDetailsVertex.getProperty(ATTR_ASSET_STARRED_AT, Long.class);
                    AtlasStruct starredDetails = getStarredDetailsStruct(assetStarredBy, assetStarredAt);
                    starredDetailsList.add(starredDetails);
                }

                starredCount = starredBy.size();
            }

            if (starEntityForUser) {
                addUserToStarredAttributes(requestUser, requestTime, starredBy, starredDetailsList);
            } else {
                removeUserFromStarredAttributes(requestUser, starredBy, starredDetailsList);
            }

            // Update entity attributes
            if (starredBy.size() != starredCount) {
                entity.setAttribute(ATTR_STARRED_BY, starredBy);
                entity.setAttribute(ATTR_STARRED_DETAILS_LIST, starredDetailsList);
                entity.setAttribute(ATTR_STARRED_COUNT, starredBy.size());
            }

        }

        RequestContext.get().endMetricRecord(metric);
    }

    private void addUserToStarredAttributes(String requestUser, long requestTime, Set<String> starredBy, Set<AtlasStruct> starredDetailsList) {
        //Check and update starredBy Attribute
        if (!starredBy.contains(requestUser)){
            starredBy.add(requestUser);
        }

        //Check and update starredDetailsList Attribute
        boolean isStarredDetailsListUpdated = false;
        for (AtlasStruct starredDetails : starredDetailsList) {
            String assetStarredBy = (String) starredDetails.getAttribute(ATTR_ASSET_STARRED_BY);
            if (assetStarredBy.equals(requestUser)) {
                starredDetails.setAttribute(ATTR_ASSET_STARRED_AT, requestTime);
                isStarredDetailsListUpdated = true;
                break;
            }
        }
        if (!isStarredDetailsListUpdated) {
            AtlasStruct starredDetails = getStarredDetailsStruct(requestUser, requestTime);
            starredDetailsList.add(starredDetails);
        }
    }

    private void removeUserFromStarredAttributes(String requestUser, Set<String> starredBy, Set<AtlasStruct> starredDetailsList) {
        //Check and update starredBy Attribute
        if (starredBy.contains(requestUser)){
            starredBy.remove(requestUser);
        }

        for (AtlasStruct starredDetails : starredDetailsList) {
            String assetStarredBy = (String) starredDetails.getAttribute(ATTR_ASSET_STARRED_BY);
            if (assetStarredBy.equals(requestUser)) {
                starredDetailsList.remove(starredDetails);
                break;
            }
        }
    }

    private AtlasStruct getStarredDetailsStruct(String assetStarredBy, long assetStarredAt) {
        AtlasStruct starredDetails = new AtlasStruct();
        starredDetails.setTypeName(STRUCT_STARRED_DETAILS);
        starredDetails.setAttribute(ATTR_ASSET_STARRED_BY, assetStarredBy);
        starredDetails.setAttribute(ATTR_ASSET_STARRED_AT, assetStarredAt);
        return starredDetails;
    }

    private void createQualifiedNameHierarchyField(AtlasEntity entity, AtlasVertex vertex) {
        MetricRecorder metric = RequestContext.get().startMetricRecord("createQualifiedNameHierarchyField");
        boolean isDataMeshType = entity.getTypeName().equals(DATA_PRODUCT_ENTITY_TYPE) || entity.getTypeName().equals(DATA_DOMAIN_ENTITY_TYPE);
        int qualifiedNameOffset = isDataMeshType ? 2 : 1;
        try {
            if (vertex == null) {
                vertex = AtlasGraphUtilsV2.findByGuid(graph, entity.getGuid());
            }
            if (entity.hasAttribute(QUALIFIED_NAME)) {
                String qualifiedName = (String) entity.getAttribute(QUALIFIED_NAME);
                if (StringUtils.isNotEmpty(qualifiedName)) {
                    vertex.removeProperty(QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY);
                    String[] parts = qualifiedName.split("/");
                    StringBuilder currentPath = new StringBuilder();

                    for (int i = 0; i < parts.length; i++) {
                        String part = parts[i];
                        if (StringUtils.isNotEmpty(part)) {
                            if (i > 0) {
                                currentPath.append("/");
                            }
                            currentPath.append(part);
                            // i>1 reason: we don't want to add the first part of the qualifiedName as it is the entity name
                            // Example qualifiedName : default/snowflake/123/db_name we only want `default/snowflake/123` and `default/snowflake/123/db_name`
                            if (i > qualifiedNameOffset) {
                                if (isDataMeshType && (part.equals("domain") || part.equals("product"))) {
                                    continue;
                                }
                                AtlasGraphUtilsV2.addEncodedProperty(vertex, QUALIFIED_NAME_HIERARCHY_PROPERTY_KEY, currentPath.toString());
                            }
                        }
                    }
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }


    public List<PreProcessor> getPreProcessor(String typeName) {
        List<PreProcessor> preProcessors = new ArrayList<>();

        switch (typeName) {
            case ATLAS_GLOSSARY_ENTITY_TYPE:
                preProcessors.add(new GlossaryPreProcessor(typeRegistry, entityRetriever, graph));
                break;

            case ATLAS_GLOSSARY_TERM_ENTITY_TYPE:
                preProcessors.add(new TermPreProcessor(typeRegistry, entityRetriever, graph, taskManagement));
                break;

            case ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE:
                preProcessors.add(new CategoryPreProcessor(typeRegistry, entityRetriever, graph, taskManagement, entityGraphMapper));
                break;

            case DATA_DOMAIN_ENTITY_TYPE:
                preProcessors.add(new DataDomainPreProcessor(typeRegistry, entityRetriever, graph));
                break;

            case DATA_PRODUCT_ENTITY_TYPE:
                preProcessors.add(new DataProductPreProcessor(typeRegistry, entityRetriever, graph, this));
                break;

            case QUERY_ENTITY_TYPE:
                preProcessors.add(new QueryPreProcessor(typeRegistry, entityRetriever));
                break;

            case QUERY_FOLDER_ENTITY_TYPE:
                preProcessors.add(new QueryFolderPreProcessor(typeRegistry, entityRetriever));
                break;

            case QUERY_COLLECTION_ENTITY_TYPE:
                preProcessors.add(new QueryCollectionPreProcessor(typeRegistry, discovery, entityRetriever, featureFlagStore, this));
                break;

            case PERSONA_ENTITY_TYPE:
                preProcessors.add(new PersonaPreProcessor(graph, typeRegistry, entityRetriever, this));
                break;

            case PURPOSE_ENTITY_TYPE:
                preProcessors.add(new PurposePreProcessor(graph, typeRegistry, entityRetriever, this));
                break;

            case POLICY_ENTITY_TYPE:
                preProcessors.add(new AuthPolicyPreProcessor(graph, typeRegistry, entityRetriever));
                break;

            case STAKEHOLDER_ENTITY_TYPE:
                preProcessors.add(new StakeholderPreProcessor(graph, typeRegistry, entityRetriever, this));
                break;

            case CONNECTION_ENTITY_TYPE:
                preProcessors.add(new ConnectionPreProcessor(graph, discovery, entityRetriever, featureFlagStore, deleteDelegate, this));
                break;

            case LINK_ENTITY_TYPE:
                preProcessors.add(new LinkPreProcessor(typeRegistry, entityRetriever));
                break;

            case README_ENTITY_TYPE:
                preProcessors.add(new ReadmePreProcessor(typeRegistry, entityRetriever));
                break;

            case CONTRACT_ENTITY_TYPE:
                preProcessors.add(new ContractPreProcessor(graph, typeRegistry, entityRetriever, storeDifferentialAudits, discovery));
                break;

            case STAKEHOLDER_TITLE_ENTITY_TYPE:
                preProcessors.add(new StakeholderTitlePreProcessor(graph, typeRegistry, entityRetriever));
                break;
        }

        //  The default global pre-processor for all AssetTypes
        preProcessors.add(new AssetPreProcessor(typeRegistry, entityRetriever, graph));

        return preProcessors;
    }

    private AtlasVertex getResolvedEntityVertex(EntityGraphDiscoveryContext context, AtlasEntity entity) throws AtlasBaseException {
        AtlasObjectId objectId = getAtlasObjectId(entity);
        AtlasVertex   ret      = context.getResolvedEntityVertex(entity.getGuid());

        if (ret != null) {
            context.addResolvedIdByUniqAttribs(objectId, ret);
            if (entity.getLabels() != null) {
                entityGraphMapper.setLabels(ret, entity.getLabels());
            }
        } else {
            ret = context.getResolvedEntityVertex(objectId);

            if (ret != null) {
                context.addResolvedGuid(entity.getGuid(), ret);
            }
        }

        return ret;
    }

    private AtlasObjectId getAtlasObjectId(AtlasEntity entity) {
        AtlasObjectId ret = entityRetriever.toAtlasObjectId(entity);

        if (ret != null && !RequestContext.get().isImportInProgress() && MapUtils.isNotEmpty(ret.getUniqueAttributes())) {
            // if uniqueAttributes is not empty, reset guid to null.
            ret.setGuid(null);
        }

        return ret;
    }

    private EntityMutationResponse deleteVertices(Collection<AtlasVertex> deletionCandidates) throws AtlasBaseException {
        EntityMutationResponse response = new EntityMutationResponse();
        try {
            RequestContext req = RequestContext.get();

            Collection<AtlasVertex> categories = new ArrayList<>();
            Collection<AtlasVertex> others = new ArrayList<>();

            MetricRecorder metric = RequestContext.get().startMetricRecord("filterCategoryVertices");
            for (AtlasVertex vertex : deletionCandidates) {
                updateModificationMetadata(vertex);

                String typeName = getTypeName(vertex);

                List<PreProcessor> preProcessors = getPreProcessor(typeName);
                for(PreProcessor processor : preProcessors){
                    processor.processDelete(vertex);
                }

                if (ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE.equals(typeName)) {
                    categories.add(vertex);
                } else {
                    others.add(vertex);
                }
            }
            RequestContext.get().endMetricRecord(metric);

            if (CollectionUtils.isNotEmpty(categories)) {
                entityGraphMapper.removeAttrForCategoryDelete(categories);
                deleteDelegate.getHandler(DeleteType.HARD).deleteEntities(categories);
            }

            if (CollectionUtils.isNotEmpty(others)) {

                deleteDelegate.getHandler().removeHasLineageOnDelete(others);
                deleteDelegate.getHandler().deleteEntities(others);
            }

            for (AtlasEntityHeader entity : req.getDeletedEntities()) {
                String handler;
                if (ATLAS_GLOSSARY_CATEGORY_ENTITY_TYPE.equals(entity.getTypeName())) {
                    handler = req.getDeleteType().equals(DeleteType.PURGE) ?
                            DeleteType.PURGE.name() : DeleteType.HARD.name();
                } else {
                    handler = RequestContext.get().getDeleteType().name();
                }
                entity.setDeleteHandler(handler);
                entity.setStatus(Status.DELETED);
                entity.setUpdatedBy(RequestContext.get().getUser());
                response.addEntity(DELETE, entity);
            }

            for (AtlasEntityHeader entity : req.getUpdatedEntities()) {
                response.addEntity(UPDATE, entity);
            }
        } catch (Exception e) {
            LOG.error("Delete vertices request failed", e);
            throw new AtlasBaseException(e);
        }

        return response;
    }

    private EntityMutationResponse restoreVertices(Collection<AtlasVertex> restoreCandidates) throws AtlasBaseException {
        EntityMutationResponse response = new EntityMutationResponse();
        RequestContext         req      = RequestContext.get();

        restoreHandlerV1.restoreEntities(restoreCandidates);

        for (AtlasEntityHeader entity : req.getRestoredEntities()) {
            response.addEntity(UPDATE, entity);
        }

        return response;
    }

    private EntityMutationResponse purgeVertices(Collection<AtlasVertex> purgeCandidates) throws AtlasBaseException {
        EntityMutationResponse response = new EntityMutationResponse();
        RequestContext         req      = RequestContext.get();

        req.setDeleteType(DeleteType.HARD);
        req.setPurgeRequested(true);
        deleteDelegate.getHandler().deleteEntities(purgeCandidates); // this will update req with list of purged entities

        for (AtlasEntityHeader entity : req.getDeletedEntities()) {
            response.addEntity(PURGE, entity);
        }

        return response;
    }

    private void validateAndNormalize(AtlasClassification classification) throws AtlasBaseException {
        AtlasClassificationType type = typeRegistry.getClassificationTypeByName(classification.getTypeName());

        if (type == null) {
            throw new AtlasBaseException(AtlasErrorCode.CLASSIFICATION_NOT_FOUND, classification.getTypeName());
        }

        List<String> messages = new ArrayList<>();

        type.validateValue(classification, classification.getTypeName(), messages);

        if (!messages.isEmpty()) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, messages);
        }

        type.getNormalizedValue(classification);
    }

    /**
     * Validate if classification is not already associated with the entities
     *
     * @param guid            unique entity id
     * @param classifications list of classifications to be associated
     */
    private void validateEntityAssociations(String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        List<String>    entityClassifications = getClassificationNames(guid);
        String          entityTypeName        = AtlasGraphUtilsV2.getTypeNameFromGuid(graph, guid);
        AtlasEntityType entityType            = typeRegistry.getEntityTypeByName(entityTypeName);
        Set<String> processedTagTypeNames = new HashSet<>();

        List <AtlasClassification> copyList = new ArrayList<>(classifications);
        for (AtlasClassification classification : copyList) {

            if (processedTagTypeNames.contains(classification.getTypeName())){
                classifications.remove(classification);
            } else {
                String newClassification = classification.getTypeName();
                processedTagTypeNames.add(newClassification);

                if (CollectionUtils.isNotEmpty(entityClassifications) && entityClassifications.contains(newClassification)) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "entity: " + guid +
                            ", already associated with classification: " + newClassification);
                }

                // for each classification, check whether there are entities it should be restricted to
                AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(newClassification);

                if (!classificationType.canApplyToEntityType(entityType)) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_ENTITY_FOR_CLASSIFICATION, guid, entityTypeName, newClassification);
                }
            }
        }
    }

    private List<String> getClassificationNames(String guid) throws AtlasBaseException {
        List<String>              ret             = null;
        List<AtlasClassification> classifications = retrieveClassifications(guid);

        if (CollectionUtils.isNotEmpty(classifications)) {
            ret = new ArrayList<>();

            for (AtlasClassification classification : classifications) {
                String entityGuid = classification.getEntityGuid();

                if (StringUtils.isEmpty(entityGuid) || StringUtils.equalsIgnoreCase(guid, entityGuid)) {
                    ret.add(classification.getTypeName());
                }
            }
        }

        return ret;
    }

    // move/remove relationship-attributes present in 'attributes'
    private void compactAttributes(AtlasEntity entity, AtlasEntityType entityType) {
        if (entity != null) {
            for (String attrName : entityType.getRelationshipAttributes().keySet()) {
                if (entity.hasAttribute(attrName)) { // relationship attribute is present in 'attributes'
                    Object attrValue = entity.removeAttribute(attrName);

                    if (attrValue != null) {
                        // if the attribute doesn't exist in relationshipAttributes, add it
                        Object relationshipAttrValue = entity.getRelationshipAttribute(attrName);

                        if (relationshipAttrValue == null) {
                            entity.setRelationshipAttribute(attrName, attrValue);

                            if (LOG.isDebugEnabled()) {
                                LOG.debug("moved attribute {}.{} from attributes to relationshipAttributes", entityType.getTypeName(), attrName);
                            }
                        } else {
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("attribute {}.{} is present in attributes and relationshipAttributes. Removed from attributes", entityType.getTypeName(), attrName);
                            }
                        }
                    }
                }
            }
        }
    }

    private void flushAutoUpdateAttributes(AtlasEntity entity, AtlasEntityType entityType) {
        if (entityType.getAllAttributes() != null) {
            Set<String> flushAttributes = new HashSet<>();

            for (String attrName : entityType.getAllAttributes().keySet()) {
                AtlasAttribute atlasAttribute = entityType.getAttribute(attrName);
                HashMap<String, ArrayList> autoUpdateAttributes = atlasAttribute.getAttributeDef().getAutoUpdateAttributes();
                if (MapUtils.isNotEmpty(autoUpdateAttributes)) {
                    autoUpdateAttributes.values()
                            .stream()
                            .flatMap(List<String>::stream)
                            .forEach(flushAttributes::add);
                }
            }

//            for (String attrName : entityType.getAllAttributes().keySet()) {
//                if (ATTR_STARRED_BY.equals(attrName) || ATTR_STARRED_COUNT.equals(attrName) || ATTR_STARRED_DETAILS_LIST.equals(attrName)) {
//                    flushAttributes.add(attrName);
//                }
//            }

            flushAttributes.forEach(entity::removeAttribute);
        }
    }

    @Override
    @GraphTransaction
    public BulkImportResponse bulkCreateOrUpdateBusinessAttributes(InputStream inputStream, String fileName) throws AtlasBaseException {
        BulkImportResponse ret = new BulkImportResponse();

        if (StringUtils.isBlank(fileName)) {
            throw new AtlasBaseException(AtlasErrorCode.FILE_NAME_NOT_FOUND, fileName);
        }

        List<String[]>           fileData              = FileUtils.readFileData(fileName, inputStream);
        Map<String, AtlasEntity> attributesToAssociate = getBusinessMetadataDefList(fileData, ret);

        for (AtlasEntity entity : attributesToAssociate.values()) {
            Map<String, Map<String, Object>> businessAttributes = entity.getBusinessAttributes();
            String                           guid               = entity.getGuid();

            try {
                addOrUpdateBusinessAttributes(guid, businessAttributes, true);

                ret.addToSuccessImportInfoList(new ImportInfo(guid, businessAttributes.toString()));
            } catch (Exception e) {
                LOG.error("Error occurred while updating BusinessMetadata Attributes for Entity " + guid);

                ret.addToFailedImportInfoList(new ImportInfo(guid, businessAttributes.toString(), FAILED, e.getMessage()));
            }
        }

        return ret;
    }

    @Override
    public List<AtlasAccessorResponse> getAccessors(List<AtlasAccessorRequest> atlasAccessorRequestList) throws AtlasBaseException {
        List<AtlasAccessorResponse> ret = new ArrayList<>();

        for (AtlasAccessorRequest accessorRequest : atlasAccessorRequestList) {
            try {
                AtlasAccessorResponse result = null;
                AtlasPrivilege action = AtlasPrivilege.valueOf(accessorRequest.getAction());;

                switch (action) {
                    case ENTITY_READ:
                    case ENTITY_CREATE:
                    case ENTITY_UPDATE:
                    case ENTITY_DELETE:
                        AtlasEntityAccessRequestBuilder entityAccessRequestBuilder = getEntityAccessRequest(accessorRequest, action);
                        result = AtlasAuthorizationUtils.getAccessors(entityAccessRequestBuilder.build());
                        break;

                    case ENTITY_READ_CLASSIFICATION:
                    case ENTITY_ADD_CLASSIFICATION:
                    case ENTITY_UPDATE_CLASSIFICATION:
                    case ENTITY_REMOVE_CLASSIFICATION:
                        entityAccessRequestBuilder = getEntityAccessRequest(accessorRequest, action);
                        entityAccessRequestBuilder.setClassification(new AtlasClassification(accessorRequest.getClassification()));
                        result = AtlasAuthorizationUtils.getAccessors(entityAccessRequestBuilder.build());
                        break;

                    case ENTITY_ADD_LABEL:
                    case ENTITY_REMOVE_LABEL:
                        entityAccessRequestBuilder = getEntityAccessRequest(accessorRequest, action);
                        entityAccessRequestBuilder.setLabel(accessorRequest.getLabel());
                        result = AtlasAuthorizationUtils.getAccessors(entityAccessRequestBuilder.build());
                        break;

                    case ENTITY_UPDATE_BUSINESS_METADATA:
                        entityAccessRequestBuilder = getEntityAccessRequest(accessorRequest, action);
                        entityAccessRequestBuilder.setBusinessMetadata(accessorRequest.getBusinessMetadata());
                        result = AtlasAuthorizationUtils.getAccessors(entityAccessRequestBuilder.build());
                        break;


                    case RELATIONSHIP_ADD:
                    case RELATIONSHIP_UPDATE:
                    case RELATIONSHIP_REMOVE:
                        AtlasEntityHeader end1EntityHeader = extractEntityHeader(accessorRequest.getEntityGuidEnd1(), accessorRequest.getEntityQualifiedNameEnd1(), accessorRequest.getEntityTypeEnd1());
                        AtlasEntityHeader end2EntityHeader = extractEntityHeader(accessorRequest.getEntityGuidEnd2(), accessorRequest.getEntityQualifiedNameEnd2(), accessorRequest.getEntityTypeEnd2());

                        AtlasRelationshipAccessRequest relAccessRequest = new AtlasRelationshipAccessRequest(typeRegistry,
                                action, accessorRequest.getRelationshipTypeName(), end1EntityHeader, end2EntityHeader);

                        result = AtlasAuthorizationUtils.getAccessors(relAccessRequest);
                        break;


                    case TYPE_READ:
                    case TYPE_CREATE:
                    case TYPE_UPDATE:
                    case TYPE_DELETE:
                        AtlasBaseTypeDef typeDef = typeRegistry.getTypeDefByName(accessorRequest.getTypeName());
                        AtlasTypeAccessRequest typeAccessRequest = new AtlasTypeAccessRequest(action, typeDef);

                        result = AtlasAuthorizationUtils.getAccessors(typeAccessRequest);
                        break;


                    default:
                        LOG.error("No implementation found for action: {}", accessorRequest.getAction());
                }

                if (result == null) {
                    throw new AtlasBaseException();
                }
                result.populateRequestDetails(accessorRequest);
                ret.add(result);

            } catch (AtlasBaseException e) {
                e.getErrorDetailsMap().put("accessorRequest", AtlasType.toJson(accessorRequest));
                throw e;
            }
        }

        return ret;
    }

    private AtlasEntityAccessRequestBuilder getEntityAccessRequest(AtlasAccessorRequest element, AtlasPrivilege action) throws AtlasBaseException {
        AtlasEntityHeader entityHeader = extractEntityHeader(element.getGuid(), element.getQualifiedName(), element.getTypeName());

        return new AtlasEntityAccessRequestBuilder(typeRegistry, action, entityHeader);
    }

    private AtlasEntityHeader extractEntityHeader(String guid, String qualifiedName, String typeName) throws AtlasBaseException {
        AtlasEntityHeader entityHeader = null;

        if (StringUtils.isNotEmpty(guid)) {
            entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);

        } else {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);
            if (entityType != null) {
                try {
                    Map<String, Object> uniqueAttrs = new HashMap<>();
                    uniqueAttrs.put(QUALIFIED_NAME, qualifiedName);

                    AtlasVertex vertex = AtlasGraphUtilsV2.getVertexByUniqueAttributes(this.graph, entityType, uniqueAttrs);
                    entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);

                } catch (AtlasBaseException abe) {
                    if (abe.getAtlasErrorCode() != AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND) {
                        throw abe;
                    }

                    Map<String, Object> attributes = new HashMap<>();
                    attributes.put(QUALIFIED_NAME, qualifiedName);
                    entityHeader = new AtlasEntityHeader(entityType.getTypeName(), attributes);
                }
            } else {
                Map<String, Object> attributes = new HashMap<>();
                attributes.put(QUALIFIED_NAME, qualifiedName);
                entityHeader = new AtlasEntityHeader(typeName, attributes);
            }
        }
        return entityHeader;
    }

    private Map<String, AtlasEntity> getBusinessMetadataDefList(List<String[]> fileData, BulkImportResponse bulkImportResponse) throws AtlasBaseException {
        Map<String, AtlasEntity> ret           = new HashMap<>();
        Map<String, AtlasVertex> vertexCache   = new HashMap<>();
        List<String>             failedMsgList = new ArrayList<>();

        for (int lineIndex = 0; lineIndex < fileData.size(); lineIndex++) {
            String[] record         = fileData.get(lineIndex);
            int      lineIndexToLog = lineIndex + 2;

            boolean missingFields = record.length < FileUtils.UNIQUE_ATTR_NAME_COLUMN_INDEX ||
                    StringUtils.isBlank(record[FileUtils.TYPENAME_COLUMN_INDEX]) ||
                    StringUtils.isBlank(record[FileUtils.UNIQUE_ATTR_VALUE_COLUMN_INDEX]) ||
                    StringUtils.isBlank(record[FileUtils.BM_ATTR_NAME_COLUMN_INDEX]) ||
                    StringUtils.isBlank(record[FileUtils.BM_ATTR_VALUE_COLUMN_INDEX]);

            if (missingFields){
                failedMsgList.add("Line #" + lineIndexToLog + ": missing fields. " + Arrays.toString(record));

                continue;
            }

            String          typeName   = record[FileUtils.TYPENAME_COLUMN_INDEX];
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            if (entityType == null) {
                failedMsgList.add("Line #" + lineIndexToLog + ": invalid entity-type '" + typeName + "'");

                continue;
            }

            String uniqueAttrValue  = record[FileUtils.UNIQUE_ATTR_VALUE_COLUMN_INDEX];
            String bmAttribute      = record[FileUtils.BM_ATTR_NAME_COLUMN_INDEX];
            String bmAttributeValue = record[FileUtils.BM_ATTR_VALUE_COLUMN_INDEX];
            String uniqueAttrName   = AtlasTypeUtil.ATTRIBUTE_QUALIFIED_NAME;

            if (record.length > FileUtils.UNIQUE_ATTR_NAME_COLUMN_INDEX && StringUtils.isNotBlank(record[FileUtils.UNIQUE_ATTR_NAME_COLUMN_INDEX])) {
                uniqueAttrName = record[FileUtils.UNIQUE_ATTR_NAME_COLUMN_INDEX];
            }

            AtlasAttribute uniqueAttribute = entityType.getAttribute(uniqueAttrName);

            if (uniqueAttribute == null) {
                failedMsgList.add("Line #" + lineIndexToLog + ": attribute '" + uniqueAttrName + "' not found in entity-type '" + typeName + "'");

                continue;
            }

            if (!uniqueAttribute.getAttributeDef().getIsUnique()) {
                failedMsgList.add("Line #" + lineIndexToLog + ": attribute '" + uniqueAttrName + "' is not an unique attribute in entity-type '" + typeName + "'");

                continue;
            }

            String      vertexKey = uniqueAttribute.getVertexPropertyName() + "_" + uniqueAttrValue;
            AtlasVertex vertex    = vertexCache.get(vertexKey);

            if (vertex == null) {
                vertex = AtlasGraphUtilsV2.findByTypeAndUniquePropertyName(graph, typeName, uniqueAttribute.getVertexUniquePropertyName(), uniqueAttrValue);

                if (vertex == null) {
                    failedMsgList.add("Line #" + lineIndexToLog + ": no " + typeName + " entity found with " + uniqueAttrName + "=" + uniqueAttrValue);

                    continue;
                }

                vertexCache.put(vertexKey, vertex);
            }

            AtlasBusinessAttribute businessAttribute = entityType.getBusinesAAttribute(bmAttribute);

            if (businessAttribute == null) {
                failedMsgList.add("Line #" + lineIndexToLog + ": invalid business-metadata '"+ bmAttribute + "' for entity type '" + entityType.getTypeName() + "'");

                continue;
            }

            final Object attrValue;

            if (businessAttribute.getAttributeType().getTypeCategory() == TypeCategory.ARRAY) {
                AtlasArrayType arrayType = (AtlasArrayType) businessAttribute.getAttributeType();
                List           arrayValue;

                if (arrayType.getElementType() instanceof AtlasEnumType) {
                    arrayValue = AtlasGraphUtilsV2.assignEnumValues(bmAttributeValue, (AtlasEnumType) arrayType.getElementType(), failedMsgList, lineIndex+1);
                } else {
                    arrayValue = assignMultipleValues(bmAttributeValue, arrayType.getElementTypeName(), failedMsgList, lineIndex+1);
                }

                attrValue = arrayValue;
            } else {
                attrValue = bmAttributeValue;
            }

            if (ret.containsKey(vertexKey)) {
                AtlasEntity entity = ret.get(vertexKey);

                entity.setBusinessAttribute(businessAttribute.getDefinedInType().getTypeName(), businessAttribute.getName(), attrValue);
            } else {
                AtlasEntity                      entity             = new AtlasEntity();
                String                           guid               = GraphHelper.getGuid(vertex);
                Map<String, Map<String, Object>> businessAttributes = entityRetriever.getBusinessMetadata(vertex);

                entity.setGuid(guid);
                entity.setTypeName(typeName);
                entity.setAttribute(uniqueAttribute.getName(), uniqueAttrValue);

                if (businessAttributes == null) {
                    businessAttributes = new HashMap<>();
                }

                entity.setBusinessAttributes(businessAttributes);
                entity.setBusinessAttribute(businessAttribute.getDefinedInType().getTypeName(), businessAttribute.getName(), attrValue);

                ret.put(vertexKey, entity);
            }
        }

        for (String failedMsg : failedMsgList) {
            LOG.error(failedMsg);

            bulkImportResponse.addToFailedImportInfoList(new ImportInfo(FAILED, failedMsg));
        }

        return ret;
    }


    private List assignMultipleValues(String bmAttributeValues, String elementTypeName, List failedTermMsgList, int lineIndex) {

        String[] arr = bmAttributeValues.split(FileUtils.ESCAPE_CHARACTER + FileUtils.PIPE_CHARACTER);
        try {
            switch (elementTypeName) {

                case AtlasBaseTypeDef.ATLAS_TYPE_FLOAT:
                    return AtlasGraphUtilsV2.floatParser(arr, failedTermMsgList, lineIndex);

                case AtlasBaseTypeDef.ATLAS_TYPE_INT:
                    return AtlasGraphUtilsV2.intParser(arr, failedTermMsgList, lineIndex);

                case AtlasBaseTypeDef.ATLAS_TYPE_LONG:
                    return AtlasGraphUtilsV2.longParser(arr, failedTermMsgList, lineIndex);

                case AtlasBaseTypeDef.ATLAS_TYPE_SHORT:
                    return AtlasGraphUtilsV2.shortParser(arr, failedTermMsgList, lineIndex);

                case AtlasBaseTypeDef.ATLAS_TYPE_DOUBLE:
                    return AtlasGraphUtilsV2.doubleParser(arr, failedTermMsgList, lineIndex);

                case AtlasBaseTypeDef.ATLAS_TYPE_DATE:
                    return AtlasGraphUtilsV2.longParser(arr, failedTermMsgList, lineIndex);

                case AtlasBaseTypeDef.ATLAS_TYPE_BOOLEAN:
                    return AtlasGraphUtilsV2.booleanParser(arr, failedTermMsgList, lineIndex);

                default:
                    return Arrays.asList(arr);
            }
        } catch (Exception e) {
            LOG.error("On line index " + lineIndex + "the provided BusinessMetadata AttributeValue " + bmAttributeValues + " are not of type - " + elementTypeName);
            failedTermMsgList.add("On line index " + lineIndex + "the provided BusinessMetadata AttributeValue " + bmAttributeValues + " are not of type - " + elementTypeName);
        }
        return null;
    }

    private boolean missingFieldsCheck(String[] record, BulkImportResponse bulkImportResponse, int lineIndex){
        boolean missingFieldsCheck = (record.length < FileUtils.UNIQUE_ATTR_NAME_COLUMN_INDEX) ||
                StringUtils.isBlank(record[FileUtils.TYPENAME_COLUMN_INDEX]) ||
                StringUtils.isBlank(record[FileUtils.UNIQUE_ATTR_VALUE_COLUMN_INDEX]) ||
                StringUtils.isBlank(record[FileUtils.BM_ATTR_NAME_COLUMN_INDEX]) ||
                StringUtils.isBlank(record[FileUtils.BM_ATTR_VALUE_COLUMN_INDEX]);

        if(missingFieldsCheck){
            LOG.error("Missing fields: " + Arrays.toString(record) + " at line #" + lineIndex);

            String failedTermMsgs = "Missing fields: " + Arrays.toString(record) + " at line #" + lineIndex;

            bulkImportResponse.addToFailedImportInfoList(new ImportInfo(FAILED, failedTermMsgs, lineIndex));
        }
        return missingFieldsCheck;
    }

    public void repairIndex() throws AtlasBaseException {
        try {
            LOG.info("ReIndexPatch: Starting...");
            PatchContext context = new PatchContext(graph, typeRegistry, null, entityGraphMapper);
            ReIndexPatch.ReindexPatchProcessor reindexPatchProcessor = new ReIndexPatch.ReindexPatchProcessor(context);

            reindexPatchProcessor.repairVertices();
            reindexPatchProcessor.repairEdges();
        } catch (Exception exception) {
            LOG.error("Error while reindexing.", exception);
            throw new AtlasBaseException(AtlasErrorCode.REPAIR_INDEX_FAILED, exception.toString());
        }
    }


    @Override
    @GraphTransaction
    public void repairHasLineage(AtlasHasLineageRequests requests) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("repairHasLineage");

        Set<AtlasEdge> inputOutputEdges = new HashSet<>();

        for (AtlasHasLineageRequest request : requests.getRequest()) {
            if (StringUtils.isNotEmpty(request.getAssetGuid())) {
                //only supports repairing scenario mentioned here - https://atlanhq.atlassian.net/browse/DG-128?focusedCommentId=20652
                repairHasLineageForAsset(request);

            } else {
                AtlasVertex processVertex = AtlasGraphUtilsV2.findByGuid(this.graph, request.getProcessGuid());
                AtlasVertex assetVertex = AtlasGraphUtilsV2.findByGuid(this.graph, request.getEndGuid());
                AtlasEdge edge = null;
                try {
                    if (processVertex != null && assetVertex != null) {
                        edge = graphHelper.getEdge(processVertex, assetVertex, request.getLabel());
                    } else {
                        LOG.warn("Skipping since vertex is null for processGuid {} and asset Guid {}"
                                ,request.getProcessGuid(),request.getEndGuid()  );
                    }
                } catch (RepositoryException re) {
                    throw new AtlasBaseException(AtlasErrorCode.HAS_LINEAGE_GET_EDGE_FAILED, re);
                }

                if (edge != null) {
                    inputOutputEdges.add(edge);
                }
            }
        }

        if (CollectionUtils.isNotEmpty(inputOutputEdges)) {
            repairHasLineageWithAtlasEdges(inputOutputEdges);
        }

        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void repairHasLineageForAsset(AtlasHasLineageRequest request) {
        //only supports repairing scenario mentioned here - https://atlanhq.atlassian.net/browse/DG-128?focusedCommentId=20652

        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("repairHasLineageForAssetGetById");
        AtlasVertex assetVertex = AtlasGraphUtilsV2.findByGuid(this.graph, request.getAssetGuid());
        RequestContext.get().endMetricRecord(metricRecorder);

        if (getEntityHasLineage(assetVertex)) {
            metricRecorder = RequestContext.get().startMetricRecord("repairHasLineageForAssetGetRelations");
            Iterator<AtlasEdge> lineageEdges = assetVertex.getEdges(AtlasEdgeDirection.BOTH, PROCESS_EDGE_LABELS).iterator();
            RequestContext.get().endMetricRecord(metricRecorder);
            boolean foundActiveRel = false;

            while (lineageEdges.hasNext()) {
                AtlasEdge edge = lineageEdges.next();
                if (getStatus(edge) == ACTIVE) {
                    AtlasVertex vertexB = edge.getOutVertex();
                    if (vertexB.equals(assetVertex)) {
                        vertexB = edge.getInVertex();
                    }

                    if (getStatus(vertexB) == ACTIVE) {
                        foundActiveRel = true;
                        break;
                    }
                }
            }

            if (!foundActiveRel) {
                metricRecorder = RequestContext.get().startMetricRecord("repairHasLineageForRequiredAsset");
                AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE, false);
                LOG.info("repairHasLineage: repairHasLineageForAsset: Repaired {}", request.getAssetGuid());
                RequestContext.get().endMetricRecord(metricRecorder);
            }
        }
    }

    public void repairHasLineageWithAtlasEdges(Set<AtlasEdge> inputOutputEdges) {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("repairHasLineageWithAtlasEdges");

        for (AtlasEdge atlasEdge : inputOutputEdges) {

            if (getStatus(atlasEdge) != ACTIVE) {
                LOG.warn("Edge id {} is not Active, so skipping  " , getRelationshipGuid(atlasEdge));
                continue;
            }

            boolean isOutputEdge = PROCESS_OUTPUTS.equals(atlasEdge.getLabel());

            AtlasVertex processVertex = atlasEdge.getOutVertex();
            AtlasVertex assetVertex = atlasEdge.getInVertex();

            if (getEntityHasLineageValid(processVertex) && getEntityHasLineage(processVertex)) {
                AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE, true);
                AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE_VALID, true);
                continue;
            }

            String oppositeEdgeLabel = isOutputEdge ? PROCESS_INPUTS : PROCESS_OUTPUTS;

            Iterator<AtlasEdge> oppositeEdges = processVertex.getEdges(AtlasEdgeDirection.BOTH, oppositeEdgeLabel).iterator();
            boolean isHasLineageSet = false;
            while (oppositeEdges.hasNext()) {
                AtlasEdge oppositeEdge = oppositeEdges.next();
                AtlasVertex oppositeEdgeAssetVertex = oppositeEdge.getInVertex();

                if (getStatus(oppositeEdge) == ACTIVE && getStatus(oppositeEdgeAssetVertex) == ACTIVE) {
                    if (!isHasLineageSet) {
                        AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE, true);
                        AtlasGraphUtilsV2.setEncodedProperty(processVertex, HAS_LINEAGE, true);

                        AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE_VALID, true);
                        AtlasGraphUtilsV2.setEncodedProperty(processVertex, HAS_LINEAGE_VALID, true);

                        isHasLineageSet = true;
                    }
                    break;
                }
            }

            if (!isHasLineageSet) {
                AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE, false);
                AtlasGraphUtilsV2.setEncodedProperty(processVertex, HAS_LINEAGE, false);
                AtlasGraphUtilsV2.setEncodedProperty(assetVertex, HAS_LINEAGE_VALID, true);
                AtlasGraphUtilsV2.setEncodedProperty(processVertex, HAS_LINEAGE_VALID, true);
            }

        }
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    private void recordRelationshipsToBePurged(AtlasVertex instanceVertex) throws AtlasBaseException {
        Iterable<AtlasEdge> incomingEdges = instanceVertex.getEdges(AtlasEdgeDirection.IN);
        Iterable<AtlasEdge> outgoingEdges = instanceVertex.getEdges(AtlasEdgeDirection.OUT);

        recordInComingEdgesToBeDeleted(incomingEdges);
        recordOutGoingEdgesToBeDeleted(outgoingEdges);
    }

    private void recordInComingEdgesToBeDeleted(Iterable<AtlasEdge> incomingEdges) throws AtlasBaseException {
        for (AtlasEdge edge : incomingEdges) {
            if (isRelationshipEdge(edge))
                AtlasRelationshipStoreV2.recordRelationshipMutation(AtlasRelationshipStoreV2.RelationshipMutation.RELATIONSHIP_HARD_DELETE, edge, entityRetriever);
        }
    }

    private void recordOutGoingEdgesToBeDeleted(Iterable<AtlasEdge> outgoingEdges) throws AtlasBaseException {
        for (AtlasEdge edge : outgoingEdges) {
            if (isRelationshipEdge(edge))
                AtlasRelationshipStoreV2.recordRelationshipMutation(AtlasRelationshipStoreV2.RelationshipMutation.RELATIONSHIP_HARD_DELETE, edge, entityRetriever);
        }
    }

    @Override
    @GraphTransaction
    public void repairMeaningAttributeForTerms(List<String> termGuid) {

        for (String guid : termGuid) {
            LOG.info(" term guid " + guid);

            AtlasVertex termVertex = AtlasGraphUtilsV2.findByGuid(this.graph, guid);

            if(termVertex!= null && ATLAS_GLOSSARY_TERM_ENTITY_TYPE.equals(getTypeName(termVertex)) &&
                    GraphHelper.getStatus(termVertex) == AtlasEntity.Status.ACTIVE) {
                Iterable<AtlasEdge> edges = termVertex.getEdges(AtlasEdgeDirection.OUT, Constants.TERM_ASSIGNMENT_LABEL);
                // Get entity to tagged with term.
                if (edges != null) {
                    for (Iterator<AtlasEdge> iter = edges.iterator(); iter.hasNext(); ) {
                        AtlasEdge edge = iter.next();
                        if (GraphHelper.getStatus(edge) == AtlasEntity.Status.ACTIVE) {
                            AtlasVertex entityVertex = edge.getInVertex();
                            if (entityVertex != null & getStatus(entityVertex) == AtlasEntity.Status.ACTIVE) {
                                if(!RequestContext.get().getProcessGuidIds().contains(getGuid(entityVertex))) {
                                    repairMeanings(entityVertex);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void repairMeanings(AtlasVertex assetVertex) {

        Iterable<AtlasEdge> edges = assetVertex.getEdges(AtlasEdgeDirection.IN, Constants.TERM_ASSIGNMENT_LABEL);
        List<String> termQNList = new ArrayList<>();
        List<String> termNameList = new ArrayList<>();
        if (edges != null) {
            for (Iterator<AtlasEdge> iter = edges.iterator(); iter.hasNext(); ) {
                AtlasEdge edge = iter.next();
                if (GraphHelper.getStatus(edge) == AtlasEntity.Status.ACTIVE) {
                    AtlasVertex termVertex = edge.getOutVertex();
                    if (termVertex != null & getStatus(termVertex) == AtlasEntity.Status.ACTIVE) {
                        String termQN = termVertex.getProperty(QUALIFIED_NAME, String.class);
                        String termName = termVertex.getProperty(NAME, String.class);
                        termQNList.add(termQN);
                        termNameList.add(termName);
                    }
                }
            }
        }

        if (termQNList.size() > 0) {

            assetVertex.removeProperty(MEANINGS_PROPERTY_KEY);
            assetVertex.removeProperty(MEANINGS_TEXT_PROPERTY_KEY);
            assetVertex.removeProperty(MEANING_NAMES_PROPERTY_KEY);

            if (CollectionUtils.isNotEmpty(termQNList)) {
                termQNList.forEach(q -> AtlasGraphUtilsV2.addEncodedProperty(assetVertex, MEANINGS_PROPERTY_KEY, q));
            }

            if (CollectionUtils.isNotEmpty(termNameList)) {
                AtlasGraphUtilsV2.setEncodedProperty(assetVertex, MEANINGS_TEXT_PROPERTY_KEY, StringUtils.join(termNameList, ","));
            }

            if (CollectionUtils.isNotEmpty(termNameList)) {
                termNameList.forEach(q -> AtlasGraphUtilsV2.addListProperty(assetVertex, MEANING_NAMES_PROPERTY_KEY, q, true));
            }

            RequestContext.get().addProcessGuidIds(getGuid(assetVertex));

            LOG.info("Updated asset {}  with term {} ",  getGuid(assetVertex) ,  StringUtils.join(termNameList, ","));
        }

    }
    @Override
    public void repairAccesscontrolAlias(String guid) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("repairAlias");
        // Fetch accesscontrolEntity with extInfo
        AtlasEntity.AtlasEntityWithExtInfo accesscontrolEntity = entityRetriever.toAtlasEntityWithExtInfo(guid);

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, new AtlasEntityHeader(accesscontrolEntity.getEntity())));

        // Validate accesscontrolEntity status
        if (accesscontrolEntity.getEntity().getStatus() != ACTIVE) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_DELETED, guid);
        }

        // Validate accesscontrolEntity type
        String entityType = accesscontrolEntity.getEntity().getTypeName();
        if (!PERSONA_ENTITY_TYPE.equals(entityType)) {
            throw new AtlasBaseException(AtlasErrorCode.OPERATION_NOT_SUPPORTED, entityType);
        }

        List<AtlasObjectId> policies = (List<AtlasObjectId>) accesscontrolEntity.getEntity().getRelationshipAttribute(REL_ATTR_POLICIES);
        for (AtlasObjectId policy : policies) {
            accesscontrolEntity.addReferredEntity(entityRetriever.toAtlasEntity(policy));
        }

        // Rebuild alias
        this.esAliasStore.updateAlias(accesscontrolEntity, null);

        RequestContext.get().endMetricRecord(metric);
    }

    @Override
    @GraphTransaction
    public void linkBusinessPolicy(List<BusinessPolicyRequest.AssetComplianceInfo> data) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("linkBusinessPolicy.GraphTransaction");
        List<AtlasVertex> atlasVertices = new ArrayList<>();
        try {
            for (BusinessPolicyRequest.AssetComplianceInfo ad : data) {
                AtlasVertex av = this.entityGraphMapper.linkBusinessPolicy(ad);
                atlasVertices.add(av);
            }
            handleEntityMutation(atlasVertices);
        } catch (Exception e) {
            LOG.error("Error during linkBusinessPolicy for policyGuid: ", e);
            throw e;
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    @Override
    @GraphTransaction
    public void unlinkBusinessPolicy(String policyGuid, Set<String> unlinkGuids) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("unlinkBusinessPolicy.GraphTransaction");
        try {
            List<AtlasVertex> vertices = this.entityGraphMapper.unlinkBusinessPolicy(policyGuid, unlinkGuids);
            if (CollectionUtils.isEmpty(vertices)) {
                return;
            }

            handleEntityMutation(vertices);
        } catch (Exception e) {
            LOG.error("Error during unlinkBusinessPolicy for policyGuid: {}", policyGuid, e);
            throw e;
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    @Override
    @GraphTransaction
    public void linkMeshEntityToAssets(String meshEntityGuid, Set<String> linkGuids) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("linkMeshEntityToAssets.GraphTransaction");

        try {
            List<String> assetGuids = new ArrayList<>(linkGuids);
            GraphTransactionInterceptor.lockObjectAndReleasePostCommit(assetGuids);
            List<AtlasVertex> vertices = this.entityGraphMapper.linkMeshEntityToAssets(meshEntityGuid, linkGuids);
            if (CollectionUtils.isEmpty(vertices)) {
                return;
            }

            LOG.info("linkMeshEntityToAssets: entityGuid={}", meshEntityGuid);

            handleEntityMutation(vertices);
        } catch (Exception e) {
            LOG.error("Error during linkMeshEntity for entityGuid: {}", meshEntityGuid, e);
            throw e;
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    @Override
    @GraphTransaction
    public void unlinkMeshEntityFromAssets(String meshEntityGuid, Set<String> unlinkGuids) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("unlinkMeshEntityFromAssets.GraphTransaction");
        try {
            List<String> assetGuids = new ArrayList<>(unlinkGuids);
            GraphTransactionInterceptor.lockObjectAndReleasePostCommit(assetGuids);
            List<AtlasVertex> vertices = this.entityGraphMapper.unlinkMeshEntityFromAssets(meshEntityGuid, unlinkGuids);
            if (CollectionUtils.isEmpty(vertices)) {
                return;
            }

            LOG.info("unlinkMeshEntityFromAssets: assetGuids={}", unlinkGuids);

            handleEntityMutation(vertices);
        } catch (Exception e) {
            LOG.error("Error during unlinkMeshEntity for assetGuids: {}", unlinkGuids, e);
            throw e;
        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    private void handleEntityMutation(List<AtlasVertex> vertices) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metricRecorder = RequestContext.get().startMetricRecord("handleEntityMutation");
        this.atlasAlternateChangeNotifier.onEntitiesMutation(vertices);
        RequestContext.get().endMetricRecord(metricRecorder);
    }

    @Override
    @GraphTransaction
    public void moveBusinessPolicies(Set<String> policyIds, String assetId, String type) throws AtlasBaseException {
        // Start performance metric recording
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("moveBusinessPolicy.GraphTransaction");

        try {
            // Attempt to move the business policy using the entityGraphMapper
            AtlasVertex vertex = entityGraphMapper.moveBusinessPolicies(policyIds, assetId, type);

            if (vertex == null) {
                LOG.warn("No vertex found for assetId: {}", assetId);
                return;
            }
            handleEntityMutation(Collections.singletonList(vertex));
        } catch (Exception e) {
            // Log the error with context and rethrow it wrapped in an AtlasBaseException
            LOG.error("Error during moveBusinessPolicy for assetId: {}", assetId, e);
            throw new AtlasBaseException(e);
        } finally {
            // End the performance metric recording
            RequestContext.get().endMetricRecord(metric);
        }
    }
}
