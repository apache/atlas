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


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.authorize.AtlasEntityAccessRequest;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.v1.DeleteHandlerV1;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.AtlasEntityUtil;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.DELETE;
import static org.apache.atlas.model.instance.EntityMutations.EntityOperation.UPDATE;


@Component
public class AtlasEntityStoreV2 implements AtlasEntityStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityStoreV2.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("store.EntityStore");


    private final DeleteHandlerV1 deleteHandler;
    private final AtlasTypeRegistry         typeRegistry;
    private final AtlasEntityChangeNotifier entityChangeNotifier;
    private final EntityGraphMapper         entityGraphMapper;
    private final EntityGraphRetriever      entityRetriever;

    @Inject
    public AtlasEntityStoreV2(DeleteHandlerV1 deleteHandler, AtlasTypeRegistry typeRegistry,
                              AtlasEntityChangeNotifier entityChangeNotifier, EntityGraphMapper entityGraphMapper) {
        this.deleteHandler        = deleteHandler;
        this.typeRegistry         = typeRegistry;
        this.entityChangeNotifier = entityChangeNotifier;
        this.entityGraphMapper    = entityGraphMapper;
        this.entityRetriever      = new EntityGraphRetriever(typeRegistry);
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

        List<String> ret = AtlasGraphUtilsV2.findEntityGUIDsByType(typename);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getEntityGUIDS({})", typename);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityWithExtInfo getById(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getById({})", guid);
        }

        AtlasEntityWithExtInfo ret = entityRetriever.toAtlasEntityWithExtInfo(guid);

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, new AtlasEntityHeader(ret.getEntity())), "read entity: guid=", guid);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getById({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntitiesWithExtInfo getByIds(List<String> guids) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getByIds({})", guids);
        }

        AtlasEntitiesWithExtInfo ret = entityRetriever.toAtlasEntitiesWithExtInfo(guids);

        // verify authorization to read the entities
        if(ret != null){
            for(String guid : guids){
                AtlasEntity entity = ret.getEntity(guid);

                AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, new AtlasEntityHeader(entity)), "read entity: guid=", guid);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getByIds({}): {}", guids, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasEntityWithExtInfo getByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> getByUniqueAttribute({}, {})", entityType.getTypeName(), uniqAttributes);
        }

        AtlasVertex            entityVertex = AtlasGraphUtilsV2.getVertexByUniqueAttributes(entityType, uniqAttributes);
        AtlasEntityWithExtInfo ret          = entityRetriever.toAtlasEntityWithExtInfo(entityVertex);

        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_READ, new AtlasEntityHeader(ret.getEntity())), "read entity: typeName=", entityType.getTypeName(), ", uniqueAttributes=", uniqAttributes);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getByUniqueAttribute({}, {}): {}", entityType.getTypeName(), uniqAttributes, ret);
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse createOrUpdate(EntityStream entityStream, boolean isPartialUpdate) throws AtlasBaseException {
        return createOrUpdate(entityStream, isPartialUpdate, false);
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse createOrUpdateForImport(EntityStream entityStream) throws AtlasBaseException {
        return createOrUpdate(entityStream, false, true);
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

            guid = AtlasGraphUtilsV2.getGuidByUniqueAttributes(typeRegistry.getEntityTypeByName(objectId.getTypeName()), objectId.getUniqueAttributes());
        }

        AtlasEntity entity = updatedEntityInfo.getEntity();

        entity.setGuid(guid);

        return createOrUpdate(new AtlasEntityStream(updatedEntityInfo), isPartialUpdate, false);
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

        String      guid   = AtlasGraphUtilsV2.getGuidByUniqueAttributes(entityType, uniqAttributes);
        AtlasEntity entity = updatedEntityInfo.getEntity();

        entity.setGuid(guid);

        return createOrUpdate(new AtlasEntityStream(updatedEntityInfo), true, false);
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

        if (attr == null) {
            throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_ATTRIBUTE, attrName, entity.getTypeName());
        }

        AtlasType   attrType     = attr.getAttributeType();
        AtlasEntity updateEntity = new AtlasEntity();

        updateEntity.setGuid(guid);
        updateEntity.setTypeName(entity.getTypeName());

        switch (attrType.getTypeCategory()) {
            case PRIMITIVE:
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

        return createOrUpdate(new AtlasEntityStream(updateEntity), true, false);
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse deleteById(final String guid) throws AtlasBaseException {
        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        Collection<AtlasVertex> deletionCandidates = new ArrayList<>();
        AtlasVertex             vertex             = AtlasGraphUtilsV2.findByGuid(guid);

        if (vertex != null) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);

            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_DELETE, entityHeader), "delete entity: guid=", guid);

            deletionCandidates.add(vertex);
        } else {
            if (LOG.isDebugEnabled()) {
                // Entity does not exist - treat as non-error, since the caller
                // wanted to delete the entity and it's already gone.
                LOG.debug("Deletion request ignored for non-existent entity with guid " + guid);
            }
        }

        EntityMutationResponse ret = deleteVertices(deletionCandidates);

        // Notify the change listeners
        entityChangeNotifier.onEntitiesMutated(ret, false);

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
            AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(guid);

            if (vertex == null) {
                if (LOG.isDebugEnabled()) {
                    // Entity does not exist - treat as non-error, since the caller
                    // wanted to delete the entity and it's already gone.
                    LOG.debug("Deletion request ignored for non-existent entity with guid " + guid);
                }

                continue;
            }

            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);

            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_DELETE, entityHeader), "delete entity: guid=", guid);

            deletionCandidates.add(vertex);
        }

        if (deletionCandidates.isEmpty()) {
            LOG.info("No deletion candidate entities were found for guids %s", guids);
        }

        EntityMutationResponse ret = deleteVertices(deletionCandidates);

        // Notify the change listeners
        entityChangeNotifier.onEntitiesMutated(ret, false);

        return ret;
    }

    @Override
    @GraphTransaction
    public EntityMutationResponse deleteByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes) throws AtlasBaseException {
        if (MapUtils.isEmpty(uniqAttributes)) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND, uniqAttributes.toString());
        }

        Collection<AtlasVertex> deletionCandidates = new ArrayList<>();
        AtlasVertex             vertex             = AtlasGraphUtilsV2.findByUniqueAttributes(entityType, uniqAttributes);

        if (vertex != null) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(vertex);

            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_DELETE, entityHeader), "delete entity: typeName=", entityType.getTypeName(), ", uniqueAttributes=", uniqAttributes);

            deletionCandidates.add(vertex);
        } else {
            if (LOG.isDebugEnabled()) {
                // Entity does not exist - treat as non-error, since the caller
                // wanted to delete the entity and it's already gone.
                LOG.debug("Deletion request ignored for non-existent entity with uniqueAttributes " + uniqAttributes);
            }
        }

        EntityMutationResponse ret = deleteVertices(deletionCandidates);

        // Notify the change listeners
        entityChangeNotifier.onEntitiesMutated(ret, false);

        return ret;
    }

    @Override
    @GraphTransaction
    public String getGuidByUniqueAttributes(AtlasEntityType entityType, Map<String, Object> uniqAttributes) throws AtlasBaseException{
        return AtlasGraphUtilsV2.getGuidByUniqueAttributes(entityType, uniqAttributes);
    }

    @Override
    @GraphTransaction
    public void addClassifications(final String guid, final List<AtlasClassification> classifications) throws AtlasBaseException {
        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }

        if (CollectionUtils.isEmpty(classifications)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "classifications(s) not specified");
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding classifications={} to entity={}", classifications, guid);
        }

        AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);

        for (AtlasClassification classification : classifications) {
            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_ADD_CLASSIFICATION, entityHeader, classification),
                                                 "add classification: guid=", guid, ", classification=", classification.getTypeName());
        }

        GraphTransactionInterceptor.lockObjectAndReleasePostCommit(guid);

        for (AtlasClassification classification : classifications) {
            validateAndNormalize(classification);
        }

        // validate if entity, not already associated with classifications
        validateEntityAssociations(guid, classifications);

        entityGraphMapper.addClassifications(new EntityMutationContext(), guid, classifications);
    }

    @Override
    @GraphTransaction
    public void updateClassifications(String guid, List<AtlasClassification> classifications) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Updating classifications={} for entity={}", classifications, guid);
        }

        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid not specified");
        }

        if (CollectionUtils.isEmpty(classifications)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "classifications(s) not specified");
        }

        AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);

        for (AtlasClassification classification : classifications) {
            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE_CLASSIFICATION, entityHeader, classification), "update classification: guid=", guid, ", classification=", classification.getTypeName());
        }

        GraphTransactionInterceptor.lockObjectAndReleasePostCommit(guid);

        for (AtlasClassification classification : classifications) {
            validateAndNormalize(classification);
        }

        entityGraphMapper.updateClassifications(new EntityMutationContext(), guid, classifications);
    }

    @Override
    @GraphTransaction
    public void addClassification(final List<String> guids, final AtlasClassification classification) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(guids)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }
        if (classification == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "classification not specified");
        }

        for (String guid : guids) {
            AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);

            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_ADD_CLASSIFICATION, entityHeader, classification),
                                                 "add classification: guid=", guid, ", classification=", classification.getTypeName());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Adding classification={} to entities={}", classification, guids);
        }

        GraphTransactionInterceptor.lockObjectAndReleasePostCommit(guids);

        validateAndNormalize(classification);

        List<AtlasClassification> classifications = Collections.singletonList(classification);

        for (String guid : guids) {
            validateEntityAssociations(guid, classifications);

            entityGraphMapper.addClassifications(new EntityMutationContext(), guid, classifications);
        }
    }

    @Override
    @GraphTransaction
    public void deleteClassifications(final String guid, final List<String> classificationNames) throws AtlasBaseException {
        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Guid(s) not specified");
        }
        if (CollectionUtils.isEmpty(classificationNames)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "classifications(s) not specified");
        }

        AtlasEntityHeader entityHeader = entityRetriever.toAtlasEntityHeaderWithClassifications(guid);

        for (String classification : classificationNames) {
            AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_REMOVE_CLASSIFICATION, entityHeader, new AtlasClassification(classification)), "remove classification: guid=", guid, ", classification=", classification);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting classifications={} from entity={}", classificationNames, guid);
        }

        GraphTransactionInterceptor.lockObjectAndReleasePostCommit(guid);

        entityGraphMapper.deleteClassifications(guid, classificationNames);
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

    private EntityMutationResponse createOrUpdate(EntityStream entityStream, boolean isPartialUpdate, boolean replaceClassifications) throws AtlasBaseException {
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

        try {
            final boolean               isImport = entityStream instanceof EntityImportStream;
            final EntityMutationContext context  = preCreateOrUpdate(entityStream, entityGraphMapper, isPartialUpdate);

            // Check if authorized to create entities
            if (!isImport && CollectionUtils.isNotEmpty(context.getCreatedEntities())) {
                for (AtlasEntity entity : context.getCreatedEntities()) {
                    AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_CREATE, new AtlasEntityHeader(entity)),
                                                         "create entity: type=", entity.getTypeName());
                }
            }

            // for existing entities, skip update if incoming entity doesn't have any change
            if (CollectionUtils.isNotEmpty(context.getUpdatedEntities())) {
                List<AtlasEntity> entitiesToSkipUpdate = null;

                for (AtlasEntity entity : context.getUpdatedEntities()) {
                    String          guid          = entity.getGuid();
                    AtlasVertex     vertex        = context.getVertex(guid);
                    AtlasEntity     entityInStore = entityRetriever.toAtlasEntity(vertex);
                    AtlasEntityType entityType    = typeRegistry.getEntityTypeByName(entity.getTypeName());

                    if (!AtlasEntityUtil.hasAnyAttributeUpdate(entityType, entity, entityInStore)) {
                        // if classifications are to be replaced as well, then skip updates only when no change in classifications as well
                        if (!replaceClassifications || Objects.equals(entity.getClassifications(), entityInStore.getClassifications())) {
                            if (entitiesToSkipUpdate == null) {
                                entitiesToSkipUpdate = new ArrayList<>();
                            }

                            entitiesToSkipUpdate.add(entity);
                        }
                    }
                }

                if (entitiesToSkipUpdate != null) {
                    context.getUpdatedEntities().removeAll(entitiesToSkipUpdate);
                }

                // Check if authorized to update entities
                if (!isImport) {
                    for (AtlasEntity entity : context.getUpdatedEntities()) {
                        AtlasAuthorizationUtils.verifyAccess(new AtlasEntityAccessRequest(typeRegistry, AtlasPrivilege.ENTITY_UPDATE, new AtlasEntityHeader(entity)),
                                                             "update entity: type=", entity.getTypeName());
                    }
                }
            }

            EntityMutationResponse ret = entityGraphMapper.mapAttributesAndClassifications(context, isPartialUpdate, replaceClassifications);

            ret.setGuidAssignments(context.getGuidAssignments());

            // Notify the change listeners
            entityChangeNotifier.onEntitiesMutated(ret, isImport);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== createOrUpdate()");
            }

            return ret;
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private EntityMutationContext preCreateOrUpdate(EntityStream entityStream, EntityGraphMapper entityGraphMapper, boolean isPartialUpdate) throws AtlasBaseException {
        EntityGraphDiscovery        graphDiscoverer  = new AtlasEntityGraphDiscoveryV2(typeRegistry, entityStream);
        EntityGraphDiscoveryContext discoveryContext = graphDiscoverer.discoverEntities();
        EntityMutationContext       context          = new EntityMutationContext(discoveryContext);

        for (String guid : discoveryContext.getReferencedGuids()) {
            AtlasVertex vertex = discoveryContext.getResolvedEntityVertex(guid);
            AtlasEntity entity = entityStream.getByGuid(guid);

            if (entity != null) { // entity would be null if guid is not in the stream but referenced by an entity in the stream
                if (vertex != null) {
                    if (!isPartialUpdate) {
                        graphDiscoverer.validateAndNormalize(entity);
                    } else {
                        graphDiscoverer.validateAndNormalizeForUpdate(entity);
                    }

                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());

                    String guidVertex = AtlasGraphUtilsV2.getIdFromVertex(vertex);

                    if (!StringUtils.equals(guidVertex, guid)) { // if entity was found by unique attribute
                        entity.setGuid(guidVertex);
                    }

                    context.addUpdated(guid, entity, entityType, vertex);
                } else {
                    graphDiscoverer.validateAndNormalize(entity);

                    AtlasEntityType entityType = typeRegistry.getEntityTypeByName(entity.getTypeName());


                    //Create vertices which do not exist in the repository
                    if ((entityStream instanceof EntityImportStream) && AtlasTypeUtil.isAssignedGuid(entity.getGuid())) {
                        vertex = entityGraphMapper.createVertexWithGuid(entity, entity.getGuid());
                    } else {
                         vertex = entityGraphMapper.createVertex(entity);
                    }

                    discoveryContext.addResolvedGuid(guid, vertex);

                    String generatedGuid = AtlasGraphUtilsV2.getIdFromVertex(vertex);

                    entity.setGuid(generatedGuid);

                    context.addCreated(guid, entity, entityType, vertex);
                }

                // during import, update the system attributes
                if (entityStream instanceof EntityImportStream) {
                    entityGraphMapper.updateSystemAttributes(vertex, entity);
                }
            }
        }

        return context;
    }

    private EntityMutationResponse deleteVertices(Collection<AtlasVertex> deletionCandidates) throws AtlasBaseException {
        EntityMutationResponse response = new EntityMutationResponse();
        RequestContext         req      = RequestContext.get();

        deleteHandler.deleteEntities(deletionCandidates); // this will update req with list of deleted/updated entities

        for (AtlasObjectId entity : req.getDeletedEntities()) {
            response.addEntity(DELETE, entity);
        }

        for (AtlasObjectId entity : req.getUpdatedEntities()) {
            response.addEntity(UPDATE, entity);
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
        String          entityTypeName        = AtlasGraphUtilsV2.getTypeNameFromGuid(guid);
        AtlasEntityType entityType            = typeRegistry.getEntityTypeByName(entityTypeName);

        for (AtlasClassification classification : classifications) {
            String newClassification = classification.getTypeName();

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
}
