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


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.EntityChangeListenerV2;
import org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;

import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.instance.EntityMutations.EntityOperation;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.converters.AtlasInstanceConverter;
import org.apache.atlas.repository.graph.FullTextMapperV2;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.PROPAGATED_CLASSIFICATION_ADD;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.PROPAGATED_CLASSIFICATION_DELETE;
import static org.apache.atlas.util.AtlasRepositoryConfiguration.isV2EntityNotificationEnabled;


@Component
public class AtlasEntityChangeNotifier {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEntityChangeNotifier.class);

    private final Set<EntityChangeListener>   entityChangeListeners;
    private final Set<EntityChangeListenerV2> entityChangeListenersV2;
    private final AtlasInstanceConverter      instanceConverter;
    private final FullTextMapperV2            fullTextMapperV2;
    private final AtlasTypeRegistry           atlasTypeRegistry;


    @Inject
    public AtlasEntityChangeNotifier(Set<EntityChangeListener> entityChangeListeners,
                                     Set<EntityChangeListenerV2> entityChangeListenersV2,
                                     AtlasInstanceConverter instanceConverter,
                                     FullTextMapperV2 fullTextMapperV2,
                                     AtlasTypeRegistry atlasTypeRegistry) {
        this.entityChangeListeners   = entityChangeListeners;
        this.entityChangeListenersV2 = entityChangeListenersV2;
        this.instanceConverter       = instanceConverter;
        this.fullTextMapperV2 = fullTextMapperV2;
        this.atlasTypeRegistry = atlasTypeRegistry;
    }

    public void onEntitiesMutated(EntityMutationResponse entityMutationResponse, boolean isImport) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(entityChangeListeners) || instanceConverter == null) {
            return;
        }

        List<AtlasEntityHeader> createdEntities          = entityMutationResponse.getCreatedEntities();
        List<AtlasEntityHeader> updatedEntities          = entityMutationResponse.getUpdatedEntities();
        List<AtlasEntityHeader> partiallyUpdatedEntities = entityMutationResponse.getPartialUpdatedEntities();
        List<AtlasEntityHeader> deletedEntities          = entityMutationResponse.getDeletedEntities();

        // complete full text mapping before calling toReferenceables(), from notifyListners(), to
        // include all vertex updates in the current graph-transaction
        doFullTextMapping(createdEntities);
        doFullTextMapping(updatedEntities);
        doFullTextMapping(partiallyUpdatedEntities);

        notifyListeners(createdEntities, EntityOperation.CREATE, isImport);
        notifyListeners(updatedEntities, EntityOperation.UPDATE, isImport);
        notifyListeners(partiallyUpdatedEntities, EntityOperation.PARTIAL_UPDATE, isImport);
        notifyListeners(deletedEntities, EntityOperation.DELETE, isImport);

        notifyPropagatedEntities();
    }

    public void onClassificationAddedToEntity(AtlasEntity entity, List<AtlasClassification> addedClassifications) throws AtlasBaseException {
        if (isV2EntityNotificationEnabled()) {
            doFullTextMapping(entity.getGuid());

            for (EntityChangeListenerV2 listener : entityChangeListenersV2) {
                listener.onClassificationsAdded(entity, addedClassifications);
            }
        } else {
            updateFullTextMapping(entity.getGuid(), addedClassifications);

            Referenceable entityRef = toReferenceable(entity.getGuid());
            List<Struct>  traits    = toStruct(addedClassifications);

            if (entity == null || CollectionUtils.isEmpty(traits)) {
                return;
            }

            for (EntityChangeListener listener : entityChangeListeners) {
                try {
                    listener.onTraitsAdded(entityRef, traits);
                } catch (AtlasException e) {
                    throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED, e, getListenerName(listener), "TraitAdd");
                }
            }
        }
    }

    public void onClassificationUpdatedToEntity(AtlasEntity entity, List<AtlasClassification> updatedClassifications) throws AtlasBaseException {
        if (isV2EntityNotificationEnabled()) {
            doFullTextMapping(entity.getGuid());

            for (EntityChangeListenerV2 listener : entityChangeListenersV2) {
                listener.onClassificationsUpdated(entity, updatedClassifications);
            }
        } else {
            doFullTextMapping(entity.getGuid());

            Referenceable entityRef = toReferenceable(entity.getGuid());
            List<Struct>  traits    = toStruct(updatedClassifications);

            if (entityRef == null || CollectionUtils.isEmpty(traits)) {
                return;
            }

            for (EntityChangeListener listener : entityChangeListeners) {
                try {
                    listener.onTraitsUpdated(entityRef, traits);
                } catch (AtlasException e) {
                    throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED, e, getListenerName(listener), "TraitUpdate");
                }
            }
        }
    }

    public void onClassificationDeletedFromEntity(AtlasEntity entity, List<AtlasClassification> deletedClassifications) throws AtlasBaseException {
        if (isV2EntityNotificationEnabled()) {
            doFullTextMapping(entity.getGuid());

            for (EntityChangeListenerV2 listener : entityChangeListenersV2) {
                listener.onClassificationsDeleted(entity, deletedClassifications);
            }
        } else {
            doFullTextMapping(entity.getGuid());

            Referenceable entityRef = toReferenceable(entity.getGuid());
            List<Struct>  traits    = toStruct(deletedClassifications);

            if (entityRef == null || CollectionUtils.isEmpty(deletedClassifications)) {
                return;
            }

            for (EntityChangeListener listener : entityChangeListeners) {
                try {
                    listener.onTraitsDeleted(entityRef, traits);
                } catch (AtlasException e) {
                    throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED, e, getListenerName(listener), "TraitDelete");
                }
            }

        }
    }

    public void onTermAddedToEntities(AtlasGlossaryTerm term, List<AtlasRelatedObjectId> entityIds) throws AtlasBaseException {
        // listeners notified on term-entity association only if v2 notifications are enabled
        if (isV2EntityNotificationEnabled()) {
            for (EntityChangeListenerV2 listener : entityChangeListenersV2) {
                listener.onTermAdded(term, entityIds);
            }
        } else {
            List<Referenceable> entityRefs = toReferenceables(entityIds);

            for (EntityChangeListener listener : entityChangeListeners) {
                try {
                    listener.onTermAdded(entityRefs, term);
                } catch (AtlasException e) {
                    throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED, e, getListenerName(listener), "TermAdd");
                }
            }
        }
    }

    public void onTermDeletedFromEntities(AtlasGlossaryTerm term, List<AtlasRelatedObjectId> entityIds) throws AtlasBaseException {
        // listeners notified on term-entity disassociation only if v2 notifications are enabled
        if (isV2EntityNotificationEnabled()) {
            for (EntityChangeListenerV2 listener : entityChangeListenersV2) {
                listener.onTermDeleted(term, entityIds);
            }
        } else {
            List<Referenceable> entityRefs = toReferenceables(entityIds);

            for (EntityChangeListener listener : entityChangeListeners) {
                try {
                    listener.onTermDeleted(entityRefs, term);
                } catch (AtlasException e) {
                    throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED, e, getListenerName(listener), "TermDelete");
                }
            }
        }
    }

    public void notifyPropagatedEntities() throws AtlasBaseException {
        RequestContextV1                       context             = RequestContextV1.get();
        Map<String, List<AtlasClassification>> addedPropagations   = context.getAddedPropagations();
        Map<String, List<AtlasClassification>> removedPropagations = context.getRemovedPropagations();

        notifyPropagatedEntities(addedPropagations, PROPAGATED_CLASSIFICATION_ADD);
        notifyPropagatedEntities(removedPropagations, PROPAGATED_CLASSIFICATION_DELETE);
    }

    private void notifyPropagatedEntities(Map<String, List<AtlasClassification>> entityPropagationMap, EntityAuditActionV2 action) throws AtlasBaseException {
        if (MapUtils.isEmpty(entityPropagationMap) || action == null) {
            return;
        }

        for (String guid : entityPropagationMap.keySet()) {
            AtlasEntityWithExtInfo entityWithExtInfo = instanceConverter.getAndCacheEntity(guid);
            AtlasEntity            entity            = entityWithExtInfo != null ? entityWithExtInfo.getEntity() : null;

            if (entity == null) {
                continue;
            }

            if (action == PROPAGATED_CLASSIFICATION_ADD) {
                onClassificationAddedToEntity(entity, entityPropagationMap.get(guid));
            } else if (action == PROPAGATED_CLASSIFICATION_DELETE) {
                onClassificationDeletedFromEntity(entity, entityPropagationMap.get(guid));
            }
        }
    }

    private String getListenerName(EntityChangeListener listener) {
        return listener.getClass().getSimpleName();
    }

    private void notifyListeners(List<AtlasEntityHeader> entityHeaders, EntityOperation operation, boolean isImport) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(entityHeaders)) {
            return;
        }

        if (isV2EntityNotificationEnabled()) {
            notifyV2Listeners(entityHeaders, operation, isImport);
        } else {
            notifyV1Listeners(entityHeaders, operation, isImport);
        }
    }

    private void notifyV1Listeners(List<AtlasEntityHeader> entityHeaders, EntityOperation operation, boolean isImport) throws AtlasBaseException {
        List<Referenceable> typedRefInsts = toReferenceables(entityHeaders, operation);

        for (EntityChangeListener listener : entityChangeListeners) {
            try {
                switch (operation) {
                    case CREATE:
                        listener.onEntitiesAdded(typedRefInsts, isImport);
                        break;
                    case UPDATE:
                    case PARTIAL_UPDATE:
                        listener.onEntitiesUpdated(typedRefInsts, isImport);
                        break;
                    case DELETE:
                        listener.onEntitiesDeleted(typedRefInsts, isImport);
                        break;
                }
            } catch (AtlasException e) {
                throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_FAILED, e, getListenerName(listener), operation.toString());
            }
        }
    }

    private void notifyV2Listeners(List<AtlasEntityHeader> entityHeaders, EntityOperation operation, boolean isImport) throws AtlasBaseException {
        List<AtlasEntity> entities = toAtlasEntities(entityHeaders);

        for (EntityChangeListenerV2 listener : entityChangeListenersV2) {
            switch (operation) {
                case CREATE:
                    listener.onEntitiesAdded(entities, isImport);
                    break;
                case UPDATE:
                case PARTIAL_UPDATE:
                    listener.onEntitiesUpdated(entities, isImport);
                    break;
                case DELETE:
                    listener.onEntitiesDeleted(entities, isImport);
                    break;
            }
        }
    }

        private List<Referenceable> toReferenceables(List<AtlasEntityHeader> entityHeaders, EntityOperation operation) throws AtlasBaseException {
        List<Referenceable> ret = new ArrayList<>(entityHeaders.size());

        // delete notifications don't need all attributes. Hence the special handling for delete operation
        if (operation == EntityOperation.DELETE) {
            for (AtlasEntityHeader entityHeader : entityHeaders) {
                ret.add(new Referenceable(entityHeader.getGuid(), entityHeader.getTypeName(), entityHeader.getAttributes()));
            }
        } else {
            for (AtlasEntityHeader entityHeader : entityHeaders) {
                ret.add(toReferenceable(entityHeader.getGuid()));
            }
        }

        return ret;
    }

    private List<Referenceable> toReferenceables(List<AtlasRelatedObjectId> entityIds) throws AtlasBaseException {
        List<Referenceable> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(entityIds)) {
            for (AtlasRelatedObjectId relatedObjectId : entityIds) {
                String entityGuid = relatedObjectId.getGuid();

                ret.add(toReferenceable(entityGuid));
            }
        }

        return ret;
    }

    private Referenceable toReferenceable(String entityId) throws AtlasBaseException {
        Referenceable ret = null;

        if (StringUtils.isNotEmpty(entityId)) {
            ret = instanceConverter.getReferenceable(entityId);
        }

        return ret;
    }

        private List<Struct> toStruct(List<AtlasClassification> classifications) throws AtlasBaseException {
        List<Struct> ret = null;

        if (classifications != null) {
            ret = new ArrayList<>(classifications.size());

            for (AtlasClassification classification : classifications) {
                if (classification != null) {
                    ret.add(instanceConverter.getTrait(classification));
                }
            }
        }

        return ret;
    }

    private List<AtlasEntity> toAtlasEntities(List<AtlasEntityHeader> entityHeaders) throws AtlasBaseException {
        List<AtlasEntity> ret = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(entityHeaders)) {
            for (AtlasEntityHeader entityHeader : entityHeaders) {
                String                 entityGuid        = entityHeader.getGuid();
                String                 typeName          = entityHeader.getTypeName();

                // Skip all internal types as the HARD DELETE will cause lookup errors
                AtlasEntityType entityType = atlasTypeRegistry.getEntityTypeByName(typeName);
                if (Objects.nonNull(entityType) && entityType.isInternalType()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Skipping internal type = {}", typeName);
                    }
                    continue;
                }

                AtlasEntityWithExtInfo entityWithExtInfo = instanceConverter.getAndCacheEntity(entityGuid);

                if (entityWithExtInfo != null) {
                    ret.add(entityWithExtInfo.getEntity());
                }
            }
        }

        return ret;
    }

    private void doFullTextMapping(List<AtlasEntityHeader> entityHeaders) {
        if (CollectionUtils.isEmpty(entityHeaders)) {
            return;
        }

        try {
            if(!AtlasRepositoryConfiguration.isFullTextSearchEnabled()) {
                return;
            }
        } catch (AtlasException e) {
            LOG.warn("Unable to determine if FullText is disabled. Proceeding with FullText mapping");
        }

        for (AtlasEntityHeader entityHeader : entityHeaders) {
            if(GraphHelper.isInternalType(entityHeader.getTypeName())) {
                continue;
            }

            String      guid   = entityHeader.getGuid();
            AtlasVertex vertex = AtlasGraphUtilsV1.findByGuid(guid);

            if(vertex == null) {
                continue;
            }

            try {
                String fullText = fullTextMapperV2.getIndexTextForEntity(guid);

                GraphHelper.setProperty(vertex, Constants.ENTITY_TEXT_PROPERTY_KEY, fullText);
            } catch (AtlasBaseException e) {
                LOG.error("FullText mapping failed for Vertex[ guid = {} ]", guid, e);
            }
        }
    }

    private void updateFullTextMapping(String entityId, List<AtlasClassification> classifications) {
        try {
            if(!AtlasRepositoryConfiguration.isFullTextSearchEnabled()) {
                return;
            }
        } catch (AtlasException e) {
            LOG.warn("Unable to determine if FullText is disabled. Proceeding with FullText mapping");
        }

        if (StringUtils.isEmpty(entityId) || CollectionUtils.isEmpty(classifications)) {
            return;
        }

        AtlasVertex atlasVertex = AtlasGraphUtilsV1.findByGuid(entityId);
        if(atlasVertex == null || GraphHelper.isInternalType(atlasVertex)) {
            return;
        }

        try {
            String classificationFullText = fullTextMapperV2.getIndexTextForClassifications(entityId, classifications);
            String existingFullText = (String) GraphHelper.getProperty(atlasVertex, Constants.ENTITY_TEXT_PROPERTY_KEY);

            String newFullText = existingFullText + " " + classificationFullText;
            GraphHelper.setProperty(atlasVertex, Constants.ENTITY_TEXT_PROPERTY_KEY, newFullText);
        } catch (AtlasBaseException e) {
            LOG.error("FullText mapping failed for Vertex[ guid = {} ]", entityId, e);
        }
    }

    private void doFullTextMapping(String guid) {
        AtlasEntityHeader entityHeader = new AtlasEntityHeader();
        entityHeader.setGuid(guid);

        doFullTextMapping(Collections.singletonList(entityHeader));
    }
}