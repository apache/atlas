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
package org.apache.atlas.notification.preprocessor;

import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.atlas.model.instance.AtlasObjectId.KEY_GUID;


public class PreprocessorContext {
    private static final Logger LOG = LoggerFactory.getLogger(PreprocessorContext.class);

    public enum PreprocessAction { NONE, IGNORE, PRUNE }

    private final AtlasKafkaMessage<HookNotification> kafkaMessage;
    private final AtlasTypeRegistry                   typeRegistry;
    private final AtlasEntitiesWithExtInfo            entitiesWithExtInfo;
    private final List<Pattern>                       hiveTablesToIgnore;
    private final List<Pattern>                       hiveTablesToPrune;
    private final Map<String, PreprocessAction>       hiveTablesCache;
    private final boolean                             hiveTypesRemoveOwnedRefAttrs;
    private final boolean                             rdbmsTypesRemoveOwnedRefAttrs;
    private final Set<String>                         ignoredEntities        = new HashSet<>();
    private final Set<String>                         prunedEntities         = new HashSet<>();
    private final Set<String>                         referredEntitiesToMove = new HashSet<>();
    private final Set<String>                         createdEntities        = new HashSet<>();
    private final Set<String>                         deletedEntities        = new HashSet<>();
    private final Map<String, String>                 guidAssignments        = new HashMap<>();
    private       List<AtlasEntity>                   postUpdateEntities     = null;

    public PreprocessorContext(AtlasKafkaMessage<HookNotification> kafkaMessage, AtlasTypeRegistry typeRegistry, List<Pattern> hiveTablesToIgnore, List<Pattern> hiveTablesToPrune, Map<String, PreprocessAction> hiveTablesCache, boolean hiveTypesRemoveOwnedRefAttrs, boolean rdbmsTypesRemoveOwnedRefAttrs) {
        this.kafkaMessage                  = kafkaMessage;
        this.typeRegistry                  = typeRegistry;
        this.hiveTablesToIgnore            = hiveTablesToIgnore;
        this.hiveTablesToPrune             = hiveTablesToPrune;
        this.hiveTablesCache               = hiveTablesCache;
        this.hiveTypesRemoveOwnedRefAttrs  = hiveTypesRemoveOwnedRefAttrs;
        this.rdbmsTypesRemoveOwnedRefAttrs = rdbmsTypesRemoveOwnedRefAttrs;

        final HookNotification  message = kafkaMessage.getMessage();

        switch (message.getType()) {
            case ENTITY_CREATE_V2:
                entitiesWithExtInfo = ((HookNotification.EntityCreateRequestV2) message).getEntities();
            break;

            case ENTITY_FULL_UPDATE_V2:
                entitiesWithExtInfo = ((HookNotification.EntityUpdateRequestV2) message).getEntities();
            break;

            default:
                entitiesWithExtInfo = null;
            break;
        }
    }

    public AtlasKafkaMessage<HookNotification> getKafkaMessage() {
        return kafkaMessage;
    }

    public long getKafkaMessageOffset() {
        return kafkaMessage.getOffset();
    }

    public int getKafkaPartition() {
        return kafkaMessage.getPartition();
    }

    public boolean getHiveTypesRemoveOwnedRefAttrs() { return hiveTypesRemoveOwnedRefAttrs; }

    public boolean getRdbmsTypesRemoveOwnedRefAttrs() { return rdbmsTypesRemoveOwnedRefAttrs; }

    public boolean isHivePreprocessEnabled() {
        return !hiveTablesToIgnore.isEmpty() || !hiveTablesToPrune.isEmpty() || hiveTypesRemoveOwnedRefAttrs;
    }

    public List<AtlasEntity> getEntities() {
        return entitiesWithExtInfo != null ? entitiesWithExtInfo.getEntities() : null;
    }

    public Map<String, AtlasEntity> getReferredEntities() {
        return entitiesWithExtInfo != null ? entitiesWithExtInfo.getReferredEntities() : null;
    }

    public AtlasEntity getEntity(String guid) {
        return entitiesWithExtInfo != null && guid != null ? entitiesWithExtInfo.getEntity(guid) : null;
    }

    public AtlasEntity removeReferredEntity(String guid) {
        Map<String, AtlasEntity> referredEntities = getReferredEntities();

        return referredEntities != null && guid != null ? referredEntities.remove(guid) : null;
    }

    public Set<String> getIgnoredEntities() { return ignoredEntities; }

    public Set<String> getPrunedEntities() { return prunedEntities; }

    public Set<String> getReferredEntitiesToMove() { return referredEntitiesToMove; }

    public Set<String> getCreatedEntities() { return createdEntities; }

    public Set<String> getDeletedEntities() { return deletedEntities; }

    public Map<String, String> getGuidAssignments() { return guidAssignments; }

    public List<AtlasEntity> getPostUpdateEntities() { return postUpdateEntities; }

    public PreprocessAction getPreprocessActionForHiveTable(String qualifiedName) {
        PreprocessAction ret = PreprocessAction.NONE;

        if (qualifiedName != null && (CollectionUtils.isNotEmpty(hiveTablesToIgnore) || CollectionUtils.isNotEmpty(hiveTablesToPrune))) {
            ret = hiveTablesCache.get(qualifiedName);

            if (ret == null) {
                if (isMatch(qualifiedName, hiveTablesToIgnore)) {
                    ret = PreprocessAction.IGNORE;
                } else if (isMatch(qualifiedName, hiveTablesToPrune)) {
                    ret = PreprocessAction.PRUNE;
                } else {
                    ret = PreprocessAction.NONE;
                }

                hiveTablesCache.put(qualifiedName, ret);
            }
        }

        return ret;
    }

    public boolean isIgnoredEntity(String guid) {
        return guid != null ? ignoredEntities.contains(guid) : false;
    }

    public boolean isPrunedEntity(String guid) {
        return guid != null ? prunedEntities.contains(guid) : false;
    }

    public void addToIgnoredEntities(AtlasEntity entity) {
        if (!ignoredEntities.contains(entity.getGuid())) {
            ignoredEntities.add(entity.getGuid());

            LOG.info("ignored entity: typeName={}, qualifiedName={}. topic-offset={}, partition={}", entity.getTypeName(), EntityPreprocessor.getQualifiedName(entity), getKafkaMessageOffset(), getKafkaPartition());
        }
    }

    public void addToPrunedEntities(AtlasEntity entity) {
        if (!prunedEntities.contains(entity.getGuid())) {
            prunedEntities.add(entity.getGuid());

            LOG.info("pruned entity: typeName={}, qualifiedName={} topic-offset={}, partition={}", entity.getTypeName(), EntityPreprocessor.getQualifiedName(entity), getKafkaMessageOffset(), getKafkaPartition());
        }
    }

    public void addToIgnoredEntities(String guid) {
        if (guid != null) {
            ignoredEntities.add(guid);
        }
    }

    public void addToPrunedEntities(String guid) {
        if (guid != null) {
            prunedEntities.add(guid);
        }
    }

    public void addToReferredEntitiesToMove(String guid) {
        if (guid != null) {
            referredEntitiesToMove.add(guid);
        }
    }

    public void addToReferredEntitiesToMove(Collection<String> guids) {
        if (guids != null) {
            for (String guid : guids) {
                addToReferredEntitiesToMove(guid);
            }
        }
    }

    public void addToIgnoredEntities(Object obj) {
        collectGuids(obj, ignoredEntities);
    }

    public void addToPrunedEntities(Object obj) {
        collectGuids(obj, prunedEntities);
    }

    public void removeRefAttributeAndRegisterToMove(AtlasEntity entity, String attrName, String relationshipType, String refAttrName) {
        Object attrVal = entity.removeAttribute(attrName);

        if (attrVal != null) {
            AtlasRelatedObjectId entityId = null;
            Set<String>          guids    = new HashSet<>();

            collectGuids(attrVal, guids);

            // removed attrVal might have elements removed (e.g. removed column); to handle this case register the entity for partial update
            addToPostUpdate(entity, attrName, attrVal);

            for (String guid : guids) {
                AtlasEntity refEntity = getEntity(guid);

                if (refEntity != null) {
                    Object refAttr = null;

                    if (refEntity.hasRelationshipAttribute(refAttrName)) {
                        refAttr = refEntity.getRelationshipAttribute(refAttrName);
                    } else if (refEntity.hasAttribute(refAttrName)) {
                        refAttr = refEntity.getAttribute(refAttrName);
                    } else {
                        if (entityId == null) {
                            entityId = AtlasTypeUtil.toAtlasRelatedObjectId(entity, typeRegistry);
                        }

                        refAttr = entityId;
                    }

                    if (refAttr != null) {
                        refAttr = setRelationshipType(refAttr, relationshipType);
                    }

                    if (refAttr != null) {
                        refEntity.setRelationshipAttribute(refAttrName, refAttr);
                    }

                    addToReferredEntitiesToMove(guid);
                }
            }
        }
    }

    public void moveRegisteredReferredEntities() {
        List<AtlasEntity>        entities         = getEntities();
        Map<String, AtlasEntity> referredEntities = getReferredEntities();

        if (entities != null && referredEntities != null && !referredEntitiesToMove.isEmpty()) {
            AtlasEntity firstEntity = entities.isEmpty() ? null : entities.get(0);

            for (String guid : referredEntitiesToMove) {
                AtlasEntity entity = referredEntities.remove(guid);

                if (entity != null) {
                    entities.add(entity);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("moved referred entity: typeName={}, qualifiedName={}. topic-offset={}, partition={}", entity.getTypeName(), EntityPreprocessor.getQualifiedName(entity), kafkaMessage.getOffset(), kafkaMessage.getPartition());
                    }
                }
            }

            if (firstEntity != null) {
                LOG.info("moved {} referred-entities to end of entities-list (firstEntity:typeName={}, qualifiedName={}). topic-offset={}, partition={}", referredEntitiesToMove.size(), firstEntity.getTypeName(), EntityPreprocessor.getQualifiedName(firstEntity), kafkaMessage.getOffset(), kafkaMessage.getPartition());
            } else {
                LOG.info("moved {} referred-entities to entities-list. topic-offset={}, partition={}", referredEntitiesToMove.size(), kafkaMessage.getOffset(), kafkaMessage.getPartition());
            }

            referredEntitiesToMove.clear();
        }
    }

    public void prepareForPostUpdate() {
        if (postUpdateEntities != null) {
            ListIterator<AtlasEntity> iter = postUpdateEntities.listIterator();

            while (iter.hasNext()) {
                AtlasEntity entity       = iter.next();
                String      assignedGuid = getAssignedGuid(entity.getGuid());

                // no need to perform partial-update for entities that are created/deleted while processing this message
                if (createdEntities.contains(assignedGuid) || deletedEntities.contains(assignedGuid)) {
                    iter.remove();
                } else {
                    entity.setGuid(assignedGuid);

                    if (entity.getAttributes() != null) {
                        setAssignedGuids(entity.getAttributes().values());
                    }

                    if (entity.getRelationshipAttributes() != null) {
                        setAssignedGuids(entity.getRelationshipAttributes().values());
                    }
                }
            }
        }
    }

    public String getTypeName(Object obj) {
        Object ret = null;

        if (obj instanceof AtlasObjectId) {
            ret = ((AtlasObjectId) obj).getTypeName();
        } else if (obj instanceof Map) {
            ret = ((Map) obj).get(AtlasObjectId.KEY_TYPENAME);
        } else if (obj instanceof AtlasEntity) {
            ret = ((AtlasEntity) obj).getTypeName();
        } else if (obj instanceof AtlasEntity.AtlasEntityWithExtInfo) {
            ret = ((AtlasEntity.AtlasEntityWithExtInfo) obj).getEntity().getTypeName();
        }

        return ret != null ? ret.toString() : null;
    }

    public String getGuid(Object obj) {
        Object ret = null;

        if (obj instanceof AtlasObjectId) {
            ret = ((AtlasObjectId) obj).getGuid();
        } else if (obj instanceof Map) {
            ret = ((Map) obj).get(KEY_GUID);
        } else if (obj instanceof AtlasEntity) {
            ret = ((AtlasEntity) obj).getGuid();
        } else if (obj instanceof AtlasEntity.AtlasEntityWithExtInfo) {
            ret = ((AtlasEntity.AtlasEntityWithExtInfo) obj).getEntity().getGuid();
        }

        return ret != null ? ret.toString() : null;
    }

    public void collectGuids(Object obj, Set<String> guids) {
        if (obj != null) {
            if (obj instanceof Collection) {
                Collection objList = (Collection) obj;

                for (Object objElem : objList) {
                    collectGuids(objElem, guids);
                }
            } else {
                collectGuid(obj, guids);
            }
        }
    }

    public void collectGuid(Object obj, Set<String> guids) {
        String guid = getGuid(obj);

        if (guid != null) {
            guids.add(guid);
        }
    }


    private boolean isMatch(String name, List<Pattern> patterns) {
        boolean ret = false;

        for (Pattern p : patterns) {
            if (p.matcher(name).matches()) {
                ret = true;

                break;
            }
        }

        return ret;
    }

    private AtlasRelatedObjectId setRelationshipType(Object attr, String relationshipType) {
        AtlasRelatedObjectId ret = null;

        if (attr instanceof AtlasRelatedObjectId) {
            ret = (AtlasRelatedObjectId) attr;
        } else if (attr instanceof AtlasObjectId) {
            ret = new AtlasRelatedObjectId((AtlasObjectId) attr);
        } else if (attr instanceof Map) {
            ret = new AtlasRelatedObjectId((Map) attr);
        }

        if (ret != null) {
            ret.setRelationshipType(relationshipType);
        }

        return ret;
    }

    private String getAssignedGuid(String guid) {
        String ret = guidAssignments.get(guid);

        return ret != null ? ret : guid;
    }

    private void setAssignedGuids(Object obj) {
        if (obj != null) {
            if (obj instanceof Collection) {
                Collection objList = (Collection) obj;

                for (Object objElem : objList) {
                    setAssignedGuids(objElem);
                }
            } else {
                setAssignedGuid(obj);
            }
        }
    }

    private void setAssignedGuid(Object obj) {
        if (obj instanceof AtlasRelatedObjectId) {
            AtlasRelatedObjectId objId = (AtlasRelatedObjectId) obj;

            objId.setGuid(getAssignedGuid(objId.getGuid()));
        } else if (obj instanceof AtlasObjectId) {
            AtlasObjectId objId = (AtlasObjectId) obj;

            objId.setGuid(getAssignedGuid(objId.getGuid()));
        } else if (obj instanceof Map) {
            Map    objId = (Map) obj;
            Object guid  = objId.get(KEY_GUID);

            if (guid != null) {
                objId.put(KEY_GUID, getAssignedGuid(guid.toString()));
            }
        }
    }

    private void addToPostUpdate(AtlasEntity entity, String attrName, Object attrVal) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("addToPostUpdate(guid={}, entityType={}, attrName={}", entity.getGuid(), entity.getTypeName(), attrName);
        }

        AtlasEntity partialEntity = null;

        if (postUpdateEntities == null) {
            postUpdateEntities = new ArrayList<>();
        }

        for (AtlasEntity existing : postUpdateEntities) {
            if (StringUtils.equals(entity.getGuid(), existing.getGuid())) {
                partialEntity = existing;

                break;
            }
        }

        if (partialEntity == null) {
            partialEntity = new AtlasEntity(entity.getTypeName(), attrName, attrVal);

            partialEntity.setGuid(entity.getGuid());

            postUpdateEntities.add(partialEntity);
        } else {
            partialEntity.setAttribute(attrName, attrVal);
        }
    }
}
