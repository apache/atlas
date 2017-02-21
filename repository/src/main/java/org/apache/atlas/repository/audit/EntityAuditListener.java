/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.audit;

import org.apache.atlas.AtlasException;
import org.apache.atlas.EntityAuditEvent;
import org.apache.atlas.EntityAuditEvent.EntityAuditAction;
import org.apache.atlas.RequestContextV1;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Listener on entity create/update/delete, tag add/delete. Adds the corresponding audit event to the audit repository.
 */
public class EntityAuditListener implements EntityChangeListener {
    private static final Logger LOG = LoggerFactory.getLogger(EntityAuditListener.class);

    private EntityAuditRepository auditRepository;

    @Inject
    public EntityAuditListener(EntityAuditRepository auditRepository) {
        this.auditRepository = auditRepository;
    }

    @Override
    public void onEntitiesAdded(Collection<ITypedReferenceableInstance> entities) throws AtlasException {
        List<EntityAuditEvent> events = new ArrayList<>();
        for (ITypedReferenceableInstance entity : entities) {
            EntityAuditEvent event = createEvent(entity, EntityAuditAction.ENTITY_CREATE);
            events.add(event);
        }

        auditRepository.putEvents(events);
    }

    @Override
    public void onEntitiesUpdated(Collection<ITypedReferenceableInstance> entities) throws AtlasException {
        List<EntityAuditEvent> events = new ArrayList<>();
        for (ITypedReferenceableInstance entity : entities) {
            EntityAuditEvent event = createEvent(entity, EntityAuditAction.ENTITY_UPDATE);
            events.add(event);
        }

        auditRepository.putEvents(events);
    }

    @Override
    public void onTraitAdded(ITypedReferenceableInstance entity, IStruct trait) throws AtlasException {
        EntityAuditEvent event = createEvent(entity, EntityAuditAction.TAG_ADD,
                                             "Added trait: " + InstanceSerialization.toJson(trait, true));

        auditRepository.putEvents(event);
    }

    @Override
    public void onTraitDeleted(ITypedReferenceableInstance entity, String traitName) throws AtlasException {
        EntityAuditEvent event = createEvent(entity, EntityAuditAction.TAG_DELETE, "Deleted trait: " + traitName);

        auditRepository.putEvents(event);
    }

    @Override
    public void onEntitiesDeleted(Collection<ITypedReferenceableInstance> entities) throws AtlasException {
        List<EntityAuditEvent> events = new ArrayList<>();
        for (ITypedReferenceableInstance entity : entities) {
            EntityAuditEvent event = createEvent(entity, EntityAuditAction.ENTITY_DELETE, "Deleted entity");
            events.add(event);
        }

        auditRepository.putEvents(events);
    }

    public List<EntityAuditEvent> getAuditEvents(String guid) throws AtlasException{
        return auditRepository.listEvents(guid, null, (short) 10);
    }

    private EntityAuditEvent createEvent(ITypedReferenceableInstance entity, EntityAuditAction action)
            throws AtlasException {
        String detail = getAuditEventDetail(entity, action);

        return createEvent(entity, action, detail);
    }

    private EntityAuditEvent createEvent(ITypedReferenceableInstance entity, EntityAuditAction action, String details)
            throws AtlasException {
        return new EntityAuditEvent(entity.getId()._getId(), RequestContextV1.get().getRequestTime(), RequestContextV1.get().getUser(), action, details, entity);
    }

    private String getAuditEventDetail(ITypedReferenceableInstance entity, EntityAuditAction action) throws AtlasException {
        Map<String, Object> prunedAttributes = pruneEntityAttributesForAudit(entity);

        String auditPrefix  = getAuditPrefix(action);
        String auditString  = auditPrefix + InstanceSerialization.toJson(entity, true);
        byte[] auditBytes   = auditString.getBytes(StandardCharsets.UTF_8);
        long   auditSize    = auditBytes != null ? auditBytes.length : 0;
        long   auditMaxSize = auditRepository.repositoryMaxSize();

        if (auditMaxSize >= 0 && auditSize > auditMaxSize) { // don't store attributes in audit
            LOG.warn("audit record too long: entityType={}, guid={}, size={}; maxSize={}. entity attribute values not stored in audit",
                    entity.getTypeName(), entity.getId()._getId(), auditSize, auditMaxSize);

            Map<String, Object> attrValues = entity.getValuesMap();

            clearAttributeValues(entity);

            auditString = auditPrefix + InstanceSerialization.toJson(entity, true);

            addAttributeValues(entity, attrValues);
        }

        restoreEntityAttributes(entity, prunedAttributes);

        return auditString;
    }

    private void clearAttributeValues(IReferenceableInstance entity) throws AtlasException {
        Map<String, Object> attributesMap = entity.getValuesMap();

        if (MapUtils.isNotEmpty(attributesMap)) {
            for (String attribute : attributesMap.keySet()) {
                entity.setNull(attribute);
            }
        }
    }

    private void addAttributeValues(ITypedReferenceableInstance entity, Map<String, Object> attributesMap) throws AtlasException {
        if (MapUtils.isNotEmpty(attributesMap)) {
            for (String attr : attributesMap.keySet()) {
                entity.set(attr, attributesMap.get(attr));
            }
        }
    }

    private Map<String, Object> pruneEntityAttributesForAudit(ITypedReferenceableInstance entity) throws AtlasException {
        Map<String, Object> ret               = null;
        Map<String, Object> entityAttributes  = entity.getValuesMap();
        List<String>        excludeAttributes = auditRepository.getAuditExcludeAttributes(entity.getTypeName());

        if (CollectionUtils.isNotEmpty(excludeAttributes) && MapUtils.isNotEmpty(entityAttributes)) {
            Map<String, AttributeInfo> attributeInfoMap = entity.fieldMapping().fields;

            for (String attrName : entityAttributes.keySet()) {
                Object        attrValue = entityAttributes.get(attrName);
                AttributeInfo attrInfo  = attributeInfoMap.get(attrName);

                if (excludeAttributes.contains(attrName)) {
                    if (ret == null) {
                        ret = new HashMap<>();
                    }

                    ret.put(attrName, attrValue);
                    entity.setNull(attrName);
                } else if (attrInfo.isComposite) {
                    if (attrValue instanceof Collection) {
                        for (Object attribute : (Collection) attrValue) {
                            if (attribute instanceof ITypedReferenceableInstance) {
                                ret = pruneAttributes(ret, (ITypedReferenceableInstance) attribute);
                            }
                        }
                    } else if (attrValue instanceof ITypedReferenceableInstance) {
                        ret = pruneAttributes(ret, (ITypedReferenceableInstance) attrValue);
                    }
                }
            }
        }

        return ret;
    }

    private Map<String, Object> pruneAttributes(Map<String, Object> ret, ITypedReferenceableInstance attribute) throws AtlasException {
        ITypedReferenceableInstance attrInstance = attribute;
        Map<String, Object>         prunedAttrs  = pruneEntityAttributesForAudit(attrInstance);

        if (MapUtils.isNotEmpty(prunedAttrs)) {
            if (ret == null) {
                ret = new HashMap<>();
            }

            ret.put(attrInstance.getId()._getId(), prunedAttrs);
        }
        return ret;
    }

    private void restoreEntityAttributes(ITypedReferenceableInstance entity, Map<String, Object> prunedAttributes) throws AtlasException {
        if (MapUtils.isEmpty(prunedAttributes)) {
            return;
        }

        Map<String, Object> entityAttributes = entity.getValuesMap();

        if (MapUtils.isNotEmpty(entityAttributes)) {
            Map<String, AttributeInfo> attributeInfoMap = entity.fieldMapping().fields;

            for (String attrName : entityAttributes.keySet()) {
                Object        attrValue = entityAttributes.get(attrName);
                AttributeInfo attrInfo  = attributeInfoMap.get(attrName);

                if (prunedAttributes.containsKey(attrName)) {
                    entity.set(attrName, prunedAttributes.get(attrName));
                } else if (attrInfo.isComposite) {
                    if (attrValue instanceof Collection) {
                        for (Object attributeEntity : (Collection) attrValue) {
                            if (attributeEntity instanceof ITypedReferenceableInstance) {
                                restoreAttributes(prunedAttributes, (ITypedReferenceableInstance) attributeEntity);
                            }
                        }
                    } else if (attrValue instanceof ITypedReferenceableInstance) {
                        restoreAttributes(prunedAttributes, (ITypedReferenceableInstance) attrValue);
                    }
                }
            }
        }
    }

    private void restoreAttributes(Map<String, Object> prunedAttributes, ITypedReferenceableInstance attributeEntity) throws AtlasException {
        Object                      obj          = prunedAttributes.get(attributeEntity.getId()._getId());

        if (obj instanceof Map) {
            restoreEntityAttributes(attributeEntity, (Map) obj);
        }
    }

    private String getAuditPrefix(EntityAuditAction action) {
        final String ret;

        switch (action) {
            case ENTITY_CREATE:
                ret = "Created: ";
                break;
            case ENTITY_UPDATE:
                ret = "Updated: ";
                break;
            case ENTITY_DELETE:
                ret = "Deleted: ";
                break;
            case TAG_ADD:
                ret = "Added trait: ";
                break;
            case TAG_DELETE:
                ret = "Deleted trait: ";
                break;
            default:
                ret = "Unknown: ";
        }

        return ret;
    }
}
