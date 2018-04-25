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
package org.apache.atlas.notification;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.listener.EntityChangeListenerV2;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.v1.model.notification.EntityNotificationV2;
import org.apache.atlas.v1.model.notification.EntityNotificationV2.OperationType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.notification.NotificationEntityChangeListener.ATLAS_ENTITY_NOTIFICATION_PROPERTY;
import static org.apache.atlas.notification.NotificationInterface.NotificationType.ENTITIES;
import static org.apache.atlas.repository.graph.GraphHelper.isInternalType;
import static org.apache.atlas.v1.model.notification.EntityNotificationV2.OperationType.*;

@Component
public class EntityNotificationListenerV2 implements EntityChangeListenerV2 {
    private final AtlasTypeRegistry         typeRegistry;
    private final NotificationInterface     notificationInterface;
    private final Configuration             configuration;
    private final Map<String, List<String>> notificationAttributesCache = new HashMap<>();

    @Inject
    public EntityNotificationListenerV2(AtlasTypeRegistry typeRegistry,
                                        NotificationInterface notificationInterface,
                                        Configuration configuration) {
        this.typeRegistry          = typeRegistry;
        this.notificationInterface = notificationInterface;
        this.configuration = configuration;
    }

    @Override
    public void onEntitiesAdded(List<AtlasEntity> entities, boolean isImport) throws AtlasBaseException {
        notifyEntityEvents(entities, ENTITY_CREATE);
    }

    @Override
    public void onEntitiesUpdated(List<AtlasEntity> entities, boolean isImport) throws AtlasBaseException {
        notifyEntityEvents(entities, ENTITY_UPDATE);
    }

    @Override
    public void onEntitiesDeleted(List<AtlasEntity> entities, boolean isImport) throws AtlasBaseException {
        notifyEntityEvents(entities, ENTITY_DELETE);
    }

    @Override
    public void onClassificationsAdded(AtlasEntity entity, List<AtlasClassification> classifications) throws AtlasBaseException {
        notifyEntityEvents(Collections.singletonList(entity), CLASSIFICATION_ADD);
    }

    @Override
    public void onClassificationsUpdated(AtlasEntity entity, List<AtlasClassification> classifications) throws AtlasBaseException {
        notifyEntityEvents(Collections.singletonList(entity), CLASSIFICATION_UPDATE);
    }

    @Override
    public void onClassificationsDeleted(AtlasEntity entity, List<AtlasClassification> classifications) throws AtlasBaseException {
        notifyEntityEvents(Collections.singletonList(entity), CLASSIFICATION_DELETE);
    }

    private void notifyEntityEvents(List<AtlasEntity> entities, OperationType operationType) throws AtlasBaseException {
        List<EntityNotificationV2> messages = new ArrayList<>();

        for (AtlasEntity entity : entities) {
            if (isInternalType(entity.getTypeName())) {
                continue;
            }

            filterNotificationAttributes(entity);

            messages.add(new EntityNotificationV2(entity, operationType, getAllClassifications(entity)));
        }

        if (!messages.isEmpty()) {
            try {
                notificationInterface.send(ENTITIES, messages);
            } catch (NotificationException e) {
                throw new AtlasBaseException(AtlasErrorCode.ENTITY_NOTIFICATION_FAILED, e, operationType.name());
            }
        }
    }

    private List<AtlasClassification> getAllClassifications(AtlasEntity entity) {
        List<AtlasClassification> ret = getAllClassifications(entity.getClassifications(), typeRegistry);

        return ret;
    }

    private static List<AtlasClassification> getAllClassifications(List<AtlasClassification> classifications, AtlasTypeRegistry typeRegistry) {
        List<AtlasClassification> ret = new LinkedList<>();

        if (CollectionUtils.isNotEmpty(classifications)) {
            for (AtlasClassification classification : classifications) {
                AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classification.getTypeName());
                Set<String>             superTypeNames     = classificationType != null ? classificationType.getAllSuperTypes() : null;

                ret.add(classification);

                if (CollectionUtils.isNotEmpty(superTypeNames)) {
                    for (String superTypeName : superTypeNames) {
                        AtlasClassification superTypeClassification = new AtlasClassification(superTypeName);

                        superTypeClassification.setEntityGuid(classification.getEntityGuid());
                        superTypeClassification.setPropagate(classification.isPropagate());

                        if (MapUtils.isNotEmpty(classification.getAttributes())) {
                            AtlasClassificationType superType = typeRegistry.getClassificationTypeByName(superTypeName);

                            if (superType != null && MapUtils.isNotEmpty(superType.getAllAttributes())) {
                                Map<String, Object> superTypeClassificationAttributes = new HashMap<>();

                                for (Map.Entry<String, Object> attrEntry : classification.getAttributes().entrySet()) {
                                    String attrName = attrEntry.getKey();

                                    if (superType.getAllAttributes().containsKey(attrName)) {
                                        superTypeClassificationAttributes.put(attrName, attrEntry.getValue());
                                    }
                                }

                                superTypeClassification.setAttributes(superTypeClassificationAttributes);
                            }
                        }

                        ret.add(superTypeClassification);
                    }
                }
            }
        }

        return ret;
    }

    private void filterNotificationAttributes(AtlasEntity entity) {
        Map<String, Object> attributesMap     = entity.getAttributes();
        List<String>        notificationAttrs = getNotificationAttributes(entity.getTypeName());

        if (MapUtils.isNotEmpty(attributesMap) && CollectionUtils.isNotEmpty(notificationAttrs)) {
            Collection<String> attributesToRemove = CollectionUtils.subtract(attributesMap.keySet(), notificationAttrs);

            for (String attributeToRemove : attributesToRemove) {
                attributesMap.remove(attributeToRemove);
            }
        }
    }

    private List<String> getNotificationAttributes(String entityType) {
        List<String> ret = null;

        if (notificationAttributesCache.containsKey(entityType)) {
            ret = notificationAttributesCache.get(entityType);
        } else if (configuration != null) {
            String attributesToIncludeKey = ATLAS_ENTITY_NOTIFICATION_PROPERTY + "." + entityType + "." + "attributes.include";
            String[] notificationAttributes = configuration.getStringArray(attributesToIncludeKey);

            if (notificationAttributes != null) {
                ret = Arrays.asList(notificationAttributes);
            }

            notificationAttributesCache.put(entityType, ret);
        }

        return ret;
    }
}