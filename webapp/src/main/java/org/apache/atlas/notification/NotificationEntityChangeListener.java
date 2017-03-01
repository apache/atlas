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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.notification.entity.EntityNotificationImpl;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Listen to the repository for entity changes and produce entity change notifications.
 */
public class NotificationEntityChangeListener implements EntityChangeListener {

    private final NotificationInterface notificationInterface;
    private final TypeSystem typeSystem;

    private Map<String, List<String>> notificationAttributesCache = new HashMap<>();
    private static final String ATLAS_ENTITY_NOTIFICATION_PROPERTY = "atlas.notification.entity";
    static Configuration APPLICATION_PROPERTIES = null;



    // ----- Constructors ------------------------------------------------------

    /**
     * Construct a NotificationEntityChangeListener.
     *
     * @param notificationInterface the notification framework interface
     * @param typeSystem the Atlas type system
     */
    @Inject
    public NotificationEntityChangeListener(NotificationInterface notificationInterface, TypeSystem typeSystem) {
        this.notificationInterface = notificationInterface;
        this.typeSystem = typeSystem;
    }


    // ----- EntityChangeListener ----------------------------------------------

    @Override
    public void onEntitiesAdded(Collection<ITypedReferenceableInstance> entities) throws AtlasException {
        notifyOfEntityEvent(entities, EntityNotification.OperationType.ENTITY_CREATE);
    }

    @Override
    public void onEntitiesUpdated(Collection<ITypedReferenceableInstance> entities) throws AtlasException {
        notifyOfEntityEvent(entities, EntityNotification.OperationType.ENTITY_UPDATE);
    }

    @Override
    public void onTraitsAdded(ITypedReferenceableInstance entity, Collection<? extends IStruct> traits) throws AtlasException {
        notifyOfEntityEvent(Collections.singleton(entity), EntityNotification.OperationType.TRAIT_ADD);
    }

    @Override
    public void onTraitsDeleted(ITypedReferenceableInstance entity, Collection<String> traitNames) throws AtlasException {
        notifyOfEntityEvent(Collections.singleton(entity), EntityNotification.OperationType.TRAIT_DELETE);
    }

    @Override
    public void onEntitiesDeleted(Collection<ITypedReferenceableInstance> entities) throws AtlasException {
        notifyOfEntityEvent(entities, EntityNotification.OperationType.ENTITY_DELETE);
    }


    // ----- helper methods -------------------------------------------------


    // ----- helper methods ----------------------------------------------------
    @VisibleForTesting
    public static List<IStruct> getAllTraits(IReferenceableInstance entityDefinition,
                                              TypeSystem typeSystem) throws AtlasException {
        List<IStruct> traitInfo = new LinkedList<>();
        for (String traitName : entityDefinition.getTraits()) {
            IStruct trait = entityDefinition.getTrait(traitName);
            String typeName = trait.getTypeName();
            Map<String, Object> valuesMap = trait.getValuesMap();
            traitInfo.add(new Struct(typeName, valuesMap));
            traitInfo.addAll(getSuperTraits(typeName, valuesMap, typeSystem));
        }
        return traitInfo;
    }

    private static List<IStruct> getSuperTraits(
            String typeName, Map<String, Object> values, TypeSystem typeSystem) throws AtlasException {

        List<IStruct> superTypes = new LinkedList<>();

        TraitType traitDef = typeSystem.getDataType(TraitType.class, typeName);
        Set<String> superTypeNames = traitDef.getAllSuperTypeNames();

        for (String superTypeName : superTypeNames) {
            TraitType superTraitDef = typeSystem.getDataType(TraitType.class, superTypeName);

            Map<String, Object> superTypeValues = new HashMap<>();

            FieldMapping fieldMapping = superTraitDef.fieldMapping();

            if (fieldMapping != null) {
                Set<String> superTypeAttributeNames = fieldMapping.fields.keySet();

                for (String superTypeAttributeName : superTypeAttributeNames) {
                    if (values.containsKey(superTypeAttributeName)) {
                        superTypeValues.put(superTypeAttributeName, values.get(superTypeAttributeName));
                    }
                }
            }
            IStruct superTrait = new Struct(superTypeName, superTypeValues);
            superTypes.add(superTrait);
            superTypes.addAll(getSuperTraits(superTypeName, values, typeSystem));
        }

        return superTypes;
    }

    // send notification of entity change
    private void notifyOfEntityEvent(Collection<ITypedReferenceableInstance> entityDefinitions,
                                     EntityNotification.OperationType operationType) throws AtlasException {
        List<EntityNotification> messages = new LinkedList<>();

        for (IReferenceableInstance entityDefinition : entityDefinitions) {
            Referenceable       entity                  = new Referenceable(entityDefinition);
            Map<String, Object> attributesMap           = entity.getValuesMap();
            List<String>        entityNotificationAttrs = getNotificationAttributes(entity.getTypeName());

            if (MapUtils.isNotEmpty(attributesMap) && CollectionUtils.isNotEmpty(entityNotificationAttrs)) {
                for (String entityAttr : attributesMap.keySet()) {
                    if (!entityNotificationAttrs.contains(entityAttr)) {
                        entity.setNull(entityAttr);
                    }
                }
            }

            EntityNotificationImpl notification = new EntityNotificationImpl(entity, operationType, getAllTraits(entity, typeSystem));

            messages.add(notification);
        }

        notificationInterface.send(NotificationInterface.NotificationType.ENTITIES, messages);
    }

    private List<String> getNotificationAttributes(String entityType) {
        List<String> ret = null;

        initApplicationProperties();

        if (notificationAttributesCache.containsKey(entityType)) {
            ret = notificationAttributesCache.get(entityType);
        } else if (APPLICATION_PROPERTIES != null) {
            String[] notificationAttributes = APPLICATION_PROPERTIES.getStringArray(ATLAS_ENTITY_NOTIFICATION_PROPERTY + "." +
                                                                                    entityType + "." + "attributes.include");

            if (notificationAttributes != null) {
                ret = Arrays.asList(notificationAttributes);
            }

            notificationAttributesCache.put(entityType, ret);
        }

        return ret;
    }

    private void initApplicationProperties() {
        if (APPLICATION_PROPERTIES == null) {
            try {
                APPLICATION_PROPERTIES = ApplicationProperties.get();
            } catch (AtlasException ex) {
                // ignore
            }
        }
    }
}
