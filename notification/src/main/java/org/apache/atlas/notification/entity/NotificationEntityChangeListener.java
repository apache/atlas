/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.notification.entity;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.atlas.AtlasException;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.types.TypeSystem;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Listen to the repository for entity changes and produce entity change notifications.
 */
public class NotificationEntityChangeListener implements EntityChangeListener {

    private static final Gson GSON = new GsonBuilder().
        registerTypeAdapter(Referenceable.class, new ReferencableSerializer()).create();

    private final NotificationInterface notificationInterface;
    private final TypeSystem typeSystem;


    // ----- Constructors ------------------------------------------------------

    /**
     * Construct a NotificationEntityChangeListener.
     *
     * @param notificationInterface the notification framework interface
     * @param typeSystem the Atlas type system
     */
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
    public void onTraitAdded(ITypedReferenceableInstance entity, IStruct trait) throws AtlasException {
        notifyOfEntityEvent(Collections.singleton(entity), EntityNotification.OperationType.TRAIT_ADD);
    }

    @Override
    public void onTraitDeleted(ITypedReferenceableInstance entity, String traitName) throws AtlasException {
        notifyOfEntityEvent(Collections.singleton(entity), EntityNotification.OperationType.TRAIT_DELETE);
    }


    // ----- helper methods -------------------------------------------------

    // send notification of entity change
    private void notifyOfEntityEvent(Collection<ITypedReferenceableInstance> entityDefinitions,
                                     EntityNotification.OperationType operationType) throws AtlasException {
        List<String> messages = new LinkedList<>();

        for (IReferenceableInstance entityDefinition : entityDefinitions) {
            Referenceable entity = new Referenceable(entityDefinition);

            EntityNotificationImpl notification =
                    new EntityNotificationImpl(entity, operationType, typeSystem);

            messages.add(GSON.toJson(notification));
        }

        notificationInterface.send(NotificationInterface.NotificationType.ENTITIES,
                messages.toArray(new String[messages.size()]));
    }


    // ----- inner class : ReferencableSerializer ---------------------------

    private static class ReferencableSerializer implements JsonSerializer<Referenceable>  {

        public static final JsonParser JSON_PARSER = new JsonParser();

        @Override
        public JsonElement serialize(Referenceable referenceable, Type type,
                                     JsonSerializationContext jsonSerializationContext) {
            return JSON_PARSER.parse(InstanceSerialization.toJson(referenceable, true)).getAsJsonObject();
        }
    }
}
