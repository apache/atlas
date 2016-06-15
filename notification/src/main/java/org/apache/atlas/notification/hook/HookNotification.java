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
package org.apache.atlas.notification.hook;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Contains the structure of messages transferred from hooks to atlas.
 */
public class HookNotification implements JsonDeserializer<HookNotification.HookNotificationMessage> {

    @Override
    public HookNotificationMessage deserialize(JsonElement json, Type typeOfT,
                                               JsonDeserializationContext context) {
        HookNotificationType type =
                context.deserialize(((JsonObject) json).get("type"), HookNotificationType.class);
        switch (type) {
        case ENTITY_CREATE:
            return context.deserialize(json, EntityCreateRequest.class);

        case ENTITY_FULL_UPDATE:
            return context.deserialize(json, EntityUpdateRequest.class);

        case ENTITY_PARTIAL_UPDATE:
            return context.deserialize(json, EntityPartialUpdateRequest.class);

        case ENTITY_DELETE:
            return context.deserialize(json, EntityDeleteRequest.class);

        case TYPE_CREATE:
        case TYPE_UPDATE:
            return context.deserialize(json, TypeRequest.class);

        default:
            throw new IllegalStateException("Unhandled type " + type);
        }
    }

    /**
     * Type of the hook message.
     */
    public enum HookNotificationType {
        TYPE_CREATE, TYPE_UPDATE, ENTITY_CREATE, ENTITY_PARTIAL_UPDATE, ENTITY_FULL_UPDATE, ENTITY_DELETE
    }

    /**
     * Base type of hook message.
     */
    public static class HookNotificationMessage {
        public static final String UNKNOW_USER = "UNKNOWN";
        protected HookNotificationType type;
        protected String user;

        private HookNotificationMessage() {
        }

        public HookNotificationMessage(HookNotificationType type, String user) {
            this.type = type;
            this.user = user;
        }

        public HookNotificationType getType() {
            return type;
        }

        public String getUser() {
            if (StringUtils.isEmpty(user)) {
                return UNKNOW_USER;
            }
            return user;
        }


    }

    /**
     * Hook message for create type definitions.
     */
    public static class TypeRequest extends HookNotificationMessage {
        private TypesDef typesDef;

        private TypeRequest() {
        }

        public TypeRequest(HookNotificationType type, TypesDef typesDef, String user) {
            super(type, user);
            this.typesDef = typesDef;
        }

        public TypesDef getTypesDef() {
            return typesDef;
        }
    }

    /**
     * Hook message for creating new entities.
     */
    public static class EntityCreateRequest extends HookNotificationMessage {
        private List<Referenceable> entities;

        private EntityCreateRequest() {
        }

        public EntityCreateRequest(String user, Referenceable... entities) {
            this(HookNotificationType.ENTITY_CREATE, Arrays.asList(entities), user);
        }

        public EntityCreateRequest(String user, List<Referenceable> entities) {
            this(HookNotificationType.ENTITY_CREATE, entities, user);
        }

        protected EntityCreateRequest(HookNotificationType type, List<Referenceable> entities, String user) {
            super(type, user);
            this.entities = entities;
        }

        public EntityCreateRequest(String user, JSONArray jsonArray) {
            super(HookNotificationType.ENTITY_CREATE, user);
            entities = new ArrayList<>();
            for (int index = 0; index < jsonArray.length(); index++) {
                try {
                    entities.add(InstanceSerialization.fromJsonReferenceable(jsonArray.getString(index), true));
                } catch (JSONException e) {
                    throw new JsonParseException(e);
                }
            }
        }

        public List<Referenceable> getEntities() throws JSONException {
            return entities;
        }

        @Override
        public String toString() {
            return entities.toString();
        }
    }

    /**
     * Hook message for updating entities(full update).
     */
    public static class EntityUpdateRequest extends EntityCreateRequest {
        public EntityUpdateRequest(String user, Referenceable... entities) {
            this(user, Arrays.asList(entities));
        }

        public EntityUpdateRequest(String user, List<Referenceable> entities) {
            super(HookNotificationType.ENTITY_FULL_UPDATE, entities, user);
        }
    }

    /**
     * Hook message for updating entities(partial update).
     */
    public static class EntityPartialUpdateRequest extends HookNotificationMessage {
        private String typeName;
        private String attribute;
        private Referenceable entity;
        private String attributeValue;

        private EntityPartialUpdateRequest() {
        }

        public EntityPartialUpdateRequest(String user, String typeName, String attribute, String attributeValue,
                                          Referenceable entity) {
            super(HookNotificationType.ENTITY_PARTIAL_UPDATE, user);
            this.typeName = typeName;
            this.attribute = attribute;
            this.attributeValue = attributeValue;
            this.entity = entity;
        }

        public String getTypeName() {
            return typeName;
        }

        public String getAttribute() {
            return attribute;
        }

        public Referenceable getEntity() {
            return entity;
        }

        public String getAttributeValue() {
            return attributeValue;
        }

        @Override
        public String toString() {
            return  "{"
                + "entityType='" + typeName + '\''
                + ", attribute=" + attribute
                + ", value=" + attributeValue
                + ", entity=" + entity
                + '}';
        }
    }

    /**
     * Hook message for creating new entities.
     */
    public static class EntityDeleteRequest extends HookNotificationMessage {

        private String typeName;
        private String attribute;
        private String attributeValue;

        private EntityDeleteRequest() {
        }

        public EntityDeleteRequest(String user, String typeName, String attribute, String attributeValue) {
            this(HookNotificationType.ENTITY_DELETE, user, typeName, attribute, attributeValue);
        }

        protected EntityDeleteRequest(HookNotificationType type,
            String user, String typeName, String attribute, String attributeValue) {
            super(type, user);
            this.typeName = typeName;
            this.attribute = attribute;
            this.attributeValue = attributeValue;
        }

        public String getTypeName() {
            return typeName;
        }

        public String getAttribute() {
            return attribute;
        }

        public String getAttributeValue() {
            return attributeValue;
        }

        @Override
        public String toString() {
            return  "{"
                + "entityType='" + typeName + '\''
                + ", attribute=" + attribute
                + ", value=" + attributeValue
                + '}';
        }
    }
}
