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

package org.apache.atlas.notification.hook;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HookNotification implements JsonDeserializer<HookNotification.HookNotificationMessage> {

    @Override
    public HookNotificationMessage deserialize(JsonElement json, Type typeOfT,
                                               JsonDeserializationContext context) throws JsonParseException {
        if (json.isJsonArray()) {
            JSONArray jsonArray = context.deserialize(json, JSONArray.class);
            return new EntityCreateRequest(jsonArray);
        } else {
            HookNotificationType type =
                    context.deserialize(((JsonObject) json).get("type"), HookNotificationType.class);
            switch (type) {
                case ENTITY_CREATE:
                    return context.deserialize(json, EntityCreateRequest.class);

                case ENTITY_FULL_UPDATE:
                    return context.deserialize(json, EntityUpdateRequest.class);

                case ENTITY_PARTIAL_UPDATE:
                    return context.deserialize(json, EntityPartialUpdateRequest.class);

                case TYPE_CREATE:
                case TYPE_UPDATE:
                    return context.deserialize(json, TypeRequest.class);
            }
            throw new IllegalStateException("Unhandled type " + type);
        }
    }

    public enum HookNotificationType {
        TYPE_CREATE, TYPE_UPDATE, ENTITY_CREATE, ENTITY_PARTIAL_UPDATE, ENTITY_FULL_UPDATE
    }

    public static class HookNotificationMessage {
        protected HookNotificationType type;

        private HookNotificationMessage() { }

        public HookNotificationMessage(HookNotificationType type) {
            this.type = type;
        }

        public HookNotificationType getType() {
            return type;
        }
    }

    public static class TypeRequest extends HookNotificationMessage {
        private TypesDef typesDef;

        private TypeRequest() { }

        public TypeRequest(HookNotificationType type, TypesDef typesDef) {
            super(type);
            this.typesDef = typesDef;
        }

        public TypesDef getTypesDef() {
            return typesDef;
        }
    }

    public static class EntityCreateRequest extends HookNotificationMessage {
        private List<Referenceable> entities;

        private EntityCreateRequest() { }

        public EntityCreateRequest(Referenceable... entities) {
            super(HookNotificationType.ENTITY_CREATE);
            this.entities = Arrays.asList(entities);
        }

        protected EntityCreateRequest(HookNotificationType type, List<Referenceable> entities) {
            super(type);
            this.entities = entities;
        }

        public EntityCreateRequest(JSONArray jsonArray) {
            super(HookNotificationType.ENTITY_CREATE);
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
    }

    public static class EntityUpdateRequest extends EntityCreateRequest {
        public EntityUpdateRequest(Referenceable... entities) {
            this(Arrays.asList(entities));
        }

        public EntityUpdateRequest(List<Referenceable> entities) {
            super(HookNotificationType.ENTITY_FULL_UPDATE, entities);
        }
    }

    public static class EntityPartialUpdateRequest extends HookNotificationMessage {
        private String typeName;
        private String attribute;
        private Referenceable entity;
        private String attributeValue;

        private EntityPartialUpdateRequest() { }

        public EntityPartialUpdateRequest(String typeName, String attribute, String attributeValue,
                                          Referenceable entity) {
            super(HookNotificationType.ENTITY_PARTIAL_UPDATE);
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
    }
}
