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
package org.apache.atlas.type;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationType;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.HookNotificationType;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.v1.model.notification.EntityNotificationV1;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityCreateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityDeleteRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityPartialUpdateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.EntityUpdateRequest;
import org.apache.atlas.v1.model.notification.HookNotificationV1.TypeRequest;
import org.codehaus.jackson.*;
import org.codehaus.jackson.map.*;
import org.codehaus.jackson.map.module.SimpleModule;
import org.codehaus.jackson.node.ObjectNode;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;
import java.util.List;


/**
 * base class that declares interface for all Atlas types.
 */

public abstract class AtlasType {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasStructType.class);

    private static final ObjectMapper mapper = new ObjectMapper()
                                            .configure(DeserializationConfig.Feature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    private static final ObjectMapper mapperV1 = new ObjectMapper()
                                            .configure(DeserializationConfig.Feature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    static {
        SimpleModule atlasSerDeModule = new SimpleModule("AtlasSerDe", new Version(1, 0, 0, null));

        atlasSerDeModule.addSerializer(Date.class, new DateSerializer());
        atlasSerDeModule.addDeserializer(Date.class, new DateDeserializer());
        atlasSerDeModule.addDeserializer(HookNotification.class, new HookNotificationDeserializer());
        atlasSerDeModule.addDeserializer(EntityNotification.class, new EntityNotificationDeserializer());

        mapperV1.registerModule(atlasSerDeModule);
    }


    private final String       typeName;
    private final TypeCategory typeCategory;

    protected AtlasType(AtlasBaseTypeDef typeDef) {
        this(typeDef.getName(), typeDef.getCategory());
    }

    protected AtlasType(String typeName, TypeCategory typeCategory) {
        this.typeName     = typeName;
        this.typeCategory = typeCategory;
    }

    void resolveReferences(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
    }

    void resolveReferencesPhase2(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
    }

    void resolveReferencesPhase3(AtlasTypeRegistry typeRegistry) throws AtlasBaseException {
    }

    public String getTypeName() { return typeName; }

    public TypeCategory getTypeCategory() { return typeCategory; }

    public abstract Object createDefaultValue();

    public Object createOptionalDefaultValue() {
        return createDefaultValue();
    }

    public Object createDefaultValue(Object val){
        return val == null ? createDefaultValue() : getNormalizedValue(val);
    }

    public abstract boolean isValidValue(Object obj);

    public abstract Object getNormalizedValue(Object obj);

    public boolean validateValue(Object obj, String objName, List<String> messages) {
        boolean ret = isValidValue(obj);

        if (!ret) {
            messages.add(objName + "=" + obj + ": invalid value for type " + getTypeName());
        }

        return ret;
    }

    public boolean isValidValueForUpdate(Object obj) { return isValidValue(obj); }

    public Object getNormalizedValueForUpdate(Object obj) { return getNormalizedValue(obj); }

    public boolean validateValueForUpdate(Object obj, String objName, List<String> messages) {
        return validateValue(obj, objName, messages);
    }

    /* for attribute of entity-type, the value would be of AtlasObjectId
     * when an attribute instance is created i.e. AtlasAttribute, this method
     * will be called to get AtlasEntityType replaced with AtlasObjectType
     */
    public AtlasType getTypeForAttribute() {
        return this;
    }

    public static String toJson(Object obj) {
        String ret;
        try {
            ret = mapper.writeValueAsString(obj);
        }catch (IOException e){
            LOG.error("AtlasType.toJson()", e);

            ret = null;
        }
        return ret;
    }

    public static <T> T fromJson(String jsonStr, Class<T> type) {
        T ret;
        try {
            ret =  mapper.readValue(jsonStr, type);
        }catch (IOException e){
            LOG.error("AtlasType.fromJson()", e);

            ret = null;
        }
        return ret;
    }

    public static String toV1Json(Object obj) {
        String ret;
        try {
            ret = mapperV1.writeValueAsString(obj);
        }catch (IOException e){
            LOG.error("AtlasType.toV1Json()", e);

            ret = null;
        }
        return ret;
    }

    public static <T> T fromV1Json(String jsonStr, Class<T> type) {
        T ret;
        try {
            ret =  mapperV1.readValue(jsonStr, type);
        }catch (IOException e){
            LOG.error("AtlasType.fromV1Json()", e);

            ret = null;
        }
        return ret;
    }

    public static <T> T fromV1Json(String jsonStr, TypeReference<T> type) {
        T ret;
        try {
            ret =  mapperV1.readValue(jsonStr, type);
        }catch (IOException e){
            LOG.error("AtlasType.toV1Json()", e);

            ret = null;
        }
        return ret;
    }

    static class DateSerializer extends JsonSerializer<Date> {
        @Override
        public void serialize(Date value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            if (value != null) {
                jgen.writeString(AtlasBaseTypeDef.DATE_FORMATTER.format(value));
            }
        }
    }

    static class DateDeserializer extends JsonDeserializer<Date> {
        @Override
        public Date deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            Date ret = null;

            String value = parser.readValueAs(String.class);

            if (value != null) {
                try {
                    ret = AtlasBaseTypeDef.DATE_FORMATTER.parse(value);
                } catch (ParseException excp) {
                }
            }

            return ret;
        }
    }

    static class HookNotificationDeserializer extends JsonDeserializer<HookNotification> {
        @Override
        public HookNotification deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            HookNotification     ret              = null;
            ObjectMapper         mapper           = (ObjectMapper) parser.getCodec();
            ObjectNode           root             = (ObjectNode) mapper.readTree(parser);
            JsonNode             typeNode         = root != null ? root.get("type") : null;
            String               strType          = typeNode != null ? typeNode.asText() : null;
            HookNotificationType notificationType = strType != null ? HookNotificationType.valueOf(strType) : null;

            if (notificationType != null) {
                switch (notificationType) {
                    case TYPE_CREATE:
                    case TYPE_UPDATE:
                        ret = mapper.readValue(root, TypeRequest.class);
                        break;

                    case ENTITY_CREATE:
                        ret = mapper.readValue(root, EntityCreateRequest.class);
                        break;

                    case ENTITY_PARTIAL_UPDATE:
                        ret = mapper.readValue(root, EntityPartialUpdateRequest.class);
                        break;

                    case ENTITY_FULL_UPDATE:
                        ret = mapper.readValue(root, EntityUpdateRequest.class);
                        break;

                    case ENTITY_DELETE:
                        ret = mapper.readValue(root, EntityDeleteRequest.class);
                        break;
                }
            }

            return ret;
        }
    }

    static class EntityNotificationDeserializer extends JsonDeserializer<EntityNotification> {
        @Override
        public EntityNotification deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            EntityNotification     ret              = null;
            ObjectMapper           mapper           = (ObjectMapper) parser.getCodec();
            ObjectNode             root             = (ObjectNode) mapper.readTree(parser);
            JsonNode               typeNode         = root != null ? root.get("type") : null;
            String                 strType          = typeNode != null ? typeNode.asText() : null;
            EntityNotificationType notificationType = strType != null ? EntityNotificationType.valueOf(strType) : EntityNotificationType.ENTITY_NOTIFICATION_V1;

            if (root != null) {
                switch (notificationType) {
                    case ENTITY_NOTIFICATION_V1:
                        ret = mapper.readValue(root, EntityNotificationV1.class);
                        break;
                }
            }

            return ret;
        }
    }
}
