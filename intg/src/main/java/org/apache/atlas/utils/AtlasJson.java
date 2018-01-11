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
package org.apache.atlas.utils;


import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasFullTextResult;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationType;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.HookNotificationType;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.v1.model.instance.AtlasSystemAttributes;
import org.apache.atlas.v1.model.instance.Id;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.atlas.v1.model.notification.EntityNotificationV1;
import org.apache.atlas.v1.model.notification.HookNotificationV1.*;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class AtlasJson {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJson.class);

    private static final ObjectMapper mapper = new ObjectMapper()
                                            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    private static final ObjectMapper mapperV1 = new ObjectMapper()
                                            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    private static final ObjectMapper mapperV1Search = new ObjectMapper()
            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    static {
        SimpleModule atlasSerDeModule = new SimpleModule("AtlasSerDe", new Version(1, 0, 0, null));

        atlasSerDeModule.addSerializer(Date.class, new DateSerializer());
        atlasSerDeModule.addDeserializer(Date.class, new DateDeserializer());
        atlasSerDeModule.addDeserializer(HookNotification.class, new HookNotificationDeserializer());
        atlasSerDeModule.addDeserializer(EntityNotification.class, new EntityNotificationDeserializer());

        mapperV1.registerModule(atlasSerDeModule);

        SimpleModule searchResultV1SerDeModule = new SimpleModule("SearchResultV1SerDe", new Version(1, 0, 0, null));

        searchResultV1SerDeModule.addSerializer(Referenceable.class, new V1SearchReferenceableSerializer());
        searchResultV1SerDeModule.addSerializer(Struct.class, new V1SearchStructSerializer());
        searchResultV1SerDeModule.addSerializer(Id.class, new V1SearchIdSerializer());
        searchResultV1SerDeModule.addSerializer(AtlasSystemAttributes.class, new V1SearchSystemAttributesSerializer());
        searchResultV1SerDeModule.addSerializer(AtlasFullTextResult.class, new V1SearchFullTextResultSerializer());
        searchResultV1SerDeModule.addSerializer(Date.class, new DateSerializer());

        mapperV1Search.registerModule(searchResultV1SerDeModule);
    }

    public static String toJson(Object obj) {
        String ret;
        try {
            if (obj instanceof JsonNode && ((JsonNode) obj).isTextual()) {
                ret = ((JsonNode) obj).textValue();
            } else {
                ret = mapperV1.writeValueAsString(obj);
            }
        }catch (IOException e){
            LOG.error("AtlasJson.toJson()", e);

            ret = null;
        }
        return ret;
    }

    public static <T> T fromJson(String jsonStr, Class<T> type) {
        T ret = null;

        if (jsonStr != null) {
            try {
                ret = mapper.readValue(jsonStr, type);
            } catch (IOException e) {
                LOG.error("AtlasType.fromJson()", e);

                ret = null;
            }
        }

        return ret;
    }

    public static String toV1Json(Object obj) {
        String ret;
        try {
            if (obj instanceof JsonNode && ((JsonNode) obj).isTextual()) {
                ret = ((JsonNode) obj).textValue();
            } else {
                ret = mapperV1.writeValueAsString(obj);
            }
        } catch (IOException e) {
            LOG.error("AtlasType.toV1Json()", e);

            ret = null;
        }
        return ret;
    }

    public static <T> T fromV1Json(String jsonStr, Class<T> type) {
        T ret = null;

        if (jsonStr != null) {
            try {
                ret = mapperV1.readValue(jsonStr, type);

                if (ret instanceof Struct) {
                    ((Struct) ret).normalize();
                }
            } catch (IOException e) {
                LOG.error("AtlasType.fromV1Json()", e);

                ret = null;
            }
        }

        return ret;
    }

    public static <T> T fromV1Json(String jsonStr, TypeReference<T> type) {
        T ret = null;

        if (jsonStr != null) {
            try {
                ret = mapperV1.readValue(jsonStr, type);
            } catch (IOException e) {
                LOG.error("AtlasType.toV1Json()", e);

                ret = null;
            }
        }

        return ret;
    }

    public static String toV1SearchJson(Object obj) {
        String ret;
        try {
            ret = mapperV1Search.writeValueAsString(obj);
        }catch (IOException e){
            LOG.error("AtlasType.toV1Json()", e);

            ret = null;
        }
        return ret;
    }


    public static ObjectNode createV1ObjectNode() {
        return mapperV1.createObjectNode();
    }

    public static ObjectNode createV1ObjectNode(String key, Object value) {
        ObjectNode ret = mapperV1.createObjectNode();

        ret.putPOJO(key, value);

        return ret;
    }

    public static ArrayNode createV1ArrayNode() {
        return mapperV1.createArrayNode();
    }

    public static ArrayNode createV1ArrayNode(Collection<?> array) {
        ArrayNode ret = mapperV1.createArrayNode();

        for (Object elem : array) {
            ret.addPOJO(elem);
        }

        return ret;
    }


    public static JsonNode parseToV1JsonNode(String json) throws IOException {
        JsonNode jsonNode = mapperV1.readTree(json);

        return jsonNode;
    }

    public static ArrayNode parseToV1ArrayNode(String json) throws IOException {
        JsonNode jsonNode = mapperV1.readTree(json);

        if (jsonNode instanceof ArrayNode) {
            return (ArrayNode)jsonNode;
        }

        throw new IOException("not an array");
    }

    public static ArrayNode parseToV1ArrayNode(Collection<String> jsonStrings) throws IOException {
        ArrayNode ret = createV1ArrayNode();

        for (String json : jsonStrings) {
            JsonNode jsonNode = mapperV1.readTree(json);

            ret.add(jsonNode);
        }

        return ret;
    }

    static class DateSerializer extends JsonSerializer<Date> {
        @Override
        public void serialize(Date value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            if (value != null) {
                jgen.writeString(AtlasBaseTypeDef.getDateFormatter().format(value));
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
                    ret = AtlasBaseTypeDef.getDateFormatter().parse(value);
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
            ObjectCodec          mapper           = parser.getCodec();
            TreeNode             root             = mapper.readTree(parser);
            JsonNode             typeNode         = root != null ? (JsonNode) root.get("type") : null;
            String               strType          = typeNode != null ? typeNode.asText() : null;
            HookNotificationType notificationType = strType != null ? HookNotificationType.valueOf(strType) : null;

            if (notificationType != null) {
                switch (notificationType) {
                    case TYPE_CREATE:
                    case TYPE_UPDATE:
                        ret = mapper.treeToValue(root, TypeRequest.class);
                        break;

                    case ENTITY_CREATE:
                        ret = mapper.treeToValue(root, EntityCreateRequest.class);
                        break;

                    case ENTITY_PARTIAL_UPDATE:
                        ret = mapper.treeToValue(root, EntityPartialUpdateRequest.class);
                        break;

                    case ENTITY_FULL_UPDATE:
                        ret = mapper.treeToValue(root, EntityUpdateRequest.class);
                        break;

                    case ENTITY_DELETE:
                        ret = mapper.treeToValue(root, EntityDeleteRequest.class);
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
            ObjectCodec            mapper           = parser.getCodec();
            TreeNode               root             = mapper.readTree(parser);
            JsonNode               typeNode         = root != null ? (JsonNode) root.get("type") : null;
            String                 strType          = typeNode != null ? typeNode.asText() : null;
            EntityNotificationType notificationType = strType != null ? EntityNotificationType.valueOf(strType) : EntityNotificationType.ENTITY_NOTIFICATION_V1;

            if (root != null) {
                switch (notificationType) {
                    case ENTITY_NOTIFICATION_V1:
                        ret = mapper.treeToValue(root, EntityNotificationV1.class);
                        break;
                }
            }

            return ret;
        }
    }

    private static final String V1_KEY_$TYPENAME          = "$typeName$";
    private static final String V1_KEY_$ID                = "$id$";
    private static final String V1_KEY_$SYSTEM_ATTRIBUTES = "$systemAttributes$";
    private static final String V1_KEY_$TRAITS            = "$traits$";
    private static final String V1_KEY_TYPENAME           = "typeName";
    private static final String V1_KEY_ID                 = "id";
    private static final String V1_KEY_GUID               = "guid";
    private static final String V1_KEY_SCORE              = "score";
    private static final String V1_KEY_VERSION            = "version";
    private static final String V1_KEY_STATE              = "state";
    private static final String V1_KEY_CREATED_BY         = "createdBy";
    private static final String V1_KEY_MODIFIED_BY        = "modifiedBy";
    private static final String V1_KEY_CREATED_TIME       = "createdTime";
    private static final String V1_KEY_MODIFIED_TIME      = "modifiedTime";

    static class V1SearchReferenceableSerializer extends JsonSerializer<Referenceable> {
        @Override
        public void serialize(Referenceable entity, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            if (entity != null) {
                Map<String, Object> valueMap = entity.getValues() != null ? new HashMap<>(entity.getValues()) : new HashMap<>();

                if (entity.getTypeName() != null) {
                    valueMap.put(V1_KEY_$TYPENAME, entity.getTypeName());
                }

                if (entity.getId() != null) {
                    valueMap.put(V1_KEY_$ID, entity.getId());
                }

                if (entity.getSystemAttributes() != null) {
                    valueMap.put(V1_KEY_$SYSTEM_ATTRIBUTES, entity.getSystemAttributes());
                }

                if (MapUtils.isNotEmpty(entity.getTraits())) {
                    valueMap.put(V1_KEY_$TRAITS, entity.getTraits());
                }

                jgen.writeObject(valueMap);
            }
        }
    }

    static class V1SearchStructSerializer extends JsonSerializer<Struct> {
        @Override
        public void serialize(Struct struct, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            if (struct != null) {
                Map<String, Object> valueMap = struct.getValues() != null ? new HashMap<>(struct.getValues()) : new HashMap<>();

                valueMap.put(V1_KEY_$TYPENAME, struct.getTypeName());

                jgen.writeObject(valueMap);
            }
        }
    }

    static class V1SearchIdSerializer extends JsonSerializer<Id> {
        @Override
        public void serialize(Id id, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            if (id != null) {
                Map<String, Object> valueMap = new HashMap<>();

                valueMap.put(V1_KEY_ID, id._getId());
                valueMap.put(V1_KEY_$TYPENAME, id.getTypeName());
                valueMap.put(V1_KEY_VERSION, id.getVersion());

                if (id.getState() != null) {
                    valueMap.put(V1_KEY_STATE, id.getState().toString());
                }

                jgen.writeObject(valueMap);
            }
        }
    }

    static class V1SearchSystemAttributesSerializer extends JsonSerializer<AtlasSystemAttributes> {
        private static final ThreadLocal<DateFormat> V1_SEARCH_RESULT_DATE_FORMAT = new ThreadLocal<DateFormat>() {
            @Override
            public DateFormat initialValue() {
                DateFormat ret = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy");

                return ret;
            }
        };

        @Override
        public void serialize(AtlasSystemAttributes systemAttributes, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            if (systemAttributes != null) {
                Map<String, Object> valueMap = new HashMap<>();

                valueMap.put(V1_KEY_CREATED_BY, systemAttributes.getCreatedBy());
                valueMap.put(V1_KEY_MODIFIED_BY, systemAttributes.getModifiedBy());

                if (systemAttributes.getCreatedTime() != null) {
                    valueMap.put(V1_KEY_CREATED_TIME, V1_SEARCH_RESULT_DATE_FORMAT.get().format(systemAttributes.getCreatedTime()));
                }

                if (systemAttributes.getModifiedTime() != null) {
                    valueMap.put(V1_KEY_MODIFIED_TIME, V1_SEARCH_RESULT_DATE_FORMAT.get().format(systemAttributes.getModifiedTime()));
                }

                jgen.writeObject(valueMap);
            }
        }
    }

    static class V1SearchFullTextResultSerializer extends JsonSerializer<AtlasFullTextResult> {
        @Override
        public void serialize(AtlasFullTextResult result, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            if (result != null && result.getEntity() != null) {
                Map<String, Object> valueMap = new HashMap<>();

                valueMap.put(V1_KEY_GUID, result.getEntity().getGuid());
                valueMap.put(V1_KEY_TYPENAME, result.getEntity().getTypeName());
                valueMap.put(V1_KEY_SCORE, result.getScore());

                jgen.writeObject(valueMap);
            }
        }
    }
}
