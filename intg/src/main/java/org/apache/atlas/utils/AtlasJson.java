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
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.atlas.model.notification.EntityNotification.EntityNotificationType;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.HookNotificationType;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.atlas.v1.model.notification.EntityNotificationV1;
import org.apache.atlas.v1.model.notification.HookNotificationV1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Date;


public class AtlasJson {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasJson.class);

    private static final ObjectMapper mapper = new ObjectMapper()
                                            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    private static final ObjectMapper mapperV1 = new ObjectMapper()
                                            .configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    static {
        SimpleModule atlasSerDeModule = new SimpleModule("AtlasSerDe", new Version(1, 0, 0, null));

        atlasSerDeModule.addSerializer(Date.class, new DateSerializer());
        atlasSerDeModule.addDeserializer(Date.class, new DateDeserializer());
        atlasSerDeModule.addDeserializer(HookNotification.class, new HookNotificationDeserializer());
        atlasSerDeModule.addDeserializer(EntityNotification.class, new EntityNotificationDeserializer());

        mapperV1.registerModule(atlasSerDeModule);
    }

    public static String toJson(Object obj) {
        String ret;
        try {
            ret = mapper.writeValueAsString(obj);
        }catch (IOException e){
            LOG.error("AtlasJson.toJson()", e);

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

            if (ret instanceof Struct) {
                ((Struct) ret).normalize();
            }
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
}
