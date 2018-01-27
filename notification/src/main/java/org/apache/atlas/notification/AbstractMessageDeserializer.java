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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base notification message deserializer.
 */
public abstract class AbstractMessageDeserializer<T> extends AtlasNotificationMessageDeserializer<T> {
    private static final ObjectMapper mapper = new ObjectMapper().configure(DeserializationConfig.Feature.USE_BIG_DECIMAL_FOR_FLOATS, true);

    private static final Map<Type, JsonDeserializer> DESERIALIZER_MAP = new HashMap<>();

    static {
        DESERIALIZER_MAP.put(ImmutableList.class, new ImmutableListDeserializer());
        DESERIALIZER_MAP.put(ImmutableMap.class, new ImmutableMapDeserializer());
        DESERIALIZER_MAP.put(JSONArray.class, new JSONArrayDeserializer());
        DESERIALIZER_MAP.put(IStruct.class, new StructDeserializer());
        DESERIALIZER_MAP.put(IReferenceableInstance.class, new ReferenceableDeserializer());
        DESERIALIZER_MAP.put(Referenceable.class, new ReferenceableDeserializer());
        DESERIALIZER_MAP.put(AtlasEntityWithExtInfo.class, new AtlasEntityWithExtInfoDeserializer());
        DESERIALIZER_MAP.put(AtlasEntitiesWithExtInfo.class, new AtlasEntitiesWithExtInfoDeserializer());
        DESERIALIZER_MAP.put(AtlasObjectId.class, new AtlasObjectIdDeserializer());
    }


    // ----- Constructors ----------------------------------------------------

    /**
     * Create a deserializer.
     *
     * @param notificationMessageType the type of the notification message
     * @param expectedVersion         the expected message version
     * @param deserializerMap         map of individual deserializers used to define this message deserializer
     * @param notificationLogger      logger for message version mismatch
     */
    public AbstractMessageDeserializer(Type notificationMessageType,
                                       MessageVersion expectedVersion,
                                       Map<Type, JsonDeserializer> deserializerMap,
                                       Logger notificationLogger) {
        super(notificationMessageType, expectedVersion, getDeserializer(deserializerMap), notificationLogger);
    }


    // ----- helper methods --------------------------------------------------

    private static Gson getDeserializer(Map<Type, JsonDeserializer> deserializerMap) {
        GsonBuilder builder = new GsonBuilder();

        for (Map.Entry<Type, JsonDeserializer> entry : DESERIALIZER_MAP.entrySet()) {
            builder.registerTypeAdapter(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Type, JsonDeserializer> entry : deserializerMap.entrySet()) {
            builder.registerTypeAdapter(entry.getKey(), entry.getValue());
        }
        return builder.create();
    }


    // ----- deserializer classes --------------------------------------------

    /**
     * Deserializer for ImmutableList.
     */
    protected static class ImmutableListDeserializer implements JsonDeserializer<ImmutableList<?>> {
        public static final Type LIST_TYPE = new TypeToken<List<?>>() {
        }.getType();

        @Override
        public ImmutableList<?> deserialize(JsonElement json, Type type,
                                            JsonDeserializationContext context) {
            final List<?> list = context.deserialize(json, LIST_TYPE);
            return ImmutableList.copyOf(list);
        }
    }

    /**
     * Deserializer for ImmutableMap.
     */
    protected static class ImmutableMapDeserializer implements JsonDeserializer<ImmutableMap<?, ?>> {

        public static final Type MAP_TYPE = new TypeToken<Map<?, ?>>() {
        }.getType();

        @Override
        public ImmutableMap<?, ?> deserialize(JsonElement json, Type type,
                                              JsonDeserializationContext context) {
            final Map<?, ?> map = context.deserialize(json, MAP_TYPE);
            return ImmutableMap.copyOf(map);
        }
    }

    /**
     * Deserializer for JSONArray.
     */
    public static final class JSONArrayDeserializer implements JsonDeserializer<JSONArray> {
        @Override
        public JSONArray deserialize(final JsonElement json, final Type type,
                                     final JsonDeserializationContext context) {
            try {
                return new JSONArray(json.toString());
            } catch (JSONException e) {
                throw new JsonParseException(e.getMessage(), e);
            }
        }
    }

    /**
     * Deserializer for Struct.
     */
    protected static final class StructDeserializer implements JsonDeserializer<IStruct> {
        @Override
        public IStruct deserialize(final JsonElement json, final Type type,
                                   final JsonDeserializationContext context) {
            return context.deserialize(json, Struct.class);
        }
    }

    /**
     * Deserializer for Referenceable.
     */
    protected static final class ReferenceableDeserializer implements JsonDeserializer<IReferenceableInstance> {
        @Override
        public IReferenceableInstance deserialize(final JsonElement json, final Type type,
                                                  final JsonDeserializationContext context) {

            return InstanceSerialization.fromJsonReferenceable(json.toString(), true);
        }
    }

    protected static final class AtlasEntityWithExtInfoDeserializer implements JsonDeserializer<AtlasEntityWithExtInfo> {
        @Override
        public AtlasEntityWithExtInfo deserialize(final JsonElement json, final Type type,
                                                  final JsonDeserializationContext context) throws JsonParseException {
            try {
                return mapper.readValue(json.toString(), AtlasEntityWithExtInfo.class);
            } catch (IOException excp) {
                throw new JsonParseException(excp);
            }
        }
    }

    protected static final class AtlasEntitiesWithExtInfoDeserializer implements JsonDeserializer<AtlasEntitiesWithExtInfo> {
        @Override
        public AtlasEntitiesWithExtInfo deserialize(final JsonElement json, final Type type,
                                                    final JsonDeserializationContext context) throws JsonParseException {
            try {
                return mapper.readValue(json.toString(), AtlasEntitiesWithExtInfo.class);
            } catch (IOException excp) {
                throw new JsonParseException(excp);
            }
        }
    }

    protected static final class AtlasObjectIdDeserializer implements JsonDeserializer<AtlasObjectId> {
        @Override
        public AtlasObjectId deserialize(final JsonElement json, final Type type,
                                         final JsonDeserializationContext context) throws JsonParseException {
            try {
                return mapper.readValue(json.toString(), AtlasObjectId.class);
            } catch (IOException excp) {
                throw new JsonParseException(excp);
            }
        }
    }
}
