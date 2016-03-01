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
import com.google.gson.JsonParser;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.notification.entity.EntityNotificationImpl;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * Abstract notification consumer.
 */
public abstract class AbstractNotificationConsumer<T> implements NotificationConsumer<T> {

    public static final Gson GSON = new GsonBuilder().
            registerTypeAdapter(ImmutableList.class, new ImmutableListDeserializer()).
            registerTypeAdapter(ImmutableMap.class, new ImmutableMapDeserializer()).
            registerTypeAdapter(EntityNotification.class, new EntityNotificationDeserializer()).
            registerTypeAdapter(IStruct.class, new StructDeserializer()).
            registerTypeAdapter(IReferenceableInstance.class, new ReferenceableSerializerDeserializer()).
            registerTypeAdapter(Referenceable.class, new ReferenceableSerializerDeserializer()).
            registerTypeAdapter(JSONArray.class, new JSONArraySerializerDeserializer()).
            registerTypeAdapter(HookNotification.HookNotificationMessage.class, new HookNotification()).
            create();

    private final Class<T> type;


    // ----- Constructors ----------------------------------------------------

    /**
     * Construct an AbstractNotificationConsumer.
     *
     * @param type  the notification type
     */
    public AbstractNotificationConsumer(Class<T> type) {
        this.type = type;
    }


    // ----- AbstractNotificationConsumer -------------------------------------

    /**
     * Get the next notification as a string.
     *
     * @return the next notification in string form
     */
    protected abstract String getNext();

    /**
     * Get the next notification as a string without advancing.
     *
     * @return the next notification in string form
     */
    protected abstract String peekMessage();


    // ----- NotificationConsumer ---------------------------------------------

    @Override
    public T next() {
        return GSON.fromJson(getNext(), type);
    }

    @Override
    public T peek() {
        return GSON.fromJson(peekMessage(), type);
    }


    /**
     * Deserializer for ImmutableList used by AbstractNotificationConsumer.GSON.
     */
    public static class ImmutableListDeserializer implements JsonDeserializer<ImmutableList<?>> {
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
     * Deserializer for ImmutableMap used by AbstractNotificationConsumer.GSON.
     */
    public static class ImmutableMapDeserializer implements JsonDeserializer<ImmutableMap<?, ?>> {

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
     * Deserializer for EntityNotification used by AbstractNotificationConsumer.GSON.
     */
    public static final class EntityNotificationDeserializer implements JsonDeserializer<EntityNotification> {
        @Override
        public EntityNotification deserialize(final JsonElement json, final Type type,
                                              final JsonDeserializationContext context) {
            return context.deserialize(json, EntityNotificationImpl.class);
        }
    }

    /**
     * Serde for Struct used by AbstractNotificationConsumer.GSON.
     */
    public static final class StructDeserializer implements JsonDeserializer<IStruct>, JsonSerializer<IStruct> {
        @Override
        public IStruct deserialize(final JsonElement json, final Type type,
                                   final JsonDeserializationContext context) {
            return context.deserialize(json, Struct.class);
        }

        @Override
        public JsonElement serialize(IStruct src, Type typeOfSrc, JsonSerializationContext context) {
            String instanceJson = InstanceSerialization.toJson(src, true);
            return new JsonParser().parse(instanceJson).getAsJsonObject();
        }
    }

    /**
     * Serde for Referenceable used by AbstractNotificationConsumer.GSON.
     */
    public static final class ReferenceableSerializerDeserializer implements JsonDeserializer<IStruct>,
            JsonSerializer<IReferenceableInstance> {
        @Override
        public IReferenceableInstance deserialize(final JsonElement json, final Type type,
                                                  final JsonDeserializationContext context) {

            return InstanceSerialization.fromJsonReferenceable(json.toString(), true);
        }

        @Override
        public JsonElement serialize(IReferenceableInstance src, Type typeOfSrc, JsonSerializationContext context) {
            String instanceJson = InstanceSerialization.toJson(src, true);
            return new JsonParser().parse(instanceJson).getAsJsonObject();
        }
    }

    /**
     * Serde for JSONArray used by AbstractNotificationConsumer.GSON.
     */
    public static final class JSONArraySerializerDeserializer implements JsonDeserializer<JSONArray>,
            JsonSerializer<JSONArray> {
        @Override
        public JSONArray deserialize(final JsonElement json, final Type type,
                                     final JsonDeserializationContext context) {
            try {
                return new JSONArray(json.toString());
            } catch (JSONException e) {
                throw new JsonParseException(e.getMessage(), e);
            }
        }

        @Override
        public JsonElement serialize(JSONArray src, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonParser().parse(src.toString()).getAsJsonArray();
        }
    }
}
