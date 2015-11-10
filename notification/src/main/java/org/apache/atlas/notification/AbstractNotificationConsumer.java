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
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.notification.entity.EntityNotificationImpl;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
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

    private static final Gson GSON = new GsonBuilder().
            registerTypeAdapter(ImmutableList.class, new ImmutableListDeserializer()).
            registerTypeAdapter(ImmutableMap.class, new ImmutableMapDeserializer()).
            registerTypeAdapter(EntityNotification.class, new EntityNotificationDeserializer()).
            registerTypeAdapter(IStruct.class, new StructDeserializer()).
            registerTypeAdapter(IReferenceableInstance.class, new ReferenceableDeserializer()).
            registerTypeAdapter(JSONArray.class, new JSONArrayDeserializer()).
            create();

    private final Class<T> type;


    // ----- Constructors ------------------------------------------------------

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


    // ----- Iterator ---------------------------------------------------------

    @Override
    public T next() {
        return GSON.fromJson(getNext(), type);
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("The remove method is not supported.");
    }


    // ----- inner class : ImmutableListDeserializer ---------------------------

    private static class ImmutableListDeserializer implements JsonDeserializer<ImmutableList<?>> {

        public static final Type LIST_TYPE = new TypeToken<List<?>>() {}.getType();

        @Override
        public ImmutableList<?> deserialize(JsonElement json, Type type,
                                            JsonDeserializationContext context) throws JsonParseException {

            final List<?> list = context.deserialize(json, LIST_TYPE);
            return ImmutableList.copyOf(list);
        }
    }


    // ----- inner class : ImmutableMapDeserializer ----------------------------

    public static class ImmutableMapDeserializer implements JsonDeserializer<ImmutableMap<?, ?>> {

        public static final Type MAP_TYPE = new TypeToken<Map<?, ?>>() {}.getType();

        @Override
        public ImmutableMap<?, ?> deserialize(JsonElement json, Type type,
                                              JsonDeserializationContext context) throws JsonParseException {
            final Map<?, ?> map = context.deserialize(json, MAP_TYPE);
            return ImmutableMap.copyOf(map);
        }
    }


    // ----- inner class : EntityNotificationDeserializer ----------------------

    public final static class EntityNotificationDeserializer implements JsonDeserializer<EntityNotification> {
        @Override
        public EntityNotification deserialize(final JsonElement json, final Type type,
                                              final JsonDeserializationContext context) throws JsonParseException {
            return context.deserialize(json, EntityNotificationImpl.class);
        }
    }


    // ----- inner class : StructDeserializer -------------------------------

    public final static class StructDeserializer implements JsonDeserializer<IStruct> {
        @Override
        public IStruct deserialize(final JsonElement json, final Type type,
                                              final JsonDeserializationContext context) throws JsonParseException {
            return context.deserialize(json, Struct.class);
        }
    }


    // ----- inner class : ReferenceableDeserializer ------------------------

    public final static class ReferenceableDeserializer implements JsonDeserializer<IStruct> {
        @Override
        public IReferenceableInstance deserialize(final JsonElement json, final Type type,
                                   final JsonDeserializationContext context) throws JsonParseException {

            return InstanceSerialization.fromJsonReferenceable(json.toString(), true);
        }
    }


    // ----- inner class : JSONArrayDeserializer ----------------------------

    public final static class JSONArrayDeserializer implements JsonDeserializer<JSONArray> {
        @Override
        public JSONArray deserialize(final JsonElement json, final Type type,
                                                  final JsonDeserializationContext context) throws JsonParseException {

            try {
                return new JSONArray(json.toString());
            } catch (JSONException e) {
                throw new JsonParseException(e.getMessage(), e);
            }
        }
    }
}
