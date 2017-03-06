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

package org.apache.atlas;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.json.InstanceSerialization;

import java.lang.reflect.Type;

@Deprecated
public class SerDe {
    public static final Gson GSON = new GsonBuilder().
             registerTypeAdapter(IStruct.class, new StructDeserializer()).
             registerTypeAdapter(IReferenceableInstance.class, new ReferenceableSerializerDeserializer()).
             registerTypeAdapter(Referenceable.class, new ReferenceableSerializerDeserializer()).
             create();

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
}
