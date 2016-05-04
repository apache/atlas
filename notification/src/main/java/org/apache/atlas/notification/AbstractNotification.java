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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jettison.json.JSONArray;


import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract notification interface implementation.
 */
public abstract class AbstractNotification implements NotificationInterface {

    /**
     * The current expected version for notification messages.
     */
    public static final MessageVersion CURRENT_MESSAGE_VERSION = new MessageVersion("1.0.0");

    private static final String PROPERTY_EMBEDDED = PROPERTY_PREFIX + ".embedded";
    private final boolean embedded;
    private final boolean isHAEnabled;

    /**
     * Used for message serialization.
     */
    public static final Gson GSON = new GsonBuilder().
        registerTypeAdapter(IReferenceableInstance.class, new ReferenceableSerializer()).
        registerTypeAdapter(Referenceable.class, new ReferenceableSerializer()).
        registerTypeAdapter(JSONArray.class, new JSONArraySerializer()).
        create();


    // ----- Constructors ----------------------------------------------------

    public AbstractNotification(Configuration applicationProperties) throws AtlasException {
        this.embedded = applicationProperties.getBoolean(PROPERTY_EMBEDDED, false);
        this.isHAEnabled = HAConfiguration.isHAEnabled(applicationProperties);
    }


    // ----- NotificationInterface -------------------------------------------

    @Override
    public <T> void send(NotificationType type, List<T> messages) throws NotificationException {
        String[] strMessages = new String[messages.size()];
        for (int index = 0; index < messages.size(); index++) {
            strMessages[index] = getMessageJson(messages.get(index));
        }
        sendInternal(type, strMessages);
    }

    @Override
    public <T> void send(NotificationType type, T... messages) throws NotificationException {
        send(type, Arrays.asList(messages));
    }

    // ----- AbstractNotification --------------------------------------------

    /**
     * Determine whether or not the notification service embedded in Atlas server.
     *
     * @return true if the the notification service embedded in Atlas server.
     */
    protected final boolean isEmbedded() {
        return embedded;
    }

    /**
     * Determine whether or not the high availability feature is enabled.
     *
     * @return true if the high availability feature is enabled.
     */
    protected final boolean isHAEnabled() {
        return isHAEnabled;
    }

    /**
     * Send the given messages.
     *
     * @param type      the message type
     * @param messages  the array of messages to send
     *
     * @throws NotificationException if an error occurs while sending
     */
    protected abstract void sendInternal(NotificationType type, String[] messages) throws NotificationException;


    // ----- utility methods -------------------------------------------------

    /**
     * Get the notification message JSON from the given object.
     *
     * @param message  the message in object form
     *
     * @return the message as a JSON string
     */
    public static String getMessageJson(Object message) {
        VersionedMessage<?> versionedMessage = new VersionedMessage<>(CURRENT_MESSAGE_VERSION, message);

        return GSON.toJson(versionedMessage);
    }


    // ----- serializers -----------------------------------------------------

    /**
     * Serializer for Referenceable.
     */
    public static final class ReferenceableSerializer implements JsonSerializer<IReferenceableInstance> {
        @Override
        public JsonElement serialize(IReferenceableInstance src, Type typeOfSrc, JsonSerializationContext context) {
            String instanceJson = InstanceSerialization.toJson(src, true);
            return new JsonParser().parse(instanceJson).getAsJsonObject();
        }
    }

    /**
     * Serializer for JSONArray.
     */
    public static final class JSONArraySerializer implements JsonSerializer<JSONArray> {
        @Override
        public JsonElement serialize(JSONArray src, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonParser().parse(src.toString()).getAsJsonArray();
        }
    }
}
