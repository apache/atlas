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

import com.google.gson.reflect.TypeToken;
import org.apache.atlas.notification.entity.EntityMessageDeserializer;
import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.notification.hook.HookMessageDeserializer;
import org.apache.atlas.notification.hook.HookNotification;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Interface to the Atlas notification framework.
 * <p>
 * Use this interface to create consumers and to send messages of a given notification type.
 * <ol>
 *   <li>Atlas sends entity notifications
 *   <li>Hooks send notifications to create/update types/entities. Atlas reads these messages
 * </ol>
 */
public interface NotificationInterface {

    /**
     * Prefix for Atlas notification related configuration properties.
     */
    String PROPERTY_PREFIX = "atlas.notification";

    /**
     * Notification message class types.
     */
    Class<HookNotification.HookNotificationMessage> HOOK_NOTIFICATION_CLASS =
        HookNotification.HookNotificationMessage.class;

    Class<EntityNotification> ENTITY_NOTIFICATION_CLASS = EntityNotification.class;

    /**
     * Versioned notification message class types.
     */
    Type HOOK_VERSIONED_MESSAGE_TYPE =
        new TypeToken<AtlasNotificationMessage<HookNotification.HookNotificationMessage>>(){}.getType();

    Type ENTITY_VERSIONED_MESSAGE_TYPE = new TypeToken<AtlasNotificationMessage<EntityNotification>>(){}.getType();

    /**
     * Atlas notification types.
     */
    enum NotificationType {

        // Notifications from the Atlas integration hooks.
        HOOK(HOOK_NOTIFICATION_CLASS, new HookMessageDeserializer()),

        // Notifications to entity change consumers.
        ENTITIES(ENTITY_NOTIFICATION_CLASS, new EntityMessageDeserializer());


        /**
         * The notification class associated with this type.
         */
        private final Class classType;

        /**
         * The message deserializer for this type.
         */
        private final MessageDeserializer deserializer;


        NotificationType(Class classType, MessageDeserializer<?> deserializer) {
            this.classType = classType;
            this.deserializer = deserializer;
        }


        // ----- accessors ---------------------------------------------------

        public Class getClassType() {
            return classType;
        }

        public MessageDeserializer getDeserializer() {
            return deserializer;
        }
    }

    /**
     * Create notification consumers for the given notification type.
     *
     * @param notificationType  the notification type (i.e. HOOK, ENTITIES)
     * @param numConsumers      the number of consumers to create
     * @param <T>               the type of the notifications
     *
     * @return the list of created consumers
     */
    <T> List<NotificationConsumer<T>> createConsumers(NotificationType notificationType, int numConsumers);

    /**
     * Send the given messages.
     *
     * @param type      the message type
     * @param messages  the messages to send
     * @param <T>       the message type
     *
     * @throws NotificationException if an error occurs while sending
     */
    <T> void send(NotificationType type, T... messages) throws NotificationException;

    /**
     * Send the given messages.
     *
     * @param type      the message type
     * @param messages  the list of messages to send
     * @param <T>       the message type
     *
     * @throws NotificationException if an error occurs while sending
     */
    <T> void send(NotificationType type, List<T> messages) throws NotificationException;

    /**
     * Shutdown any notification producers and consumers associated with this interface instance.
     */
    void close();
}
