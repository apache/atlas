/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.notification;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.notification.MessageSource;
import org.apache.atlas.notification.entity.EntityMessageDeserializer;
import org.apache.atlas.notification.hook.HookMessageDeserializer;

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
     *
     * @param source: Name of the source
     * @param failedMessagesLogger: Logger for failed messages
     * @return
     */
    void init(String source, Object failedMessagesLogger);

    /**
     *
     * @param user Name of the user under which the processes is running
     */
    void setCurrentUser(String user);

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
    <T> void send(NotificationType type, List<T> messages, MessageSource source) throws NotificationException;

    void close();

    /**
     *  Check if underlying notification mechanism is ready for use.
     *
     * @param type tye message type
     * @return true if available, false otherwise
     *
     */
    boolean isReady(NotificationType type);

    /**
     * Abstract notification wiring for async import messages
     * @param topic async import topic to publish
     * @param messages messages to send
     * @param source source of the message
     */
    default <T> void send(String topic, List<T> messages, MessageSource source) throws NotificationException {}

    /**
     * Associates the specified topic with the given notification type.
     *
     * @param notificationType The type of notification to which the topic should be added.
     * @param topic The name of the topic to be associated with the notification type.
     */
    default void addTopicToNotificationType(NotificationType notificationType, String topic) throws AtlasBaseException {}

    /**
     * Closes the producer associated with the specified notification type and topic.
     *
     * @param notificationType The type of notification for which the producer is to be closed.
     * @param topic The name of the topic associated with the producer.
     */
    default void closeProducer(NotificationType notificationType, String topic) {}

    /**
     * Deletes the specified topic associated with the given notification type.
     *
     * @param notificationType The type of notification related to the topic.
     * @param topicName The name of the topic to be deleted.
     */
    default void deleteTopic(NotificationType notificationType, String topicName) {}

    /**
     * Closes the consumer associated with the specified notification type.
     *
     * @param notificationType The type of notification for which the consumer is to be closed.
     * @param topic The consumer to close with assignment.
     *
     */
    default void closeConsumer(NotificationType notificationType, String topic) {}

    /**
     * Atlas notification types.
     */
    enum NotificationType {
        // Notifications from the Atlas integration hooks.
        HOOK(new HookMessageDeserializer()),

        // Notifications from the Atlas integration hooks - unsorted.
        HOOK_UNSORTED(new HookMessageDeserializer()),

        // Notifications to entity change consumers.
        ENTITIES(new EntityMessageDeserializer()),

        // Notifications from Atlas async importer
        ASYNC_IMPORT(new HookMessageDeserializer());

        private final AtlasNotificationMessageDeserializer deserializer;

        NotificationType(AtlasNotificationMessageDeserializer deserializer) {
            this.deserializer = deserializer;
        }

        public AtlasNotificationMessageDeserializer getDeserializer() {
            return deserializer;
        }
    }
}
