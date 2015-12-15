/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.atlas.notification.entity.EntityNotification;
import org.apache.atlas.notification.hook.HookNotification;

import java.util.List;

// TODO : docs!
public interface NotificationInterface {

    String PROPERTY_PREFIX = "atlas.notification";

    enum NotificationType {
        HOOK(HookNotification.HookNotificationMessage.class), ENTITIES(EntityNotification.class);

        private final Class classType;

        NotificationType(Class classType) {
            this.classType = classType;
        }

        public Class getClassType() {
            return classType;
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

    <T> void send(NotificationType type, T... messages) throws NotificationException;

    <T> void send(NotificationType type, List<T> messages) throws NotificationException;

    void close();
}
