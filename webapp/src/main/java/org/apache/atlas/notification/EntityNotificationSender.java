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

import org.apache.atlas.GraphTransactionInterceptor;
import org.apache.atlas.RequestContext;
import org.apache.atlas.model.notification.EntityNotification;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.notification.NotificationInterface.NotificationType.ENTITIES;
import static org.apache.atlas.notification.NotificationInterface.NotificationType.RELATIONSHIPS;


public class EntityNotificationSender<T> {
    private static final Logger LOG = LoggerFactory.getLogger(EntityNotificationSender.class);

    private final static boolean NOTIFY_POST_COMMIT_DEFAULT = true;

    private final NotificationSender<T> notificationSender;

    public EntityNotificationSender(NotificationInterface notificationInterface, Configuration configuration) {
        this(notificationInterface, configuration != null ? configuration.getBoolean("atlas.notification.send.postcommit", NOTIFY_POST_COMMIT_DEFAULT) : NOTIFY_POST_COMMIT_DEFAULT);
    }

    public EntityNotificationSender(NotificationInterface notificationInterface, boolean sendPostCommit) {
        if (sendPostCommit) {
            LOG.info("EntityNotificationSender: notifications will be sent after transaction commit");

            this.notificationSender = new PostCommitNotificationSender(notificationInterface);
        } else {
            LOG.info("EntityNotificationSender: notifications will be sent inline (i.e. not waiting for transaction to commit)");

            this.notificationSender = new InlineNotificationSender(notificationInterface);
        }
    }

    public void send(EntityNotification.EntityNotificationV2.OperationType operationType, List<T> notifications) throws NotificationException {
        this.notificationSender.send(operationType, notifications);
    }


    private interface NotificationSender<T> {
        void send(EntityNotification.EntityNotificationV2.OperationType operationType, List<T> notifications) throws NotificationException;
    }

    private class InlineNotificationSender<T> implements NotificationSender<T> {
        private final NotificationInterface notificationInterface;

        public InlineNotificationSender(NotificationInterface notificationInterface) {
            this.notificationInterface = notificationInterface;
        }

        @Override
        public void send(EntityNotification.EntityNotificationV2.OperationType operationType, List<T> notifications) throws NotificationException {
            if (isRelationshipEvent(operationType))
                notificationInterface.send(RELATIONSHIPS, notifications);
            else
                notificationInterface.send(ENTITIES, notifications);
        }
    }

    private class PostCommitNotificationSender<T> implements NotificationSender<T> {
        private final NotificationInterface                   notificationInterface;
        private final ThreadLocal<PostCommitNotificationHook> postCommitNotificationHooks = new ThreadLocal<>();

        public PostCommitNotificationSender(NotificationInterface notificationInterface) {
            this.notificationInterface = notificationInterface;
        }

        @Override
        public void send(EntityNotification.EntityNotificationV2.OperationType operationType, List<T> notifications) throws NotificationException {
            PostCommitNotificationHook notificationHook = postCommitNotificationHooks.get();

            if (notificationHook == null) {
                notificationHook = new PostCommitNotificationHook(operationType, notifications);
                postCommitNotificationHooks.set(notificationHook);
            } else {
                if (isRelationshipEvent(operationType)) notificationHook.addRelationshipNotifications(notifications);
                else notificationHook.addNotifications(notifications);
            }

            if (RequestContext.get().isAlternatePath()) {
                notificationHook.onComplete(true);
            }
        }

        class PostCommitNotificationHook<T> extends GraphTransactionInterceptor.PostTransactionHook {
            private final List<T> notifications = new ArrayList<>();
            private final List<T> relationshipNotifications = new ArrayList<>();

            public PostCommitNotificationHook(EntityNotification.EntityNotificationV2.OperationType operationType, List<T> notifications) {
                if (isRelationshipEvent(operationType))
                    this.addRelationshipNotifications(notifications);
                else
                    this.addNotifications(notifications);
            }

            public void addNotifications(List<T> notifications) {
                if (notifications != null) {
                    this.notifications.addAll(notifications);
                }
            }

            public void addRelationshipNotifications(List<T> notifications) {
                if (notifications != null) {
                    this.relationshipNotifications.addAll(notifications);
                }
            }

            @Override
            public void onComplete(boolean isSuccess) {
                postCommitNotificationHooks.remove();

                if (CollectionUtils.isNotEmpty(notifications) || CollectionUtils.isNotEmpty(relationshipNotifications)) {
                    if (isSuccess) {
                        try {
                            notificationInterface.send(ENTITIES, notifications);
                            notificationInterface.send(RELATIONSHIPS, relationshipNotifications);
                        } catch (NotificationException excp) {
                            LOG.error("failed to send entity notifications", excp);
                        }
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Transaction not committed. Not sending {} notifications: {}", notifications.size(), notifications);
                        }
                    }
                }

            }
        }
    }

    private static boolean isRelationshipEvent(EntityNotification.EntityNotificationV2.OperationType operationType) {
        return EntityNotification.EntityNotificationV2.OperationType.RELATIONSHIP_CREATE.equals(operationType) || EntityNotification.EntityNotificationV2.OperationType.RELATIONSHIP_UPDATE.equals(operationType) || EntityNotification.EntityNotificationV2.OperationType.RELATIONSHIP_DELETE.equals(operationType);
    }
}