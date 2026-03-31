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

import io.micrometer.core.instrument.Counter;
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
import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;


public class EntityNotificationSender<T> {
    private static final Logger LOG = LoggerFactory.getLogger(EntityNotificationSender.class);

    private static final int    MAX_RETRY_COUNT = 3;
    private static final long   RETRY_INTERVAL_MS = 1000;

    private static final Counter NOTIFICATIONS_SENT     = Counter.builder("atlas_notifications_sent_total")
            .description("Total entity/relationship notifications successfully sent to Kafka")
            .register(getMeterRegistry());
    private static final Counter NOTIFICATIONS_DROPPED  = Counter.builder("atlas_notifications_dropped_total")
            .description("Notifications dropped because the transaction was not committed (isSuccess=false)")
            .register(getMeterRegistry());
    private static final Counter NOTIFICATIONS_FAILED   = Counter.builder("atlas_notifications_failed_total")
            .description("Notifications that failed to send to Kafka after transaction commit")
            .register(getMeterRegistry());

    private final static boolean NOTIFY_POST_COMMIT_DEFAULT = true;

    private final NotificationSender<T> notificationSender;

    public EntityNotificationSender(NotificationInterface notificationInterface, Configuration configuration) {
        this(notificationInterface, configuration != null ? configuration.getBoolean("atlas.notification.send.postcommit", NOTIFY_POST_COMMIT_DEFAULT) : NOTIFY_POST_COMMIT_DEFAULT);
    }

    public EntityNotificationSender(NotificationInterface notificationInterface, boolean sendPostCommit) {
        if (sendPostCommit) {
            LOG.debug("EntityNotificationSender: notifications will be sent after transaction commit");

            this.notificationSender = new PostCommitNotificationSender(notificationInterface);
        } else {
            LOG.debug("EntityNotificationSender: notifications will be sent inline (i.e. not waiting for transaction to commit)");

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

                int entityCount       = notifications.size();
                int relationshipCount = relationshipNotifications.size();

                if (CollectionUtils.isNotEmpty(notifications) || CollectionUtils.isNotEmpty(relationshipNotifications)) {
                    if (isSuccess) {
                        sendWithRetry(entityCount, relationshipCount);
                    } else {
                        NOTIFICATIONS_DROPPED.increment(entityCount + relationshipCount);
                        LOG.warn("Transaction not committed (isSuccess=false). Dropping {} entity and {} relationship notifications.",
                                entityCount, relationshipCount);
                    }
                }
            }

            private void sendWithRetry(int entityCount, int relationshipCount) {
                NotificationException lastException = null;

                for (int attempt = 1; attempt <= MAX_RETRY_COUNT; attempt++) {
                    try {
                        notificationInterface.send(ENTITIES, notifications);
                        notificationInterface.send(RELATIONSHIPS, relationshipNotifications);
                        NOTIFICATIONS_SENT.increment(entityCount + relationshipCount);

                        if (attempt > 1) {
                            LOG.info("Notification send succeeded on attempt {}/{} for {} entity and {} relationship notifications",
                                    attempt, MAX_RETRY_COUNT, entityCount, relationshipCount);
                        }
                        return;
                    } catch (NotificationException excp) {
                        lastException = excp;
                        LOG.warn("Notification send failed on attempt {}/{} for {} entity and {} relationship notifications: {}",
                                attempt, MAX_RETRY_COUNT, entityCount, relationshipCount, excp.getMessage());

                        if (attempt < MAX_RETRY_COUNT) {
                            try {
                                Thread.sleep(RETRY_INTERVAL_MS * attempt);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                }

                NOTIFICATIONS_FAILED.increment(entityCount + relationshipCount);
                LOG.error("Failed to send {} entity and {} relationship notifications after {} attempts",
                        entityCount, relationshipCount, MAX_RETRY_COUNT, lastException);
            }
        }
    }

    private static boolean isRelationshipEvent(EntityNotification.EntityNotificationV2.OperationType operationType) {
        return EntityNotification.EntityNotificationV2.OperationType.RELATIONSHIP_CREATE.equals(operationType) || EntityNotification.EntityNotificationV2.OperationType.RELATIONSHIP_UPDATE.equals(operationType) || EntityNotification.EntityNotificationV2.OperationType.RELATIONSHIP_DELETE.equals(operationType);
    }
}