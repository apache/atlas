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

package org.apache.atlas.hook;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationModule;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.security.InMemoryJAASConfiguration;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * A base class for atlas hooks.
 */
public abstract class AtlasHook {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasHook.class);

    protected static Configuration atlasProperties;

    protected static NotificationInterface notifInterface;

    private static boolean logFailedMessages;
    private static FailedMessagesLogger failedMessagesLogger;
    private static int notificationRetryInterval;
    public static final String ATLAS_NOTIFICATION_RETRY_INTERVAL = "atlas.notification.hook.retry.interval";

    public static final String ATLAS_NOTIFICATION_FAILED_MESSAGES_FILENAME_KEY =
            "atlas.notification.failed.messages.filename";
    public static final String ATLAS_HOOK_FAILED_MESSAGES_LOG_DEFAULT_NAME = "atlas_hook_failed_messages.log";
    public static final String ATLAS_NOTIFICATION_LOG_FAILED_MESSAGES_ENABLED_KEY =
            "atlas.notification.log.failed.messages";

    static {
        try {
            atlasProperties = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }

        String failedMessageFile = atlasProperties.getString(ATLAS_NOTIFICATION_FAILED_MESSAGES_FILENAME_KEY,
                ATLAS_HOOK_FAILED_MESSAGES_LOG_DEFAULT_NAME);
        logFailedMessages = atlasProperties.getBoolean(ATLAS_NOTIFICATION_LOG_FAILED_MESSAGES_ENABLED_KEY, true);
        if (logFailedMessages) {
            failedMessagesLogger = new FailedMessagesLogger(failedMessageFile);
            failedMessagesLogger.init();
        }

        if (!isLoginKeytabBased()) {
            if (isLoginTicketBased()) {
                InMemoryJAASConfiguration.setConfigSectionRedirect("KafkaClient", "ticketBased-KafkaClient");
            }
        }

        notificationRetryInterval = atlasProperties.getInt(ATLAS_NOTIFICATION_RETRY_INTERVAL, 1000);
        Injector injector = Guice.createInjector(new NotificationModule());
        notifInterface = injector.getInstance(NotificationInterface.class);

        LOG.info("Created Atlas Hook");
    }

    protected abstract String getNumberOfRetriesPropertyKey();

    protected void notifyEntities(String user, Collection<Referenceable> entities) {
        JSONArray entitiesArray = new JSONArray();

        for (Referenceable entity : entities) {
            LOG.info("Adding entity for type: {}", entity.getTypeName());
            final String entityJson = InstanceSerialization.toJson(entity, true);
            entitiesArray.put(entityJson);
        }

        List<HookNotification.HookNotificationMessage> hookNotificationMessages = new ArrayList<>();
        hookNotificationMessages.add(new HookNotification.EntityCreateRequest(user, entitiesArray));
        notifyEntities(hookNotificationMessages);
    }

    /**
     * Notify atlas of the entity through message. The entity can be a
     * complex entity with reference to other entities.
     * De-duping of entities is done on server side depending on the
     * unique attribute on the entities.
     *
     * @param messages   hook notification messages
     * @param maxRetries maximum number of retries while sending message to messaging system
     */
    public static void notifyEntities(List<HookNotification.HookNotificationMessage> messages, int maxRetries) {
        notifyEntitiesInternal(messages, maxRetries, notifInterface, logFailedMessages, failedMessagesLogger);
    }

    @VisibleForTesting
    static void notifyEntitiesInternal(List<HookNotification.HookNotificationMessage> messages, int maxRetries,
                                       NotificationInterface notificationInterface,
                                       boolean shouldLogFailedMessages, FailedMessagesLogger logger) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        final String message = messages.toString();
        int numRetries = 0;
        while (true) {
            try {
                notificationInterface.send(NotificationInterface.NotificationType.HOOK, messages);
                return;
            } catch (Exception e) {
                numRetries++;
                if (numRetries < maxRetries) {
                    LOG.error("Failed to send notification - attempt #{}; error={}", numRetries, e.getMessage());
                    try {
                        LOG.debug("Sleeping for {} ms before retry", notificationRetryInterval);
                        Thread.sleep(notificationRetryInterval);
                    } catch (InterruptedException ie) {
                        LOG.error("Notification hook thread sleep interrupted");
                    }

                } else {
                    if (shouldLogFailedMessages && e instanceof NotificationException) {
                        List<String> failedMessages = ((NotificationException) e).getFailedMessages();
                        for (String msg : failedMessages) {
                            logger.log(msg);
                        }
                    }
                    LOG.error("Failed to notify atlas for entity {} after {} retries. Quitting",
                            message, maxRetries, e);
                    return;
                }
            }
        }
    }

    /**
     * Notify atlas of the entity through message. The entity can be a
     * complex entity with reference to other entities.
     * De-duping of entities is done on server side depending on the
     * unique attribute on the entities.
     *
     * @param messages hook notification messages
     */
    protected void notifyEntities(List<HookNotification.HookNotificationMessage> messages) {
        final int maxRetries = atlasProperties.getInt(getNumberOfRetriesPropertyKey(), 3);
        notifyEntities(messages, maxRetries);
    }

    /**
     * Returns the logged in user.
     *
     * @return
     */
    public static String getUser() {
        return getUser(null, null);
    }

    public static String getUser(String userName) {
        return getUser(userName, null);
    }

    /**
     * Returns the user. Order of preference:
     * 1. Given userName
     * 2. ugi.getShortUserName()
     * 3. UserGroupInformation.getCurrentUser().getShortUserName()
     * 4. System.getProperty("user.name")
     */

    public static String getUser(String userName, UserGroupInformation ugi) {
        if (StringUtils.isNotEmpty(userName)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Returning userName {}", userName);
            }
            return userName;
        }

        if (ugi != null && StringUtils.isNotEmpty(ugi.getShortUserName())) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Returning ugi.getShortUserName {}", userName);
            }
            return ugi.getShortUserName();
        }

        try {
            return UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException e) {
            LOG.warn("Failed for UserGroupInformation.getCurrentUser() ", e);
            return System.getProperty("user.name");
        }
    }

    private static boolean isLoginKeytabBased() {
        boolean ret = false;

        try {
            ret = UserGroupInformation.isLoginKeytabBased();
        } catch (Exception excp) {
            LOG.warn("Error in determining keytab for KafkaClient-JAAS config", excp);
        }

        return ret;
    }

    private static boolean isLoginTicketBased() {
        boolean ret = false;

        try {
            ret = UserGroupInformation.isLoginTicketBased();
        } catch (Exception excp) {
            LOG.warn("Error in determining ticket-cache for KafkaClient-JAAS config", excp);
        }

        return ret;
    }

}
