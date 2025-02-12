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

package org.apache.atlas.hook;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasConstants;
import org.apache.atlas.kafka.NotificationProvider;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.MessageSource;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.utils.AtlasConfigurationUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ShutdownHookManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * A base class for atlas hooks.
 */
public abstract class AtlasHook {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasHook.class);

    public static final String ATLAS_NOTIFICATION_ASYNCHRONOUS                    = "atlas.notification.hook.asynchronous";
    public static final String ATLAS_NOTIFICATION_ASYNCHRONOUS_MIN_THREADS        = "atlas.notification.hook.asynchronous.minThreads";
    public static final String ATLAS_NOTIFICATION_ASYNCHRONOUS_MAX_THREADS        = "atlas.notification.hook.asynchronous.maxThreads";
    public static final String ATLAS_NOTIFICATION_ASYNCHRONOUS_KEEP_ALIVE_TIME_MS = "atlas.notification.hook.asynchronous.keepAliveTimeMs";
    public static final String ATLAS_NOTIFICATION_ASYNCHRONOUS_QUEUE_SIZE         = "atlas.notification.hook.asynchronous.queueSize";
    public static final String ATLAS_NOTIFICATION_MAX_RETRIES                     = "atlas.notification.hook.retry.maxRetries";
    public static final String ATLAS_NOTIFICATION_RETRY_INTERVAL                  = "atlas.notification.hook.retry.interval";
    public static final String ATLAS_NOTIFICATION_FAILED_MESSAGES_FILENAME_KEY    = "atlas.notification.failed.messages.filename";
    public static final String ATLAS_NOTIFICATION_LOG_FAILED_MESSAGES_ENABLED_KEY = "atlas.notification.log.failed.messages";
    public static final String ATLAS_HOOK_FAILED_MESSAGES_LOG_DEFAULT_NAME        = "atlas_hook_failed_messages.log";
    public static final String CONF_METADATA_NAMESPACE                            = "atlas.metadata.namespace";
    public static final String CLUSTER_NAME_KEY                                   = "atlas.cluster.name";
    public static final String DEFAULT_CLUSTER_NAME                               = "primary";
    public static final String CONF_ATLAS_HOOK_MESSAGES_SORT_ENABLED              = "atlas.hook.messages.sort.enabled";
    public static final String ATLAS_HOOK_ENTITY_IGNORE_PATTERN                   = "atlas.hook.entity.ignore.pattern";
    public static final String ATTRIBUTE_QUALIFIED_NAME                           = "qualifiedName";

    public static final boolean isRESTNotificationEnabled;
    public static final boolean isHookMsgsSortEnabled;

    protected static final Configuration         atlasProperties;
    protected static final NotificationInterface notificationInterface;

    private static final String               metadataNamespace;
    private static final int                  SHUTDOWN_HOOK_WAIT_TIME_MS = 3000;
    private static final boolean              logFailedMessages;
    private static final FailedMessagesLogger failedMessagesLogger;
    private static final int                  notificationMaxRetries;
    private static final int                  notificationRetryInterval;
    private static final List<Pattern>        entitiesToIgnore = new ArrayList<>();
    private static final boolean              shouldPreprocess;
    private static final ExecutorService      executor;

    protected MessageSource source;

    public AtlasHook() {
        source = new MessageSource(getMessageSource());

        notificationInterface.init(this.getClass().getSimpleName(), failedMessagesLogger);
    }

    public AtlasHook(String name) {
        source = new MessageSource(getMessageSource());

        LOG.info("AtlasHook: Spool name: Passed from caller.: {}", name);

        notificationInterface.init(name, failedMessagesLogger);
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
    public static void notifyEntities(List<HookNotification> messages, UserGroupInformation ugi, int maxRetries, MessageSource source) {
        if (executor == null) { // send synchronously
            notifyEntitiesPostPreprocess(messages, ugi, maxRetries, source);
        } else {
            executor.submit(() -> notifyEntitiesPostPreprocess(messages, ugi, maxRetries, source));
        }
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
            LOG.debug("Returning userName {}", userName);

            return userName;
        }

        if (ugi != null && StringUtils.isNotEmpty(ugi.getShortUserName())) {
            LOG.debug("Returning ugi.getShortUserName {}", userName);

            return ugi.getShortUserName();
        }

        try {
            return UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException e) {
            LOG.warn("Failed for UserGroupInformation.getCurrentUser() ", e);

            return System.getProperty("user.name");
        }
    }

    protected static boolean isMatch(String qualifiedName, List<Pattern> patterns) {
        return patterns.stream().anyMatch((Pattern pattern) -> pattern.matcher(qualifiedName).matches());
    }

    @VisibleForTesting
    static void notifyEntitiesInternal(List<HookNotification> messages, int maxRetries, UserGroupInformation ugi, NotificationInterface notificationInterface, boolean shouldLogFailedMessages, FailedMessagesLogger logger, MessageSource source) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        final int maxAttempts         = maxRetries < 1 ? 1 : maxRetries;
        Exception notificationFailure = null;

        for (int numAttempt = 1; numAttempt <= maxAttempts; numAttempt++) {
            if (numAttempt > 1) { // retry attempt
                try {
                    LOG.debug("Sleeping for {} ms before retry", notificationRetryInterval);

                    Thread.sleep(notificationRetryInterval);
                } catch (InterruptedException ie) {
                    LOG.error("Notification hook thread sleep interrupted");

                    break;
                }
            }

            try {
                if (ugi == null) {
                    notificationInterface.send(NotificationInterface.NotificationType.HOOK, messages, source);
                } else {
                    PrivilegedExceptionAction<Object> privilegedNotify = () -> {
                        notificationInterface.send(NotificationInterface.NotificationType.HOOK, messages, source);

                        return messages;
                    };

                    ugi.doAs(privilegedNotify);
                }

                notificationFailure = null; // notification sent successfully, reset error

                break;
            } catch (Exception e) {
                notificationFailure = e;

                LOG.error("Failed to send notification - attempt #{}; error={}", numAttempt, e.getMessage());
            }
        }

        if (notificationFailure != null) {
            if (shouldLogFailedMessages && notificationFailure instanceof NotificationException) {
                final List<String> failedMessages = ((NotificationException) notificationFailure).getFailedMessages();

                for (String msg : failedMessages) {
                    logger.log(msg);
                }
            }

            LOG.error("Giving up after {} failed attempts to send notification to Atlas: {}", maxAttempts, messages, notificationFailure);
        }
    }

    public abstract String getMessageSource();

    public String getMetadataNamespace() {
        return metadataNamespace;
    }

    /**
     * Notify atlas of the entity through message. The entity can be a
     * complex entity with reference to other entities.
     * De-duping of entities is done on server side depending on the
     * unique attribute on the entities.
     *
     * @param messages hook notification messages
     */
    protected void notifyEntities(List<HookNotification> messages, UserGroupInformation ugi) {
        notifyEntities(messages, ugi, notificationMaxRetries, source);
    }

    private static AtlasEntity.AtlasEntitiesWithExtInfo getAtlasEntitiesWithExtInfo(HookNotification hookNotification) {
        final AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo;

        switch (hookNotification.getType()) {
            case ENTITY_CREATE_V2:
                entitiesWithExtInfo = ((HookNotification.EntityCreateRequestV2) hookNotification).getEntities();
                break;
            case ENTITY_FULL_UPDATE_V2:
                entitiesWithExtInfo = ((HookNotification.EntityUpdateRequestV2) hookNotification).getEntities();
                break;
            default:
                entitiesWithExtInfo = null;
        }

        return entitiesWithExtInfo;
    }

    private static void preprocessEntities(List<HookNotification> hookNotifications) {
        for (int i = 0; i < hookNotifications.size(); i++) {
            HookNotification hookNotification = hookNotifications.get(i);

            AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = getAtlasEntitiesWithExtInfo(hookNotification);

            if (entitiesWithExtInfo == null) {
                return;
            }

            List<AtlasEntity> entities = entitiesWithExtInfo.getEntities();

            entities = ((entities != null) ? entities : Collections.emptyList());

            entities.removeIf((AtlasEntity entity) -> isMatch(entity.getAttribute(ATTRIBUTE_QUALIFIED_NAME).toString(), entitiesToIgnore));

            Map<String, AtlasEntity> referredEntitiesMap = entitiesWithExtInfo.getReferredEntities();

            referredEntitiesMap = ((referredEntitiesMap != null) ? referredEntitiesMap : Collections.emptyMap());

            referredEntitiesMap.entrySet().removeIf((Map.Entry<String, AtlasEntity> entry) -> isMatch(entry.getValue().getAttribute(ATTRIBUTE_QUALIFIED_NAME).toString(), entitiesToIgnore));

            if (CollectionUtils.isEmpty(entities) && CollectionUtils.isEmpty(referredEntitiesMap.values())) {
                hookNotifications.remove(i--);

                LOG.info("ignored message: {}", hookNotification);
            }
        }
    }

    private static void notifyEntitiesPostPreprocess(List<HookNotification> messages, UserGroupInformation ugi, int maxRetries, MessageSource source) {
        if (shouldPreprocess) {
            preprocessEntities(messages);
        }

        if (CollectionUtils.isNotEmpty(messages)) {
            notifyEntitiesInternal(messages, maxRetries, ugi, notificationInterface, logFailedMessages, failedMessagesLogger, source);
        }
    }

    private static String getMetadataNamespace(Configuration config) {
        return AtlasConfigurationUtil.getRecentString(config, CONF_METADATA_NAMESPACE, getClusterName(config));
    }

    private static String getClusterName(Configuration config) {
        return config.getString(CLUSTER_NAME_KEY, DEFAULT_CLUSTER_NAME);
    }

    static {
        Configuration conf = null;

        try {
            conf = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }

        atlasProperties   = conf;
        logFailedMessages = atlasProperties.getBoolean(ATLAS_NOTIFICATION_LOG_FAILED_MESSAGES_ENABLED_KEY, true);

        if (logFailedMessages) {
            failedMessagesLogger = new FailedMessagesLogger();
        } else {
            failedMessagesLogger = null;
        }

        isRESTNotificationEnabled = AtlasConfiguration.NOTIFICATION_HOOK_REST_ENABLED.getBoolean();
        isHookMsgsSortEnabled     = atlasProperties.getBoolean(CONF_ATLAS_HOOK_MESSAGES_SORT_ENABLED, isRESTNotificationEnabled);
        metadataNamespace         = getMetadataNamespace(atlasProperties);
        notificationMaxRetries    = atlasProperties.getInt(ATLAS_NOTIFICATION_MAX_RETRIES, 3);
        notificationRetryInterval = atlasProperties.getInt(ATLAS_NOTIFICATION_RETRY_INTERVAL, 1000);
        notificationInterface     = NotificationProvider.get();

        String[] patternsToIgnoreEntities = atlasProperties.getStringArray(ATLAS_HOOK_ENTITY_IGNORE_PATTERN);

        if (patternsToIgnoreEntities != null) {
            for (String pattern : patternsToIgnoreEntities) {
                try {
                    entitiesToIgnore.add(Pattern.compile(pattern));
                } catch (Throwable t) {
                    LOG.warn("failed to compile pattern {}", pattern, t);
                    LOG.warn("Ignoring invalid pattern in configuration {}: {}", ATLAS_HOOK_ENTITY_IGNORE_PATTERN, pattern);
                }
            }

            LOG.info("{}={}", ATLAS_HOOK_ENTITY_IGNORE_PATTERN, entitiesToIgnore);
        }

        shouldPreprocess = CollectionUtils.isNotEmpty(entitiesToIgnore);

        String currentUser = "";

        try {
            currentUser = getUser();
        } catch (Exception excp) {
            LOG.warn("Error in determining current user", excp);
        }

        notificationInterface.setCurrentUser(currentUser);

        boolean isAsync = atlasProperties.getBoolean(ATLAS_NOTIFICATION_ASYNCHRONOUS, Boolean.TRUE);

        if (isAsync) {
            int  minThreads      = atlasProperties.getInt(ATLAS_NOTIFICATION_ASYNCHRONOUS_MIN_THREADS, 1);
            int  maxThreads      = atlasProperties.getInt(ATLAS_NOTIFICATION_ASYNCHRONOUS_MAX_THREADS, 1);
            long keepAliveTimeMs = atlasProperties.getLong(ATLAS_NOTIFICATION_ASYNCHRONOUS_KEEP_ALIVE_TIME_MS, 10000);
            int  queueSize       = atlasProperties.getInt(ATLAS_NOTIFICATION_ASYNCHRONOUS_QUEUE_SIZE, 10000);

            executor = new ThreadPoolExecutor(minThreads, maxThreads, keepAliveTimeMs, TimeUnit.MILLISECONDS,
                    new LinkedBlockingDeque<>(queueSize),
                    new ThreadFactoryBuilder().setNameFormat("Atlas Notifier %d").setDaemon(true).build());

            ShutdownHookManager.get().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        LOG.info("==> Shutdown of Atlas Hook");

                        notificationInterface.close();
                        executor.shutdown();

                        boolean ignored = executor.awaitTermination(SHUTDOWN_HOOK_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException excp) {
                        LOG.info("Interrupt received in shutdown.", excp);
                    } finally {
                        LOG.info("<== Shutdown of Atlas Hook");
                    }
                }
            }, AtlasConstants.ATLAS_SHUTDOWN_HOOK_PRIORITY);
        } else {
            executor = null;
        }

        LOG.info("Created Atlas Hook");
    }
}
