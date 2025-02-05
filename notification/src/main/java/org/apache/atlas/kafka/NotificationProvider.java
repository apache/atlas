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
package org.apache.atlas.kafka;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.rest.RestNotification;
import org.apache.atlas.notification.spool.AtlasFileSpool;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provider class for Notification interfaces
 */
public class NotificationProvider {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationProvider.class);

    @VisibleForTesting
    public static final  String CONF_ATLAS_HOOK_SPOOL_ENABLED = "atlas.hook.spool.enabled";

    private static final String  CONF_ATLAS_HOOK_SPOOL_DIR             = "atlas.hook.spool.dir";
    private static final boolean CONF_ATLAS_HOOK_SPOOL_ENABLED_DEFAULT = false;

    private static NotificationInterface notificationProvider;

    private NotificationProvider() {
        // to block instantiation
    }

    public static NotificationInterface get() {
        if (notificationProvider == null) {
            try {
                Configuration        conf     = ApplicationProperties.get();
                String               spoolDir = getSpoolDir(conf);
                AbstractNotification absNotifier;

                if (AtlasHook.isRESTNotificationEnabled) {
                    absNotifier = new RestNotification(conf);
                } else {
                    absNotifier = new KafkaNotification(conf);
                }

                if (isSpoolingEnabled(conf) && StringUtils.isNotEmpty(spoolDir)) {
                    LOG.info("Notification spooling is enabled: spool directory={}", spoolDir);

                    conf.setProperty(CONF_ATLAS_HOOK_SPOOL_DIR, spoolDir);

                    notificationProvider = new AtlasFileSpool(conf, absNotifier);
                } else {
                    LOG.info("Notification spooling is not enabled");

                    notificationProvider = absNotifier;
                }
            } catch (AtlasException e) {
                throw new RuntimeException("Error while initializing Notification interface", e);
            }
        }

        LOG.debug("NotificationInterface of type {} is enabled", notificationProvider.getClass().getSimpleName());

        return notificationProvider;
    }

    private static boolean isSpoolingEnabled(Configuration configuration) {
        return configuration.getBoolean(CONF_ATLAS_HOOK_SPOOL_ENABLED, CONF_ATLAS_HOOK_SPOOL_ENABLED_DEFAULT);
    }

    private static String getSpoolDir(Configuration configuration) {
        return configuration.getString(CONF_ATLAS_HOOK_SPOOL_DIR);
    }
}
