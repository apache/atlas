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

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationModule;
import org.apache.atlas.notification.hook.HookNotification;
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

    static {
        try {
            atlasProperties = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Failed to load application properties", e);
        }

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
     * @param messages hook notification messages
     * @param maxRetries maximum number of retries while sending message to messaging system
     */
    public static void notifyEntities(List<HookNotification.HookNotificationMessage> messages, int maxRetries) {
        final String message = messages.toString();

        int numRetries = 0;
        while (true) {
            try {
                notifInterface.send(NotificationInterface.NotificationType.HOOK, messages);
                return;
            } catch(Exception e) {
                numRetries++;
                if (numRetries < maxRetries) {
                    LOG.debug("Failed to notify atlas for entity {}. Retrying", message, e);
                } else {
                    LOG.error("Failed to notify atlas for entity {} after {} retries. Quitting",
                            message, maxRetries, e);
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
     * @return
     */
    public static String getUser() {
        return getUser(null, null);
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
            return userName;
        }

        if (ugi != null && StringUtils.isNotEmpty(ugi.getShortUserName())) {
            return ugi.getShortUserName();
        }

        try {
            return UserGroupInformation.getCurrentUser().getShortUserName();
        } catch (IOException e) {
            LOG.warn("Failed for UserGroupInformation.getCurrentUser()");
            return System.getProperty("user.name");
        }
    }
}
