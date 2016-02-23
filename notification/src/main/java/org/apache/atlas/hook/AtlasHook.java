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
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationModule;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.commons.configuration.Configuration;
import org.codehaus.jettison.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * A base class for atlas hooks.
 */
public abstract class AtlasHook {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasHook.class);
    private static final String DEFAULT_ATLAS_URL = "http://localhost:21000/";

    public static final String ATLAS_ENDPOINT = "atlas.rest.address";

    protected final AtlasClient atlasClient;

    /**
     * Hadoop Cluster name for this instance, typically used for namespace.
     */
    protected static Configuration atlasProperties;

    @Inject
    protected static NotificationInterface notifInterface;

    static {
        try {
            atlasProperties = ApplicationProperties.get();
        } catch (Exception e) {
            LOG.info("Attempting to send msg while shutdown in progress.", e);
        }

        Injector injector = Guice.createInjector(new NotificationModule());
        notifInterface = injector.getInstance(NotificationInterface.class);

        LOG.info("Created Atlas Hook");
    }

    public AtlasHook() {
        this(new AtlasClient(atlasProperties.getString(ATLAS_ENDPOINT, DEFAULT_ATLAS_URL)));
    }

    public AtlasHook(AtlasClient atlasClient) {
        this.atlasClient = atlasClient;
        //TODO - take care of passing in - ugi, doAsUser for secure cluster
    }

    protected abstract String getNumberOfRetriesPropertyKey();

    protected void notifyEntities(Collection<Referenceable> entities) {
        JSONArray entitiesArray = new JSONArray();

        for (Referenceable entity : entities) {
            LOG.info("Adding entity for type: {}", entity.getTypeName());
            final String entityJson = InstanceSerialization.toJson(entity, true);
            entitiesArray.put(entityJson);
        }

        List<HookNotification.HookNotificationMessage> hookNotificationMessages = new ArrayList<>();
        hookNotificationMessages.add(new HookNotification.EntityCreateRequest(entitiesArray));
        notifyEntities(hookNotificationMessages);
    }

    /**
     * Notify atlas
     * of the entity through message. The entity can be a
     * complex entity with reference to other entities.
     * De-duping of entities is done on server side depending on the
     * unique attribute on the entities.
     *
     * @param entities entities
     */
    protected void notifyEntities(List<HookNotification.HookNotificationMessage> entities) {
        final int maxRetries = atlasProperties.getInt(getNumberOfRetriesPropertyKey(), 3);
        final String message = entities.toString();

        int numRetries = 0;
        while (true) {
            try {
                notifInterface.send(NotificationInterface.NotificationType.HOOK, entities);
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
}
