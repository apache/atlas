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
package org.apache.atlas.web.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAdminAccessRequest;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.kafka.KafkaNotification;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.utils.AtlasJson;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Path("v2/notification")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class NotificationREST {
    private static final Logger LOG = LoggerFactory.getLogger(NotificationREST.class);

    public static final String ATLAS_HOOK_TOPIC     = AtlasConfiguration.NOTIFICATION_HOOK_TOPIC_NAME.getString();
    public static final String ATLAS_ENTITIES_TOPIC = AtlasConfiguration.NOTIFICATION_ENTITIES_TOPIC_NAME.getString();

    private static final String[]    ATLAS_HOOK_CONSUMER_TOPICS     = AtlasConfiguration.NOTIFICATION_HOOK_CONSUMER_TOPIC_NAMES.getStringArray(ATLAS_HOOK_TOPIC);
    private static final String[]    ATLAS_ENTITIES_CONSUMER_TOPICS = AtlasConfiguration.NOTIFICATION_ENTITIES_CONSUMER_TOPIC_NAMES.getStringArray(ATLAS_ENTITIES_TOPIC);
    private static final Set<String> TOPICS                         = new HashSet<>();

    private final NotificationInterface notificationInterface;

    @Inject
    public NotificationREST(NotificationInterface notificationInterface) {
        this.notificationInterface = notificationInterface;
    }

    /**
     * Publish notifications on Kafka topic
     *
     *  @param topicName - nameOfTheQueue
     *  @throws AtlasBaseException
     */
    @POST
    @Path("/topic/{topicName}")
    @Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
    public void handleNotifications(@PathParam("topicName") String topicName, @Context HttpServletRequest request) throws AtlasBaseException, IOException {
        LOG.debug("Handling notifications for topic {}", topicName);

        AtlasAuthorizationUtils.verifyAccess(new AtlasAdminAccessRequest(AtlasPrivilege.SERVICE_NOTIFICATION_POST), "post on rest notification service");

        if (!TOPICS.contains(topicName)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_TOPIC_NAME, topicName);
        }

        String       messagesAsJson = Servlets.getRequestPayload(request);
        List<String> messages       = getMessagesToNotify(messagesAsJson);

        try {
            KafkaNotification notifier = (KafkaNotification) notificationInterface;

            notifier.sendInternal(topicName, messages, AtlasHook.isHookMsgsSortEnabled);
        } catch (NotificationException exception) {
            List<String> failedMessages      = exception.getFailedMessages();
            String       concatenatedMessage = StringUtils.join(failedMessages, "\n");

            throw new AtlasBaseException(AtlasErrorCode.NOTIFICATION_EXCEPTION, exception, concatenatedMessage);
        }
    }

    private List<String> getMessagesToNotify(String messagesAsJson) {
        List<String> messages = new ArrayList<>();

        try {
            ArrayNode messageNodes = AtlasJson.parseToV1ArrayNode(messagesAsJson);

            for (JsonNode messageNode : messageNodes) {
                messages.add(AtlasJson.toV1Json(messageNode));
            }
        } catch (IOException e) {
            messages.add(messagesAsJson);
        }

        return messages;
    }

    static {
        TOPICS.addAll(Arrays.asList(ATLAS_HOOK_CONSUMER_TOPICS));
        TOPICS.addAll(Arrays.asList(ATLAS_ENTITIES_CONSUMER_TOPICS));
    }
}
