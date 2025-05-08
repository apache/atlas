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
package org.apache.atlas.notification.rest;

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.notification.AtlasNotificationBaseMessage;
import org.apache.atlas.notification.AbstractNotification;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationException;
import org.apache.atlas.utils.AuthenticationUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.kafka.KafkaNotification.ATLAS_ENTITIES_TOPIC;
import static org.apache.atlas.kafka.KafkaNotification.ATLAS_HOOK_TOPIC;

public class RestNotification extends AbstractNotification {
    private static final Logger LOG = LoggerFactory.getLogger(RestNotification.class);

    private static final int    BATCH_MAX_LENGTH_BYTES = AtlasConfiguration.NOTIFICATION_REST_BODY_MAX_LENGTH_BYTES.getInt();
    private static final String ATLAS_ENDPOINT         = "atlas.rest.address";
    private static final String BASIC_AUTH_USERNAME    = "atlas.rest.basic.auth.username";
    private static final String BASIC_AUTH_PASSWORD    = "atlas.rest.basic.auth.password";
    private static final String DEFAULT_ATLAS_URL      = "http://localhost:31000/";

    private static final Map<NotificationType, String> PRODUCER_TOPIC_MAP = new HashMap<>();

    @VisibleForTesting
    public AtlasClientV2 atlasClientV2;

    public RestNotification(Configuration configuration) throws AtlasException {
        super();

        setupAtlasClientV2(configuration);
    }

    @Override
    public void sendInternal(NotificationType type, List<String> messages) throws NotificationException {
        String             topic   = PRODUCER_TOPIC_MAP.get(type);
        List<List<String>> batches = getBatches(messages);

        int batchCounter = 0;

        try {
            for (List<String> batch : batches) {
                batchCounter++;

                atlasClientV2.postNotificationToTopic(topic, batch);
            }
        } catch (AtlasServiceException e) {
            if (e.getMessage().contains(AtlasErrorCode.NOTIFICATION_EXCEPTION.getErrorCode())) {
                LOG.error("Sending notifications through REST interface failed starting from batch# {}", batchCounter);

                throw new NotificationException(e);
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void sendInternal(String topic, List<String> messages) throws NotificationException {
        throw new NotImplementedException("sendInternal method is not implemented.");
    }

    @Override
    public <T> List<NotificationConsumer<T>> createConsumers(NotificationType notificationType, int numConsumers) {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public boolean isReady(NotificationType type) {
        return true;
    }

    private AtlasClientV2 setupAtlasClientV2(Configuration configuration) throws AtlasException {
        if (atlasClientV2 != null) {
            return atlasClientV2;
        }

        try {
            String[] atlasEndPoint = configuration.getStringArray(ATLAS_ENDPOINT);

            if (atlasEndPoint == null || atlasEndPoint.length == 0) {
                atlasEndPoint = new String[] {DEFAULT_ATLAS_URL};
            }

            if (!AuthenticationUtil.isKerberosAuthenticationEnabled()) {
                String fileAuthUsername = configuration.getString(BASIC_AUTH_USERNAME, "admin");
                String fileAuthPassword = configuration.getString(BASIC_AUTH_PASSWORD, "admin123");

                String[] basicAuthUsernamePassword = (fileAuthUsername == null || fileAuthPassword == null) ? AuthenticationUtil.getBasicAuthenticationInput() : new String[] {fileAuthUsername, fileAuthPassword};

                atlasClientV2 = new AtlasClientV2(atlasEndPoint, basicAuthUsernamePassword);
            } else {
                atlasClientV2 = new AtlasClientV2(atlasEndPoint);
            }
        } catch (AtlasException e) {
            throw new AtlasException(e);
        }

        return atlasClientV2;
    }

    private List<List<String>> getBatches(List<String> messages) {
        List<List<String>> batches   = new ArrayList<>();
        List<String>       batch     = new ArrayList<>();
        int                batchSize = 0;

        for (String message : messages) {
            byte[] msgBytes = AtlasNotificationBaseMessage.getBytesUtf8(message);

            if (batchSize > 0 && batchSize + msgBytes.length > BATCH_MAX_LENGTH_BYTES) {
                batches.add(batch);

                batch     = new ArrayList<>();
                batchSize = 0;
            }

            batch.add(message);
            batchSize += msgBytes.length;
        }

        batches.add(batch);

        return batches;
    }

    static {
        PRODUCER_TOPIC_MAP.put(NotificationType.HOOK, ATLAS_HOOK_TOPIC);
        PRODUCER_TOPIC_MAP.put(NotificationType.ENTITIES, ATLAS_ENTITIES_TOPIC);
    }
}
