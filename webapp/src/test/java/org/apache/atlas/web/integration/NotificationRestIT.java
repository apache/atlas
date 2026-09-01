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
package org.apache.atlas.web.integration;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import static org.apache.atlas.kafka.KafkaNotification.ATLAS_HOOK_TOPIC;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * ATLAS-5377: hook notification REST ingress was removed from the main Atlas webapp.
 * Canonical endpoint is rest-notification-webapp only. These integration tests verify
 * that POST /api/atlas/v2/notification/topic/{topicName} is not served on main webapp.
 */
public class NotificationRestIT extends BaseResourceIT {
    @Test
    public void unAuthPostNotificationRejected() throws IOException {
        AtlasClientV2 unAuthClient = new AtlasClientV2(atlasUrls, new String[] {"admin", "wr0ng_pa55w0rd"});

        try {
            unAuthClient.postNotificationToTopic(ATLAS_HOOK_TOPIC, new ArrayList<>(Collections.singletonList("Dummy")));

            fail("Expected postNotificationToTopic to fail on main webapp");
        } catch (AtlasServiceException e) {
            assertNotNull(e.getStatus(), "expected server error code in the status");
        }
    }

    @Test
    public void notificationEndpointRemovedFromMainWebapp() throws Exception {
        String dbName        = "db_" + randomString();
        String clusterName   = "cl" + randomString();

        String notificationString = TestResourceFileUtils.getJson("notifications/create-db")
                .replaceAll("--name--", dbName).replaceAll("--clName--", clusterName)
                .replace("\"--ts--\"", String.valueOf((new Date()).getTime()));

        try {
            atlasClientV2.postNotificationToTopic(ATLAS_HOOK_TOPIC, new ArrayList<>(Collections.singletonList(notificationString)));

            fail("Expected notification POST to fail — endpoint removed from main webapp (ATLAS-5377)");
        } catch (AtlasServiceException e) {
            assertNotNull(e.getStatus(), "expected HTTP error when posting to removed endpoint");

            assertTrue(e.getStatus().getStatusCode() != ClientResponse.Status.NO_CONTENT.getStatusCode(),
                    "notification POST must not return 204 on main webapp");
        }
    }
}
