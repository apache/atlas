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

import com.fasterxml.jackson.databind.node.ArrayNode;
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
import static org.testng.Assert.assertNull;

public class NotificationRestIT extends BaseResourceIT {
    @Test
    public void unAuthPostNotification() throws IOException {
        AtlasClientV2 unAuthClient = new AtlasClientV2(atlasUrls, new String[] {"admin", "wr0ng_pa55w0rd"});

        try {
            unAuthClient.postNotificationToTopic(ATLAS_HOOK_TOPIC, new ArrayList<String>(Collections.singletonList("Dummy")));
        } catch (AtlasServiceException e) {
            assertNotNull(e.getStatus(), "expected server error code in the status");
        }
    }

    @Test
    public void postNotificationBasicTest() throws Exception {
        String dbName        = "db_" + randomString();
        String clusterName   = "cl" + randomString();
        String qualifiedName = dbName + "@" + clusterName;

        String notificationString = TestResourceFileUtils.getJson("notifications/create-db")
                .replaceAll("--name--", dbName).replaceAll("--clName--", clusterName)
                .replace("\"--ts--\"", String.valueOf((new Date()).getTime()));

        try {
            atlasClientV2.postNotificationToTopic(ATLAS_HOOK_TOPIC, new ArrayList<String>(Collections.singletonList(notificationString)));

            waitFor(MAX_WAIT_TIME, () -> {
                ArrayNode results = searchByDSL(String.format("%s where qualifiedName='%s'", DATABASE_TYPE_BUILTIN, qualifiedName));

                return results.size() == 1;
            });
        } catch (AtlasServiceException e) {
            assertNull(e.getStatus(), "expected no server error code in the status");
        }
    }
}
