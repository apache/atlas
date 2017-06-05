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

package org.apache.atlas.kafka;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.apache.atlas.notification.hook.HookNotification.HookNotificationMessage;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class KafkaNotificationTest {

    private KafkaNotification kafkaNotification;

    @BeforeClass
    public void setup() throws Exception {
        Configuration properties = ApplicationProperties.get();
        properties.setProperty("atlas.kafka.data", "target/" + RandomStringUtils.randomAlphanumeric(5));

        kafkaNotification = new KafkaNotification(properties);
        kafkaNotification.start();
    }

    @AfterClass
    public void shutdown() throws Exception {
        kafkaNotification.close();
        kafkaNotification.stop();
    }

    @Test
    public void testReceiveKafkaMessages() throws Exception {
        kafkaNotification.send(NotificationInterface.NotificationType.HOOK,
                new HookNotification.EntityCreateRequest("u1", new Referenceable("type")));
        kafkaNotification.send(NotificationInterface.NotificationType.HOOK,
                new HookNotification.EntityCreateRequest("u2", new Referenceable("type")));
        kafkaNotification.send(NotificationInterface.NotificationType.HOOK,
                new HookNotification.EntityCreateRequest("u3", new Referenceable("type")));
        kafkaNotification.send(NotificationInterface.NotificationType.HOOK,
                new HookNotification.EntityCreateRequest("u4", new Referenceable("type")));

        NotificationConsumer<Object> consumer =
                kafkaNotification.createConsumers(NotificationInterface.NotificationType.HOOK, 1).get(0);
        List<AtlasKafkaMessage<Object>> messages = null ;
        long startTime = System.currentTimeMillis(); //fetch starting time
        while ((System.currentTimeMillis() - startTime) < 10000) {
             messages = consumer.receive(1000L);
            if (messages.size() > 0) {
                break;
            }
        }

        int i=1;
        for (AtlasKafkaMessage<Object> msg :  messages){
            HookNotification.HookNotificationMessage message =  (HookNotificationMessage) msg.getMessage();
            assertEquals(message.getUser(), "u"+i++);
        }

        consumer.close();
    }
}
