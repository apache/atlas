/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.inject.Inject;
import org.apache.atlas.AtlasException;
import org.apache.atlas.notification.NotificationConsumer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.notification.NotificationModule;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Guice(modules = NotificationModule.class)
public class KafkaNotificationTest {

    @Inject
    private KafkaNotification kafka;

    @BeforeClass
    public void setUp() throws Exception {
        kafka.start();
    }

    @Test
    public void testSendReceiveMessage() throws AtlasException {
        String msg1 = "message" + random();
        String msg2 = "message" + random();
        kafka.send(NotificationInterface.NotificationType.HOOK, msg1, msg2);
        NotificationConsumer consumer = kafka.createConsumers(NotificationInterface.NotificationType.HOOK, 1).get(0);
        Assert.assertTrue(consumer.hasNext());
        Assert.assertEquals(msg1, consumer.next());
        Assert.assertTrue(consumer.hasNext());
        Assert.assertEquals(msg2, consumer.next());
    }

    private String random() {
        return RandomStringUtils.randomAlphanumeric(5);
    }

    @AfterClass
    public void teardown() throws Exception {
        kafka.stop();
    }
}
