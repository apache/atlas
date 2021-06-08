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

package org.apache.atlas.notification;

import org.apache.atlas.AtlasException;
import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.model.notification.HookNotification.HookNotificationType;
import org.apache.atlas.notification.NotificationInterface.NotificationType;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.configuration.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

/**
 * AbstractNotification tests.
 */
public class AbstractNotificationTest {

    @org.testng.annotations.Test
    public void testSend() throws Exception {
        Configuration    configuration = mock(Configuration.class);
        TestNotification notification  = new TestNotification(configuration);
        Test             message1      = new Test(HookNotificationType.ENTITY_CREATE, "user1");
        Test             message2      = new Test(HookNotificationType.TYPE_CREATE, "user1");
        Test             message3      = new Test(HookNotificationType.ENTITY_FULL_UPDATE, "user1");
        List<KeyValue<String, String>> messageJson = new ArrayList<>();

        AbstractNotification.createNotificationMessages(message1, "key1", messageJson);
        AbstractNotification.createNotificationMessages(message2, "key2", messageJson);
        AbstractNotification.createNotificationMessages(message3, "key3", messageJson);

        notification.send(NotificationType.HOOK,
                new KeyValue<>("key1", message1),
                new KeyValue<>("key2", message2),
                new KeyValue<>("key3", message3));

        assertEquals(NotificationType.HOOK, notification.type);
        assertEquals(3, notification.messages.size());

        for (int i = 0; i < notification.messages.size(); i++) {
            assertEquals(notification.messages.get(i).getKey(), messageJson.get(i).getKey());
            assertEqualsMessageJson(notification.messages.get(i).getValue(), messageJson.get(i).getValue());
        }
    }

    @org.testng.annotations.Test
    public void testSend2() throws Exception {
        Configuration    configuration = mock(Configuration.class);
        TestNotification notification  = new TestNotification(configuration);
        Test             message1      = new Test(HookNotificationType.ENTITY_CREATE, "user1");
        Test             message2      = new Test(HookNotificationType.TYPE_CREATE, "user1");
        Test             message3      = new Test(HookNotificationType.ENTITY_FULL_UPDATE, "user1");
        List<Test>       messages      = Arrays.asList(message1, message2, message3);
        List<KeyValue<String, String>> messageJson = new ArrayList<>();

        AbstractNotification.createNotificationMessages(message1, "key1", messageJson);
        AbstractNotification.createNotificationMessages(message2, "key2", messageJson);
        AbstractNotification.createNotificationMessages(message3, "key3", messageJson);

        notification.send(NotificationInterface.NotificationType.HOOK,
                new KeyValue<>("key1", message1),
                new KeyValue<>("key2", message2),
                new KeyValue<>("key3", message3));

        assertEquals(notification.type, NotificationType.HOOK);
        assertEquals(notification.messages.size(), messageJson.size());

        for (int i = 0; i < notification.messages.size(); i++) {
            assertEquals(notification.messages.get(i).getKey(), messageJson.get(i).getKey());
            assertEqualsMessageJson(notification.messages.get(i).getValue(), messageJson.get(i).getValue());
        }
    }

    public static class Test extends HookNotification {

        public Test(HookNotificationType type, String user) {
            super(type, user);
        }
    }

    // ignore msgCreationTime in Json
    private void assertEqualsMessageJson(String msgJsonActual, String msgJsonExpected) {
        Map<Object, Object> msgActual   = AtlasType.fromV1Json(msgJsonActual, Map.class);
        Map<Object, Object> msgExpected = AtlasType.fromV1Json(msgJsonExpected, Map.class);

        msgActual.remove("msgCreationTime");
        msgExpected.remove("msgCreationTime");

        assertEquals(msgActual, msgExpected);
    }

    public static class TestNotification extends AbstractNotification {
        private NotificationType type;
        private List<KeyValue<String, String>> messages;

        public TestNotification(Configuration applicationProperties) throws AtlasException {
            super(applicationProperties);
        }

        @Override
        public void sendInternal(NotificationType notificationType, List<KeyValue<String, String>> notificationMessages)
            throws NotificationException {

            type     = notificationType;
            messages = notificationMessages;
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
    }
}
