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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.atlas.kafka.AtlasKafkaMessage;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;
import org.apache.kafka.common.TopicPartition;

/**
 * AbstractNotificationConsumer tests.
 */
public class AbstractNotificationConsumerTest {

    private static final Gson GSON = new Gson();

    @Test
    public void testReceive() throws Exception {
        Logger logger = mock(Logger.class);

        TestMessage testMessage1 = new TestMessage("sValue1", 99);
        TestMessage testMessage2 = new TestMessage("sValue2", 98);
        TestMessage testMessage3 = new TestMessage("sValue3", 97);
        TestMessage testMessage4 = new TestMessage("sValue4", 96);

        List jsonList = new LinkedList<>();

        jsonList.add(GSON.toJson(new AtlasNotificationMessage<>(new MessageVersion("1.0.0"), testMessage1)));
        jsonList.add(GSON.toJson(new AtlasNotificationMessage<>(new MessageVersion("1.0.0"), testMessage2)));
        jsonList.add(GSON.toJson(new AtlasNotificationMessage<>(new MessageVersion("1.0.0"), testMessage3)));
        jsonList.add(GSON.toJson(new AtlasNotificationMessage<>(new MessageVersion("1.0.0"), testMessage4)));

        Type notificationMessageType = new TypeToken<AtlasNotificationMessage<TestMessage>>(){}.getType();

        NotificationConsumer<TestMessage> consumer =
                new TestNotificationConsumer<>(notificationMessageType, jsonList, logger);

        List<AtlasKafkaMessage<TestMessage>> messageList = consumer.receive();

        assertFalse(messageList.isEmpty());

        assertEquals(testMessage1, messageList.get(0).getMessage());

        assertEquals(testMessage2, messageList.get(1).getMessage());

        assertEquals(testMessage3, messageList.get(2).getMessage());

        assertEquals(testMessage4, messageList.get(3).getMessage());
    }

    @Test
    public void testNextBackVersion() throws Exception {
        Logger logger = mock(Logger.class);

        TestMessage testMessage1 = new TestMessage("sValue1", 99);
        TestMessage testMessage2 = new TestMessage("sValue2", 98);
        TestMessage testMessage3 = new TestMessage("sValue3", 97);
        TestMessage testMessage4 = new TestMessage("sValue4", 96);

        List jsonList = new LinkedList<>();

        String json1 = GSON.toJson(new AtlasNotificationMessage<>(new MessageVersion("1.0.0"), testMessage1));
        String json2 = GSON.toJson(new AtlasNotificationMessage<>(new MessageVersion("0.0.5"), testMessage2));
        String json3 = GSON.toJson(new AtlasNotificationMessage<>(new MessageVersion("0.5.0"), testMessage3));
        String json4 = GSON.toJson(testMessage4);

        jsonList.add(json1);
        jsonList.add(json2);
        jsonList.add(json3);
        jsonList.add(json4);

        Type notificationMessageType = new TypeToken<AtlasNotificationMessage<TestMessage>>(){}.getType();

        NotificationConsumer<TestMessage> consumer =
            new TestNotificationConsumer<>(notificationMessageType, jsonList, logger);

        List<AtlasKafkaMessage<TestMessage>> messageList = consumer.receive();

        assertEquals(new TestMessage("sValue1", 99), messageList.get(0).getMessage());

        assertEquals(new TestMessage("sValue2", 98), messageList.get(1).getMessage());

        assertEquals(new TestMessage("sValue3", 97), messageList.get(2).getMessage());

        assertEquals(new TestMessage("sValue4", 96), messageList.get(3).getMessage());

    }

    @Test
    public void testNextForwardVersion() throws Exception {
        Logger logger = mock(Logger.class);

        TestMessage testMessage1 = new TestMessage("sValue1", 99);
        TestMessage testMessage2 = new TestMessage("sValue2", 98);

        List jsonList = new LinkedList<>();

        String json1 = GSON.toJson(new AtlasNotificationMessage<>(new MessageVersion("1.0.0"), testMessage1));
        String json2 = GSON.toJson(new AtlasNotificationMessage<>(new MessageVersion("2.0.0"), testMessage2));

        jsonList.add(json1);
        jsonList.add(json2);

        Type notificationMessageType = new TypeToken<AtlasNotificationMessage<TestMessage>>(){}.getType();

        NotificationConsumer<TestMessage> consumer =
            new TestNotificationConsumer<>(notificationMessageType, jsonList, logger);
        try {
            List<AtlasKafkaMessage<TestMessage>> messageList = consumer.receive();

            messageList.get(1).getMessage();

            fail("Expected VersionMismatchException!");
        } catch (IncompatibleVersionException e) {

        }

    }



    private static class TestMessage {
        private String s;
        private int i;

        public TestMessage(String s, int i) {
            this.s = s;
            this.i = i;
        }

        public String getS() {
            return s;
        }

        public void setS(String s) {
            this.s = s;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestMessage that = (TestMessage) o;
            return i == that.i &&
                    Objects.equals(s, that.s);
        }

        @Override
        public int hashCode() {
            return Objects.hash(s, i);
        }
    }

    private static class TestNotificationConsumer<T> extends AbstractNotificationConsumer<T> {
        private final List<T> messageList;
        private int index = 0;

        public TestNotificationConsumer(Type notificationMessageType, List<T> messages, Logger logger) {
            super(new TestDeserializer<T>(notificationMessageType, logger));
            this.messageList = messages;
        }

        @Override
        public void commit(TopicPartition partition, long offset) {
            // do nothing.
        }

        @Override
        public void close() {
            //do nothing
        }

        @Override
        public void wakeup() {

        }

        @Override
        public List<AtlasKafkaMessage<T>> receive() {
            return receive(1000L);
        }

        @Override
        public List<AtlasKafkaMessage<T>> receive(long timeoutMilliSeconds) {
            List<AtlasKafkaMessage<T>> tempMessageList = new ArrayList();
            for(Object json :  messageList) {
                tempMessageList.add(new AtlasKafkaMessage(deserializer.deserialize((String)json), -1, -1));
            }
            return tempMessageList;
        }
    }

    private static final class TestDeserializer<T> extends AtlasNotificationMessageDeserializer<T> {

        private TestDeserializer(Type notificationMessageType, Logger logger) {
            super(notificationMessageType, AbstractNotification.CURRENT_MESSAGE_VERSION, GSON, logger);
        }
    }
}
