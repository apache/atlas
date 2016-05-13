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
import org.slf4j.Logger;
import org.testng.annotations.Test;

import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.*;

/**
 * AbstractNotificationConsumer tests.
 */
public class AbstractNotificationConsumerTest {

    private static final Gson GSON = new Gson();

    @Test
    public void testNext() throws Exception {
        Logger logger = mock(Logger.class);

        TestMessage testMessage1 = new TestMessage("sValue1", 99);
        TestMessage testMessage2 = new TestMessage("sValue2", 98);
        TestMessage testMessage3 = new TestMessage("sValue3", 97);
        TestMessage testMessage4 = new TestMessage("sValue4", 96);

        List<String> jsonList = new LinkedList<>();

        jsonList.add(GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), testMessage1)));
        jsonList.add(GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), testMessage2)));
        jsonList.add(GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), testMessage3)));
        jsonList.add(GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), testMessage4)));

        Type versionedMessageType = new TypeToken<VersionedMessage<TestMessage>>(){}.getType();

        NotificationConsumer<TestMessage> consumer =
            new TestNotificationConsumer<>(versionedMessageType, jsonList, logger);

        assertTrue(consumer.hasNext());

        assertEquals(testMessage1, consumer.next());

        assertTrue(consumer.hasNext());

        assertEquals(testMessage2, consumer.next());

        assertTrue(consumer.hasNext());

        assertEquals(testMessage3, consumer.next());

        assertTrue(consumer.hasNext());

        assertEquals(testMessage4, consumer.next());

        assertFalse(consumer.hasNext());
    }

    @Test
    public void testNextBackVersion() throws Exception {
        Logger logger = mock(Logger.class);

        TestMessage testMessage1 = new TestMessage("sValue1", 99);
        TestMessage testMessage2 = new TestMessage("sValue2", 98);
        TestMessage testMessage3 = new TestMessage("sValue3", 97);
        TestMessage testMessage4 = new TestMessage("sValue4", 96);

        List<String> jsonList = new LinkedList<>();

        String json1 = GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), testMessage1));
        String json2 = GSON.toJson(new VersionedMessage<>(new MessageVersion("0.0.5"), testMessage2));
        String json3 = GSON.toJson(new VersionedMessage<>(new MessageVersion("0.5.0"), testMessage3));
        String json4 = GSON.toJson(testMessage4);

        jsonList.add(json1);
        jsonList.add(json2);
        jsonList.add(json3);
        jsonList.add(json4);

        Type versionedMessageType = new TypeToken<VersionedMessage<TestMessage>>(){}.getType();

        NotificationConsumer<TestMessage> consumer =
            new TestNotificationConsumer<>(versionedMessageType, jsonList, logger);
        assertTrue(consumer.hasNext());

        assertEquals(new TestMessage("sValue1", 99), consumer.next());

        assertTrue(consumer.hasNext());

        assertEquals(new TestMessage("sValue2", 98), consumer.next());
        verify(logger).info(json2);

        assertTrue(consumer.hasNext());

        assertEquals(new TestMessage("sValue3", 97), consumer.next());
        verify(logger).info(json3);

        assertTrue(consumer.hasNext());

        assertEquals(new TestMessage("sValue4", 96), consumer.next());
        verify(logger).info(json4);

        assertFalse(consumer.hasNext());
    }

    @Test
    public void testNextForwardVersion() throws Exception {
        Logger logger = mock(Logger.class);

        TestMessage testMessage1 = new TestMessage("sValue1", 99);
        TestMessage testMessage2 = new TestMessage("sValue2", 98);

        List<String> jsonList = new LinkedList<>();

        String json1 = GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), testMessage1));
        String json2 = GSON.toJson(new VersionedMessage<>(new MessageVersion("2.0.0"), testMessage2));

        jsonList.add(json1);
        jsonList.add(json2);

        Type versionedMessageType = new TypeToken<VersionedMessage<TestMessage>>(){}.getType();

        NotificationConsumer<TestMessage> consumer =
            new TestNotificationConsumer<>(versionedMessageType, jsonList, logger);
        assertTrue(consumer.hasNext());

        assertEquals(testMessage1, consumer.next());

        assertTrue(consumer.hasNext());

        try {
            consumer.next();
            fail("Expected VersionMismatchException!");
        } catch (IncompatibleVersionException e) {
            verify(logger).info(json2);
        }

        assertFalse(consumer.hasNext());
    }

    @Test
    public void testPeek() throws Exception {
        Logger logger = mock(Logger.class);

        TestMessage testMessage1 = new TestMessage("sValue1", 99);
        TestMessage testMessage2 = new TestMessage("sValue2", 98);
        TestMessage testMessage3 = new TestMessage("sValue3", 97);
        TestMessage testMessage4 = new TestMessage("sValue4", 96);

        List<String> jsonList = new LinkedList<>();

        jsonList.add(GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), testMessage1)));
        jsonList.add(GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), testMessage2)));
        jsonList.add(GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), testMessage3)));
        jsonList.add(GSON.toJson(new VersionedMessage<>(new MessageVersion("1.0.0"), testMessage4)));

        Type versionedMessageType = new TypeToken<VersionedMessage<TestMessage>>(){}.getType();

        NotificationConsumer<TestMessage> consumer =
            new TestNotificationConsumer<>(versionedMessageType, jsonList, logger);
        assertTrue(consumer.hasNext());

        assertEquals(testMessage1, consumer.peek());

        assertTrue(consumer.hasNext());

        assertEquals(testMessage1, consumer.peek());

        assertTrue(consumer.hasNext());
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
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TestMessage that = (TestMessage) o;

            return i == that.i && (s != null ? s.equals(that.s) : that.s == null);
        }

        @Override
        public int hashCode() {
            int result = s != null ? s.hashCode() : 0;
            result = 31 * result + i;
            return result;
        }
    }

    private static class TestNotificationConsumer<T> extends AbstractNotificationConsumer<T> {
        private final List<String> messageList;
        private int index = 0;

        public TestNotificationConsumer(Type versionedMessageType, List<String> messages, Logger logger) {
            super(new TestDeserializer<T>(versionedMessageType, logger));
            this.messageList = messages;
        }

        @Override
        protected String getNext() {
            return messageList.get(index++);
        }

        @Override
        protected String peekMessage() {
            return messageList.get(index);
        }

        @Override
        public boolean hasNext() {
            return index < messageList.size();
        }

        @Override
        public void commit() {
            // do nothing.
        }
    }

    private static final class TestDeserializer<T> extends VersionedMessageDeserializer<T> {

        private TestDeserializer(Type versionedMessageType, Logger logger) {
            super(versionedMessageType, AbstractNotification.CURRENT_MESSAGE_VERSION, GSON, logger);
        }
    }
}
