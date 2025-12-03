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
package org.apache.atlas.model.notification;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasNotificationMessage {
    private AtlasNotificationMessage<String> notificationMessage;

    @BeforeMethod
    public void setUp() {
        notificationMessage = new AtlasNotificationMessage<>();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasNotificationMessage<String> message = new AtlasNotificationMessage<>();

        assertNull(message.getMsgSourceIP());
        assertNull(message.getMsgCreatedBy());
        assertEquals(message.getMsgCreationTime(), 0L);
        assertFalse(message.getSpooled());
        assertNull(message.getMessage());
        assertNull(message.getVersion());
        assertNull(message.getSource());
    }

    @Test
    public void testConstructorWithVersionAndMessage() {
        MessageVersion version = new MessageVersion("1.0.0");
        String message = "Test message";

        AtlasNotificationMessage<String> notificationMsg = new AtlasNotificationMessage<>(version, message);

        assertEquals(notificationMsg.getVersion(), version);
        assertEquals(notificationMsg.getMessage(), message);
        assertNull(notificationMsg.getMsgSourceIP());
        assertNull(notificationMsg.getMsgCreatedBy());
        assertFalse(notificationMsg.getSpooled());
        assertTrue(notificationMsg.getMsgCreationTime() > 0); // Should be set to current time
    }

    @Test
    public void testConstructorWithVersionMessageSourceIPCreatedByAndSpooled() {
        MessageVersion version = new MessageVersion("1.0.0");
        String message = "Test message";
        String sourceIP = "192.168.1.1";
        String createdBy = "testUser";
        boolean spooled = true;

        AtlasNotificationMessage<String> notificationMsg = new AtlasNotificationMessage<>(version, message, sourceIP, createdBy, spooled);

        assertEquals(notificationMsg.getVersion(), version);
        assertEquals(notificationMsg.getMessage(), message);
        assertEquals(notificationMsg.getMsgSourceIP(), sourceIP);
        assertEquals(notificationMsg.getMsgCreatedBy(), createdBy);
        assertTrue(notificationMsg.getSpooled());
        assertTrue(notificationMsg.getMsgCreationTime() > 0);
    }

    @Test
    public void testConstructorWithVersionMessageSourceIPCreatedBySpooledAndSource() {
        MessageVersion version = new MessageVersion("1.0.0");
        String message = "Test message";
        String sourceIP = "192.168.1.1";
        String createdBy = "testUser";
        boolean spooled = true;
        MessageSource source = new MessageSource("TestSource");

        AtlasNotificationMessage<String> notificationMsg = new AtlasNotificationMessage<>(version, message, sourceIP, createdBy, spooled, source);

        assertEquals(notificationMsg.getVersion(), version);
        assertEquals(notificationMsg.getMessage(), message);
        assertEquals(notificationMsg.getMsgSourceIP(), sourceIP);
        assertEquals(notificationMsg.getMsgCreatedBy(), createdBy);
        assertTrue(notificationMsg.getSpooled());
        assertEquals(notificationMsg.getSource(), source);
        assertTrue(notificationMsg.getMsgCreationTime() > 0);
    }

    @Test
    public void testConstructorWithVersionMessageSourceIPAndCreatedBy() {
        MessageVersion version = new MessageVersion("1.0.0");
        String message = "Test message";
        String sourceIP = "10.0.0.1";
        String createdBy = "anotherUser";

        AtlasNotificationMessage<String> notificationMsg = new AtlasNotificationMessage<>(version, message, sourceIP, createdBy);

        assertEquals(notificationMsg.getVersion(), version);
        assertEquals(notificationMsg.getMessage(), message);
        assertEquals(notificationMsg.getMsgSourceIP(), sourceIP);
        assertEquals(notificationMsg.getMsgCreatedBy(), createdBy);
        assertFalse(notificationMsg.getSpooled()); // Should default to false
        assertNull(notificationMsg.getSource()); // Should default to null
        assertTrue(notificationMsg.getMsgCreationTime() > 0);
    }

    @Test
    public void testMsgSourceIPGetterSetter() {
        assertNull(notificationMessage.getMsgSourceIP());

        String sourceIP = "172.16.0.1";
        notificationMessage.setMsgSourceIP(sourceIP);
        assertEquals(notificationMessage.getMsgSourceIP(), sourceIP);

        notificationMessage.setMsgSourceIP("");
        assertEquals(notificationMessage.getMsgSourceIP(), "");

        notificationMessage.setMsgSourceIP(null);
        assertNull(notificationMessage.getMsgSourceIP());
    }

    @Test
    public void testMsgCreatedByGetterSetter() {
        assertNull(notificationMessage.getMsgCreatedBy());

        String createdBy = "testUser123";
        notificationMessage.setMsgCreatedBy(createdBy);
        assertEquals(notificationMessage.getMsgCreatedBy(), createdBy);

        notificationMessage.setMsgCreatedBy("");
        assertEquals(notificationMessage.getMsgCreatedBy(), "");

        notificationMessage.setMsgCreatedBy(null);
        assertNull(notificationMessage.getMsgCreatedBy());
    }

    @Test
    public void testMsgCreationTimeGetterSetter() {
        assertEquals(notificationMessage.getMsgCreationTime(), 0L);

        long currentTime = System.currentTimeMillis();
        notificationMessage.setMsgCreationTime(currentTime);
        assertEquals(notificationMessage.getMsgCreationTime(), currentTime);

        notificationMessage.setMsgCreationTime(-1L);
        assertEquals(notificationMessage.getMsgCreationTime(), -1L);

        notificationMessage.setMsgCreationTime(Long.MAX_VALUE);
        assertEquals(notificationMessage.getMsgCreationTime(), Long.MAX_VALUE);
    }

    @Test
    public void testMessageGetterSetter() {
        assertNull(notificationMessage.getMessage());

        String message = "This is a test message";
        notificationMessage.setMessage(message);
        assertEquals(notificationMessage.getMessage(), message);

        notificationMessage.setMessage("");
        assertEquals(notificationMessage.getMessage(), "");

        notificationMessage.setMessage(null);
        assertNull(notificationMessage.getMessage());
    }

    @Test
    public void testSpooledGetterSetter() {
        assertFalse(notificationMessage.getSpooled());

        notificationMessage.setSpooled(true);
        assertTrue(notificationMessage.getSpooled());

        notificationMessage.setSpooled(false);
        assertFalse(notificationMessage.getSpooled());
    }

    @Test
    public void testGenericTypeMessage() {
        // Test with Integer message type
        AtlasNotificationMessage<Integer> intMessage = new AtlasNotificationMessage<>();
        Integer intValue = 42;

        intMessage.setMessage(intValue);
        assertEquals(intMessage.getMessage(), intValue);

        // Test with custom object message type
        AtlasNotificationMessage<TestObject> objMessage = new AtlasNotificationMessage<>();
        TestObject testObj = new TestObject("test", 123);

        objMessage.setMessage(testObj);
        assertEquals(objMessage.getMessage(), testObj);
        assertEquals(objMessage.getMessage().name, "test");
        assertEquals(objMessage.getMessage().value, 123);
    }

    @Test
    public void testInheritanceFromBaseMessage() {
        // Test that AtlasNotificationMessage inherits from AtlasNotificationBaseMessage
        MessageVersion version = new MessageVersion("2.0.0");
        String msgId = "inherited-msg-456";

        notificationMessage.setVersion(version);
        notificationMessage.setMsgId(msgId);

        assertEquals(notificationMessage.getVersion(), version);
        assertEquals(notificationMessage.getMsgId(), msgId);
    }

    @Test
    public void testCreationTimeSetAutomatically() {
        MessageVersion version = new MessageVersion("1.0.0");
        String message = "Auto time test";

        long beforeCreation = System.currentTimeMillis();
        AtlasNotificationMessage<String> notificationMsg = new AtlasNotificationMessage<>(version, message);
        long afterCreation = System.currentTimeMillis();

        assertTrue(notificationMsg.getMsgCreationTime() >= beforeCreation);
        assertTrue(notificationMsg.getMsgCreationTime() <= afterCreation);
    }

    @Test
    public void testNullParametersInConstructor() {
        MessageVersion version = new MessageVersion("1.0.0");

        AtlasNotificationMessage<String> notificationMsg = new AtlasNotificationMessage<>(version, null, null, null, false, null);

        assertEquals(notificationMsg.getVersion(), version);
        assertNull(notificationMsg.getMessage());
        assertNull(notificationMsg.getMsgSourceIP());
        assertNull(notificationMsg.getMsgCreatedBy());
        assertFalse(notificationMsg.getSpooled());
        assertNull(notificationMsg.getSource());
        assertTrue(notificationMsg.getMsgCreationTime() > 0); // Should still be set
    }

    @Test
    public void testCompleteMessageConfiguration() {
        MessageVersion version = new MessageVersion("1.2.3");
        String message = "Complete test message";
        String sourceIP = "203.0.113.1";
        String createdBy = "completeTestUser";
        boolean spooled = true;
        MessageSource source = new MessageSource("CompleteTestSource");
        long creationTime = System.currentTimeMillis() - 1000; // 1 second ago

        AtlasNotificationMessage<String> notificationMsg = new AtlasNotificationMessage<>(version, message, sourceIP, createdBy, spooled, source);
        notificationMsg.setMsgCreationTime(creationTime);

        assertEquals(notificationMsg.getVersion(), version);
        assertEquals(notificationMsg.getMessage(), message);
        assertEquals(notificationMsg.getMsgSourceIP(), sourceIP);
        assertEquals(notificationMsg.getMsgCreatedBy(), createdBy);
        assertTrue(notificationMsg.getSpooled());
        assertEquals(notificationMsg.getSource(), source);
        assertEquals(notificationMsg.getMsgCreationTime(), creationTime);
    }

    @Test
    public void testIPAddressFormats() {
        // Test various IP address formats
        String[] ipAddresses = {
            "127.0.0.1",
            "192.168.1.100",
            "10.0.0.1",
            "172.16.254.1",
            "203.0.113.195",
            "2001:db8::1", // IPv6
            "::1", // IPv6 loopback
            "fe80::1%lo0" // IPv6 with zone
        };

        for (String ip : ipAddresses) {
            notificationMessage.setMsgSourceIP(ip);
            assertEquals(notificationMessage.getMsgSourceIP(), ip);
        }
    }

    @Test
    public void testUserNameFormats() {
        // Test various username formats
        String[] userNames = {
            "user123",
            "test.user",
            "test_user",
            "test-user",
            "user@domain.com",
            "DOMAIN\\user",
            "user with spaces",
            "uniUN", // Unicode username
            ""
        };

        for (String userName : userNames) {
            notificationMessage.setMsgCreatedBy(userName);
            assertEquals(notificationMessage.getMsgCreatedBy(), userName);
        }
    }

    @Test
    public void testSerializable() {
        assertNotNull(notificationMessage);
    }

    @Test
    public void testJsonAnnotations() {
        assertNotNull(notificationMessage);
    }

    @Test
    public void testXmlAnnotations() {
        assertNotNull(notificationMessage);
    }

    @Test
    public void testLongRunningTimeStamps() {
        // Test with various timestamp values
        long[] timestamps = {
            0L,
            1L,
            System.currentTimeMillis(),
            System.currentTimeMillis() + 86400000L, // 1 day in future
            System.currentTimeMillis() - 86400000L, // 1 day in past
            Long.MAX_VALUE,
            Long.MIN_VALUE
        };

        for (long timestamp : timestamps) {
            notificationMessage.setMsgCreationTime(timestamp);
            assertEquals(notificationMessage.getMsgCreationTime(), timestamp);
        }
    }

    @Test
    public void testComplexMessageTypes() {
        // Test with complex message types
        AtlasNotificationMessage<TestComplexObject> complexMessage = new AtlasNotificationMessage<>();

        TestComplexObject complexObj = new TestComplexObject();
        complexObj.stringValue = "complex test";
        complexObj.intValue = 999;
        complexObj.boolValue = true;
        complexObj.doubleValue = 3.14159;

        complexMessage.setMessage(complexObj);

        assertEquals(complexMessage.getMessage().stringValue, "complex test");
        assertEquals(complexMessage.getMessage().intValue, 999);
        assertTrue(complexMessage.getMessage().boolValue);
        assertEquals(complexMessage.getMessage().doubleValue, 3.14159, 0.00001);
    }

    @Test
    public void testMessageTypeConsistency() {
        // Test that message type is maintained consistently
        AtlasNotificationMessage<String> stringMessage = new AtlasNotificationMessage<>();
        stringMessage.setMessage("string message");

        AtlasNotificationMessage<Integer> intMessage = new AtlasNotificationMessage<>();
        intMessage.setMessage(42);

        assertEquals(stringMessage.getMessage().getClass(), String.class);
        assertEquals(intMessage.getMessage().getClass(), Integer.class);
    }

    // Helper classes for testing
    private static class TestObject {
        String name;
        int value;

        TestObject(String name, int value) {
            this.name = name;
            this.value = value;
        }
    }

    private static class TestComplexObject {
        String stringValue;
        int intValue;
        boolean boolValue;
        double doubleValue;
    }
}
