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

import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestAtlasNotificationStringMessage {
    private AtlasNotificationStringMessage stringMessage;

    @BeforeMethod
    public void setUp() {
        stringMessage = new AtlasNotificationStringMessage();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage();

        assertNull(message.getMessage());
        assertEquals(message.getVersion(), MessageVersion.CURRENT_VERSION);
    }

    @Test
    public void testConstructorWithMessage() {
        String testMessage = "Test notification message";
        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(testMessage);

        assertEquals(message.getMessage(), testMessage);
        assertEquals(message.getVersion(), MessageVersion.CURRENT_VERSION);
    }

    @Test
    public void testConstructorWithMessageAndMsgIdAndCompression() {
        String testMessage = "Test message with compression";
        String msgId = "msg-123";
        AtlasNotificationBaseMessage.CompressionKind compressionKind = AtlasNotificationBaseMessage.CompressionKind.GZIP;

        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                testMessage, msgId, compressionKind);

        assertEquals(message.getMessage(), testMessage);
        assertEquals(message.getMsgId(), msgId);
        assertEquals(message.getMsgCompressionKind(), compressionKind);
        assertEquals(message.getVersion(), MessageVersion.CURRENT_VERSION);
    }

    @Test
    public void testConstructorWithMessageAndMsgIdAndCompressionAndSplit() {
        String testMessage = "Test message with split";
        String msgId = "msg-456";
        AtlasNotificationBaseMessage.CompressionKind compressionKind = AtlasNotificationBaseMessage.CompressionKind.NONE;
        int msgSplitIdx = 1;
        int msgSplitCount = 3;

        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                testMessage, msgId, compressionKind, msgSplitIdx, msgSplitCount);

        assertEquals(message.getMessage(), testMessage);
        assertEquals(message.getMsgId(), msgId);
        assertEquals(message.getMsgCompressionKind(), compressionKind);
        assertEquals(message.getMsgSplitIdx(), msgSplitIdx);
        assertEquals(message.getMsgSplitCount(), msgSplitCount);
        assertEquals(message.getVersion(), MessageVersion.CURRENT_VERSION);
    }

    @Test
    public void testConstructorWithEncodedBytes() {
        String originalMessage = "Test encoded message";
        byte[] encodedBytes = originalMessage.getBytes(StandardCharsets.UTF_8);
        String msgId = "msg-789";
        AtlasNotificationBaseMessage.CompressionKind compressionKind = AtlasNotificationBaseMessage.CompressionKind.NONE;

        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                encodedBytes, msgId, compressionKind);

        assertEquals(message.getMessage(), originalMessage);
        assertEquals(message.getMsgId(), msgId);
        assertEquals(message.getMsgCompressionKind(), compressionKind);
    }

    @Test
    public void testConstructorWithEncodedBytesAndOffset() {
        String fullMessage = "PrefixTestMessageSuffix";
        String expectedMessage = "TestMessage";
        byte[] encodedBytes = fullMessage.getBytes(StandardCharsets.UTF_8);
        int offset = 6; // Skip "Prefix"
        int length = 11; // Length of "TestMessage"
        String msgId = "msg-offset";
        AtlasNotificationBaseMessage.CompressionKind compressionKind = AtlasNotificationBaseMessage.CompressionKind.NONE;
        int msgSplitIdx = 0;
        int msgSplitCount = 1;

        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                encodedBytes, offset, length, msgId, compressionKind, msgSplitIdx, msgSplitCount);

        assertEquals(message.getMessage(), expectedMessage);
        assertEquals(message.getMsgId(), msgId);
        assertEquals(message.getMsgCompressionKind(), compressionKind);
        assertEquals(message.getMsgSplitIdx(), msgSplitIdx);
        assertEquals(message.getMsgSplitCount(), msgSplitCount);
    }

    @Test
    public void testMessageGetterSetter() {
        assertNull(stringMessage.getMessage());

        String message = "Test message content";
        stringMessage.setMessage(message);
        assertEquals(stringMessage.getMessage(), message);

        stringMessage.setMessage("");
        assertEquals(stringMessage.getMessage(), "");

        stringMessage.setMessage(null);
        assertNull(stringMessage.getMessage());
    }

    @Test
    public void testMessageWithSpecialCharacters() {
        String specialMessage = "Message with special chars: !@#$%^&*()_+-={}[]|\\:;\"'<>,.?/";
        stringMessage.setMessage(specialMessage);
        assertEquals(stringMessage.getMessage(), specialMessage);
    }

    @Test
    public void testMessageWithUnicodeCharacters() {
        String unicodeMessage = "uniMes";
        stringMessage.setMessage(unicodeMessage);
        assertEquals(stringMessage.getMessage(), unicodeMessage);
    }

    @Test
    public void testMessageWithNewlines() {
        String multilineMessage = "Line 1\nLine 2\r\nLine 3\rLine 4";
        stringMessage.setMessage(multilineMessage);
        assertEquals(stringMessage.getMessage(), multilineMessage);
    }

    @Test
    public void testLongMessage() {
        StringBuilder longMessage = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            longMessage.append("This is a very long message part ").append(i).append(". ");
        }
        String longMessageStr = longMessage.toString();

        stringMessage.setMessage(longMessageStr);
        assertEquals(stringMessage.getMessage(), longMessageStr);
    }

    @Test
    public void testEmptyMessage() {
        stringMessage.setMessage("");
        assertEquals(stringMessage.getMessage(), "");
    }

    @Test
    public void testNullMessage() {
        stringMessage.setMessage(null);
        assertNull(stringMessage.getMessage());
    }

    @Test
    public void testSerializable() {
        assertNotNull(stringMessage);
    }

    @Test
    public void testSerialVersionUID() throws Exception {
        java.lang.reflect.Field serialVersionUIDField = AtlasNotificationStringMessage.class.getDeclaredField("serialVersionUID");
        serialVersionUIDField.setAccessible(true);
        long serialVersionUID = (Long) serialVersionUIDField.get(null);

        assertEquals(serialVersionUID, 1L);
    }

    @Test
    public void testJsonAnnotations() {
        assertNotNull(stringMessage);
    }

    @Test
    public void testXmlAnnotations() {
        assertNotNull(stringMessage);
    }

    @Test
    public void testInheritanceFromBaseMessage() {
        assertTrue(stringMessage instanceof AtlasNotificationBaseMessage);

        // Test inherited functionality
        stringMessage.setMsgId("inherited-test-id");
        assertEquals(stringMessage.getMsgId(), "inherited-test-id");

        stringMessage.setMsgCompressed(AtlasNotificationBaseMessage.CompressionKind.GZIP);
        assertEquals(stringMessage.getMsgCompressionKind(), AtlasNotificationBaseMessage.CompressionKind.GZIP);
    }

    @Test
    public void testConstructorWithNullMessage() {
        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(null);
        assertNull(message.getMessage());
    }

    @Test
    public void testConstructorWithEmptyMessage() {
        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage("");
        assertEquals(message.getMessage(), "");
    }

    @Test
    public void testConstructorWithNullMsgId() {
        String testMessage = "Test message";
        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                testMessage, null, AtlasNotificationBaseMessage.CompressionKind.NONE);

        assertEquals(message.getMessage(), testMessage);
        assertNull(message.getMsgId());
    }

    @Test
    public void testConstructorWithNullCompressionKind() {
        String testMessage = "Test message";
        String msgId = "test-id";
        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                testMessage, msgId, null);

        assertEquals(message.getMessage(), testMessage);
        assertEquals(message.getMsgId(), msgId);
        assertNull(message.getMsgCompressionKind());
    }

    @Test
    public void testConstructorWithZeroSplitValues() {
        String testMessage = "Test message";
        String msgId = "test-id";
        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                testMessage, msgId, AtlasNotificationBaseMessage.CompressionKind.NONE, 0, 0);

        assertEquals(message.getMessage(), testMessage);
        assertEquals(message.getMsgSplitIdx(), 0);
        assertEquals(message.getMsgSplitCount(), 0);
    }

    @Test
    public void testConstructorWithNegativeSplitValues() {
        String testMessage = "Test message";
        String msgId = "test-id";
        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                testMessage, msgId, AtlasNotificationBaseMessage.CompressionKind.NONE, -1, -1);

        assertEquals(message.getMessage(), testMessage);
        assertEquals(message.getMsgSplitIdx(), -1);
        assertEquals(message.getMsgSplitCount(), -1);
    }

    @Test
    public void testConstructorWithEmptyEncodedBytes() {
        byte[] emptyBytes = new byte[0];
        String msgId = "empty-bytes-id";
        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                emptyBytes, msgId, AtlasNotificationBaseMessage.CompressionKind.NONE);

        assertEquals(message.getMessage(), "");
        assertEquals(message.getMsgId(), msgId);
    }

    @Test
    public void testConstructorWithZeroOffsetAndLength() {
        String originalMessage = "TestMessage";
        byte[] encodedBytes = originalMessage.getBytes(StandardCharsets.UTF_8);
        String msgId = "zero-offset-id";

        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                encodedBytes, 0, 0, msgId, AtlasNotificationBaseMessage.CompressionKind.NONE, 0, 1);

        assertEquals(message.getMessage(), "");
        assertEquals(message.getMsgId(), msgId);
    }

    @Test
    public void testConstructorWithFullLengthOffset() {
        String originalMessage = "TestMessage";
        byte[] encodedBytes = originalMessage.getBytes(StandardCharsets.UTF_8);
        String msgId = "full-length-id";

        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                encodedBytes, 0, encodedBytes.length, msgId, AtlasNotificationBaseMessage.CompressionKind.NONE, 0, 1);

        assertEquals(message.getMessage(), originalMessage);
        assertEquals(message.getMsgId(), msgId);
    }

    @Test
    public void testAllCompressionKinds() {
        String testMessage = "Test compression message";
        AtlasNotificationBaseMessage.CompressionKind[] compressionKinds = AtlasNotificationBaseMessage.CompressionKind.values();

        for (AtlasNotificationBaseMessage.CompressionKind kind : compressionKinds) {
            AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                    testMessage, "test-id-" + kind.name(), kind);

            assertEquals(message.getMessage(), testMessage);
            assertEquals(message.getMsgCompressionKind(), kind);
        }
    }

    @Test
    public void testCompleteMessageScenario() {
        // Create a complete message with all parameters
        String originalMessage = "Complete test message with all parameters";
        String msgId = "complete-test-id";
        AtlasNotificationBaseMessage.CompressionKind compressionKind = AtlasNotificationBaseMessage.CompressionKind.GZIP;
        int msgSplitIdx = 2;
        int msgSplitCount = 5;

        AtlasNotificationStringMessage message = new AtlasNotificationStringMessage(
                originalMessage, msgId, compressionKind, msgSplitIdx, msgSplitCount);

        // Verify all fields
        assertEquals(message.getMessage(), originalMessage);
        assertEquals(message.getMsgId(), msgId);
        assertEquals(message.getMsgCompressionKind(), compressionKind);
        assertEquals(message.getMsgSplitIdx(), msgSplitIdx);
        assertEquals(message.getMsgSplitCount(), msgSplitCount);
        assertEquals(message.getVersion(), MessageVersion.CURRENT_VERSION);

        // Test modification after creation
        String newMessage = "Modified message";
        message.setMessage(newMessage);
        assertEquals(message.getMessage(), newMessage);
    }

    private static void assertTrue(boolean condition) {
        if (!condition) {
            throw new AssertionError("Expected true but was false");
        }
    }
}
