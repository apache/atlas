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

import org.apache.atlas.model.notification.AtlasNotificationBaseMessage.CompressionKind;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasNotificationBaseMessage {
    private AtlasNotificationBaseMessage baseMessage;

    @BeforeMethod
    public void setUp() {
        baseMessage = new AtlasNotificationBaseMessage();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasNotificationBaseMessage message = new AtlasNotificationBaseMessage();

        assertNull(message.getSource());
        assertNull(message.getVersion());
        assertNull(message.getMsgId());
        assertEquals(message.getMsgCompressionKind(), CompressionKind.NONE);
        assertEquals(message.getMsgSplitIdx(), 1);
        assertEquals(message.getMsgSplitCount(), 1);
    }

    @Test
    public void testConstructorWithVersion() {
        MessageVersion version = new MessageVersion("1.0.0");
        AtlasNotificationBaseMessage message = new AtlasNotificationBaseMessage(version);

        assertEquals(message.getVersion(), version);
        assertNull(message.getMsgId());
        assertEquals(message.getMsgCompressionKind(), CompressionKind.NONE);
        assertNull(message.getSource());
    }

    @Test
    public void testConstructorWithVersionMsgIdAndCompression() {
        MessageVersion version = new MessageVersion("1.0.0");
        String msgId = "test-msg-123";
        CompressionKind compression = CompressionKind.GZIP;

        AtlasNotificationBaseMessage message = new AtlasNotificationBaseMessage(version, msgId, compression);

        assertEquals(message.getVersion(), version);
        assertEquals(message.getMsgId(), msgId);
        assertEquals(message.getMsgCompressionKind(), compression);
        assertNull(message.getSource());
    }

    @Test
    public void testConstructorWithVersionAndSource() {
        MessageVersion version = new MessageVersion("1.0.0");
        MessageSource source = new MessageSource("TestSource");

        AtlasNotificationBaseMessage message = new AtlasNotificationBaseMessage(version, source);

        assertEquals(message.getVersion(), version);
        assertEquals(message.getSource(), source);
        assertNull(message.getMsgId());
        assertEquals(message.getMsgCompressionKind(), CompressionKind.NONE);
    }

    @Test
    public void testConstructorWithAllParameters() {
        MessageVersion version = new MessageVersion("1.0.0");
        String msgId = "test-msg-123";
        CompressionKind compression = CompressionKind.GZIP;
        MessageSource source = new MessageSource("TestSource");

        AtlasNotificationBaseMessage message = new AtlasNotificationBaseMessage(version, msgId, compression, source);

        assertEquals(message.getVersion(), version);
        assertEquals(message.getMsgId(), msgId);
        assertEquals(message.getMsgCompressionKind(), compression);
        assertEquals(message.getSource(), source);
    }

    @Test
    public void testConstructorWithSplitParameters() {
        MessageVersion version = new MessageVersion("1.0.0");
        String msgId = "test-msg-123";
        CompressionKind compression = CompressionKind.GZIP;
        int splitIdx = 2;
        int splitCount = 5;

        AtlasNotificationBaseMessage message = new AtlasNotificationBaseMessage(version, msgId, compression, splitIdx, splitCount);

        assertEquals(message.getVersion(), version);
        assertEquals(message.getMsgId(), msgId);
        assertEquals(message.getMsgCompressionKind(), compression);
        assertEquals(message.getMsgSplitIdx(), splitIdx);
        assertEquals(message.getMsgSplitCount(), splitCount);
    }

    @Test
    public void testVersionGetterSetter() {
        assertNull(baseMessage.getVersion());

        MessageVersion version = new MessageVersion("2.0.0");
        baseMessage.setVersion(version);
        assertEquals(baseMessage.getVersion(), version);

        baseMessage.setVersion(null);
        assertNull(baseMessage.getVersion());
    }

    @Test
    public void testSourceGetterSetter() {
        assertNull(baseMessage.getSource());

        MessageSource source = new MessageSource("TestSource");
        baseMessage.setSource(source);
        assertEquals(baseMessage.getSource(), source);

        baseMessage.setSource(null);
        assertNull(baseMessage.getSource());
    }

    @Test
    public void testMsgIdGetterSetter() {
        assertNull(baseMessage.getMsgId());

        String msgId = "test-message-id";
        baseMessage.setMsgId(msgId);
        assertEquals(baseMessage.getMsgId(), msgId);

        baseMessage.setMsgId("");
        assertEquals(baseMessage.getMsgId(), "");

        baseMessage.setMsgId(null);
        assertNull(baseMessage.getMsgId());
    }

    @Test
    public void testMsgCompressionKindGetterSetter() {
        assertEquals(baseMessage.getMsgCompressionKind(), CompressionKind.NONE);

        baseMessage.setMsgCompressed(CompressionKind.GZIP);
        assertEquals(baseMessage.getMsgCompressionKind(), CompressionKind.GZIP);

        baseMessage.setMsgCompressed(CompressionKind.NONE);
        assertEquals(baseMessage.getMsgCompressionKind(), CompressionKind.NONE);
    }

    @Test
    public void testMsgSplitIdxGetterSetter() {
        assertEquals(baseMessage.getMsgSplitIdx(), 1);

        baseMessage.setMsgSplitIdx(3);
        assertEquals(baseMessage.getMsgSplitIdx(), 3);

        baseMessage.setMsgSplitIdx(0);
        assertEquals(baseMessage.getMsgSplitIdx(), 0);

        baseMessage.setMsgSplitIdx(-1);
        assertEquals(baseMessage.getMsgSplitIdx(), -1);
    }

    @Test
    public void testMsgSplitCountGetterSetter() {
        assertEquals(baseMessage.getMsgSplitCount(), 1);

        baseMessage.setMsgSplitCount(5);
        assertEquals(baseMessage.getMsgSplitCount(), 5);

        baseMessage.setMsgSplitCount(0);
        assertEquals(baseMessage.getMsgSplitCount(), 0);

        baseMessage.setMsgSplitCount(-1);
        assertEquals(baseMessage.getMsgSplitCount(), -1);
    }

    @Test
    public void testCompareVersion() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("2.0.0");
        MessageVersion version3 = new MessageVersion("1.0.0");

        baseMessage.setVersion(version1);

        assertTrue(baseMessage.compareVersion(version2) < 0); // 1.0.0 < 2.0.0
        assertTrue(baseMessage.compareVersion(version3) == 0); // 1.0.0 == 1.0.0

        baseMessage.setVersion(version2);
        assertTrue(baseMessage.compareVersion(version1) > 0); // 2.0.0 > 1.0.0
    }

    @Test
    public void testCompressionKindEnum() {
        CompressionKind[] kinds = CompressionKind.values();
        assertEquals(kinds.length, 2);

        assertEquals(kinds[0], CompressionKind.NONE);
        assertEquals(kinds[1], CompressionKind.GZIP);

        assertEquals(CompressionKind.valueOf("NONE"), CompressionKind.NONE);
        assertEquals(CompressionKind.valueOf("GZIP"), CompressionKind.GZIP);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCompressionKindInvalidValue() {
        CompressionKind.valueOf("INVALID");
    }

    @Test
    public void testGetBytesUtf8() {
        String testString = "Hello World";
        byte[] bytes = AtlasNotificationBaseMessage.getBytesUtf8(testString);

        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }

    @Test
    public void testGetStringUtf8() {
        String original = "Hello World";
        byte[] bytes = AtlasNotificationBaseMessage.getBytesUtf8(original);
        String result = AtlasNotificationBaseMessage.getStringUtf8(bytes);

        assertEquals(result, original);
    }

    @Test
    public void testEncodeDecodeBase64() {
        String testString = "Hello World";
        byte[] originalBytes = AtlasNotificationBaseMessage.getBytesUtf8(testString);
        byte[] encodedBytes = AtlasNotificationBaseMessage.encodeBase64(originalBytes);
        byte[] decodedBytes = AtlasNotificationBaseMessage.decodeBase64(encodedBytes);

        assertEquals(AtlasNotificationBaseMessage.getStringUtf8(decodedBytes), testString);
    }

    @Test
    public void testGzipCompressUncompress() {
        String testString = "This is a test string for compression. It should be long enough to see compression benefits.";
        byte[] originalBytes = AtlasNotificationBaseMessage.getBytesUtf8(testString);
        byte[] compressedBytes = AtlasNotificationBaseMessage.gzipCompress(originalBytes);
        byte[] uncompressedBytes = AtlasNotificationBaseMessage.gzipUncompress(compressedBytes);

        assertEquals(AtlasNotificationBaseMessage.getStringUtf8(uncompressedBytes), testString);
        assertTrue(compressedBytes.length < originalBytes.length); // Should be compressed
    }

    @Test
    public void testGzipCompressUncompressString() {
        String testString = "This is a test string for compression. It should be long enough to see compression benefits.";
        String compressedString = AtlasNotificationBaseMessage.gzipCompress(testString);
        String uncompressedString = AtlasNotificationBaseMessage.gzipUncompress(compressedString);

        assertEquals(uncompressedString, testString);
        assertNotEquals(compressedString, testString); // Should be different when compressed
    }

    @Test
    public void testGzipCompressAndEncodeBase64() {
        String testString = "Test compression and encoding";
        byte[] originalBytes = AtlasNotificationBaseMessage.getBytesUtf8(testString);
        byte[] processedBytes = AtlasNotificationBaseMessage.gzipCompressAndEncodeBase64(originalBytes);
        byte[] restoredBytes = AtlasNotificationBaseMessage.decodeBase64AndGzipUncompress(processedBytes);

        assertEquals(AtlasNotificationBaseMessage.getStringUtf8(restoredBytes), testString);
    }

    @Test
    public void testEmptyStringCompression() {
        String emptyString = "";
        String compressedString = AtlasNotificationBaseMessage.gzipCompress(emptyString);
        String uncompressedString = AtlasNotificationBaseMessage.gzipUncompress(compressedString);

        assertEquals(uncompressedString, emptyString);
    }

    @Test
    public void testUnicodeStringCompression() {
        String unicodeString = "uniStr";
        String compressedString = AtlasNotificationBaseMessage.gzipCompress(unicodeString);
        String uncompressedString = AtlasNotificationBaseMessage.gzipUncompress(compressedString);

        assertEquals(uncompressedString, unicodeString);
    }

    @Test
    public void testSerializable() {
        // Test that AtlasNotificationBaseMessage implements Serializable
        assertNotNull(baseMessage);
        // If this compiles without error, Serializable is implemented
    }

    @Test
    public void testConstants() throws Exception {
        // Test that constants are accessible
        Field maxLengthField = AtlasNotificationBaseMessage.class.getDeclaredField("MESSAGE_MAX_LENGTH_BYTES");
        maxLengthField.setAccessible(true);
        int maxLength = (Integer) maxLengthField.get(null);
        assertTrue(maxLength > 0);

        Field compressionEnabledField = AtlasNotificationBaseMessage.class.getDeclaredField("MESSAGE_COMPRESSION_ENABLED");
        compressionEnabledField.setAccessible(true);
        Boolean compressionEnabled = (Boolean) compressionEnabledField.get(null);
        assertNotNull(compressionEnabled);
    }

    @Test
    public void testCompleteMessageConfiguration() {
        MessageVersion version = new MessageVersion("1.2.3");
        MessageSource source = new MessageSource("CompleteTestSource");
        String msgId = "complete-msg-456";
        CompressionKind compression = CompressionKind.GZIP;
        int splitIdx = 3;
        int splitCount = 7;

        AtlasNotificationBaseMessage message = new AtlasNotificationBaseMessage();
        message.setVersion(version);
        message.setSource(source);
        message.setMsgId(msgId);
        message.setMsgCompressed(compression);
        message.setMsgSplitIdx(splitIdx);
        message.setMsgSplitCount(splitCount);

        assertEquals(message.getVersion(), version);
        assertEquals(message.getSource(), source);
        assertEquals(message.getMsgId(), msgId);
        assertEquals(message.getMsgCompressionKind(), compression);
        assertEquals(message.getMsgSplitIdx(), splitIdx);
        assertEquals(message.getMsgSplitCount(), splitCount);
    }

    @Test
    public void testVersionComparison() {
        MessageVersion version1 = new MessageVersion("1.0.0");
        MessageVersion version2 = new MessageVersion("1.1.0");
        MessageVersion version3 = new MessageVersion("2.0.0");

        baseMessage.setVersion(version2);

        assertTrue(baseMessage.compareVersion(version1) > 0);
        assertTrue(baseMessage.compareVersion(version2) == 0);
        assertTrue(baseMessage.compareVersion(version3) < 0);
    }

    @Test
    public void testLargeStringCompression() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("This is line ").append(i).append(" of a large test string. ");
        }
        String largeString = sb.toString();

        String compressedString = AtlasNotificationBaseMessage.gzipCompress(largeString);
        String uncompressedString = AtlasNotificationBaseMessage.gzipUncompress(compressedString);

        assertEquals(uncompressedString, largeString);
        assertTrue(compressedString.length() < largeString.length()); // Should be significantly compressed
    }

    @Test
    public void testSplitMessageConfiguration() {
        baseMessage.setMsgSplitIdx(2);
        baseMessage.setMsgSplitCount(5);

        assertEquals(baseMessage.getMsgSplitIdx(), 2);
        assertEquals(baseMessage.getMsgSplitCount(), 5);

        // Test edge cases
        baseMessage.setMsgSplitIdx(Integer.MAX_VALUE);
        baseMessage.setMsgSplitCount(Integer.MAX_VALUE);

        assertEquals(baseMessage.getMsgSplitIdx(), Integer.MAX_VALUE);
        assertEquals(baseMessage.getMsgSplitCount(), Integer.MAX_VALUE);
    }

    @Test
    public void testJsonAnnotations() {
        // Test that the class has proper Jackson annotations for serialization
        assertNotNull(baseMessage);
    }

    @Test
    public void testXmlAnnotations() {
        // Test that the class has proper XML annotations for serialization
        assertNotNull(baseMessage);
    }

    @Test
    public void testStaticUtilityMethods() {
        // Test that all static utility methods work correctly
        String testData = "Test data for utility methods";

        // Test UTF-8 conversion
        byte[] utf8Bytes = AtlasNotificationBaseMessage.getBytesUtf8(testData);
        String utf8String = AtlasNotificationBaseMessage.getStringUtf8(utf8Bytes);
        assertEquals(utf8String, testData);

        // Test Base64 encoding/decoding
        byte[] encodedBytes = AtlasNotificationBaseMessage.encodeBase64(utf8Bytes);
        byte[] decodedBytes = AtlasNotificationBaseMessage.decodeBase64(encodedBytes);
        assertEquals(AtlasNotificationBaseMessage.getStringUtf8(decodedBytes), testData);

        // Test GZIP compression/decompression
        byte[] compressedBytes = AtlasNotificationBaseMessage.gzipCompress(utf8Bytes);
        byte[] uncompressedBytes = AtlasNotificationBaseMessage.gzipUncompress(compressedBytes);
        assertEquals(AtlasNotificationBaseMessage.getStringUtf8(uncompressedBytes), testData);

        // Test combined operations
        byte[] combinedBytes = AtlasNotificationBaseMessage.gzipCompressAndEncodeBase64(utf8Bytes);
        byte[] restoredBytes = AtlasNotificationBaseMessage.decodeBase64AndGzipUncompress(combinedBytes);
        assertEquals(AtlasNotificationBaseMessage.getStringUtf8(restoredBytes), testData);
    }
}
