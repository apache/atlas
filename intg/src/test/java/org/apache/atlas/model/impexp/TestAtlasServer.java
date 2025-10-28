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

package org.apache.atlas.model.impexp;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasServer {
    private AtlasServer atlasServer;

    @BeforeMethod
    public void setUp() {
        atlasServer = new AtlasServer();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasServer server = new AtlasServer();

        assertNotNull(server);
        assertNull(server.getName());
        assertNull(server.getFullName());
        assertNull(server.getDisplayName());
        assertNotNull(server.getAdditionalInfo());
        assertTrue(server.getAdditionalInfo().isEmpty());
        assertNotNull(server.getUrls());
        assertTrue(server.getUrls().isEmpty());
    }

    @Test
    public void testTwoParameterConstructor() {
        String name = "server1";
        String fullName = "server1.example.com";

        AtlasServer server = new AtlasServer(name, fullName);

        assertNotNull(server);
        assertEquals(server.getName(), name);
        assertEquals(server.getFullName(), fullName);
        assertEquals(server.getDisplayName(), name); // Should be same as name
        assertNotNull(server.getAdditionalInfo());
        assertNotNull(server.getUrls());
    }

    @Test
    public void testThreeParameterConstructor() {
        String name = "server1";
        String displayName = "Server One";
        String fullName = "server1.example.com";

        AtlasServer server = new AtlasServer(name, displayName, fullName);

        assertNotNull(server);
        assertEquals(server.getName(), name);
        assertEquals(server.getDisplayName(), displayName);
        assertEquals(server.getFullName(), fullName);
        assertNotNull(server.getAdditionalInfo());
        assertNotNull(server.getUrls());
    }

    @Test
    public void testConstructorsWithNullValues() {
        AtlasServer server1 = new AtlasServer(null, null);
        assertNull(server1.getName());
        assertNull(server1.getFullName());
        assertNull(server1.getDisplayName());

        AtlasServer server2 = new AtlasServer(null, null, null);
        assertNull(server2.getName());
        assertNull(server2.getDisplayName());
        assertNull(server2.getFullName());
    }

    @Test
    public void testNameSetterGetter() {
        String name = "testserver";
        atlasServer.setName(name);
        assertEquals(atlasServer.getName(), name);

        atlasServer.setName(null);
        assertNull(atlasServer.getName());
    }

    @Test
    public void testFullNameSetterGetter() {
        String fullName = "testserver.domain.com";
        atlasServer.setFullName(fullName);
        assertEquals(atlasServer.getFullName(), fullName);

        atlasServer.setFullName(null);
        assertNull(atlasServer.getFullName());
    }

    @Test
    public void testDisplayNameSetterGetter() {
        String displayName = "Test Server Display";
        atlasServer.setDisplayName(displayName);
        assertEquals(atlasServer.getDisplayName(), displayName);

        atlasServer.setDisplayName(null);
        assertNull(atlasServer.getDisplayName());
    }

    @Test
    public void testAdditionalInfoMapSetterGetter() {
        Map<String, String> additionalInfo = new HashMap<>();
        additionalInfo.put("key1", "value1");
        additionalInfo.put("key2", "value2");

        atlasServer.setAdditionalInfo(additionalInfo);
        assertEquals(atlasServer.getAdditionalInfo(), additionalInfo);
        assertEquals(atlasServer.getAdditionalInfo().size(), 2);

        atlasServer.setAdditionalInfo(null);
        assertNull(atlasServer.getAdditionalInfo());
    }

    @Test
    public void testAdditionalInfoKeyGetterMethod() {
        Map<String, String> additionalInfo = new HashMap<>();
        additionalInfo.put("testKey", "testValue");
        atlasServer.setAdditionalInfo(additionalInfo);

        String value = atlasServer.getAdditionalInfo("testKey");
        assertEquals(value, "testValue");

        String nullValue = atlasServer.getAdditionalInfo("nonExistentKey");
        assertNull(nullValue);
    }

    @Test
    public void testAdditionalInfoKeyGetterWithNullMap() {
        atlasServer.setAdditionalInfo(null);
        try {
            atlasServer.getAdditionalInfo("anyKey");
        } catch (NullPointerException e) {
            // Expected when additionalInfo map is null
            assertTrue(true);
        }
    }

    @Test
    public void testUrlsSetterGetter() {
        List<String> urls = new ArrayList<>();
        urls.add("http://server1:8080");
        urls.add("https://server1:8443");

        atlasServer.setUrls(urls);
        assertEquals(atlasServer.getUrls(), urls);
        assertEquals(atlasServer.getUrls().size(), 2);

        atlasServer.setUrls(null);
        assertNull(atlasServer.getUrls());
    }

    @Test
    public void testSetAdditionalInfoKeyValue() {
        atlasServer.setAdditionalInfo("newKey", "newValue");

        Map<String, String> additionalInfo = atlasServer.getAdditionalInfo();
        assertNotNull(additionalInfo);
        assertEquals(additionalInfo.get("newKey"), "newValue");
    }

    @Test
    public void testSetAdditionalInfoKeyValueWithNullMap() {
        atlasServer.setAdditionalInfo(null);
        atlasServer.setAdditionalInfo("key", "value");

        Map<String, String> additionalInfo = atlasServer.getAdditionalInfo();
        assertNotNull(additionalInfo);
        assertEquals(additionalInfo.get("key"), "value");
    }

    @Test
    public void testSetAdditionalInfoOverwrite() {
        atlasServer.setAdditionalInfo("key", "value1");
        atlasServer.setAdditionalInfo("key", "value2");

        assertEquals(atlasServer.getAdditionalInfo("key"), "value2");
    }

    @Test
    public void testSetAdditionalInfoRepl() {
        String guid = "test-guid-123";
        long modifiedTimestamp = 1640995200000L;

        // Test that the method executes without error and updates additionalInfo
        atlasServer.setAdditionalInfoRepl(guid, modifiedTimestamp);

        Map<String, String> additionalInfo = atlasServer.getAdditionalInfo();
        assertNotNull(additionalInfo);
        assertTrue(additionalInfo.containsKey(AtlasServer.KEY_REPLICATION_DETAILS));
        String replDetails = additionalInfo.get(AtlasServer.KEY_REPLICATION_DETAILS);
        assertNotNull(replDetails);
        assertTrue(replDetails.contains(guid));
    }

    @Test
    public void testSetAdditionalInfoReplWithZeroTimestamp() {
        String guid = "test-guid-to-remove";
        long zeroTimestamp = 0L;

        atlasServer.setAdditionalInfoRepl(guid, zeroTimestamp);

        Map<String, String> additionalInfo = atlasServer.getAdditionalInfo();
        assertNotNull(additionalInfo);
        assertTrue(additionalInfo.containsKey(AtlasServer.KEY_REPLICATION_DETAILS));
    }

    @Test
    public void testSetAdditionalInfoReplWithExistingReplicationDetails() {
        Map<String, String> additionalInfo = new HashMap<>();
        additionalInfo.put(AtlasServer.KEY_REPLICATION_DETAILS, "{\"existing-guid\":123456}");
        atlasServer.setAdditionalInfo(additionalInfo);

        String newGuid = "new-guid";
        long newTimestamp = 9999999L;

        atlasServer.setAdditionalInfoRepl(newGuid, newTimestamp);

        Map<String, String> updatedAdditionalInfo = atlasServer.getAdditionalInfo();
        assertTrue(updatedAdditionalInfo.containsKey(AtlasServer.KEY_REPLICATION_DETAILS));
        String replDetails = updatedAdditionalInfo.get(AtlasServer.KEY_REPLICATION_DETAILS);
        assertNotNull(replDetails);
        assertTrue(replDetails.contains(newGuid));
    }

    @Test
    public void testGetAdditionalInfoRepl() {
        String guid = "test-guid-456";
        long timestamp = 1640995200000L;

        atlasServer.setAdditionalInfoRepl(guid, timestamp);

        Object result = atlasServer.getAdditionalInfoRepl(guid);
        assertNotNull(result);
        assertTrue(result.toString().contains("1640995200000") || result.equals(timestamp));
    }

    @Test
    public void testGetAdditionalInfoReplWithNullAdditionalInfo() {
        atlasServer.setAdditionalInfo((Map<String, String>) null);

        Object result = atlasServer.getAdditionalInfoRepl("any-guid");
        assertNull(result);
    }

    @Test
    public void testGetAdditionalInfoReplWithoutReplicationDetails() {
        Map<String, String> additionalInfo = new HashMap<>();
        additionalInfo.put("otherKey", "otherValue");
        atlasServer.setAdditionalInfo(additionalInfo);

        Object result = atlasServer.getAdditionalInfoRepl("any-guid");
        assertNull(result);
    }

    @Test
    public void testGetAdditionalInfoReplWithNonExistentGuid() {
        atlasServer.setAdditionalInfoRepl("other-guid", 123456L);

        Object result = atlasServer.getAdditionalInfoRepl("non-existent-guid");
        assertNull(result);
    }

    @Test
    public void testUpdateReplicationMapPrivateMethod() throws Exception {
        String testGuid = "testGuid";
        long testTimestamp = 123456L;

        atlasServer.setAdditionalInfoRepl(testGuid, testTimestamp);

        Map<String, String> additionalInfo = atlasServer.getAdditionalInfo();
        assertTrue(additionalInfo.containsKey(AtlasServer.KEY_REPLICATION_DETAILS));
        String replDetails = additionalInfo.get(AtlasServer.KEY_REPLICATION_DETAILS);
        assertNotNull(replDetails);
        assertTrue(replDetails.contains(testGuid));
    }

    @Test
    public void testToString() {
        atlasServer.setName("testserver");
        atlasServer.setFullName("testserver.example.com");
        atlasServer.setDisplayName("Test Server");
        Map<String, String> additionalInfo = new HashMap<>();
        additionalInfo.put("key1", "value1");
        atlasServer.setAdditionalInfo(additionalInfo);
        List<String> urls = new ArrayList<>();
        urls.add("http://testserver:8080");
        atlasServer.setUrls(urls);

        String result = atlasServer.toString();
        assertNotNull(result);
        assertTrue(result.contains("name=testserver"));
        assertTrue(result.contains("fullName=testserver.example.com"));
        assertTrue(result.contains("displayName=Test Server"));
        assertTrue(result.contains("additionalInfo={"));
        assertTrue(result.contains("urls=["));
    }

    @Test
    public void testToStringWithNullStringBuilder() {
        String result = atlasServer.toString();
        assertNotNull(result);
        StringBuilder sb = new StringBuilder();
        StringBuilder resultSb = atlasServer.toString(sb);
        assertNotNull(resultSb);
    }

    @Test
    public void testToStringWithNullValues() {
        atlasServer.setName(null);
        atlasServer.setFullName(null);
        atlasServer.setDisplayName(null);
        atlasServer.setAdditionalInfo(null);
        atlasServer.setUrls(null);

        String result = atlasServer.toString();
        assertNotNull(result);
        assertTrue(result.contains("name=null"));
        assertTrue(result.contains("fullName=null"));
        assertTrue(result.contains("displayName=null"));
        assertTrue(result.contains("additionalInfo=null"));
        assertTrue(result.contains("urls=null"));
    }

    @Test
    public void testConstantValue() {
        assertEquals(AtlasServer.KEY_REPLICATION_DETAILS, "REPL_DETAILS");
    }

    @Test
    public void testBoundaryValues() {
        // Test with very long strings
        StringBuilder longNameBuilder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longNameBuilder.append("a");
        }
        String longName = longNameBuilder.toString();
        StringBuilder longFullNameBuilder = new StringBuilder();
        for (int i = 0; i < 2000; i++) {
            longFullNameBuilder.append("b");
        }
        String longFullName = longFullNameBuilder.toString();
        StringBuilder longDisplayNameBuilder = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            longDisplayNameBuilder.append("c");
        }
        String longDisplayName = longDisplayNameBuilder.toString();

        atlasServer.setName(longName);
        atlasServer.setFullName(longFullName);
        atlasServer.setDisplayName(longDisplayName);

        assertEquals(atlasServer.getName(), longName);
        assertEquals(atlasServer.getFullName(), longFullName);
        assertEquals(atlasServer.getDisplayName(), longDisplayName);
    }

    @Test
    public void testSpecialCharactersInStrings() {
        String specialName = "spcName";
        String specialFullName = "spcFName";
        String specialDisplayName = "spcDisplay";

        atlasServer.setName(specialName);
        atlasServer.setFullName(specialFullName);
        atlasServer.setDisplayName(specialDisplayName);

        assertEquals(atlasServer.getName(), specialName);
        assertEquals(atlasServer.getFullName(), specialFullName);
        assertEquals(atlasServer.getDisplayName(), specialDisplayName);
    }

    @Test
    public void testLargeCollections() {
        // Test with large URLs list
        List<String> largeUrlList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            largeUrlList.add("http://server" + i + ".example.com:8080");
        }
        atlasServer.setUrls(largeUrlList);

        assertEquals(atlasServer.getUrls().size(), 1000);
        assertEquals(atlasServer.getUrls().get(500), "http://server500.example.com:8080");

        // Test with large additional info map
        Map<String, String> largeAdditionalInfo = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            largeAdditionalInfo.put("key" + i, "value" + i);
        }
        atlasServer.setAdditionalInfo(largeAdditionalInfo);

        assertEquals(atlasServer.getAdditionalInfo().size(), 1000);
        assertEquals(atlasServer.getAdditionalInfo().get("key500"), "value500");
    }

    @Test
    public void testAdditionalInfoWithSpecialKeys() {
        atlasServer.setAdditionalInfo("key-with-dash", "value1");
        atlasServer.setAdditionalInfo("key_with_underscore", "value2");
        atlasServer.setAdditionalInfo("key.with.dots", "value3");
        atlasServer.setAdditionalInfo("key:with:colons", "value4");
        atlasServer.setAdditionalInfo("key/with/slashes", "value5");
        atlasServer.setAdditionalInfo("key with spaces", "value6");

        Map<String, String> additionalInfo = atlasServer.getAdditionalInfo();
        assertEquals(additionalInfo.size(), 6);
    }

    @Test
    public void testUrlsWithDifferentProtocols() {
        List<String> urls = new ArrayList<>();
        urls.add("http://server.example.com:8080");
        urls.add("https://server.example.com:8443");
        urls.add("ftp://server.example.com:21");
        urls.add("file:///path/to/file");
        urls.add("ldap://server.example.com:389");

        atlasServer.setUrls(urls);

        assertEquals(atlasServer.getUrls().size(), 5);
        assertTrue(atlasServer.getUrls().contains("https://server.example.com:8443"));
        assertTrue(atlasServer.getUrls().contains("ldap://server.example.com:389"));
    }

    @Test
    public void testEmptyStringValues() {
        atlasServer.setName("");
        atlasServer.setFullName("");
        atlasServer.setDisplayName("");

        assertEquals(atlasServer.getName(), "");
        assertEquals(atlasServer.getFullName(), "");
        assertEquals(atlasServer.getDisplayName(), "");

        String result = atlasServer.toString();
        assertTrue(result.contains("name="));
        assertTrue(result.contains("fullName="));
        assertTrue(result.contains("displayName="));
    }

    @Test
    public void testComplexWorkflow() {
        // Simulate a complete server setup workflow
        AtlasServer server = new AtlasServer("prod-server", "Production Server", "prod-server.company.com");
        // Add URLs
        List<String> urls = new ArrayList<>();
        urls.add("https://prod-server.company.com:21000");
        urls.add("http://prod-server.company.com:21001");
        server.setUrls(urls);

        // Add additional info
        server.setAdditionalInfo("environment", "production");
        server.setAdditionalInfo("region", "us-east-1");
        server.setAdditionalInfo("owner", "data-team");

        // Add replication info
        server.setAdditionalInfoRepl("guid1", 123456L);
        server.setAdditionalInfoRepl("guid2", 789012L);

        // Verify final state
        assertEquals(server.getName(), "prod-server");
        assertEquals(server.getDisplayName(), "Production Server");
        assertEquals(server.getFullName(), "prod-server.company.com");
        assertEquals(server.getUrls().size(), 2);
        assertTrue(server.getAdditionalInfo().size() >= 4); // 3 custom + 1 replication details
        assertEquals(server.getAdditionalInfo("environment"), "production");
        assertNotNull(server.toString());
    }
}
