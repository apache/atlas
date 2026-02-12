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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Purpose Discovery API (POST /api/atlas/v2/purposes/user).
 *
 * Tests the ability to discover Purpose entities accessible to a user based on
 * their username and group memberships via AuthPolicy entities.
 *
 * <p>Note: Some tests require AuthPolicy entity creation which may not be available
 * in all test environments. Tests will be skipped via JUnit Assumptions when
 * prerequisites are not met.</p>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PurposeDiscoveryIntegrationTest extends AtlasInProcessBaseIT {
    private static final Logger LOG = LoggerFactory.getLogger(PurposeDiscoveryIntegrationTest.class);

    private static final String AUTH_HEADER = "Basic " + Base64.getEncoder().encodeToString("admin:admin".getBytes());

    private final ObjectMapper mapper = new ObjectMapper();
    private HttpClient httpClient;

    // Store created entity GUIDs for cleanup and verification
    private String purposeGuid;
    private String policyGuid;
    private boolean authPolicyCreationSupported = true;

    private String getApiBaseUrl() {
        return getAtlasBaseUrl() + "/api/atlas/v2";
    }

    private String getPurposeApiUrl() {
        return getAtlasBaseUrl() + "/api/atlas/v2/purposes/user";
    }

    @Test
    @Order(1)
    @DisplayName("Test Purpose Discovery API - Setup: Create Purpose Entity")
    void testSetupCreatePurpose() throws Exception {
        httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        LOG.info("Creating Purpose entity for test...");

        String uniqueId = UUID.randomUUID().toString().substring(0, 8);
        String purposeQualifiedName = "test-purpose-" + uniqueId;

        String payload = String.format("""
            {
                "entities": [
                    {
                        "typeName": "Purpose",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "Test Purpose %s",
                            "description": "Purpose created for integration testing",
                            "purposeClassifications": ["PII", "Confidential"]
                        }
                    }
                ]
            }
            """, purposeQualifiedName, uniqueId);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getApiBaseUrl() + "/entity/bulk"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", AUTH_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Create Purpose response: {} - {}", response.statusCode(), response.body());
        assertEquals(200, response.statusCode(), "Failed to create Purpose entity");

        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        assertNotNull(result.get("mutatedEntities"), "Response should contain mutatedEntities");

        // Extract the created Purpose GUID
        JsonNode createNode = result.get("mutatedEntities").get("CREATE");
        assertNotNull(createNode, "Should have CREATE in mutatedEntities");
        assertTrue(createNode.isArray() && createNode.size() > 0, "CREATE should have at least one entity");

        purposeGuid = createNode.get(0).get("guid").asText();
        LOG.info("Created Purpose with GUID: {}", purposeGuid);
        assertNotNull(purposeGuid, "Purpose GUID should not be null");

        // Wait for ES indexing
        Thread.sleep(2000);
    }

    @Test
    @Order(2)
    @DisplayName("Test Purpose Discovery API - Setup: Create AuthPolicy linking to Purpose")
    void testSetupCreateAuthPolicy() throws Exception {
        Assumptions.assumeTrue(purposeGuid != null, "Skipping: Purpose creation failed");

        LOG.info("Creating AuthPolicy with policyCategory=purpose...");

        String uniqueId = UUID.randomUUID().toString().substring(0, 8);

        // Create AuthPolicy with policyCategory="purpose" and link to the Purpose
        // Note: Using simpler payload without qualifiedName - let the system generate it
        String payload = String.format("""
            {
                "entities": [
                    {
                        "typeName": "AuthPolicy",
                        "attributes": {
                            "name": "Test Policy %s",
                            "policyCategory": "purpose",
                            "policyType": "allow",
                            "policyUsers": ["admin", "testuser"],
                            "policyGroups": ["testgroup", "developers"],
                            "policyActions": ["entity-read"],
                            "accessControl": {
                                "guid": "%s",
                                "typeName": "Purpose"
                            }
                        }
                    }
                ]
            }
            """, uniqueId, purposeGuid);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getApiBaseUrl() + "/entity/bulk"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", AUTH_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Create AuthPolicy response: {} - {}", response.statusCode(), response.body());

        if (response.statusCode() != 200) {
            LOG.warn("AuthPolicy creation failed with status {}. Some tests will be skipped.", response.statusCode());
            authPolicyCreationSupported = false;
            return;
        }

        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        JsonNode mutatedEntities = result.get("mutatedEntities");
        if (mutatedEntities != null) {
            JsonNode createNode = mutatedEntities.get("CREATE");
            if (createNode != null && createNode.size() > 0) {
                policyGuid = createNode.get(0).get("guid").asText();
                LOG.info("Created AuthPolicy with GUID: {}", policyGuid);
            }
        }

        // Wait for ES indexing
        Thread.sleep(2000);
    }

    @Test
    @Order(3)
    @DisplayName("Test Purpose Discovery API - Basic API functionality")
    void testDiscoverPurposesBasicFunctionality() throws Exception {
        LOG.info("Testing Purpose Discovery API basic functionality...");

        String payload = """
            {
                "username": "admin",
                "groups": [],
                "limit": 100,
                "offset": 0
            }
            """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getPurposeApiUrl()))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", AUTH_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Purpose Discovery response: {} - {}", response.statusCode(), response.body());
        assertEquals(200, response.statusCode(), "Purpose Discovery API should return 200");

        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);

        // Verify response structure
        assertTrue(result.has("purposes"), "Response should have 'purposes' field");
        assertTrue(result.has("totalCount"), "Response should have 'totalCount' field");
        assertTrue(result.has("count"), "Response should have 'count' field");
        assertTrue(result.has("hasMore"), "Response should have 'hasMore' field");

        ArrayNode purposes = (ArrayNode) result.get("purposes");
        assertNotNull(purposes, "Purposes array should not be null");

        LOG.info("API returned {} purposes", purposes.size());
    }

    @Test
    @Order(4)
    @DisplayName("Test Purpose Discovery API - Discover purposes by username (requires AuthPolicy)")
    void testDiscoverPurposesByUsername() throws Exception {
        Assumptions.assumeTrue(authPolicyCreationSupported, "Skipping: AuthPolicy creation not supported");
        Assumptions.assumeTrue(policyGuid != null, "Skipping: AuthPolicy not created");

        LOG.info("Testing Purpose Discovery API with username filter...");

        String payload = """
            {
                "username": "admin",
                "groups": [],
                "limit": 100,
                "offset": 0
            }
            """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getPurposeApiUrl()))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", AUTH_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Purpose Discovery response: {} - {}", response.statusCode(), response.body());
        assertEquals(200, response.statusCode(), "Purpose Discovery API should return 200");

        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        ArrayNode purposes = (ArrayNode) result.get("purposes");

        boolean foundPurpose = false;
        for (JsonNode purpose : purposes) {
            if (purposeGuid.equals(purpose.get("guid").asText())) {
                foundPurpose = true;
                LOG.info("Found our created Purpose in the response: {}", purpose);
                break;
            }
        }

        assertTrue(foundPurpose, "Created Purpose should be discoverable by username 'admin'");
    }

    @Test
    @Order(5)
    @DisplayName("Test Purpose Discovery API - Authorization check rejects queries for other users")
    void testDiscoverPurposesAuthorizationCheck() throws Exception {
        LOG.info("Testing Purpose Discovery API authorization check...");

        // Try to query purposes for a different user while logged in as admin
        // This should be rejected with 403 Unauthorized
        String payload = """
            {
                "username": "otheruser",
                "groups": ["somegroup"],
                "limit": 100,
                "offset": 0
            }
            """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getPurposeApiUrl()))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", AUTH_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Purpose Discovery (authorization) response: {} - {}", response.statusCode(), response.body());

        // Should return 403 Forbidden - users can only query their own purposes
        assertEquals(403, response.statusCode(),
            "Should return 403 when querying purposes for a different user");
    }

    @Test
    @Order(6)
    @DisplayName("Test Purpose Discovery API - Pagination")
    void testDiscoverPurposesPagination() throws Exception {
        LOG.info("Testing Purpose Discovery API pagination...");

        // Request with limit=1
        String payload = """
            {
                "username": "admin",
                "groups": [],
                "limit": 1,
                "offset": 0
            }
            """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getPurposeApiUrl()))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", AUTH_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Purpose Discovery (pagination) response: {} - {}", response.statusCode(), response.body());
        assertEquals(200, response.statusCode());

        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);

        // Verify pagination fields are present
        assertTrue(result.has("hasMore"), "Response should have 'hasMore' field");
        assertTrue(result.has("count"), "Response should have 'count' field");
        assertTrue(result.has("totalCount"), "Response should have 'totalCount' field");

        ArrayNode purposes = (ArrayNode) result.get("purposes");
        assertTrue(purposes.size() <= 1, "Should return at most 1 purpose with limit=1");
    }

    @Test
    @Order(7)
    @DisplayName("Test Purpose Discovery API - Bad Request validation")
    void testDiscoverPurposesBadRequest() throws Exception {
        LOG.info("Testing Purpose Discovery API validation...");

        // Missing required username field
        String payload = """
            {
                "groups": ["testgroup"],
                "limit": 100
            }
            """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getPurposeApiUrl()))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", AUTH_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Purpose Discovery (bad request) response: {} - {}", response.statusCode(), response.body());

        // Should return 400 Bad Request for missing username
        assertEquals(400, response.statusCode(),
            "Should return 400 for missing required 'username' field");
    }

    @Test
    @Order(8)
    @DisplayName("Test Purpose Discovery API - Custom attributes selection")
    void testDiscoverPurposesWithCustomAttributes() throws Exception {
        LOG.info("Testing Purpose Discovery API with custom attributes...");

        String payload = """
            {
                "username": "admin",
                "groups": [],
                "limit": 100,
                "offset": 0,
                "attributes": ["name", "qualifiedName", "description"]
            }
            """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(getPurposeApiUrl()))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", AUTH_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Purpose Discovery (custom attrs) response: {} - {}", response.statusCode(), response.body());
        assertEquals(200, response.statusCode());

        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        ArrayNode purposes = (ArrayNode) result.get("purposes");

        // Just verify the API accepts the attributes parameter and returns valid response
        assertNotNull(purposes, "Purposes array should not be null");
    }
}
