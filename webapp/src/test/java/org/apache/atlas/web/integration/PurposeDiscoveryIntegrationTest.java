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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.TestcontainersExtension;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the Purpose Discovery API (POST /api/meta/purposes/user).
 *
 * Tests the ability to discover Purpose entities accessible to a user based on
 * their username and group memberships via AuthPolicy entities.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestcontainersExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class PurposeDiscoveryIntegrationTest extends AtlasDockerIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(PurposeDiscoveryIntegrationTest.class);

    private static final String AUTH_HEADER = "Basic " + Base64.getEncoder().encodeToString("admin:admin".getBytes());

    // Store created entity GUIDs for cleanup and verification
    private String purposeGuid;
    private String policyGuid;
    private String purposeQualifiedName;

    @Test
    @Order(1)
    @DisplayName("Test Purpose Discovery API - Setup: Create Purpose Entity")
    void testSetupCreatePurpose() throws Exception {
        LOG.info("Creating Purpose entity for test...");

        String uniqueId = UUID.randomUUID().toString().substring(0, 8);
        purposeQualifiedName = "test-purpose-" + uniqueId;

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
                .uri(URI.create(ATLAS_BASE_URL + "/entity/bulk"))
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
        LOG.info("Creating AuthPolicy with policyCategory=purpose...");

        assertNotNull(purposeGuid, "Purpose must be created first");

        String uniqueId = UUID.randomUUID().toString().substring(0, 8);
        String policyQualifiedName = purposeQualifiedName + "/policy-" + uniqueId;

        // Create AuthPolicy with policyCategory="purpose" and link to the Purpose
        String payload = String.format("""
            {
                "entities": [
                    {
                        "typeName": "AuthPolicy",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "Test Policy %s",
                            "policyCategory": "purpose",
                            "policyType": "datapolicy",
                            "policyServiceName": "atlas",
                            "policySubCategory": "metadata",
                            "policyUsers": ["admin", "testuser"],
                            "policyGroups": ["testgroup", "developers"],
                            "policyActions": ["entity-read"],
                            "policyResources": ["entity:*"],
                            "policyResourceCategory": "ENTITY",
                            "accessControl": {
                                "guid": "%s",
                                "typeName": "Purpose"
                            }
                        }
                    }
                ]
            }
            """, policyQualifiedName, uniqueId, purposeGuid);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/entity/bulk"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", AUTH_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Create AuthPolicy response: {} - {}", response.statusCode(), response.body());
        assertEquals(200, response.statusCode(), "Failed to create AuthPolicy entity");

        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        JsonNode createNode = result.get("mutatedEntities").get("CREATE");

        if (createNode != null && createNode.size() > 0) {
            policyGuid = createNode.get(0).get("guid").asText();
            LOG.info("Created AuthPolicy with GUID: {}", policyGuid);
        }

        // Wait for ES indexing
        Thread.sleep(2000);
    }

    @Test
    @Order(3)
    @DisplayName("Test Purpose Discovery API - Discover purposes by username")
    void testDiscoverPurposesByUsername() throws Exception {
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
                .uri(URI.create(ATLAS_BASE_URL.replace("/api/atlas/v2", "") + "/api/meta/purposes/user"))
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

        // Check that our created Purpose is in the response
        ArrayNode purposes = (ArrayNode) result.get("purposes");
        assertNotNull(purposes, "Purposes array should not be null");

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
    @Order(4)
    @DisplayName("Test Purpose Discovery API - Discover purposes by group membership")
    void testDiscoverPurposesByGroup() throws Exception {
        LOG.info("Testing Purpose Discovery API with group filter...");

        String payload = """
            {
                "username": "otheruser",
                "groups": ["testgroup"],
                "limit": 100,
                "offset": 0
            }
            """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL.replace("/api/atlas/v2", "") + "/api/meta/purposes/user"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header("Authorization", AUTH_HEADER)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        LOG.info("Purpose Discovery (by group) response: {} - {}", response.statusCode(), response.body());
        assertEquals(200, response.statusCode(), "Purpose Discovery API should return 200");

        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        ArrayNode purposes = (ArrayNode) result.get("purposes");

        boolean foundPurpose = false;
        for (JsonNode purpose : purposes) {
            if (purposeGuid.equals(purpose.get("guid").asText())) {
                foundPurpose = true;
                break;
            }
        }

        assertTrue(foundPurpose, "Created Purpose should be discoverable by group 'testgroup'");
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
                .uri(URI.create(ATLAS_BASE_URL.replace("/api/atlas/v2", "") + "/api/meta/purposes/user"))
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
                "groups": ["testgroup"],
                "limit": 1,
                "offset": 0
            }
            """;

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL.replace("/api/atlas/v2", "") + "/api/meta/purposes/user"))
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
        assertTrue(result.has("limit"), "Response should have 'limit' field");
        assertTrue(result.has("offset"), "Response should have 'offset' field");
        assertEquals(1, result.get("limit").asInt(), "Limit should be 1");
        assertEquals(0, result.get("offset").asInt(), "Offset should be 0");

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
                .uri(URI.create(ATLAS_BASE_URL.replace("/api/atlas/v2", "") + "/api/meta/purposes/user"))
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
                .uri(URI.create(ATLAS_BASE_URL.replace("/api/atlas/v2", "") + "/api/meta/purposes/user"))
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

        if (purposes.size() > 0) {
            JsonNode firstPurpose = purposes.get(0);
            JsonNode attributes = firstPurpose.get("attributes");

            // Verify the requested attributes are present
            if (attributes != null) {
                LOG.info("Purpose attributes: {}", attributes);
                // At minimum, name and qualifiedName should be present
                assertTrue(attributes.has("name") || attributes.has("qualifiedName"),
                    "Response should include requested attributes");
            }
        }
    }
}
