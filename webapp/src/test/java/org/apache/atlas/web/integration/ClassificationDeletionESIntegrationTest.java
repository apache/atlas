package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.web.integration.utils.ESUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.TestcontainersExtension;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test to verify that deleted classifications are properly removed
 * from Elasticsearch fields __traitNames and __classificationNames.
 * 
 * This test validates the fix for stale classification data in ES after deletion.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestcontainersExtension.class)
public class ClassificationDeletionESIntegrationTest extends AtlasDockerIntegrationTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(ClassificationDeletionESIntegrationTest.class);
    private static final int ES_SYNC_WAIT_MS = 3000; // Wait for ES to sync after operations
    
    @Test
    @Disabled
    @DisplayName("Test: Deleted classification should not appear in ES __traitNames and __classificationNames")
    void testDeletedClassificationNotInES() throws Exception {
        LOG.info("TEST: Verify deleted classification is removed from ES denormalized fields");
        
        // Step 1: Create classification type
        String classificationTypeName = createSimpleClassificationType("TestTag_ES_" + System.currentTimeMillis());
        LOG.info("Created classification type: {}", classificationTypeName);
        
        // Step 2: Create entity WITH classification
        String qualifiedName = "test_table_es_" + System.currentTimeMillis();
        String createPayload = buildCreateEntityWithTagPayload(qualifiedName, classificationTypeName);
        
        HttpResponse<String> createResponse = sendBulkEntityRequest(createPayload, "");
        assertEquals(200, createResponse.statusCode(), "Entity creation should succeed");
        
        ObjectNode createResult = mapper.readValue(createResponse.body(), ObjectNode.class);
        String entityGuid = createResult.get("mutatedEntities")
                .get("CREATE")
                .get(0)
                .get("guid")
                .asText();
        LOG.info("Created entity, GUID: {}", entityGuid);
        
        // Step 3: Wait for ES to sync and verify tag is present
        Thread.sleep(ES_SYNC_WAIT_MS);
        verifyClassificationInES(entityGuid, classificationTypeName, true);
        
        // Step 4: Detach classification using setClassifications API with empty array
        String detachPayload = buildDetachClassificationPayload(entityGuid, qualifiedName);
        
        HttpResponse<String> detachResponse = sendSetClassificationsRequest(detachPayload);
        assertTrue(detachResponse.statusCode() == 200 || detachResponse.statusCode() == 204, 
            "Classification detachment should succeed (200 or 204). Got: " + detachResponse.statusCode() + 
            ". Response: " + detachResponse.body());
        LOG.info("Detached classification from entity");
        
        // Step 5: Wait for ES to sync
        Thread.sleep(ES_SYNC_WAIT_MS);
        
        // Step 6: Verify entity no longer has classification via REST API
        HttpResponse<String> entityResponse = getEntityByGuid(entityGuid);
        assertEquals(200, entityResponse.statusCode(), "Should be able to fetch entity");
        
        ObjectNode entityResult = mapper.readValue(entityResponse.body(), ObjectNode.class);
        JsonNode classifications = entityResult.get("entity").get("classifications");
        assertTrue(classifications.isEmpty() || classifications.size() == 0, 
            "Entity should have no classifications after detachment");
        
        // Step 7: Verify classification is NOT in ES __traitNames and __classificationNames
        verifyClassificationInES(entityGuid, classificationTypeName, false);
        
        LOG.info("TEST PASSED: Deleted classification correctly removed from ES");
    }
    
    /**
     * Verifies whether a classification is present or absent in ES denormalized fields.
     */
    private void verifyClassificationInES(String entityGuid, String classificationTypeName, boolean shouldBePresent) throws Exception {
        SearchResponse searchResponse = ESUtil.searchWithGuid(entityGuid);
        assertNotNull(searchResponse, "ES search response should not be null");
        assertEquals(1, searchResponse.getHits().getTotalHits().value, 
            "Should find exactly one document in ES for entity GUID");
        
        SearchHit hit = searchResponse.getHits().getHits()[0];
        Map<String, Object> source = hit.getSourceAsMap();
        
        // Check __traitNames
        Object traitNamesObj = source.get("__traitNames");
        List<String> traitNames = null;
        if (traitNamesObj instanceof List) {
            traitNames = (List<String>) traitNamesObj;
        }
        
        // Check __classificationNames
        String classificationNames = (String) source.get("__classificationNames");
        
        if (shouldBePresent) {
            // Classification should be present
            assertNotNull(traitNames, "__traitNames should not be null");
            assertTrue(traitNames.contains(classificationTypeName), 
                String.format("__traitNames should contain classification '%s'. Found: %s", 
                    classificationTypeName, traitNames));
            
            assertNotNull(classificationNames, "__classificationNames should not be null");
            assertTrue(classificationNames.contains(classificationTypeName), 
                String.format("__classificationNames should contain classification '%s'. Found: %s", 
                    classificationTypeName, classificationNames));
            
            LOG.info("Verified classification '{}' is present in ES", classificationTypeName);
        } else {
            // Classification should NOT be present
            if (traitNames != null) {
                assertFalse(traitNames.contains(classificationTypeName), 
                    String.format("__traitNames should NOT contain deleted classification '%s'. Found: %s", 
                        classificationTypeName, traitNames));
            }
            
            if (classificationNames != null && !classificationNames.isEmpty()) {
                assertFalse(classificationNames.contains(classificationTypeName), 
                    String.format("__classificationNames should NOT contain deleted classification '%s'. Found: %s", 
                        classificationTypeName, classificationNames));
            }
            
            LOG.info("Verified classification '{}' is NOT present in ES (as expected)", classificationTypeName);
        }
    }
    
    /**
     * Creates a simple classification type with no attributes.
     */
    private String createSimpleClassificationType(String displayName) throws Exception {
        String auth = "admin:admin";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
        
        String typedefPayload = String.format("""
            {
                "classificationDefs": [
                    {
                        "attributeDefs": [],
                        "superTypes": [],
                        "displayName": "%s",
                        "description": "Test classification",
                        "options": {
                            "color": "Gray",
                            "iconName": "atlanTags",
                            "iconType": "icon"
                        }
                    }
                ]
            }""", displayName);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/types/typedefs"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + encodedAuth)
                .POST(HttpRequest.BodyPublishers.ofString(typedefPayload))
                .timeout(Duration.ofSeconds(10))
                .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Classification type creation should succeed");
        
        ObjectNode result = mapper.readValue(response.body(), ObjectNode.class);
        return result.get("classificationDefs").get(0).get("name").asText();
    }
    
    /**
     * Sends a bulk entity request.
     */
    private HttpResponse<String> sendBulkEntityRequest(String payload, String queryParams) throws Exception {
        String auth = "admin:admin";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
        
        String url = ATLAS_BASE_URL + "/entity/bulk";
        if (queryParams != null && !queryParams.isEmpty()) {
            url += "?" + queryParams;
        }
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + encodedAuth)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(10))
                .build();
        
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
    
    /**
     * Sends a setClassifications request to detach classifications.
     */
    private HttpResponse<String> sendSetClassificationsRequest(String payload) throws Exception {
        String auth = "admin:admin";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/entity/bulk/setClassifications"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + encodedAuth)
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .timeout(Duration.ofSeconds(10))
                .build();
        
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
    
    /**
     * Builds payload for detaching classifications (empty classifications array).
     */
    private String buildDetachClassificationPayload(String entityGuid, String qualifiedName) {
        // Extract name from qualifiedName (last part after /)
        String name = qualifiedName;
        if (qualifiedName.contains("/")) {
            name = qualifiedName.substring(qualifiedName.lastIndexOf("/") + 1);
        }
        
        return String.format("""
            {
                "guidHeaderMap": {
                    "%s": {
                        "guid": "%s",
                        "typeName": "Table",
                        "attributes": {
                            "name": "%s",
                            "qualifiedName": "%s"
                        },
                        "classifications": []
                    }
                }
            }""", entityGuid, entityGuid, name, qualifiedName);
    }
    
    /**
     * Gets entity by GUID.
     */
    private HttpResponse<String> getEntityByGuid(String entityGuid) throws Exception {
        String auth = "admin:admin";
        String encodedAuth = Base64.getEncoder().encodeToString(auth.getBytes());
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(ATLAS_BASE_URL + "/entity/guid/" + entityGuid))
                .header("Accept", "application/json;charset=UTF-8")
                .header("Authorization", "Basic " + encodedAuth)
                .GET()
                .timeout(Duration.ofSeconds(10))
                .build();
        
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    }
    
    /**
     * Builds payload for creating an entity with a classification.
     */
    private String buildCreateEntityWithTagPayload(String qualifiedName, String classificationTypeName) {
        return String.format("""
            {
                "entities": [
                    {
                        "typeName": "Table",
                        "guid": "-1",
                        "status": "ACTIVE",
                        "attributes": {
                            "qualifiedName": "%s",
                            "name": "%s"
                        },
                        "classifications": [
                            {
                                "typeName": "%s",
                                "propagate": false,
                                "removePropagationsOnEntityDelete": true,
                                "restrictPropagationThroughLineage": false,
                                "restrictPropagationThroughHierarchy": false
                            }
                        ]
                    }
                ]
            }""", qualifiedName, qualifiedName, classificationTypeName);
    }
    
}
