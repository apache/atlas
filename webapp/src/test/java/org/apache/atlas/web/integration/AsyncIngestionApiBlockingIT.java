package org.apache.atlas.web.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for blocking admin and repair APIs when async ingestion mode is enabled.
 *
 * Verifies that when ENABLE_ASYNC_INGESTION is enabled, admin/repair write endpoints return 503
 * with the ASYNC_INGESTION_MODE_ENABLED error code, while GET requests and normal CRUD APIs
 * remain unaffected.
 *
 * Extends AtlasInProcessBaseIT which provides:
 * - Testcontainers (Cassandra, ES, Redis, Kafka)
 * - In-process Atlas server
 * - AtlasClientV2 for REST API calls
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AsyncIngestionApiBlockingIT extends AtlasInProcessBaseIT {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncIngestionApiBlockingIT.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ASYNC_INGESTION_ERROR_CODE = "ATLAS-503-00-004";

    // =================== Helper classes ===================

    private static class HttpResult {
        final int statusCode;
        final String body;

        HttpResult(int statusCode, String body) {
            this.statusCode = statusCode;
            this.body = body;
        }
    }

    // =================== Config Helpers ===================

    private void setAsyncIngestionEnabled(boolean enabled) throws Exception {
        String configUrl = getAtlasBaseUrl() + "/api/atlas/configs/ENABLE_ASYNC_INGESTION";

        URL url = new URL(configUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("PUT");
        conn.setRequestProperty("Authorization", "Basic " +
                Base64.getEncoder().encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8)));
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        String body = "{\"value\":\"" + enabled + "\"}";
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        int status = conn.getResponseCode();
        LOG.info("Set ENABLE_ASYNC_INGESTION={} - HTTP status: {}", enabled, status);
        assertEquals(200, status, "Failed to set ENABLE_ASYNC_INGESTION flag");
        conn.disconnect();

        // Wait for config propagation
        Thread.sleep(500);
    }

    // =================== HTTP Helpers ===================

    private HttpResult makeRequest(String method, String path, String body) throws IOException {
        String fullUrl = getAtlasBaseUrl() + "/api/atlas" + path;
        URL url = new URL(fullUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setRequestProperty("Authorization", "Basic " +
                Base64.getEncoder().encodeToString("admin:admin".getBytes(StandardCharsets.UTF_8)));
        conn.setRequestProperty("Content-Type", "application/json");

        if (body != null && ("POST".equals(method) || "PUT".equals(method) || "DELETE".equals(method))) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
        }

        int statusCode = conn.getResponseCode();
        String responseBody;
        try {
            InputStream is = (statusCode >= 400) ? conn.getErrorStream() : conn.getInputStream();
            responseBody = (is != null) ? new String(is.readAllBytes(), StandardCharsets.UTF_8) : "";
        } catch (Exception e) {
            responseBody = "";
        }

        conn.disconnect();
        LOG.info("{} {} -> HTTP {}", method, path, statusCode);
        return new HttpResult(statusCode, responseBody);
    }

    // =================== Assertion Helpers ===================

    private void assertBlockedByAsyncIngestion(HttpResult result) throws Exception {
        assertEquals(503, result.statusCode, "Expected 503 for blocked endpoint");
        JsonNode json = MAPPER.readTree(result.body);
        assertEquals(ASYNC_INGESTION_ERROR_CODE, json.get("errorCode").asText(),
                "Expected ASYNC_INGESTION_MODE_ENABLED error code");
        assertTrue(json.get("errorMessage").asText().toLowerCase().contains("async ingestion"),
                "Error message should mention async ingestion");
    }

    private void assertNotBlockedByAsyncIngestion(HttpResult result) {
        assertNotEquals(503, result.statusCode,
                "Should NOT be blocked (503) - got: " + result.statusCode);
    }

    // =================== Tests ===================

    @Test
    @Order(1)
    void testAdminApisWorkWhenAsyncIngestionDisabled() throws Exception {
        // Ensure async ingestion is off
        setAsyncIngestionEnabled(false);

        HttpResult result = makeRequest("POST", "/admin/checkstate", "{}");
        assertNotBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(2)
    void testEnableAsyncIngestion() throws Exception {
        setAsyncIngestionEnabled(true);
        // If we get here without assertion failure, flag was set successfully
    }

    @Test
    @Order(3)
    void testAdminPurgeBlocked() throws Exception {
        HttpResult result = makeRequest("PUT", "/admin/purge", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(4)
    void testAdminCheckstateBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/admin/checkstate", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(5)
    void testAdminFeatureFlagBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/admin/featureFlag", "{\"name\":\"TEST_FLAG\",\"value\":\"false\"}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(6)
    void testAdminTasksDeleteBlocked() throws Exception {
        HttpResult result = makeRequest("DELETE", "/admin/tasks?guids=fake-guid", null);
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(7)
    void testRepairSingleIndexBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/v2/repair/single-index", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(8)
    void testEntityRepairIndexBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/v2/entity/repairindex", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(9)
    void testEntityRepairClassificationsBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/v2/entity/bulk/repairClassificationsMappings", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(10)
    void testEntityRepairHasLineageBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/v2/entity/repairhaslineage", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(11)
    void testMigrationRepairBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/v2/migration/repair-unique-qualified-name", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(12)
    void testGetAdminStatusAllowed() throws Exception {
        HttpResult result = makeRequest("GET", "/admin/status", null);
        assertNotBlockedByAsyncIngestion(result);
        assertEquals(200, result.statusCode);
    }

    @Test
    @Order(13)
    void testGetAdminHealthAllowed() throws Exception {
        HttpResult result = makeRequest("GET", "/admin/health", null);
        assertNotBlockedByAsyncIngestion(result);
        assertEquals(200, result.statusCode);
    }

    @Test
    @Order(14)
    void testConfigApiAllowed() throws Exception {
        // Config APIs should not be blocked (they're used to manage async ingestion itself)
        HttpResult result = makeRequest("PUT", "/admin/config?key=SOME_TEST_KEY&value=test", "{}");
        assertNotBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(15)
    void testNormalEntityCreateAllowed() throws Exception {
        // Normal entity CRUD should not be blocked
        String entityJson = "{\"entity\":{\"typeName\":\"Table\",\"attributes\":{" +
                "\"name\":\"async-block-test-table\"," +
                "\"qualifiedName\":\"test://async-block-test/" + System.currentTimeMillis() + "\"" +
                "}}}";
        HttpResult result = makeRequest("POST", "/v2/entity", entityJson);
        assertNotBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(16)
    void testDisableAsyncIngestion() throws Exception {
        setAsyncIngestionEnabled(false);
        // If we get here without assertion failure, flag was cleared successfully
    }

    @Test
    @Order(17)
    void testAdminApisWorkAfterDisabling() throws Exception {
        HttpResult result = makeRequest("POST", "/admin/checkstate", "{}");
        assertNotBlockedByAsyncIngestion(result);
    }

    // =================== Additional blocked-endpoint tests (async ingestion re-enabled) ===================

    @Test
    @Order(18)
    void testReEnableAsyncIngestionForAdditionalTests() throws Exception {
        setAsyncIngestionEnabled(true);
    }

    @Test
    @Order(19)
    void testAdminRepairMeaningsBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/admin/repairmeanings", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(20)
    void testAdminActiveSearchesDeleteBlocked() throws Exception {
        HttpResult result = makeRequest("DELETE", "/admin/activeSearches/fake-id", null);
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(21)
    void testRepairCompositeIndexBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/v2/repair/composite-index", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(22)
    void testRepairBatchBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/v2/repair/batch", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(23)
    void testMigrationRepairStakeholderQualifiedNameBlocked() throws Exception {
        HttpResult result = makeRequest("POST", "/v2/migration/repair-stakeholder-qualified-name", "{}");
        assertBlockedByAsyncIngestion(result);
    }

    @Test
    @Order(24)
    void testDisableAsyncIngestionAfterAdditionalTests() throws Exception {
        setAsyncIngestionEnabled(false);
    }
}
