package org.apache.atlas.repository.graphdb.migrator;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Re-indexes all migrated vertices into Elasticsearch.
 *
 * Uses the ES low-level REST client (no Lucene dependency) to avoid
 * version conflicts with JanusGraph's bundled Lucene in the fat jar.
 *
 * Reads from the target Cassandra vertices table and bulk-indexes into ES
 * using vertex_id as the ES document ID.
 */
public class ElasticsearchReindexer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchReindexer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MigratorConfig   config;
    private final MigrationMetrics metrics;
    private final CqlSession       targetSession;
    private final RestClient       esClient;

    public ElasticsearchReindexer(MigratorConfig config, MigrationMetrics metrics, CqlSession targetSession) {
        this.config        = config;
        this.metrics       = metrics;
        this.targetSession = targetSession;
        this.esClient      = createEsClient();
    }

    private RestClient createEsClient() {
        RestClientBuilder builder = RestClient.builder(
            new HttpHost(config.getTargetEsHostname(), config.getTargetEsPort(), config.getTargetEsProtocol()));

        String username = config.getTargetEsUsername();
        String password = config.getTargetEsPassword();
        if (username != null && !username.isEmpty()) {
            BasicCredentialsProvider creds = new BasicCredentialsProvider();
            creds.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            builder.setHttpClientConfigCallback(b -> b.setDefaultCredentialsProvider(creds));
        }

        // Increase timeouts for bulk operations
        builder.setRequestConfigCallback(b -> b
            .setConnectTimeout(30_000)
            .setSocketTimeout(120_000));

        return builder.build();
    }

    /**
     * Re-index all vertices from the target Cassandra vertices table into Elasticsearch.
     */
    public void reindexAll() throws IOException {
        String ks = config.getTargetCassandraKeyspace();
        String esIndex = config.getTargetEsIndex();
        int bulkSize = config.getEsBulkSize();

        LOG.info("Starting ES re-indexing from {}.vertices to ES index '{}'", ks, esIndex);

        // Ensure the index exists
        ensureIndexExists(esIndex);

        ResultSet rs = targetSession.execute(
            "SELECT vertex_id, properties, type_name, state FROM " + ks + ".vertices");

        StringBuilder bulkBody = new StringBuilder();
        int batchCount = 0;
        long totalDocs = 0;

        long skipped = 0;

        for (Row row : rs) {
            String vertexId = row.getString("vertex_id");
            String propsJson = row.getString("properties");
            String typeName = row.getString("type_name");
            String state = row.getString("state");

            if (propsJson == null || propsJson.equals("{}")) {
                continue;
            }

            // Skip non-entity vertices: system vertices (patches, index recovery, etc.)
            // that lack both __typeName and __type should NOT be indexed in ES.
            // They pollute search results — the UI can't render them as assets.
            if (typeName == null || typeName.isEmpty()) {
                // Check if it's a type definition vertex (__type = "typeSystem")
                try {
                    Map<String, Object> checkProps = MAPPER.readValue(propsJson, Map.class);
                    Object vertexType = checkProps.get("__type");
                    if (vertexType == null) {
                        skipped++;
                        continue;
                    }
                } catch (Exception e) {
                    skipped++;
                    continue;
                }
            }

            // Build ES document: merge properties with top-level vertex metadata
            try {
                Map<String, Object> rawDoc = MAPPER.readValue(propsJson, Map.class);

                // Sanitize field names: ES 7.x interprets dots in JSON keys as
                // nested object paths. TypeDef vertices have properties like
                // "__type.DbtTest.dbtTestStatus" alongside "__type": "typeSystem",
                // which causes a mapping conflict (text vs object).
                // Replace dots with underscores in field names to avoid this.
                Map<String, Object> doc = new java.util.LinkedHashMap<>(rawDoc.size());
                for (Map.Entry<String, Object> entry : rawDoc.entrySet()) {
                    String key = entry.getKey();
                    if (key.contains(".")) {
                        key = key.replace('.', '_');
                    }
                    doc.put(key, entry.getValue());
                }

                // Ensure key fields are present at top level for ES queries
                if (typeName != null) doc.putIfAbsent("__typeName", typeName);
                if (state != null)    doc.putIfAbsent("__state", state);

                String docJson = MAPPER.writeValueAsString(doc);

                // NDJSON bulk format: action line + doc line
                bulkBody.append("{\"index\":{\"_index\":\"").append(esIndex)
                        .append("\",\"_id\":\"").append(escapeJson(vertexId)).append("\"}}\n");
                bulkBody.append(docJson).append("\n");

                batchCount++;
                totalDocs++;

                if (batchCount >= bulkSize) {
                    executeBulk(bulkBody.toString(), batchCount, totalDocs);
                    bulkBody.setLength(0);
                    batchCount = 0;
                }
            } catch (Exception e) {
                LOG.warn("Failed to index vertex {} to ES", vertexId, e);
            }
        }

        // Flush remaining
        if (batchCount > 0) {
            executeBulk(bulkBody.toString(), batchCount, totalDocs);
        }

        LOG.info("ES re-indexing complete: {} documents indexed, {} non-entity vertices skipped",
                 String.format("%,d", totalDocs), String.format("%,d", skipped));
    }

    private void ensureIndexExists(String esIndex) {
        try {
            Response response = esClient.performRequest(new Request("HEAD", "/" + esIndex));
            if (response.getStatusLine().getStatusCode() == 200) {
                LOG.info("ES index '{}' already exists", esIndex);
                return;
            }
        } catch (IOException e) {
            // Index doesn't exist, create it below
        }

        // Try to copy mappings + settings from the source JanusGraph index (migrated tenant).
        // If the source index doesn't exist (fresh tenant), fall back to empty body
        // so the ES index template (atlan-template) applies its defaults.
        String sourceIndex = config.getSourceEsIndex();
        String createBody = getCreateBodyFromSourceIndex(sourceIndex);

        try {
            Request createReq = new Request("PUT", "/" + esIndex);
            createReq.setJsonEntity(createBody);
            esClient.performRequest(createReq);

            if ("{}".equals(createBody)) {
                LOG.info("Created ES index '{}' (fresh tenant — settings from ES index template)", esIndex);
            } else {
                LOG.info("Created ES index '{}' with mappings+settings copied from source index '{}'",
                         esIndex, sourceIndex);
            }
        } catch (IOException e) {
            LOG.warn("Failed to create ES index '{}' (may already exist): {}", esIndex, e.getMessage());
        }
    }

    /**
     * Default mappings applied when the source index doesn't exist (e.g., deleted during
     * remigration cleanup). Maps all strings to keyword to match the Atlas ES schema
     * (addons/elasticsearch/es-mappings.json). Without this, ES dynamic mapping creates
     * text fields which break sort/aggregation queries on __guid, __typeName, etc.
     */
    private static final String DEFAULT_MAPPINGS_JSON =
        "{\"properties\":{\"relationshipList\":{\"type\":\"nested\",\"properties\":" +
        "{\"typeName\":{\"type\":\"keyword\"},\"guid\":{\"type\":\"keyword\"}," +
        "\"provenanceType\":{\"type\":\"integer\"},\"endName\":{\"type\":\"keyword\"}," +
        "\"endGuid\":{\"type\":\"keyword\"},\"endTypeName\":{\"type\":\"keyword\"}," +
        "\"endQualifiedName\":{\"type\":\"keyword\"},\"label\":{\"type\":\"keyword\"}," +
        "\"propagateTags\":{\"type\":\"keyword\"},\"status\":{\"type\":\"keyword\"}," +
        "\"createdBy\":{\"type\":\"keyword\"},\"updatedBy\":{\"type\":\"keyword\"}," +
        "\"createTime\":{\"type\":\"long\"},\"updateTime\":{\"type\":\"long\"}," +
        "\"version\":{\"type\":\"long\"}}}}," +
        "\"dynamic_templates\":[{\"custom_metadata_strings\":" +
        "{\"match_mapping_type\":\"string\",\"mapping\":{\"type\":\"keyword\",\"ignore_above\":5120}}}]}";

    /**
     * Reads mappings and settings from the source JanusGraph ES index and returns
     * a JSON body suitable for PUT /{index} to create the target index with
     * identical field mappings and analyzer settings.
     *
     * Falls back to default Atlas mappings (keyword for strings) if the source
     * index doesn't exist (e.g., deleted during remigration cleanup).
     */
    private String getCreateBodyFromSourceIndex(String sourceIndex) {
        if (sourceIndex == null || sourceIndex.isEmpty()) {
            LOG.info("No source ES index configured, using default Atlas mappings");
            return "{\"mappings\":" + DEFAULT_MAPPINGS_JSON + "}";
        }

        try {
            // Check if source index exists
            Response headResp = esClient.performRequest(new Request("HEAD", "/" + sourceIndex));
            if (headResp.getStatusLine().getStatusCode() != 200) {
                LOG.info("Source ES index '{}' does not exist, using default Atlas mappings", sourceIndex);
                return "{\"mappings\":" + DEFAULT_MAPPINGS_JSON + "}";
            }
        } catch (IOException e) {
            LOG.info("Source ES index '{}' not found, using default Atlas mappings", sourceIndex);
            return "{\"mappings\":" + DEFAULT_MAPPINGS_JSON + "}";
        }

        try {
            // Read mappings from source index
            String mappingsJson = "{}";
            Response mappingResp = esClient.performRequest(new Request("GET", "/" + sourceIndex + "/_mapping"));
            if (mappingResp.getStatusLine().getStatusCode() == 200) {
                String body = EntityUtils.toString(mappingResp.getEntity());
                Map<String, Object> parsed = MAPPER.readValue(body, Map.class);
                // Response: { "indexName": { "mappings": { ... } } }
                Map<String, Object> indexData = (Map<String, Object>) parsed.values().iterator().next();
                Object mappings = indexData.get("mappings");
                if (mappings != null) {
                    mappingsJson = MAPPER.writeValueAsString(mappings);
                }
            }

            // Read settings from source index (only the user-defined settings, not ES internals)
            String settingsJson = null;
            Response settingsResp = esClient.performRequest(new Request("GET", "/" + sourceIndex + "/_settings"));
            if (settingsResp.getStatusLine().getStatusCode() == 200) {
                String body = EntityUtils.toString(settingsResp.getEntity());
                Map<String, Object> parsed = MAPPER.readValue(body, Map.class);
                // Response: { "indexName": { "settings": { "index": { ... } } } }
                Map<String, Object> indexData = (Map<String, Object>) parsed.values().iterator().next();
                Map<String, Object> settings = (Map<String, Object>) indexData.get("settings");
                if (settings != null) {
                    Map<String, Object> indexSettings = (Map<String, Object>) settings.get("index");
                    if (indexSettings != null) {
                        // Remove read-only / auto-generated settings that can't be set on creation
                        indexSettings.remove("creation_date");
                        indexSettings.remove("provided_name");
                        indexSettings.remove("uuid");
                        indexSettings.remove("version");
                        indexSettings.remove("routing");
                        indexSettings.remove("history");
                        settingsJson = MAPPER.writeValueAsString(Map.of("index", indexSettings));
                    }
                }
            }

            // Build the create index body with both mappings and settings
            StringBuilder createBody = new StringBuilder("{");
            if (settingsJson != null) {
                createBody.append("\"settings\":").append(settingsJson);
            }
            if (!"{}".equals(mappingsJson)) {
                if (settingsJson != null) createBody.append(",");
                createBody.append("\"mappings\":").append(mappingsJson);
            }
            createBody.append("}");

            LOG.info("Copied mappings+settings from source index '{}' for target index creation", sourceIndex);
            return createBody.toString();

        } catch (Exception e) {
            LOG.warn("Failed to read mappings/settings from source index '{}', falling back to default Atlas mappings: {}",
                     sourceIndex, e.getMessage());
            return "{\"mappings\":" + DEFAULT_MAPPINGS_JSON + "}";
        }
    }

    private void executeBulk(String bulkBody, int docCount, long totalSoFar) throws IOException {
        long bulkStart = System.currentTimeMillis();

        Request request = new Request("POST", "/_bulk");
        request.setJsonEntity(bulkBody);
        Response response = esClient.performRequest(request);
        long bulkMs = System.currentTimeMillis() - bulkStart;

        metrics.incrEsDocsIndexed(docCount);

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= 300) {
            String body = EntityUtils.toString(response.getEntity());
            LOG.warn("ES bulk response status {}: {}", statusCode, body.substring(0, Math.min(500, body.length())));
        } else {
            // Check for per-item errors in the response
            String body = EntityUtils.toString(response.getEntity());
            if (body.contains("\"errors\":true")) {
                LOG.warn("ES bulk had item-level errors (total: {}): {}...",
                         String.format("%,d", totalSoFar),
                         body.substring(0, Math.min(500, body.length())));
            }
        }

        LOG.info("ES bulk indexed {} docs in {}ms (total: {}, rate: {}/s)",
                 docCount, bulkMs, String.format("%,d", totalSoFar),
                 bulkMs > 0 ? (docCount * 1000L / bulkMs) : "N/A");
    }

    /** Escape special JSON characters in a string value */
    private static String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @Override
    public void close() {
        try {
            esClient.close();
        } catch (IOException e) {
            LOG.warn("Error closing ES client", e);
        }
    }
}
