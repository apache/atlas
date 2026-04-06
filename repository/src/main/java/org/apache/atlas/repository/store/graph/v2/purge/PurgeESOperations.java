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
package org.apache.atlas.repository.store.graph.v2.purge;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase;
import org.apache.atlas.repository.store.graph.v2.purge.BulkPurgeModel.BatchWork;
import org.apache.atlas.repository.store.graph.v2.purge.BulkPurgeModel.PurgeContext;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import static org.apache.atlas.repository.Constants.*;
import static org.apache.atlas.repository.store.graph.v2.purge.BulkPurgeModel.MAPPER;

/**
 * All Elasticsearch I/O for the BulkPurge subsystem: PIT-based pagination,
 * reconciliation, query builders, ES doc deletion, and ES-based entity lookups.
 *
 * <p>Uses Point-in-Time (PIT) + {@code search_after} instead of the deprecated
 * scroll API. PIT is stateless on the client and more memory-efficient on ES.
 */
public class PurgeESOperations {
    private static final Logger LOG = LoggerFactory.getLogger(PurgeESOperations.class);

    private static final int    ES_PAGE_SIZE           = 5000;
    private static final int    PIT_KEEP_ALIVE_MINUTES = 30;
    private static final String PIT_KEEP_ALIVE         = PIT_KEEP_ALIVE_MINUTES + "m";

    private final AtlasGraph graph;
    private final long esSettleWaitMs;

    // Lazily initialized ES client (cached for reuse)
    private volatile RestClient esClient;

    public PurgeESOperations(AtlasGraph graph, long esSettleWaitMs) {
        this.graph          = graph;
        this.esSettleWaitMs = esSettleWaitMs;
    }

    // ======================== ES CLIENT ========================

    public synchronized RestClient getEsClient() {
        if (esClient == null) {
            esClient = AtlasElasticsearchDatabase.getLowLevelClient();
        }
        return esClient;
    }

    @VisibleForTesting
    public void setEsClient(RestClient client) {
        this.esClient = client;
    }

    // ======================== COUNT ========================

    public long getEntityCount(String esQuery) throws Exception {
        RestClient client = getEsClient();
        String endpoint = "/" + VERTEX_INDEX_NAME + "/_count";

        Request request = new Request("POST", endpoint);
        request.setEntity(new NStringEntity(esQuery, ContentType.APPLICATION_JSON));

        Response response = client.performRequest(request);
        String responseBody = readResponseBody(response);
        JsonNode root = MAPPER.readTree(responseBody);
        return root.get("count").asLong();
    }

    // ======================== PIT MANAGEMENT ========================

    /**
     * Open a Point-in-Time (PIT) against the vertex index.
     * @return PIT ID string
     */
    public String openPIT(RestClient client) throws Exception {
        String endpoint = "/" + VERTEX_INDEX_NAME + "/_pit?keep_alive=" + PIT_KEEP_ALIVE;
        Request request = new Request("POST", endpoint);

        Response response = client.performRequest(request);
        String responseBody = readResponseBody(response);
        JsonNode root = MAPPER.readTree(responseBody);
        return root.get("id").asText();
    }

    /**
     * Close a Point-in-Time to free ES resources.
     */
    public void closePIT(RestClient client, String pitId) {
        try {
            if (pitId != null) {
                Request request = new Request("DELETE", "/_pit");
                String body = MAPPER.writeValueAsString(
                        MAPPER.createObjectNode().put("id", pitId));
                request.setEntity(new NStringEntity(body, ContentType.APPLICATION_JSON));
                client.performRequest(request);
            }
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to close PIT", e);
        }
    }

    // ======================== PIT-BASED STREAMING ========================

    /**
     * Stream ES results into the batch queue using PIT + search_after pagination.
     */
    public void streamESScrollIntoBatchQueue(PurgeContext ctx,
                                             BlockingQueue<BatchWork> batchQueue,
                                             int batchSize) throws Exception {
        RestClient client = getEsClient();
        String pitId = openPIT(client);

        try {
            String searchQuery = buildPITSearchQuery(ctx.esQuery, ES_PAGE_SIZE, pitId, null);

            Request searchRequest = new Request("POST", "/_search");
            searchRequest.setEntity(new NStringEntity(searchQuery, ContentType.APPLICATION_JSON));

            Response response = client.performRequest(searchRequest);
            String responseBody = readResponseBody(response);
            JsonNode root = MAPPER.readTree(responseBody);
            JsonNode hits = root.get("hits").get("hits");

            // Update PIT ID from response (ES may return a new one)
            if (root.has("pit_id")) {
                pitId = root.get("pit_id").asText();
            }

            List<String> currentBatch = new ArrayList<>(batchSize);
            int batchIndex = 0;

            while (hits != null && hits.size() > 0 && !ctx.cancelRequested) {
                JsonNode lastHit = null;
                for (JsonNode hit : hits) {
                    if (ctx.cancelRequested) break;

                    String esDocId = hit.get("_id").asText();
                    String vertexId = String.valueOf(LongEncoding.decode(esDocId));
                    currentBatch.add(vertexId);

                    if (currentBatch.size() >= batchSize) {
                        batchQueue.put(new BatchWork(new ArrayList<>(currentBatch), batchIndex++));
                        currentBatch.clear();
                    }
                    lastHit = hit;
                }

                if (ctx.cancelRequested || lastHit == null) break;

                // Extract sort values from last hit for search_after
                JsonNode sortValues = lastHit.get("sort");
                if (sortValues == null) break;

                searchQuery = buildPITSearchQuery(ctx.esQuery, ES_PAGE_SIZE, pitId, sortValues);

                searchRequest = new Request("POST", "/_search");
                searchRequest.setEntity(new NStringEntity(searchQuery, ContentType.APPLICATION_JSON));

                response = client.performRequest(searchRequest);
                responseBody = readResponseBody(response);
                root = MAPPER.readTree(responseBody);
                hits = root.get("hits").get("hits");

                if (root.has("pit_id")) {
                    pitId = root.get("pit_id").asText();
                }
            }

            if (!currentBatch.isEmpty() && !ctx.cancelRequested) {
                batchQueue.put(new BatchWork(new ArrayList<>(currentBatch), batchIndex));
            }
        } finally {
            closePIT(client, pitId);
        }
    }

    // ======================== ES RECONCILIATION ========================

    /**
     * Reconciliation-based ES cleanup. Waits for JanusGraph's ES index mutations
     * to settle, then paginates remaining docs and verifies each against JanusGraph.
     */
    public void reconcileESCleanup(PurgeContext ctx) {
        try {
            RestClient client = getEsClient();

            refreshEsIndex(client);
            try { Thread.sleep(esSettleWaitMs); } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
            refreshEsIndex(client);

            long remaining = getEntityCount(ctx.esQuery);
            if (remaining <= 0) {
                LOG.info("BulkPurge: ES reconciliation -- 0 docs remaining, nothing to clean for purgeKey={}", ctx.purgeKey);
                return;
            }

            LOG.info("BulkPurge: ES reconciliation -- {} docs remain after graph phase for purgeKey={}, " +
                    "reconciling against JanusGraph", remaining, ctx.purgeKey);

            reconcileESOrphans(client, ctx);
        } catch (Exception e) {
            LOG.error("BulkPurge: ES reconciliation failed for purgeKey={}. Manual cleanup may be needed.", ctx.purgeKey, e);
        }
    }

    /**
     * Paginate remaining ES docs using PIT + search_after and for each:
     * - If vertex is gone from graph: delete the ES doc (index lag orphan)
     * - If vertex still in graph: retry graph deletion, then delete ES doc
     */
    private void reconcileESOrphans(RestClient client, PurgeContext ctx) throws Exception {
        String pitId = openPIT(client);

        int totalOrphansDeleted = 0;
        int totalRetryDeleted = 0;
        int totalRetryFailed = 0;
        int totalCheckErrors = 0;

        try {
            String searchQuery = buildPITSearchQuery(ctx.esQuery, ES_PAGE_SIZE, pitId, null);

            Request searchRequest = new Request("POST", "/_search");
            searchRequest.setEntity(new NStringEntity(searchQuery, ContentType.APPLICATION_JSON));

            Response response = client.performRequest(searchRequest);
            String responseBody = readResponseBody(response);
            JsonNode root = MAPPER.readTree(responseBody);
            JsonNode hits = root.get("hits").get("hits");

            if (root.has("pit_id")) {
                pitId = root.get("pit_id").asText();
            }

            while (hits != null && hits.size() > 0 && !ctx.cancelRequested) {
                List<String> esDocIds = new ArrayList<>(hits.size());
                JsonNode lastHit = null;
                for (JsonNode hit : hits) {
                    esDocIds.add(hit.get("_id").asText());
                    lastHit = hit;
                }

                List<String> orphanDocIds = new ArrayList<>();
                List<String> retryVertexIds = new ArrayList<>();

                for (String esDocId : esDocIds) {
                    if (ctx.cancelRequested) break;

                    try {
                        String vertexId = String.valueOf(LongEncoding.decode(esDocId));
                        AtlasVertex vertex = graph.getVertex(vertexId);
                        if (vertex == null) {
                            orphanDocIds.add(esDocId);
                        } else {
                            retryVertexIds.add(vertexId);
                        }
                    } catch (Exception e) {
                        LOG.debug("BulkPurge: ES reconciliation -- could not verify vertex for ES doc {}", esDocId, e);
                        totalCheckErrors++;
                    }
                }

                if (!orphanDocIds.isEmpty()) {
                    deleteESDocsByIds(client, orphanDocIds);
                    totalOrphansDeleted += orphanDocIds.size();
                }

                if (!retryVertexIds.isEmpty() && !ctx.cancelRequested) {
                    int[] result = retryGraphDeletion(retryVertexIds);
                    totalRetryDeleted += result[0];
                    totalRetryFailed += result[1];
                }

                if (ctx.cancelRequested || lastHit == null) break;

                JsonNode sortValues = lastHit.get("sort");
                if (sortValues == null) break;

                searchQuery = buildPITSearchQuery(ctx.esQuery, ES_PAGE_SIZE, pitId, sortValues);

                searchRequest = new Request("POST", "/_search");
                searchRequest.setEntity(new NStringEntity(searchQuery, ContentType.APPLICATION_JSON));

                response = client.performRequest(searchRequest);
                responseBody = readResponseBody(response);
                root = MAPPER.readTree(responseBody);
                hits = root.get("hits").get("hits");

                if (root.has("pit_id")) {
                    pitId = root.get("pit_id").asText();
                }
            }
        } finally {
            closePIT(client, pitId);
        }

        refreshEsIndex(client);

        LOG.info("BulkPurge: ES reconciliation completed for purgeKey={}: orphansDeleted={}, " +
                        "retryDeleted={}, retryFailed={}, checkErrors={}",
                ctx.purgeKey, totalOrphansDeleted, totalRetryDeleted, totalRetryFailed, totalCheckErrors);

        if (totalRetryDeleted > 0) {
            ctx.totalDeleted.addAndGet(totalRetryDeleted);
        }
    }

    /**
     * Retry graph deletion for vertices that survived the main parallel phase.
     * @return int[2]: [successCount, failureCount]
     */
    private int[] retryGraphDeletion(List<String> vertexIds) {
        int deleted = 0;
        int failed = 0;

        for (String vertexId : vertexIds) {
            try {
                AtlasVertex vertex = graph.getVertex(vertexId);
                if (vertex == null) {
                    deleted++;
                    continue;
                }

                Iterable<AtlasEdge> edges = vertex.getEdges(AtlasEdgeDirection.BOTH);
                for (AtlasEdge edge : edges) {
                    graph.removeEdge(edge);
                }
                graph.removeVertex(vertex);
                graph.commit();
                deleted++;
            } catch (Exception e) {
                LOG.warn("BulkPurge: Retry graph deletion failed for vertex {}", vertexId, e);
                failed++;
                try { graph.rollback(); } catch (Exception re) {
                    LOG.debug("BulkPurge: Rollback failed after retry deletion failure", re);
                }
            }
        }

        if (deleted > 0 || failed > 0) {
            LOG.info("BulkPurge: Retry graph deletion batch: deleted={}, failed={}", deleted, failed);
        }
        return new int[]{deleted, failed};
    }

    // ======================== ES DOC OPERATIONS ========================

    public void deleteESDocsByIds(RestClient client, List<String> docIds) throws Exception {
        ObjectNode query = MAPPER.createObjectNode();
        ObjectNode queryBody = MAPPER.createObjectNode();
        ObjectNode terms = MAPPER.createObjectNode();
        terms.set("_id", MAPPER.valueToTree(docIds));
        queryBody.set("terms", terms);
        query.set("query", queryBody);

        String endpoint = "/" + VERTEX_INDEX_NAME + "/_delete_by_query?conflicts=proceed&refresh=false";
        Request request = new Request("POST", endpoint);
        request.setEntity(new NStringEntity(MAPPER.writeValueAsString(query), ContentType.APPLICATION_JSON));
        client.performRequest(request);
    }

    public void refreshEsIndex(RestClient client) {
        try {
            Request refreshRequest = new Request("POST", "/" + VERTEX_INDEX_NAME + "/_refresh");
            client.performRequest(refreshRequest);
        } catch (Exception e) {
            LOG.warn("BulkPurge: ES index refresh failed (verification count may be stale)", e);
        }
    }

    public void deleteConnectionFromES(PurgeContext ctx, String connGuid) {
        try {
            RestClient client = getEsClient();
            String query = buildTermQuery(GUID_PROPERTY_KEY, connGuid);
            String endpoint = "/" + VERTEX_INDEX_NAME + "/_delete_by_query?conflicts=proceed&refresh=false&requests_per_second=5000";

            Request request = new Request("POST", endpoint);
            request.setEntity(new NStringEntity(query, ContentType.APPLICATION_JSON));

            Response response = client.performRequest(request);
            String responseBody = readResponseBody(response);
            LOG.info("BulkPurge: Connection ES document deleted for purgeKey={}, guid={}, response={}",
                    ctx.purgeKey, connGuid, responseBody);
        } catch (Exception e) {
            LOG.warn("BulkPurge: Failed to delete Connection ES document for purgeKey={}, guid={}. " +
                    "Document will be orphaned in ES.", ctx.purgeKey, connGuid, e);
        }
    }

    // ======================== ES QUERY HELPERS ========================

    /**
     * Query ES to find vertex IDs matching a term query on a specific field.
     * Returns decoded JanusGraph vertex IDs.
     */
    public List<String> findVerticesByTermQuery(String field, String value) throws Exception {
        RestClient client = getEsClient();
        String query = buildTermQuery(field, value);

        ObjectNode queryNode = (ObjectNode) MAPPER.readTree(query);
        queryNode.put("size", 10000);
        queryNode.put("_source", false);

        String endpoint = "/" + VERTEX_INDEX_NAME + "/_search";
        Request request = new Request("POST", endpoint);
        request.setEntity(new NStringEntity(MAPPER.writeValueAsString(queryNode), ContentType.APPLICATION_JSON));

        Response response = client.performRequest(request);
        JsonNode root = MAPPER.readTree(response.getEntity().getContent());
        JsonNode hits = root.path("hits").path("hits");

        List<String> vertexIds = new ArrayList<>();
        for (JsonNode hit : hits) {
            String esId = hit.path("_id").asText();
            try {
                vertexIds.add(String.valueOf(LongEncoding.decode(esId)));
            } catch (Exception e) {
                LOG.debug("BulkPurge: Could not decode ES _id {} for term query", esId);
            }
        }
        return vertexIds;
    }

    /**
     * Query ES to find DataProduct vertices that reference any of the given GUIDs
     * in their daapOutputPortGuids or daapInputPortGuids fields.
     */
    public List<String> findDataProductsReferencingGuids(List<String> guids) throws Exception {
        RestClient client = getEsClient();

        ObjectNode query = MAPPER.createObjectNode();
        ObjectNode boolNode = MAPPER.createObjectNode();
        ObjectNode mustNode = MAPPER.createObjectNode();
        mustNode.set("term", MAPPER.createObjectNode().put("__typeName.keyword", "DataProduct"));

        ObjectNode shouldOutput = MAPPER.createObjectNode();
        ObjectNode outputTerms = MAPPER.createObjectNode();
        outputTerms.set("daapOutputPortGuids", MAPPER.valueToTree(guids));
        shouldOutput.set("terms", outputTerms);

        ObjectNode shouldInput = MAPPER.createObjectNode();
        ObjectNode inputTerms = MAPPER.createObjectNode();
        inputTerms.set("daapInputPortGuids", MAPPER.valueToTree(guids));
        shouldInput.set("terms", inputTerms);

        boolNode.set("must", MAPPER.createArrayNode().add(mustNode));
        boolNode.set("should", MAPPER.createArrayNode().add(shouldOutput).add(shouldInput));
        boolNode.put("minimum_should_match", 1);

        query.set("query", MAPPER.createObjectNode().set("bool", boolNode));
        query.put("size", 10000);
        query.put("_source", false);

        String endpoint = "/" + VERTEX_INDEX_NAME + "/_search";
        Request request = new Request("POST", endpoint);
        request.setEntity(new NStringEntity(query.toString(), ContentType.APPLICATION_JSON));

        Response response = client.performRequest(request);
        JsonNode root = MAPPER.readTree(response.getEntity().getContent());
        JsonNode hits = root.path("hits").path("hits");

        List<String> vertexIds = new ArrayList<>();
        for (JsonNode hit : hits) {
            String esId = hit.path("_id").asText();
            try {
                vertexIds.add(String.valueOf(LongEncoding.decode(esId)));
            } catch (Exception e) {
                LOG.debug("BulkPurge: Could not decode ES _id {} for DataProduct lookup", esId);
            }
        }
        return vertexIds;
    }

    /**
     * Query ES to find AuthPolicy vertex IDs for a connection's bootstrap policies.
     */
    public List<String> findConnectionPolicyVertexIds(String connGuid, String roleName) throws Exception {
        RestClient client = getEsClient();

        ObjectNode query = MAPPER.createObjectNode();
        ObjectNode boolNode = MAPPER.createObjectNode();

        boolNode.set("must", MAPPER.createArrayNode()
                .add(MAPPER.createObjectNode().set("term", MAPPER.createObjectNode().put("__typeName.keyword", "AuthPolicy")))
                .add(MAPPER.createObjectNode().set("prefix", MAPPER.createObjectNode().put(QUALIFIED_NAME, connGuid + "/")))
                .add(MAPPER.createObjectNode().set("term", MAPPER.createObjectNode().put("policyRoles", roleName))));

        query.set("query", MAPPER.createObjectNode().set("bool", boolNode));
        query.put("size", 1000);
        query.put("_source", false);

        String endpoint = "/" + VERTEX_INDEX_NAME + "/_search";
        Request request = new Request("POST", endpoint);
        request.setEntity(new NStringEntity(query.toString(), ContentType.APPLICATION_JSON));

        Response response = client.performRequest(request);
        JsonNode root = MAPPER.readTree(response.getEntity().getContent());
        JsonNode hits = root.path("hits").path("hits");

        List<String> vertexIds = new ArrayList<>();
        for (JsonNode hit : hits) {
            String esId = hit.path("_id").asText();
            try {
                vertexIds.add(String.valueOf(LongEncoding.decode(esId)));
            } catch (Exception e) {
                LOG.debug("BulkPurge: Could not decode ES _id {} for policy lookup", esId);
            }
        }
        return vertexIds;
    }

    /**
     * Query ES to find Stakeholder vertex IDs linked to a StakeholderTitle GUID.
     */
    public List<String> findStakeholdersForTitle(String titleGuid) throws Exception {
        RestClient client = getEsClient();

        ObjectNode query = MAPPER.createObjectNode();
        ObjectNode boolNode = MAPPER.createObjectNode();

        boolNode.set("must", MAPPER.createArrayNode()
                .add(MAPPER.createObjectNode().set("term",
                        MAPPER.createObjectNode().put("__typeName.keyword", "Stakeholder")))
                .add(MAPPER.createObjectNode().set("term",
                        MAPPER.createObjectNode().put("__state", "ACTIVE")))
                .add(MAPPER.createObjectNode().set("term",
                        MAPPER.createObjectNode().put("stakeholderTitleGuid", titleGuid))));

        query.set("query", MAPPER.createObjectNode().set("bool", boolNode));
        query.put("size", 10000);
        query.put("_source", false);

        String endpoint = "/" + VERTEX_INDEX_NAME + "/_search";
        Request request = new Request("POST", endpoint);
        request.setEntity(new NStringEntity(query.toString(), ContentType.APPLICATION_JSON));

        Response response = client.performRequest(request);
        JsonNode root = MAPPER.readTree(response.getEntity().getContent());
        JsonNode hits = root.path("hits").path("hits");

        List<String> vertexIds = new ArrayList<>();
        for (JsonNode hit : hits) {
            String esId = hit.path("_id").asText();
            try {
                vertexIds.add(String.valueOf(LongEncoding.decode(esId)));
            } catch (Exception e) {
                LOG.debug("BulkPurge: Could not decode ES _id {} for stakeholder lookup", esId);
            }
        }
        return vertexIds;
    }

    // ======================== QUERY BUILDERS ========================

    public String buildTermQuery(String field, String value) {
        try {
            ObjectNode query = MAPPER.createObjectNode();
            ObjectNode queryBody = MAPPER.createObjectNode();
            ObjectNode term = MAPPER.createObjectNode();
            term.put(field, value);
            queryBody.set("term", term);
            query.set("query", queryBody);
            return MAPPER.writeValueAsString(query);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build term query", e);
        }
    }

    public String buildPrefixQuery(String field, String value) {
        try {
            ObjectNode query = MAPPER.createObjectNode();
            ObjectNode queryBody = MAPPER.createObjectNode();
            ObjectNode prefix = MAPPER.createObjectNode();
            prefix.put(field, value);
            queryBody.set("prefix", prefix);
            query.set("query", queryBody);
            return MAPPER.writeValueAsString(query);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build prefix query", e);
        }
    }

    /**
     * Build a search query for PIT + search_after pagination.
     *
     * @param esQuery      the base ES query (containing a "query" field)
     * @param pageSize     number of hits per page
     * @param pitId        the PIT ID
     * @param searchAfter  sort values from the last hit of the previous page (null for first page)
     */
    public String buildPITSearchQuery(String esQuery, int pageSize, String pitId, JsonNode searchAfter) {
        try {
            JsonNode queryNode = MAPPER.readTree(esQuery);
            ObjectNode searchQuery = MAPPER.createObjectNode();
            searchQuery.set("query", queryNode.get("query"));
            searchQuery.put("size", pageSize);
            searchQuery.put("_source", false);
            searchQuery.put("track_total_hits", true);

            // PIT configuration
            ObjectNode pitNode = MAPPER.createObjectNode();
            pitNode.put("id", pitId);
            pitNode.put("keep_alive", PIT_KEEP_ALIVE);
            searchQuery.set("pit", pitNode);

            // Sort by _shard_doc for efficient PIT pagination (no real sort cost)
            ArrayNode sortArray = MAPPER.createArrayNode();
            sortArray.add(MAPPER.createObjectNode().put("_shard_doc", "asc"));
            searchQuery.set("sort", sortArray);

            // search_after for subsequent pages
            if (searchAfter != null) {
                searchQuery.set("search_after", searchAfter);
            }

            return MAPPER.writeValueAsString(searchQuery);
        } catch (Exception e) {
            throw new RuntimeException("Failed to build PIT search query", e);
        }
    }

    public String readResponseBody(Response response) throws Exception {
        try (InputStream is = response.getEntity().getContent()) {
            return new String(is.readAllBytes());
        }
    }
}
