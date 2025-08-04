package org.apache.atlas.repository.store.graph.v2;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.MapUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.janusgraph.util.StringUtils;
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX_NAME;import static org.apache.atlas.repository.audit.ESBasedAuditRepository.getHttpHosts;

public class ESConnector implements Closeable {
    private static final Logger LOG      = LoggerFactory.getLogger(ESConnector.class);

    private static RestClient lowLevelClient;

    private static Set<String> DENORM_ATTRS;
    private static String GET_DOCS_BY_ID = VERTEX_INDEX_NAME + "/_mget";

    static {
        try {
            lowLevelClient = initializeClient();
            DENORM_ATTRS = initializeDenormAttributes();
        } catch (AtlasException e) {
            throw new RuntimeException("Failed to initialize ESConnector", e);
        }
    }

    private static RestClient initializeClient() throws AtlasException {
        try {
            List<HttpHost> httpHosts = getHttpHosts();
            RestClientBuilder builder = RestClient.builder(httpHosts.get(0))
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                            .setConnectTimeout(AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                            .setSocketTimeout(AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT.getInt()));

            return builder.build();
        } catch (Exception e) {
            throw new AtlasException("Failed to initialize Elasticsearch client", e);
        }
    }

    private static Set<String> initializeDenormAttributes() {
        Set<String> attrs = new HashSet<>();
        attrs.add(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY);
        attrs.add(PROPAGATED_CLASSIFICATION_NAMES_KEY);
        attrs.add(CLASSIFICATION_TEXT_KEY);
        attrs.add(TRAIT_NAMES_PROPERTY_KEY);
        attrs.add(CLASSIFICATION_NAMES_KEY);
        return Collections.unmodifiableSet(attrs);
    }

    public static void writeTagProperties(Map<String, Map<String, Object>> entitiesMap) {
        writeTagProperties(entitiesMap, false);
    }

    /**
     * Updates and writes tag properties for multiple entities to Elasticsearch index.
     *
     * This method processes the provided entities map to prepare an Elasticsearch bulk
     * request for updating tag properties and denormalized attributes. The modifications
     * include attributes specified in the {@code DENORM_ATTRS} field and a modification
     * timestamp. The bulk request is then executed using a low-level client.
     *
     * @param entitiesMap A map where the keys represent the entity vertex IDs (as strings),
     *                    and the values are maps containing the attributes to be updated
     *                    for each entity.
     * @param upsert A boolean flag that indicates whether the update operation should upsert
     *               (create new doc if not found) the document in the Elasticsearch index.
     */
    public static void writeTagProperties(Map<String, Map<String, Object>> entitiesMap, boolean upsert) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("writeTagPropertiesES");

        try {
            if (MapUtils.isEmpty(entitiesMap))
                return;

            StringBuilder bulkRequestBody = new StringBuilder();

            for (String assetVertexId : entitiesMap.keySet()) {
                Map<String, Object> entry = entitiesMap.get(assetVertexId);
                Map<String, Object> toUpdate = new HashMap<>();

                DENORM_ATTRS.stream().filter(entry::containsKey).forEach(x -> toUpdate.put(x, entry.get(x)));
//                toUpdate.put("__modificationTimestamp", System.currentTimeMillis());


                long vertexId = Long.parseLong(assetVertexId);
                String docId = LongEncoding.encode(vertexId);
                bulkRequestBody.append("{\"update\":{\"_index\":\"janusgraph_vertex_index\",\"_id\":\"").append(docId).append("\" }}\n");

                bulkRequestBody.append("{");
                String attrsToUpdate = AtlasType.toJson(toUpdate);
                bulkRequestBody.append("\"doc\":").append(attrsToUpdate);

                if (upsert) {
                    bulkRequestBody.append(",\"upsert\":").append(attrsToUpdate);
                }

                bulkRequestBody.append("}\n");
            }

            Request request = new Request("POST", "/_bulk");
            request.setEntity(new StringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON));

            try {
                lowLevelClient.performRequest(request);
            } catch (IOException e) {
                LOG.error("Failed to update ES doc for denorm attributes");
                throw new RuntimeException(e);
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    @Override
    public void close() throws IOException {
        if (lowLevelClient != null) {
            lowLevelClient.close();
        }
    }
}