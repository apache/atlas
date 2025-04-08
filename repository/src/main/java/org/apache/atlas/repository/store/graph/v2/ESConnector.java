package org.apache.atlas.repository.store.graph.v2;

import com.datastax.oss.driver.shaded.json.JSONArray;
import com.datastax.oss.driver.shaded.json.JSONObject;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.utils.AtlasPerfMetrics;
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
import org.janusgraph.util.encoding.LongEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_TEXT_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_TRAIT_NAMES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX_NAME;import static org.apache.atlas.repository.audit.ESBasedAuditRepository.getHttpHosts;

public class ESConnector {
    private static final Logger LOG      = LoggerFactory.getLogger(ESConnector.class);

    private static RestClient lowLevelClient;

    private static Set<String> DENORM_ATTRS;
    private static String endpoint = VERTEX_INDEX_NAME + "/_update/%s";
    private static String GET_DOCS_BY_ID = VERTEX_INDEX_NAME + "/_mget";

    static {
        try {
            if (lowLevelClient == null) {
                try {
                    LOG.info("ESBasedAuditRepo - setLowLevelClient!");
                    List<HttpHost> httpHosts = getHttpHosts();

                    RestClientBuilder builder = RestClient.builder(httpHosts.get(0));
                    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                            .setConnectTimeout(AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                            .setSocketTimeout(AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT.getInt()));

                    lowLevelClient = builder.build();

                    DENORM_ATTRS = new HashSet<>();
                    DENORM_ATTRS.add(PROPAGATED_TRAIT_NAMES_PROPERTY_KEY); //List
                    DENORM_ATTRS.add(PROPAGATED_CLASSIFICATION_NAMES_KEY); //String
                    DENORM_ATTRS.add(CLASSIFICATION_TEXT_KEY); //String


                } catch (AtlasException e) {
                    LOG.error("Failed to initialize low level rest client for ES");
                    throw new AtlasException(e);
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void writeTagProperties(Map<String, Map<String, Object>> entitiesMap) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("writeTagPropertiesES");

        try {
            StringBuilder bulkRequestBody = new StringBuilder();

            for (String assetVertexId : entitiesMap.keySet()) {
                Map<String, Object> entry = entitiesMap.get(assetVertexId);
                Map<String, Object> toUpdate = new HashMap<>();

                DENORM_ATTRS.stream().filter(entry::containsKey).forEach(x -> toUpdate.put(x, entry.get(x)));
                toUpdate.put("__modificationTimestamp", System.currentTimeMillis());

                if (!toUpdate.isEmpty()) {
                    long vertexId = Long.valueOf(assetVertexId);
                    String docId = LongEncoding.encode(vertexId);
                    bulkRequestBody.append("{ \"update\": { \"_index\": \"janusgraph_vertex_index\", \"_id\": \"" + docId + "\" } }\n");

                    String attrsToUpdate = AtlasType.toJson(toUpdate);
                    bulkRequestBody.append("{ \"doc\": " + attrsToUpdate + " }\n");
                }
            }

            Request request = new Request("POST", "/_bulk");
            request.setEntity(new StringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON));

            try {
                Response response = lowLevelClient.performRequest(request);
            } catch (IOException e) {
                LOG.error("Failed to update ES doc for denorm attributes");
                throw new RuntimeException(e);
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static Map<String, Map<String, Object>> getTagAttributes(Collection<AtlasVertex> vertices) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("getTagAttributesES");
        Map<String, Map<String, Object>> ret = new HashMap<>();

        try {
            Map<String, String> docIdTovertexIdMap = new HashMap<>();
            List<String> vertexIds = vertices.stream().map(x -> x.getIdForDisplay()).toList();
            vertexIds.stream().map(vertexId -> docIdTovertexIdMap.put(LongEncoding.encode(Long.parseLong(vertexId)), vertexId));
            Set<String> docIds = docIdTovertexIdMap.keySet();

            JSONObject requestBody = new JSONObject();
            requestBody.put("ids", new JSONArray(docIds));
            requestBody.put("_source", new JSONArray(DENORM_ATTRS));


            Request request = new Request("POST", GET_DOCS_BY_ID);
            HttpEntity entity = new NStringEntity(requestBody.toString(), ContentType.APPLICATION_JSON);
            request.setEntity(entity);

            Response response = lowLevelClient.performRequest(request);
            String responseBody = EntityUtils.toString(response.getEntity());

            JSONObject jsonResponse = new JSONObject(responseBody);
            JSONArray docs = jsonResponse.getJSONArray("docs");

            for (int i = 0; i < docs.length(); i++) {
                JSONObject doc = docs.getJSONObject(i);
                String docId = doc.getString("_id");
                Map<String, Object> assetAttributes = new HashMap<>();

                if (doc.getBoolean("found")) {
                    JSONObject source = doc.getJSONObject("_source");

                    // Print all properties in the source
                    Iterator<String> keys = source.keys();
                    while (keys.hasNext()) {
                        String key = keys.next();
                        assetAttributes.put(key, source.get(key));
                    }
                } else {
                    LOG.warn("Document with docId {} not found", docId);
                }

                ret.put(docIdTovertexIdMap.get(docId) ,assetAttributes);
            }

        } catch (IOException e) {
            LOG.error("Failed to GET denorm attributes from ES");
            throw new RuntimeException(e);
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return ret;
    }
}