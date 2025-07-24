package org.apache.atlas.repository.search;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.Service;
import org.apache.atlas.type.AtlasType;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.xcontent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.audit.ESBasedAuditRepository.getHttpHosts;

/**
 * Elasticsearch implementation of DirectSearchRepository.
 */
@Singleton
@Component
@Order(8)
public class ESDirectSearchRepository implements DirectSearchRepository, Service {
    private static final Logger LOG = LoggerFactory.getLogger(ESDirectSearchRepository.class);

    private RestClient lowLevelClient;

    @Inject
    public ESDirectSearchRepository() {
        // Constructor for dependency injection
    }

    @Override
    public void start() throws AtlasException {
        LOG.info("ESDirectSearchRepository - start!");
        initializeClient();
    }

    @Override
    public void stop() throws AtlasException {
        try {
            LOG.info("ESDirectSearchRepository - stop!");
            if (lowLevelClient != null) {
                lowLevelClient.close();
                lowLevelClient = null;
            }
        } catch (IOException e) {
            LOG.error("ESDirectSearchRepository - error while closing es lowlevel client", e);
            throw new AtlasException(e);
        }
    }

    private void initializeClient() throws AtlasException {
        if (lowLevelClient == null) {
            try {
                LOG.info("ESDirectSearchRepository - initializing client!");
                List<HttpHost> httpHosts = getHttpHosts();

                RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[0]));
                builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> 
                    httpAsyncClientBuilder.setKeepAliveStrategy((response, context) -> 3600000));
                builder.setRequestConfigCallback(requestConfigBuilder -> 
                    requestConfigBuilder
                        .setConnectTimeout(AtlasConfiguration.INDEX_CLIENT_CONNECTION_TIMEOUT.getInt())
                        .setSocketTimeout(AtlasConfiguration.INDEX_CLIENT_SOCKET_TIMEOUT.getInt()));

                lowLevelClient = builder.build();
            } catch (AtlasException e) {
                LOG.error("Failed to initialize low level rest client for ES", e);
                throw new AtlasException(e);
            }
        }
    }

    @Override
    public Map<String, Object> searchWithRawJson(String indexName, String queryJson) throws AtlasBaseException {
        try {
            if (lowLevelClient == null) {
                initializeClient();
            }

            Request request = new Request("POST", indexName + "/_search");
            request.setJsonEntity(queryJson);

            Response response = lowLevelClient.performRequest(request);
            String responseString = EntityUtils.toString(response.getEntity());

            // Parse the raw JSON response into a Map
            Map<String, Object> searchResponse = AtlasType.fromJson(responseString, Map.class);
            LOG.debug("<== ESDirectSearchRepository.searchWithRawJson() - found {} hits",
                    ((Map)((Map)searchResponse.get("hits")).get("total")).get("value"));
            return searchResponse;
        } catch (IOException | AtlasException e) {
            LOG.error("Error performing raw JSON search on index {}: query={}, error={}",
                    indexName, queryJson, e.getMessage(), e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED,
                    String.format("Search failed on index %s: %s", indexName, e.getMessage()));
        }
    }

    @Override
    public OpenPointInTimeResponse openPointInTime(OpenPointInTimeRequest pitRequest) throws AtlasBaseException {
        if (pitRequest == null || pitRequest.indices() == null || pitRequest.indices().length == 0) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "PIT request or indices cannot be null");
        }

        String indexName = pitRequest.indices()[0];
        LOG.debug("==> ESDirectSearchRepository.openPointInTime(index={}, keepAlive={})",
                indexName, pitRequest.keepAlive());

        try {
            if (lowLevelClient == null) {
                initializeClient();
            }

            String keepAliveParam = String.format("keep_alive=%s", pitRequest.keepAlive());
            String endpoint = indexName + "/_pit?" + keepAliveParam;

            Request request = new Request("POST", endpoint);
            request.setOptions(RequestOptions.DEFAULT.toBuilder()
                    .addHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType())
                    .build());
            Response response = lowLevelClient.performRequest(request);
            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            response.getEntity().getContent())) {
                OpenPointInTimeResponse pitResponse = OpenPointInTimeResponse.fromXContent(parser);
                LOG.debug("<== ESDirectSearchRepository.openPointInTime() - created PIT: {}",
                        pitResponse.getPointInTimeId());
                return pitResponse;
            }
        } catch (IOException | AtlasException e) {
            LOG.error("Error opening PIT for index {}, keepAlive={}: {}",
                    indexName, pitRequest.keepAlive(), e.getMessage(), e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED,
                    String.format("Failed to open PIT for index %s: %s", indexName, e.getMessage()));
        }
    }

    @Override
    public ClosePointInTimeResponse closePointInTime(ClosePointInTimeRequest closeRequest) throws AtlasBaseException {
        if (closeRequest == null || closeRequest.getId() == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "Close request or PIT ID cannot be null");
        }

        String pitId = closeRequest.getId();
        LOG.debug("==> ESDirectSearchRepository.closePointInTime(pitId={})", pitId);

        try {
            if (lowLevelClient == null) {
                initializeClient();
            }

            Request request = new Request("DELETE", "/_pit");

            // Create request body with PIT ID
            String requestBody = String.format("{\"id\": \"%s\"}", pitId);
            HttpEntity entity = new StringEntity(requestBody, ContentType.APPLICATION_JSON);
            request.setEntity(entity);

            Response response = lowLevelClient.performRequest(request);

            try (XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(NamedXContentRegistry.EMPTY,
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            response.getEntity().getContent())) {
                ClosePointInTimeResponse closeResponse = ClosePointInTimeResponse.fromXContent(parser);
                LOG.debug("<== ESDirectSearchRepository.closePointInTime() - closed PIT: {}, succeeded: {}",
                        pitId, closeResponse.isSucceeded());
                return closeResponse;
            }
        } catch (IOException | AtlasException e) {
            LOG.error("Error closing PIT ID {}: {}", pitId, e.getMessage(), e);
            throw new AtlasBaseException(AtlasErrorCode.DISCOVERY_QUERY_FAILED,
                    String.format("Failed to close PIT %s: %s", pitId, e.getMessage()));
        }
    }
}