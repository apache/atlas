package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.atlas.repository.Constants.INDEX_PREFIX;
import static org.apache.atlas.repository.Constants.VERTEX_INDEX;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getClient;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getLowLevelClient;

@Repository
public class AtlasJanusVertexIndexESRepositoryImpl implements AtlasJanusVertexIndexRepository {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusVertexIndexESRepositoryImpl.class);
    private final RestHighLevelClient elasticSearchClient = getClient();
    private final RestClient elasticSearchLowLevelClient = getLowLevelClient();
    private static final int MAX_RETRIES = 3;
    private static final int RETRY_TIME_IN_MILLIS = 500;
    private static final String INDEX = INDEX_PREFIX + VERTEX_INDEX;
    private static final String BULK_REQUEST_TIMEOUT_MINUTES = "2m";
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("AtlasJanusVertexIndexESRepositoryImpl");


    @Override
    public BulkResponse performBulk(BulkRequest bulkRequest) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG))
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "updateDocsInBulk()");
        try {
            int count = 0;
            int retryTime = RETRY_TIME_IN_MILLIS;
            while(true) {
                try {
                    LOG.info("Updating {} requests in ES", bulkRequest.requests().size());
                    return elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    LOG.error("Exception while trying to update in bulk for req. Retrying", e);
                    try {
                        retryTime *= 2; // exponential backoff
                        LOG.info("Retrying with delay of {} ms ", retryTime);
                        Thread.sleep(retryTime);
                    } catch (InterruptedException ex) {
                        LOG.error("Retry interrupted during bulk update request");
                        throw new AtlasBaseException(AtlasErrorCode.RUNTIME_EXCEPTION, ex);
                    }
                    if (++count == MAX_RETRIES) {
                        LOG.error("Retries exhausted. Failed to execute bulk update request on ES {}", e.getMessage());
                        throw new AtlasBaseException(AtlasErrorCode.ES_BULK_UPDATE_FAILED, e.getMessage());
                    }
                }
            }
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @Override
    public void performBulkAsyncV2(List<UpdateRequest> updateRequests) {
        int count = 0;
        int batch = 1000;
        BulkRequest bulkRequest = buildBulkRequestInstance();
        for (UpdateRequest updateRequest : updateRequests) {
            count++;
            bulkRequest.add(updateRequest);
            if(count % batch == 0) {
                elasticSearchClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new AtlasRelationshipIndexIOHandler(bulkRequest, this).getListener());
                bulkRequest = buildBulkRequestInstance();
            }
        }
        if (bulkRequest.numberOfActions() > 0)
            elasticSearchClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new AtlasRelationshipIndexIOHandler(bulkRequest, this).getListener());
    }

    @Override
    public void performBulkAsync(BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG))
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "updateDocsInBulkAsync()");
        try {
            LOG.info("Updating {} requests in ES", bulkRequest.requests().size());
            elasticSearchClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, listener);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @Override
    public UpdateResponse updateDoc(UpdateRequest request, RequestOptions options) throws AtlasBaseException {
        int count = 0;
        while(true) {
            try {
                if (LOG.isDebugEnabled())
                    LOG.debug("Updating entity in ES with req {}", request.toString());
                return elasticSearchClient.update(request, options);
            } catch (IOException e) {
                LOG.warn(String.format("Exception while trying to create nested relationship for req %s. Retrying",
                        request.toString()), e);
                LOG.info("Retrying with delay of {} ms ", RETRY_TIME_IN_MILLIS);
                try {
                    Thread.sleep(RETRY_TIME_IN_MILLIS);
                } catch (InterruptedException ex) {
                    LOG.warn("Retry interrupted during edge creation ");
                    throw new AtlasBaseException(AtlasErrorCode.RUNTIME_EXCEPTION, ex);
                }
                if (++count == MAX_RETRIES) {
                    LOG.error("Failed to execute direct update on ES {}", e.getMessage());
                    throw new AtlasBaseException(AtlasErrorCode.ES_DIRECT_UPDATE_FAILED, e.getMessage());
                }
            }
        }
    }

    @Override
    public void updateDocAsync(UpdateRequest request, RequestOptions options, ActionListener<UpdateResponse> listener) {
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG))
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "updateDocAsync()");
        try {
            elasticSearchClient.updateAsync(request, options, listener);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @Override
    public Response performRawRequest(String queryJson, String docId) throws AtlasBaseException {
        Objects.requireNonNull(queryJson, "query");
        int count = 0;
        while(true) {
            final String endPoint = "/" + INDEX + "/_update" + "/" + docId + "?retry_on_conflict=5";
            try {
                final Request request = buildHttpRequest(queryJson, endPoint);
                return elasticSearchLowLevelClient.performRequest(request);
            } catch (IOException e) {
                LOG.error(ExceptionUtils.getStackTrace(e));
                LOG.info("Retrying with delay of {} ms ", RETRY_TIME_IN_MILLIS);
                try {
                    Thread.sleep(RETRY_TIME_IN_MILLIS);
                } catch (InterruptedException ex) {
                    LOG.warn("Retry interrupted during ES relationship creation/deletion");
                    throw new AtlasBaseException(AtlasErrorCode.RUNTIME_EXCEPTION, ex);
                }
                if (++count == MAX_RETRIES) {
                    LOG.error("Failed to execute direct update on ES {}", e.getMessage());
                    throw new AtlasBaseException(AtlasErrorCode.ES_DIRECT_UPDATE_FAILED, e.getMessage());
                }
            }
        }
    }

    @Override
    public void performRawRequestAsync(String queryJson, String docId, ResponseListener listener) {
        Objects.requireNonNull(queryJson, "query");
        AtlasPerfTracer perf = null;
        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG))
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "performRawRequestAsync()");
        try {
            final String endPoint = "/" + INDEX + "/_update" + "/" + docId + "?retry_on_conflict=5";
            final Request request = buildHttpRequest(queryJson, endPoint);
            elasticSearchLowLevelClient.performRequestAsync(request, listener);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private static Request buildHttpRequest(String queryJson, String endPoint) {
        Request request = new Request(
                "POST",
                endPoint);
        request.addParameters(Collections.emptyMap());
        HttpEntity entity = new StringEntity(queryJson, ContentType.APPLICATION_JSON);
        request.setEntity(entity);
        return request;
    }

    private static BulkRequest buildBulkRequestInstance() {
        return new BulkRequest().timeout(BULK_REQUEST_TIMEOUT_MINUTES);
    }

}