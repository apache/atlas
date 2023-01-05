package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides all callback logic for responses
 * to handle failures and retries.
 * The listener provides methods to access to the response and the failure events.
 */
public class AtlasRelationshipIndexIOHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipIndexIOHandler.class);
    private final BulkRequest bulkRequest;
    private final AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository;
    private final ActionListener<BulkResponse> listener;

    public AtlasRelationshipIndexIOHandler(BulkRequest bulkRequest, AtlasJanusVertexIndexRepository atlasJanusVertexIndexRepository) {
        this.bulkRequest = bulkRequest;
        this.atlasJanusVertexIndexRepository = atlasJanusVertexIndexRepository;
        this.listener = new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    handleBulkResponseFailures(bulkResponse);
                }
                @Override
                public void onFailure(Exception e) {
                    LOG.error("------- bulk update failed -------", e);
                    try {
                        atlasJanusVertexIndexRepository.performBulk(bulkRequest);
                    } catch (AtlasBaseException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
    }

    public ActionListener<BulkResponse> getListener() {
        return listener;
    }

    private static void handleBulkResponseFailures(BulkResponse response) {
        if (response.hasFailures()) {
            for (BulkItemResponse bulkItemResponse : response) {
                if (bulkItemResponse.isFailed()) {
                    DocWriteResponse itemResponse = bulkItemResponse.getResponse();
                    if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                        UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                        // TODO:
                    }
                }
            }
        }
    }

}
