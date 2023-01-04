package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;

import java.util.List;

public interface AtlasJanusVertexIndexRepository {

    UpdateResponse updateDoc(UpdateRequest request, RequestOptions options) throws AtlasBaseException;
    void updateDocAsync(UpdateRequest request, RequestOptions options, ActionListener<UpdateResponse> listener);
    Response performRawRequest(String query, String docId) throws AtlasBaseException;
    void performRawRequestAsync(String query, String docId, ResponseListener listener);
    BulkResponse performBulk(BulkRequest bulkRequest) throws AtlasBaseException;
    void performBulkAsync(BulkRequest bulkRequest, ActionListener<BulkResponse> listener);
    void performBulkAsyncV2(List<UpdateRequest> updateRequests);

}