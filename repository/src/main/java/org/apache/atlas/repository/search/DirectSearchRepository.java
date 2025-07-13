package org.apache.atlas.repository.search;

import org.apache.atlas.exception.AtlasBaseException;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;

import java.util.Map;

/**
 * Interface for performing direct search operations against the search backend.
 */
public interface DirectSearchRepository {

    /**
     * Executes a search query on the specified index using raw JSON.
     *
     * @param indexName the name of the index to search
     * @param queryJson the raw JSON query string
     * @return a Map containing the search results
     * @throws AtlasBaseException if an error occurs during the search
     */
    Map<String, Object> searchWithRawJson(String indexName, String queryJson) throws AtlasBaseException;

    /**
     * Opens a Point In Time (PIT) for the specified index.
     *
     * @param pitRequest the request containing PIT parameters
     * @return OpenPointInTimeResponse containing the PIT ID
     * @throws AtlasBaseException if an error occurs while opening the PIT
     */
    OpenPointInTimeResponse openPointInTime(OpenPointInTimeRequest pitRequest) throws AtlasBaseException;

    /**
     * Closes a Point In Time (PIT) using the provided ClosePointInTimeRequest.
     *
     * @param closeRequest the request containing the PIT ID to close
     * @return ClosePointInTimeResponse indicating success or failure
     * @throws AtlasBaseException if an error occurs while closing the PIT
     */
    ClosePointInTimeResponse closePointInTime(ClosePointInTimeRequest closeRequest) throws AtlasBaseException;
} 