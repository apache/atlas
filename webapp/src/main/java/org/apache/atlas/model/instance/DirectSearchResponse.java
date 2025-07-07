package org.apache.atlas.model.instance;

import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchResponse;
import java.util.Objects;

/**
 * Wrapper class to handle different types of Elasticsearch responses.
 * This class encapsulates responses from various Elasticsearch operations
 * including search, PIT creation, and PIT deletion.
 */
public class DirectSearchResponse {
    private final SearchResponse searchResponse;
    private final OpenPointInTimeResponse pitCreateResponse;
    private final ClosePointInTimeResponse pitDeleteResponse;
    private final String responseType;

    private DirectSearchResponse(SearchResponse searchResponse,
                                 OpenPointInTimeResponse pitCreateResponse,
                                 ClosePointInTimeResponse pitDeleteResponse,
                                 String responseType) {
        this.searchResponse = searchResponse;
        this.pitCreateResponse = pitCreateResponse;
        this.pitDeleteResponse = pitDeleteResponse;
        this.responseType = responseType;
    }

    /**
     * Creates a DirectSearchResponse instance from a SearchResponse.
     * @param response the Elasticsearch SearchResponse
     * @return a new DirectSearchResponse instance
     */
    public static DirectSearchResponse fromSearchResponse(SearchResponse response) {
        return new DirectSearchResponse(response, null, null, "SEARCH");
    }

    /**
     * Creates a DirectSearchResponse instance from an OpenPointInTimeResponse.
     * @param response the Elasticsearch OpenPointInTimeResponse
     * @return a new DirectSearchResponse instance
     */
    public static DirectSearchResponse fromPitCreateResponse(OpenPointInTimeResponse response) {
        return new DirectSearchResponse(null, response, null, "PIT_CREATE");
    }

    /**
     * Creates a DirectSearchResponse instance from a ClosePointInTimeResponse.
     * @param response the Elasticsearch ClosePointInTimeResponse
     * @return a new DirectSearchResponse instance
     */
    public static DirectSearchResponse fromPitDeleteResponse(ClosePointInTimeResponse response) {
        return new DirectSearchResponse(null, null, response, "PIT_DELETE");
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public OpenPointInTimeResponse getPitCreateResponse() {
        return pitCreateResponse;
    }

    public ClosePointInTimeResponse getPitDeleteResponse() {
        return pitDeleteResponse;
    }

    public String getResponseType() {
        return responseType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DirectSearchResponse that = (DirectSearchResponse) o;
        return Objects.equals(searchResponse, that.searchResponse) &&
                Objects.equals(pitCreateResponse, that.pitCreateResponse) &&
                Objects.equals(pitDeleteResponse, that.pitDeleteResponse) &&
                Objects.equals(responseType, that.responseType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchResponse, pitCreateResponse, pitDeleteResponse, responseType);
    }

    @Override
    public String toString() {
        return switch (responseType) {
            case "SEARCH" -> searchResponse != null ? searchResponse.toString() : "null";
            case "PIT_CREATE" -> pitCreateResponse != null ? pitCreateResponse.toString() : "null";
            case "PIT_DELETE" -> pitDeleteResponse != null ? pitDeleteResponse.toString() : "null";
            default -> "Unknown response type";
        };
    }
} 