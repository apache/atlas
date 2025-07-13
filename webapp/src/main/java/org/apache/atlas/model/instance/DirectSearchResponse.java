package org.apache.atlas.model.instance;

import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchResponse;
import java.util.Map;
import java.util.Objects;

/**
 * Wrapper class to handle different types of Elasticsearch responses.
 * This class encapsulates responses from various Elasticsearch operations
 * including search, PIT creation, and PIT deletion.
 */
public class DirectSearchResponse {
    private final Map<String, Object> rawSearchResponse;
    private final OpenPointInTimeResponse pitCreateResponse;
    private final ClosePointInTimeResponse pitDeleteResponse;
    private final String responseType;

    private DirectSearchResponse(Map<String, Object> rawSearchResponse,
                                 OpenPointInTimeResponse pitCreateResponse,
                                 ClosePointInTimeResponse pitDeleteResponse,
                                 String responseType) {
        this.rawSearchResponse = rawSearchResponse;
        this.pitCreateResponse = pitCreateResponse;
        this.pitDeleteResponse = pitDeleteResponse;
        this.responseType = responseType;
    }

    /**
     * Creates a DirectSearchResponse instance from a raw JSON response map.
     * @param response the raw JSON response as a Map
     * @return a new DirectSearchResponse instance
     */
    public static DirectSearchResponse fromSearchResponse(Map<String, Object> response) {
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

    public Map<String, Object> getRawSearchResponse() {
        return rawSearchResponse;
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
        return Objects.equals(rawSearchResponse, that.rawSearchResponse) &&
                Objects.equals(pitCreateResponse, that.pitCreateResponse) &&
                Objects.equals(pitDeleteResponse, that.pitDeleteResponse) &&
                Objects.equals(responseType, that.responseType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawSearchResponse, pitCreateResponse, pitDeleteResponse, responseType);
    }

    @Override
    public String toString() {
        return switch (responseType) {
            case "SEARCH" -> rawSearchResponse != null ? rawSearchResponse.toString() : "null";
            case "PIT_CREATE" -> pitCreateResponse != null ? pitCreateResponse.toString() : "null";
            case "PIT_DELETE" -> pitDeleteResponse != null ? pitDeleteResponse.toString() : "null";
            default -> "Unknown response type";
        };
    }
} 