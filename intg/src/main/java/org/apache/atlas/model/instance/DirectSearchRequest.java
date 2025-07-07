package org.apache.atlas.model.instance;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Request object for direct Elasticsearch operations.
 * Supports simple search, Point-in-Time (PIT) creation, PIT search, and PIT deletion.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class DirectSearchRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private DirectSearchType searchType;
    private String indexName;
    private Map<String, Object> query;
    private String pitId;

    @JsonProperty("keepAlive") // This will be used for serialization
    @JsonAlias({"keep_alive"})
    private Long keepAlive;
    private List<Object> searchAfter;
    private Integer size;
    private List<Map<String, String>> sort;

    // Default constructor required for JSON deserialization
    public DirectSearchRequest() {
    }

    /**
     * Gets the type of search operation to perform.
     * @return the search type
     */
    public DirectSearchType getSearchType() {
        return searchType;
    }

    public void setSearchType(DirectSearchType searchType) {
        this.searchType = searchType;
    }

    /**
     * Gets the index name for SIMPLE search and PIT_CREATE operations.
     * @return the index name
     */
    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     * Gets the Elasticsearch query DSL as a Map.
     * Required for SIMPLE and PIT_SEARCH operations.
     * @return the query map
     */
    public Map<String, Object> getQuery() {
        return query;
    }

    public void setQuery(Map<String, Object> query) {
        this.query = query;
    }

    /**
     * Gets the Point-in-Time ID.
     * Required for PIT_SEARCH and PIT_DELETE operations.
     * @return the PIT ID
     */
    public String getPitId() {
        return pitId;
    }

    public void setPitId(String pitId) {
        this.pitId = pitId;
    }

    /**
     * Gets the keep-alive duration in milliseconds for PIT operations.
     * @return the keep-alive duration
     */
    public Long getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(Long keepAlive) {
        this.keepAlive = keepAlive;
    }

    /**
     * Gets the search_after values for pagination.
     * @return the search_after values
     */
    public List<Object> getSearchAfter() {
        return searchAfter;
    }

    public void setSearchAfter(List<Object> searchAfter) {
        this.searchAfter = searchAfter;
    }

    /**
     * Gets the size parameter for limiting results.
     * @return the size
     */
    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    /**
     * Gets the sort configuration.
     * Each map in the list should have one entry: field -> "asc"/"desc"
     * @return the sort configuration
     */
    public List<Map<String, String>> getSort() {
        return sort;
    }

    public void setSort(List<Map<String, String>> sort) {
        this.sort = sort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DirectSearchRequest that = (DirectSearchRequest) o;
        return Objects.equals(searchType, that.searchType) &&
                Objects.equals(indexName, that.indexName) &&
                Objects.equals(query, that.query) &&
                Objects.equals(pitId, that.pitId) &&
                Objects.equals(keepAlive, that.keepAlive) &&
                Objects.equals(searchAfter, that.searchAfter) &&
                Objects.equals(size, that.size) &&
                Objects.equals(sort, that.sort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchType, indexName, query, pitId, keepAlive, searchAfter, size, sort);
    }

    @Override
    public String toString() {
        return "DirectSearchRequest{" +
                "searchType=" + searchType +
                ", indexName='" + indexName + '\'' +
                ", query=" + query +
                ", pitId='" + pitId + '\'' +
                ", keepAlive=" + keepAlive +
                ", searchAfter=" + searchAfter +
                ", size=" + size +
                ", sort=" + sort +
                '}';
    }
}





