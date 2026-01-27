package org.apache.atlas.auth.client.heracles.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper class for the Heracles Groups API response.
 * The API returns an object with records array, not a direct array.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HeraclesGroupsResponse {
    
    private List<HeraclesGroupViewRepresentation> records;
    private Integer totalCount;
    private Integer filterCount;
    
    public HeraclesGroupsResponse() {
    }
    
    public List<HeraclesGroupViewRepresentation> getRecords() {
        return records == null ? new ArrayList<>() : records;
    }
    
    public void setRecords(List<HeraclesGroupViewRepresentation> records) {
        this.records = records;
    }
    
    public Integer getTotalCount() {
        return totalCount;
    }
    
    public void setTotalCount(Integer totalCount) {
        this.totalCount = totalCount;
    }
    
    public Integer getFilterCount() {
        return filterCount;
    }
    
    public void setFilterCount(Integer filterCount) {
        this.filterCount = filterCount;
    }
}

