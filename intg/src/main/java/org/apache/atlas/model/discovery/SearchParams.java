package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Set;


@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown=true)
public class SearchParams {

    Set<String> attributes;
    Set<String> relationAttributes;
    Set<String> collapseAttributes;
    Set<String> collapseRelationAttributes;
    boolean showSearchScore;
    boolean suppressLogs;
    boolean excludeMeanings;
    boolean excludeClassifications;
    boolean enableFullRestriction;

    boolean includeClassificationNames = false;

    RequestMetadata requestMetadata = new RequestMetadata();

    Async async = new Async();
    boolean showHighlights;

    boolean showSearchMetadata;

    boolean includeSourceInResults;

    public boolean isIncludeSourceInResults() {
        return includeSourceInResults;
    }

    public void setIncludeSourceInResults(boolean includeSourceInResults) {
        this.includeSourceInResults = includeSourceInResults;
    }

    public String getQuery() {
        return getQuery();
    }

    public boolean getEnableFullRestriction() {
        return enableFullRestriction;
    }

    public void setEnableFullRestriction(boolean enableFullRestriction) {
        this.enableFullRestriction = enableFullRestriction;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public Set<String> getRelationAttributes() {
        return relationAttributes;
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }

    public Set<String> getCollapseAttributes() {
        return collapseAttributes;
    }

    public void setCollapseAttributes(Set<String> collapseAttributes) {
        this.collapseAttributes = collapseAttributes;
    }

    public Set<String> getCollapseRelationAttributes() {
        return collapseRelationAttributes;
    }

    public void setCollapseRelationAttributes(Set<String> collapseRelationAttributes) {
        this.collapseRelationAttributes = collapseRelationAttributes;
    }

    public Set<String> getUtmTags() {
        return requestMetadata.utmTags;
    }

    public void setUtmTags(Set<String> utmTags) {
        this.requestMetadata.utmTags = utmTags;
    }

    public boolean getShowSearchScore() {
        return showSearchScore;
    }

    public void setShowSearchScore(boolean showSearchScore) {
        this.showSearchScore = showSearchScore;
    }

    public boolean getSuppressLogs() {
        return suppressLogs;
    }

    public void setSuppressLogs(boolean suppressLogs) {
        this.suppressLogs = suppressLogs;
    }

    public boolean isExcludeClassifications() {
        return excludeClassifications;
    }

    public void setExcludeClassifications(boolean excludeClassifications) {
        this.excludeClassifications = excludeClassifications;
    }

    public boolean isExcludeMeanings() {
        return excludeMeanings;
    }

    public void setExcludeMeanings(boolean excludeMeanings) {
        this.excludeMeanings = excludeMeanings;
    }

    public boolean isIncludeClassificationNames() {
        return includeClassificationNames;
    }

    public void setIncludeClassificationNames(boolean includeClassificationNames) {
        this.includeClassificationNames = includeClassificationNames;
    }

    public boolean isSaveSearchLog() {
        return requestMetadata.saveSearchLog;
    }

    public void setSaveSearchLog(boolean saveSearchLog) {
        this.requestMetadata.saveSearchLog = saveSearchLog;
    }

    public RequestMetadata getRequestMetadata() {
        return requestMetadata;
    }

    public String getRequestMetadataPersona() {
        return requestMetadata.getPersona();
    }

    public void setRequestMetadata(RequestMetadata requestMetadata) {
        this.requestMetadata = requestMetadata;
    }

    public void setQuery(String query) {
        setQuery(query);
    }

    public Async getAsync() {
        return async;
    }

    public void setAsync(Async async) {
        this.async = async;
    }

    public boolean isCallAsync() {
        return async.getIsCallAsync();
    }

    public String getSearchContextId() {
        return async.getSearchContextId();
    }

    public Integer getSearchContextSequenceNo() {
        return async.getSearchContextSequenceNo();
    }

    public Long getRequestTimeoutInSecs() {
        return async.getRequestTimeoutInSecs();
    }

    public String getSearchInput() {
        return this.requestMetadata.getSearchInput();
    }

    public boolean getShowHighlights() {
        return showHighlights;
    }

    public boolean getShowSearchMetadata() {
        return showSearchMetadata;
    }


    static class RequestMetadata {
        private String searchInput;
        private Set<String> utmTags;
        private boolean saveSearchLog;

        private String persona;

        public String getSearchInput() {
            return searchInput;
        }

        public Set<String> getUtmTags() {
            return utmTags;
        }

        public boolean isSaveSearchLog() {
            return saveSearchLog;
        }

        public void setSearchInput(String searchInput) {
            this.searchInput = searchInput;
        }

        public void setUtmTags(Set<String> utmTags) {
            this.utmTags = utmTags;
        }

        public void setSaveSearchLog(boolean saveSearchLog) {
            this.saveSearchLog = saveSearchLog;
        }

        public String getPersona() {
            return persona;
        }

        public void setPersona(String persona) {
            this.persona = persona;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown=true)
    static class Async {
        private boolean isCallAsync;

        private String searchContextId;

        private Integer searchContextSequenceNo;

        private Long requestTimeoutInSecs;

        public boolean getIsCallAsync() {
            return isCallAsync;
        }

        public String getSearchContextId() {
            return searchContextId;
        }

        public Integer getSearchContextSequenceNo() {
            return searchContextSequenceNo;
        }

        public Long getRequestTimeoutInSecs() {
            return requestTimeoutInSecs;
        }
    }


}
