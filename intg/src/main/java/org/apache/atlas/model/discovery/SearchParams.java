package org.apache.atlas.model.discovery;

import java.util.Set;

public abstract class SearchParams {

    Set<String> attributes;
    Set<String> relationAttributes;
    boolean showSearchScore;
    boolean showCollapsedResults;
    boolean showCollapsedResultsCount;
    boolean suppressLogs;

    public abstract String getQuery();

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

    public boolean getShowSearchScore() {
        return showSearchScore;
    }

    public void setShowSearchScore(boolean showSearchScore) {
        this.showSearchScore = showSearchScore;
    }

    public boolean getShowCollapsedResults() {
        return showCollapsedResults;
    }

    public void getShowCollapsedResults(boolean showCollapsedResults) {
        this.showCollapsedResults = showCollapsedResults;
    }

    public boolean getShowCollapsedResultsCount() {
        return showCollapsedResultsCount;
    }

    public boolean getSuppressLogs() {
        return suppressLogs;
    }

    public void setSuppressLogs(boolean suppressLogs) {
        this.suppressLogs = suppressLogs;
    }
}
