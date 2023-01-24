package org.apache.atlas.model.discovery;

import java.util.Set;

public abstract class SearchParams {

    Set<String> attributes;
    Set<String> relationAttributes;
    Set<String> collapseAttributes;
    Set<String> collapseRelationAttributes;
    boolean showSearchScore;
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
}
