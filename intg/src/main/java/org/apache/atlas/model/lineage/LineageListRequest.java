package org.apache.atlas.model.lineage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.discovery.SearchParameters;

import java.util.HashSet;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class LineageListRequest {
    private String                          guid;
    private Integer                         size;
    private Integer                         from;
    private Integer                         depth;
    private LineageDirection                direction;
    private SearchParameters.FilterCriteria entityTraversalFilters;
    private SearchParameters.FilterCriteria relationshipTraversalFilters;
    private Set<String>                     attributes;
    private boolean                         fetchProcesses;

    public enum LineageDirection {INPUT, OUTPUT}

    public LineageListRequest() {
        this.attributes = new HashSet<>();
    }

    public LineageListRequest(String guid, Integer size, Integer from, Integer depth, LineageDirection direction) {
        this.guid      = guid;
        this.size      = size;
        this.from      = from;
        this.depth     = depth;
        this.direction = direction;
    }

    public LineageListRequest(String guid, Integer size, Integer from, Integer depth, LineageDirection direction,
                              SearchParameters.FilterCriteria entityTraversalFilters, SearchParameters.FilterCriteria relationshipTraversalFilters,
                              Set<String> attributes, Set<String> relationAttributes, boolean fetchProcesses) {
        this.guid = guid;
        this.size = size;
        this.from = from;
        this.depth = depth;
        this.direction = direction;
        this.entityTraversalFilters = entityTraversalFilters;
        this.relationshipTraversalFilters = relationshipTraversalFilters;
        this.attributes = attributes;
        this.fetchProcesses = fetchProcesses;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public Integer getSize() {
        return size;
    }

    public void setSize(Integer size) {
        this.size = size;
    }

    public Integer getFrom() {
        return from;
    }

    public void setFrom(Integer from) {
        this.from = from;
    }

    public Integer getDepth() {
        return depth;
    }

    public void setDepth(Integer depth) {
        this.depth = depth;
    }

    public LineageDirection getDirection() {
        return direction;
    }

    public void setDirection(LineageDirection direction) {
        this.direction = direction;
    }

    public SearchParameters.FilterCriteria getEntityTraversalFilters() {
        return entityTraversalFilters;
    }

    public void setEntityTraversalFilters(SearchParameters.FilterCriteria entityTraversalFilters) {
        this.entityTraversalFilters = entityTraversalFilters;
    }

    public SearchParameters.FilterCriteria getRelationshipTraversalFilters() {
        return relationshipTraversalFilters;
    }

    public void setRelationshipTraversalFilters(SearchParameters.FilterCriteria relationshipTraversalFilters) {
        this.relationshipTraversalFilters = relationshipTraversalFilters;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public boolean isFetchProcesses() {
        return fetchProcesses;
    }

    public void setFetchProcesses(boolean fetchProcesses) {
        this.fetchProcesses = fetchProcesses;
    }
}
