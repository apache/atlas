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
    private SearchParameters.FilterCriteria entityFilters;
    private SearchParameters.FilterCriteria edgeFilters;
    private Set<String>                     attributes;
    private Set<String>                     relationAttributes;

    public enum LineageDirection {INPUT, OUTPUT}

    public LineageListRequest() {
        this.attributes = new HashSet<>();
        this.relationAttributes = new HashSet<>();
    }

    public LineageListRequest(String guid, Integer size, Integer from, Integer depth, LineageDirection direction) {
        this.guid      = guid;
        this.size      = size;
        this.from      = from;
        this.depth     = depth;
        this.direction = direction;
    }

    public LineageListRequest(String guid, Integer size, Integer from, Integer depth, LineageDirection direction,
                              SearchParameters.FilterCriteria entityFilters, SearchParameters.FilterCriteria edgeFilters,
                              Set<String> attributes, Set<String> relationAttributes) {
        this.guid = guid;
        this.size = size;
        this.from = from;
        this.depth = depth;
        this.direction = direction;
        this.entityFilters = entityFilters;
        this.edgeFilters = edgeFilters;
        this.attributes = attributes;
        this.relationAttributes = relationAttributes;
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

    public SearchParameters.FilterCriteria getEntityFilters() {
        return entityFilters;
    }

    public void setEntityFilters(SearchParameters.FilterCriteria entityFilters) {
        this.entityFilters = entityFilters;
    }

    public SearchParameters.FilterCriteria getEdgeFilters() {
        return edgeFilters;
    }

    public void setEdgeFilters(SearchParameters.FilterCriteria edgeFilters) {
        this.edgeFilters = edgeFilters;
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
}
