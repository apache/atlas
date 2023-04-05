package org.apache.atlas.model.lineage;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.discovery.SearchParameters;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class LineageSizeRequest {
    private String                          guid;
    private Integer                         depth;
    private Integer                         upperLimit;
    private LineageDirection                direction;
    private SearchParameters.FilterCriteria entityTraversalFilters;
    private SearchParameters.FilterCriteria relationshipTraversalFilters;

    public enum LineageDirection {INPUT, OUTPUT}

    public LineageSizeRequest(String guid, Integer depth, Integer upperLimit, LineageDirection direction,
                              SearchParameters.FilterCriteria entityTraversalFilters, SearchParameters.FilterCriteria relationshipTraversalFilters) {
        this.guid = guid;
        this.depth = depth;
        this.upperLimit = upperLimit;
        this.direction = direction;
        this.entityTraversalFilters = entityTraversalFilters;
        this.relationshipTraversalFilters = relationshipTraversalFilters;
    }

    public LineageSizeRequest() {}

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
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

    public Integer getUpperLimit() {
        return upperLimit;
    }

    public void setUpperLimit(Integer upperLimit) {
        this.upperLimit = upperLimit;
    }

    @Override
    public String toString() {
        return "LineageSizeRequest{" +
                "guid='" + guid + '\'' +
                ", depth=" + depth +
                ", upperLimit=" + upperLimit +
                ", direction=" + direction +
                ", entityTraversalFilters=" + entityTraversalFilters +
                ", relationshipTraversalFilters=" + relationshipTraversalFilters +
                '}';
    }
}