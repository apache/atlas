package org.apache.atlas.discovery;

import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.lineage.LineageListRequest;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.Predicate;
import org.apache.commons.lang3.tuple.Pair;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public final class AtlasLineageListContext {
    private String                              guid;
    private Integer                             size;
    private Integer                             from;
    private Integer                             depth;
    private LineageListRequest.LineageDirection direction;
    private Predicate                           vertexPredicate;
    private Predicate                           edgePredicate;
    private Set<String>                         attributes;
    private Set<String>                         relationAttributes;
    private boolean                             fetchProcesses;

    public AtlasLineageListContext(LineageListRequest lineageListRequest, AtlasTypeRegistry typeRegistry) {
        this.guid = lineageListRequest.getGuid();
        this.size = lineageListRequest.getSize();
        this.from = lineageListRequest.getFrom();
        this.depth = lineageListRequest.getDepth();
        this.direction = lineageListRequest.getDirection();
        this.vertexPredicate = constructInMemoryPredicate(typeRegistry, lineageListRequest.getEntityTraversalFilters());
        this.edgePredicate = constructInMemoryPredicate(typeRegistry, lineageListRequest.getRelationshipTraversalFilters());
        this.attributes = lineageListRequest.getAttributes();
        this.relationAttributes = lineageListRequest.getRelationAttributes();
        this.fetchProcesses = lineageListRequest.isFetchProcesses();
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

    public LineageListRequest.LineageDirection getDirection() {
        return direction;
    }

    public void setDirection(LineageListRequest.LineageDirection direction) {
        this.direction = direction;
    }

    public Predicate getVertexPredicate() {
        return vertexPredicate;
    }

    public void setVertexPredicate(Predicate vertexPredicate) {
        this.vertexPredicate = vertexPredicate;
    }

    public Predicate getEdgePredicate() {
        return edgePredicate;
    }

    public void setEdgePredicate(Predicate edgePredicate) {
        this.edgePredicate = edgePredicate;
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

    public boolean isFetchProcesses() {
        return fetchProcesses;
    }

    public void setFetchProcesses(boolean fetchProcesses) {
        this.fetchProcesses = fetchProcesses;
    }

    protected Predicate constructInMemoryPredicate(AtlasTypeRegistry typeRegistry, SearchParameters.FilterCriteria filterCriteria) {
        LineageSearchProcessor lineageSearchProcessor = new LineageSearchProcessor();
        return lineageSearchProcessor.constructInMemoryPredicate(typeRegistry, filterCriteria);
    }

    protected boolean evaluate(AtlasVertex vertex) {
        if (vertexPredicate != null) {
            return vertexPredicate.evaluate(vertex);
        }
        return true;
    }

    protected boolean evaluate(AtlasEdge edge) {
        if (edgePredicate != null) {
            return edgePredicate.evaluate(edge);
        }
        return true;
    }
}
