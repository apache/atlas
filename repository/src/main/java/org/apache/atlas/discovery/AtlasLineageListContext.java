package org.apache.atlas.discovery;

import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.lineage.LineageListRequest;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.Predicate;
import java.util.Set;

public final class AtlasLineageListContext {
    private String                              guid;
    private Integer                             size;
    private Integer                             from;
    private Integer                             depth;
    private LineageListRequest.LineageDirection direction;
    private Predicate                           vertexPredicate;
    private Predicate                           vertexTraversalPredicate;
    private Predicate                           edgeTraversalPredicate;
    private Set<String>                         attributes;
    private int                                 currentFromCounter;
    private int                                 currentEntityCounter;
    private boolean                             depthLimitReached;
    private boolean                             hasMoreUpdated;

    public AtlasLineageListContext(LineageListRequest lineageListRequest, AtlasTypeRegistry typeRegistry) {
        this.guid = lineageListRequest.getGuid();
        this.size = lineageListRequest.getSize();
        this.from = lineageListRequest.getFrom();
        this.depth = lineageListRequest.getDepth();
        this.direction = lineageListRequest.getDirection();
        this.vertexPredicate = constructInMemoryPredicate(typeRegistry, lineageListRequest.getEntityFilters());
        this.vertexTraversalPredicate = constructInMemoryPredicate(typeRegistry, lineageListRequest.getEntityTraversalFilters());
        this.edgeTraversalPredicate = constructInMemoryPredicate(typeRegistry, lineageListRequest.getRelationshipTraversalFilters());
        this.attributes = lineageListRequest.getAttributes();
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

    /*
    * Clients assume depth limit at node level
    *  eg. Atlas depth 1 would return processes (BFS algo) and depth 2 would return processes + nodes, whereas client needs processes + nodes for depth 1
    */
    public Integer getDepth() {
        return 2*depth;
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

    public Predicate getVertexTraversalPredicate() {
        return vertexTraversalPredicate;
    }

    public void setVertexTraversalPredicate(Predicate vertexTraversalPredicate) {
        this.vertexTraversalPredicate = vertexTraversalPredicate;
    }

    public Predicate getEdgeTraversalPredicate() {
        return edgeTraversalPredicate;
    }

    public void setEdgeTraversalPredicate(Predicate edgeTraversalPredicate) {
        this.edgeTraversalPredicate = edgeTraversalPredicate;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public int getCurrentFromCounter() {
        return currentFromCounter;
    }

    public void setCurrentFromCounter(int currentFromCounter) {
        this.currentFromCounter = currentFromCounter;
    }

    public int getCurrentEntityCounter() {
        return currentEntityCounter;
    }

    public void setCurrentEntityCounter(int currentEntityCounter) {
        this.currentEntityCounter = currentEntityCounter;
    }

    protected Predicate constructInMemoryPredicate(AtlasTypeRegistry typeRegistry, SearchParameters.FilterCriteria filterCriteria) {
        LineageSearchProcessor lineageSearchProcessor = new LineageSearchProcessor();
        return lineageSearchProcessor.constructInMemoryPredicate(typeRegistry, filterCriteria);
    }

    protected boolean evaluateVertexFilter(AtlasVertex vertex) {
        if (vertexPredicate != null) {
            return vertexPredicate.evaluate(vertex);
        }
        return true;
    }

    protected boolean evaluateTraversalFilter(AtlasVertex vertex) {
        if (vertexTraversalPredicate != null) {
            return vertexTraversalPredicate.evaluate(vertex);
        }
        return true;
    }

    protected boolean evaluateTraversalFilter(AtlasEdge edge) {
        if (edgeTraversalPredicate != null) {
            return edgeTraversalPredicate.evaluate(edge);
        }
        return true;
    }

    public void incrementCurrentFromCounter() {
        this.currentFromCounter++;
    }

    public boolean isEntityLimitReached() {
        return this.currentEntityCounter == this.size;
    }

    public void incrementEntityCount() {
        this.currentEntityCounter++;
    }

    public boolean isDepthLimitReached() {
        return depthLimitReached;
    }

    public void setDepthLimitReached(boolean depthLimitReached) {
        this.depthLimitReached = depthLimitReached;
    }

    public boolean isHasMoreUpdated() {
        return hasMoreUpdated;
    }

    public void setHasMoreUpdated(boolean hasMoreUpdated) {
        this.hasMoreUpdated = hasMoreUpdated;
    }
}
