package org.apache.atlas.discovery;

import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.lineage.LineageSizeRequest;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.Predicate;

import java.util.HashSet;
import java.util.Set;

public final class AtlasLineageSizeContext {
    private final String                              guid;
    private final LineageSizeRequest.LineageDirection direction;
    private final Predicate                           vertexPredicate;
    private final Predicate                           edgePredicate;
    private final Set<String>                         visited;

    private final Integer                             depth;
    private final Integer                             upperLimit;

    public AtlasLineageSizeContext(LineageSizeRequest lineageSizeRequest, AtlasTypeRegistry typeRegistry) {
        this.guid = lineageSizeRequest.getGuid();
        this.direction = lineageSizeRequest.getDirection();
        this.depth = lineageSizeRequest.getDepth();
        this.upperLimit = lineageSizeRequest.getUpperLimit();
        this.vertexPredicate = constructInMemoryPredicate(typeRegistry, lineageSizeRequest.getEntityTraversalFilters());
        this.edgePredicate = constructInMemoryPredicate(typeRegistry, lineageSizeRequest.getRelationshipTraversalFilters());
        visited = new HashSet<>();
    }

    public String getGuid() {
        return guid;
    }

    public LineageSizeRequest.LineageDirection getDirection() {
        return direction;
    }

    public Set<String> getVisited() {
        return visited;
    }

    public Predicate getVertexPredicate() {
        return vertexPredicate;
    }

    public Predicate getEdgePredicate() {
        return edgePredicate;
    }

    public Integer getDepth() {
        return depth;
    }

    public Integer getUpperLimit() {
        return upperLimit;
    }

    protected Predicate constructInMemoryPredicate(AtlasTypeRegistry typeRegistry, SearchParameters.FilterCriteria filterCriteria) {
        return new LineageSearchProcessor().constructInMemoryPredicate(typeRegistry, filterCriteria);
    }

    protected boolean evaluate(AtlasVertex vertex) {
        if (vertexPredicate != null)
            return vertexPredicate.evaluate(vertex);
        return true;
    }

    protected boolean evaluate(AtlasEdge edge) {
        if (edgePredicate != null)
            return edgePredicate.evaluate(edge);
        return true;
    }
}
