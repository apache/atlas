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

    private final int                                 depth;
    private final int                                 upperLimit;

    private static final int MAX_DEFAULT_DEPTH = 21;
    private static final int MAX_DEFAULT_UPPER_LIMIT = 1000;

    public AtlasLineageSizeContext(LineageSizeRequest lineageSizeRequest, AtlasTypeRegistry typeRegistry) {
        this.guid = lineageSizeRequest.getGuid();
        this.direction = lineageSizeRequest.getDirection();

        if (lineageSizeRequest.getDepth() == null || lineageSizeRequest.getDepth() < 0)
            this.depth = MAX_DEFAULT_DEPTH;
        else
            this.depth = lineageSizeRequest.getDepth();
        if (lineageSizeRequest.getUpperLimit() == null || lineageSizeRequest.getUpperLimit() < 0)
            this.upperLimit = MAX_DEFAULT_UPPER_LIMIT;
        else
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

    public int getDepth() {
        return depth;
    }

    public int getUpperLimit() {
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
