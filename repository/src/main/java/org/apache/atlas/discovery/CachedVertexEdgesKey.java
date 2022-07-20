package org.apache.atlas.discovery;

import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;

import java.util.Objects;

public class CachedVertexEdgesKey {

    private final Object vertexId;
    private final AtlasEdgeDirection direction;
    private final String edgeLabel;

    public CachedVertexEdgesKey(Object vertexId, AtlasEdgeDirection direction, String edgeLabel) {
        this.vertexId = vertexId;
        this.direction = direction;
        this.edgeLabel = edgeLabel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CachedVertexEdgesKey that = (CachedVertexEdgesKey) o;
        return vertexId.equals(that.vertexId) && direction == that.direction && edgeLabel.equals(that.edgeLabel);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexId, direction, edgeLabel);
    }
}
