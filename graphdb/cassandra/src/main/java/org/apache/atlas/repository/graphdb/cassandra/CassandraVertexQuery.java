package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.*;

import java.util.*;
import java.util.stream.StreamSupport;

public class CassandraVertexQuery implements AtlasVertexQuery<CassandraVertex, CassandraEdge> {

    private final CassandraGraph  graph;
    private final CassandraVertex vertex;
    private AtlasEdgeDirection    direction = AtlasEdgeDirection.BOTH;
    private String[]              labels;
    private final Map<String, Object> hasPredicates = new LinkedHashMap<>();

    public CassandraVertexQuery(CassandraGraph graph, CassandraVertex vertex) {
        this.graph  = graph;
        this.vertex = vertex;
    }

    @Override
    public AtlasVertexQuery<CassandraVertex, CassandraEdge> direction(AtlasEdgeDirection queryDirection) {
        this.direction = queryDirection;
        return this;
    }

    @Override
    public AtlasVertexQuery<CassandraVertex, CassandraEdge> label(String label) {
        this.labels = new String[]{label};
        return this;
    }

    @Override
    public AtlasVertexQuery<CassandraVertex, CassandraEdge> label(String... labels) {
        this.labels = labels;
        return this;
    }

    @Override
    public AtlasVertexQuery<CassandraVertex, CassandraEdge> has(String key, Object value) {
        hasPredicates.put(key, value);
        return this;
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices() {
        return getAdjacentVertices(-1);
    }

    @Override
    public Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> vertices(int limit) {
        return getAdjacentVertices(limit);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges() {
        return getMatchingEdges(-1);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges(int limit) {
        return getMatchingEdges(limit);
    }

    @Override
    public long count() {
        // When there are no has-predicates, use server-side CQL COUNT(*)
        // instead of materialising all edges into Java.
        if (hasPredicates.isEmpty()) {
            long total = 0;
            if (labels != null && labels.length > 0) {
                for (String label : labels) {
                    total += vertex.getEdgesCount(direction, label);
                }
            } else {
                total = vertex.getEdgesCount(direction, null);
            }
            return total;
        }

        // Has-predicates require property inspection — fall back to materialise-and-count
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges = getMatchingEdges(-1);
        return StreamSupport.stream(edges.spliterator(), false).count();
    }

    private Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> getMatchingEdges(int limit) {
        List<AtlasEdge<CassandraVertex, CassandraEdge>> result = new ArrayList<>();

        if (labels != null && labels.length > 0) {
            for (String label : labels) {
                Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges = vertex.getEdges(direction, label);
                for (AtlasEdge<CassandraVertex, CassandraEdge> edge : edges) {
                    if (matchesHasPredicates(edge)) {
                        result.add(edge);
                        if (limit > 0 && result.size() >= limit) {
                            return result;
                        }
                    }
                }
            }
        } else {
            Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges = vertex.getEdges(direction);
            for (AtlasEdge<CassandraVertex, CassandraEdge> edge : edges) {
                if (matchesHasPredicates(edge)) {
                    result.add(edge);
                    if (limit > 0 && result.size() >= limit) {
                        return result;
                    }
                }
            }
        }

        return result;
    }

    private Iterable<AtlasVertex<CassandraVertex, CassandraEdge>> getAdjacentVertices(int limit) {
        List<AtlasVertex<CassandraVertex, CassandraEdge>> result = new ArrayList<>();
        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges = getMatchingEdges(-1);

        for (AtlasEdge<CassandraVertex, CassandraEdge> edge : edges) {
            AtlasVertex<CassandraVertex, CassandraEdge> adjacent;
            if (direction == AtlasEdgeDirection.OUT) {
                adjacent = edge.getInVertex();
            } else if (direction == AtlasEdgeDirection.IN) {
                adjacent = edge.getOutVertex();
            } else {
                // BOTH: return the other vertex
                String vertexId = vertex.getIdString();
                CassandraEdge ce = (CassandraEdge) edge;
                if (ce.getOutVertexId().equals(vertexId)) {
                    adjacent = edge.getInVertex();
                } else {
                    adjacent = edge.getOutVertex();
                }
            }

            if (adjacent != null) {
                result.add(adjacent);
                if (limit > 0 && result.size() >= limit) {
                    break;
                }
            }
        }

        return result;
    }

    private boolean matchesHasPredicates(AtlasEdge<CassandraVertex, CassandraEdge> edge) {
        for (Map.Entry<String, Object> entry : hasPredicates.entrySet()) {
            Object actual = edge.getProperty(entry.getKey(), Object.class);
            if (actual == null || !actual.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }
}
