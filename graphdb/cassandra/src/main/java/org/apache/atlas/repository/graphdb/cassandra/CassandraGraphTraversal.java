package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.*;

import java.util.*;
import java.util.function.BiPredicate;

/**
 * Graph traversal implementation for Cassandra backend.
 * Replaces Gremlin traversals with direct vertex/edge queries.
 */
public class CassandraGraphTraversal extends AtlasGraphTraversal<AtlasVertex, AtlasEdge> {

    private final CassandraGraph graph;
    private final List<AtlasVertex> vertexResults = new ArrayList<>();
    private final List<AtlasEdge> edgeResults = new ArrayList<>();

    // For V(vertexIds...) start step
    public CassandraGraphTraversal(CassandraGraph graph, Object... vertexIds) {
        super(); // no-arg DefaultGraphTraversal
        this.graph = graph;

        if (vertexIds != null) {
            for (Object id : vertexIds) {
                AtlasVertex v = graph.getVertex(String.valueOf(id));
                if (v != null) {
                    vertexResults.add(v);
                }
            }
        }
    }

    // For E(edgeIds...) start step
    public CassandraGraphTraversal(CassandraGraph graph, boolean isEdgeTraversal, Object... edgeIds) {
        super(); // no-arg DefaultGraphTraversal
        this.graph = graph;

        if (edgeIds != null) {
            for (Object id : edgeIds) {
                AtlasEdge e = graph.getEdge(String.valueOf(id));
                if (e != null) {
                    edgeResults.add(e);
                }
            }
        }
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> startAnonymousTraversal() {
        return new CassandraGraphTraversal(graph);
    }

    @Override
    public List<AtlasVertex> getAtlasVertexList() {
        return vertexResults;
    }

    @Override
    public Set<AtlasVertex> getAtlasVertexSet() {
        return new LinkedHashSet<>(vertexResults);
    }

    @Override
    public Map<String, Collection<AtlasVertex>> getAtlasVertexMap() {
        Map<String, Collection<AtlasVertex>> result = new LinkedHashMap<>();
        for (AtlasVertex v : vertexResults) {
            String id = String.valueOf(v.getId());
            result.computeIfAbsent(id, k -> new ArrayList<>()).add(v);
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<AtlasEdge> getAtlasEdgeSet() {
        return new LinkedHashSet<>(edgeResults);
    }

    @Override
    public Map<String, AtlasEdge> getAtlasEdgeMap() {
        Map<String, AtlasEdge> result = new LinkedHashMap<>();
        for (AtlasEdge e : edgeResults) {
            result.put(String.valueOf(e.getId()), e);
        }
        return result;
    }

    @Override
    public TextPredicate textPredicate() {
        return new CassandraTextPredicate();
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> textRegEx(String key, String value) {
        return this;
    }

    @Override
    public AtlasGraphTraversal<AtlasVertex, AtlasEdge> textContainsRegEx(String value, String removeRedundantQuotes) {
        return this;
    }

    private static class CassandraTextPredicate implements TextPredicate {
        @Override
        public BiPredicate contains() {
            return (val, pattern) -> String.valueOf(val).toLowerCase().contains(String.valueOf(pattern).toLowerCase());
        }

        @Override
        public BiPredicate containsPrefix() {
            return (val, pattern) -> String.valueOf(val).toLowerCase().startsWith(String.valueOf(pattern).toLowerCase());
        }

        @Override
        public BiPredicate containsRegex() {
            return (val, pattern) -> String.valueOf(val).matches(String.valueOf(pattern));
        }

        @Override
        public BiPredicate prefix() {
            return (val, pattern) -> String.valueOf(val).startsWith(String.valueOf(pattern));
        }

        @Override
        public BiPredicate regex() {
            return (val, pattern) -> String.valueOf(val).matches(String.valueOf(pattern));
        }
    }
}
