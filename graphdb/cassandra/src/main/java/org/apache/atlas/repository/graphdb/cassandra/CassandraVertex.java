package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.*;

import java.util.*;
import java.util.stream.StreamSupport;

public class CassandraVertex extends CassandraElement implements AtlasVertex<CassandraVertex, CassandraEdge> {

    private String vertexLabel;

    public CassandraVertex(String id, CassandraGraph graph) {
        super(id, graph);
    }

    public CassandraVertex(String id, Map<String, Object> properties, CassandraGraph graph) {
        super(id, properties, graph);
    }

    public CassandraVertex(String id, String vertexLabel, Map<String, Object> properties, CassandraGraph graph) {
        super(id, properties, graph);
        this.vertexLabel = vertexLabel;
    }

    public String getVertexLabel() {
        return vertexLabel;
    }

    public void setVertexLabel(String vertexLabel) {
        this.vertexLabel = vertexLabel;
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> getEdges(AtlasEdgeDirection direction, String edgeLabel) {
        return graph.getEdgesForVertex(this.id, direction, edgeLabel);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> getEdges(AtlasEdgeDirection direction, String[] edgeLabels) {
        if (edgeLabels == null || edgeLabels.length == 0) {
            return getEdges(direction);
        }

        List<AtlasEdge<CassandraVertex, CassandraEdge>> result = new ArrayList<>();
        for (String label : edgeLabels) {
            Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> edges = getEdges(direction, label);
            for (AtlasEdge<CassandraVertex, CassandraEdge> edge : edges) {
                result.add(edge);
            }
        }
        return result;
    }

    @Override
    public Set<CassandraEdge> getInEdges(String[] edgeLabelsToExclude) {
        Set<String> excludeSet = edgeLabelsToExclude != null
                ? new HashSet<>(Arrays.asList(edgeLabelsToExclude))
                : Collections.emptySet();

        Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> allInEdges = getEdges(AtlasEdgeDirection.IN);
        Set<CassandraEdge> result = new HashSet<>();
        for (AtlasEdge<CassandraVertex, CassandraEdge> edge : allInEdges) {
            if (!excludeSet.contains(edge.getLabel())) {
                result.add((CassandraEdge) edge);
            }
        }
        return result;
    }

    @Override
    public long getEdgesCount(AtlasEdgeDirection direction, String edgeLabel) {
        // Server-side count via CQL COUNT(*) — avoids materialising all edges
        long cassandraCount = graph.countEdgesForVertex(this.id, direction, edgeLabel);

        // Adjust for uncommitted buffer: add new buffered edges, subtract removed buffered edges
        long bufferAdjustment = graph.countBufferedEdgeAdjustment(this.id, direction, edgeLabel);

        return cassandraCount + bufferAdjustment;
    }

    @Override
    public boolean hasEdges(AtlasEdgeDirection dir, String edgeLabel) {
        // Check buffer first — if there's a new buffered edge, return true without hitting Cassandra
        if (graph.hasBufferedEdges(this.id, dir, edgeLabel)) {
            return true;
        }

        // CQL LIMIT 1 check — avoids materialising all edges
        return graph.hasEdgesForVertex(this.id, dir, edgeLabel);
    }

    @Override
    public Iterable<AtlasEdge<CassandraVertex, CassandraEdge>> getEdges(AtlasEdgeDirection direction) {
        return graph.getEdgesForVertex(this.id, direction, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void addProperty(String propertyName, T value) {
        Object existing = properties.get(propertyName);
        if (existing instanceof Set) {
            ((Set<Object>) existing).add(value);
        } else if (existing instanceof Collection) {
            // After Cassandra round-trip, Sets are deserialized as ArrayLists.
            // Convert back to Set and append the new value.
            Set<Object> set = new LinkedHashSet<>((Collection<?>) existing);
            set.add(value);
            properties.put(propertyName, set);
        } else if (existing != null) {
            Set<Object> set = new LinkedHashSet<>();
            set.add(existing);
            set.add(value);
            properties.put(propertyName, set);
        } else {
            Set<Object> set = new LinkedHashSet<>();
            set.add(value);
            properties.put(propertyName, set);
        }
        markDirty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> void addListProperty(String propertyName, T value) {
        Object existing = properties.get(propertyName);
        if (existing instanceof List) {
            ((List<Object>) existing).add(value);
        } else if (existing != null) {
            List<Object> list = new ArrayList<>();
            list.add(existing);
            list.add(value);
            properties.put(propertyName, list);
        } else {
            List<Object> list = new ArrayList<>();
            list.add(value);
            properties.put(propertyName, list);
        }
        markDirty();
    }

    /**
     * Override setProperty to eagerly compute deterministic vertex ID.
     *
     * When both qualifiedName and __typeName are set on a new vertex,
     * the deterministic ID (SHA-256 of typeName + qualifiedName) is computed
     * immediately. This ensures getIdForDisplay() returns the stable ID
     * before commit(), fixing Tags V2 which writes to TagDAO using vertex ID
     * as key during the same transaction as entity creation.
     */
    @Override
    public <T> void setProperty(String propertyName, T value) {
        super.setProperty(propertyName, value);
        if (isNew() && graph.getIdStrategy() == RuntimeIdStrategy.DETERMINISTIC) {
            tryEagerDeterministicId();
        }
    }

    private void tryEagerDeterministicId() {
        String typeName = (String) properties.get("__typeName");
        String qn = (String) properties.get("qualifiedName");
        if (qn == null) {
            qn = (String) properties.get("Referenceable.qualifiedName");
        }
        String identityKey = GraphIdUtil.buildIdentityKey(typeName, qn);
        if (identityKey != null) {
            String deterministicId = GraphIdUtil.deterministicVertexId(identityKey);
            if (!deterministicId.equals(this.id)) {
                String oldId = this.id;
                this.id = deterministicId;
                graph.notifyVertexIdChanged(oldId, deterministicId, this);
            }
        }
    }

    @Override
    public AtlasVertexQuery<CassandraVertex, CassandraEdge> query() {
        return new CassandraVertexQuery(graph, this);
    }

    @Override
    public CassandraVertex getV() {
        return this;
    }

    @Override
    public String toString() {
        return "CassandraVertex{id='" + id + "', label='" + vertexLabel + "'}";
    }
}
