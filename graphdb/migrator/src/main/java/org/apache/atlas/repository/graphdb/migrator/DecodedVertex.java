package org.apache.atlas.repository.graphdb.migrator;

import java.util.*;

/**
 * A vertex decoded from JanusGraph's binary edgestore format.
 * Contains the JG vertex ID, all properties, and all outgoing edges.
 */
public class DecodedVertex {

    private final long jgVertexId;
    private final Map<String, Object> properties;
    private final List<DecodedEdge> outEdges;

    public DecodedVertex(long jgVertexId) {
        this.jgVertexId = jgVertexId;
        this.properties = new LinkedHashMap<>();
        this.outEdges   = new ArrayList<>();
    }

    public long getJgVertexId() {
        return jgVertexId;
    }

    /**
     * The new vertex ID for the target schema: string representation of JG long ID.
     * This preserves a deterministic, reversible mapping without any external lookup.
     */
    public String getVertexId() {
        return String.valueOf(jgVertexId);
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void addProperty(String name, Object value) {
        // Handle multi-valued properties (SET / LIST cardinality)
        Object existing = properties.get(name);
        if (existing != null) {
            if (existing instanceof List) {
                ((List<Object>) existing).add(value);
            } else {
                List<Object> list = new ArrayList<>();
                list.add(existing);
                list.add(value);
                properties.put(name, list);
            }
        } else {
            properties.put(name, value);
        }
    }

    public List<DecodedEdge> getOutEdges() {
        return outEdges;
    }

    public void addOutEdge(DecodedEdge edge) {
        outEdges.add(edge);
    }

    // Convenience accessors for common Atlas properties
    public String getGuid() {
        Object v = properties.get("__guid");
        return v != null ? v.toString() : null;
    }

    public String getTypeName() {
        Object v = properties.get("__typeName");
        return v != null ? v.toString() : null;
    }

    public String getState() {
        Object v = properties.get("__state");
        return v != null ? v.toString() : null;
    }

    public String getVertexLabel() {
        Object v = properties.get("__type");
        return v != null ? v.toString() : null;
    }

    @Override
    public String toString() {
        return "DecodedVertex{jgId=" + jgVertexId +
               ", typeName=" + getTypeName() +
               ", guid=" + getGuid() +
               ", props=" + properties.size() +
               ", edges=" + outEdges.size() + "}";
    }
}
