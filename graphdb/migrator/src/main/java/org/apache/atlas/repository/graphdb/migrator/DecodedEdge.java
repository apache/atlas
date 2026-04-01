package org.apache.atlas.repository.graphdb.migrator;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An edge decoded from JanusGraph's binary edgestore format.
 * Contains the JG edge relation ID, source/target vertex IDs, label, and properties.
 */
public class DecodedEdge {

    private final long   jgRelationId;
    private final long   outVertexJgId;
    private final long   inVertexJgId;
    private final String label;
    private final Map<String, Object> properties;

    public DecodedEdge(long jgRelationId, long outVertexJgId, long inVertexJgId, String label) {
        this.jgRelationId  = jgRelationId;
        this.outVertexJgId = outVertexJgId;
        this.inVertexJgId  = inVertexJgId;
        this.label         = label;
        this.properties    = new LinkedHashMap<>();
    }

    public long getJgRelationId() {
        return jgRelationId;
    }

    /** Edge ID for target schema: string of JG relation ID */
    public String getEdgeId() {
        return String.valueOf(jgRelationId);
    }

    public long getOutVertexJgId() {
        return outVertexJgId;
    }

    public String getOutVertexId() {
        return String.valueOf(outVertexJgId);
    }

    public long getInVertexJgId() {
        return inVertexJgId;
    }

    public String getInVertexId() {
        return String.valueOf(inVertexJgId);
    }

    public String getLabel() {
        return label;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void addProperty(String name, Object value) {
        properties.put(name, value);
    }

    @Override
    public String toString() {
        return "DecodedEdge{id=" + jgRelationId +
               ", " + outVertexJgId + " -[" + label + "]-> " + inVertexJgId +
               ", props=" + properties.size() + "}";
    }
}
