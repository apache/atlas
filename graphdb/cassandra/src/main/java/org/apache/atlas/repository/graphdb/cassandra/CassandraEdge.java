package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;

import java.util.Map;

public class CassandraEdge extends CassandraElement implements AtlasEdge<CassandraVertex, CassandraEdge> {

    private final String outVertexId;
    private final String inVertexId;
    private final String label;

    public CassandraEdge(String id, String outVertexId, String inVertexId, String label, CassandraGraph graph) {
        super(id, graph);
        this.outVertexId = outVertexId;
        this.inVertexId  = inVertexId;
        this.label       = label;
    }

    public CassandraEdge(String id, String outVertexId, String inVertexId, String label,
                         Map<String, Object> properties, CassandraGraph graph) {
        super(id, properties, graph);
        this.outVertexId = outVertexId;
        this.inVertexId  = inVertexId;
        this.label       = label;
    }

    @Override
    public AtlasVertex<CassandraVertex, CassandraEdge> getInVertex() {
        return graph.getVertex(inVertexId);
    }

    @Override
    public AtlasVertex<CassandraVertex, CassandraEdge> getOutVertex() {
        return graph.getVertex(outVertexId);
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public CassandraEdge getE() {
        return this;
    }

    public String getOutVertexId() {
        return outVertexId;
    }

    public String getInVertexId() {
        return inVertexId;
    }

    @Override
    public String toString() {
        return "CassandraEdge{id='" + id + "', label='" + label + "', out='" + outVertexId + "', in='" + inVertexId + "'}";
    }
}
