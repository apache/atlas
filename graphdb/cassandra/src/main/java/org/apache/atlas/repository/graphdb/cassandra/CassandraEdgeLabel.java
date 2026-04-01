package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasEdgeLabel;

public class CassandraEdgeLabel implements AtlasEdgeLabel {

    private final String name;

    public CassandraEdgeLabel(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
