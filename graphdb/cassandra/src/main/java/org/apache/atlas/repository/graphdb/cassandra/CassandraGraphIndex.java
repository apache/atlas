package org.apache.atlas.repository.graphdb.cassandra;

import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;

import java.util.HashSet;
import java.util.Set;

public class CassandraGraphIndex implements AtlasGraphIndex {

    private final String              name;
    private final boolean             isMixed;
    private final boolean             isComposite;
    private final boolean             isEdgeIndex;
    private final boolean             isVertexIndex;
    private final boolean             isUnique;
    private final Set<AtlasPropertyKey> fieldKeys;

    public CassandraGraphIndex(String name, boolean isMixed, boolean isComposite,
                               boolean isEdgeIndex, boolean isVertexIndex, boolean isUnique) {
        this.name          = name;
        this.isMixed       = isMixed;
        this.isComposite   = isComposite;
        this.isEdgeIndex   = isEdgeIndex;
        this.isVertexIndex = isVertexIndex;
        this.isUnique      = isUnique;
        this.fieldKeys     = new HashSet<>();
    }

    @Override
    public boolean isMixedIndex() {
        return isMixed;
    }

    @Override
    public boolean isCompositeIndex() {
        return isComposite;
    }

    @Override
    public boolean isEdgeIndex() {
        return isEdgeIndex;
    }

    @Override
    public boolean isVertexIndex() {
        return isVertexIndex;
    }

    @Override
    public boolean isUnique() {
        return isUnique;
    }

    @Override
    public Set<AtlasPropertyKey> getFieldKeys() {
        return fieldKeys;
    }

    public String getName() {
        return name;
    }

    public void addFieldKey(AtlasPropertyKey key) {
        fieldKeys.add(key);
    }
}
