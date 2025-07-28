package org.apache.atlas.model;

import org.apache.atlas.model.instance.AtlasClassification;

import java.util.Map;

public class CassandraTagOperation {
    private final String vertexId;
    private final String tagTypeName;
    private final OperationType operationType;
    private final AtlasClassification atlasClassification;
    private final Map<String, Object> minAssetMap;

    public CassandraTagOperation(String vertexId, String tagTypeName, OperationType operationType, AtlasClassification atlasClassification, Map<String, Object> minAssetMap) {
        this.vertexId = vertexId;
        this.tagTypeName = tagTypeName;
        this.operationType = operationType;
        this.atlasClassification = atlasClassification;
        this.minAssetMap = minAssetMap;
    }

    public enum OperationType {
        INSERT,
        UPDATE,
        DELETE
    }

    public String getVertexId() {
        return vertexId;
    }

    public String getTagTypeName() {
        return tagTypeName;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public AtlasClassification getAtlasClassification() {
        return atlasClassification;
    }

    public Map<String, Object> getMinAssetMap() {
        return minAssetMap;
    }

}
