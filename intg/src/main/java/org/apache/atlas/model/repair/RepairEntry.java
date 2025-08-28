package org.apache.atlas.model.repair;

public class RepairEntry {
    private String qualifiedName;
    private String typeName;
    private Long vertexId;
    private String message;

    public RepairEntry(String qualifiedName, String typeName, Long vertexId, String message) {
        this.qualifiedName = qualifiedName;
        this.typeName = typeName;
        this.vertexId = vertexId;
        this.message = message;
    }

    public String getQualifiedName() { return qualifiedName; }
    public String getTypeName() { return typeName; }
    public Long getVertexId() { return vertexId; }
    public String getMessage() { return message; }
}