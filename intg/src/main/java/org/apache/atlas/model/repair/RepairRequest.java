package org.apache.atlas.model.repair;

public class RepairRequest {
    private String qualifiedName;
    private String typeName;

    public RepairRequest() {}

    public RepairRequest(String qualifiedName, String typeName) {
        this.qualifiedName = qualifiedName;
        this.typeName = typeName;
    }

    public String getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(String qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    @Override
    public String toString() {
        return "RepairRequest{" +
                "qualifiedName='" + qualifiedName + '\'' +
                ", typeName='" + typeName + '\'' +
                '}';
    }
}