package org.apache.atlas.model;

import java.util.Map;

public class ESDeferredOperation {

    public enum OperationType {TAG_DENORM_FOR_ADD_CLASSIFICATIONS, TAG_DENORM_FOR_DELETE_CLASSIFICATIONS }

    private final OperationType operationType;
    private final String entityId;
    private final Map<String, Map<String, Object>> payload;

    public ESDeferredOperation(OperationType operationType, String entityId, Map<String, Map<String, Object>> payload) {
        this.operationType = operationType;
        this.entityId = entityId;
        this.payload = payload;
    }

    public OperationType getOperationType() { return operationType; }
    public String getEntityId() { return entityId; }
    public Map<String, Map<String, Object>> getPayload() { return payload; }

}