package org.apache.atlas.observability;

import java.util.List;
import java.util.Map;

/**
 * Simple data model for Atlas observability metrics.
 */
public class AtlasObservabilityData {
    
    // Request info
    private String traceId;
    private String xAtlanAgentId;
    private String xAtlanClientOrigin;
    private long timestamp;
    private long duration;
    
    // Payload info
    private int payloadAssetSize;
    private long payloadRequestBytes;
    private List<String> assetGuids;
    private List<String> vertexIds;
    
    // Array relationships
    private Map<String, Integer> relationshipAttributes;
    private Map<String, Integer> appendRelationshipAttributes;
    private Map<String, Integer> removeRelationshipAttributes;
    private int totalArrayRelationships;
    
    // Array attributes (non-relationship arrays)
    private Map<String, Integer> arrayAttributes;
    private int totalArrayAttributes;
    
    // Timing
    private long diffCalcTime;
    private long lineageCalcTime;
    private long validationTime;
    private long ingestionTime;
    private long notificationTime;
    private long auditLogTime;
    
    public AtlasObservabilityData() {
        this.timestamp = System.currentTimeMillis();
    }
    
    public AtlasObservabilityData(String traceId, String xAtlanAgentId, String xAtlanClientOrigin) {
        this();
        this.traceId = traceId;
        this.xAtlanAgentId = xAtlanAgentId;
        this.xAtlanClientOrigin = xAtlanClientOrigin;
    }
    
    // Getters and setters
    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }
    
    public String getXAtlanAgentId() { return xAtlanAgentId; }
    public void setXAtlanAgentId(String xAtlanAgentId) { this.xAtlanAgentId = xAtlanAgentId; }
    
    public String getXAtlanClientOrigin() { return xAtlanClientOrigin; }
    public void setXAtlanClientOrigin(String xAtlanClientOrigin) { this.xAtlanClientOrigin = xAtlanClientOrigin; }
    
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
    public long getDuration() { return duration; }
    public void setDuration(long duration) { this.duration = duration; }
    
    public int getPayloadAssetSize() { return payloadAssetSize; }
    public void setPayloadAssetSize(int payloadAssetSize) { this.payloadAssetSize = payloadAssetSize; }
    
    public long getPayloadRequestBytes() { return payloadRequestBytes; }
    public void setPayloadRequestBytes(long payloadRequestBytes) { this.payloadRequestBytes = payloadRequestBytes; }
    
    public List<String> getAssetGuids() { return assetGuids; }
    public void setAssetGuids(List<String> assetGuids) { this.assetGuids = assetGuids; }
    
    public List<String> getVertexIds() { return vertexIds; }
    public void setVertexIds(List<String> vertexIds) { this.vertexIds = vertexIds; }
    
    public Map<String, Integer> getRelationshipAttributes() { return relationshipAttributes; }
    public void setRelationshipAttributes(Map<String, Integer> relationshipAttributes) { this.relationshipAttributes = relationshipAttributes; }
    
    public Map<String, Integer> getAppendRelationshipAttributes() { return appendRelationshipAttributes; }
    public void setAppendRelationshipAttributes(Map<String, Integer> appendRelationshipAttributes) { this.appendRelationshipAttributes = appendRelationshipAttributes; }
    
    public Map<String, Integer> getRemoveRelationshipAttributes() { return removeRelationshipAttributes; }
    public void setRemoveRelationshipAttributes(Map<String, Integer> removeRelationshipAttributes) { this.removeRelationshipAttributes = removeRelationshipAttributes; }
    
    public int getTotalArrayRelationships() { return totalArrayRelationships; }
    public void setTotalArrayRelationships(int totalArrayRelationships) { this.totalArrayRelationships = totalArrayRelationships; }
    
    public long getDiffCalcTime() { return diffCalcTime; }
    public void setDiffCalcTime(long diffCalcTime) { this.diffCalcTime = diffCalcTime; }
    
    public long getLineageCalcTime() { return lineageCalcTime; }
    public void setLineageCalcTime(long lineageCalcTime) { this.lineageCalcTime = lineageCalcTime; }
    
    public long getValidationTime() { return validationTime; }
    public void setValidationTime(long validationTime) { this.validationTime = validationTime; }
    
    public long getIngestionTime() { return ingestionTime; }
    public void setIngestionTime(long ingestionTime) { this.ingestionTime = ingestionTime; }
    
    public long getNotificationTime() { return notificationTime; }
    public void setNotificationTime(long notificationTime) { this.notificationTime = notificationTime; }
    
    public long getAuditLogTime() { return auditLogTime; }
    public void setAuditLogTime(long auditLogTime) { this.auditLogTime = auditLogTime; }
    
    public Map<String, Integer> getArrayAttributes() { return arrayAttributes; }
    public void setArrayAttributes(Map<String, Integer> arrayAttributes) { this.arrayAttributes = arrayAttributes; }
    
    public int getTotalArrayAttributes() { return totalArrayAttributes; }
    public void setTotalArrayAttributes(int totalArrayAttributes) { this.totalArrayAttributes = totalArrayAttributes; }
}