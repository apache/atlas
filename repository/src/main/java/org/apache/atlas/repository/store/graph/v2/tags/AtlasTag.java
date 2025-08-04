package org.apache.atlas.repository.store.graph.v2.tags;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a tag in Atlas with its metadata
 */
public class AtlasTag {
    @JsonProperty("__typeName")
    private String typeName;

    @JsonProperty("__entityGuid")
    private String entityGuid;

    @JsonProperty("__timestamp")
    private Long timestamp;

    @JsonProperty("__modificationTimestamp")
    private Long modificationTimestamp;

    @JsonProperty("__createdBy")
    private String createdBy;

    @JsonProperty("__modifiedBy")
    private String modifiedBy;

    @JsonProperty("__propagate")
    private Boolean propagate;

    @JsonProperty("__removePropagations")
    private Boolean removePropagations;

    @JsonProperty("__restrictPropagationThroughLineage")
    private Boolean restrictPropagationThroughLineage;

    @JsonProperty("__restrictPropagationThroughHierarchy")
    private Boolean restrictPropagationThroughHierarchy;

    private String tagTypeName;

    // Getters and Setters
    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public String getEntityGuid() {
        return entityGuid;
    }

    public void setEntityGuid(String entityGuid) {
        this.entityGuid = entityGuid;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getModificationTimestamp() {
        return modificationTimestamp;
    }

    public void setModificationTimestamp(Long modificationTimestamp) {
        this.modificationTimestamp = modificationTimestamp;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public String getModifiedBy() {
        return modifiedBy;
    }

    public void setModifiedBy(String modifiedBy) {
        this.modifiedBy = modifiedBy;
    }

    public Boolean getPropagate() {
        return propagate;
    }

    public void setPropagate(Boolean propagate) {
        this.propagate = propagate;
    }

    public Boolean getRemovePropagations() {
        return removePropagations;
    }

    public void setRemovePropagations(Boolean removePropagations) {
        this.removePropagations = removePropagations;
    }

    public Boolean getRestrictPropagationThroughLineage() {
        return restrictPropagationThroughLineage;
    }

    public void setRestrictPropagationThroughLineage(Boolean restrictPropagationThroughLineage) {
        this.restrictPropagationThroughLineage = restrictPropagationThroughLineage;
    }

    public Boolean getRestrictPropagationThroughHierarchy() {
        return restrictPropagationThroughHierarchy;
    }

    public void setRestrictPropagationThroughHierarchy(Boolean restrictPropagationThroughHierarchy) {
        this.restrictPropagationThroughHierarchy = restrictPropagationThroughHierarchy;
    }

    public String getTagTypeName() {
        return tagTypeName;
    }

    public void setTagTypeName(String tagTypeName) {
        this.tagTypeName = tagTypeName;
    }
}