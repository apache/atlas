package org.apache.atlas.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;

import java.util.Date;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;
import static org.apache.atlas.AtlasConfiguration.CLASSIFICATION_PROPAGATION_DEFAULT;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class Tag {

    int bucket;

    @JsonProperty("id")
    String vertexId;

    @JsonProperty("tag_type_name")
    String tagTypeName;

    @JsonProperty("is_propagated")
    boolean isPropagated;

    @JsonProperty("source_id")
    String sourceVertexId;

    @JsonProperty("updated_at")
    Date updatedAt;

    @JsonProperty("tag_meta_json")
    Map<String, Object> tagMetaJson;

    @JsonProperty("asset_metadata")
    Map<String, Object> assetMetadata;

    private ObjectMapper objectMapper = new ObjectMapper();

    public int getBucket() {
        return bucket;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }

    public String getVertexId() {
        return vertexId;
    }

    public void setVertexId(String vertexId) {
        this.vertexId = vertexId;
    }

    public String getTagTypeName() {
        return tagTypeName;
    }

    public void setTagTypeName(String tagTypeName) {
        this.tagTypeName = tagTypeName;
    }

    public boolean isPropagated() {
        return isPropagated;
    }

    public void setPropagated(boolean propagated) {
        isPropagated = propagated;
    }

    public String getSourceVertexId() {
        return sourceVertexId;
    }

    public void setSourceVertexId(String sourceVertexId) {
        this.sourceVertexId = sourceVertexId;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Date updatedAt) {
        this.updatedAt = updatedAt;
    }

    public Map<String, Object> getAssetMetadata() {
        return assetMetadata;
    }

    public void setAssetMetadata(Map<String, Object> assetMetadata) {
        this.assetMetadata = assetMetadata;
    }

    public Map<String, Object> getTagMetaJson() {
        return tagMetaJson;
    }

    public void setTagMetaJson(Map<String, Object> tagMetaJson) {
        this.tagMetaJson = tagMetaJson;
    }

    public boolean getRestrictPropagationThroughLineage(){
        if (tagMetaJson != null && tagMetaJson.containsKey("restrictPropagationThroughLineage")) {
            return (boolean) tagMetaJson.get("restrictPropagationThroughLineage");
        } else {
            return false;
        }
    }

    public boolean getRestrictPropagationThroughHierarchy() {
        if (tagMetaJson != null && tagMetaJson.containsKey("restrictPropagationThroughHierarchy")) {
            return (boolean) tagMetaJson.get("restrictPropagationThroughHierarchy");
        } else {
            return false;
        }
    }

    public boolean isPropagationEnabled() {
        if (tagMetaJson != null && tagMetaJson.containsKey("propagate")) {
            return (boolean) tagMetaJson.get("propagate");
        } else {
            return true;
        }
    }

    public boolean getRemovePropagationsOnEntityDelete() {
        if (tagMetaJson != null && tagMetaJson.containsKey("removePropagationsOnEntityDelete")) {
            return (boolean) tagMetaJson.get("removePropagationsOnEntityDelete");
        } else {
            return true;
        }
    }

    public AtlasClassification toAtlasClassification() throws AtlasBaseException {
        AtlasClassification classification = objectMapper.convertValue(tagMetaJson, AtlasClassification.class);
        // Set default value is tagMetadataJson fields are null
        if (classification.getRestrictPropagationThroughLineage() == null) {
            classification.setRestrictPropagationThroughLineage(false);
        }
        if (classification.getRestrictPropagationThroughHierarchy() == null) {
            classification.setRestrictPropagationThroughHierarchy(false);
        }
        if (classification.getRemovePropagationsOnEntityDelete() == null) {
            classification.setRemovePropagationsOnEntityDelete(true);
        }
        if (classification.getPropagate() == null) {
            classification.setPropagate(CLASSIFICATION_PROPAGATION_DEFAULT.getBoolean());
        }
        return classification;
    }

    public boolean isPropagatable() {
        // Return True if
        // 1. tag itself is propagated
        // OR
        // 2. tag is a direct attachment with propagate option as true
        return this.isPropagated() || this.isPropagationEnabled();
    }

    @Override
    public String toString() {
        return "Tag{" +
                "bucket=" + bucket +
                ", vertexId='" + vertexId + '\'' +
                ", tagTypeName='" + tagTypeName + '\'' +
                ", isPropagated=" + isPropagated +
                ", sourceVertexId='" + sourceVertexId + '\'' +
                ", updatedAt=" + updatedAt +
                '}';
    }
}
