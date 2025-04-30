package org.apache.atlas.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.Date;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

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
}
