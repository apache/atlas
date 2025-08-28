package org.apache.atlas.model.repair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
public class BatchRepairRequest implements Serializable {
    private static final long serialVersionUID = 1L;
    private List<RepairRequest> entities;
    private String indexType;  // Optional: "SINGLE", "COMPOSITE", or "AUTO"
    private boolean stopOnError;
    private int maxParallel;

    public BatchRepairRequest() {
        this.entities = new ArrayList<>();
        this.indexType = "AUTO";  // Default to AUTO
        this.stopOnError = false;  // Default to continue on error
        this.maxParallel = 1;  // Default to sequential processing
    }

    public BatchRepairRequest(List<RepairRequest> entities) {
        this();
        this.entities = entities;
    }

    // Getters and Setters
    public List<RepairRequest> getEntities() {
        return entities;
    }

    public void setEntities(List<RepairRequest> entities) {
        this.entities = entities;
    }

    public String getIndexType() {
        return indexType;
    }

    public void setIndexType(String indexType) {
        this.indexType = indexType;
    }

    public boolean isStopOnError() {
        return stopOnError;
    }

    public void setStopOnError(boolean stopOnError) {
        this.stopOnError = stopOnError;
    }

    public int getMaxParallel() {
        return maxParallel;
    }

    public void setMaxParallel(int maxParallel) {
        this.maxParallel = maxParallel;
    }

    // Utility methods
    public void addEntity(String qualifiedName, String typeName) {
        if (entities == null) {
            entities = new ArrayList<>();
        }
        entities.add(new RepairRequest(qualifiedName, typeName));
    }

    public void addEntity(RepairRequest request) {
        if (entities == null) {
            entities = new ArrayList<>();
        }
        entities.add(request);
    }

    public int getEntityCount() {
        return entities != null ? entities.size() : 0;
    }

    public boolean isEmpty() {
        return entities == null || entities.isEmpty();
    }

    @Override
    public String toString() {
        return "BatchRepairRequest{" +
                "entityCount=" + getEntityCount() +
                ", indexType='" + indexType + '\'' +
                ", stopOnError=" + stopOnError +
                ", maxParallel=" + maxParallel +
                '}';
    }
}