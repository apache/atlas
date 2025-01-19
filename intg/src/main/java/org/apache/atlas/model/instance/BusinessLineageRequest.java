package org.apache.atlas.model.instance;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class BusinessLineageRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<LineageOperation> lineageOperations;

    public List<LineageOperation> getLineageOperations() {
        return lineageOperations;
    }

    public void setLineageOperations(List<LineageOperation> lineageOperations) {
        this.lineageOperations = lineageOperations;
    }

    @Override
    public String toString() {
        return "BusinessLineageRequest{" +
                "lineageOperations=" + lineageOperations +
                '}';
    }

    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class LineageOperation implements Serializable {
        private static final long serialVersionUID = 1L;

        private String workflowId;
        private String assetGuid;
        private String productGuid;
        private OperationType operation;
        private String edgeLabel;

        public String getWorkflowId() {
            return workflowId;
        }

        public void setWorkflowId(String workflowId) {
            this.workflowId = workflowId;
        }

        public String getAssetGuid() {
            return assetGuid;
        }

        public void setAssetGuid(String assetGuid) {
            this.assetGuid = assetGuid;
        }

        public String getProductGuid() {
            return productGuid;
        }

        public void setProductGuid(String productGuid) {
            this.productGuid = productGuid;
        }

        public OperationType getOperation() {
            return operation;
        }

        public void setOperation(OperationType operation) {
            this.operation = operation;
        }

        public String getEdgeLabel() {
            return edgeLabel;
        }

        public void setEdgeLabel(String edgeLabel) {
            this.edgeLabel = edgeLabel;
        }

        @Override
        public String toString() {
            return "LineageOperation{" +
                    "workflowId='" + workflowId + '\'' +
                    ", assetGuid='" + assetGuid + '\'' +
                    ", productGuid='" + productGuid + '\'' +
                    ", operation=" + operation +
                    ", edgeLabel='" + edgeLabel + '\'' +
                    '}';
        }
    }

    public enum OperationType {
        ADD,
        REMOVE
    }
}