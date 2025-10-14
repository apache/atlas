package org.apache.atlas.model.notification;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonSerialize(include=JsonSerialize.Inclusion.ALWAYS)
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class AtlasDistributedTaskNotification {
    public enum AtlasTaskType {
        CLEANUP_ARCHIVED_RELATIONSHIPS,
        CALCULATE_HAS_LINEAGE
    }
    protected AtlasTaskType taskType;
    protected Map<String, Object> parameters;

    public AtlasDistributedTaskNotification() {
    }

    public AtlasDistributedTaskNotification(AtlasTaskType taskType, Map<String, Object> parameters) {
        this.taskType = taskType;
        this.parameters = parameters;
    }

    public AtlasTaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(AtlasTaskType taskType) {
        this.taskType = taskType;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public void setParameter(String key, Object value) {
        if (parameters != null) {
            parameters.put(key, value);
        }
    }

    public void normalize() { }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("AtlasDistributedTaskNotification{");
        sb.append("taskType=").append(taskType);
        sb.append(", parameters=").append(parameters);
        sb.append('}');

        return sb;
    }

    public String toString() {
        return toString(new StringBuilder()).toString();
    }
}
