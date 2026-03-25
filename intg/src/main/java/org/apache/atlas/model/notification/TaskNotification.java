/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.notification;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.PROPERTY)
public class TaskNotification implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Status {
        STARTED,
        COMPLETED,
        FAILED
    }

    private String              taskId;
    private String              taskType;
    private Status              status;
    private String              entityGuid;
    private String              classificationName;
    private int                 assetsAffected;
    private long                startTime;
    private long                endTime;
    private String              errorMessage;
    private Boolean             restrictPropagationThroughLineage;
    private Boolean             restrictPropagationThroughHierarchy;
    private Map<String, Object> parameters;

    public TaskNotification() {
    }

    public TaskNotification(String taskId, String taskType, Status status) {
        this.taskId   = taskId;
        this.taskType = taskType;
        this.status   = status;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskType() {
        return taskType;
    }

    public void setTaskType(String taskType) {
        this.taskType = taskType;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getEntityGuid() {
        return entityGuid;
    }

    public void setEntityGuid(String entityGuid) {
        this.entityGuid = entityGuid;
    }

    public String getClassificationName() {
        return classificationName;
    }

    public void setClassificationName(String classificationName) {
        this.classificationName = classificationName;
    }

    public int getAssetsAffected() {
        return assetsAffected;
    }

    public void setAssetsAffected(int assetsAffected) {
        this.assetsAffected = assetsAffected;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
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

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public void setParameter(String key, Object value) {
        if (this.parameters == null) {
            this.parameters = new HashMap<>();
        }
        this.parameters.put(key, value);
    }

    public void normalize() { }

    @Override
    public String toString() {
        return "TaskNotification{" +
                "taskId='" + taskId + '\'' +
                ", taskType='" + taskType + '\'' +
                ", status=" + status +
                ", entityGuid='" + entityGuid + '\'' +
                ", classificationName='" + classificationName + '\'' +
                ", assetsAffected=" + assetsAffected +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", errorMessage='" + errorMessage + '\'' +
                ", restrictPropagationThroughLineage=" + restrictPropagationThroughLineage +
                ", restrictPropagationThroughHierarchy=" + restrictPropagationThroughHierarchy +
                '}';
    }
}
