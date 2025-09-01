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

package org.apache.atlas.model.tasks;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang.StringUtils;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AtlasTask {
    @JsonIgnore
    public static final int MAX_ATTEMPT_COUNT = 3;

    public String getTagTypeName() {
        return tagTypeName;
    }

    public void setTagTypeName(String tagTypeName) {
        this.tagTypeName = tagTypeName;
    }

    public void setParentEntityGuid(String parentEntityGuid) {
        this.parentEntityGuid = parentEntityGuid;
    }

    public String getParentEntityGuid() {
        return parentEntityGuid;
    }

    private String parentEntityGuid;
    public enum Status {
        PENDING,
        IN_PROGRESS,
        COMPLETE,
        FAILED,
        DELETED;

        public static Status from(String s) {
            if(StringUtils.isEmpty(s)) {
                return PENDING;
            }

            switch (s.toLowerCase()) {
                case "pending":
                    return PENDING;

                case "in_progress":
                    return IN_PROGRESS;

                case "complete":
                    return COMPLETE;

                case "failed":
                    return FAILED;

                case "deleted":
                    return DELETED;

                default:
                    return PENDING;
            }
        }
    }

    private String              type;
    private String              guid;
    private String              createdBy;
    private Date                createdTime;
    private Date                updatedTime;
    private Date                startTime;
    private Date                endTime;
    private Long                timeTakenInSeconds;
    private Map<String, Object> parameters;
    private int                 attemptCount;
    private String              errorMessage;
    private Status              status;
    private String              classificationId;
    private String              entityGuid;
    private String              tagTypeName;
    private Map<String, Object> headers;

    public AtlasTask() {
    }

    public AtlasTask(String type, String createdBy, Map<String, Object> parameters,
                     String classificationId, String tagTypeName,
                     String entityGuid) {
        this.guid               = UUID.randomUUID().toString();
        this.type               = type;
        this.createdBy          = createdBy;
        this.createdTime        = new Date();
        this.updatedTime        = this.createdTime;
        this.parameters         = parameters;
        this.status             = Status.PENDING;
        this.attemptCount       = 0;
        this.classificationId   = classificationId;
        this.entityGuid         = entityGuid;
        this.tagTypeName        = tagTypeName;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Date getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Date createdTime) {
        this.createdTime = createdTime;
    }

    public Date getUpdatedTime() {
        return updatedTime;
    }

    public void setUpdatedTime(Date updatedTime) {
        this.updatedTime = updatedTime;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> val) {
        this.parameters = val;
    }

    public Map<String, Object> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, Object> headers) {
        this.headers = headers;
    }

    public void setType(String val) {
        this.type = val;
    }

    public String getType() {
        return this.type;
    }

    public void setStatus(String val) {
        if (StringUtils.isNotEmpty(val)) {
            this.status = Status.valueOf(val);
        }
    }

    public void setStatus(Status val) {
        this.status = val;
    }

    public Status getStatus() {
        return this.status;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public void incrementAttemptCount() {
        this.attemptCount++;
    }

    public void setStatusPending() {
        this.status = Status.PENDING;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Long getTimeTakenInSeconds() {
        return timeTakenInSeconds;
    }

    public void setTimeTakenInSeconds(Long timeTakenInSeconds) {
        this.timeTakenInSeconds = timeTakenInSeconds;
    }

    public void setClassificationId(String classificationId) {
        this.classificationId = classificationId;
    }

    public String getClassificationId() {
        return classificationId;
    }

    public void setEntityGuid(String entityGuid) {
        this.entityGuid = entityGuid;
    }

    public String getEntityGuid() {
        return entityGuid;
    }

    @JsonIgnore
    public void start() {
        this.setStatus(Status.IN_PROGRESS);
        this.setStartTime(new Date());
    }

    @JsonIgnore
    public void end() {
        this.setEndTime(new Date());
    }

    @JsonIgnore
    public void updateStatusFromAttemptCount() {
        setStatus((attemptCount < MAX_ATTEMPT_COUNT) ? AtlasTask.Status.PENDING : AtlasTask.Status.FAILED);
    }

    @Override
    public String toString() {
        return "AtlasTask{" +
                "type='" + type + '\'' +
                ", guid='" + guid + '\'' +
                ", createdBy='" + createdBy + '\'' +
                ", createdTime=" + createdTime +
                ", updatedTime=" + updatedTime +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", timeTakenInSeconds=" + timeTakenInSeconds +
                ", parameters=" + parameters +
                ", attemptCount=" + attemptCount +
                ", errorMessage='" + errorMessage + '\'' +
                ", status=" + status +
                ", classificationId='" + classificationId + '\'' +
                ", entityGuid='" + entityGuid + '\'' +
                ", tagTypeName='" + tagTypeName + '\'' +
                '}';
    }
}