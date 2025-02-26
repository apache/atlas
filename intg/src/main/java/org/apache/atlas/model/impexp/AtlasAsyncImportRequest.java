/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.impexp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.AtlasBaseModelObject;
import org.apache.atlas.utils.AtlasEntityUtil;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AtlasAsyncImportRequest extends AtlasBaseModelObject implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final String ASYNC_IMPORT_TYPE_NAME = "__AtlasAsyncImportRequest";
    public static final String ASYNC_IMPORT_TOPIC_PREFIX = "ATLAS_IMPORT_";
    public static final String REQUEST_ID_PREFIX_PROPERTY = "async_import_";

    public enum ImportStatus {
        STAGING("STAGING"),
        WAITING("WAITING"),
        PROCESSING("PROCESSING"),
        SUCCESSFUL("SUCCESSFUL"),
        PARTIAL_SUCCESS("PARTIAL_SUCCESS"),
        ABORTED("ABORTED"),
        FAILED("FAILED");

        private final String status;

        ImportStatus(String status) {
            this.status = status;
        }

        public String getStatus() {
            return status;
        }

        @Override
        public String toString() {
            return status;
        }
    }

    @JsonIgnore
    private String requestId;

    @JsonProperty("importId")
    private String importId;

    @JsonProperty("status")
    private ImportStatus status;

    @JsonIgnore
    private int skipTo;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("atlasImportResult")
    private AtlasImportResult atlasImportResult;

    @JsonProperty("importDetails")
    private ImportDetails importDetails = new ImportDetails();

    @JsonProperty("receivedAt")
    private long receivedAt;

    @JsonProperty("stagedAt")
    private long stagedAt;

    @JsonProperty("startedProcessingAt")
    private long startedProcessingAt;

    @JsonProperty("completedAt")
    private long completedAt;

    public AtlasAsyncImportRequest() {}

    public AtlasAsyncImportRequest(String guid) {
        setGuid(guid);
    }

    public AtlasAsyncImportRequest(AtlasImportResult result) {
        this.atlasImportResult           = result;
        this.status                      = ImportStatus.STAGING;
        this.skipTo                      = 0;
        this.receivedAt                  = 0L;
        this.stagedAt                    = 0L;
        this.startedProcessingAt         = 0L;
        this.completedAt                 = 0L;
        this.importDetails               = new ImportDetails();
        setGuid(getGuid());
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getImportId() {
        return importId;
    }

    public void setImportId(String importId) {
        this.importId = importId;
        setRequestId(REQUEST_ID_PREFIX_PROPERTY + importId + "@" + AtlasEntityUtil.getMetadataNamespace());
    }

    public ImportStatus getStatus() {
        return status;
    }

    public void setStatus(ImportStatus status) {
        this.status = status;
    }

    public ImportDetails getImportDetails() {
        return importDetails;
    }

    public void setImportDetails(ImportDetails importDetails) {
        this.importDetails = importDetails;
    }

    @JsonIgnore
    public String getTopicName() {
        return ASYNC_IMPORT_TOPIC_PREFIX + importId;
    }

    public int getSkipTo() {
        return skipTo;
    }

    public void setSkipTo(int skipTo) {
        this.skipTo = skipTo;
    }

    public AtlasImportResult getAtlasImportResult() {
        return atlasImportResult;
    }

    public void setAtlasImportResult(AtlasImportResult atlasImportResult) {
        this.atlasImportResult = atlasImportResult;
    }

    public long getReceivedAt() {
        return receivedAt;
    }

    public void setReceivedAt(long receivedAt) {
        this.receivedAt = receivedAt;
    }

    public long getStagedAt() {
        return stagedAt;
    }

    public void setStagedAt(long stagedAt) {
        this.stagedAt = stagedAt;
    }

    public long getStartedProcessingAt() {
        return startedProcessingAt;
    }

    public void setStartedProcessingAt(long startedProcessingAt) {
        this.startedProcessingAt = startedProcessingAt;
    }

    public long getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(long completedAt) {
        this.completedAt = completedAt;
    }

    @JsonIgnore
    public Map<String, Object> getImportMinInfo() {
        Map<String, Object> minInfoResponse = new HashMap<>();

        String importId = this.getImportId();
        long timestamp = this.receivedAt;
        String isoDate = toIsoDate(new Date(timestamp));

        minInfoResponse.put("importId", importId);
        minInfoResponse.put("status", status);
        minInfoResponse.put("importRequestReceivedAt", isoDate);
        minInfoResponse.put("importRequestReceivedBy", atlasImportResult.getUserName());

        return minInfoResponse;
    }

    private String toIsoDate(Date value) {
        final TimeZone tz = TimeZone.getTimeZone("UTC");
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        df.setTimeZone(tz);
        return df.format(value);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AtlasAsyncImportRequest)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        AtlasAsyncImportRequest that = (AtlasAsyncImportRequest) o;
        return Objects.equals(atlasImportResult, that.atlasImportResult) &&
                Objects.equals(importId, that.importId) &&
                Objects.equals(status, that.status) &&
                Objects.equals(importDetails, that.importDetails) &&
                Objects.equals(requestId, that.requestId) &&
                Objects.equals(receivedAt, that.receivedAt) &&
                Objects.equals(stagedAt, that.stagedAt) &&
                Objects.equals(startedProcessingAt, that.startedProcessingAt) &&
                Objects.equals(completedAt, that.completedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), requestId, atlasImportResult,
                importId, status, importDetails, receivedAt, stagedAt, startedProcessingAt, completedAt);
    }

    @Override
    protected StringBuilder toString(StringBuilder sb) {
        sb.append(", atlasImportResult=");
        if (atlasImportResult == null) {
            sb.append("null");
        } else {
            sb.append(atlasImportResult);
        }
        sb.append(", requestId=").append(requestId);
        sb.append(", importId=").append(importId);
        sb.append(", status=").append(status);
        sb.append(", receivedAt=").append(receivedAt);
        sb.append(", stagedAt=").append(stagedAt);
        sb.append(", startedProcessingAt=").append(startedProcessingAt);
        sb.append(", completedAt=").append(completedAt);
        sb.append(", importDetails=").append(importDetails);

        return sb;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ImportDetails {
        private int publishedEntityCount;
        private int totalEntitiesCount;
        private int importedEntitiesCount;
        private int failedEntitiesCount;
        private List<String> failedEntities = new ArrayList<>();
        private float importProgress;
        private final Map<String, String> failures = new ConcurrentHashMap<>();
        @JsonIgnore
        private List<String> creationOrder = new ArrayList<>();

        public int getPublishedEntityCount() {
            return publishedEntityCount;
        }

        public void setPublishedEntityCount(int count) {
            this.publishedEntityCount = count;
        }

        public int getTotalEntitiesCount() {
            return totalEntitiesCount;
        }

        public void setTotalEntitiesCount(int count) {
            this.totalEntitiesCount = count;
        }

        public int getImportedEntitiesCount() {
            return importedEntitiesCount;
        }

        public void setImportedEntitiesCount(int count) {
            this.importedEntitiesCount = count;
        }

        public int getFailedEntitiesCount() {
            return failedEntitiesCount;
        }

        public void setFailedEntitiesCount(int count) {
            this.failedEntitiesCount = count;
        }

        public float getImportProgress() {
            return importProgress;
        }

        public void setImportProgress(float progress) {
            this.importProgress = progress;
        }

        public Map<String, String> getFailures() {
            return failures;
        }

        public void addFailure(String guid, String message) {
            this.failures.put(guid, message);
        }

        public List<String> getFailedEntities() {
            return failedEntities;
        }

        public void setFailedEntities(List<String> failedEntities) {
            this.failedEntities = failedEntities;
        }

        public List<String> getCreationOrder() {
            return creationOrder;
        }

        public void setCreationOrder(List<String> creationOrder) {
            this.creationOrder = creationOrder;
        }

        @Override
        public String toString() {
            return "ImportDetails{" +
                    "publishedEntityCount=" + publishedEntityCount +
                    ", totalEntitiesCount=" + totalEntitiesCount +
                    ", importedEntitiesCount=" + importedEntitiesCount +
                    ", failedEntitiesCount=" + failedEntitiesCount +
                    ", importProgress=" + importProgress +
                    ", failures=" + failures +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ImportDetails)) {
                return false;
            }
            ImportDetails that = (ImportDetails) o;
            return publishedEntityCount == that.publishedEntityCount &&
                    totalEntitiesCount == that.totalEntitiesCount &&
                    importedEntitiesCount == that.importedEntitiesCount &&
                    failedEntitiesCount == that.failedEntitiesCount &&
                    Float.compare(that.importProgress, importProgress) == 0 &&
                    Objects.equals(failures, that.failures);
        }

        @Override
        public int hashCode() {
            return Objects.hash(publishedEntityCount, totalEntitiesCount, importedEntitiesCount, failedEntitiesCount, importProgress, failures);
        }
    }
}
