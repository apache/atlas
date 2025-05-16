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
import org.apache.atlas.AtlasConfiguration;
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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AtlasAsyncImportRequest extends AtlasBaseModelObject implements Serializable {
    private static final long serialVersionUID = 1L;

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

    private String            importId;
    private ImportStatus      status;
    private ImportDetails     importDetails;
    private long              receivedTime;
    private long              stagedTime;
    private long              processingStartTime;
    private long              completedTime;
    private AtlasImportResult importResult;

    @JsonIgnore
    private ImportTrackingInfo importTrackingInfo;

    public AtlasAsyncImportRequest() {}

    public AtlasAsyncImportRequest(String guid) {
        setGuid(guid);
    }

    public AtlasAsyncImportRequest(AtlasImportResult result) {
        this.importResult        = result;
        this.status              = ImportStatus.STAGING;
        this.receivedTime        = 0L;
        this.stagedTime          = 0L;
        this.processingStartTime = 0L;
        this.completedTime       = 0L;
        this.importDetails       = new ImportDetails();
        this.importTrackingInfo  = new ImportTrackingInfo(null, 0);

        setGuid(getGuid());
    }

    public String getImportId() {
        return importId;
    }

    public void setImportId(String importId) {
        this.importId = importId;

        if (importTrackingInfo != null) {
            importTrackingInfo.setRequestId(AtlasConfiguration.ASYNC_IMPORT_REQUEST_ID_PREFIX.getString() + importId + "@" + AtlasEntityUtil.getMetadataNamespace());
        }
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

    public long getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(long receivedTime) {
        this.receivedTime = receivedTime;
    }

    public long getStagedTime() {
        return stagedTime;
    }

    public void setStagedTime(long stagedTime) {
        this.stagedTime = stagedTime;
    }

    public long getProcessingStartTime() {
        return processingStartTime;
    }

    public void setProcessingStartTime(long processingStartTime) {
        this.processingStartTime = processingStartTime;
    }

    @JsonIgnore
    public String getTopicName() {
        return AtlasConfiguration.ASYNC_IMPORT_TOPIC_PREFIX.getString() + importId;
    }

    public AtlasImportResult getImportResult() {
        return importResult;
    }

    public void setImportResult(AtlasImportResult importResult) {
        this.importResult = importResult;
    }

    public long getCompletedTime() {
        return completedTime;
    }

    public void setCompletedTime(long completedTime) {
        this.completedTime = completedTime;
    }

    public ImportTrackingInfo getImportTrackingInfo() {
        return importTrackingInfo;
    }

    public void setImportTrackingInfo(ImportTrackingInfo importTrackingInfo) {
        this.importTrackingInfo = importTrackingInfo;
    }

    @JsonIgnore
    public AsyncImportStatus toImportMinInfo() {
        return new AsyncImportStatus(this.getImportId(), status, toIsoDate(new Date(this.receivedTime)), importResult.getUserName());
    }

    private String toIsoDate(Date value) {
        final TimeZone   tz = TimeZone.getTimeZone("UTC");
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

        df.setTimeZone(tz);

        return df.format(value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof AtlasAsyncImportRequest)) {
            return false;
        } else if (!super.equals(o)) {
            return false;
        }

        AtlasAsyncImportRequest that = (AtlasAsyncImportRequest) o;

        return Objects.equals(importResult, that.importResult) &&
                Objects.equals(importId, that.importId) &&
                Objects.equals(status, that.status) &&
                Objects.equals(importDetails, that.importDetails) &&
                (importTrackingInfo == null ? that.importTrackingInfo == null : (that.importTrackingInfo != null && Objects.equals(importTrackingInfo.getRequestId(), that.importTrackingInfo.getRequestId()))) &&
                Objects.equals(receivedTime, that.receivedTime) &&
                Objects.equals(stagedTime, that.stagedTime) &&
                Objects.equals(processingStartTime, that.processingStartTime) &&
                Objects.equals(completedTime, that.completedTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), importResult, importId, status, importDetails,
                importTrackingInfo == null ? null : importTrackingInfo.getRequestId(), receivedTime, stagedTime, processingStartTime, completedTime);
    }

    @Override
    protected StringBuilder toString(StringBuilder sb) {
        sb.append(", importResult=").append(importResult);
        sb.append(", requestId=").append(importTrackingInfo == null ? null : importTrackingInfo.getRequestId());
        sb.append(", importId=").append(importId);
        sb.append(", status=").append(status);
        sb.append(", receivedTime=").append(receivedTime);
        sb.append(", stagedTime=").append(stagedTime);
        sb.append(", processingStartTime=").append(processingStartTime);
        sb.append(", completedTime=").append(completedTime);
        sb.append(", importDetails=").append(importDetails);

        return sb;
    }

    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ImportDetails implements Serializable {
        private static final long serialVersionUID = 1L;

        private int                 publishedEntityCount;
        private int                 totalEntitiesCount;
        private int                 importedEntitiesCount;
        private int                 failedEntitiesCount;
        private List<String>        failedEntities;
        private float               importProgress;
        private Map<String, String> failures;

        @JsonIgnore
        private List<String> creationOrder = new ArrayList<>();

        public ImportDetails() {
            this.failedEntities = new ArrayList<>();
            this.failures       = new HashMap<>();
        }

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
            } else if (!(o instanceof ImportDetails)) {
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

    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ImportTrackingInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private String requestId;
        private int    startEntityPosition;

        public ImportTrackingInfo() {
        }

        public ImportTrackingInfo(String requestId, int startEntityPosition) {
            this.requestId           = requestId;
            this.startEntityPosition = startEntityPosition;
        }

        public String getRequestId() {
            return requestId;
        }

        public void setRequestId(String requestId) {
            this.requestId = requestId;
        }

        public int getStartEntityPosition() {
            return startEntityPosition;
        }

        public void setStartEntityPosition(int startEntityPosition) {
            this.startEntityPosition = startEntityPosition;
        }

        @Override
        public String toString() {
            return "ImportTrackingInfo{" +
                    "requestId='" + requestId + '\'' +
                    ", startEntityPosition=" + startEntityPosition +
                    '}';
        }
    }
}
