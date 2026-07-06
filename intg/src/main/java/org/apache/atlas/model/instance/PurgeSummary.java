/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.instance;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Summary counts for purge operations.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PurgeSummary extends MutationSummary {
    private static final long serialVersionUID = 1L;

    private long requestedCount;
    private long purgedCount;
    private long purgedDependenciesCount;
    private long failedCount;
    private long failedDependenciesCount;
    private long skippedCount;
    private long validGuidCount;
    private boolean executionFailed;
    private long expandedEntityCount;
    private long skippedRequestedCount;
    private long skippedDependenciesCount;
    private long unprocessedCount;
    private long batchCount;

    public PurgeSummary() {
    }

    public PurgeSummary(long requestedCount, long purgedCount, long purgedDependenciesCount, long failedCount, long skippedCount) {
        this(requestedCount, purgedCount, purgedDependenciesCount, failedCount, 0, skippedCount);
    }

    public PurgeSummary(long requestedCount, long purgedCount, long purgedDependenciesCount, long failedCount,
                        long failedDependenciesCount, long skippedCount) {
        this.requestedCount            = requestedCount;
        this.purgedCount               = purgedCount;
        this.purgedDependenciesCount   = purgedDependenciesCount;
        this.failedCount               = failedCount;
        this.failedDependenciesCount   = failedDependenciesCount;
        this.skippedCount              = skippedCount;
    }

    public long getRequestedCount() {
        return requestedCount;
    }

    public void setRequestedCount(long requestedCount) {
        this.requestedCount = requestedCount;
    }

    public long getPurgedCount() {
        return purgedCount;
    }

    public void setPurgedCount(long purgedCount) {
        this.purgedCount = purgedCount;
    }

    public long getPurgedDependenciesCount() {
        return purgedDependenciesCount;
    }

    public void setPurgedDependenciesCount(long purgedDependenciesCount) {
        this.purgedDependenciesCount = purgedDependenciesCount;
    }

    public long getFailedCount() {
        return failedCount;
    }

    public void setFailedCount(long failedCount) {
        this.failedCount = failedCount;
    }

    public long getFailedDependenciesCount() {
        return failedDependenciesCount;
    }

    public void setFailedDependenciesCount(long failedDependenciesCount) {
        this.failedDependenciesCount = failedDependenciesCount;
    }

    public long getSkippedCount() {
        return skippedCount;
    }

    public void setSkippedCount(long skippedCount) {
        this.skippedCount = skippedCount;
    }

    public long getValidGuidCount() {
        return validGuidCount;
    }

    public void setValidGuidCount(long validGuidCount) {
        this.validGuidCount = validGuidCount;
    }

    public boolean getExecutionFailed() {
        return executionFailed;
    }

    public void setExecutionFailed(boolean executionFailed) {
        this.executionFailed = executionFailed;
    }

    public long getExpandedEntityCount() {
        return expandedEntityCount;
    }

    public void setExpandedEntityCount(long expandedEntityCount) {
        this.expandedEntityCount = expandedEntityCount;
    }

    public long getSkippedRequestedCount() {
        return skippedRequestedCount;
    }

    public void setSkippedRequestedCount(long skippedRequestedCount) {
        this.skippedRequestedCount = skippedRequestedCount;
    }

    public long getSkippedDependenciesCount() {
        return skippedDependenciesCount;
    }

    public void setSkippedDependenciesCount(long skippedDependenciesCount) {
        this.skippedDependenciesCount = skippedDependenciesCount;
    }

    public long getUnprocessedCount() {
        return unprocessedCount;
    }

    public void setUnprocessedCount(long unprocessedCount) {
        this.unprocessedCount = unprocessedCount;
    }

    public long getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(long batchCount) {
        this.batchCount = batchCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PurgeSummary{");
        sb.append("requestedCount=").append(requestedCount);
        sb.append(", purgedCount=").append(purgedCount);
        sb.append(", purgedDependenciesCount=").append(purgedDependenciesCount);
        sb.append(", failedCount=").append(failedCount);
        sb.append(", failedDependenciesCount=").append(failedDependenciesCount);
        sb.append(", skippedCount=").append(skippedCount);
        sb.append(", validGuidCount=").append(validGuidCount);
        sb.append(", executionFailed=").append(executionFailed);
        sb.append(", expandedEntityCount=").append(expandedEntityCount);
        sb.append(", skippedRequestedCount=").append(skippedRequestedCount);
        sb.append(", skippedDependenciesCount=").append(skippedDependenciesCount);
        sb.append(", unprocessedCount=").append(unprocessedCount);
        sb.append(", batchCount=").append(batchCount);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PurgeSummary that = (PurgeSummary) o;
        return requestedCount == that.requestedCount &&
               purgedCount == that.purgedCount &&
               purgedDependenciesCount == that.purgedDependenciesCount &&
               failedCount == that.failedCount &&
               failedDependenciesCount == that.failedDependenciesCount &&
               skippedCount == that.skippedCount &&
               validGuidCount == that.validGuidCount &&
               executionFailed == that.executionFailed &&
               expandedEntityCount == that.expandedEntityCount &&
               skippedRequestedCount == that.skippedRequestedCount &&
               skippedDependenciesCount == that.skippedDependenciesCount &&
               unprocessedCount == that.unprocessedCount &&
               batchCount == that.batchCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestedCount, purgedCount, purgedDependenciesCount, failedCount, failedDependenciesCount,
                skippedCount, validGuidCount, executionFailed, expandedEntityCount, skippedRequestedCount,
                skippedDependenciesCount, unprocessedCount, batchCount);
    }
}
