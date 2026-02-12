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
package org.apache.atlas.model.discovery;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.model.instance.AtlasEntityHeader;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * Response object containing Purpose entities accessible to a user.
 * Returned by the POST /api/meta/purposes/user endpoint.
 */
@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class PurposeUserResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<AtlasEntityHeader> purposes;
    private int count;
    private long totalCount;
    private boolean hasMore;
    private boolean approximateCount;

    public PurposeUserResponse() {
    }

    public PurposeUserResponse(List<AtlasEntityHeader> purposes, long totalCount, int limit, int offset) {
        this(purposes, totalCount, limit, offset, false);
    }

    public PurposeUserResponse(List<AtlasEntityHeader> purposes, long totalCount, int limit, int offset, boolean approximateCount) {
        this.purposes = purposes != null ? purposes : Collections.emptyList();
        this.count = this.purposes.size();
        this.totalCount = totalCount;
        this.hasMore = (offset + this.count) < totalCount;
        this.approximateCount = approximateCount;
    }

    /**
     * Creates an empty response with no purposes.
     */
    public static PurposeUserResponse empty() {
        return new PurposeUserResponse(Collections.emptyList(), 0, 0, 0);
    }

    public List<AtlasEntityHeader> getPurposes() {
        return purposes;
    }

    public void setPurposes(List<AtlasEntityHeader> purposes) {
        this.purposes = purposes;
        this.count = purposes != null ? purposes.size() : 0;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }

    public boolean isHasMore() {
        return hasMore;
    }

    public void setHasMore(boolean hasMore) {
        this.hasMore = hasMore;
    }

    /**
     * Returns true if the totalCount is approximate due to hitting internal query limits.
     * When true, there may be more purposes accessible to the user than indicated by totalCount.
     */
    public boolean isApproximateCount() {
        return approximateCount;
    }

    public void setApproximateCount(boolean approximateCount) {
        this.approximateCount = approximateCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PurposeUserResponse that = (PurposeUserResponse) o;
        return count == that.count &&
                totalCount == that.totalCount &&
                hasMore == that.hasMore &&
                approximateCount == that.approximateCount &&
                Objects.equals(purposes, that.purposes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(purposes, count, totalCount, hasMore, approximateCount);
    }

    @Override
    public String toString() {
        return "PurposeUserResponse{" +
                "count=" + count +
                ", totalCount=" + totalCount +
                ", hasMore=" + hasMore +
                ", approximateCount=" + approximateCount +
                ", purposes=" + purposes +
                '}';
    }
}
