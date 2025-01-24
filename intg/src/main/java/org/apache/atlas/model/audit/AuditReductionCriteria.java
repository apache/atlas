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
package org.apache.atlas.model.audit;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AuditReductionCriteria implements Serializable {
    private static final long serialVersionUID = 1L;

    private boolean auditAgingEnabled;
    private boolean defaultAgeoutEnabled;
    private boolean auditSweepoutEnabled;
    private boolean createEventsAgeoutAllowed;
    private boolean subTypesIncluded;
    private boolean ignoreDefaultAgeoutTTL;

    private int defaultAgeoutAuditCount;
    private int defaultAgeoutTTLInDays;
    private int customAgeoutAuditCount;
    private int customAgeoutTTLInDays;

    private String customAgeoutEntityTypes;
    private String customAgeoutActionTypes;

    private String sweepoutEntityTypes;
    private String sweepoutActionTypes;

    public boolean isAuditAgingEnabled() {
        return auditAgingEnabled;
    }

    public void setAuditAgingEnabled(boolean auditAgingEnabled) {
        this.auditAgingEnabled = auditAgingEnabled;
    }

    public boolean isDefaultAgeoutEnabled() {
        return defaultAgeoutEnabled;
    }

    public void setDefaultAgeoutEnabled(boolean defaultAgeoutEnabled) {
        this.defaultAgeoutEnabled = defaultAgeoutEnabled;
    }

    public boolean isAuditSweepoutEnabled() {
        return auditSweepoutEnabled;
    }

    public void setAuditSweepoutEnabled(boolean auditSweepoutEnabled) {
        this.auditSweepoutEnabled = auditSweepoutEnabled;
    }

    public boolean isCreateEventsAgeoutAllowed() {
        return createEventsAgeoutAllowed;
    }

    public void setCreateEventsAgeoutAllowed(boolean createEventsAgeoutAllowed) {
        this.createEventsAgeoutAllowed = createEventsAgeoutAllowed;
    }

    public boolean isSubTypesIncluded() {
        return subTypesIncluded;
    }

    public void setSubTypesIncluded(boolean subTypesIncluded) {
        this.subTypesIncluded = subTypesIncluded;
    }

    public boolean ignoreDefaultAgeoutTTL() {
        return ignoreDefaultAgeoutTTL;
    }

    public void setIgnoreDefaultAgeoutTTL(boolean ignoreDefaultAgeoutTTL) {
        this.ignoreDefaultAgeoutTTL = ignoreDefaultAgeoutTTL;
    }

    public int getDefaultAgeoutTTLInDays() {
        return defaultAgeoutTTLInDays;
    }

    public void setDefaultAgeoutTTLInDays(int defaultAgeoutTTLInDays) {
        this.defaultAgeoutTTLInDays = defaultAgeoutTTLInDays;
    }

    public int getDefaultAgeoutAuditCount() {
        return defaultAgeoutAuditCount;
    }

    public void setDefaultAgeoutAuditCount(int defaultAgeoutAuditCount) {
        this.defaultAgeoutAuditCount = defaultAgeoutAuditCount;
    }

    public int getCustomAgeoutTTLInDays() {
        return customAgeoutTTLInDays;
    }

    public void setCustomAgeoutTTLInDays(int customAgeoutTTLInDays) {
        this.customAgeoutTTLInDays = customAgeoutTTLInDays;
    }

    public int getCustomAgeoutAuditCount() {
        return customAgeoutAuditCount;
    }

    public void setCustomAgeoutAuditCount(int customAgeoutAuditCount) {
        this.customAgeoutAuditCount = customAgeoutAuditCount;
    }

    public String getCustomAgeoutEntityTypes() {
        return customAgeoutEntityTypes;
    }

    public void setCustomAgeoutEntityTypes(String customAgeoutEntityTypes) {
        this.customAgeoutEntityTypes = customAgeoutEntityTypes;
    }

    public String getCustomAgeoutActionTypes() {
        return customAgeoutActionTypes;
    }

    public void setCustomAgeoutActionTypes(String customAgeoutActionTypes) {
        this.customAgeoutActionTypes = customAgeoutActionTypes;
    }

    public String getSweepoutEntityTypes() {
        return sweepoutEntityTypes;
    }

    public void setSweepoutEntityTypes(String sweepoutEntityTypes) {
        this.sweepoutEntityTypes = sweepoutEntityTypes;
    }

    public String getSweepoutActionTypes() {
        return sweepoutActionTypes;
    }

    public void setSweepoutActionTypes(String sweepoutActionTypes) {
        this.sweepoutActionTypes = sweepoutActionTypes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(auditAgingEnabled, defaultAgeoutEnabled, auditSweepoutEnabled, createEventsAgeoutAllowed, subTypesIncluded, ignoreDefaultAgeoutTTL, defaultAgeoutAuditCount, defaultAgeoutTTLInDays, customAgeoutAuditCount, customAgeoutTTLInDays,
                customAgeoutEntityTypes, customAgeoutActionTypes, sweepoutEntityTypes, sweepoutActionTypes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AuditReductionCriteria that = (AuditReductionCriteria) o;

        return auditAgingEnabled == that.auditAgingEnabled &&
                defaultAgeoutEnabled == that.defaultAgeoutEnabled &&
                auditSweepoutEnabled == that.auditSweepoutEnabled &&
                createEventsAgeoutAllowed == that.createEventsAgeoutAllowed &&
                subTypesIncluded == that.subTypesIncluded &&
                ignoreDefaultAgeoutTTL == that.ignoreDefaultAgeoutTTL &&
                defaultAgeoutAuditCount == that.defaultAgeoutAuditCount &&
                defaultAgeoutTTLInDays == that.defaultAgeoutTTLInDays &&
                customAgeoutAuditCount == that.customAgeoutAuditCount &&
                customAgeoutTTLInDays == that.customAgeoutTTLInDays &&
                Objects.equals(customAgeoutEntityTypes, that.customAgeoutEntityTypes) &&
                Objects.equals(customAgeoutActionTypes, that.customAgeoutActionTypes) &&
                Objects.equals(sweepoutEntityTypes, that.sweepoutEntityTypes) &&
                Objects.equals(sweepoutActionTypes, that.sweepoutActionTypes);
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append('{');
        sb.append("auditAgingEnabled='").append(auditAgingEnabled).append('\'');
        sb.append(", createEventsAgeoutAllowed='").append(createEventsAgeoutAllowed).append('\'');
        sb.append(", subTypesIncluded='").append(subTypesIncluded).append('\'');
        sb.append(", defaultAgeoutEnabled='").append(defaultAgeoutEnabled).append('\'');
        sb.append(", ignoreDefaultAgeoutTTL='").append(ignoreDefaultAgeoutTTL).append('\'');
        sb.append(", defaultAgeoutTTLInDays='").append(defaultAgeoutTTLInDays).append('\'');
        sb.append(", defaultAgeoutAuditCount='").append(defaultAgeoutAuditCount).append('\'');
        sb.append(", auditSweepoutEnabled='").append(auditSweepoutEnabled).append('\'');
        sb.append(", customAgeoutAuditCount='").append(customAgeoutAuditCount).append('\'');
        sb.append(", customAgeoutTTLInDays='").append(customAgeoutTTLInDays).append('\'');
        sb.append(", customAgeoutEntityTypes=").append(customAgeoutEntityTypes);
        sb.append(", customAgeoutActionTypes=").append(customAgeoutActionTypes);
        sb.append(", sweepoutEntityTypes=").append(sweepoutEntityTypes);
        sb.append(", sweepoutActionTypes=").append(sweepoutActionTypes);
        sb.append('}');

        return sb;
    }
}
