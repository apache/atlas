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

package org.apache.atlas.model.searchlog;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.atlas.type.AtlasType;

import java.util.Map;
import java.util.Set;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class SearchRequestLogData {

    private Map<String, Object> dsl;
    private String dslText;
    private Set<String> attributes;
    private Set<String> relationAttributes;

    private String persona;
    private String purpose;

    private String userAgent;
    private String host;
    private String ipAddress;
    private String userName;
    private String errorDetails;
    private String errorCode;

    private Set<String> entityGuidsAll;
    private Set<String> entityQFNamesAll;
    private Set<String> entityGuidsAllowed;
    private Set<String> entityQFNamesAllowed;
    private Set<String> entityGuidsDenied;
    private Set<String> entityQFNamesDenied;
    private Set<String> entityTypeNamesAll;
    private Set<String> entityTypeNamesAllowed;
    private Set<String> entityTypeNamesDenied;
    private Set<String> utmTags;
    private String searchInput;

    private boolean hasResult;
    private boolean isFailed;

    private long resultsCount;
    private long responseTime;
    private long createdAt;
    private long timestamp;

    public SearchRequestLogData(Map<String, Object> dsl, Set<String> attributes, Set<String> relationAttributes,
                                String searchInput, String persona, String purpose,
                                String userAgent, String host, String ipAddress, String userName,
                                String errorDetails, String errorCode,
                                Set<String> entityGuidsAll, Set<String> entityQFNamesAll, Set<String> entityGuidsAllowed,
                                Set<String> entityQFNamesAllowed, Set<String> entityGuidsDenied, Set<String> entityQFNamesDenied,
                                Set<String> entityTypeNamesAll, Set<String> entityTypeNamesAllowed, Set<String> entityTypeNamesDenied,
                                Set<String> utmTags, boolean hasResult, boolean isFailed,
                                long resultsCount, long responseTime, long timestamp) {
        this.dsl = dsl;
        this.dslText = AtlasType.toJson(dsl);
        this.attributes = attributes;
        this.relationAttributes = relationAttributes;
        this.persona = persona;
        this.purpose = purpose;
        this.userAgent = userAgent;
        this.host = host;
        this.ipAddress = ipAddress;
        this.userName = userName;
        this.errorDetails = errorDetails;
        this.errorCode = errorCode;
        this.entityGuidsAll = entityGuidsAll;
        this.entityQFNamesAll = entityQFNamesAll;
        this.entityGuidsAllowed = entityGuidsAllowed;
        this.entityQFNamesAllowed = entityQFNamesAllowed;
        this.entityGuidsDenied = entityGuidsDenied;
        this.entityQFNamesDenied = entityQFNamesDenied;
        this.entityTypeNamesAll = entityTypeNamesAll;
        this.entityTypeNamesAllowed = entityTypeNamesAllowed;
        this.entityTypeNamesDenied = entityTypeNamesDenied;
        this.utmTags = utmTags;
        this.searchInput = searchInput;
        this.hasResult = hasResult;
        this.isFailed = isFailed;
        this.resultsCount = resultsCount;
        this.responseTime = responseTime;
        this.timestamp = timestamp;
    }

    //ES mapping type : nested
    @JsonProperty("request.dsl")
    public Map<String, Object> getDsl() {
        return dsl;
    }

    @JsonProperty("request.dslText")
    public String getDslText() {
        return dslText;
    }

    @JsonProperty("request.attributes")
    public Set<String> getAttributes() {
        return attributes;
    }

    @JsonProperty("request.relationAttributes")
    public Set<String> getRelationAttributes() {
        return relationAttributes;
    }

    public String getPersona() {
        return persona;
    }

    public String getPurpose() {
        return purpose;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public String getHost() {
        return host;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getUserName() {
        return userName;
    }

    public String getErrorDetails() {
        return errorDetails;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public Set<String> getEntityGuidsAll() {
        return entityGuidsAll;
    }

    public Set<String> getEntityQFNamesAll() {
        return entityQFNamesAll;
    }

    public Set<String> getEntityGuidsAllowed() {
        return entityGuidsAllowed;
    }

    public Set<String> getEntityQFNamesAllowed() {
        return entityQFNamesAllowed;
    }

    public Set<String> getEntityGuidsDenied() {
        return entityGuidsDenied;
    }

    public Set<String> getEntityQFNamesDenied() {
        return entityQFNamesDenied;
    }

    public Set<String> getEntityTypeNamesAll() {
        return entityTypeNamesAll;
    }

    public Set<String> getEntityTypeNamesAllowed() {
        return entityTypeNamesAllowed;
    }

    public Set<String> getEntityTypeNamesDenied() {
        return entityTypeNamesDenied;
    }

    public Set<String> getUtmTags() {
        return utmTags;
    }

    public boolean isHasResult() {
        return hasResult;
    }

    public boolean isFailed() {
        return isFailed;
    }

    public long getResultsCount() {
        return resultsCount;
    }

    public long getResponseTime() {
        return responseTime;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public String getSearchInput() {
        return searchInput;
    }

    public static class SearchRequestLogDataBuilder {
        private Map<String, Object> dsl;
        private Set<String> attributes;
        private Set<String> relationAttributes;

        private String persona;
        private String purpose;

        private String userAgent;
        private String host;
        private String ipAddress;
        private String userName;
        private String errorDetails;
        private String errorCode;

        private Set<String> entityGuidsAll;
        private Set<String> entityQFNamesAll;
        private Set<String> entityGuidsAllowed;
        private Set<String> entityQFNamesAllowed;
        private Set<String> entityGuidsDenied;
        private Set<String> entityQFNamesDenied;
        private Set<String> entityTypeNamesAll;
        private Set<String> entityTypeNamesAllowed;
        private Set<String> entityTypeNamesDenied;
        private Set<String> utmTags;
        private String searchInput;

        private boolean hasResult;
        private boolean isFailed;

        private long resultsCount;
        private long responseTime;
        private long timestamp;

        public SearchRequestLogDataBuilder(){}

        public SearchRequestLogDataBuilder setDsl(Map<String, Object> dsl) {
            this.dsl = dsl;
            return this;
        }

        public SearchRequestLogDataBuilder setSearchInput(String searchInput) {
            this.searchInput = searchInput;
            return this;
        }

        public SearchRequestLogDataBuilder setPersona(String persona) {
            this.persona = persona;
            return this;
        }

        public SearchRequestLogDataBuilder setPurpose(String purpose) {
            this.purpose = purpose;
            return this;
        }

        public SearchRequestLogDataBuilder setAttributes(Set<String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public SearchRequestLogDataBuilder setRelationAttributes(Set<String> relationAttributes) {
            this.relationAttributes = relationAttributes;
            return this;
        }

        public SearchRequestLogDataBuilder setUserAgent(String userAgent) {
            this.userAgent = userAgent;
            return this;
        }

        public SearchRequestLogDataBuilder setHost(String host) {
            this.host = host;
            return this;
        }

        public SearchRequestLogDataBuilder setIpAddress(String ipAddress) {
            this.ipAddress = ipAddress;
            return this;
        }

        public SearchRequestLogDataBuilder setUserName(String userName) {
            this.userName = userName;
            return this;
        }

        public SearchRequestLogDataBuilder setErrorDetails(String errorDetails) {
            this.errorDetails = errorDetails;
            return this;
        }

        public SearchRequestLogDataBuilder setErrorCode(String errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public SearchRequestLogDataBuilder setEntityGuidsAll(Set<String> entityGuidsAll) {
            this.entityGuidsAll = entityGuidsAll;
            return this;
        }

        public SearchRequestLogDataBuilder setEntityQFNamesAll(Set<String> entityQFNamesAll) {
            this.entityQFNamesAll = entityQFNamesAll;
            return this;
        }

        public SearchRequestLogDataBuilder setEntityGuidsAllowed(Set<String> entityGuidsAllowed) {
            this.entityGuidsAllowed = entityGuidsAllowed;
            return this;
        }

        public SearchRequestLogDataBuilder setEntityQFNamesAllowed(Set<String> entityQFNamesAllowed) {
            this.entityQFNamesAllowed = entityQFNamesAllowed;
            return this;
        }

        public SearchRequestLogDataBuilder setEntityGuidsDenied(Set<String> entityGuidsDenied) {
            this.entityGuidsDenied = entityGuidsDenied;
            return this;
        }

        public SearchRequestLogDataBuilder setEntityQFNamesDenied(Set<String> entityQFNamesDenied) {
            this.entityQFNamesDenied = entityQFNamesDenied;
            return this;
        }

        public SearchRequestLogDataBuilder setEntityTypeNamesAll(Set<String> entityTypeNamesAll) {
            this.entityTypeNamesAll = entityTypeNamesAll;
            return this;
        }

        public SearchRequestLogDataBuilder setEntityTypeNamesAllowed(Set<String> entityTypeNamesAllowed) {
            this.entityTypeNamesAllowed = entityTypeNamesAllowed;
            return this;
        }

        public SearchRequestLogDataBuilder setEntityTypeNamesDenied(Set<String> entityTypeNamesDenied) {
            this.entityTypeNamesDenied = entityTypeNamesDenied;
            return this;
        }

        public SearchRequestLogDataBuilder setUtmTags(Set<String> utmTags) {
            this.utmTags = utmTags;
            return this;
        }

        public SearchRequestLogDataBuilder setHasResult(boolean hasResult) {
            this.hasResult = hasResult;
            return this;
        }

        public SearchRequestLogDataBuilder setFailed(boolean failed) {
            isFailed = failed;
            return this;
        }

        public SearchRequestLogDataBuilder setResultsCount(long resultsCount) {
            this.resultsCount = resultsCount;
            return this;
        }

        public SearchRequestLogDataBuilder setResponseTime(long responseTime) {
            this.responseTime = responseTime;
            return this;
        }

        public SearchRequestLogDataBuilder setTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public SearchRequestLogData build() {
            return new SearchRequestLogData(dsl, attributes, relationAttributes, searchInput, persona, purpose,
                    userAgent, host, ipAddress, userName,
                    errorDetails, errorCode, entityGuidsAll, entityQFNamesAll, entityGuidsAllowed,
                    entityQFNamesAllowed, entityGuidsDenied, entityQFNamesDenied,
                    entityTypeNamesAll, entityTypeNamesAllowed, entityTypeNamesDenied, utmTags,
                    hasResult, isFailed, resultsCount, responseTime, timestamp);
        }
    }
}