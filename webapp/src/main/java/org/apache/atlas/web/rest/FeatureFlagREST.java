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
package org.apache.atlas.web.rest;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.service.FeatureFlag;
import org.apache.atlas.service.FeatureFlagStore;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;

/**
 * REST interface for feature flag operations.
 * 
 * Provides CRUD operations for managing feature flags:
 * - Get all feature flags with their current values
 * - Get individual feature flag by key
 * - Update/set feature flag value
 * - Delete feature flag (reset to default)
 */
@Path("featureflags")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class FeatureFlagREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.FeatureFlagREST");
    private static final Logger LOG = LoggerFactory.getLogger(FeatureFlagREST.class);

    private final FeatureFlagStore featureFlagStore;

    @Inject
    public FeatureFlagREST(FeatureFlagStore featureFlagStore) {
        this.featureFlagStore = featureFlagStore;
    }

    /**
     * Get all feature flags with their current values and metadata.
     * 
     * Returns all predefined feature flags along with:
     * - Current value (from Redis/cache or default)
     * - Default value
     * - Key name
     * 
     * @param request HTTP servlet request
     * @return FeatureFlagListResponse containing all feature flags
     * @throws AtlasBaseException if operation fails
     */
    @GET
    @Timed
    public FeatureFlagListResponse getAllFeatureFlags(@Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> FeatureFlagREST.getAllFeatureFlags()");
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FeatureFlagREST.getAllFeatureFlags()");
            }

            FeatureFlagListResponse response = new FeatureFlagListResponse();
            List<FeatureFlagInfo> flagList = new ArrayList<>();

            // Get all predefined feature flags
            String[] allKeys = FeatureFlag.getAllKeys();
            
            for (String key : allKeys) {
                FeatureFlag flag = FeatureFlag.fromKey(key);
                String currentValue = FeatureFlagStore.getFlag(key);

                FeatureFlagInfo flagInfo = new FeatureFlagInfo();
                flagInfo.setKey(key);
                flagInfo.setCurrentValue(currentValue);
                flagInfo.setDefaultValue(String.valueOf(flag.getDefaultValue()));

                flagList.add(flagInfo);
            }

            response.setFeatureFlags(flagList);
            response.setTotalCount(flagList.size());
            response.setTimestamp(new Date());

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== FeatureFlagREST.getAllFeatureFlags()");
            }
        }
    }

    /**
     * Get a specific feature flag by its key.
     * 
     * @param key Feature flag key
     * @param request HTTP servlet request
     * @return FeatureFlagInfo containing flag details
     * @throws AtlasBaseException if flag is invalid or operation fails
     */
    @GET
    @Path("{key}")
    @Timed
    public FeatureFlagInfo getFeatureFlag(@PathParam("key") String key,
                                         @Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> FeatureFlagREST.getFeatureFlag({})", key);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FeatureFlagREST.getFeatureFlag(" + key + ")");
            }

            if (StringUtils.isBlank(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Feature flag key cannot be empty");
            }

            if (!FeatureFlag.isValidFlag(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, 
                    "Invalid feature flag key: " + key + ". Valid keys are: " + Arrays.toString(FeatureFlag.getAllKeys()));
            }

            FeatureFlag flag = FeatureFlag.fromKey(key);
            String currentValue = FeatureFlagStore.getFlag(key);
            
            FeatureFlagInfo flagInfo = new FeatureFlagInfo();
            flagInfo.setKey(key);
            flagInfo.setCurrentValue(currentValue);
            flagInfo.setDefaultValue(String.valueOf(flag.getDefaultValue()));
            flagInfo.setTimestamp(new Date());

            return flagInfo;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== FeatureFlagREST.getFeatureFlag({})", key);
            }
        }
    }

    /**
     * Update or set a feature flag value.
     * 
     * @param key Feature flag key
     * @param request Update request containing the new value
     * @param servletRequest HTTP servlet request
     * @return FeatureFlagResponse indicating success
     * @throws AtlasBaseException if flag is invalid or operation fails
     */
    @PUT
    @Path("{key}")
    @Timed
    public FeatureFlagResponse updateFeatureFlag(@PathParam("key") String key,
                                               FeatureFlagUpdateRequest request,
                                               @Context HttpServletRequest servletRequest) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> FeatureFlagREST.updateFeatureFlag({}, {})", key, request != null ? request.getValue() : "null");
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FeatureFlagREST.updateFeatureFlag(" + key + ")");
            }

            if (StringUtils.isBlank(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Feature flag key cannot be empty");
            }

            if (request == null || StringUtils.isBlank(request.getValue())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Feature flag value cannot be empty");
            }

            if (!FeatureFlag.isValidFlag(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, 
                    "Invalid feature flag key: " + key + ". Valid keys are: " + Arrays.toString(FeatureFlag.getAllKeys()));
            }

            // Set the feature flag
            FeatureFlagStore.setFlag(key, request.getValue());

            LOG.info("Feature flag '{}' updated to value: {} by user: {}", key, request.getValue(), 
                    servletRequest.getRemoteUser() != null ? servletRequest.getRemoteUser() : "anonymous");

            FeatureFlagResponse response = new FeatureFlagResponse();
            response.setSuccess(true);
            response.setMessage("Feature flag '" + key + "' updated successfully to: " + request.getValue());
            response.setKey(key);
            response.setValue(request.getValue());
            response.setTimestamp(new Date());

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== FeatureFlagREST.updateFeatureFlag({}, {})", key, request != null ? request.getValue() : "null");
            }
        }
    }

    /**
     * Delete a feature flag (reset to default value).
     * 
     * This removes any custom value set for the feature flag,
     * causing it to fall back to its default value.
     * 
     * @param key Feature flag key
     * @param request HTTP servlet request
     * @return FeatureFlagResponse indicating success
     * @throws AtlasBaseException if flag is invalid or operation fails
     */
    @DELETE
    @Path("{key}")
    @Timed
    public FeatureFlagResponse deleteFeatureFlag(@PathParam("key") String key,
                                               @Context HttpServletRequest request) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> FeatureFlagREST.deleteFeatureFlag({})", key);
        }

        AtlasPerfTracer perf = null;
        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "FeatureFlagREST.deleteFeatureFlag(" + key + ")");
            }

            if (StringUtils.isBlank(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Feature flag key cannot be empty");
            }

            if (!FeatureFlag.isValidFlag(key)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, 
                    "Invalid feature flag key: " + key + ". Valid keys are: " + Arrays.toString(FeatureFlag.getAllKeys()));
            }

            // Get current value before deletion for logging
            String previousValue = FeatureFlagStore.getFlag(key);
            
            // Delete the feature flag (resets to default)
            FeatureFlagStore.deleteFlag(key);
            
            // Get default value for response
            FeatureFlag flag = FeatureFlag.fromKey(key);
            String defaultValue = String.valueOf(flag.getDefaultValue());

            LOG.info("Feature flag '{}' deleted (reset to default) by user: {}. Previous value: {}, Default value: {}", 
                    key, request.getRemoteUser() != null ? request.getRemoteUser() : "anonymous", 
                    previousValue, defaultValue);

            FeatureFlagResponse response = new FeatureFlagResponse();
            response.setSuccess(true);
            response.setMessage("Feature flag '" + key + "' deleted successfully (reset to default: " + defaultValue + ")");
            response.setKey(key);
            response.setValue(defaultValue);
            response.setTimestamp(new Date());

            return response;

        } finally {
            AtlasPerfTracer.log(perf);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== FeatureFlagREST.deleteFeatureFlag({})", key);
            }
        }
    }

    /**
     * Response containing all feature flags
     */
    public static class FeatureFlagListResponse {
        private List<FeatureFlagInfo> featureFlags = new ArrayList<>();
        private int totalCount;
        private Date timestamp;

        public List<FeatureFlagInfo> getFeatureFlags() { return featureFlags; }
        public void setFeatureFlags(List<FeatureFlagInfo> featureFlags) { this.featureFlags = featureFlags; }

        public int getTotalCount() { return totalCount; }
        public void setTotalCount(int totalCount) { this.totalCount = totalCount; }

        public Date getTimestamp() { return timestamp; }
        public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
    }

    /**
     * Information about a single feature flag
     */
    public static class FeatureFlagInfo {
        private String key;
        private String currentValue;
        private String defaultValue;
        private Date timestamp;

        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }

        public String getCurrentValue() { return currentValue; }
        public void setCurrentValue(String currentValue) { this.currentValue = currentValue; }

        public String getDefaultValue() { return defaultValue; }
        public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }

        public Date getTimestamp() { return timestamp; }
        public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
    }

    /**
     * Request for updating a feature flag
     */
    public static class FeatureFlagUpdateRequest {
        private String value;

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
    }

    /**
     * Response for feature flag operations
     */
    public static class FeatureFlagResponse {
        private boolean success;
        private String message;
        private String key;
        private String value;
        private Date timestamp;

        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }

        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }

        public String getKey() { return key; }
        public void setKey(String key) { this.key = key; }

        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }

        public Date getTimestamp() { return timestamp; }
        public void setTimestamp(Date timestamp) { this.timestamp = timestamp; }
    }
}
