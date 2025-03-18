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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.atlas.repository.ogm.impexp;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.impexp.AtlasAsyncImportRequest;
import org.apache.atlas.model.impexp.AtlasImportResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.repository.impexp.AuditsWriter;
import org.apache.atlas.repository.ogm.AbstractDataTransferObject;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ASYNC_IMPORT_TYPE_NAME;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportDetails;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.REQUEST_ID_PREFIX_PROPERTY;

/**
 * AtlasAsyncImportRequestDTO is the bridge class between AtlasAsyncImportRequest and AtlasEntity.
 */
@Component
public class AtlasAsyncImportRequestDTO extends AbstractDataTransferObject<AtlasAsyncImportRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAsyncImportRequestDTO.class);

    public static final String IMPORT_RESULT_PROPERTY                        = "importResult";
    public static final String REQUEST_ID_PROPERTY                           = "requestId";
    public static final String IMPORT_ID_PROPERTY                            = "importId";
    public static final String SKIP_TO_PROPERTY                              = "skipTo";
    public static final String STATUS_PROPERTY                               = "status";
    public static final String IMPORT_DETAILS_PROPERTY                       = "importDetails";
    public static final String RECEIVED_AT_PROPERTY                          = "receivedAt";
    public static final String STAGED_AT_PROPERTY                            = "stagedAt";
    public static final String STARTED_PROCESSING_AT                         = "startedProcessingAt";
    public static final String COMPLETED_AT                                  = "completedAt";

    @Inject
    public AtlasAsyncImportRequestDTO(AtlasTypeRegistry typeRegistry) {
        super(typeRegistry, AtlasAsyncImportRequest.class, ASYNC_IMPORT_TYPE_NAME);
    }

    @Override
    public AtlasAsyncImportRequest from(AtlasEntity entity) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasAsyncImportRequestDTO.from({})", entity);
        }

        AtlasAsyncImportRequest asyncImportRequest = null;
        ImportDetails importDetails = null;

        String requestId                   = (String) entity.getAttribute(REQUEST_ID_PROPERTY);
        String jsonImportResult       = (String) entity.getAttribute(IMPORT_RESULT_PROPERTY);
        String importId                    = (String) entity.getAttribute(IMPORT_ID_PROPERTY);
        String status                      = (String) entity.getAttribute(STATUS_PROPERTY);
        int skipTo                         = Integer.parseInt((String) entity.getAttribute(SKIP_TO_PROPERTY));
        String jsonImportDetails           = (String) entity.getAttribute(IMPORT_DETAILS_PROPERTY);
        long receivedAt                    = objectToLong(entity.getAttribute(RECEIVED_AT_PROPERTY));
        long stagedAt                     = objectToLong(entity.getAttribute(STAGED_AT_PROPERTY));
        long startedProcessingAt          = objectToLong(entity.getAttribute(STARTED_PROCESSING_AT));
        long completedAt                  = objectToLong(entity.getAttribute(COMPLETED_AT));

        if (StringUtils.isNotEmpty(jsonImportResult)) {
            asyncImportRequest = new AtlasAsyncImportRequest(AtlasType.fromJson(jsonImportResult, AtlasImportResult.class));
        }

        if (StringUtils.isNotEmpty(jsonImportDetails)) {
            importDetails = AtlasType.fromJson(jsonImportDetails, ImportDetails.class);
        }

        if (asyncImportRequest == null) {
            LOG.error("AtlasAsyncImportRequest cannot be created without atlas import request and result. Null has been returned");
        } else {
            asyncImportRequest.setGuid(entity.getGuid());
            asyncImportRequest.setRequestId(requestId);
            asyncImportRequest.setImportId(importId);
            asyncImportRequest.setSkipTo(skipTo);
            asyncImportRequest.setStatus(ImportStatus.valueOf(status));
            asyncImportRequest.setImportDetails(importDetails);
            asyncImportRequest.setReceivedAt(receivedAt);
            asyncImportRequest.setStagedAt(stagedAt);
            asyncImportRequest.setStartedProcessingAt(startedProcessingAt);
            asyncImportRequest.setCompletedAt(completedAt);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasAsyncImportRequestDTO.from() : {}", asyncImportRequest);
        }

        return asyncImportRequest;
    }

    @Override
    public AtlasAsyncImportRequest from(AtlasEntityWithExtInfo entityWithExtInfo) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasAsyncImportRequestDTO.from({})", entityWithExtInfo);
        }

        AtlasAsyncImportRequest request = from(entityWithExtInfo.getEntity());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasAsyncImportRequestDTO.from() : {}", request);
        }
        return request;
    }

    @Override
    public AtlasEntity toEntity(AtlasAsyncImportRequest obj) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasAsyncImportRequestDTO.toEntity({})", obj);
        }

        AtlasEntity entity = getDefaultAtlasEntity(obj);

        entity.setAttribute(REQUEST_ID_PROPERTY, getUniqueValue(obj));
        if (obj.getImportResult() != null) {
            entity.setAttribute(IMPORT_RESULT_PROPERTY, AtlasType.toJson(obj.getImportResult()));
        }
        entity.setAttribute(IMPORT_ID_PROPERTY, obj.getImportId());
        entity.setAttribute(STATUS_PROPERTY, obj.getStatus());
        entity.setAttribute(IMPORT_DETAILS_PROPERTY, AtlasType.toJson(obj.getImportDetails()));
        entity.setAttribute(SKIP_TO_PROPERTY, String.valueOf(obj.getSkipTo()));

        entity.setAttribute(RECEIVED_AT_PROPERTY, String.valueOf(obj.getReceivedAt()));
        entity.setAttribute(STAGED_AT_PROPERTY, String.valueOf(obj.getStagedAt()));
        entity.setAttribute(STARTED_PROCESSING_AT, String.valueOf(obj.getStartedProcessingAt()));
        entity.setAttribute(COMPLETED_AT, String.valueOf(obj.getCompletedAt()));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasAsyncImportRequestDTO.toEntity() : {}", entity);
        }

        return entity;
    }

    @Override
    public AtlasEntityWithExtInfo toEntityWithExtInfo(AtlasAsyncImportRequest obj) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasAsyncImportRequestDTO.toEntityWithExtInfo({})", obj);
        }

        AtlasEntityWithExtInfo ret = new AtlasEntityWithExtInfo(toEntity(obj));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasAsyncImportRequestDTO.toEntityWithExtInfo() : {}", ret);
        }

        return ret;
    }

    @Override
    public Map<String, Object> getUniqueAttributes(AtlasAsyncImportRequest obj) {
        Map<String, Object> ret = new HashMap<>();
        if (obj.getImportId() != null) {
            ret.put(REQUEST_ID_PROPERTY, getUniqueValue(obj));
        }
        return ret;
    }

    private long objectToLong(Object object) {
        return Optional.ofNullable(object)
                .map(Object::toString)
                .map(Long::parseLong)
                .orElse(0L);
    }

    public static String convertToValidJson(String mapString) {
        // Remove enclosing braces
        String trimmed = mapString.trim();
        if (trimmed.startsWith("{")) {
            trimmed = trimmed.substring(1);
        }
        if (trimmed.endsWith("}")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }

        // Split into key-value pairs
        String[] keyValuePairs = trimmed.split(",\\s*(?![^\\[\\]]*\\])"); // Avoid splitting inside arrays

        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{");

        for (int i = 0; i < keyValuePairs.length; i++) {
            String[] keyValue = keyValuePairs[i].split("=", 2);
            String key = keyValue[0].trim();
            String value = keyValue[1].trim();

            // Add quotes around keys
            jsonBuilder.append("\"").append(key).append("\":");

            // Handle array values
            if (value.startsWith("[") && value.endsWith("]")) {
                // Process array elements
                String arrayContent = value.substring(1, value.length() - 1).trim(); // Remove brackets
                if (arrayContent.isEmpty()) {
                    // Empty array
                    jsonBuilder.append("[]");
                } else {
                    // Non-empty array
                    String[] arrayElements = arrayContent.split(",\\s*");
                    jsonBuilder.append("[");
                    for (int j = 0; j < arrayElements.length; j++) {
                        String element = arrayElements[j].trim();
                        if (isNumeric(element)) {
                            jsonBuilder.append(element);
                        } else {
                            jsonBuilder.append("\"").append(element).append("\"");
                        }
                        if (j < arrayElements.length - 1) {
                            jsonBuilder.append(",");
                        }
                    }
                    jsonBuilder.append("]");
                }
            } else if (isNumeric(value)) {
                // Add numeric values directly
                jsonBuilder.append(value);
            } else {
                // Add quotes around other values
                jsonBuilder.append("\"").append(value).append("\"");
            }

            // Add comma if not the last pair
            if (i < keyValuePairs.length - 1) {
                jsonBuilder.append(",");
            }
        }

        jsonBuilder.append("}");
        return jsonBuilder.toString();
    }

    private static boolean isNumeric(String value) {
        try {
            Double.parseDouble(value);
            return true; // If parsing is successful, it's numeric
        } catch (NumberFormatException e) {
            return false; // Otherwise, it's not numeric
        }
    }

    private String getUniqueValue(AtlasAsyncImportRequest obj) {
        return REQUEST_ID_PREFIX_PROPERTY + obj.getImportId() + "@" + AuditsWriter.getCurrentClusterName();
    }
}
