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

import org.apache.atlas.AtlasConfiguration;
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

import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportDetails;
import static org.apache.atlas.model.impexp.AtlasAsyncImportRequest.ImportStatus;

/**
 * AtlasAsyncImportRequestDTO is the bridge class between AtlasAsyncImportRequest and AtlasEntity.
 */
@Component
public class AtlasAsyncImportRequestDTO extends AbstractDataTransferObject<AtlasAsyncImportRequest> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAsyncImportRequestDTO.class);

    public static final String ASYNC_IMPORT_TYPE_NAME         = "__AtlasAsyncImportRequest";
    public static final String IMPORT_RESULT_PROPERTY         = "importResult";
    public static final String REQUEST_ID_PROPERTY            = "requestId";
    public static final String IMPORT_ID_PROPERTY             = "importId";
    public static final String START_ENTITY_POSITION_PROPERTY = "startEntityPosition";
    public static final String STATUS_PROPERTY                = "status";
    public static final String IMPORT_DETAILS_PROPERTY        = "importDetails";
    public static final String RECEIVED_TIME_PROPERTY         = "receivedTime";
    public static final String STAGED_TIME_PROPERTY           = "stagedTime";
    public static final String PROCESSING_START_TIME          = "processingStartTime";
    public static final String COMPLETED_TIME                 = "completedTime";

    @Inject
    public AtlasAsyncImportRequestDTO(AtlasTypeRegistry typeRegistry) {
        super(typeRegistry, AtlasAsyncImportRequest.class, ASYNC_IMPORT_TYPE_NAME);
    }

    @Override
    public AtlasAsyncImportRequest from(AtlasEntity entity) {
        LOG.debug("==> AtlasAsyncImportRequestDTO.from({})", entity);

        AtlasAsyncImportRequest asyncImportRequest = null;
        String                  jsonImportResult   = (String) entity.getAttribute(IMPORT_RESULT_PROPERTY);

        if (StringUtils.isEmpty(jsonImportResult)) {
            LOG.error("AtlasAsyncImportRequest.from(entity={}): empty/null value in attribute {}", entity, IMPORT_RESULT_PROPERTY);
        } else {
            String requestId           = (String) entity.getAttribute(REQUEST_ID_PROPERTY);
            String importId            = (String) entity.getAttribute(IMPORT_ID_PROPERTY);
            String status              = (String) entity.getAttribute(STATUS_PROPERTY);
            int    startEntityPosition = Integer.parseInt((String) entity.getAttribute(START_ENTITY_POSITION_PROPERTY));
            String jsonImportDetails   = (String) entity.getAttribute(IMPORT_DETAILS_PROPERTY);
            long   receivedTime        = objectToLong(entity.getAttribute(RECEIVED_TIME_PROPERTY));
            long   stagedTime          = objectToLong(entity.getAttribute(STAGED_TIME_PROPERTY));
            long   processingStartTime = objectToLong(entity.getAttribute(PROCESSING_START_TIME));
            long   completedTime       = objectToLong(entity.getAttribute(COMPLETED_TIME));

            asyncImportRequest = new AtlasAsyncImportRequest(AtlasType.fromJson(jsonImportResult, AtlasImportResult.class));

            asyncImportRequest.setGuid(entity.getGuid());
            asyncImportRequest.getImportTrackingInfo().setRequestId(requestId);
            asyncImportRequest.setImportId(importId);
            asyncImportRequest.getImportTrackingInfo().setStartEntityPosition(startEntityPosition);
            asyncImportRequest.setStatus(ImportStatus.valueOf(status));
            asyncImportRequest.setImportDetails(StringUtils.isNotEmpty(jsonImportDetails) ? AtlasType.fromJson(jsonImportDetails, ImportDetails.class) : null);
            asyncImportRequest.setReceivedTime(receivedTime);
            asyncImportRequest.setStagedTime(stagedTime);
            asyncImportRequest.setProcessingStartTime(processingStartTime);
            asyncImportRequest.setCompletedTime(completedTime);
        }

        LOG.debug("<== AtlasAsyncImportRequestDTO.from(entity={}): ret={}", entity, asyncImportRequest);

        return asyncImportRequest;
    }

    @Override
    public AtlasAsyncImportRequest from(AtlasEntityWithExtInfo entityWithExtInfo) {
        LOG.debug("==> AtlasAsyncImportRequestDTO.from(entity={})", entityWithExtInfo);

        AtlasAsyncImportRequest request = from(entityWithExtInfo.getEntity());

        LOG.debug("<== AtlasAsyncImportRequestDTO.from(entity={}): ret={}", entityWithExtInfo, request);

        return request;
    }

    @Override
    public AtlasEntity toEntity(AtlasAsyncImportRequest obj) throws AtlasBaseException {
        LOG.debug("==> AtlasAsyncImportRequestDTO.toEntity(obj={})", obj);

        AtlasEntity entity = getDefaultAtlasEntity(obj);

        entity.setAttribute(REQUEST_ID_PROPERTY, getUniqueValue(obj));

        if (obj.getImportResult() != null) {
            entity.setAttribute(IMPORT_RESULT_PROPERTY, AtlasType.toJson(obj.getImportResult()));
        }

        entity.setAttribute(IMPORT_ID_PROPERTY, obj.getImportId());
        entity.setAttribute(STATUS_PROPERTY, obj.getStatus());
        entity.setAttribute(IMPORT_DETAILS_PROPERTY, AtlasType.toJson(obj.getImportDetails()));
        entity.setAttribute(START_ENTITY_POSITION_PROPERTY, String.valueOf(obj.getImportTrackingInfo().getStartEntityPosition()));
        entity.setAttribute(RECEIVED_TIME_PROPERTY, String.valueOf(obj.getReceivedTime()));
        entity.setAttribute(STAGED_TIME_PROPERTY, String.valueOf(obj.getStagedTime()));
        entity.setAttribute(PROCESSING_START_TIME, String.valueOf(obj.getProcessingStartTime()));
        entity.setAttribute(COMPLETED_TIME, String.valueOf(obj.getCompletedTime()));

        LOG.debug("<== AtlasAsyncImportRequestDTO.toEntity(obj={}): ret={}", obj, entity);

        return entity;
    }

    @Override
    public AtlasEntityWithExtInfo toEntityWithExtInfo(AtlasAsyncImportRequest obj) throws AtlasBaseException {
        LOG.debug("==> AtlasAsyncImportRequestDTO.toEntityWithExtInfo(obj={})", obj);

        AtlasEntityWithExtInfo ret = new AtlasEntityWithExtInfo(toEntity(obj));

        LOG.debug("<== AtlasAsyncImportRequestDTO.toEntityWithExtInfo(obj={}): ret={}", obj, ret);

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
        String trimmed = mapString.trim();

        if (trimmed.startsWith("{")) {
            trimmed = trimmed.substring(1);
        }

        if (trimmed.endsWith("}")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }

        String[]      keyValuePairs = trimmed.split(",\\s*(?![^\\[\\]]*\\])");
        StringBuilder jsonBuilder   = new StringBuilder();

        jsonBuilder.append("{");

        for (int i = 0; i < keyValuePairs.length; i++) {
            String[] keyValue = keyValuePairs[i].split("=", 2);
            String   key      = keyValue[0].trim();
            String   value    = keyValue[1].trim();

            jsonBuilder.append("\"").append(key).append("\":");

            if (value.startsWith("[") && value.endsWith("]")) {
                String arrayContent = value.substring(1, value.length() - 1).trim();

                if (arrayContent.isEmpty()) {
                    jsonBuilder.append("[]");
                } else {
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
                jsonBuilder.append(value);
            } else {
                jsonBuilder.append("\"").append(value).append("\"");
            }

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
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private String getUniqueValue(AtlasAsyncImportRequest obj) {
        return AtlasConfiguration.ASYNC_IMPORT_REQUEST_ID_PREFIX.getString() + obj.getImportId() + "@" + AuditsWriter.getCurrentClusterName();
    }
}
