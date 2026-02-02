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

package org.apache.atlas.web.errors;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * AtlasBaseException mapper for Jersey.
 */
@Provider
@Component
public class AtlasBaseExceptionMapper implements ExceptionMapper<AtlasBaseException> {

    @Context
    private HttpServletRequest httpServletRequest;

    @Override
    public Response toResponse(AtlasBaseException exception) {
        // Log request body for bulk endpoints on error (reads from cached request)
        ExceptionMapperUtil.logRequestBodyOnError(httpServletRequest);

        // Only log the full exception stack trace for internal server errors
        if (exception.getAtlasErrorCode().getHttpCode() == Response.Status.INTERNAL_SERVER_ERROR) {
            ExceptionMapperUtil.logException(exception);
        }

        return buildAtlasBaseExceptionResponse(exception);
    }

    protected Response buildAtlasBaseExceptionResponse(AtlasBaseException baseException) {
        Map<String, Object> errorJsonMap = new LinkedHashMap<>();
        AtlasErrorCode errorCode = baseException.getAtlasErrorCode();
        errorJsonMap.put("errorCode", errorCode.getErrorCode());
        errorJsonMap.put("errorMessage", baseException.getMessage());

        if (StringUtils.isNotEmpty(baseException.getEntityGuid())) {
            errorJsonMap.put("entityGuid", baseException.getEntityGuid());
        }

        if (MapUtils.isNotEmpty(baseException.getErrorDetailsMap())) {
            errorJsonMap.put("errorDetailsMap", baseException.getErrorDetailsMap());
        }

        if (baseException.getCause() != null) {
            errorJsonMap.put("errorCause", baseException.getCause().getMessage());
        }

        Response.ResponseBuilder responseBuilder = Response.status(errorCode.getHttpCode());

        // No body for 204 (and maybe 304)
        if (Response.Status.NO_CONTENT != errorCode.getHttpCode()) {
            responseBuilder.entity(AtlasType.toJson(errorJsonMap));
        }
        return responseBuilder.build();
    }
}
