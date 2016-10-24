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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import javax.inject.Singleton;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Exception mapper for Jersey.
 * @param <E>
 */
@Provider
@Singleton
public class AtlasBaseExceptionMapper implements ExceptionMapper<AtlasBaseException> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AtlasBaseExceptionMapper.class);

    @Override
    public Response toResponse(AtlasBaseException exception) {
        final long id = ThreadLocalRandom.current().nextLong();

        // Log the response and use the error codes from the Exception
        logException(id, exception);
        return buildAtlasBaseExceptionResponse((AtlasBaseException) exception);
    }

    protected Response buildAtlasBaseExceptionResponse(AtlasBaseException baseException) {
        Map<String, String> errorJsonMap = new LinkedHashMap<>();
        AtlasErrorCode errorCode = baseException.getAtlasErrorCode();
        errorJsonMap.put("errorCode", errorCode.getErrorCode());
        errorJsonMap.put("errorMessage", baseException.getMessage());
        Response.ResponseBuilder responseBuilder = Response.status(errorCode.getHttpCode());

        // No body for 204 (and maybe 304)
        if (Response.Status.NO_CONTENT != errorCode.getHttpCode()) {
            responseBuilder.entity(AtlasType.toJson(errorJsonMap));
        }
        return responseBuilder.build();
    }

    @SuppressWarnings("UnusedParameters")
    protected String formatErrorMessage(long id, AtlasBaseException exception) {
        return String.format("There was an error processing your request. It has been logged (ID %016x).", id);
    }

    protected void logException(long id, AtlasBaseException exception) {
        LOGGER.error(formatLogMessage(id, exception), exception);
    }

    @SuppressWarnings("UnusedParameters")
    protected String formatLogMessage(long id, Throwable exception) {
        return String.format("Error handling a request: %016x", id);
    }

}
