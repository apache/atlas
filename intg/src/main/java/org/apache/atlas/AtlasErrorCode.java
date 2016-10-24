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
package org.apache.atlas;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.Arrays;

import javax.ws.rs.core.Response;

public enum AtlasErrorCode {
    NO_SEARCH_RESULTS(204, "ATLAS2041E", "Given search filter did not yield any results"),

    UNKNOWN_TYPE(400, "ATLAS4001E", "Unknown type {0} for {1}.{2}"),
    TYPE_NAME_NOT_FOUND(404, "ATLAS4041E", "Given typename {0} was invalid"),
    TYPE_GUID_NOT_FOUND(404, "ATLAS4042E", "Given type guid {0} was invalid"),
    EMPTY_RESULTS(404, "ATLAS4044E", "No result found for {0}"),

    TYPE_ALREADY_EXISTS(409, "ATLAS4091E", "Given type {0} already exists"),
    TYPE_HAS_REFERENCES(409, "ATLAS4092E", "Given type {0} has references"),
    TYPE_MATCH_FAILED(409, "ATLAS4093E", "Given type {0} doesn't match {1}"),

    INTERNAL_ERROR(500, "ATLAS5001E", "Internal server error {0}");

    private String errorCode;
    private String errorMessage;
    private Response.Status httpCode;

    private static final Logger LOG = LoggerFactory.getLogger(AtlasErrorCode.class);

    AtlasErrorCode(int httpCode, String errorCode, String errorMessage) {
        this.httpCode = Response.Status.fromStatusCode(httpCode);
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;

    }

    public String getFormattedErrorMessage(String... params) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("<== AtlasErrorCode.getMessage(%s)", Arrays.toString(params)));
        }

        MessageFormat mf = new MessageFormat(errorMessage);
        String result = mf.format(params);

        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("==> AtlasErrorCode.getMessage(%s): %s", Arrays.toString(params), result));
        }
        return result;
    }

    public Response.Status getHttpCode() {
        return httpCode;
    }

    public String getErrorCode() {
        return errorCode;
    }
}
