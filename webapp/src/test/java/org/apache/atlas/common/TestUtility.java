/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.atlas.common;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;

import javax.servlet.http.HttpServletRequest;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestUtility {
    private TestUtility() {
    }

    public static void assertInvalidQueryLength(AtlasBaseException exception, String msgParams) {
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INVALID_QUERY_LENGTH);
        assertEquals(exception.getMessage(), AtlasErrorCode.INVALID_QUERY_LENGTH.getFormattedErrorMessage(msgParams));
    }

    public static void assertInvalidParamLength(AtlasBaseException exception, String paramName) {
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INVALID_QUERY_PARAM_LENGTH);
        assertEquals(exception.getMessage(), AtlasErrorCode.INVALID_QUERY_PARAM_LENGTH.getFormattedErrorMessage(paramName));
    }

    public static void assertInvalidParameters(AtlasBaseException exception, String msgParams) {
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INVALID_PARAMETERS);
        assertEquals(exception.getMessage(), AtlasErrorCode.INVALID_PARAMETERS.getFormattedErrorMessage(msgParams));
    }

    public static void assertBadRequests(AtlasBaseException exception, String msgParams) {
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.BAD_REQUEST);
        assertEquals(exception.getMessage(), AtlasErrorCode.BAD_REQUEST.getFormattedErrorMessage(msgParams));
    }

    public static void assertGUIDNotFoundException(AtlasBaseException exception, String... guids) {
        assertEquals(exception.getAtlasErrorCode(), AtlasErrorCode.INSTANCE_GUID_NOT_FOUND);
        assertEquals(exception.getMessage(), AtlasErrorCode.INSTANCE_GUID_NOT_FOUND.getFormattedErrorMessage(guids));
    }

    public static HttpServletRequest buildAndGetMockServletRequest(Map<String, String[]> params) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        if (params != null) {
            when(request.getParameterMap()).thenReturn(params);
            for (Map.Entry<String, String[]> entry : params.entrySet()) {
                when(request.getParameterValues(entry.getKey())).thenReturn(entry.getValue());
                when(request.getParameter(entry.getKey()))
                        .thenReturn(entry.getValue() != null && entry.getValue().length > 0 ? entry.getValue()[0] : null);
            }
        }
        return request;
    }

    public static String generateString(int n, char c) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
}
