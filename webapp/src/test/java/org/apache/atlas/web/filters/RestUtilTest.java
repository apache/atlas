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
package org.apache.atlas.web.filters;

import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import java.util.Collections;
import java.util.Enumeration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class RestUtilTest {
    private static <T> Enumeration<T> enumeration(T... values) {
        return Collections.enumeration(java.util.Arrays.asList(values));
    }

    @Test
    public void testConstructForwardableURL_withProtoHostContext() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.getHeaderNames()).thenReturn(enumeration(
                "x-forwarded-proto", "x-forwarded-host", "x-forwarded-context"));
        when(request.getHeaders("x-forwarded-proto")).thenReturn(enumeration("https"));
        when(request.getHeaders("x-forwarded-host")).thenReturn(enumeration("example.com"));
        when(request.getHeaders("x-forwarded-context")).thenReturn(enumeration("/custom"));

        when(request.getRequestURI()).thenReturn("/api/test");

        String result = RestUtil.constructForwardableURL(request);

        assertEquals(result, "https://example.com/custom/atlas/api/test");
    }

    @Test
    public void testConstructForwardableURL_withMultipleHosts() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.getHeaderNames()).thenReturn(enumeration("x-forwarded-proto", "x-forwarded-host"));
        when(request.getHeaders("x-forwarded-proto")).thenReturn(enumeration("http"));
        when(request.getHeaders("x-forwarded-host")).thenReturn(enumeration("host1.com, host2.com"));

        when(request.getRequestURI()).thenReturn("/data");

        String result = RestUtil.constructForwardableURL(request);

        assertEquals(result, "http://host1.com/data");
    }

    @Test
    public void testConstructForwardableURL_withOnlyProto() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.getHeaderNames()).thenReturn(enumeration("x-forwarded-proto"));
        when(request.getHeaders("x-forwarded-proto")).thenReturn(enumeration("https"));
        when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:21000/atlas/api"));

        String result = RestUtil.constructForwardableURL(request);

        assertEquals(result, "https://localhost:21000/atlas/api");
    }

    @Test
    public void testConstructForwardableURL_withNoForwardHeaders() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.getHeaderNames()).thenReturn(Collections.emptyEnumeration());
        when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:21000/atlas/api"));

        String result = RestUtil.constructForwardableURL(request);

        assertEquals(result, "");
    }

    @Test
    public void testConstructRedirectURL_withForwardedURL() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        String result = RestUtil.constructRedirectURL(
                request,
                "http://login.com/auth",
                "https://forwarded.com/atlas/api",
                "originalUrl");

        assertEquals(result, "http://login.com/auth?originalUrl=https://forwarded.com/atlas/api");
    }

    @Test
    public void testConstructRedirectURL_withoutForwardedURL() {
        HttpServletRequest request = mock(HttpServletRequest.class);

        when(request.getRequestURL()).thenReturn(new StringBuffer("http://localhost:21000/atlas/api"));
        when(request.getQueryString()).thenReturn("param=1");

        String result = RestUtil.constructRedirectURL(
                request,
                "http://login.com/auth",
                null,
                "originalUrl");

        assertEquals(result, "http://login.com/auth?originalUrl=http://localhost:21000/atlas/api?param=1");
    }
}
