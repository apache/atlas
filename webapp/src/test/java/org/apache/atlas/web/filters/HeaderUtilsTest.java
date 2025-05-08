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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class HeaderUtilsTest {
    private Map<String, String> originalHeaders;

    @Before
    public void setUp() throws Exception {
        originalHeaders = new HashMap<>(HeadersUtil.getAllHeaders());
    }

    @After
    public void tearDown() throws Exception {
        HeadersUtil.initializeHttpResponseHeaders(convertMapToProperties(originalHeaders));
    }

    private Properties convertMapToProperties(Map<String, String> map) {
        Properties props = new Properties();

        map.forEach(props::setProperty);

        return props;
    }

    @Test
    public void testLoadHeadersFromProperties() {
        HeadersUtil.initializeHttpResponseHeaders(createPropertiesWithHeaders("X-Custom-Header-One", "ValueOne", "X-Custom-Header-Two", "ValueTwo"));

        assertEquals("ValueOne", HeadersUtil.getHeaderMap("X-Custom-Header-One"));
        assertEquals("ValueTwo", HeadersUtil.getHeaderMap("X-Custom-Header-Two"));
    }

    @Test
    public void testGetHeaderMapReturnsNullForMissingKey() {
        HeadersUtil.initializeHttpResponseHeaders(createPropertiesWithHeaders("X-Exists", "ExistsValue"));

        assertNull(HeadersUtil.getHeaderMap("X-Does-Not-Exist"));
    }

    @Test
    public void testSetSecurityHeadersSetsAllHeaders() {
        HeadersUtil.initializeHttpResponseHeaders(createPropertiesWithHeaders("X-One", "Val1", "X-Two", "Val2"));

        AtlasResponseRequestWrapper mockWrapper = mock(AtlasResponseRequestWrapper.class);
        HeadersUtil.setSecurityHeaders(mockWrapper);

        verify(mockWrapper).setHeader("X-One", "Val1");
        verify(mockWrapper).setHeader("X-Two", "Val2");
    }

    @Test
    public void testSetHeaderMapAttributes() {
        HeadersUtil.initializeHttpResponseHeaders(createPropertiesWithHeaders("X-Test", "HeaderTestValue"));

        AtlasResponseRequestWrapper mockWrapper = mock(AtlasResponseRequestWrapper.class);
        HeadersUtil.setHeaderMapAttributes(mockWrapper, "X-Test");

        verify(mockWrapper).setHeader("X-Test", "HeaderTestValue");
    }

    @Test
    public void testDefaultHeadersArePresent() {
        HeadersUtil.initializeHttpResponseHeaders(null);

        assertEquals("DENY", HeadersUtil.getHeaderMap(HeadersUtil.X_FRAME_OPTIONS_KEY));
        assertEquals("nosniff", HeadersUtil.getHeaderMap(HeadersUtil.X_CONTENT_TYPE_OPTIONS_KEY));
        assertEquals("1; mode=block", HeadersUtil.getHeaderMap(HeadersUtil.X_XSS_PROTECTION_KEY));
    }

    private Properties createPropertiesWithHeaders(String... headers) {
        Properties props = new Properties();

        for (int i = 0; i < headers.length / 2; i++) {
            props.setProperty(headers[i * 2], headers[(i * 2) + 1]);
        }

        return props;
    }
}
