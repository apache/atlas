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

import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.testng.Assert.assertNotNull;

public class AtlasHeaderFilterTest {
    @Mock
    private FilterConfig mockFilterConfig;

    @Mock
    private ServletRequest mockServletRequest;

    @Mock
    private HttpServletResponse mockHttpServletResponse;

    @Mock
    private FilterChain mockFilterChain;

    @Mock
    private AtlasResponseRequestWrapper mockResponseWrapper;

    private AtlasHeaderFilter atlasHeaderFilter;
    private Map<String, String> originalHeaders;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        atlasHeaderFilter = new AtlasHeaderFilter();
        originalHeaders = new HashMap<>(HeadersUtil.getAllHeaders());
    }

    @AfterMethod
    public void tearDown() throws Exception {
        // Restore original headers
        HeadersUtil.initializeHttpResponseHeaders(convertMapToProperties(originalHeaders));
    }

    @Test
    public void testInit() {
        // Should not throw any exception
        atlasHeaderFilter.init(mockFilterConfig);

        verifyNoInteractions(mockFilterConfig);
    }

    @Test
    public void testDestroy() {
        // Should not throw any exception
        atlasHeaderFilter.destroy();
    }

    @Test(expectedExceptions = IOException.class)
    public void testDoFilterWithIOException() throws Exception {
        // Mock FilterChain to throw IOException
        doThrow(new IOException("Test exception")).when(mockFilterChain).doFilter(any(), any());

        atlasHeaderFilter.doFilter(mockServletRequest, mockHttpServletResponse, mockFilterChain);
    }

    @Test
    public void testSetHeaders() throws Exception {
        // Use MockedStatic to mock the static HeadersUtil.setSecurityHeaders method
        try (MockedStatic<HeadersUtil> mockedHeadersUtil = mockStatic(HeadersUtil.class)) {
            // Mock the static method
            mockedHeadersUtil.when(() -> HeadersUtil.setSecurityHeaders(any(AtlasResponseRequestWrapper.class)))
                    .thenAnswer(invocation -> {
                        AtlasResponseRequestWrapper wrapper = invocation.getArgument(0);
                        // Verify the wrapper was created with the correct response
                        assertNotNull(wrapper);
                        return null;
                    });

            // Call the method under test
            atlasHeaderFilter.setHeaders(mockHttpServletResponse);

            // Verify that HeadersUtil.setSecurityHeaders was called
            mockedHeadersUtil.verify(() -> HeadersUtil.setSecurityHeaders(any(AtlasResponseRequestWrapper.class)));
        }
    }

    @Test(expectedExceptions = ClassCastException.class)
    public void testDoFilterWithNonHttpServletResponse() throws Exception {
        // Create a mock ServletResponse that is not HttpServletResponse
        ServletResponse mockServletResponse = mock(ServletResponse.class);

        atlasHeaderFilter.doFilter(mockServletRequest, mockServletResponse, mockFilterChain);
    }

    @Test
    public void testFilterWithDifferentRequestTypes() throws Exception {
        // Test with different request types
        ServletRequest[] differentRequests = {
                mock(ServletRequest.class),
                mock(ServletRequest.class),
                mock(ServletRequest.class)
        };

        for (ServletRequest request : differentRequests) {
            atlasHeaderFilter.doFilter(request, mockHttpServletResponse, mockFilterChain);
        }

        // Verify filter chain was called for each request
        verify(mockFilterChain, times(3)).doFilter(any(), any());
    }

    private Properties convertMapToProperties(Map<String, String> map) {
        Properties props = new Properties();
        if (map != null) {
            map.forEach(props::setProperty);
        }
        return props;
    }
}
