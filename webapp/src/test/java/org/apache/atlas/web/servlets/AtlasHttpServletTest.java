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
package org.apache.atlas.web.servlets;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.web.filters.HeadersUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertTrue;

public class AtlasHttpServletTest {
    private AtlasHttpServlet servlet;

    private Map<String, String> originalHeaders;

    @Before
    public void beforeSetUp() throws NoSuchFieldException, IllegalAccessException {
        servlet = new AtlasHttpServlet();

        Field headerMapField = HeadersUtil.class.getDeclaredField("HEADER_MAP");
        headerMapField.setAccessible(true);
        originalHeaders = new HashMap<>((Map<String, String>) headerMapField.get(null));

        Map<String, String> headerMap = (Map<String, String>) headerMapField.get(null);
        headerMap.clear();
    }

    @After
    public void cleanUp() throws IllegalAccessException, NoSuchFieldException {
        Field headerMapField = HeadersUtil.class.getDeclaredField("HEADER_MAP");
        headerMapField.setAccessible(true);
        Map<String, String> headerMap = (Map<String, String>) headerMapField.get(null);
        headerMap.clear();
        headerMap.putAll(originalHeaders);
    }

    @Test
    public void loadsConfiguration() {
        PropertiesConfiguration baseConfig = new PropertiesConfiguration();
        Configuration spyConfig = Mockito.spy(baseConfig);

        Configuration mockSubset = mock(Configuration.class);
        Mockito.doReturn(mockSubset).when(spyConfig).subset("atlas.headers");

        Properties mockHeaders = new Properties();
        mockHeaders.put("X-Test", "Hello");

        try (MockedStatic<ApplicationProperties> mockedAppProps = Mockito.mockStatic(ApplicationProperties.class);
                MockedStatic<ConfigurationConverter> mockedConverter = Mockito.mockStatic(ConfigurationConverter.class)) {
            mockedAppProps.when(ApplicationProperties::get).thenReturn(spyConfig);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(mockSubset)).thenReturn(mockHeaders);

            try {
                servlet.init();
            } catch (Exception e) {
                fail("Expected no exception, but got: " + e.getMessage());
            }
        }
    }

    @Test
    public void shouldThrowExceptionOnFailConfiguration() {
        try (MockedStatic<ApplicationProperties> mockedAppProps = Mockito.mockStatic(ApplicationProperties.class)) {
            mockedAppProps.when(ApplicationProperties::get)
                    .thenThrow(new RuntimeException("Load error"));

            servlet = new AtlasHttpServlet();

            ServletException ex = assertThrows(ServletException.class, () -> servlet.init());
            assertTrue(ex.getMessage().contains("Failed to initialize AtlasHttpServlet"));
        }
    }

    @Test
    public void appliesHeadersAndIncludesTemplate() throws Exception {
        PropertiesConfiguration baseConfig = new PropertiesConfiguration();
        Configuration           configSpy  = Mockito.spy(baseConfig);
        Configuration           mockSubset = mock(Configuration.class);
        Mockito.doReturn(mockSubset).when(configSpy).subset("atlas.headers");

        Properties configHeaders = new Properties();
        configHeaders.put("X-Test-Config", "ConfigVal");

        Field headerMapField = HeadersUtil.class.getDeclaredField("HEADER_MAP");
        headerMapField.setAccessible(true);
        Map<String, String> headerMap = (Map<String, String>) headerMapField.get(null);
        headerMap.put("X-Static", "StaticVal");

        try (MockedStatic<ApplicationProperties>  mockedAppProps  = Mockito.mockStatic(ApplicationProperties.class);
                MockedStatic<ConfigurationConverter> mockedConverter = Mockito.mockStatic(ConfigurationConverter.class)) {
            mockedAppProps.when(ApplicationProperties::get).thenReturn(configSpy);
            mockedConverter.when(() -> ConfigurationConverter.getProperties(mockSubset)).thenReturn(configHeaders);

            HttpServletRequest  request            = mock(HttpServletRequest.class);
            HttpServletResponse response           = mock(HttpServletResponse.class);
            ServletContext      mockServletContext = mock(ServletContext.class);
            RequestDispatcher   dispatcher         = mock(RequestDispatcher.class);
            ServletConfig       servletConfig      = mock(ServletConfig.class);

            when(servletConfig.getServletContext()).thenReturn(mockServletContext);
            when(mockServletContext.getRequestDispatcher("/login.jsp")).thenReturn(dispatcher);

            servlet.init(servletConfig);
            servlet.includeResponse(request, response, "/login.jsp");

            verify(response).setContentType("text/html");
            verify(response).setHeader("X-Static", "StaticVal");
            verify(response).setHeader("X-Test-Config", "ConfigVal");

            verify(dispatcher).include(request, response);
        }
    }
}
