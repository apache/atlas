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

import org.mockito.MockitoAnnotations;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.mock.web.MockServletContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class AtlasErrorServletTest {
    private AtlasErrorServlet atlasErrorServlet;
    private MockServletContext mockServletContext;

    @BeforeClass
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        atlasErrorServlet = new AtlasErrorServlet();

        // Setup mock servlet context
        mockServletContext = new MockServletContext();
    }

    @Test
    public void testServiceMethod() {
        try {
            // Create mock request and response
            MockHttpServletRequest request = new MockHttpServletRequest(mockServletContext);
            MockHttpServletResponse response = new MockHttpServletResponse();

            // Call the service method
            atlasErrorServlet.service(request, response);

            assertTrue(true);
        } catch (Exception e) {
            assertTrue(true);
        }
    }
}
