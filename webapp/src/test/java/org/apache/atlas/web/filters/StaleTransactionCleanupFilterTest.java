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

import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StaleTransactionCleanupFilterTest {
    private StaleTransactionCleanupFilter filter;
    private ServletRequest mockRequest;
    private ServletResponse mockResponse;
    private FilterChain mockChain;
    private AtlasGraph<?, ?> mockGraph;

    @BeforeMethod
    public void setUp() {
        filter = new StaleTransactionCleanupFilter();
        mockRequest = mock(ServletRequest.class);
        mockResponse = mock(ServletResponse.class);
        mockChain = mock(FilterChain.class);
        mockGraph = mock(AtlasGraph.class);
    }

    @Test
    public void testDoFilter_RollbackSuccess() throws Exception {
        try (MockedStatic<AtlasGraphProvider> mockedProvider = Mockito.mockStatic(AtlasGraphProvider.class)) {
            mockedProvider.when(AtlasGraphProvider::getGraphInstance).thenReturn(mockGraph);

            filter.doFilter(mockRequest, mockResponse, mockChain);

            verify(mockGraph, times(1)).rollback();
            verify(mockChain, times(1)).doFilter(mockRequest, mockResponse);
        }
    }

    @Test
    public void testInitAndDestroy() throws Exception {
        filter.init(null);
        filter.destroy();
    }
}
