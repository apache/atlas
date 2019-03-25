/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.filters;

import org.mockito.Mockito;
import org.testng.annotations.Test;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

import static org.mockito.Mockito.*;

public class AtlasCSRFPreventionFilterTest {
	private static final String EXPECTED_MESSAGE = "Missing Required Header for CSRF Vulnerability Protection";
	private static final String X_CUSTOM_HEADER = "X-CUSTOM_HEADER";
	private String userAgent = "Mozilla";
	
	@Test
	public void testNoHeaderDefaultConfig_badRequest() throws ServletException, IOException {
		// CSRF has not been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_DEFAULT)).thenReturn(null);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);		

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		PrintWriter mockWriter = Mockito.mock(PrintWriter.class);
		Mockito.when(mockRes.getWriter()).thenReturn(mockWriter);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		verify(mockRes, atLeastOnce()).setStatus(HttpServletResponse.SC_BAD_REQUEST);
		Mockito.verifyZeroInteractions(mockChain);
	}
	
	@Test
	public void testHeaderPresentDefaultConfig_goodRequest() throws ServletException, IOException {
		// CSRF HAS been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_DEFAULT)).thenReturn("valueUnimportant");
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		Mockito.verify(mockChain).doFilter(mockReq, mockRes);
	}

	@Test
	public void testHeaderPresentCustomHeaderConfig_goodRequest() throws ServletException, IOException {
		// CSRF HAS been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(X_CUSTOM_HEADER)).thenReturn("valueUnimportant");

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		Mockito.verify(mockChain).doFilter(mockReq, mockRes);
	}

	@Test
	public void testMissingHeaderWithCustomHeaderConfig_badRequest() throws ServletException, IOException {
		// CSRF has not been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(X_CUSTOM_HEADER)).thenReturn(null);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		PrintWriter mockWriter = Mockito.mock(PrintWriter.class);
		Mockito.when(mockRes.getWriter()).thenReturn(mockWriter);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		Mockito.verifyZeroInteractions(mockChain);
	}

	@Test
	public void testMissingHeaderIgnoreGETMethodConfig_goodRequest()
			throws ServletException, IOException {
		// CSRF has not been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_DEFAULT)).thenReturn(null);
		Mockito.when(mockReq.getMethod()).thenReturn("GET");
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		Mockito.verify(mockChain).doFilter(mockReq, mockRes);
	}

	@Test
	public void testMissingHeaderMultipleIgnoreMethodsConfig_badRequest()
			throws ServletException, IOException {
		// CSRF has not been sent
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_DEFAULT))
				.thenReturn(null);
		Mockito.when(mockReq.getMethod()).thenReturn("PUT");
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);

		// Objects to verify interactions based on request
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		PrintWriter mockWriter = Mockito.mock(PrintWriter.class);
		Mockito.when(mockRes.getWriter()).thenReturn(mockWriter);

		FilterChain mockChain = Mockito.mock(FilterChain.class);

		// Object under test
		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(mockReq, mockRes, mockChain);

		Mockito.verifyZeroInteractions(mockChain);
	}

	@Test
	public void testMethodsToIgnoreMultiValue()
			throws ServletException, IOException {
		// -- delete requests should be blocked since it has not been configured to be ignored in the atlas-application.properties
		HttpServletRequest req = createRequest("DELETE");
		HttpServletResponse res = createResponse();

		// -- Objects to verify interactions based on request
		FilterChain chain = Mockito.mock(FilterChain.class);

		AtlasCSRFPreventionFilter filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(req, res, chain);
		Mockito.verifyZeroInteractions(chain);

		// -- options requests should pass since it has been configured to be ignored in the atlas-application.properties
		req = createRequest("OPTIONS");
		res = createResponse();

		// -- Objects to verify interactions based on request
		chain = Mockito.mock(FilterChain.class);

		filter = new AtlasCSRFPreventionFilter();
		filter.doFilter(req, res, chain);
		Mockito.atLeastOnce();

		verify(chain, times(1)).doFilter(req, res);
	}

	private HttpServletRequest createRequest(String method) {
		HttpServletRequest mockReq = Mockito.mock(HttpServletRequest.class);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_DEFAULT))
				.thenReturn(null);
		Mockito.when(mockReq.getMethod()).thenReturn(method);
		Mockito.when(mockReq.getHeader(AtlasCSRFPreventionFilter.HEADER_USER_AGENT)).thenReturn(userAgent);

		return mockReq;
	}

	private HttpServletResponse createResponse() throws IOException {
		HttpServletResponse mockRes = Mockito.mock(HttpServletResponse.class);
		PrintWriter mockWriter = Mockito.mock(PrintWriter.class);
		Mockito.when(mockRes.getWriter()).thenReturn(mockWriter);

		return mockRes;
	}
}
