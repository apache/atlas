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

import org.apache.atlas.server.common.filters.HeadersUtil;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import javax.servlet.FilterChain;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

public class CspNonceHtmlFilterTest {
    @Test
    public void testDoFilterInjectsNonceForHtmlResponse() throws Exception {
        HttpServletRequest  request  = Mockito.mock(HttpServletRequest.class);
        HttpServletResponse response = Mockito.mock(HttpServletResponse.class);
        FilterChain         chain    = Mockito.mock(FilterChain.class);

        String cspNonce = "serverNonce123";
        ByteArrayOutputStream responseBytes = new ByteArrayOutputStream();

        Mockito.when(request.getRequestURI()).thenReturn("/n3/index.html");
        Mockito.when(request.getAttribute(HeadersUtil.CONTENT_SEC_POLICY_NONCE_REQUEST_ATTRIBUTE)).thenReturn(cspNonce);
        Mockito.when(response.getOutputStream()).thenReturn(new TestServletOutputStream(responseBytes));
        Mockito.when(response.isCommitted()).thenReturn(false);
        Mockito.when(response.getCharacterEncoding()).thenReturn("UTF-8");

        Mockito.doAnswer(invocation -> {
            ServletResponse wrappedResponse = invocation.getArgument(1);
            wrappedResponse.setContentType("text/html");
            wrappedResponse.getWriter().write("<html><head><meta name=\"csp-nonce\" content=\"\" /></head><body><script>init()</script></body></html>");
            return null;
        }).when(chain).doFilter(any(), any());

        CspNonceHtmlFilter filter = new CspNonceHtmlFilter();
        filter.doFilter(request, response, chain);

        String output = responseBytes.toString(StandardCharsets.UTF_8.name());
        verify(response, atLeastOnce()).setContentLength(Mockito.anyInt());

        org.testng.Assert.assertTrue(output.contains("content=\"serverNonce123\""));
        org.testng.Assert.assertTrue(output.contains("<script nonce=\"serverNonce123\">init()</script>"));
    }

    private static class TestServletOutputStream extends ServletOutputStream {
        private final ByteArrayOutputStream outputStream;

        private TestServletOutputStream(ByteArrayOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        @Override
        public void write(int b) {
            outputStream.write(b);
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
            // No-op in unit test stream.
        }
    }
}
