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
import org.apache.commons.lang3.StringUtils;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class CspNonceHtmlFilter implements Filter {
    private static final String HTML_CONTENT_TYPE = "text/html";

    private final CspNonceHtmlProcessor htmlProcessor = new CspNonceHtmlProcessor();

    @Override
    public void init(FilterConfig filterConfig) {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (!(request instanceof HttpServletRequest) || !(response instanceof HttpServletResponse)) {
            chain.doFilter(request, response);
            return;
        }

        HttpServletRequest          httpRequest  = (HttpServletRequest) request;
        HttpServletResponse         httpResponse = (HttpServletResponse) response;
        BufferedHttpServletResponse wrappedResponse = new BufferedHttpServletResponse(httpResponse);

        chain.doFilter(request, wrappedResponse);

        byte[] responseBody = wrappedResponse.getCapturedBody();
        if (httpResponse.isCommitted()) {
            return;
        }

        if (responseBody.length == 0) {
            return;
        }

        String cspNonce = HeadersUtil.getOrCreateCspNonce(httpRequest);
        byte[] output = responseBody;

        if (isHtmlResponse(httpRequest, wrappedResponse) && StringUtils.isNotBlank(cspNonce)) {
            Charset responseCharset = resolveCharset(wrappedResponse.getCharacterEncoding());
            String  htmlBody        = new String(responseBody, responseCharset);
            String  updatedHtmlBody = htmlProcessor.injectNonce(htmlBody, cspNonce);

            output = updatedHtmlBody.getBytes(responseCharset);
            httpResponse.setContentLength(output.length);
        }

        ServletOutputStream responseOutputStream = httpResponse.getOutputStream();
        responseOutputStream.write(output);
        responseOutputStream.flush();
    }

    @Override
    public void destroy() {
    }

    private boolean isHtmlResponse(HttpServletRequest request, HttpServletResponse response) {
        String contentType = response.getContentType();
        if (StringUtils.isNotBlank(contentType) && contentType.toLowerCase(Locale.ROOT).contains(HTML_CONTENT_TYPE)) {
            return true;
        }

        String requestUri = request.getRequestURI();
        return StringUtils.endsWithIgnoreCase(requestUri, ".html")
                || StringUtils.endsWithIgnoreCase(requestUri, ".jsp");
    }

    private Charset resolveCharset(String charset) {
        if (StringUtils.isBlank(charset)) {
            return StandardCharsets.UTF_8;
        }

        try {
            return Charset.forName(charset);
        } catch (Exception e) {
            return StandardCharsets.UTF_8;
        }
    }

    private static class BufferedHttpServletResponse extends HttpServletResponseWrapper {
        private final ByteArrayOutputStream outputStreamBuffer = new ByteArrayOutputStream();
        private ServletOutputStream         servletOutputStream;
        private PrintWriter                 printWriter;

        BufferedHttpServletResponse(HttpServletResponse response) {
            super(response);
        }

        @Override
        public ServletOutputStream getOutputStream() {
            if (printWriter != null) {
                throw new IllegalStateException("getWriter() has already been called for this response");
            }

            if (servletOutputStream == null) {
                servletOutputStream = new BufferedServletOutputStream(outputStreamBuffer);
            }

            return servletOutputStream;
        }

        @Override
        public PrintWriter getWriter() throws IOException {
            if (servletOutputStream != null) {
                throw new IllegalStateException("getOutputStream() has already been called for this response");
            }

            if (printWriter == null) {
                Charset charset = StandardCharsets.UTF_8;
                if (StringUtils.isNotBlank(getCharacterEncoding())) {
                    try {
                        charset = Charset.forName(getCharacterEncoding());
                    } catch (Exception ignored) {
                        charset = StandardCharsets.UTF_8;
                    }
                }

                printWriter = new PrintWriter(new OutputStreamWriter(outputStreamBuffer, charset), true);
            }

            return printWriter;
        }

        @Override
        public void flushBuffer() throws IOException {
            if (printWriter != null) {
                printWriter.flush();
            }

            if (servletOutputStream != null) {
                servletOutputStream.flush();
            }
        }

        byte[] getCapturedBody() throws IOException {
            flushBuffer();
            return outputStreamBuffer.toByteArray();
        }
    }

    private static class BufferedServletOutputStream extends ServletOutputStream {
        private final ByteArrayOutputStream outputStream;

        BufferedServletOutputStream(ByteArrayOutputStream outputStream) {
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
            // No-op: this wrapper does not support async I/O callbacks.
        }
    }
}
