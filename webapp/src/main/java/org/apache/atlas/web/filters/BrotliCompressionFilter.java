package org.apache.atlas.web.filters;

import com.aayushatharva.brotli4j.Brotli4jLoader;
import com.aayushatharva.brotli4j.encoder.Encoder;
import com.aayushatharva.brotli4j.encoder.Encoder.Parameters;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class BrotliCompressionFilter implements Filter {

    @Override
    public void init(FilterConfig filterConfig) {
        Brotli4jLoader.ensureAvailability();
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        // Ensure request and response are HttpServletRequest and HttpServletResponse
        if (request instanceof HttpServletRequest && response instanceof HttpServletResponse) {
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            HttpServletResponse httpResponse = (HttpServletResponse) response;

            String acceptEncoding = httpRequest.getHeader("Accept-Encoding");

            // Check if the client supports Brotli compression
            if (acceptEncoding != null && acceptEncoding.contains("br")) {
                // Wrap the response with a Brotli compression wrapper
                BrotliResponseWrapper responseWrapper = new BrotliResponseWrapper(httpResponse);
                chain.doFilter(request, responseWrapper);

                // Compress the response content with Brotli
                byte[] uncompressedData = responseWrapper.getOutputStreamData();
                Parameters params = new Parameters().setQuality(6); // Set Brotli quality level
                byte[] compressedOutput = Encoder.compress(uncompressedData, params);

                // Write Brotli-compressed data to the actual response
                httpResponse.setHeader("Content-Encoding", "br");
                httpResponse.setContentLength(compressedOutput.length);
                httpResponse.getOutputStream().write(compressedOutput);
            } else {
                // Proceed without compression
                chain.doFilter(request, response);
            }
        } else {
            // Proceed without compression if not HTTP
            chain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {
        // Optional: Add cleanup logic here if needed
    }
}
