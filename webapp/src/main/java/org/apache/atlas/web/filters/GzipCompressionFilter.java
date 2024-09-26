package org.apache.atlas.web.filters;

import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class GzipCompressionFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(GzipCompressionFilter.class);

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("GzipCompressionFilter initialized");
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        if (httpResponse.getHeader("Content-Encoding") == null) {
            GzipResponseWrapper gzipResponseWrapper = new GzipResponseWrapper(httpResponse);
            chain.doFilter(request, gzipResponseWrapper);
            gzipResponseWrapper.close();
        } else {
            chain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {
        
    }
}