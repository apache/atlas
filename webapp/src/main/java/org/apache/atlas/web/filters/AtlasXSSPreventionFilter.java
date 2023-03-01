package org.apache.atlas.web.filters;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.web.util.CachedBodyHttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.jsoup.safety.Safelist;
import org.jsoup.Jsoup;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.regex.Pattern;

@Component
public class AtlasXSSPreventionFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasCSRFPreventionFilter.class);
    private static Pattern pattern;
    private static final String MASK_STRING = "##ATLAN##";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String ERROR_INVALID_CHARACTERS = "invalid characters in the request body (XSS Filter)";

    @Inject
    public AtlasXSSPreventionFilter() throws ServletException {
        LOG.info("AtlasXSSPreventionFilter initialized");
        try {
            init(null);
        } catch (ServletException e) {
            LOG.error("Error while initializing AtlasXSSPreventionFilter", e);
            throw e;
        }
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        pattern     = Pattern.compile(AtlasConfiguration.REST_API_XSS_FILTER_MASK_STRING.getString());

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        HttpServletRequest request = (HttpServletRequest) servletRequest;

        response.setHeader("Content-Type", CONTENT_TYPE_JSON);

        String method = request.getMethod();
        if(!method.equals("POST") && !method.equals("PUT")) {
            filterChain.doFilter(request, response);
            return;
        }

        String contentType = request.getContentType();
        if(StringUtils.isEmpty(contentType) || !contentType.contains(CONTENT_TYPE_JSON)) {
            filterChain.doFilter(request, response);
            return;
        }

        CachedBodyHttpServletRequest cachedBodyHttpServletRequest = new CachedBodyHttpServletRequest(request);
        String body = IOUtils.toString(cachedBodyHttpServletRequest.getInputStream(), "UTF-8");
        String reqBodyStr = pattern.matcher(body).replaceAll(MASK_STRING);
        Safelist safelist = Safelist.relaxed();

        if(!Jsoup.isValid(reqBodyStr, safelist)) {
            response.setStatus(400);
            response.getWriter().write(getErrorMessages(ERROR_INVALID_CHARACTERS));
            return;
        }

        filterChain.doFilter(cachedBodyHttpServletRequest, response);

    }



    @Override
    public void destroy() {
        LOG.info("AtlasXSSPreventionFilter destroyed");
    }



    private String getErrorMessages(String err) {
        return "{\"code\":1000,\"error\":\"XSS\",\"message\":\"" + err + "\"}";
    }
}
