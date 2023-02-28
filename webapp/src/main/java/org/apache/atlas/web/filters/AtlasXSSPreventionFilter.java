package org.apache.atlas.web.filters;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.web.util.CachedBodyHttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Pattern;

@Component
public class AtlasXSSPreventionFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasCSRFPreventionFilter.class);
    private static Pattern pattern;
    private PolicyFactory policy;
    private static final String MASK_STRING = "##ATLAN##";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String ERROR_INVALID_CHARACTERS = "invalid characters in the request body (XSS Filter)";
    //Init the regex map with default values
    private static final HashMap<String, Pattern> REGEX_MAPS = new HashMap<String, Pattern> () {{
        put("NUMBER",                   Pattern.compile("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$"));
        put("INTEGER",                  Pattern.compile("^[0-9]+$"));
        put("ISO8601",                  Pattern.compile("^[0-9]{4}(-[0-9]{2}(-[0-9]{2}([ T][0-9]{2}(:[0-9]{2}){1,2}(.[0-9]{1,6})"+
                "?Z?([\\+-][0-9]{2}:[0-9]{2})?)?)?)?$"));
        put("PARAGRAPH",                Pattern.compile("^[\\p{L}\\p{N}\\s\\-_',\\[\\]!\\./\\\\\\(\\)]*$"));
        put("SPACE_SEPARATED_TOKENS",   Pattern.compile("^([\\s\\p{L}\\p{N}_-]+)$"));
        put("DIRECTION",                Pattern.compile("\"(?i)^(rtl|ltr)$\""));
        put("IMAGE_ALIGNMENT",          Pattern.compile("(?i)^(left|right|top|texttop|middle|absmiddle|baseline|bottom|absbottom)$"));
        put("NUMBER_OR_PERCENT",        Pattern.compile("^[0-9]+[%]?$"));
        put("LIST_TYPE",                Pattern.compile("(?i)^(circle|disc|square|a|A|i|I|1)$"));
        put("CELL_ALIGN",               Pattern.compile("(?i)^(center|justify|left|right|char)$"));
        put("CELL_VERTICAL_ALIGN",              Pattern.compile("(?i)^(top|middle|bottom|baseline)$"));
        put("SHAPE",                    Pattern.compile("(?i)^(rect|circle|poly|default)$"));


    }};

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

        policy = new HtmlPolicyBuilder()
                // "class" is not permitted as we are not allowing users to style their own
                // content.  "style" is not permitted as it is a security risk.
                .allowElements("a")
                .allowCommonBlockElements()
                .allowStandardUrlProtocols()
                .allowCommonInlineFormattingElements()
                .allowAttributes("href").onElements("a")
                .requireRelNofollowOnLinks()
                .toFactory();

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        AtlasPerfMetrics.MetricRecorder metric = AtlasPerfMetrics.getMetricRecorder("XSSFilter");
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
        String sanitizedBody = policy.sanitize(reqBodyStr);

        if(!StringUtils.equals(reqBodyStr, StringEscapeUtils.unescapeHtml4(sanitizedBody))) {
            response.setStatus(400);
            response.getWriter().write(getErrorMessages(ERROR_INVALID_CHARACTERS));
            return;
        }
        
        RequestContext.get().endMetricRecord(metric);
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
