package org.apache.atlas.web.filters;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.RequestContext;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.web.util.CachedBodyHttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.jsoup.safety.Cleaner;
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
    private Safelist safelist;
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
        safelist = Safelist.relaxed()
                .preserveRelativeLinks(true)
                .addEnforcedAttribute("a", "rel", "nofollow")
                .addEnforcedAttribute("area", "rel", "nofollow")
                .removeProtocols("a", "href", "ftp", "http", "https", "mailto")
                .addTags("article", "aside")
                .addTags("figure","section","summary","hgroup", "wbr")
                .addTags("abbr", "acronym", "cite", "code", "dfn", "em", "figcaption", "mark", "s", "samp", "strong", "sub", "sup", "var")
                .addTags("b", "i", "pre", "small", "strike", "tt", "u", "hr")
                .addTags("rp", "rt", "ruby")

                .addAttributes("details","open")
                .addAttributes("map", "name")
                .addAttributes("area", "alt", "coords", "shape", "href","rel", "target")
                .addAttributes("iframe", "src", "width", "height", "frameborder", "allowfullscreen")
                .addAttributes("object", "width", "height")
                .addAttributes("q","cite")
                .addAttributes("time", "datetime")
                .addAttributes("bdi", "dir")
                .addAttributes("bdo", "dir")
                .addAttributes("del", "cite", "datetime")
                .addAttributes("ins", "cite", "datetime")
                .addAttributes("ol", "reversed", "start", "type")
                .addAttributes("ul", "reversed", "start", "type")
                .addAttributes("li", "type", "value")
                .addTags("dl", "dt", "dd")
                .addAttributes("table", "height", "width", "summary")
                .addAttributes("col","align", "height", "span", "valign", "width")
                .addAttributes("colgroup","align", "height", "span", "valign", "width")
                .addAttributes("thead", "align", "char", "charoff", "valign")
                .addAttributes("tfoot", "align", "char", "charoff", "valign")
                .addAttributes("tbody", "align", "char", "charoff", "valign")
                .addAttributes("tr", "align", "char", "charoff", "valign")
                .addAttributes("td", "abbr", "align", "axis", "char", "charoff", "colspan", "headers", "height", "rowspan", "scope", "valign", "width")
                .addAttributes("th", "abbr", "align", "axis", "char", "charoff", "colspan", "headers", "height", "rowspan", "scope", "valign", "width")
                .addTags("caption", "col", "colgroup", "tbody", "tfoot", "thead")
                .addAttributes("meter", "high", "low", "max", "min", "optimum", "value")
                .addAttributes("progress", "max", "value");
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        HttpServletRequest request = (HttpServletRequest) servletRequest;

        String serverName = request.getServerName();
        if (AtlasConfiguration.REST_API_XSS_FILTER_EXLUDE_SERVER_NAME.getString().equals(serverName)) {
            LOG.debug("AtlasXSSPreventionFilter: skipping filter for serverName: {}", serverName);
            filterChain.doFilter(request, response);
            return;
        }

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
        Cleaner htmlCleaner = new Cleaner(safelist);

        if(!htmlCleaner.isValid(Jsoup.parse(reqBodyStr))) {
            response.setHeader("Content-Type", CONTENT_TYPE_JSON);
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
