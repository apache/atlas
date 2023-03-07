package org.apache.atlas.web.filters;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.web.util.CachedBodyHttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;
import org.owasp.html.Sanitizers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import com.google.common.base.Predicate;

import java.util.regex.Pattern;

@Component
public class AtlasXSSPreventionFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasXSSPreventionFilter.class);
    private static Pattern pattern;
    private PolicyFactory policy;
    private static final String MASK_STRING = "##ATLAN##";
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String ERROR_INVALID_CHARACTERS = "invalid characters in the request body (XSS Filter)";
    private static final Pattern REGEX_NUMBER                   = Pattern.compile("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$");
    public  static final Pattern REGEX_INTEGER                  = Pattern.compile("^[0-9]+$");
    public static final  Pattern REGEX_ISO8601                  = Pattern.compile("^^[0-9]{4}(-[0-9]{2}(-[0-9]{2}([ T][0-9]{2}(:[0-9]{2}){1,2}(.[0-9]{1,6})"
            +"?Z?([\\+-][0-9]{2}:[0-9]{2})?)?)?)?$");
    public static final  Pattern REGEX_PARAGRAPH                = Pattern.compile("^[\\p{L}\\p{N}\\s\\-_',\\[\\]!\\./\\\\\\(\\)]*$");
    public static final  Pattern REGEX_SPACE_SEPARATED_TOKENS   = Pattern.compile("^([\\s\\p{L}\\p{N}_-]+)$");
    public static final  Pattern REGEX_DIRECTION                = Pattern.compile("(?i)^(rtl|ltr)$");
    public static final  Pattern REGEX_IMAGE_ALIGNMENT          = Pattern.compile("(?i)^(left|right|top|texttop|middle|absmiddle|baseline|bottom|absbottom)$");
    public static final  Pattern REGEX_NUMBER_OR_PERCENT        = Pattern.compile("^[0-9]+[%]?$");
    public static final  Pattern REGEX_LIST_TYPE                = Pattern.compile("(?i)^(circle|disc|square|a|A|i|I|1)$");
    public static final  Pattern REGEX_CELL_ALIGN               = Pattern.compile("(?i)^(center|justify|left|right|char)$");
    public static final  Pattern REGEX_CELL_VERTICAL_ALIGN      = Pattern.compile("(?i)^(top|middle|bottom|baseline)$");
    public static final  Pattern REGEX_SHAPE                    = Pattern.compile("(?i)^(rect|circle|poly|default)$");
    public static final  Pattern REGEX_LANG                     = Pattern.compile("^[a-zA-Z]{2,20}");
    public static final  Pattern REGEX_ID                       = Pattern.compile("[a-zA-Z0-9\\:\\-_\\.]+");
    public static final  Pattern REGEX_NAME                     = Pattern.compile("^([\\p{L}\\p{N}_-]+)$");
    public static final  Pattern REGEX_USEMAP                   = Pattern.compile("(?i)^#[\\p{L}\\p{N}_-]+$");
    public static final  Pattern REGEX_OPEN                     = Pattern.compile("(?i)^(|open)$");
    public static final  Pattern REGEX_COORDS                   = Pattern.compile("^([0-9]+,)+[0-9]+$");
    public static final  Pattern REGEX_SCOPE                    = Pattern.compile("(?i)(?:row|col)(?:group)?");
    public static final  Pattern REGEX_NOWRAP                   = Pattern.compile("(?i)|nowrap");
    public static final  Pattern REGEX_ONSITE_URL               = Pattern.compile("(?:[\\p{L}\\p{N}\\\\\\.\\#@\\$%\\+&;\\-_~,\\?=/!]+|\\#(\\w)+)");
    public static final  Pattern REGEX_OFFSITE_URL              = Pattern.compile("\\s*(?:(?:ht|f)tps?://|mailto:)[\\p{L}\\p{N}]"
            + "[\\p{L}\\p{N}\\p{Zs}\\.\\#@\\$%\\+&;:\\-_~,\\?=/!\\(\\)]*+\\s*");
    public static final  Predicate<String> REGEX_ON_OFFSITE_URL = matchesEither(REGEX_ONSITE_URL, REGEX_OFFSITE_URL);

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
        pattern = Pattern.compile(AtlasConfiguration.REST_API_XSS_FILTER_MASK_STRING.getString());

        policy = new HtmlPolicyBuilder()
                .allowStandardUrlProtocols()
                .requireRelNofollowOnLinks()
                .allowCommonInlineFormattingElements()
                .allowCommonBlockElements()
                .allowElements("article", "aside","details","figure","section","summary","hgroup")
                .allowAttributes("open").matching(REGEX_OPEN).onElements("details")
                //Allow common header elements
                .allowElements("h1","h2","h3","h4","h5","h6")
                .allowElements("blockquote")
                .allowAttributes("cite").onElements("blockquote")
                //allow common html tags
                .allowElements("br", "div", "hr", "p", "span", "wbr")
                //Links
                .allowElements("a", "map", "area")
                .allowAttributes("href").matching(REGEX_OFFSITE_URL).onElements("a", "area")
                .allowAttributes("name").matching(REGEX_NAME).onElements("map")
                .allowAttributes("alt").matching(REGEX_PARAGRAPH).onElements("area")
                .allowAttributes("coords").matching(REGEX_COORDS).onElements("area")
                .allowAttributes("shape").matching(REGEX_SHAPE).onElements("area")
                .allowAttributes("rel").matching(REGEX_SPACE_SEPARATED_TOKENS).onElements("area")
                .allowAttributes("target").matching(REGEX_SPACE_SEPARATED_TOKENS).onElements("a")
                .allowAttributes("usemap").matching(REGEX_USEMAP).onElements("img")
                //phrase elements
                .allowElements("abbr", "acronym", "cite", "code", "dfn", "em",
                        "figcaption", "mark", "s", "samp", "strong", "sub", "sup", "var", "q", "time")
                .allowAttributes("cite").onElements( "q")
                .allowAttributes("datetime").matching(REGEX_ISO8601).onElements("time")

                //Style elements
                .allowElements("b", "i", "pre", "small", "strike", "tt", "u")
                //HTML5 formatting elements
                .allowElements("bdi","bdo", "rp", "rt", "ruby", "wbr", "del", "ins")
                .allowAttributes("cite").matching(REGEX_PARAGRAPH).onElements("del", "ins")
                .allowAttributes("datetime").matching(REGEX_ISO8601).onElements("del", "ins")

                //Lists
                .allowElements("dl", "dt", "dd", "ol", "ul", "li")
                .allowAttributes("type").matching(REGEX_LIST_TYPE).onElements("ol", "ul", "li")
                .allowAttributes("value").matching(REGEX_INTEGER).onElements("li")
                //allow Tables
                .allowElements("table", "caption", "col", "colgroup", "tbody", "td", "tfoot", "th", "thead", "tr")
                .allowAttributes("height","width").matching(REGEX_NUMBER_OR_PERCENT).onElements("table","col", "colgroup", "tbody", "td", "tfoot", "th", "thead", "tr")
                .allowAttributes("summary").matching(REGEX_PARAGRAPH).onElements("table")
                .allowAttributes("align").matching(REGEX_CELL_ALIGN).onElements("col", "colgroup", "td", "th", "tbody", "tfoot")
                .allowAttributes("valign").matching(REGEX_CELL_VERTICAL_ALIGN).onElements("col", "colgroup", "td", "th", "tbody", "tfoot")
                .allowAttributes("abbr").matching(REGEX_PARAGRAPH).onElements("td", "th")
                .allowAttributes("colspan","rowspan").matching(REGEX_INTEGER).onElements("td", "th")
                .allowAttributes("headers").matching(REGEX_SPACE_SEPARATED_TOKENS).onElements("td", "th")
                .allowAttributes("scope").matching(REGEX_SCOPE).onElements("td", "th")
                .allowAttributes("nowrap").matching(REGEX_NOWRAP).onElements("td", "th")
                //allow Forms
                //By and large, forms are not permitted. However there are some form
                // elements that can be used to present data, and we do permit those
                .allowElements("meter", "progress")
                .allowAttributes("value", "min", "max", "low", "high", "optimum").matching(REGEX_NUMBER).onElements("meter", "progress")
                .allowAttributes("value","max").matching(REGEX_NUMBER).onElements("progress")
                //Allow Image
                .allowElements("img")
                .allowAttributes("align").matching(REGEX_IMAGE_ALIGNMENT).onElements("img")
                .allowAttributes("alt").matching(REGEX_PARAGRAPH).onElements("img")
                .allowAttributes("height", "width").matching(REGEX_NUMBER_OR_PERCENT).onElements("img")
                .allowAttributes("src").matching(REGEX_ON_OFFSITE_URL).onElements("img")
                //Allow Global Attributes
                .allowAttributes("dir").matching(REGEX_DIRECTION).globally()
                .allowAttributes("lang").matching(REGEX_LANG).globally()
                .allowAttributes("id").matching(REGEX_ID).globally()
                .allowAttributes("title").matching(REGEX_PARAGRAPH).globally()

                .allowWithoutAttributes("a", "span")
                .toFactory()
                .and(Sanitizers.IMAGES);

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        HttpServletRequest request = (HttpServletRequest) servletRequest;

        String serverName = request.getServerName();
        if (StringUtils.isNotEmpty(serverName) && serverName.contains(AtlasConfiguration.REST_API_XSS_FILTER_EXLUDE_SERVER_NAME.getString())) {
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
        String body             = IOUtils.toString(cachedBodyHttpServletRequest.getInputStream(), "UTF-8");
        String reqBodyStr       = StringEscapeUtils.unescapeJava(pattern.matcher(body).replaceAll(MASK_STRING));
        String sanitizedBody    = policy.sanitize(reqBodyStr);

        if(!StringUtils.equals(reqBodyStr, StringEscapeUtils.unescapeHtml4(sanitizedBody))) {
            response.setHeader("Content-Type", CONTENT_TYPE_JSON);
            response.setStatus(400);
            response.getWriter().write(getErrorMessages(ERROR_INVALID_CHARACTERS));
            return;
        }

        filterChain.doFilter(cachedBodyHttpServletRequest, response);
    }

    private static Predicate<String> matchesEither(
            final Pattern a, final Pattern b) {
        return new Predicate<String>() {
            public boolean apply(String s) {
                return a.matcher(s).matches() || b.matcher(s).matches();
            }
        };
    }

    @Override
    public void destroy() {
        LOG.debug("AtlasXSSPreventionFilter destroyed");
    }



    private String getErrorMessages(String err) {
        return "{\"code\":1000,\"error\":\"XSS\",\"message\":\"" + err + "\"}";
    }
}
