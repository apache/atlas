package org.apache.atlas.web.filters;

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.web.util.CachedBodyHttpServletRequest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Jsoup;
import org.jsoup.safety.Safelist;
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
                .allowAttributes("dir").matching(REGEX_MAPS.get("DIRECTION")).globally()
                .allowAttributes("lang").matching(Pattern.compile("[a-zA-Z]{2,20}")).globally()
                .allowAttributes("id").matching(Pattern.compile("[a-zA-Z0-9\\:\\-_\\.]+")).globally()
                .allowAttributes("title").matching(REGEX_MAPS.get("PARAGRAPH")).globally()
                //Global URL format policy
                .allowStandardUrlProtocols()
                .requireRelNofollowOnLinks()
                // "xml" "xslt" "DOCTYPE" "html" "head" are not permitted as we are
                // expecting user generated content to be a fragment of HTML and not a full
                // document.

                // Sectioning root tags //

                // "article" and "aside" are permitted and takes no attributes
                .allowElements("article","aside")
                // "body" is not permitted as we are expecting user generated content to be a fragment
                // of HTML and not a full document.

                // "details" is permitted, including the "open" attribute which can either
                // be blank or the value "open".
                .allowAttributes("open").matching(Pattern.compile("(?i)^(|open)$")).onElements("details")
                // "fieldset" is not permitted as we are not allowing forms to be created.

                // "figure","section", "summary" is permitted and takes no attributes
                .allowElements("figure", "section","summary")
                // Headings and footers //

                // "footer" is not permitted as we expect user content to be a fragment and
                // not structural to this extent

                // "h1" through "h6" and "hgroup" are permitted and take no attributes
                .allowElements("h1", "h2", "h3", "h4", "h5", "h6")
                .allowElements("hgroup")

                // Content grouping and separating //

                // "blockquote" is permitted, including the "cite" attribute which must be
                // a standard URL.
                .allowAttributes("cite").onElements("blockquote")
                // "br" "div" "hr" "p" "span" "wbr" are permitted and take no attributes
                .allowElements("br", "div", "hr", "p", "span", "wbr")
                // Links //

                // "a" is permitted
                .allowAttributes("a").onElements("href")
                // "area" is permitted along with the attributes that map image maps work
                .allowAttributes("name").matching(Pattern.compile("^([\\p{L}\\p{N}_-]+)$")) .onElements("map")
                .allowAttributes("alt").matching(REGEX_MAPS.get("PARAGRAPH")).onElements("area")
                .allowAttributes("coords").matching(Pattern.compile("^([0-9]+,)+[0-9]+$")).onElements("area")
                .allowAttributes("href").onElements("area")
                .allowAttributes("rel").matching(REGEX_MAPS.get("SPACE_SEPARATED_TOKENS")).onElements("area")
                .allowAttributes("shape").matching(REGEX_MAPS.get("SHAPE")).onElements("area")
                .allowAttributes("usemap").matching(Pattern.compile("(?i)^#[\\p{L}\\p{N}_-]+$")).onElements("img")

                // Phrase elements //

                // The following are all inline phrasing elements
                .allowElements("abbr", "acronym", "cite", "code", "dfn", "em", "figcaption", "mark", "s", "samp", "strong", "sub", "sup", "var")
                // "q" is permitted and "cite" is a URL and handled by URL policies
                .allowAttributes("cite").onElements("q")
                // "time" is permitted
                .allowAttributes("datetime").matching(REGEX_MAPS.get("ISO8601")).onElements("time")

                // Style elements //

                // block and inline elements that impart no semantic meaning but style the
                // document
                .allowElements("b", "i", "pre", "small", "strike", "tt", "u")
                // "style" is not permitted as we are not yet sanitising CSS and it is an XSS attack vector

                // HTML5 Formatting //

                // "bdi" "bdo" are permitted
                .allowAttributes("dir").matching(REGEX_MAPS.get("DIRECTION")).onElements("bdo","bdi")
                .allowElements("rp", "rt", "ruby")

                // HTML5 Change tracking //

                // "del" "ins" are permitted
                .allowAttributes("cite").matching(REGEX_MAPS.get("PARAGRAPH")).onElements("del", "ins")
                .allowAttributes("datetime").matching(REGEX_MAPS.get("ISO8601")).onElements("del", "ins")
                // Forms //

                // By and large, forms are not permitted. However there are some form
                // elements that can be used to present data, and we do permit those
                //
                // "button" "fieldset" "input" "keygen" "label" "output" "select" "datalist"
                // "textarea" "optgroup" "option" are all not permitted

                // "meter" is permitted
                .allowAttributes("value", "min", "max", "low", "high", "optimum").matching(REGEX_MAPS.get("NUMBER")).onElements("meter")
                // "progress" is permitted
                .allowAttributes("value","max").matching(REGEX_MAPS.get("NUMBER")).onElements("progress")

                // Embedded content //

                // Vast majority not permitted
                // "audio" "canvas" "embed" "iframe" "object" "param" "source" "svg" "track"
                // "video" are all not permitted
                .allowAttributes("align").matching(REGEX_MAPS.get("IMAGE_ALIGNMENT")).onElements("img")
                .allowAttributes("alt").matching(REGEX_MAPS.get("PARAGRAPH")).onElements("img")
                .allowAttributes("height", "width").matching(REGEX_MAPS.get("NUMBER_OR_PERCENT")).onElements("img")
                .allowAttributes("src").onElements("img")

                //Allow Lists//

                //Write code to allow lists
                .allowAttributes("type").matching(REGEX_MAPS.get("LIST_TYPE")).onElements("ul", "ol", "li")

                .allowAttributes("value").matching(REGEX_MAPS.get("INTEGER")).onElements("li")
                .allowElements("dl", "dt", "dd")
                // Tables //

                .allowAttributes("height", "width").matching(REGEX_MAPS.get("NUMBER_OR_PERCENT")).onElements("table")
                .allowAttributes("summary").matching(REGEX_MAPS.get("PARAGRAPH")).onElements("table")
                .allowElements("caption")
                .allowAttributes("align").matching(REGEX_MAPS.get("CELL_ALIGN")).onElements("col", "colgroup")
                .allowAttributes("height", "width").matching(REGEX_MAPS.get("NUMBER_OR_PERCENT")).onElements("col", "colgroup")
                .allowAttributes("span").matching(REGEX_MAPS.get("INTEGER")).onElements("colgroup", "col")
                .allowAttributes("valign").matching(REGEX_MAPS.get("CELL_VERTICAL_ALIGN")).onElements("col", "colgroup")
                .allowAttributes("align").matching(REGEX_MAPS.get("CELL_ALIGN")).onElements("thead", "tr")
                .allowAttributes("valign").matching(REGEX_MAPS.get("CELL_VERTICAL_ALIGN")).onElements("thead", "tr")
                .allowAttributes("abbr").matching(REGEX_MAPS.get("PARAGRAPH")).onElements("td", "th")
                .allowAttributes("align").matching(REGEX_MAPS.get("CELL_ALIGN")).onElements("td", "th")
                .allowAttributes("colspan", "rowspan").matching(REGEX_MAPS.get("INTEGER")).onElements("td", "th")
                .allowAttributes("headers").matching(REGEX_MAPS.get("SPACE_SEPARATED_TOKENS")).onElements("td", "th")
                .allowAttributes("height", "width").matching(REGEX_MAPS.get("NUMBER_OR_PERCENT")).onElements("td", "th")
                .allowAttributes("scope").matching(Pattern.compile("(?i)(?:row|col)(?:group)?")).onElements("td", "th")
                .allowAttributes("valign").matching(REGEX_MAPS.get("CELL_VERTICAL_ALIGN")).onElements("td", "th")
                .allowAttributes("nowrap").matching(Pattern.compile("(?i)|nowrap")).onElements("td", "th")
                .allowAttributes("align").matching(REGEX_MAPS.get("CELL_ALIGN")).onElements("tbody", "tfoot")
                .allowAttributes("valign").matching(REGEX_MAPS.get("CELL_VERTICAL_ALIGN")).onElements("tbody", "tfoot")
                .toFactory();

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
        String sanitizedBody = policy.sanitize(reqBodyStr);

        if(!StringUtils.equals(reqBodyStr, StringEscapeUtils.unescapeHtml4(sanitizedBody))) {
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
