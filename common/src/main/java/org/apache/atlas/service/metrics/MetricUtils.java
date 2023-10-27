package org.apache.atlas.service.metrics;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang.StringUtils.EMPTY;

@Component
public class MetricUtils {
    private final static Logger LOG = LoggerFactory.getLogger(MetricUtils.class);

    private static final String URI = "uri";
    private static final String LOCAL = "local";
    private static final String STATUS = "status";
    private static final String METHOD = "method";
    private static final String SERVICE = "service";
    private static final String INTEGRATION = "integration";
    private static final String ATLAS_METASTORE = "atlas-metastore";
    private static final String REGEX_URI_PLACEHOLDER = "\\[\\^/\\]\\+";
    private static final String HTTP_SERVER_REQUESTS = "http.server.requests";
    private static final String ATLAS_METRICS_URI_PATTERNS = "atlas.metrics.uri_patterns";
    private static final double[] PERCENTILES = {0.5, 0.90, 0.99};

    private static Map<String, String> METRIC_URI_PATTERNS_MAP;
    private static final PrometheusMeterRegistry METER_REGISTRY;

    static {
        try {
            METRIC_URI_PATTERNS_MAP = Arrays.stream(ApplicationProperties.get().getStringArray(ATLAS_METRICS_URI_PATTERNS))
                    .distinct().collect(Collectors.toMap(uri->uri, uri->uri.replaceAll(REGEX_URI_PLACEHOLDER, "*")));
        } catch (Exception e) {
            LOG.error("Failed to load 'atlas.metrics.uri_patterns from properties");
        }
        METER_REGISTRY = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        METER_REGISTRY.config().withHighCardinalityTagsDetector().commonTags(SERVICE, ATLAS_METASTORE, INTEGRATION, LOCAL);
        Metrics.globalRegistry.add(METER_REGISTRY);
    }

    public Timer.Sample start(String uri) {
        return matchCanonicalPattern(uri).isPresent() ? Timer.start(getMeterRegistry()) : null;
    }

    public void recordHttpTimer(Timer.Sample sample, String method, String rawPath, int code, String... additionalTags) {
        if (Objects.isNull(sample)) {
            return;
        }
        sample.stop(getTimer(HTTP_SERVER_REQUESTS, method, code, rawPath, additionalTags));
    }

    private Timer getTimer(String timerName, String method, int code, String rawPath, String... additionalTags) {
        Tags tags = getTags(method, code, rawPath);
        if (Objects.nonNull(additionalTags) && additionalTags.length > 0) {
            tags = tags.and(additionalTags);
        }
        return Timer.builder(timerName)
                .publishPercentiles(PERCENTILES)
                .tags(tags)
                .register(getMeterRegistry());
    }

    private Tags getTags(String httpMethod, int httpResponseStatus, String uri) {
        return Tags.of(METHOD, httpMethod,
                STATUS, String.valueOf(httpResponseStatus),
                URI, matchCanonicalPattern(uri).get());
    }

    public static Optional<String> matchCanonicalPattern(String uri) {
        if (Objects.isNull(uri) || uri.isEmpty()) {
            return Optional.empty();
        }
        if (uri.endsWith("/")) {
            uri = uri.substring(0, uri.lastIndexOf("/"));
        }
        String updatedUrl = uri;
        Optional<String> patternOp = METRIC_URI_PATTERNS_MAP.keySet().stream()
                .filter(pattern -> updatedUrl.matches(pattern + "$"))
                .findFirst();
        return Optional.ofNullable(METRIC_URI_PATTERNS_MAP.get(patternOp.orElse(EMPTY)));
    }

    public static PrometheusMeterRegistry getMeterRegistry() {
        return METER_REGISTRY;
    }

}
