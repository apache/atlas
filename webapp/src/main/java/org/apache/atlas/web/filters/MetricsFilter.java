package org.apache.atlas.web.filters;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.atlas.service.metrics.MetricsRegistry;
import org.apache.atlas.web.model.MetricContext;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.context.support.SpringBeanAutowiringSupport;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.inject.Inject;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Filter used to record HTTP request & response metrics
 */
@Component
public class MetricsFilter extends OncePerRequestFilter {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsFilter.class);

    private static final String HTTP_SERVER_REQUESTS = "http.server.requests";
    private static final String METHOD = "method";
    private static final String STATUS = "status";
    private static final String URI = "uri";

    @Inject
    private MetricsRegistry metricsRegistry;
    @Inject
    private PrometheusMeterRegistry prometheusMeterRegistry;

    public MetricsFilter() {
        SpringBeanAutowiringSupport.processInjectionBasedOnCurrentContext(this);
    }

    @Override
    protected void doFilterInternal(@NotNull HttpServletRequest request, @NotNull HttpServletResponse response,
                                    @NotNull FilterChain filterChain) throws ServletException, IOException {
        MetricContext metricContext = MetricContext.get(request);
        try {
            if (metricContext == null) {
                metricContext = startMetricsTimer(request);
            }
            filterChain.doFilter(request, response);
        } finally {
            record(metricContext, request, response);
        }
    }

    private void record(MetricContext metricContext, ServletRequest request, ServletResponse response) {
        try {
            Timer.Sample timerSample = metricContext.getTimerSample();
            Timer.Builder builder = Timer.builder(HTTP_SERVER_REQUESTS);
            timerSample.stop(builder.tags(getTags((HttpServletRequest) request, (HttpServletResponse) response))
                    .register(this.prometheusMeterRegistry));
        } catch (Exception ex) {
            LOG.warn("Failed to record http requests metrics", ex);
        }
    }

    private Tags getTags(HttpServletRequest request, HttpServletResponse response) {
        return Tags.of(METHOD, request.getMethod(),
                STATUS, String.valueOf(response.getStatus()),
                URI, request.getRequestURI());
    }

    private MetricContext startMetricsTimer(HttpServletRequest request) {
        Timer.Sample timerSample = Timer.start(this.prometheusMeterRegistry);
        MetricContext timingContext = new MetricContext(timerSample);
        timingContext.attachTo(request);
        return timingContext;
    }
}
