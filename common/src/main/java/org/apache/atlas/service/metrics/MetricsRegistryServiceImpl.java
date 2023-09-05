package org.apache.atlas.service.metrics;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;


@Component
public class MetricsRegistryServiceImpl implements MetricsRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsRegistryServiceImpl.class);

    private static final String NAME = "name";
    private static final String METHOD_DIST_SUMMARY = "method_dist_summary";
    private static final double[] PERCENTILES = {0.99};
    private static final String METHOD_LEVEL_METRICS_ENABLE = "atlas.metrics.method_level.enable";
    private static final String ATLAS_METRICS_METHOD_PATTERNS = "atlas.metrics.method_patterns";
    private final List<String> filteredMethods;

    @Inject
    public MetricsRegistryServiceImpl() throws AtlasException {
        this.filteredMethods = Arrays.stream(ApplicationProperties.get().getStringArray(ATLAS_METRICS_METHOD_PATTERNS)).collect(Collectors.toList());
    }

    @Override
    public void collect(String requestId, AtlasPerfMetrics metrics) {
        try {
            if (!ApplicationProperties.get().getBoolean(METHOD_LEVEL_METRICS_ENABLE, false)) {
                return;
            }

            for (String name : this.filteredMethods) {
                if(metrics.hasMetric(name)) {
                    AtlasPerfMetrics.Metric metric = metrics.getMetric(name);
                    Timer.builder(METHOD_DIST_SUMMARY).tags(Tags.of(NAME, metric.getName())).publishPercentiles(PERCENTILES)
                            .register(getMeterRegistry()).record(metric.getTotalTimeMSecs(), TimeUnit.MILLISECONDS);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to collect metrics", e);
            return;
        }
    }

    @Override
    public void scrape(PrintWriter writer) {
        Metrics.globalRegistry.getRegistries().forEach(r -> {
            try {
                ((PrometheusMeterRegistry) r).scrape(writer);
                writer.flush();
            } catch (IOException e) {
                LOG.warn("Failed to write metrics while scraping", e);
            } finally {
                writer.close();
            }
        });
    }

}
