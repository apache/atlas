package org.apache.atlas.service.metrics;

import io.micrometer.core.instrument.*;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.utils.AtlasMetricType;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.atlas.service.metrics.MetricUtils.getMeterRegistry;


@Component
public class MetricsRegistryServiceImpl implements MetricsRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsRegistryServiceImpl.class);

    private static final String NAME = "name";
    private static final String URI = "uri";
    private static final String ORIGIN = "origin";
    private static final String METHOD_DIST_SUMMARY = "method_dist_summary";
    private static final String APPLICATION_LEVEL_METRICS_SUMMARY = "application_level_metrics_summary";
    private static final double[] PERCENTILES = {0.99};
    private static final String METHOD_LEVEL_METRICS_ENABLE = "atlas.metrics.method_level.enable";
    private static final String ATLAS_METRICS_METHOD_PATTERNS = "atlas.metrics.method_patterns";
    private final List<String> filteredMethods;

    @Inject
    public MetricsRegistryServiceImpl() throws AtlasException {
        this.filteredMethods = Arrays.stream(ApplicationProperties.get().getStringArray(ATLAS_METRICS_METHOD_PATTERNS)).collect(Collectors.toList());
    }

    @Override
    public void collect(String requestId, String requestUri, AtlasPerfMetrics metrics, String clientOrigin) {
        try {
            if (!ApplicationProperties.get().getBoolean(METHOD_LEVEL_METRICS_ENABLE, false)) {
                return;
            }

            for (String name : this.filteredMethods) {
                if(metrics.hasMetric(name)) {
                    AtlasPerfMetrics.Metric metric = metrics.getMetric(name);
                    Timer.builder(METHOD_DIST_SUMMARY).tags(Tags.of(NAME, metric.getName(), URI, requestUri, ORIGIN, clientOrigin)).publishPercentiles(PERCENTILES)
                            .register(getMeterRegistry()).record(metric.getTotalTimeMSecs(), TimeUnit.MILLISECONDS);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to collect metrics", e);
            return;
        }
    }
    //Use this if you want to publish Histograms
    public void collectApplicationMetrics(String requestId, String requestUri, List<AtlasPerfMetrics.Metric> applicationMetrics){
        try {
            for(AtlasPerfMetrics.Metric metric : applicationMetrics){
                if (metric.getMetricType() == AtlasMetricType.COUNTER) {
                    Counter.builder(metric.getName())
                            .tags(convertToMicrometerTags(metric.getTags()))
                            .register(getMeterRegistry())
                            .increment(metric.getInvocations());
                } else {
                    Timer.builder(APPLICATION_LEVEL_METRICS_SUMMARY)
                            .serviceLevelObjectives(
                                    Duration.ofMillis(500),
                                    Duration.ofMillis(750),
                                    Duration.ofMillis(1000),
                                    Duration.ofMillis(1200),
                                    Duration.ofMillis(1500),
                                    Duration.ofSeconds(2),
                                    Duration.ofSeconds(3),
                                    Duration.ofSeconds(4),
                                    Duration.ofSeconds(5),
                                    Duration.ofSeconds(7),
                                    Duration.ofSeconds(10),
                                    Duration.ofSeconds(15),
                                    Duration.ofSeconds(20),
                                    Duration.ofSeconds(25),
                                    Duration.ofSeconds(30),
                                    Duration.ofSeconds(40),
                                    Duration.ofSeconds(60),
                                    Duration.ofSeconds(90),
                                    Duration.ofSeconds(120),
                                    Duration.ofSeconds(180)
                            )
                            .publishPercentiles(PERCENTILES)
                            .tags(convertToMicrometerTags(metric.getTags()))
                            .register(getMeterRegistry()).record(metric.getTotalTimeMSecs(), TimeUnit.MILLISECONDS);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to collect metrics", e);
            return;
        }
    }

    private static Iterable<Tag> convertToMicrometerTags(Map<String, String> tagsMap) {
        return tagsMap.entrySet().stream()
                .map(entry -> Tag.of(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
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
