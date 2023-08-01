package org.apache.atlas.service.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;

@Component
public class MetricsRegistryServiceImpl implements MetricsRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsRegistryServiceImpl.class);

    private static final String NAME = "name";
    private static final String METHOD_DIST_SUMMARY = "method_dist_summary";
    private static final double[] PERCENTILES = {0.90, 0.99};
    private static final int SEC_MILLIS_SCALE = 1;
    private static final String METHOD_LEVEL_METRICS_ENABLE = "atlas.metrics.method_level.enable";

    private final PrometheusMeterRegistry prometheusMeterRegistry;
    private final DistributionStatisticConfig distributionStatisticConfig;

    @Inject
    public MetricsRegistryServiceImpl(PrometheusMeterRegistry prometheusMeterRegistry) {
        this.prometheusMeterRegistry = prometheusMeterRegistry;
        this.distributionStatisticConfig =  DistributionStatisticConfig.builder().percentilePrecision(2)
                                            .percentiles(PERCENTILES)
                                            .bufferLength(3)
                                            .percentilesHistogram(false)
                                            .minimumExpectedValue(1.0)
                                            .maximumExpectedValue(Double.MAX_VALUE)
                                            .expiry(Duration.ofMinutes(2)).build();
    }

    @Override
    public void collect(String requestId, AtlasPerfMetrics metrics) {
        try {
            if (!ApplicationProperties.get().getBoolean(METHOD_LEVEL_METRICS_ENABLE, false)) {
                return;
            }
        } catch (AtlasException e) {
            LOG.error("Failed to read {} property from atlas config", METHOD_LEVEL_METRICS_ENABLE, e);
            return;
        }
        for (String name : metrics.getMetricsNames()) {
            AtlasPerfMetrics.Metric metric = metrics.getMetric(name);
            this.prometheusMeterRegistry.newDistributionSummary(new Meter.Id(METHOD_DIST_SUMMARY,
                                    Tags.of(NAME, metric.getName()), BaseUnits.MILLISECONDS, METHOD_DIST_SUMMARY,
                                    Meter.Type.TIMER), distributionStatisticConfig, SEC_MILLIS_SCALE)
                    .record(metric.getTotalTimeMSecs());
        }
    }

    @Override
    public void scrape(PrintWriter writer) throws IOException {
        try {
            this.prometheusMeterRegistry.scrape(writer);
            writer.flush();
        } finally {
            writer.close();
        }
    }

}
