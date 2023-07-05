package org.apache.atlas.service.metrics;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;

@Component
@ConditionalOnAtlasProperty(property = "atlas.prometheus.service.impl")
public class MetricsRegistryServiceImpl implements MetricsRegistry {

    private static final String SERVICE = "service";
    private static final String ATLAS_METASTORE = "atlas-metastore";
    private static final String NAME = "name";
    private static final String METHOD_DIST_SUMMARY = "method_dist_summary";
    private static final double[] PERCENTILES = {0.90, 0.99};
    private static final int SEC_MILLIS_SCALE = 1000;

    private final PrometheusMeterRegistry prometheusMeterRegistry;

    @Inject
    public MetricsRegistryServiceImpl(PrometheusMeterRegistry prometheusMeterRegistry, ApplicationContext applicationContext) {
        this.prometheusMeterRegistry = prometheusMeterRegistry;
        this.prometheusMeterRegistry.config().withHighCardinalityTagsDetector().commonTags(SERVICE, ATLAS_METASTORE);
    }

    @Override
    public void collect(String requestId, AtlasPerfMetrics metrics) {
        for (String name : metrics.getMetricsNames()) {
            AtlasPerfMetrics.Metric metric = metrics.getMetric(name);
            this.prometheusMeterRegistry.newDistributionSummary(new Meter.Id(METHOD_DIST_SUMMARY,
                                    Tags.of(NAME, metric.getName()), BaseUnits.MILLISECONDS, METHOD_DIST_SUMMARY, Meter.Type.TIMER),
                                     DistributionStatisticConfig.builder().percentilePrecision(2)
                                    .percentiles(PERCENTILES)
                                    .bufferLength(3)
                                    .percentilesHistogram(false)
                                    .minimumExpectedValue(1.0)
                                    .maximumExpectedValue(Double.MAX_VALUE)
                                    .expiry(Duration.ofMinutes(1)).build(), SEC_MILLIS_SCALE)
                                    .record(metric.getTotalTimeMSecs());
        }
    }

    @Override
    public void scrape(PrintWriter writer) throws IOException {
        try {
            this.prometheusMeterRegistry.scrape(writer);
            this.prometheusMeterRegistry.clear();
            writer.flush();
        } finally {
            writer.close();
        }
    }

}
