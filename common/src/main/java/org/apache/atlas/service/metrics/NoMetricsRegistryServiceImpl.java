package org.apache.atlas.service.metrics;

import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.annotation.EnableConditional;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.PrintWriter;

@Component
@ConditionalOnAtlasProperty(property = "atlas.prometheus.service.impl", isDefault = true)
public class NoMetricsRegistryServiceImpl implements MetricsRegistry {

    @Override
    public void collect(String requestId, AtlasPerfMetrics metrics) {
        //do nothing
    }

    @Override
    public void scrape(PrintWriter writer) throws IOException {
        writer.close();
    }
}
