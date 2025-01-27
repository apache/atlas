package org.apache.atlas.service.metrics;

import org.apache.atlas.utils.AtlasPerfMetrics;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

public interface MetricsRegistry {

    void collect(String requestId, String requestUri, AtlasPerfMetrics metrics, String clientOrigin);

    void collectApplicationMetrics(String requestId, String requestUri, List<AtlasPerfMetrics.Metric> applicationMetrics);

    void scrape(PrintWriter writer) throws IOException;

}
