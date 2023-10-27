package org.apache.atlas.service.metrics;

import org.apache.atlas.utils.AtlasPerfMetrics;

import java.io.IOException;
import java.io.PrintWriter;

public interface MetricsRegistry {

    void collect(String requestId, String requestUri, AtlasPerfMetrics metrics);

    void scrape(PrintWriter writer) throws IOException;

}
