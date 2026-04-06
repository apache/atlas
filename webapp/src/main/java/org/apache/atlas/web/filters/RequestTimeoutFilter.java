/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.filters;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import org.apache.atlas.service.metrics.MetricUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Servlet filter that monitors per-request duration and tracks in-flight requests.
 *
 * This filter provides two types of observability:
 *
 * 1. **In-flight request tracking** (real-time): Gauges that show how many requests
 *    are currently running and how many have been running longer than threshold.
 *    During the MS-864 outage, 387 threads were blocked for ~14 minutes with zero
 *    visibility. These gauges would have shown the accumulation in real time.
 *
 * 2. **Timeout detection** (post-completion): Logs a warning and increments a counter
 *    when a request exceeds the endpoint-specific threshold. This is a lagging indicator
 *    that only fires after the request finishes.
 *
 * Note: This filter does NOT abort or cancel requests. The connector idle timeout
 * ({@code atlas.webserver.idle.timeout.ms}) is the hard cutoff for idle connections.
 *
 * @see <a href="https://atlanhq.atlassian.net/wiki/spaces/Metastore/pages/1792278594">RCA: Fleet-Wide Outage</a>
 */
@Component
public class RequestTimeoutFilter implements Filter {
    private static final Logger LOG = LoggerFactory.getLogger(RequestTimeoutFilter.class);

    private static final long SEARCH_TIMEOUT_MS  = 60_000;  // 60s for search/indexsearch
    private static final long DEFAULT_TIMEOUT_MS = 300_000; // 300s (5 min) for all other endpoints

    private static final long LONG_RUNNING_THRESHOLD_MS = 30_000;  // 30s

    // Maps threadId -> request start time (epoch ms) for all in-flight requests
    private static final ConcurrentHashMap<Long, Long> IN_FLIGHT_REQUESTS = new ConcurrentHashMap<>();

    private static final Counter TIMEOUT_COUNTER = Counter.builder("atlas.request.timeout.total")
            .description("Total number of requests that exceeded the timeout limit")
            .register(MetricUtils.getMeterRegistry());

    static {
        Gauge.builder("atlas.requests.inflight.total", IN_FLIGHT_REQUESTS, ConcurrentHashMap::size)
                .description("Number of requests currently being processed")
                .register(MetricUtils.getMeterRegistry());

        Gauge.builder("atlas.requests.inflight.long_running", IN_FLIGHT_REQUESTS, map -> {
            long now = System.currentTimeMillis();
            return map.values().stream()
                    .filter(startTime -> (now - startTime) > LONG_RUNNING_THRESHOLD_MS)
                    .count();
        }).description("Number of requests currently running longer than 30 seconds")
                .register(MetricUtils.getMeterRegistry());

        Gauge.builder("atlas.requests.inflight.stuck", IN_FLIGHT_REQUESTS, map -> {
            long now = System.currentTimeMillis();
            return map.values().stream()
                    .filter(startTime -> (now - startTime) > DEFAULT_TIMEOUT_MS)
                    .count();
        }).description("Number of requests currently running longer than 300 seconds (likely stuck)")
                .register(MetricUtils.getMeterRegistry());

        Gauge.builder("atlas.requests.inflight.max_age_seconds", IN_FLIGHT_REQUESTS, map -> {
            if (map.isEmpty()) return 0.0;
            long now = System.currentTimeMillis();
            return map.values().stream()
                    .mapToLong(startTime -> now - startTime)
                    .max()
                    .orElse(0) / 1000.0;
        }).description("Age in seconds of the oldest currently in-flight request")
                .register(MetricUtils.getMeterRegistry());
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        LOG.info("RequestTimeoutFilter initialized: searchTimeout={}ms, defaultTimeout={}ms, longRunningThreshold={}ms",
                SEARCH_TIMEOUT_MS, DEFAULT_TIMEOUT_MS, LONG_RUNNING_THRESHOLD_MS);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String uri = httpRequest.getRequestURI();
        long timeoutMs = getTimeoutForUri(uri);

        long threadId = Thread.currentThread().getId();
        long startTime = System.currentTimeMillis();
        IN_FLIGHT_REQUESTS.put(threadId, startTime);

        try {
            chain.doFilter(request, response);
        } finally {
            IN_FLIGHT_REQUESTS.remove(threadId);
            long duration = System.currentTimeMillis() - startTime;
            if (duration > timeoutMs) {
                TIMEOUT_COUNTER.increment();
                LOG.warn("Request exceeded timeout: uri={}, duration={}ms, timeout={}ms, method={}",
                        uri, duration, timeoutMs, httpRequest.getMethod());
            }
        }
    }

    private long getTimeoutForUri(String uri) {
        if (uri == null) {
            return DEFAULT_TIMEOUT_MS;
        }
        if (uri.contains("/indexsearch") || uri.contains("/direct/search")) {
            return SEARCH_TIMEOUT_MS;
        }
        return DEFAULT_TIMEOUT_MS;
    }

    @Override
    public void destroy() {
        IN_FLIGHT_REQUESTS.clear();
    }
}
