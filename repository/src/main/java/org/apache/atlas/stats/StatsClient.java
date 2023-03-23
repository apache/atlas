/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.stats;

import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.service.Service;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.inject.Singleton;

@Singleton
@Component
@Order(7)
public class StatsClient implements Service {
    private static final Logger LOG = LoggerFactory.getLogger(StatsClient.class);

    private static final String STATSD_ENABLE_CONF = "atlas.statsd.enable";
    private static final String STATSD_HOST_CONF = "atlas.statsd.hostname";
    private static final String STATSD_PORT_CONF = "atlas.statsd.port";
    private static final String STATSD_PREFIX_CONF = "atlas.statsd.prefix";

    private StatsDClient statsDClient;
    private String defaultTags;

    @Override
    public void start() throws AtlasException {

        Configuration configuration = ApplicationProperties.get();

        boolean isStatsdEnabled = configuration.getBoolean(STATSD_ENABLE_CONF, false);

        if (isStatsdEnabled) {
            statsDClient = new NonBlockingStatsDClient(
                    configuration.getString(STATSD_PREFIX_CONF, "atlas"),
                    configuration.getString(STATSD_HOST_CONF, "localhost"),
                    configuration.getInt(STATSD_PORT_CONF, 8125)
            );
        } else {
            statsDClient = new NoOpStatsDClient();
        }

        LOG.info("Started statsd server..");
    }

    public void count(String metric, long delta) {
        this.statsDClient.count(metric, delta);
    }

    public void increment(String metric) {
        this.statsDClient.increment(metric);
    }

    public void recordExecutionTime(String metric, long ms) {
        this.statsDClient.recordExecutionTime(metric, ms);
    }

    public void time(String metric, long ms) {
        this.statsDClient.time(metric, ms);
    }

    public void gauge(String metric, long delta) {
        this.statsDClient.gauge(metric, delta);
    }

    public void recordGaugeDelta(String metric, long delta) {
        this.statsDClient.recordGaugeDelta(metric, delta);
    }

    @Override
    public void stop() {
        statsDClient.stop();
    }
}