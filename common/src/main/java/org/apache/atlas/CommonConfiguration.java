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
package org.apache.atlas;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@EnableAspectJAutoProxy
public class CommonConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonConfiguration.class);
    private static final String SERVICE = "service";
    private static final String ATLAS_METASTORE = "atlas-metastore";
    private static final PrometheusMeterRegistry METER_REGISTRY;
    static {
        METER_REGISTRY = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        METER_REGISTRY.config().withHighCardinalityTagsDetector().commonTags(SERVICE, ATLAS_METASTORE);
        Metrics.globalRegistry.add(METER_REGISTRY);
    }

    @Bean
    public org.apache.commons.configuration.Configuration getAtlasConfig() throws AtlasException {
        try {
            return ApplicationProperties.get();
        } catch (AtlasException e) {
            LOGGER.warn("AtlasConfig init failed", e);
            throw e;
        }
    }

    public static PrometheusMeterRegistry getMeterRegistry() {
        return METER_REGISTRY;
    }

}
