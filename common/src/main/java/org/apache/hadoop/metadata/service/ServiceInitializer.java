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

package org.apache.hadoop.metadata.service;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Initializer that uses at startup to bring up all the Metadata startup services.
 */
public class ServiceInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceInitializer.class);
    private final Services services = Services.get();

    public void initialize() throws MetadataException {
        String serviceClassNames;
        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration("application.properties");
            serviceClassNames = configuration.getString("application.services");
        } catch (ConfigurationException e) {
            throw new MetadataException("unable to get server properties");
        }

        serviceClassNames
                = "org.apache.hadoop.metadata.services.TitanGraphService,org.apache.hadoop.metadata.services.GraphBackedMetadataRepositoryService";

        for (String serviceClassName : serviceClassNames.split(",")) {
            serviceClassName = serviceClassName.trim();
            if (serviceClassName.isEmpty()) {
                continue;
            }
            Service service = ReflectionUtils.getInstanceByClassName(serviceClassName);
            services.register(service);
            LOG.info("Initializing service: {}", serviceClassName);
            try {
                service.start();
            } catch (Throwable t) {
                LOG.error("Failed to initialize service {}", serviceClassName, t);
                throw new MetadataException(t);
            }
            LOG.info("Service initialized: {}", serviceClassName);
        }
    }

    public void destroy() throws MetadataException {
        for (Service service : services) {
            LOG.info("Destroying service: {}", service.getClass().getName());
            try {
                service.stop();
            } catch (Throwable t) {
                LOG.error("Failed to destroy service {}", service.getClass().getName(), t);
                throw new MetadataException(t);
            }
            LOG.info("Service destroyed: {}", service.getClass().getName());
        }
    }
}
