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

package org.apache.hadoop.metadata.web.listeners;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.service.ServiceInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Listener for bootstrapping Services and configuration properties.
 */
public class ApplicationStartupListener implements ServletContextListener {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationStartupListener.class);

    private final ServiceInitializer startupServices = new ServiceInitializer();

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            startupServices.initialize();
            showStartupInfo();
        } catch (MetadataException e) {
            throw new RuntimeException("Error starting services", e);
        }
    }

    private void showStartupInfo() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("\n############################################");
        buffer.append("\n        Metadata Server (STARTED)           ");
        buffer.append("\n############################################");

        try {
            PropertiesConfiguration configuration = new PropertiesConfiguration("application.properties");
            buffer.append(configuration.toString());

        } catch (ConfigurationException e) {
            buffer.append("*** Unable to get build info ***").append(e.getMessage());
        }

        LOG.info(buffer.toString());
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        try {
            startupServices.destroy();
        } catch (MetadataException e) {
            LOG.warn("Error destroying services", e);
        }

        StringBuilder buffer = new StringBuilder();
        buffer.append("\n############################################");
        buffer.append("\n       Metadata Server (SHUTDOWN)           ");
        buffer.append("\n############################################");
        LOG.info(buffer.toString());
    }
}