/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.web.listeners;

import javax.servlet.ServletContextEvent;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Module;

public class TestGuiceServletConfig extends GuiceServletConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TestGuiceServletConfig.class);
    private boolean servicesEnabled;

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        LOG.info("Initializing test servlet listener");
        super.contextInitialized(servletContextEvent);
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        super.contextDestroyed(servletContextEvent);

        if(injector != null) {
            AtlasGraph graph = AtlasGraphProvider.getGraphInstance();

            LOG.info("Clearing graph store");
            try {
                AtlasGraphProvider.cleanup();
            } catch (Exception e) {
                LOG.warn("Clearing graph store failed ", e);
            }
        }
    }

    @Override
    protected Module getRepositoryModule() {
        return new TestModule();
    }

    @Override
    protected void startServices() {
        try {
            Configuration conf = ApplicationProperties.get();
            servicesEnabled = conf.getBoolean("atlas.services.enabled", true);
            if (servicesEnabled) {
                super.startServices();
            }
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void stopServices() {
        if (servicesEnabled) {
            super.stopServices();
        }
    }
}
