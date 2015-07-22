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

import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Graph;
import org.apache.atlas.repository.graph.GraphProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;

public class TestGuiceServletConfig extends GuiceServletConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TestGuiceServletConfig.class);

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        LOG.info("Initializing test servlet listener");
        super.contextInitialized(servletContextEvent);
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        if(injector != null) {
            TypeLiteral<GraphProvider<TitanGraph>> graphProviderType = new TypeLiteral<GraphProvider<TitanGraph>>() {};
            Provider<GraphProvider<TitanGraph>> graphProvider = injector.getProvider(Key.get(graphProviderType));
            TitanGraph graph = graphProvider.get().get();

            LOG.info("Clearing graph store");
            try {
                graph.shutdown();
                TitanCleanup.clear(graph);
            } catch (Exception e) {
                LOG.warn("Clearing graph store failed ", e);
            }
        }
    }
}
