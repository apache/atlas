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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.bridge.BridgeTypeBootstrapper;
import org.apache.hadoop.metadata.repository.graph.GraphService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

public class GuiceServletConfig extends GuiceServletContextListener {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(GuiceServletConfig.class);
	private static final String GUICE_CTX_PARAM = "guice.packages";

	@Override
	protected Injector getInjector() {
		LOG.info("Loading Guice modules");
		/*
		 * More information on this can be found here:
		 * https://jersey.java.net/nonav/apidocs/1.11/contribs/jersey-guice/com/sun/jersey/guice/spi/container/servlet/package-summary.html
		 */
		Injector injector =  Guice.createInjector(
				new RepositoryMetadataModule(),
				new JerseyServletModule() {

			@Override
			protected void configureServlets() {
				String packages = getServletContext().getInitParameter(GUICE_CTX_PARAM);
				
				LOG.info("Jersey loading from packages: " + packages);
				
				Map<String, String> params = new HashMap<String, String>();
				params.put(PackagesResourceConfig.PROPERTY_PACKAGES, packages);
				serve("/api/metadata/*").with(GuiceContainer.class, params);
			}
		});
		
		LOG.info("Guice modules loaded");
		LOG.info("Bootstrapping services");
		
		// get the Graph Service
		GraphService graphService = injector.getInstance(GraphService.class);
		try {
			// start/init the service
			graphService.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		LOG.info(String.format("Loaded Service: %s", graphService.getClass().getName()));
		
		LOG.info("Services bootstrapped successfully");
		/*
		 * Turned off for now - will be revisited later
		 * 
		LOG.info("Bootstrapping types into Type System");
		
		BridgeTypeBootstrapper bootstrapper = injector.getInstance(BridgeTypeBootstrapper.class);
		try {
			bootstrapper.bootstrap();
		} catch(Exception e) {
			throw new RuntimeException(e);
		}
		
		LOG.info("Types bootstrapped successfully");
		*/
		
		return injector;
	}
}
