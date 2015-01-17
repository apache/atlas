package org.apache.hadoop.metadata.web.listeners;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.metadata.RepositoryMetadataModule;
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
		
		return injector;
	}
}
