package org.apache.hadoop.metadata.web.listeners;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.metadata.RepositoryMetadataModule;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

public class GuiceServletConfig extends GuiceServletContextListener {
	
	private static final String GUICE_CTX_PARAM = "guice.packages";

	@Override
	protected Injector getInjector() {
		/*
		 * More information on this can be found here:
		 * https://jersey.java.net/nonav/apidocs/1.11/contribs/jersey-guice/com/sun/jersey/guice/spi/container/servlet/package-summary.html
		 */
		return Guice.createInjector(
				new RepositoryMetadataModule(),
				new JerseyServletModule() {

			@Override
			protected void configureServlets() {
				String packages = getServletContext().getInitParameter(GUICE_CTX_PARAM);
				
				Map<String, String> params = new HashMap<String, String>();
				params.put(PackagesResourceConfig.PROPERTY_PACKAGES, packages);
				serve("/*").with(GuiceContainer.class, params);
			}
		});
	}
}
