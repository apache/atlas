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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.PropertiesUtil;
import org.apache.hadoop.metadata.RepositoryMetadataModule;
import org.apache.hadoop.metadata.repository.typestore.ITypeStore;
import org.apache.hadoop.metadata.typesystem.TypesDef;
import org.apache.hadoop.metadata.typesystem.types.TypeSystem;
import org.apache.hadoop.metadata.web.filters.AuditFilter;
import org.apache.hadoop.metadata.web.filters.MetadataAuthenticationFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContextEvent;
import java.util.HashMap;
import java.util.Map;

public class GuiceServletConfig extends GuiceServletContextListener {

    private static final Logger LOG = LoggerFactory.getLogger(GuiceServletConfig.class);

    private static final String GUICE_CTX_PARAM = "guice.packages";
    static final String HTTP_AUTHENTICATION_ENABLED = "metadata.http.authentication.enabled";
    private Injector injector;

    public static final String BASE_URI = "/api/metadata/";

    @Override
    protected Injector getInjector() {
        LOG.info("Loading Guice modules");
        /*
         * More information on this can be found here:
		 * https://jersey.java.net/nonav/apidocs/1
		 * .11/contribs/jersey-guice/com/sun/jersey/guice/spi/container/servlet/package-summary
		 * .html
		 */
        if (injector == null) {
            injector = Guice.createInjector(
                    new RepositoryMetadataModule(),
                    new JerseyServletModule() {
                        @Override
                        protected void configureServlets() {
                            filter("/*").through(AuditFilter.class);
                            try {
                                configureAuthenticationFilter();
                            } catch (ConfigurationException e) {
                                LOG.warn("Unable to add and configure authentication filter", e);
                            }

                            String packages = getServletContext().getInitParameter(GUICE_CTX_PARAM);

                            LOG.info("Jersey loading from packages: " + packages);

                            Map<String, String> params = new HashMap<>();
                            params.put(PackagesResourceConfig.PROPERTY_PACKAGES, packages);
                            serve(BASE_URI + "*").with(GuiceContainer.class, params);
                        }

                        private void configureAuthenticationFilter() throws ConfigurationException {
                            try {
                                PropertiesConfiguration configuration = PropertiesUtil.getApplicationProperties();
                                if (Boolean.valueOf(configuration.getString(HTTP_AUTHENTICATION_ENABLED))) {
                                    filter("/*").through(MetadataAuthenticationFilter.class);
                                }
                            } catch (MetadataException e) {
                                LOG.warn("Error loading configuration and initializing authentication filter", e);
                            }
                        }
                    });

            LOG.info("Guice modules loaded");
        }

        return injector;
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        super.contextInitialized(servletContextEvent);

        // perform login operations
        LoginProcessor loginProcessor = new LoginProcessor();
        loginProcessor.login();

        restoreTypeSystem();
    }

    private void restoreTypeSystem() {
        LOG.info("Restoring type system from the store");
        Injector injector = getInjector();
        ITypeStore typeStore = injector.getInstance(ITypeStore.class);
        try {
            TypesDef typesDef = typeStore.restore();
            TypeSystem typeSystem = injector.getInstance(TypeSystem.class);
            typeSystem.defineTypes(typesDef);
        } catch (MetadataException e) {
            throw new RuntimeException(e);
        }
        LOG.info("Restored type system from the store");
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {
        super.contextDestroyed(servletContextEvent);
    }
}