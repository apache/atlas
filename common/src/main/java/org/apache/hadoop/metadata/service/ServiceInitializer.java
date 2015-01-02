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
 * Initializer that uses at startup to bring up all the Metadata startup
 * services.
 */
public class ServiceInitializer {

	private static final Logger LOG = LoggerFactory
			.getLogger(ServiceInitializer.class);
	private final Services services = Services.get();

	// default property file name/path
	private static final String DEFAULT_CONFIG_PATH = "application.properties";
	// system property referenced by this class to extract user-overriden
	// properties file
	public static final String PROPERTIES_SYS_PROP = "metadata.properties";

	// Path to the properties file (must be on the classpath for
	// PropertiesConfiguration to work)
	private final String propertyPath;

	/**
	 * Default constructor. Use the metadata.properties System property to
	 * determine the property file name.
	 */
	public ServiceInitializer() {
		propertyPath = System.getProperty(PROPERTIES_SYS_PROP,
				DEFAULT_CONFIG_PATH);
	}

	/**
	 * Create a ServiceInitializer, specifying the properties file filename
	 * explicitly
	 * 
	 * @param propPath
	 *            the filename of the properties file with the service
	 *            intializer information
	 */
	public ServiceInitializer(String propPath) {
		propertyPath = propPath;
	}

	/**
	 * Get the configuration properties for the ServiceInitializer
	 * 
	 * @return
	 * @throws ConfigurationException
	 */
	public PropertiesConfiguration getConfiguration()
			throws ConfigurationException {
		return new PropertiesConfiguration(propertyPath);
	}

	/**
	 * Initialize the services specified by the application.services property
	 * 
	 * @throws MetadataException
	 */
	public void initialize() throws MetadataException {
		/*
		 * TODO - determine whether this service model is the right model;
		 * Inter-service dependencies can wreak havoc using the current model
		 */

		String[] serviceClassNames;
		LOG.info("Loading services using properties file: {}", propertyPath);
		try {
			PropertiesConfiguration configuration = getConfiguration();
			serviceClassNames = configuration
					.getStringArray("application.services");
		} catch (ConfigurationException e) {
			throw new RuntimeException("unable to get server properties");
		}

		for (String serviceClassName : serviceClassNames) {
			serviceClassName = serviceClassName.trim();
			if (serviceClassName.isEmpty()) {
				continue;
			}
			Service service = ReflectionUtils
					.getInstanceByClassName(serviceClassName);
			services.register(service);
			LOG.info("Initializing service: {}", serviceClassName);
			try {
				service.start();
			} catch (Throwable t) {
				LOG.error("Failed to initialize service {}", serviceClassName,
						t);
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
				LOG.error("Failed to destroy service {}", service.getClass()
						.getName(), t);
				throw new MetadataException(t);
			}
			LOG.info("Service destroyed: {}", service.getClass().getName());
		}
	}
}
