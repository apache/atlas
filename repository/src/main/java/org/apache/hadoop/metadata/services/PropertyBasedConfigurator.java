package org.apache.hadoop.metadata.services;

import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public abstract class PropertyBasedConfigurator<T> {
	private final String propertyName;
	private final String defaultImplClass;
	private final String configurationPath;

	PropertyBasedConfigurator(String propertyNameProp, String defaultImplClassProp,
			String configurationPathProp, String propertyNameDefaultProp,
			String defaultImplClassDefaultProp, String configPathDefaultProp) {
		Properties props = System.getProperties();
		this.propertyName = props.getProperty(propertyNameProp,
				propertyNameDefaultProp);
		this.defaultImplClass = props.getProperty(defaultImplClassProp,
				defaultImplClassDefaultProp);
		this.configurationPath = props.getProperty(configurationPathProp,
				configPathDefaultProp);
	}

	PropertyBasedConfigurator(String propertyNameProp, String defaultImplClassProp,
			String configurationPathProp) {
		Properties props = System.getProperties();
		this.propertyName = props.getProperty(propertyNameProp);
		this.defaultImplClass = props.getProperty(defaultImplClassProp);
		this.configurationPath = props.getProperty(configurationPathProp);
	}

	public String getPropertyName() {
		return propertyName;
	}

	public String getDefaultImplClass() {
		return defaultImplClass;
	}

	public String getConfigurationPath() {
		return configurationPath;
	}

	public Configuration getConfiguration() {
		String path = getConfigurationPath();
		Configuration config = null;
		try {
			config = new PropertiesConfiguration(path);
		} catch (ConfigurationException e) {
			config = new PropertiesConfiguration();
		}
		return config;
	}

	public String getClassName() {
		Configuration config = getConfiguration();

		String propName = getPropertyName();
		String defaultClass = getDefaultImplClass();

		return config.getString(propName, defaultClass);
	}

	@SuppressWarnings("unchecked")
	public Class<? extends T> getImplClass() {
		String className = getClassName();
		Class<? extends T> ret = null;
		try {
			ret = (Class<? extends T>) PropertyBasedConfigurator.class
					.getClassLoader().loadClass(className);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		return ret;
	}
}
