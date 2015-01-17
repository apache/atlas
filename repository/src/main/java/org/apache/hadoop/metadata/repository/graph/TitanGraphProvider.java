package org.apache.hadoop.metadata.repository.graph;

import javax.inject.Singleton;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;

public class TitanGraphProvider implements GraphProvider<TitanGraph> {
	private static final String SYSTEM_PROP = "";
	private static final String DEFAULT_PATH = "graph.properties";

	private final String configPath;

	public TitanGraphProvider() {
		configPath = System.getProperties().getProperty(SYSTEM_PROP,
				DEFAULT_PATH);
	}

	public Configuration getConfiguration() throws ConfigurationException {
		return new PropertiesConfiguration(configPath);
	}

	@Override
	@Singleton
	public TitanGraph get() throws ConfigurationException {
		TitanGraph graph = null;

		Configuration config;
		try {
			config = getConfiguration();
		} catch (ConfigurationException e) {
			throw new RuntimeException(e);
		}
		graph = TitanFactory.open(config);

		return graph;
	}
}
