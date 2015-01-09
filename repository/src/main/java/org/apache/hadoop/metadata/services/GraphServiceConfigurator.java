package org.apache.hadoop.metadata.services;

public class GraphServiceConfigurator extends PropertyBasedConfigurator<GraphService> {
	private static final String PROPERTY_NAME = "metadata.graph.impl.class";
	private static final String DEFAULT_IMPL_CLASS = "no.default.graph.class";
	private static final String CONFIG_PATH = "metadata.graph.properties";

	public GraphServiceConfigurator() {
		super("metadata.graph.propertyName", "metadata.graph.defaultImplClass",
				"metadata.graph.configurationPath", PROPERTY_NAME,
				DEFAULT_IMPL_CLASS, CONFIG_PATH);
	}
}
