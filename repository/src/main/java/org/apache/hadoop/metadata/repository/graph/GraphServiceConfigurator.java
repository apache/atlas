package org.apache.hadoop.metadata.repository.graph;

import com.thinkaurelius.titan.core.TitanGraph;

public class GraphServiceConfigurator extends PropertyBasedConfigurator<GraphService> {
	private static final String PROPERTY_NAME = "metadata.graph.impl.class";
	private static final String DEFAULT_IMPL_CLASS = TitanGraph.class.getName();
	private static final String CONFIG_PATH = "application.properties";

	public GraphServiceConfigurator() {
		super("metadata.graph.propertyName", "metadata.graph.defaultImplClass",
				"metadata.graph.configurationPath", PROPERTY_NAME,
				DEFAULT_IMPL_CLASS, CONFIG_PATH);
	}
}
