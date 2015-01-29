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

package org.apache.hadoop.metadata.repository.graph;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Default implementation for Graph service backed by Titan.
 */
@Singleton
public class TitanGraphService implements GraphService {

	private static final Logger LOG = LoggerFactory.getLogger(TitanGraphService.class);

	/**
	 * Constant for the configuration property that indicates the prefix.
	 */
	private static final String INDEXER_PREFIX = "metadata.indexer.vertex.";
	private static final List<String> acceptedTypes = Arrays.asList("String", "Int", "Long");

	// TODO - When we get the TypeSystem to return types, this will support the commented code block below.
	//private static final TypeSystem typeSystem = TypeSystem.getInstance();

	private final TitanGraph titanGraph;

	/**
	 * Initialize this service through injection with a custom Provider.
	 * 
	 * @param graph graph implementation
	 * @throws ConfigurationException
	 */
	@Inject
	public TitanGraphService(GraphProvider<TitanGraph> graph) throws ConfigurationException {
		// TODO reimplement to save the Provider and initialize the graph inside the start() method
		this.titanGraph = graph.get();
		//start();
	}

	/**
	 * Initializes this Service.  The starting of Titan is handled by the Provider
	 * @throws ConfigurationException
	 */
	@Override
	public void start() throws ConfigurationException {
		createIndicesForVertexKeys();
		// todo - create Edge Cardinality Constraints
		LOG.info("Initialized titanGraph db: {}", titanGraph);

		Set<String> vertexIndexedKeys = getVertexIndexedKeys();
		LOG.info("Init vertex property keys: {}", vertexIndexedKeys);

		Set<String> edgeIndexedKeys = getEdgeIndexedKeys();
		LOG.info("Init edge property keys: {}", edgeIndexedKeys);
	}

	private static Configuration getConfiguration(String filename, String prefix)
			throws ConfigurationException {
		PropertiesConfiguration configProperties = new PropertiesConfiguration(
				filename);

		Configuration graphConfig = new PropertiesConfiguration();

		final Iterator<String> iterator = configProperties.getKeys();
		while (iterator.hasNext()) {
			String key = iterator.next();
			if (key.startsWith(prefix)) {
				String value = (String) configProperties.getProperty(key);
				key = key.substring(prefix.length());
				graphConfig.setProperty(key, value);
			}
		}

		return graphConfig;
	}

	/**
	 * Initializes the indices for the graph.
	 * @throws ConfigurationException
	 */
	// TODO move this functionality to the SearchIndexer?
	protected void createIndicesForVertexKeys() throws ConfigurationException {
		if (!titanGraph.getIndexedKeys(Vertex.class).isEmpty()) {
			LOG.info("Indexes already exist for titanGraph");
			return;
		}

		LOG.info("Indexes do not exist, Creating indexes for titanGraph using indexer.properties.");

		TitanManagement mgmt = titanGraph.getManagementSystem();
		TitanGraphIndex graphIndex =  mgmt.buildIndex(Constants.INDEX_NAME, Vertex.class)
				.buildMixedIndex(Constants.BACKING_INDEX);

        Configuration indexConfig = getConfiguration("indexer.properties", INDEXER_PREFIX);
        // Properties are formatted: prop_name:type;prop_name:type
		// E.g. Name:String;Date:Long
		if (!indexConfig.isEmpty()) {

			// Get a list of property names to iterate through...
			List<String> propList = new ArrayList<>();

			Iterator<String> it = indexConfig.getKeys("property.name");

			while (it.hasNext()) {
				propList.add(it.next());
			}

			it = propList.iterator();
			while (it.hasNext()) {

				// Pull the property name and index, so we can register the name
				// and look up the type.
				String prop = it.next();
				String index = prop.substring(prop.lastIndexOf(".") + 1);
				String type;
				prop = indexConfig.getProperty(prop).toString();

				// Look up the type for the specified property name.
				if (indexConfig.containsKey("property.type." + index)) {
					type = indexConfig.getProperty("property.type." + index)
							.toString();
				} else {
					throw new ConfigurationException(
							"No type specified for property " + index
									+ " in indexer.properties.");
				}

				// Is the type submitted one of the approved ones?
				if (!acceptedTypes.contains(type)) {
					throw new ConfigurationException(
							"The type provided in indexer.properties for property "
									+ prop
									+ " is not supported.  Supported types are: "
									+ acceptedTypes.toString());
				}

				// Add the key.
				LOG.info("Adding property: " + prop + " to index as type: "
						+ type);
				mgmt.addIndexKey(graphIndex, mgmt.makePropertyKey(prop)
						.dataType(type.getClass()).make());

			}

			mgmt.commit();
			LOG.info("Index creation complete.");
		}
	}

	/**
	 * Stops the service. This method blocks until the service has completely
	 * shut down.
	 */
	@Override
	public void stop() {
		if (titanGraph != null) {
			titanGraph.shutdown();
		}
	}

	/**
	 * A version of stop() that is designed to be usable in Java7 closure
	 * clauses. Implementation classes MUST relay this directly to
	 * {@link #stop()}
	 *
	 * @throws java.io.IOException
	 *             never
	 * @throws RuntimeException
	 *             on any failure during the stop operation
	 */
	@Override
	public void close() throws IOException {
		stop();
	}

	@Override
	public Graph getBlueprintsGraph() {
		return titanGraph;
	}

	@Override
	public KeyIndexableGraph getIndexableGraph() {
		return titanGraph;
	}

	@Override
	public TransactionalGraph getTransactionalGraph() {
		return titanGraph;
	}

	public TitanGraph getTitanGraph() {
		return titanGraph;
	}

	@Override
	public Set<String> getVertexIndexedKeys() {
		// this must use the graph API instead of setting this value as a class member - it can change after creation
		return getIndexableGraph().getIndexedKeys(Vertex.class);
	}

	@Override
	public Set<String> getEdgeIndexedKeys() {
		// this must use the graph API instead of setting this value as a class member - it can change after creation
		return getIndexableGraph().getIndexedKeys(Edge.class);
	}
}
