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

package org.apache.hadoop.metadata.services;

import com.thinkaurelius.titan.core.TitanFactory;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Default implementation for Graph service backed by Titan.
 */
public class TitanGraphService implements GraphService {

    private static final Logger LOG = LoggerFactory.getLogger(TitanGraphService.class);
    public static final String NAME = TitanGraphService.class.getSimpleName();

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    private static final String METADATA_PREFIX = "metadata.graph.";
    private static final String INDEXER_PREFIX = "metadata.indexer.vertex.";
    private static final List<String> acceptedTypes = Arrays.asList("String","Int","Long");

    private TitanGraph titanGraph;
    private Set<String> vertexIndexedKeys;
    private Set<String> edgeIndexedKeys;

    /**
     * Name of the service.
     *
     * @return name of the service
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Starts the service. This method blocks until the service has completely started.
     *
     * @throws Exception
     */
    @Override
    public void start() throws Exception {
        Configuration graphConfig = getConfiguration();
        titanGraph = initializeGraphDB(graphConfig);

        createIndicesForVertexKeys();
        // todo - create Edge Cardinality Constraints
        LOG.info("Initialized titanGraph db: {}", titanGraph);

        vertexIndexedKeys = getIndexableGraph().getIndexedKeys(Vertex.class);
        LOG.info("Init vertex property keys: {}", vertexIndexedKeys);

        edgeIndexedKeys = getIndexableGraph().getIndexedKeys(Edge.class);
        LOG.info("Init edge property keys: {}", edgeIndexedKeys);
    }

    private static Configuration getConfiguration() throws ConfigurationException {
        PropertiesConfiguration configProperties =
                new PropertiesConfiguration("application.properties");

        Configuration graphConfig = new PropertiesConfiguration();
        final Iterator<String> iterator = configProperties.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            if (key.startsWith(METADATA_PREFIX)) {
                String value = (String) configProperties.getProperty(key);
                key = key.substring(METADATA_PREFIX.length());
                graphConfig.setProperty(key, value);
            }
        }

        return graphConfig;
    }
    
    private static Configuration getConfiguration(String filename, String prefix) throws ConfigurationException {
        PropertiesConfiguration configProperties =
                new PropertiesConfiguration(filename);

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

    protected TitanGraph initializeGraphDB(Configuration graphConfig) {
        LOG.info("Initializing titanGraph db");
        return TitanFactory.open(graphConfig);
    }

    protected void createIndicesForVertexKeys() throws ConfigurationException {
    	
        if (!titanGraph.getIndexedKeys(Vertex.class).isEmpty()) {
            LOG.info("Indexes already exist for titanGraph");
            return;
        }

        LOG.info("Indexes do not exist, Creating indexes for titanGraph using indexer.properties.");

		TitanManagement mgmt = titanGraph.getManagementSystem();
		mgmt.buildIndex("mainIndex", Vertex.class).buildMixedIndex("search");
		TitanGraphIndex graphIndex = mgmt.getGraphIndex("mainIndex");

        mgmt.addIndexKey(graphIndex, mgmt.makePropertyKey("guid").dataType(String.class).make());

        Configuration indexConfig = getConfiguration("indexer.properties", INDEXER_PREFIX);
        // Properties are formatted: prop_name:type;prop_name:type
		// E.g. Name:String;Date:Long
		if (!indexConfig.isEmpty()) {

			// Get a list of property names to iterate through...
			List<String> propList =  new ArrayList<String>();
			
			Iterator<String> it = indexConfig.getKeys("property.name");
		
			while (it.hasNext()) {
				propList.add(it.next());
			}
			
			it = propList.iterator();
			while (it.hasNext()) {
			
				// Pull the property name and index, so we can register the name and look up the type.
				String prop = it.next().toString();
				String index = prop.substring(prop.lastIndexOf(".") + 1);
				String type = null;
				prop = indexConfig.getProperty(prop).toString();
			
				// Look up the type for the specified property name.
				if (indexConfig.containsKey("property.type." + index)) {
					type = indexConfig.getProperty("property.type." + index).toString();
				} else {
					throw new ConfigurationException("No type specified for property " + index + " in indexer.properties.");
				}
				
				// Is the type submitted one of the approved ones? 
				if (!acceptedTypes.contains(type)) {
					throw new ConfigurationException("The type provided in indexer.properties for property " + prop + " is not supported.  Supported types are: " + acceptedTypes.toString());
				}
			
				// Add the key.
				LOG.info("Adding property: " + prop + " to index as type: " + type);
		 		mgmt.addIndexKey(graphIndex,mgmt.makePropertyKey(prop).dataType(type.getClass()).make());
			
		 	}
		
			mgmt.commit();
			LOG.info("Index creation complete.");
		}
    }

    /**
     * Stops the service. This method blocks until the service has completely shut down.
     */
    @Override
    public void stop() {
        if (titanGraph != null) {
            titanGraph.shutdown();
        }
    }

    /**
     * A version of stop() that is designed to be usable in Java7 closure
     * clauses.
     * Implementation classes MUST relay this directly to {@link #stop()}
     *
     * @throws java.io.IOException never
     * @throws RuntimeException    on any failure during the stop operation
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
        return vertexIndexedKeys;
    }

    @Override
    public Set<String> getEdgeIndexedKeys() {
        return edgeIndexedKeys;
    }
}
