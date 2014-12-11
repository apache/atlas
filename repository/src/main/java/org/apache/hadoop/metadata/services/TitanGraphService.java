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

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
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
import java.util.Iterator;
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
    private static final String METADATA_PREFIX = "metadata.titanGraph.";
    private static final String METADATA_INDEX_KEY = "index.name";

    private Configuration graphConfig;
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
//        graphConfig = getConfiguration();

        titanGraph = initializeGraphDB();

//        createIndicesForVertexKeys();
        // todo - create Edge Cardinality Constraints
        LOG.info("Initialized titanGraph db: {}", titanGraph);

        vertexIndexedKeys = getIndexableGraph().getIndexedKeys(Vertex.class);
        LOG.info("Init vertex property keys: {}", vertexIndexedKeys);

        edgeIndexedKeys = getIndexableGraph().getIndexedKeys(Edge.class);
        LOG.info("Init edge property keys: {}", edgeIndexedKeys);
    }

    protected TitanGraph initializeGraphDB() {
        LOG.info("Initializing titanGraph db");

        // todo: externalize this
        Configuration graphConfig = new PropertiesConfiguration();
        graphConfig.setProperty("storage.backend", "berkeleyje");
        graphConfig.setProperty("storage.directory", "target/data/graphdb");

        return TitanFactory.open(graphConfig);
    }

    private static Configuration getConfiguration() throws ConfigurationException {
        PropertiesConfiguration configProperties = new PropertiesConfiguration("application.properties");

        Configuration graphConfig = new PropertiesConfiguration();
        final Iterator<String> iterator = configProperties.getKeys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            System.out.println("key = " + key);
            if (key.startsWith(METADATA_PREFIX)) {
                String value = (String) configProperties.getProperty(key);
                key = key.substring(METADATA_PREFIX.length());
                System.out.println("**** key = " + key + ", value = " + value);
                graphConfig.setProperty(key, value);
            }
        }

        return graphConfig;
    }

    /**
     * This unfortunately requires a handle to Titan implementation since
     * com.tinkerpop.blueprints.KeyIndexableGraph#createKeyIndex does not create an index.
     */
    protected void createIndicesForVertexKeys() {
        if (!((KeyIndexableGraph) titanGraph).getIndexedKeys(Vertex.class).isEmpty()) {
            LOG.info("Indexes already exist for titanGraph");
            return;
        }

        LOG.info("Indexes does not exist, Creating indexes for titanGraph");
        // todo - externalize this
        String indexName = graphConfig.getString(METADATA_INDEX_KEY);
        PropertyKey guid = createPropertyKey("guid", String.class, Cardinality.SINGLE);
        createIndex(indexName, guid, Vertex.class, true);

        getTitanGraph().commit();
    }

    private PropertyKey createPropertyKey(String propertyKeyName, Class<String> dataType,
                                          Cardinality cardinality) {
        PropertyKey propertyKey = getTitanGraph().getManagementSystem()
                .makePropertyKey(propertyKeyName)
                .dataType(dataType)
                .cardinality(cardinality)
                .make();
        LOG.info("Created property key {}", propertyKey);
        return propertyKey;
    }

    private void createIndex(String indexName, PropertyKey propertyKey,
                             Class<? extends Element> clazz, boolean isUnique) {
        TitanManagement managementSystem = getTitanGraph().getManagementSystem();
        managementSystem.buildPropertyIndex(propertyKey, indexName);

        TitanManagement.IndexBuilder indexBuilder = managementSystem
                .buildIndex(indexName, clazz)
                .addKey(propertyKey);
        if (isUnique) {
            indexBuilder.unique();
        }

        indexBuilder.buildCompositeIndex();
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
