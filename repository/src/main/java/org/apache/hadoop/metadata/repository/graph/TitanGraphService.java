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
import java.util.Iterator;
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
        initialize();
    }

    private static Configuration getConfiguration(String filename, String prefix)
    throws ConfigurationException {
        PropertiesConfiguration configProperties = new PropertiesConfiguration(filename);

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
     * Initializes this Service.  The starting of Titan is handled by the Provider
     * @throws ConfigurationException
     */
    public void initialize() throws ConfigurationException {
        // todo - create Edge Cardinality Constraints
        LOG.info("Initialized titanGraph db: {}", titanGraph);

        Set<String> vertexIndexedKeys = getVertexIndexedKeys();
        LOG.info("Init vertex property keys: {}", vertexIndexedKeys);

        Set<String> edgeIndexedKeys = getEdgeIndexedKeys();
        LOG.info("Init edge property keys: {}", edgeIndexedKeys);
    }

    /**
     * Stops the service. This method blocks until the service has completely
     * shut down.
     */
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
