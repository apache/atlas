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

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
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
    private static final String METADATA_PREFIX = "metadata.graph.";

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

    protected TitanGraph initializeGraphDB(Configuration graphConfig) {
        LOG.info("Initializing titanGraph db");
        return TitanFactory.open(graphConfig);
    }

    protected void createIndicesForVertexKeys() {
        if (!titanGraph.getIndexedKeys(Vertex.class).isEmpty()) {
            LOG.info("Indexes already exist for titanGraph");
            return;
        }

        LOG.info("Indexes does not exist, Creating indexes for titanGraph");
        // todo - add index for vertex and edge property keys
        
        // Titan index backend does not support Mixed Index - must use a separate backend for that.
        // Using composite for now.  Literal matches only.  Using Global Titan index.
        TitanManagement mgmt = titanGraph.getManagementSystem();
             
        // This is the first run try-it-out index setup for property keys.
        // These were pulled from the Hive bridge.  Edge  indices to come.
        PropertyKey desc = mgmt.makePropertyKey("DESC").dataType(String.class).make();
        mgmt.buildIndex("byDesc",Vertex.class).addKey(desc).buildCompositeIndex();
        
        PropertyKey dbLoc = mgmt.makePropertyKey("DB_LOCATION_URI").dataType(String.class).make();
        mgmt.buildIndex("byDbloc",Vertex.class).addKey(dbLoc).buildCompositeIndex();
        
        PropertyKey name = mgmt.makePropertyKey("NAME").dataType(String.class).make();
        mgmt.buildIndex("byName",Vertex.class).addKey(name).buildCompositeIndex();
        
        PropertyKey tableName = mgmt.makePropertyKey("TBL_NAME").dataType(String.class).make();
        mgmt.buildIndex("byTableName",Vertex.class).addKey(tableName).buildCompositeIndex();
        
        PropertyKey tableType = mgmt.makePropertyKey("TBL_TYPE").dataType(String.class).make();
        mgmt.buildIndex("byTableType",Vertex.class).addKey(tableType).buildCompositeIndex();
        
        PropertyKey createTime = mgmt.makePropertyKey("CREATE_TIME").dataType(Long.class).make();
        mgmt.buildIndex("byCreateTime",Vertex.class).addKey(createTime).buildCompositeIndex();
        
        PropertyKey colName = mgmt.makePropertyKey("COLUMN_NAME").dataType(String.class).make();
        mgmt.buildIndex("byColName",Vertex.class).addKey(colName).buildCompositeIndex();
        
        PropertyKey typeName = mgmt.makePropertyKey("TYPE_NAME").dataType(String.class).make();
        mgmt.buildIndex("byTypeName",Vertex.class).addKey(typeName).buildCompositeIndex();
        
        /*  More attributes from the Hive bridge.

        PropertyKey ownerName = mgmt.makePropertyKey("OWNER_NAME").dataType(String.class).make();
        mgmt.buildIndex("byOwnerName",Vertex.class).addKey(ownerName).buildCompositeIndex();
        
        PropertyKey lastAccess = mgmt.makePropertyKey("LAST_ACCESS_TIME").dataType(Long.class).make();
        mgmt.buildIndex("byLastAccess",Vertex.class).addKey(lastAccess).buildCompositeIndex();

        PropertyKey viewExpandedText = mgmt.makePropertyKey("VIEW_EXPANDED_TEXT").dataType(String.class).make();
        mgmt.buildIndex("byExpandedText",Vertex.class).addKey(viewExpandedText).buildCompositeIndex();
        
        PropertyKey viewOrigText= mgmt.makePropertyKey("VIEW_ORIGINAL_TEXT").dataType(String.class).make();
        mgmt.buildIndex("byOrigText",Vertex.class).addKey(viewOrigText).buildCompositeIndex();
        
        PropertyKey comment = mgmt.makePropertyKey("COMMENT").dataType(Integer.class).make();
        mgmt.buildIndex("byComment",Vertex.class).addKey(comment).buildCompositeIndex();
        */
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
