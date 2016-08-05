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
package org.apache.atlas.repository.graphdb.titan0;

import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

/**
 * Titan 0.5.4 implementation of AtlasGraphManagement.
 */
public class Titan0DatabaseManager implements AtlasGraphManagement {

    private static final Logger LOG = LoggerFactory.getLogger(Titan0DatabaseManager.class);

    private TitanManagement management;

    public Titan0DatabaseManager(TitanManagement managementSystem) {

        management = managementSystem;
    }

    @Override
    public void buildMixedVertexIndex(String index, String backingIndex) {
        buildMixedIndex(index, Vertex.class, backingIndex);
    }

    @Override
    public void buildMixedEdgeIndex(String index, String backingIndex) {
        buildMixedIndex(index, Edge.class, backingIndex);
    }

    private void buildMixedIndex(String index, Class<? extends Element> titanClass, String backingIndex) {

        management.buildIndex(index, titanClass).buildMixedIndex(backingIndex);
    }

    @Override
    public void createFullTextIndex(String indexName, AtlasPropertyKey propertyKey, String backingIndex) {

        PropertyKey fullText = TitanObjectFactory.createPropertyKey(propertyKey);

        management.buildIndex(indexName, Vertex.class)
                .addKey(fullText, com.thinkaurelius.titan.core.schema.Parameter.of("mapping", Mapping.TEXT))
                .buildMixedIndex(backingIndex);
    }

    @Override
    public boolean containsPropertyKey(String propertyKey) {
        return management.containsPropertyKey(propertyKey);
    }

    @Override
    public void rollback() {
        management.rollback();

    }

    @Override
    public void commit() {
        management.commit();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.atlas.repository.graphdb.AtlasGraphManagement#makePropertyKey(
     * java.lang.String, java.lang.Class,
     * org.apache.atlas.typesystem.types.Multiplicity)
     */
    @Override
    public AtlasPropertyKey makePropertyKey(String propertyName, Class propertyClass, Multiplicity multiplicity) {

        PropertyKeyMaker propertyKeyBuilder = management.makePropertyKey(propertyName).dataType(propertyClass);

        if (multiplicity != null) {
            Cardinality cardinality = TitanObjectFactory.createCardinality(multiplicity);
            propertyKeyBuilder.cardinality(cardinality);
        }
        PropertyKey propertyKey = propertyKeyBuilder.make();
        return GraphDbObjectFactory.createPropertyKey(propertyKey);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.atlas.repository.graphdb.AtlasGraphManagement#getPropertyKey(
     * java.lang.String)
     */
    @Override
    public AtlasPropertyKey getPropertyKey(String propertyName) {

        return GraphDbObjectFactory.createPropertyKey(management.getPropertyKey(propertyName));
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.atlas.repository.graphdb.AtlasGraphManagement#
     * createCompositeIndex(java.lang.String,
     * org.apache.atlas.repository.graphdb.AtlasPropertyKey, boolean)
     */
    @Override
    public void createCompositeIndex(String propertyName, AtlasPropertyKey propertyKey, boolean enforceUniqueness) {
        PropertyKey titanKey = TitanObjectFactory.createPropertyKey(propertyKey);
        TitanManagement.IndexBuilder indexBuilder = management.buildIndex(propertyName, Vertex.class).addKey(titanKey);
        if (enforceUniqueness) {
            indexBuilder.unique();
        }
        indexBuilder.buildCompositeIndex();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.atlas.repository.graphdb.AtlasGraphManagement#addIndexKey(java
     * .lang.String, org.apache.atlas.repository.graphdb.AtlasPropertyKey)
     */
    @Override
    public void addIndexKey(String indexName, AtlasPropertyKey propertyKey) {
        PropertyKey titanKey = TitanObjectFactory.createPropertyKey(propertyKey);
        TitanGraphIndex vertexIndex = management.getGraphIndex(indexName);
        management.addIndexKey(vertexIndex, titanKey);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.atlas.repository.graphdb.AtlasGraphManagement#getGraphIndex(
     * java.lang.String)
     */
    @Override
    public AtlasGraphIndex getGraphIndex(String indexName) {
        TitanGraphIndex index = management.getGraphIndex(indexName);
        return GraphDbObjectFactory.createGraphIndex(index);
    }

}
