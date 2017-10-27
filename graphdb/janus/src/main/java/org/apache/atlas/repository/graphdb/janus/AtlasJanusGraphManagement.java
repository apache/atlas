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
package org.apache.atlas.repository.graphdb.janus;

import com.google.common.base.Preconditions;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.internal.Token;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasEdgeLabel;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Janus implementation of AtlasGraphManagement.
 */
public class AtlasJanusGraphManagement implements AtlasGraphManagement {

    private static final Logger LOG = LoggerFactory.getLogger(AtlasJanusGraphManagement.class);

    private static final char[] RESERVED_CHARS = { '{', '}', '"', '$', Token.SEPARATOR_CHAR };

    private AtlasJanusGraph graph;
    private JanusGraphManagement management;

    private Set<String> newMultProperties = new HashSet<>();

    public AtlasJanusGraphManagement(AtlasJanusGraph graph, JanusGraphManagement managementSystem) {
        this.management = managementSystem;
        this.graph = graph;
    }

    @Override
    public void createVertexIndex(String propertyName, String backingIndex, List<AtlasPropertyKey> propertyKeys) {

        JanusGraphManagement.IndexBuilder indexBuilder = management.buildIndex(propertyName, Vertex.class);
        for (AtlasPropertyKey key : propertyKeys) {
            PropertyKey janusKey = AtlasJanusObjectFactory.createPropertyKey(key);
            indexBuilder.addKey(janusKey);
        }
        indexBuilder.buildMixedIndex(backingIndex);
    }

    @Override
    public void createEdgeIndex(String index, String backingIndex) {
        buildMixedIndex(index, Edge.class, backingIndex);
    }

    private void buildMixedIndex(String index, Class<? extends Element> janusClass, String backingIndex) {

        management.buildIndex(index, janusClass).buildMixedIndex(backingIndex);
    }

    @Override
    public void createFullTextIndex(String indexName, AtlasPropertyKey propertyKey, String backingIndex) {

        PropertyKey fullText = AtlasJanusObjectFactory.createPropertyKey(propertyKey);

        management.buildIndex(indexName, Vertex.class)
                .addKey(fullText, org.janusgraph.core.schema.Parameter.of("mapping", Mapping.TEXT))
                .buildMixedIndex(backingIndex);
    }

    @Override
    public boolean containsPropertyKey(String propertyName) {
        return management.containsPropertyKey(propertyName);
    }

    @Override
    public void rollback() {
        management.rollback();

    }

    @Override
    public void commit() {
        graph.addMultiProperties(newMultProperties);
        newMultProperties.clear();
        management.commit();
    }

    private static void checkName(String name) {
        //for some reason, name checking was removed from StandardPropertyKeyMaker.make()
        //in Janus.  For consistency, do the check here.
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "Need to specify name");
        for (char c : RESERVED_CHARS) {
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name can not contains reserved character %s: %s", c,
                    name);
        }

    }

    @Override
    public AtlasPropertyKey makePropertyKey(String propertyName, Class propertyClass, AtlasCardinality cardinality) {

        if (cardinality.isMany()) {
            newMultProperties.add(propertyName);
        }
        PropertyKeyMaker propertyKeyBuilder = management.makePropertyKey(propertyName).dataType(propertyClass);
        if (cardinality != null) {
            Cardinality janusCardinality = AtlasJanusObjectFactory.createCardinality(cardinality);
            propertyKeyBuilder.cardinality(janusCardinality);
        }
        PropertyKey propertyKey = propertyKeyBuilder.make();
        return GraphDbObjectFactory.createPropertyKey(propertyKey);
    }

    @Override
    public AtlasEdgeLabel makeEdgeLabel(String label) {
        EdgeLabel edgeLabel = management.makeEdgeLabel(label).make();

        return GraphDbObjectFactory.createEdgeLabel(edgeLabel);
    }

    @Override
    public void deletePropertyKey(String propertyKey) {
        PropertyKey janusPropertyKey = management.getPropertyKey(propertyKey);

        if (null == janusPropertyKey) return;

        for (int i = 0;; i++) {
            String deletedKeyName = janusPropertyKey + "_deleted_" + i;
            if (null == management.getPropertyKey(deletedKeyName)) {
                management.changeName(janusPropertyKey, deletedKeyName);
                break;
            }
        }
    }

    @Override
    public AtlasPropertyKey getPropertyKey(String propertyName) {
        checkName(propertyName);
        return GraphDbObjectFactory.createPropertyKey(management.getPropertyKey(propertyName));
    }

    @Override
    public AtlasEdgeLabel getEdgeLabel(String label) {
        return GraphDbObjectFactory.createEdgeLabel(management.getEdgeLabel(label));
    }

    public void createExactMatchVertexIndex(String propertyName, boolean enforceUniqueness,
                                      List<AtlasPropertyKey> propertyKeys) {

        JanusGraphManagement.IndexBuilder indexBuilder = management.buildIndex(propertyName, Vertex.class);
        for (AtlasPropertyKey key : propertyKeys) {
            PropertyKey janusKey = AtlasJanusObjectFactory.createPropertyKey(key);
            indexBuilder.addKey(janusKey);
        }
        if (enforceUniqueness) {
            indexBuilder.unique();
        }
        indexBuilder.buildCompositeIndex();
    }

    @Override
    public void addVertexIndexKey(String indexName, AtlasPropertyKey propertyKey) {
        PropertyKey janusKey = AtlasJanusObjectFactory.createPropertyKey(propertyKey);
        JanusGraphIndex vertexIndex = management.getGraphIndex(indexName);
        management.addIndexKey(vertexIndex, janusKey);
    }

    @Override
    public AtlasGraphIndex getGraphIndex(String indexName) {
        JanusGraphIndex index = management.getGraphIndex(indexName);
        return GraphDbObjectFactory.createGraphIndex(index);
    }

    @Override
    public void createExactMatchIndex(String propertyName, boolean isUnique,
            List<AtlasPropertyKey> propertyKeys) {
        createExactMatchVertexIndex(propertyName, isUnique, propertyKeys);
    }

}
