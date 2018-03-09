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

package org.apache.atlas.repository.graphdb;

import java.util.List;

/**
 * Management interface for a graph.
 *
 */
public interface AtlasGraphManagement {

    /**
     * Checks whether a property with the given key has been defined in the graph schema.
     *
     * @param key
     * @return
     */
    boolean containsPropertyKey(String key);

    /**
     * Rolls back the changes that have been made to the management system.
     */
    void rollback();

    /**
     * Commits the changes that have been made to the management system.
     */

    void commit();

    /**
     * @param propertyName
     * @param propertyClass
     * @param cardinality
     * @return
     */
    AtlasPropertyKey makePropertyKey(String propertyName, Class propertyClass, AtlasCardinality cardinality);

    /**
     *
     * @param label edge label to be created
     */
    AtlasEdgeLabel makeEdgeLabel(String label);

    /**
     *  @param propertyKey
     *
     */
    void deletePropertyKey(String propertyKey);

    /**
     * @param propertyName
     * @return
     */
    AtlasPropertyKey getPropertyKey(String propertyName);

    /**
     * @param label
     * @return
     */
    AtlasEdgeLabel getEdgeLabel(String label);

    /**
     * Creates a composite vertex index for the graph.
     *
     * @param propertyName
     * @param isUnique
     * @param propertyKeys
     */
    void createVertexCompositeIndex(String propertyName, boolean isUnique, List<AtlasPropertyKey> propertyKeys);

    /**
     * Creates a composite edge index for the graph.
     *
     * @param propertyName
     * @param isUnique
     * @param propertyKeys
     */
    void createEdgeCompositeIndex(String propertyName, boolean isUnique, List<AtlasPropertyKey> propertyKeys);

    /**
     * Looks up the index with the specified name in the graph.  Returns null if
     * there is no index with the given name.
     *
     * @param indexName
     * @return
     */
    AtlasGraphIndex getGraphIndex(String indexName);

    /**
     * Checks if a vertex-centric edge exists already.
     *
     * @param label
     * @param indexName
     * @return
     */
    boolean edgeIndexExist(String label, String indexName);

    /**
     * Creates a mixed Vertex index for the graph.
     *
     * @param name the name of the index to create
     * @param backingIndex the name of the backing index to use
     * @param propertyKeys list of propertyKeys to be added to the index
     */
    void createVertexMixedIndex(String name, String backingIndex, List<AtlasPropertyKey> propertyKeys);

    /**
     * Creates a mixed Edge index for the graph.
     *
     * @param index the name of the index to create
     * @param backingIndex the name of the backing index to use
     * @param propertyKeys list of propertyKeys to be added to the index
     */
    void createEdgeMixedIndex(String index, String backingIndex, List<AtlasPropertyKey> propertyKeys);

    /**
     * Creates a vertex-centric edge index for the graph.
     *
     * @param label edge label name
     * @param indexName name of the edge index
     * @param edgeDirection direction of the edge to index
     * @param propertyKeys edge property keys to be added to the index
     */
    void createEdgeIndex(String label, String indexName, AtlasEdgeDirection edgeDirection, List<AtlasPropertyKey> propertyKeys);

    /**
     * Creates a full text index for the given property.
     *
     * @param index the name of the index to create
     * @param backingIndex the name of the backing index to use
     * @param propertyKeys list of propertyKeys to be added to the index
     */
    void createFullTextMixedIndex(String index, String backingIndex, List<AtlasPropertyKey> propertyKeys);

    /**
     * Adds a property key to the given index in the graph.
     *
     * @param vertexIndex
     * @param propertyKey
     */
    void addMixedIndex(String vertexIndex, AtlasPropertyKey propertyKey);
}
