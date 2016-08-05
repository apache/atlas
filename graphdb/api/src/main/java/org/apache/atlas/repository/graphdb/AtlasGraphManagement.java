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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.types.Multiplicity;

/**
 * Management interface for a graph
 *
 */
public interface AtlasGraphManagement {

    public static final Set<String> MULTIPLICITY_MANY_PROPERTY_KEYS =
            Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(
                    Constants.SUPER_TYPES_PROPERTY_KEY,
                    Constants.TRAIT_NAMES_PROPERTY_KEY )));


    /**
     * Checks whether a property with the given key has been defined in the graph schema.
     *
     * @param key
     * @return
     */
    boolean containsPropertyKey(String key);

    /**
     * Creates a mixed Vertex index for the graph
     *
     * @param index the name of the index to create
     * @param backingIndex the name of the backing index to use
     */
    void buildMixedVertexIndex(String index, String backingIndex);


    /**
     * Creates a mixed Edge index for the graph
     *
     * @param index the name of the index to create
     * @param backingIndex the name of the backing index to use
     */
    void buildMixedEdgeIndex(String index, String backingIndex);


    /**
     * Creates a full text index for the given property
     *
     * @param  indexName the name of the index to create
     * @param propertyKey full text property to index
     * @param backingIndex the name of the backing index to use
     */
    void createFullTextIndex(String indexName, AtlasPropertyKey propertyKey, String backingIndex);

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
    AtlasPropertyKey makePropertyKey(String propertyName, Class propertyClass, Multiplicity multiplicity);

    /**
     * @param propertyKey
     * @return
     */
    AtlasPropertyKey getPropertyKey(String propertyName);

    /**
     * Creates a composite index for the graph.
     *
     * @param propertyName
     * @param propertyKey
     * @param isUnique
     */
    void createCompositeIndex(String propertyName, AtlasPropertyKey propertyKey, boolean isUnique);

    /**
     * Adds a property key to the given index in the graph.
     *
     * @param vertexIndex
     * @param propertyKey
     */
    void addIndexKey(String vertexIndex, AtlasPropertyKey propertyKey);

    /**
     * Looks up the index with the specified name in the graph.  Returns null if
     * there is no index with the given name.
     *
     * @param edgeIndex
     * @return
     */
    AtlasGraphIndex getGraphIndex(String indexName);


}
