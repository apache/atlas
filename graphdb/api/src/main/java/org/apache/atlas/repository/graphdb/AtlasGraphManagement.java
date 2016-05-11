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

import org.apache.atlas.typesystem.types.Multiplicity;

/**
 * Management interface for a graph
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
     * @param propertyKey the name of the property
     * @param backingIndex the name of the backing index to use
     */
    void createFullTextIndex(String indexName, String propertyKey, String backingIndex);

    /**
     * Rolls back the changes that have been made to the management system.
     */
    void rollback();

    /**
     * Commits the changes that have been made to the management system.
     */

    void commit();

    /**
     * Creates a composite index for the given property.
     * 
     * @param propertyName name of the property being indexed
     * @param propertyClass the java class of the property value(s)
     * @param multiplicity the multiplicity of the property
     * @param isUnique whether the property values must be unique
     */
    void createCompositeIndex(String propertyName, Class propertyClass, Multiplicity multiplicity,
            boolean isUnique);
    
    /**
     * Creates an index for a property.
     * 
     * @param propertyName name of the property being indexed
     * @param vertexIndexName name of the index to create
     * @param propertyClass the java class of the property value(s)
     * @param multiplicity the multiplicity of the property
     */
    void createBackingIndex(String propertyName, String vertexIndexName, Class propertyClass,
            Multiplicity multiplicity);


}
