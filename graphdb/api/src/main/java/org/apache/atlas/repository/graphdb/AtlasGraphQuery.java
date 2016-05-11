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

/**
 * Represents a query against the graph.
 *
 * @param <V> vertex class used by the graph
 * @param <E> edge class used by the graph
 */
public interface AtlasGraphQuery<V,E> {

    /**
     * Adds a predicate that the returned elements must have the specified 
     * property and that one of the values of the property must be the
     * given value.
     * 
     * @param propertyKey
     * @param value
     * @return
     */
    AtlasGraphQuery<V,E> has(String propertyKey, Object value);

    /**
     * Executes the query and returns the matching vertices.
     * @return
     */
    Iterable<AtlasVertex<V, E>> vertices();

    
    /**
     * Executes the query and returns the matching edges.
     * @return
     */
    Iterable<AtlasEdge<V, E>> edges();

    

    /**
     * Adds a predicate that the returned elements must have the specified 
     * property and that its value matches the criterion specified.
     * 
     * @param propertyKey
     * @param value
     * @return
     */
    AtlasGraphQuery<V,E> has(String propertyKey, ComparisionOperator compMethod, Object value);

    public static enum ComparisionOperator {
        GREATER_THAN_EQUAL,
        EQUAL,
        LESS_THAN_EQUAL
    }

}
