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

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import javax.script.Bindings;
import javax.script.ScriptException;

/**
 * Represents a graph
 * 
 * @param <V> vertex implementation class
 * @param <E> edge implementation class
 */
public interface AtlasGraph<V,E> {

    /**
     * Adds an edge to the graph
     * 
     * @param outVertex
     * @param inVertex
     * @param label
     * @return
     */
    AtlasEdge<V,E> addEdge(AtlasVertex<V,E> outVertex, AtlasVertex<V,E> inVertex, String label);

    /**
     * Adds a vertex to the graph
     * 
     * @return
     */
    AtlasVertex<V,E> addVertex();

    /**
     * Removes the specified edge from the graph
     * 
     * @param edge
     */
    void removeEdge(AtlasEdge<V,E> edge);

    /**
     * Removes the specified vertex from the graph.
     * 
     * @param vertex
     */
    void removeVertex(AtlasVertex<V,E> vertex);

    /**
     * Retrieves the edge with the specified id
     * @param edgeId
     * @return
     */
    AtlasEdge<V,E> getEdge(String edgeId);

    /**
     * Gets all the edges in the graph.
     * @return
     */
    Iterable<AtlasEdge<V,E>> getEdges();

    /**
     * Gets all the vertices in the graph.
     * @return
     */
    Iterable<AtlasVertex<V,E>> getVertices();

    /**
     * Gets the vertex with the specified id
     * 
     * @param vertexId
     * @return
     */
    AtlasVertex<V, E> getVertex(String vertexId);

    /**
     * Gets the names of the indexes on edges
     * type.
     * 
     * @param type
     * @return
     */
    Set<String> getEdgeIndexKeys();


    /**
     * Gets the names of the indexes on vertices.
     * type.
     * 
     * @param type
     * @return
     */
    Set<String> getVertexIndexKeys();

    
    /**
     * Finds the vertices where the given property key
     * has the specified value.  For multi-valued properties,
     * finds the vertices where the value list contains
     * the specified value.
     * 
     * @param key
     * @param value
     * @return
     */
    Iterable<AtlasVertex<V,E>> getVertices(String key, Object value);

    /**
     * Creates a graph query
     * @return
     */
    AtlasGraphQuery<V,E> query();

    /**
     * Creates an index query
     * 
     * @param indexName index name
     * @param queryString the query
     * 
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html">Elastic Search Reference</a> for query syntax
     */
    AtlasIndexQuery<V,E> indexQuery(String indexName, String queryString);

    /**
     * Gets the management object associated with this graph and opens a transaction
     * for changes that are made.
     * @return
     */
    AtlasGraphManagement getManagementSystem();

    /**
     * Commits changes made to the graph in the current transaction.
     */
    void commit();

    /**
     * Rolls back changes made to the graph in the current transaction.
     */
    void rollback();

    /**
     * Unloads and releases any resources associated with the graph.
     */
    void shutdown();
    
    /**
     * deletes everything in the graph.  For testing only 
     */
    void clear();

    /**
     * Converts the graph to gson and writes it to the specified stream
     * 
     * @param os
     * @throws IOException
     */
    void exportToGson(OutputStream os) throws IOException;
    
    //the following methods insulate Atlas from the details
    //of the interaction with Gremlin
       
    
    /**
     *    
     * When we construct Gremlin select queries, the information we request
     * is grouped by the vertex the information is coming from.  Each vertex
     * is assigned a column name which uniquely identifies it.  The queries
     * are specially formatted so that the value associated with each of
     * these column names is an array with the various things we need
     * about that particular vertex.  The query evaluator creates a mapping
     * that knows what index each bit of information is stored at within
     * this array.
     * <p/>
     * When we execute a Gremlin query, the exact java objects we get
     * back vary depending on whether Gremlin 2 or Gremlin 3 is being used.
     * This method takes as input a raw row result that was obtained by
     * executing a Gremlin query and extracts the value that was found
     * at the given index in the array for the given column name.
     * <p/>
     * If the value found is a vertex or edge, it is automatically converted
     * to an AtlasVertex/AtlasEdge.
     * 
     * @param rowValue the raw row value that was returned by Gremin
     * @param colName the column name to use
     * @param idx the index of the value within the column to retrieve.    
     * 
     */
    Object getGremlinColumnValue(Object rowValue, String colName, int idx);

    /**
     * When Gremlin queries are executed, they return 
     * Vertex and Edge objects that are specific to the underlying
     * graph database.  This method provides a way to convert these
     * objects back into the AtlasVertex/AtlasEdge objects that
     * Atlas requires. 
     * 
     * @param rawValue the value that was obtained from Gremlin
     * @return either an AtlasEdge, an AtlasVertex, or the original
     * value depending on whether the rawValue represents an edge,
     * vertex, or something else.
     * 
     */
    Object convertGremlinValue(Object rawValue);

    /**
     * Gremlin 2 and Gremlin 3 represent the results of "path"
     * queries differently.  This method takes as input the
     * path from Gremlin and returns the list of objects in the path.
     * 
     * @param rawValue
     * @return
     */
    List<Object> convertPathQueryResultToList(Object rawValue);

    /**
     * Gets the version of Gremlin that this graph uses.
     * 
     * @return
     */
    GremlinVersion getSupportedGremlinVersion();
    
    /**
     * Executes a gremlin query, returns an object with the raw
     * result.
     * 
     * @param gremlinQuery
     * @return
     */
    Object executeGremlinScript(String gremlinQuery) throws ScriptException;
     

}