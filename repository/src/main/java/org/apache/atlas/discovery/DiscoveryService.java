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

package org.apache.atlas.discovery;

import org.apache.atlas.query.QueryParams;

import java.util.List;
import java.util.Map;

/**
 * Metadata discovery service.
 */
public interface DiscoveryService {

    /**
     * Searches using Full text query
     * @param query query string
     * @param queryParams Default query parameters like limit, offset
     * @return results json
     * @throws DiscoveryException
     */
    String searchByFullText(String query, QueryParams queryParams) throws DiscoveryException;

    /**
     * Searches using DSL query
     * @param dslQuery query string
     * @param queryParams Default query parameters like limit, offset
     * @return results json
     * @throws DiscoveryException
     */
    String searchByDSL(String dslQuery, QueryParams queryParams) throws DiscoveryException;

    /**
     * Assumes the User is familiar with the persistence structure of the Repository.
     * The given query is run uninterpreted against the underlying Graph Store.
     * The results are returned as a List of Rows. each row is a Map of Key,Value pairs.
     *
     * @param gremlinQuery query in gremlin dsl format
     * @return List of Maps
     * @throws org.apache.atlas.discovery.DiscoveryException
     */
    List<Map<String, String>> searchByGremlin(String gremlinQuery) throws DiscoveryException;
}
