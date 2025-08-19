/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.discovery;


import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasQuickSearchResult;
import org.apache.atlas.model.discovery.AtlasSuggestionsResult;
import org.apache.atlas.model.discovery.QuickSearchParameters;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.model.profile.AtlasUserSavedSearch;
import org.apache.atlas.model.searchlog.SearchLogSearchParams;
import org.apache.atlas.model.searchlog.SearchLogSearchResult;

import java.util.List;
import java.util.Map;

public interface AtlasDiscoveryService {
    /**
     * Search for entities matching the search criteria
     * @param searchParameters Search criteria
     * @return Matching entities
     * @throws AtlasBaseException
     */
    AtlasSearchResult searchWithParameters(SearchParameters searchParameters) throws AtlasBaseException;
    /**
     * Search for direct ES query
     * @param searchParams Search criteria
     * @return Matching entities
     * @throws AtlasBaseException
     */
    AtlasSearchResult directIndexSearch(SearchParams searchParams) throws AtlasBaseException;

    AtlasSearchResult directIndexSearch(SearchParams searchParams, boolean useVertexEdgeBulkFetching) throws AtlasBaseException;

    /**
     * Search for direct ES query in janusgraph_edge_index
     * @param searchParams Search criteria
     * @return Matching entities
     * @throws AtlasBaseException
     */
    AtlasSearchResult directRelationshipIndexSearch(SearchParams searchParams) throws AtlasBaseException;

    /**
     * Search for direct ES query on search logs index
     * @param searchParams Search criteria
     * @return Matching search logs
     * @throws AtlasBaseException
     */
    SearchLogSearchResult searchLogs(SearchLogSearchParams searchParams) throws AtlasBaseException;

    /**
     * Raw Elasticsearch search. Returns direct ES response as-is.
     */
    Map<String, Object> directEsIndexSearch(SearchParams searchParams) throws AtlasBaseException;

    /**
     * Elasticsearch count API. Returns document count for the query.
     */
    Long directCountIndexSearch(SearchParams searchParams) throws AtlasBaseException;

}
