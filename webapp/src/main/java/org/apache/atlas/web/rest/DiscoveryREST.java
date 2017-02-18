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
package org.apache.atlas.web.rest;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.web.util.Servlets;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

/**
 * REST interface for data discovery using dsl or full text search
 */
@Path("v2/search")
@Singleton
public class DiscoveryREST {
    private final AtlasDiscoveryService atlasDiscoveryService;

    @Inject
    public DiscoveryREST(AtlasDiscoveryService discoveryService) {
        this.atlasDiscoveryService = discoveryService;
    }

    /**
     * Retrieve data for the specified DSL
     * @param query DSL query
     * @param limit limit the result set to only include the specified number of entries
     * @param offset start offset of the result set (useful for pagination)
     * @return Search results
     * @throws AtlasBaseException
     * @HTTP 200 On successful DSL execution with some results, might return an empty list if execution succeeded
     * without any results
     * @HTTP 400 Invalid DSL or query parameters
     */
    @GET
    @Path("/dsl")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasSearchResult searchUsingDSL(@QueryParam("query") String query,
                                            @QueryParam("limit") int limit,
                                            @QueryParam("offset") int offset) throws AtlasBaseException {

        AtlasSearchResult ret = atlasDiscoveryService.searchUsingDslQuery(query, limit, offset);

        return ret;
    }

    /**
     * Retrieve data for the specified fulltext query
     * @param query Fulltext query
     * @param limit limit the result set to only include the specified number of entries
     * @param offset start offset of the result set (useful for pagination)
     * @return Search results
     * @throws AtlasBaseException
     * @HTTP 200 On successful FullText lookup with some results, might return an empty list if execution succeeded
     * without any results
     * @HTTP 400 Invalid fulltext or query parameters
     */
    @GET
    @Path("/fulltext")
    @Consumes(Servlets.JSON_MEDIA_TYPE)
    @Produces(Servlets.JSON_MEDIA_TYPE)
    public AtlasSearchResult searchUsingFullText(@QueryParam("query") String query,
                                                 @QueryParam("limit") int limit,
                                                 @QueryParam("offset") int offset) throws AtlasBaseException {

        AtlasSearchResult ret = atlasDiscoveryService.searchUsingFullTextQuery(query, limit, offset);

        return ret;
    }
}