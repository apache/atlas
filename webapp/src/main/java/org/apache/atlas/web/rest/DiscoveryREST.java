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

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.SortOrder;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.profile.AtlasUserSavedSearch;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.springframework.stereotype.Service;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;

/**
 * REST interface for data discovery using dsl or full text search
 */
@Path("v2/search")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class DiscoveryREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.DiscoveryREST");

    @Context
    private       HttpServletRequest httpServletRequest;
    private final int                maxFullTextQueryLength;
    private final int                maxDslQueryLength;

    private final AtlasTypeRegistry     typeRegistry;
    private final AtlasDiscoveryService atlasDiscoveryService;

    @Inject
    public DiscoveryREST(AtlasTypeRegistry typeRegistry, AtlasDiscoveryService atlasDiscoveryService, Configuration configuration) {
        this.typeRegistry           = typeRegistry;
        this.atlasDiscoveryService  = atlasDiscoveryService;
        this.maxFullTextQueryLength = configuration.getInt(Constants.MAX_FULLTEXT_QUERY_STR_LENGTH, 4096);
        this.maxDslQueryLength      = configuration.getInt(Constants.MAX_DSL_QUERY_STR_LENGTH, 4096);
    }

    /**
     * Retrieve data for the specified DSL
     *
     * @param query          DSL query
     * @param typeName       limit the result to only entities of specified type or its sub-types
     * @param classification limit the result to only entities tagged with the given classification or or its sub-types
     * @param limit          limit the result set to only include the specified number of entries
     * @param offset         start offset of the result set (useful for pagination)
     * @return Search results
     * @throws AtlasBaseException
     * @HTTP 200 On successful DSL execution with some results, might return an empty list if execution succeeded
     * without any results
     * @HTTP 400 Invalid DSL or query parameters
     */
    @GET
    @Path("/dsl")
    public AtlasSearchResult searchUsingDSL(@QueryParam("query")          String query,
                                            @QueryParam("typeName")       String typeName,
                                            @QueryParam("classification") String classification,
                                            @QueryParam("limit")          int    limit,
                                            @QueryParam("offset")         int    offset) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        Servlets.validateQueryParamLength("classification", classification);

        if (StringUtils.isNotEmpty(query) && query.length() > maxDslQueryLength) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_DSL_QUERY_STR_LENGTH);
        }

        AtlasPerfTracer perf = null;

        try {

            query = Servlets.decodeQueryString(query);

            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchUsingDSL(" + query + "," + typeName
                        + "," + classification + "," + limit + "," + offset + ")");
            }

            String queryStr = atlasDiscoveryService.getDslQueryUsingTypeNameClassification(query, typeName, classification);

            return atlasDiscoveryService.searchUsingDslQuery(queryStr, limit, offset);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }



    /**
     * Retrieve data for the specified fulltext query
     *
     * @param query  Fulltext query
     * @param limit  limit the result set to only include the specified number of entries
     * @param offset start offset of the result set (useful for pagination)
     * @return Search results
     * @throws AtlasBaseException
     * @HTTP 200 On successful FullText lookup with some results, might return an empty list if execution succeeded
     * without any results
     * @HTTP 400 Invalid fulltext or query parameters
     */
    @GET
    @Path("/fulltext")
    public AtlasSearchResult searchUsingFullText(@QueryParam("query")                  String  query,
                                                 @QueryParam("excludeDeletedEntities") boolean excludeDeletedEntities,
                                                 @QueryParam("limit")                  int     limit,
                                                 @QueryParam("offset")                 int     offset) throws AtlasBaseException {
        // Validate FullText query for max allowed length
        if(StringUtils.isNotEmpty(query) && query.length() > maxFullTextQueryLength){
            throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH );
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchUsingFullText(" + query + "," +
                        limit + "," + offset + ")");
            }

            return atlasDiscoveryService.searchUsingFullTextQuery(query, excludeDeletedEntities, limit, offset);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Retrieve data for the specified fulltext query
     *
     * @param query          Fulltext query
     * @param typeName       limit the result to only entities of specified type or its sub-types
     * @param classification limit the result to only entities tagged with the given classification or or its sub-types
     * @param limit          limit the result set to only include the specified number of entries
     * @param offset         start offset of the result set (useful for pagination)
     * @return Search results
     * @throws AtlasBaseException
     * @HTTP 200 On successful FullText lookup with some results, might return an empty list if execution succeeded
     * without any results
     * @HTTP 400 Invalid fulltext or query parameters
     */
    @GET
    @Path("/basic")
    public AtlasSearchResult searchUsingBasic(@QueryParam("query")                  String  query,
                                              @QueryParam("typeName")               String  typeName,
                                              @QueryParam("classification")         String  classification,
                                              @QueryParam("excludeDeletedEntities") boolean excludeDeletedEntities,
                                              @QueryParam("limit")                  int     limit,
                                              @QueryParam("offset")                 int     offset) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        Servlets.validateQueryParamLength("classification", classification);
        if (StringUtils.isNotEmpty(query) && query.length() > maxFullTextQueryLength) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchUsingBasic(" + query + "," +
                        typeName + "," + classification + "," + limit + "," + offset + ")");
            }

            SearchParameters searchParameters = new SearchParameters();
            searchParameters.setTypeName(typeName);
            searchParameters.setClassification(classification);
            searchParameters.setQuery(query);
            searchParameters.setExcludeDeletedEntities(excludeDeletedEntities);
            searchParameters.setLimit(limit);
            searchParameters.setOffset(offset);

            return atlasDiscoveryService.searchWithParameters(searchParameters);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Retrieve data for the specified attribute search query
     *
     * @param attrName        Attribute name
     * @param attrValuePrefix Attibute value to search on
     * @param typeName        limit the result to only entities of specified type or its sub-types
     * @param limit           limit the result set to only include the specified number of entries
     * @param offset          start offset of the result set (useful for pagination)
     * @return Search results
     * @throws AtlasBaseException
     * @HTTP 200 On successful FullText lookup with some results, might return an empty list if execution succeeded
     * without any results
     * @HTTP 400 Invalid wildcard or query parameters
     */
    @GET
    @Path("/attribute")
    public AtlasSearchResult searchUsingAttribute(@QueryParam("attrName")        String attrName,
                                                  @QueryParam("attrValuePrefix") String attrValuePrefix,
                                                  @QueryParam("typeName")        String typeName,
                                                  @QueryParam("limit")           int    limit,
                                                  @QueryParam("offset")          int    offset) throws AtlasBaseException {
        Servlets.validateQueryParamLength("attrName", attrName);
        Servlets.validateQueryParamLength("attrValuePrefix", attrValuePrefix);
        Servlets.validateQueryParamLength("typeName", typeName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchUsingAttribute(" + attrName + "," +
                        attrValuePrefix + "," + typeName + "," + limit + "," + offset + ")");
            }

            if (StringUtils.isEmpty(attrName) && StringUtils.isEmpty(attrValuePrefix)) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                        String.format("attrName : %s, attrValue: %s for attribute search.", attrName, attrValuePrefix));
            }

            if (StringUtils.isEmpty(attrName)) {
                AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

                if (entityType != null) {
                    String[] defaultAttrNames = new String[] { AtlasClient.QUALIFIED_NAME, AtlasClient.NAME };

                    for (String defaultAttrName : defaultAttrNames) {
                        AtlasStructType.AtlasAttribute attribute = entityType.getAttribute(defaultAttrName);

                        if (attribute != null) {
                            attrName = defaultAttrName;

                            break;
                        }
                    }
                }

                if (StringUtils.isEmpty(attrName)) {
                    attrName = AtlasClient.QUALIFIED_NAME;
                }
            }

            SearchParameters searchParams = new SearchParameters();
            FilterCriteria   attrFilter   = new FilterCriteria();

            attrFilter.setAttributeName(StringUtils.isEmpty(attrName) ? AtlasClient.QUALIFIED_NAME : attrName);
            attrFilter.setOperator(SearchParameters.Operator.STARTS_WITH);
            attrFilter.setAttributeValue(attrValuePrefix);

            searchParams.setTypeName(typeName);
            searchParams.setEntityFilters(attrFilter);
            searchParams.setOffset(offset);
            searchParams.setLimit(limit);

            return searchWithParameters(searchParams);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Attribute based search for entities satisfying the search parameters
     *
     * @param parameters Search parameters
     * @return Atlas search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     * @HTTP 400 Tag/Entity doesn't exist or Tag/entity filter is present without tag/type name
     */
    @Path("basic")
    @POST
    public AtlasSearchResult searchWithParameters(SearchParameters parameters) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchWithParameters(" + parameters + ")");
            }

            if (parameters.getLimit() < 0 || parameters.getOffset() < 0) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Limit/offset should be non-negative");
            }

            if (StringUtils.isEmpty(parameters.getTypeName()) && !isEmpty(parameters.getEntityFilters())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "EntityFilters specified without Type name");
            }

            if (StringUtils.isEmpty(parameters.getClassification()) && !isEmpty(parameters.getTagFilters())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "TagFilters specified without tag name");
            }

            if (StringUtils.isEmpty(parameters.getTypeName()) && StringUtils.isEmpty(parameters.getClassification()) && StringUtils.isEmpty(parameters.getQuery()) && StringUtils.isEmpty(parameters.getTermName())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_SEARCH_PARAMS);
            }

            validateSearchParameters(parameters);

            return atlasDiscoveryService.searchWithParameters(parameters);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Relationship search to search for related entities satisfying the search parameters
     *
     * @param guid            Attribute name
     * @param relation        relationName
     * @param sortByAttribute sort the result using this attribute name, default value is 'name'
     * @param sortOrder       sorting order
     * @param limit           limit the result set to only include the specified number of entries
     * @param offset          start offset of the result set (useful for pagination)
     * @return Atlas search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     * @HTTP 400 guid is not a valid entity type or attributeName is not a valid relationship attribute
     */
    @GET
    @Path("relationship")
    public AtlasSearchResult searchRelatedEntities(@QueryParam("guid")                   String    guid,
                                                   @QueryParam("relation")               String    relation,
                                                   @QueryParam("sortBy")                 String    sortByAttribute,
                                                   @QueryParam("sortOrder")              SortOrder sortOrder,
                                                   @QueryParam("excludeDeletedEntities") boolean   excludeDeletedEntities,
                                                   @QueryParam("limit")                  int       limit,
                                                   @QueryParam("offset")                 int       offset) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);
        Servlets.validateQueryParamLength("relation", relation);
        Servlets.validateQueryParamLength("sortBy", sortByAttribute);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.relatedEntitiesSearchUsingGremlin(" + guid +
                        ", " + relation + ", " + sortByAttribute + ", " + sortOrder + ", " + excludeDeletedEntities + ", " + ", " + limit + ", " + offset + ")");
            }

            return atlasDiscoveryService.searchRelatedEntities(guid, relation, sortByAttribute, sortOrder, excludeDeletedEntities, limit, offset);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * @param savedSearch
     * @return the saved search-object
     * @throws AtlasBaseException
     * @throws IOException
     */
    @POST
    @Path("saved")
    public AtlasUserSavedSearch addSavedSearch(AtlasUserSavedSearch savedSearch) throws AtlasBaseException, IOException {
        validateUserSavedSearch(savedSearch);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.addSavedSearch(userName=" + savedSearch.getOwnerName() + ", name=" + savedSearch.getName() + ", searchType=" + savedSearch.getSearchType() + ")");
            }

            return atlasDiscoveryService.addSavedSearch(Servlets.getUserName(httpServletRequest), savedSearch);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /***
     *
     * @param savedSearch
     * @return the updated search-object
     * @throws AtlasBaseException
     */
    @PUT
    @Path("saved")
    public AtlasUserSavedSearch updateSavedSearch(AtlasUserSavedSearch savedSearch) throws AtlasBaseException {
        validateUserSavedSearch(savedSearch);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.updateSavedSearch(userName=" + savedSearch.getOwnerName() + ", name=" + savedSearch.getName() + ", searchType=" + savedSearch.getSearchType() + ")");
            }

            return atlasDiscoveryService.updateSavedSearch(Servlets.getUserName(httpServletRequest), savedSearch);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     *
     * @param searchName Name of the saved search
     * @param userName User for whom the search is retrieved
     * @return
     * @throws AtlasBaseException
     */
    @GET
    @Path("saved/{name}")
    public AtlasUserSavedSearch getSavedSearch(@PathParam("name") String searchName,
                                               @QueryParam("user") String userName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", searchName);
        Servlets.validateQueryParamLength("user", userName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.getSavedSearch(userName=" + userName + ", name=" + searchName + ")");
            }

            return atlasDiscoveryService.getSavedSearchByName(Servlets.getUserName(httpServletRequest), userName, searchName);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     *
     * @param userName User for whom the search is retrieved
     * @throws AtlasBaseException
     * @return list of all saved searches for given user
     */
    @GET
    @Path("saved")
    public List<AtlasUserSavedSearch> getSavedSearches(@QueryParam("user") String userName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("user", userName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.getSavedSearches(userName=" + userName + ")");
            }

            return atlasDiscoveryService.getSavedSearches(Servlets.getUserName(httpServletRequest), userName);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * @param guid Name of the saved search
     */
    @DELETE
    @Path("saved/{guid}")
    public void deleteSavedSearch(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.deleteSavedSearch(guid=" + guid + ")");
            }

            atlasDiscoveryService.deleteSavedSearch(Servlets.getUserName(httpServletRequest), guid);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }


    /**
     * Attribute based search for entities satisfying the search parameters
     *
     * @param searchName name of saved-search
     * @param userName saved-search owner
     * @return Atlas search result
     * @throws AtlasBaseException
     */
    @Path("saved/execute/{name}")
    @GET
    public AtlasSearchResult executeSavedSearchByName(@PathParam("name") String searchName,
                                                      @QueryParam("user") String userName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", searchName);
        Servlets.validateQueryParamLength("user", userName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG,
                        "DiscoveryREST.executeSavedSearchByName(userName=" + userName + ", " + "name=" + searchName + ")");
            }

            AtlasUserSavedSearch savedSearch = atlasDiscoveryService.getSavedSearchByName(Servlets.getUserName(httpServletRequest), userName, searchName);

            return executeSavedSearch(savedSearch);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Attribute based search for entities satisfying the search parameters
     *
     * @param searchGuid Guid identifying saved search
     * @return Atlas search result
     * @throws AtlasBaseException
     */
    @Path("saved/execute/guid/{guid}")
    @GET
    public AtlasSearchResult executeSavedSearchByGuid(@PathParam("guid") String searchGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", searchGuid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.executeSavedSearchByGuid(" + searchGuid + ")");
            }

            AtlasUserSavedSearch savedSearch = atlasDiscoveryService.getSavedSearchByGuid(Servlets.getUserName(httpServletRequest), searchGuid);

            return executeSavedSearch(savedSearch);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    private boolean isEmpty(SearchParameters.FilterCriteria filterCriteria) {
        return filterCriteria == null ||
                (StringUtils.isEmpty(filterCriteria.getAttributeName()) && CollectionUtils.isEmpty(filterCriteria.getCriterion()));
    }

    private AtlasSearchResult executeSavedSearch(AtlasUserSavedSearch savedSearch) throws AtlasBaseException {
        SearchParameters sp = savedSearch.getSearchParameters();

        if(savedSearch.getSearchType() == AtlasUserSavedSearch.SavedSearchType.ADVANCED) {
            String dslQuery = atlasDiscoveryService.getDslQueryUsingTypeNameClassification(sp.getQuery(), sp.getTypeName(), sp.getClassification());

            return atlasDiscoveryService.searchUsingDslQuery(dslQuery, sp.getLimit(), sp.getOffset());
        } else {
            return atlasDiscoveryService.searchWithParameters(sp);
        }
    }

    private void validateUserSavedSearch(AtlasUserSavedSearch savedSearch) throws AtlasBaseException {
        if (savedSearch != null) {
            Servlets.validateQueryParamLength("name", savedSearch.getName());
            Servlets.validateQueryParamLength("ownerName", savedSearch.getOwnerName());
            Servlets.validateQueryParamLength("guid", savedSearch.getGuid());

            validateSearchParameters(savedSearch.getSearchParameters());
        }
    }

    private void validateSearchParameters(SearchParameters parameters) throws AtlasBaseException {
        if (parameters != null) {
            Servlets.validateQueryParamLength("typeName", parameters.getTypeName());
            Servlets.validateQueryParamLength("classification", parameters.getClassification());
            if (StringUtils.isNotEmpty(parameters.getQuery()) && parameters.getQuery().length() > maxFullTextQueryLength) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
            }

        }
    }
}
