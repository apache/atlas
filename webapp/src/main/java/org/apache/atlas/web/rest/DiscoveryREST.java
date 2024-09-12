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
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.SortOrder;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.*;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.profile.AtlasUserSavedSearch;
import org.apache.atlas.model.searchlog.SearchLogSearchParams;
import org.apache.atlas.model.searchlog.SearchLogSearchResult;
import org.apache.atlas.model.searchlog.SearchRequestLogData.SearchRequestLogDataBuilder;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.searchlog.SearchLoggingManagement;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Arrays;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.Constants.REQUEST_HEADER_HOST;
import static org.apache.atlas.repository.Constants.REQUEST_HEADER_USER_AGENT;

/**
 * REST interface for data discovery using dsl or full text search
 */
@Path("search")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class DiscoveryREST {
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.DiscoveryREST");
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryREST.class);
    @Context
    private       HttpServletRequest httpServletRequest;
    private final int                maxFullTextQueryLength;
    private final int                maxDslQueryLength;
    private final boolean            enableSearchLogging;

    private final AtlasTypeRegistry     typeRegistry;
    private final AtlasDiscoveryService discoveryService;
    private final SearchLoggingManagement loggerManagement;

    private static final String INDEXSEARCH_TAG_NAME = "indexsearch";
    private static final String RELATIONSHIP_INDEXSEARCH_TAG_NAME = "relationshipIndexsearch";
    private static final Set<String> TRACKING_UTM_TAGS = new HashSet<>(Arrays.asList("ui_main_list", "ui_popup_searchbar"));
    private static final String UTM_TAG_FROM_PRODUCT = "project_webapp";

    @Inject
    public DiscoveryREST(AtlasTypeRegistry typeRegistry, AtlasDiscoveryService discoveryService,
                         SearchLoggingManagement loggerManagement, Configuration configuration) {
        this.typeRegistry           = typeRegistry;
        this.discoveryService       = discoveryService;
        this.loggerManagement       = loggerManagement;
        this.maxFullTextQueryLength = configuration.getInt(Constants.MAX_FULLTEXT_QUERY_STR_LENGTH, 4096);
        this.maxDslQueryLength      = configuration.getInt(Constants.MAX_DSL_QUERY_STR_LENGTH, 4096);
        this.enableSearchLogging    = AtlasConfiguration.ENABLE_SEARCH_LOGGER.getBoolean();
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
    @Timed
    public AtlasSearchResult searchUsingDSL(@QueryParam("query")          String query,
                                            @QueryParam("typeName")       String typeName,
                                            @QueryParam("classification") String classification,
                                            @QueryParam("limit")          int    limit,
                                            @QueryParam("offset")         int    offset) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        Servlets.validateQueryParamLength("classification", classification);

        if (StringUtils.isNotEmpty(query)) {
            if (query.length() > maxDslQueryLength) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_DSL_QUERY_STR_LENGTH);
            }
            query = Servlets.decodeQueryString(query);
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchUsingDSL(" + query + "," + typeName
                        + "," + classification + "," + limit + "," + offset + ")");
            }

            String queryStr = discoveryService.getDslQueryUsingTypeNameClassification(query, typeName, classification);

            return discoveryService.searchUsingDslQuery(queryStr, limit, offset);
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
    @Timed
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

            return discoveryService.searchUsingFullTextQuery(query, excludeDeletedEntities, limit, offset);
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
    @Timed
    public AtlasSearchResult searchUsingBasic(@QueryParam("query")                  String  query,
                                              @QueryParam("typeName")               String  typeName,
                                              @QueryParam("classification")         String  classification,
                                              @QueryParam("sortBy")                 String    sortByAttribute,
                                              @QueryParam("sortOrder")              SortOrder sortOrder,
                                              @QueryParam("excludeDeletedEntities") boolean excludeDeletedEntities,
                                              @QueryParam("limit")                  int     limit,
                                              @QueryParam("offset")                 int     offset,
                                              @QueryParam("marker")                 String  marker) throws AtlasBaseException {
        Servlets.validateQueryParamLength("typeName", typeName);
        Servlets.validateQueryParamLength("classification", classification);
        Servlets.validateQueryParamLength("sortBy", sortByAttribute);
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
            searchParameters.setMarker(marker);
            searchParameters.setSortBy(sortByAttribute);
            searchParameters.setSortOrder(sortOrder);

            return discoveryService.searchWithParameters(searchParameters);
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
    @Timed
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
    @Timed
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

            if (StringUtils.isEmpty(parameters.getTypeName()) && StringUtils.isEmpty(parameters.getClassification()) &&
                StringUtils.isEmpty(parameters.getQuery()) && StringUtils.isEmpty(parameters.getTermName())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_SEARCH_PARAMS);
            }

            validateSearchParameters(parameters);

            return discoveryService.searchWithParameters(parameters);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Index based search for query direct on Elasticsearch
     *
     * @param parameters Index Search parameters @IndexSearchParams.java
     * @return Atlas search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     */
    @Path("indexsearch")
    @POST
    @Timed
    public AtlasSearchResult indexSearch(@Context HttpServletRequest servletRequest, IndexSearchParams parameters) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        long startTime = System.currentTimeMillis();

        RequestContext.get().setIncludeMeanings(!parameters.isExcludeMeanings());
        RequestContext.get().setIncludeClassifications(!parameters.isExcludeClassifications());
        RequestContext.get().setIncludeClassificationNames(parameters.isIncludeClassificationNames());
        try     {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.indexSearch(" + parameters + ")");
            }

            if (StringUtils.isEmpty(parameters.getQuery())) {
                AtlasBaseException abe = new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid search query");
                if (enableSearchLogging && parameters.isSaveSearchLog()) {
                    logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
                }
                throw abe;
            }

            if(LOG.isDebugEnabled()){
                LOG.debug("Performing indexsearch for the params ({})", parameters);
            }
            AtlasSearchResult result = discoveryService.directIndexSearch(parameters);
            if (result == null) {
                return null;
            }
            long endTime = System.currentTimeMillis();

            if (enableSearchLogging && parameters.isSaveSearchLog()) {
                logSearchLog(parameters, result, servletRequest, endTime - startTime);
            }

            return result;
        } catch (AtlasBaseException abe) {
            if (enableSearchLogging && parameters.isSaveSearchLog()) {
                logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            throw abe;
        } catch (Exception e) {
            AtlasBaseException abe = new AtlasBaseException(e.getMessage(), e.getCause());
            if (enableSearchLogging && parameters.isSaveSearchLog()) {
                logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            throw abe;
        } finally {
            if(CollectionUtils.isNotEmpty(parameters.getUtmTags())) {
                AtlasPerfMetrics.Metric indexsearchMetric = new AtlasPerfMetrics.Metric(INDEXSEARCH_TAG_NAME);
                indexsearchMetric.addTag("utmTag", "other");
                indexsearchMetric.addTag("source", "other");
                for (String utmTag : parameters.getUtmTags()) {
                    if (TRACKING_UTM_TAGS.contains(utmTag)) {
                        indexsearchMetric.addTag("utmTag", utmTag);
                        break;
                    }
                }
                if (parameters.getUtmTags().contains(UTM_TAG_FROM_PRODUCT)) {
                    indexsearchMetric.addTag("source", UTM_TAG_FROM_PRODUCT);
                }
                indexsearchMetric.addTag("name", INDEXSEARCH_TAG_NAME);
                indexsearchMetric.setTotalTimeMSecs(System.currentTimeMillis() - startTime);
                RequestContext.get().addApplicationMetrics(indexsearchMetric);
            }
            AtlasPerfTracer.log(perf);
        }
    }


    /**
     * Index based search for query direct on Elasticsearch Edge index
     *
     * @param parameters Index Search parameters @IndexSearchParams.java
     * @return Atlas search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     */
    @Path("/relationship/indexsearch")
    @POST
    @Timed
    public AtlasSearchResult relationshipIndexSearch(@Context HttpServletRequest servletRequest, IndexSearchParams parameters) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        
        try     {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.relationshipIndexSearch(" + parameters + ")");
            }

            if (StringUtils.isEmpty(parameters.getQuery())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid search query");
            }

            if(LOG.isDebugEnabled()){
                LOG.debug("Performing relationship indexsearch for the params ({})", parameters);
            }
            return discoveryService.directRelationshipIndexSearch(parameters);

        } catch (AtlasBaseException abe) {
            throw abe;
        } catch (Exception e) {
            throw new AtlasBaseException(e.getMessage(), e.getCause());
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Index based search for query direct on ES search-logs index
     *
     * @param parameters Index Search parameters @SearchLogSearchParams.java
     * @return search log search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     */
    @Path("searchlog")
    @POST
    @Timed
    public SearchLogSearchResult searchLogs(SearchLogSearchParams parameters) throws AtlasBaseException {
        AtlasPerfTracer perf = null;
        SearchLogSearchResult result;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchLogs(" + parameters + ")");
            }

            result = discoveryService.searchLogs(parameters);

        } catch (Exception e) {
            throw e;
        } finally {
            AtlasPerfTracer.log(perf);
        }

        return result;
    }

    /**
     * Relationship search to search for related entities satisfying the search parameters
     *
     * @param guid            Attribute name
     * @param relation        relationName
     * @param attributes      set of attributes in search result.
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
    @Timed
    public AtlasSearchResult searchRelatedEntities(@QueryParam("guid")                            String      guid,
                                                   @QueryParam("relation")                        String      relation,
                                                   @QueryParam("attributes")                      Set<String> attributes,
                                                   @QueryParam("sortBy")                          String      sortByAttribute,
                                                   @QueryParam("sortOrder")                       SortOrder   sortOrder,
                                                   @QueryParam("excludeDeletedEntities")          boolean     excludeDeletedEntities,
                                                   @QueryParam("includeClassificationAttributes") boolean     includeClassificationAttributes,
                                                   @QueryParam("excludeMeaningsAttributes")       boolean     excludeMeaningsAttributes,
                                                   @QueryParam("getApproximateCount")             boolean     getApproximateCount,
                                                   @QueryParam("limit")                           int         limit,
                                                   @QueryParam("offset")                          int         offset) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);
        Servlets.validateQueryParamLength("relation", relation);
        Servlets.validateQueryParamLength("sortBy", sortByAttribute);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.relatedEntitiesSearch(" + guid +
                        ", " + relation + ", " + sortByAttribute + ", " + sortOrder + ", " + excludeDeletedEntities + ", " + getApproximateCount + ", " + limit + ", " + offset + ")");
            }

            SearchParameters parameters = new SearchParameters();
            parameters.setAttributes(attributes);
            parameters.setSortBy(sortByAttribute);
            parameters.setSortOrder(sortOrder);
            parameters.setExcludeDeletedEntities(excludeDeletedEntities);
            parameters.setLimit(limit);
            parameters.setOffset(offset);
            parameters.setIncludeClassificationAttributes(includeClassificationAttributes);
            parameters.setExcludeClassifications(!includeClassificationAttributes);
            parameters.setExcludeMeanings(excludeMeaningsAttributes);
            RequestContext.get().setIncludeClassifications(includeClassificationAttributes);
            RequestContext.get().setIncludeMeanings(!excludeMeaningsAttributes);
            return discoveryService.searchRelatedEntities(guid, relation, getApproximateCount, parameters);

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
    @Timed
    public AtlasUserSavedSearch addSavedSearch(AtlasUserSavedSearch savedSearch) throws AtlasBaseException, IOException {
        validateUserSavedSearch(savedSearch);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.addSavedSearch(userName=" + savedSearch.getOwnerName() + ", name=" + savedSearch.getName() + ", searchType=" + savedSearch.getSearchType() + ")");
            }

            return discoveryService.addSavedSearch(AtlasAuthorizationUtils.getCurrentUserName(), savedSearch);
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
    @Timed
    public AtlasUserSavedSearch updateSavedSearch(AtlasUserSavedSearch savedSearch) throws AtlasBaseException {
        validateUserSavedSearch(savedSearch);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.updateSavedSearch(userName=" + savedSearch.getOwnerName() + ", name=" + savedSearch.getName() + ", searchType=" + savedSearch.getSearchType() + ")");
            }

            return discoveryService.updateSavedSearch(AtlasAuthorizationUtils.getCurrentUserName(), savedSearch);
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
    @Timed
    public AtlasUserSavedSearch getSavedSearch(@PathParam("name") String searchName,
                                               @QueryParam("user") String userName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("name", searchName);
        Servlets.validateQueryParamLength("user", userName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.getSavedSearch(userName=" + userName + ", name=" + searchName + ")");
            }

            return discoveryService.getSavedSearchByName(AtlasAuthorizationUtils.getCurrentUserName(), userName, searchName);
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
    @Timed
    public List<AtlasUserSavedSearch> getSavedSearches(@QueryParam("user") String userName) throws AtlasBaseException {
        Servlets.validateQueryParamLength("user", userName);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.getSavedSearches(userName=" + userName + ")");
            }

            return discoveryService.getSavedSearches(AtlasAuthorizationUtils.getCurrentUserName(), userName);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * @param guid Name of the saved search
     */
    @DELETE
    @Path("saved/{guid}")
    @Timed
    public void deleteSavedSearch(@PathParam("guid") String guid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", guid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.deleteSavedSearch(guid=" + guid + ")");
            }

            discoveryService.deleteSavedSearch(AtlasAuthorizationUtils.getCurrentUserName(), guid);
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
    @Timed
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

            AtlasUserSavedSearch savedSearch = discoveryService.getSavedSearchByName(AtlasAuthorizationUtils.getCurrentUserName(), userName, searchName);

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
    @Timed
    public AtlasSearchResult executeSavedSearchByGuid(@PathParam("guid") String searchGuid) throws AtlasBaseException {
        Servlets.validateQueryParamLength("guid", searchGuid);

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.executeSavedSearchByGuid(" + searchGuid + ")");
            }

            AtlasUserSavedSearch savedSearch = discoveryService.getSavedSearchByGuid(AtlasAuthorizationUtils.getCurrentUserName(), searchGuid);

            return executeSavedSearch(savedSearch);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Attribute based search for entities satisfying the search parameters
     *@return Atlas search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     * @HTTP 400 Tag/Entity doesn't exist or Tag/entity filter is present without tag/type name
     */
    @Path("/quick")
    @GET
    @Timed
    public AtlasQuickSearchResult quickSearch(@QueryParam("query")                  String    query,
                                              @QueryParam("typeName")               String    typeName,
                                              @QueryParam("excludeDeletedEntities") boolean   excludeDeletedEntities,
                                              @QueryParam("offset")                 int       offset,
                                              @QueryParam("limit")                  int       limit,
                                              @QueryParam("sortBy")                 String    sortByAttribute,
                                              @QueryParam("sortOrder")              SortOrder sortOrder) throws AtlasBaseException {



        if (StringUtils.isNotEmpty(query) && query.length() > maxFullTextQueryLength) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
        }

        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.quick(" + query + "," +
                        "excludeDeletedEntities:" + excludeDeletedEntities + "," + limit + "," + offset + ")");
            }

            QuickSearchParameters quickSearchParameters = new QuickSearchParameters(query,
                                                                                    typeName,
                                                                                    null,  // entityFilters
                                                                                    false, // includeSubTypes
                                                                                    excludeDeletedEntities,
                                                                                    offset,
                                                                                    limit,
                                                                                    null, // attributes
                                                                                    sortByAttribute,
                                                                                    sortOrder);

            return discoveryService.quickSearch(quickSearchParameters);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    /**
     * Attribute based search for entities satisfying the search parameters
     *@return Atlas search result
     * @throws AtlasBaseException
     * @HTTP 200 On successful search
     * @HTTP 400 Entity/attribute doesn't exist or entity filter is present without type name
     */
    @Path("/quick")
    @POST
    @Timed
    public AtlasQuickSearchResult quickSearch(QuickSearchParameters quickSearchParameters) throws AtlasBaseException {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.searchWithParameters(" + quickSearchParameters + ")");
            }

            if (quickSearchParameters.getLimit() < 0 || quickSearchParameters.getOffset() < 0) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Limit/offset should be non-negative");
            }

            if (StringUtils.isEmpty(quickSearchParameters.getTypeName()) &&
                !isEmpty(quickSearchParameters.getEntityFilters())) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "EntityFilters specified without Type name");
            }

            if (StringUtils.isEmpty(quickSearchParameters.getTypeName()) &&
                StringUtils.isEmpty(quickSearchParameters.getQuery())){
                throw new AtlasBaseException(AtlasErrorCode.INVALID_SEARCH_PARAMS);
            }

            if (StringUtils.isEmpty(quickSearchParameters.getTypeName()) &&
                    (StringUtils.isNotEmpty(quickSearchParameters.getSortBy()))) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "SortBy specified without Type name");
            }

            validateSearchParameters(quickSearchParameters);

            return discoveryService.quickSearch(quickSearchParameters);
        } finally {
            AtlasPerfTracer.log(perf);
        }
    }

    @Path("suggestions")
    @GET
    @Timed
    public AtlasSuggestionsResult getSuggestions(@QueryParam("prefixString") String prefixString, @QueryParam("fieldName") String fieldName) {
        AtlasPerfTracer perf = null;

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.getSuggestions(" + prefixString + "," + fieldName + ")");
            }

            return discoveryService.getSuggestions(prefixString, fieldName);
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
            String dslQuery = discoveryService.getDslQueryUsingTypeNameClassification(sp.getQuery(), sp.getTypeName(), sp.getClassification());

            return discoveryService.searchUsingDslQuery(dslQuery, sp.getLimit(), sp.getOffset());
        } else {
            return discoveryService.searchWithParameters(sp);
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
            Servlets.validateQueryParamLength("sortBy", parameters.getSortBy());
            if (StringUtils.isNotEmpty(parameters.getQuery()) && parameters.getQuery().length() > maxFullTextQueryLength) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_QUERY_LENGTH, Constants.MAX_FULLTEXT_QUERY_STR_LENGTH);
            }

        }
    }

    private void validateSearchParameters(QuickSearchParameters parameters) throws AtlasBaseException {
        if (parameters != null) {
            validateSearchParameters(EntityDiscoveryService.createSearchParameters(parameters));
        }
    }

    private void logSearchLog(IndexSearchParams parameters, AtlasSearchResult result,
                              HttpServletRequest servletRequest, long requestTime) {
        SearchRequestLogDataBuilder builder = new SearchRequestLogDataBuilder();
        builder.setHasResult(false);

        if (CollectionUtils.isNotEmpty(result.getEntities())) {
            builder.setHasResult(true);
            builder.setResultsCount(result.getApproximateCount());

            Set<String> entityGuidsAll = new HashSet<>();
            Set<String> entityQFNamesAll = new HashSet<>();
            Set<String> entityGuidsAllowed = new HashSet<>();
            Set<String> entityQFNamesAllowed = new HashSet<>();
            Set<String> entityGuidsDenied = new HashSet<>();
            Set<String> entityQFNamesDenied = new HashSet<>();
            Set<String> entityTypesAll = new HashSet<>();
            Set<String> entityTypesAllowed = new HashSet<>();
            Set<String> entityTypesDenied = new HashSet<>();

            result.getEntities().forEach(x -> {
                boolean allowed = x.getScrubbed() == null;

                String guid = x.getGuid();

                if (guid != null) {
                    entityGuidsAll.add(guid);
                    entityTypesAll.add(x.getTypeName());

                    if (allowed) {
                        entityGuidsAllowed.add(guid);
                        entityTypesAllowed.add(x.getTypeName());
                    } else {
                        entityGuidsDenied.add(guid);
                        entityTypesDenied.add(x.getTypeName());
                    }
                }

                try {
                    String qualifiedName = (String) x.getAttribute(QUALIFIED_NAME);

                    if (qualifiedName != null) {

                        entityQFNamesAll.add(qualifiedName);
                        if (allowed) {
                            entityQFNamesAllowed.add(qualifiedName);
                        } else {
                            entityQFNamesDenied.add(qualifiedName);
                        }
                    }
                } catch (NullPointerException npe) {
                    //no attributes for entity
                }
            });

            builder.setEntityGuidsAll(entityGuidsAll)
                    .setEntityQFNamesAll(entityQFNamesAll)
                    .setEntityGuidsAllowed(entityGuidsAllowed)
                    .setEntityQFNamesAllowed(entityQFNamesAllowed)
                    .setEntityGuidsDenied(entityGuidsDenied)
                    .setEntityQFNamesDenied(entityQFNamesDenied)
                    .setEntityTypeNamesAll(entityTypesAll)
                    .setEntityTypeNamesAllowed(entityTypesAllowed)
                    .setEntityTypeNamesDenied(entityTypesDenied);

        }

        logSearchLog(parameters, servletRequest, builder, requestTime);
    }

    private void logSearchLog(IndexSearchParams parameters, HttpServletRequest servletRequest,
                              AtlasBaseException e, long requestTime) {
        SearchRequestLogDataBuilder builder = new SearchRequestLogDataBuilder();

        builder.setErrorDetails(e.getMessage())
                .setErrorCode(e.getAtlasErrorCode().getErrorCode())
                .setFailed(true);


        logSearchLog(parameters, servletRequest, builder, requestTime);
    }

    private void logSearchLog(IndexSearchParams parameters, HttpServletRequest servletRequest,
                              SearchRequestLogDataBuilder builder, long requestTime) {

        String persona = StringUtils.isNotEmpty(parameters.getPersona()) ? parameters.getPersona() : parameters.getRequestMetadataPersona();
        if (StringUtils.isNotEmpty(persona)) {
            builder.setPersona(persona);
        } else {
            builder.setPurpose(parameters.getPurpose());
        }

        builder.setDsl(parameters.getDsl())
                .setSearchInput(parameters.getSearchInput())
                .setUtmTags(parameters.getUtmTags())
                .setAttributes(parameters.getAttributes())
                .setRelationAttributes(parameters.getRelationAttributes())

                .setUserAgent(servletRequest.getHeader(REQUEST_HEADER_USER_AGENT))
                .setHost(servletRequest.getHeader(REQUEST_HEADER_HOST))

                .setIpAddress(AtlasAuthorizationUtils.getRequestIpAddress(servletRequest))
                .setUserName(AtlasAuthorizationUtils.getCurrentUserName())

                .setTimestamp(RequestContext.get().getRequestTime())
                .setResponseTime(requestTime);

        loggerManagement.log(builder.build());
    }
}
