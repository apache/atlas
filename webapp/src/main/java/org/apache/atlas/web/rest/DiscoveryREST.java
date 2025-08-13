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

import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.SortOrder;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
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

import static org.apache.atlas.repository.Constants.NAME;
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
        RequestContext.get().setIsInvokedByIndexSearch(true);
        long startTime = System.currentTimeMillis();

        try     {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "DiscoveryREST.indexSearch(" + parameters + ")");
            }

            if (AtlasConfiguration.ATLAS_INDEXSEARCH_ENABLE_API_LIMIT.getBoolean() && parameters.getQuerySize() > AtlasConfiguration.ATLAS_INDEXSEARCH_QUERY_SIZE_MAX_LIMIT.getLong()) {
                if(CollectionUtils.isEmpty(parameters.getUtmTags())) {
                    throw new AtlasBaseException(AtlasErrorCode.INVALID_DSL_QUERY_SIZE, String.valueOf(AtlasConfiguration.ATLAS_INDEXSEARCH_QUERY_SIZE_MAX_LIMIT.getLong()));
                }
                for (String utmTag : parameters.getUtmTags()) {
                    if (Arrays.stream(AtlasConfiguration.ATLAS_INDEXSEARCH_LIMIT_UTM_TAGS.getStringArray()).anyMatch(utmTag::equalsIgnoreCase)) {
                            throw new AtlasBaseException(AtlasErrorCode.INVALID_DSL_QUERY_SIZE, String.valueOf(AtlasConfiguration.ATLAS_INDEXSEARCH_QUERY_SIZE_MAX_LIMIT.getLong()));
                    }
                }
            }

            if (StringUtils.isEmpty(parameters.getQuery())) {
                AtlasBaseException abe = new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid search query");
                if (enableSearchLogging && parameters.isSaveSearchLog() && !shouldSkipSearchLog(parameters)) {
                    logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
                }
                throw abe;
            }

            if(LOG.isDebugEnabled()){
                LOG.debug("Performing indexsearch for the params ({})", parameters);
            }
            AtlasSearchResult result = discoveryService.directIndexSearch(parameters, true);
            if (result == null) {
                return null;
            }
            long endTime = System.currentTimeMillis();

            if (enableSearchLogging && parameters.isSaveSearchLog() && !shouldSkipSearchLog(parameters)) {
                logSearchLog(parameters, result, servletRequest, endTime - startTime);
            }

            return result;
        } catch (AtlasBaseException abe) {
            if (enableSearchLogging && parameters.isSaveSearchLog() && !shouldSkipSearchLog(parameters)) {
                logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            throw abe;
        } catch (Exception e) {
            AtlasBaseException abe = new AtlasBaseException(e.getMessage(), e.getCause());
            if (enableSearchLogging && parameters.isSaveSearchLog() && !shouldSkipSearchLog(parameters)) {
                logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            abe.setStackTrace(e.getStackTrace());
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

    private boolean shouldSkipSearchLog(IndexSearchParams parameters) {

        // Skip if utmTags contain specific landing page patterns
        Set<String> utmTags = parameters.getUtmTags();
        if (CollectionUtils.isNotEmpty(utmTags)) {
            // Pattern 1: Bootstrap/landing page load
            if (utmTags.contains("project_webapp") &&
                    utmTags.contains("action_bootstrap") &&
                    utmTags.contains("action_fetch_starred_assets")) {
                return true;
            }

            // Pattern 2: Page loading
            if (utmTags.contains("project_webapp") &&
                    utmTags.contains("action_searched_on_load")) {
                return true;
            }

            // Pattern 3: Assets page with search input empty
            if (utmTags.contains("project_webapp") &&
                    utmTags.contains("page_assets") &&
                    utmTags.contains("action_searched")) {
                return StringUtils.isEmpty(parameters.getSearchInput());
            }
        }

        return false;
    }
}
