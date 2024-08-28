package org.apache.atlas.web.rest;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.searchlog.SearchLoggingManagement;
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
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.util.*;

import static org.apache.commons.collections.MapUtils.getMap;

@Path("model")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class ModelREST {

    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.DiscoveryREST");
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryREST.class);
    @Context
    private HttpServletRequest httpServletRequest;
    private final int                maxFullTextQueryLength;
    private final int                maxDslQueryLength;
    private final boolean            enableSearchLogging;

    private final AtlasTypeRegistry typeRegistry;
    private final AtlasDiscoveryService discoveryService;
    private final SearchLoggingManagement loggerManagement;

    private static final String INDEXSEARCH_TAG_NAME = "indexsearch";
    private static final Set<String> TRACKING_UTM_TAGS = new HashSet<>(Arrays.asList("ui_main_list", "ui_popup_searchbar"));
    private static final String UTM_TAG_FROM_PRODUCT = "project_webapp";

    @Inject
    public ModelREST(AtlasTypeRegistry typeRegistry, AtlasDiscoveryService discoveryService,
                         SearchLoggingManagement loggerManagement, Configuration configuration) {
        this.typeRegistry           = typeRegistry;
        this.discoveryService       = discoveryService;
        this.loggerManagement       = loggerManagement;
        this.maxFullTextQueryLength = configuration.getInt(Constants.MAX_FULLTEXT_QUERY_STR_LENGTH, 4096);
        this.maxDslQueryLength      = configuration.getInt(Constants.MAX_DSL_QUERY_STR_LENGTH, 4096);
        this.enableSearchLogging    = AtlasConfiguration.ENABLE_SEARCH_LOGGER.getBoolean();
    }

    @Path("/namespace/{namespace}/businessDate/{businessDate}")
    @POST
    @Timed
    public AtlasSearchResult dataSearch(@PathParam("namespace") String namespace, @PathParam("businessDate") Date businessDate,
                                        @Context HttpServletRequest servletRequest, IndexSearchParams parameters) throws AtlasBaseException {

        Servlets.validateQueryParamLength("namespace", namespace);
        Servlets.validateQueryParamLength("businessDate", String.valueOf(businessDate));
        AtlasPerfTracer perf = null;
        long startTime = System.currentTimeMillis();

        RequestContext.get().setIncludeMeanings(!parameters.isExcludeMeanings());
        RequestContext.get().setIncludeClassifications(!parameters.isExcludeClassifications());
        RequestContext.get().setIncludeClassificationNames(parameters.isIncludeClassificationNames());
        try     {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ModelREST.dataSearch(" + parameters + ")");
            }

            if (StringUtils.isEmpty(parameters.getQuery())) {
                AtlasBaseException abe = new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid search query");
                if (enableSearchLogging && parameters.isSaveSearchLog()) {
                    //logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
                }
                throw abe;
            }

            if(LOG.isDebugEnabled()){
                LOG.debug("Performing indexsearch for the params ({})", parameters);
            }

            // retrieve query
            // compose dsl

            AtlasSearchResult result = discoveryService.directIndexSearch(parameters);
            if (result == null) {
                return null;
            }
            long endTime = System.currentTimeMillis();

            if (enableSearchLogging && parameters.isSaveSearchLog()) {
               // logSearchLog(parameters, result, servletRequest, endTime - startTime);
            }

            return result;
        } catch (AtlasBaseException abe) {
            if (enableSearchLogging && parameters.isSaveSearchLog()) {
               // logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            throw abe;
        } catch (Exception e) {
            AtlasBaseException abe = new AtlasBaseException(e.getMessage(), e.getCause());
            if (enableSearchLogging && parameters.isSaveSearchLog()) {
                //logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
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

//    private void addBusinessFiltersToSearchQuery(final String namespace, final Date businessDate, SearchParams searchParams){
//        try {
//            AtlasPerfMetrics.MetricRecorder addBusinessFiltersToSearchQueryMetric = RequestContext.get().startMetricRecord("addPreFiltersToSearchQuery");
//            ObjectMapper mapper = new ObjectMapper();
//            List<Map<String, Object>> mustClauseList = new ArrayList<>();
//            Map<String, Object> allPreFiltersBoolClause = NewAuthorizerUtils.getPreFilterDsl(persona, purpose, actions);
//            mustClauseList.add(allPreFiltersBoolClause);
//
//            mustClauseList.add((Map<String, Object>) ((IndexSearchParams) searchParams).getDsl().get("query"));
//
//            String dslString = searchParams.getQuery();
//            JsonNode node = mapper.readTree(dslString);
//            /*JsonNode userQueryNode = node.get("query");
//            if (userQueryNode != null) {
//
//                String userQueryString = userQueryNode.toString();
//
//                String userQueryBase64 = Base64.getEncoder().encodeToString(userQueryString.getBytes());
//                mustClauseList.add(getMap("wrapper", getMap("query", userQueryBase64)));
//            }*/
//
//            JsonNode updateQueryNode = mapper.valueToTree(getMap("bool", getMap("must", mustClauseList)));
//
//            ((ObjectNode) node).set("query", updateQueryNode);
//            searchParams.setQuery(node.toString());
//
//
//            RequestContext.get().endMetricRecord(addBusinessFiltersToSearchQueryMetric);
//
//        }catch (Exception e){
//            LOG.error("Error -> addBusinessFiltersToSearchQuery!", e);
//        }
//    }

//    private void addPreFiltersToSearchQuery(SearchParams searchParams) {
//        try {
//            String persona = ((IndexSearchParams) searchParams).getPersona();
//            String purpose = ((IndexSearchParams) searchParams).getPurpose();
//
//            AtlasPerfMetrics.MetricRecorder addPreFiltersToSearchQueryMetric = RequestContext.get().startMetricRecord("addPreFiltersToSearchQuery");
//            ObjectMapper mapper = new ObjectMapper();
//            List<Map<String, Object>> mustClauseList = new ArrayList<>();
//
//            List<String> actions = new ArrayList<>();
//            actions.add("entity-read");
//
//            Map<String, Object> allPreFiltersBoolClause = NewAuthorizerUtils.getPreFilterDsl(persona, purpose, actions);
//            mustClauseList.add(allPreFiltersBoolClause);
//
//            mustClauseList.add((Map<String, Object>) ((IndexSearchParams) searchParams).getDsl().get("query"));
//
//            String dslString = searchParams.getQuery();
//            JsonNode node = mapper.readTree(dslString);
//            /*JsonNode userQueryNode = node.get("query");
//            if (userQueryNode != null) {
//
//                String userQueryString = userQueryNode.toString();
//
//                String userQueryBase64 = Base64.getEncoder().encodeToString(userQueryString.getBytes());
//                mustClauseList.add(getMap("wrapper", getMap("query", userQueryBase64)));
//            }*/
//
//            JsonNode updateQueryNode = mapper.valueToTree(getMap("bool", getMap("must", mustClauseList)));
//
//            ((ObjectNode) node).set("query", updateQueryNode);
//            searchParams.setQuery(node.toString());
//
//            RequestContext.get().endMetricRecord(addPreFiltersToSearchQueryMetric);
//
//        } catch (Exception e) {
//            LOG.error("Error -> addPreFiltersToSearchQuery!", e);
//        }
//    }

}
