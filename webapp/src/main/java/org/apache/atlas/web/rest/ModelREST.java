package org.apache.atlas.web.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.annotation.Timed;
import org.apache.atlas.discovery.AtlasDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.searchlog.SearchLoggingManagement;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.atlas.web.util.Servlets;
import org.apache.commons.collections.CollectionUtils;
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
import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.Set;

@Path("model")
@Singleton
@Service
@Consumes({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
@Produces({Servlets.JSON_MEDIA_TYPE, MediaType.APPLICATION_JSON})
public class ModelREST {

    private static final String BUSINESS_DATE = "dMDataModelBusinessDate";
    private static final String EXPIRED_BUSINESS_DATE = "dMDataModelExpiredAtBusinessDate";
    private static final String LESSER_THAN_EQUAL_TO = "lte";
    private static final String SYSTEM_DATE = "dMDataModelSystemDate";
    private static final String EXPIRED_SYSTEM_DATE = "dMDataModelExpiredAtSystemDate";
    private static final String NAMESPACE = "dMDataModelNamespace";
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("rest.DiscoveryREST");
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryREST.class);

    @Context
    private HttpServletRequest httpServletRequest;
    private final boolean enableSearchLogging;

    private final AtlasTypeRegistry typeRegistry;
    private final AtlasDiscoveryService discoveryService;
    private final SearchLoggingManagement loggerManagement;

    private static final String INDEXSEARCH_TAG_NAME = "indexsearch";
    private static final Set<String> TRACKING_UTM_TAGS = new HashSet<>(Arrays.asList("ui_main_list", "ui_popup_searchbar"));
    private static final String UTM_TAG_FROM_PRODUCT = "project_webapp";

    @Inject
    public ModelREST(AtlasTypeRegistry typeRegistry, AtlasDiscoveryService discoveryService,
                     SearchLoggingManagement loggerManagement) {
        this.typeRegistry = typeRegistry;
        this.discoveryService = discoveryService;
        this.loggerManagement = loggerManagement;
        this.enableSearchLogging = AtlasConfiguration.ENABLE_SEARCH_LOGGER.getBoolean();
    }

    @Path("/search")
    @POST
    @Timed
    public AtlasSearchResult dataSearch(@QueryParam("namespace") String namespace,
                                        @QueryParam("businessDate") String businessDate,
                                        @QueryParam("systemDate") String systemDate,
                                        @Context HttpServletRequest servletRequest, IndexSearchParams parameters) throws AtlasBaseException {

        Servlets.validateQueryParamLength("namespace", namespace);
        Servlets.validateQueryParamLength("businessDate", businessDate);
        AtlasPerfTracer perf = null;
        long startTime = System.currentTimeMillis();

        try {
            if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
                perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "ModelREST.dataSearch(" + parameters + ")");
            }

            parameters = parameters == null ? new IndexSearchParams() : parameters;

            String queryStringUsingFiltersAndUserDSL = createQueryStringUsingFiltersAndUserDSL(namespace,
                    businessDate,
                    systemDate,
                    parameters.getQuery());

            if (StringUtils.isEmpty(queryStringUsingFiltersAndUserDSL)) {
                AtlasBaseException abe = new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "Invalid model search query");
                throw abe;
            }

            parameters.setQuery(queryStringUsingFiltersAndUserDSL);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Performing indexsearch for the params ({})", parameters);
            }

            AtlasSearchResult result = discoveryService.directIndexSearch(parameters);
            if (result == null) {
                return null;
            }


            return result;
        } catch (AtlasBaseException abe) {
            if (enableSearchLogging && parameters.isSaveSearchLog()
            ) {
                // logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            throw abe;
        } catch (Exception e) {
            AtlasBaseException abe = new AtlasBaseException(e.getMessage(), e.getCause());
            if (enableSearchLogging && parameters.isSaveSearchLog()
            ) {
                //logSearchLog(parameters, servletRequest, abe, System.currentTimeMillis() - startTime);
            }
            throw abe;
        } finally {
            if (CollectionUtils.isNotEmpty(parameters.getUtmTags())) {
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

    /***
     * combines user query/dsl along with business parameters
     *
     * creates query as following :
     * {"query":{"bool":{"must":[{"bool":{"filter":[{"match":{"namespace":"{namespace}"}},{"bool":{"must":[{"range":{"businessDate":{"lte":"businessDate"}}},{"bool":{"should":[{"range":{"expiredAtBusinessDate":{"gt":"{businessDate}"}}},{"bool":{"must_not":[{"exists":{"field":"expiredAtBusiness"}}]}}],"minimum_should_match":1}}]}}]}},{"wrapper":{"query":"user query"}}]}}}
     * @param namespace
     * @param businessDate
     * @param dslString
     * @return
     */
    private String createQueryStringUsingFiltersAndUserDSL(final String namespace,
                                                           final String businessDate,
                                                           final String systemDate,
                                                           final String dslString) {
        try {
            AtlasPerfMetrics.MetricRecorder addBusinessFiltersToSearchQueryMetric = RequestContext.get().startMetricRecord("createQueryStringUsingFiltersAndUserDSL");
            // Create an ObjectMapper instance
            ObjectMapper objectMapper = new ObjectMapper();

            // Create the root 'query' node
            ObjectNode rootNode = objectMapper.createObjectNode();
            ObjectNode queryNode = objectMapper.createObjectNode();
            ObjectNode boolNode = objectMapper.createObjectNode();
            ArrayNode mustArray = objectMapper.createArrayNode();

            // Create the first 'bool' object inside 'must'
            ObjectNode firstBoolNode = objectMapper.createObjectNode();
            ObjectNode filterBoolNode = objectMapper.createObjectNode();
            ArrayNode filterArray = objectMapper.createArrayNode();

            // Create 'match' object
            ObjectNode matchNode = objectMapper.createObjectNode();
            matchNode.put(NAMESPACE.concat(".keyword"), namespace);

            // Add 'match' object to filter
            ObjectNode matchWrapper = objectMapper.createObjectNode();
            matchWrapper.set("term", matchNode);
            filterArray.add(matchWrapper);

            // add 'businessDateValidation'
            ObjectNode businessDateWrapper = dateValidation(businessDate, true, objectMapper);
            filterArray.add(businessDateWrapper);

            // add 'systemDateValidation'
            if (!StringUtils.isEmpty(systemDate)) {
                ObjectNode systemDateWrapper = dateValidation(systemDate, false, objectMapper);
                filterArray.add(systemDateWrapper);
            }

            // Add filter to firstBool
            filterBoolNode.set("filter", filterArray);
            firstBoolNode.set("bool", filterBoolNode);

            // Add firstBool to must array
            mustArray.add(firstBoolNode);

            // process user query
            if (!StringUtils.isEmpty(dslString)) {
                JsonNode node = new ObjectMapper().readTree(dslString);
                JsonNode userQueryNode = node.get("query");
                ObjectNode wrapperNode = objectMapper.createObjectNode();
                String userQueryString = userQueryNode.toString();
                String userQueryBase64 = Base64.getEncoder().encodeToString(userQueryString.getBytes());
                wrapperNode.put("query", userQueryBase64);
                // Add wrapper to must array
                ObjectNode wrapperWrapper = objectMapper.createObjectNode();
                wrapperWrapper.set("wrapper", wrapperNode);
                mustArray.add(wrapperWrapper);
            }


            // Add must array to bool node
            boolNode.set("must", mustArray);

            // Add bool to query
            queryNode.set("bool", boolNode);

            rootNode.set("query", queryNode);

            // Print the JSON representation of the query
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
        } catch (Exception e) {
            LOG.error("Error -> createQueryStringUsingFiltersAndUserDSL!", e);
        }
        return "";
    }

    private ObjectNode dateValidation(final String date, final boolean isBusinessDate, ObjectMapper objectMapper) {

        String condition = LESSER_THAN_EQUAL_TO, dateType = BUSINESS_DATE, expiredDateType = EXPIRED_BUSINESS_DATE;

        if (!isBusinessDate) {
            dateType = SYSTEM_DATE;
            expiredDateType = EXPIRED_SYSTEM_DATE;
        }
        // Create the nested 'bool' object inside filter
        ObjectNode nestedBoolNode = objectMapper.createObjectNode();
        ArrayNode nestedMustArray = objectMapper.createArrayNode();
        ObjectNode rangeBusinessDateNode = objectMapper.createObjectNode();
        rangeBusinessDateNode.put(condition, date);

        // Add 'range' object to nestedMust
        ObjectNode rangeBusinessDateWrapper = objectMapper.createObjectNode();
        rangeBusinessDateWrapper.set("range", objectMapper.createObjectNode().set(dateType, rangeBusinessDateNode));
        nestedMustArray.add(rangeBusinessDateWrapper);


        // Create 'bool' object for 'should'
        ObjectNode shouldBoolNodeWrapper = objectMapper.createObjectNode();
        ObjectNode shouldBoolNode = objectMapper.createObjectNode();
        ArrayNode shouldArray = objectMapper.createArrayNode();

        // Create 'range' object for 'expiredAtBusinessDate'
        ObjectNode rangeExpiredAtNode = objectMapper.createObjectNode();
        rangeExpiredAtNode.put("gt", date);

        // Add 'range' object to should array
        ObjectNode rangeExpiredAtWrapper = objectMapper.createObjectNode();
        rangeExpiredAtWrapper.set("range", objectMapper.createObjectNode().set(expiredDateType, rangeExpiredAtNode));
        shouldArray.add(rangeExpiredAtWrapper);

        // add 'term' object to should array
        ObjectNode termNode = objectMapper.createObjectNode();
        termNode.put(expiredDateType, 0);
        ObjectNode termNodeWrapper = objectMapper.createObjectNode();
        termNodeWrapper.set("term", termNode);
        shouldArray.add(termNodeWrapper);

        // Add 'should' to should array
        shouldBoolNode.set("should", shouldArray);
        shouldBoolNode.put("minimum_should_match", 1);
        shouldBoolNodeWrapper.set("bool", shouldBoolNode);

        // Add shouldBoolNodeWrapper to nestedMust
        nestedMustArray.add(shouldBoolNodeWrapper);

        // Add nestedMust to nestedBool
        nestedBoolNode.set("must", nestedMustArray);

        // Add nestedBool to filter
        ObjectNode nestedBoolWrapper = objectMapper.createObjectNode();
        nestedBoolWrapper.set("bool", nestedBoolNode);
        return nestedBoolWrapper;
    }
}