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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.discovery.PurposeUserRequest;
import org.apache.atlas.model.discovery.PurposeUserResponse;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Implementation of PurposeDiscoveryService that uses ES queries to efficiently
 * discover Purpose entities accessible to a user.
 * <p>
 * The implementation follows a two-step approach:
 * 1. Query AuthPolicies with policyCategory="purpose" filtered by user/groups,
 *    including the accessControl relationship attribute
 * 2. Extract unique Purpose GUIDs and fetch Purpose details
 * </p>
 * <p>
 * Note: This class is NOT a Spring-managed bean to avoid initialization order
 * issues during context startup. It is instantiated manually by PurposeDiscoveryREST.
 * </p>
 */
public class PurposeDiscoveryServiceImpl implements PurposeDiscoveryService {
    private static final Logger LOG = LoggerFactory.getLogger(PurposeDiscoveryServiceImpl.class);

    // Type names
    private static final String TYPE_AUTH_POLICY = "AuthPolicy";
    private static final String TYPE_PURPOSE = "Purpose";

    // Policy filter constants
    private static final String POLICY_CATEGORY_PURPOSE = "purpose";
    private static final String PUBLIC_GROUP = "public";

    // Attribute names
    private static final String ATTR_POLICY_CATEGORY = "policyCategory";
    private static final String ATTR_POLICY_GROUPS = "policyGroups";
    private static final String ATTR_POLICY_USERS = "policyUsers";
    private static final String ATTR_ACCESS_CONTROL = "accessControl";
    private static final String ATTR_STATE = "__state";
    private static final String ATTR_TYPE_NAME = "__typeName.keyword";
    private static final String ATTR_GUID = "__guid";

    // Configuration keys
    private static final String CONFIG_MAX_AGGREGATION_SIZE = "atlas.discovery.purpose.max-aggregation-size";
    private static final String CONFIG_MAX_POLICY_FETCH_SIZE = "atlas.discovery.purpose.max-policy-fetch-size";

    // Defaults
    private static final int DEFAULT_MAX_AGGREGATION_SIZE = 500;
    private static final int DEFAULT_MAX_POLICY_FETCH_SIZE = 1000;

    private final AtlasDiscoveryService discoveryService;
    private final ObjectMapper mapper;

    // Lazy-initialized configuration values
    private volatile Integer maxAggregationSize;
    private volatile Integer maxPolicyFetchSize;

    public PurposeDiscoveryServiceImpl(AtlasDiscoveryService discoveryService) {
        this.discoveryService = discoveryService;
        this.mapper = new ObjectMapper();
        // Configuration is loaded lazily on first use to avoid blocking during Spring context init
    }

    /**
     * Lazily loads configuration values on first use.
     */
    private void ensureConfigLoaded() {
        if (maxAggregationSize == null) {
            synchronized (this) {
                if (maxAggregationSize == null) {
                    int aggregationSize = DEFAULT_MAX_AGGREGATION_SIZE;
                    int policyFetchSize = DEFAULT_MAX_POLICY_FETCH_SIZE;

                    try {
                        Configuration config = ApplicationProperties.get();
                        aggregationSize = config.getInt(CONFIG_MAX_AGGREGATION_SIZE, DEFAULT_MAX_AGGREGATION_SIZE);
                        policyFetchSize = config.getInt(CONFIG_MAX_POLICY_FETCH_SIZE, DEFAULT_MAX_POLICY_FETCH_SIZE);
                    } catch (Exception e) {
                        LOG.warn("Failed to load configuration for PurposeDiscoveryService, using defaults", e);
                    }

                    this.maxPolicyFetchSize = policyFetchSize;
                    this.maxAggregationSize = aggregationSize;

                    LOG.info("PurposeDiscoveryServiceImpl config loaded: maxAggregationSize={}, maxPolicyFetchSize={}",
                            maxAggregationSize, maxPolicyFetchSize);
                }
            }
        }
    }

    private int getMaxAggregationSize() {
        ensureConfigLoaded();
        return maxAggregationSize;
    }

    private int getMaxPolicyFetchSize() {
        ensureConfigLoaded();
        return maxPolicyFetchSize;
    }

    @Override
    public PurposeUserResponse discoverPurposesForUser(PurposeUserRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("discoverPurposesForUser");

        try {
            LOG.debug("Discovering purposes for user: {}, groups: {}",
                    request.getUsername(), request.getGroups());

            // Step 1: Get unique Purpose GUIDs from AuthPolicies
            Set<String> purposeGuids = getUniquePurposeGuids(request);

            if (CollectionUtils.isEmpty(purposeGuids)) {
                LOG.debug("No purposes found for user: {}", request.getUsername());
                return PurposeUserResponse.empty();
            }

            long totalCount = purposeGuids.size();
            LOG.debug("Found {} unique purpose GUIDs for user: {}", totalCount, request.getUsername());

            // Step 2: Apply pagination to GUIDs
            List<String> paginatedGuids = applyPagination(new ArrayList<>(purposeGuids),
                    request.getOffset(), request.getLimit());

            if (paginatedGuids.isEmpty()) {
                return new PurposeUserResponse(Collections.emptyList(), totalCount,
                        request.getLimit(), request.getOffset());
            }

            // Step 3: Fetch Purpose details
            List<AtlasEntityHeader> purposes = fetchPurposeDetails(paginatedGuids, request.getAttributes());

            return new PurposeUserResponse(purposes, totalCount, request.getLimit(), request.getOffset());

        } finally {
            RequestContext.get().endMetricRecord(metric);
        }
    }

    /**
     * Queries AuthPolicies with policyCategory="purpose" filtered by user/groups
     * and extracts unique Purpose GUIDs from the accessControl relationship.
     */
    private Set<String> getUniquePurposeGuids(PurposeUserRequest request) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("getUniquePurposeGuids");
        boolean wasInvokedByIndexSearch = RequestContext.get().isInvokedByIndexSearch();

        try {
            // Build the DSL query for AuthPolicies
            Map<String, Object> dsl = buildAuthPolicyQuery(request);

            // Create IndexSearchParams
            IndexSearchParams searchParams = new IndexSearchParams();
            searchParams.setDsl(dsl);
            // Include accessControl attribute to get the Purpose reference
            searchParams.setAttributes(new HashSet<>(Arrays.asList(ATTR_ACCESS_CONTROL)));
            searchParams.setRelationAttributes(new HashSet<>(Arrays.asList("guid", "typeName")));
            searchParams.setIncludeRelationshipAttributes(true);
            searchParams.setSuppressLogs(true);

            // Set index search context flag (required for relationship attribute processing)
            RequestContext.get().setIsInvokedByIndexSearch(true);

            // Execute the query with bulk fetching enabled to populate relationship attributes
            AtlasSearchResult searchResult = discoveryService.directIndexSearch(searchParams, true);

            if (searchResult == null || CollectionUtils.isEmpty(searchResult.getEntities())) {
                LOG.info("No AuthPolicies found for user: {}, groups: {}", request.getUsername(), request.getGroups());
                return Collections.emptySet();
            }

            int resultCount = searchResult.getEntities().size();
            LOG.info("Found {} AuthPolicies for user: {}", resultCount, request.getUsername());

            // Warn if we hit the fetch limit - results may be incomplete
            if (resultCount >= getMaxPolicyFetchSize()) {
                LOG.warn("AuthPolicy fetch hit limit of {} for user: {}. Results may be incomplete. " +
                        "Consider increasing 'atlas.discovery.purpose.max-policy-fetch-size' configuration.",
                        getMaxPolicyFetchSize(), request.getUsername());
            }

            // Extract unique Purpose GUIDs from accessControl relationship
            Set<String> purposeGuids = new LinkedHashSet<>();
            for (AtlasEntityHeader policy : searchResult.getEntities()) {
                String purposeGuid = extractPurposeGuid(policy);
                if (purposeGuid != null) {
                    purposeGuids.add(purposeGuid);
                }
            }

            return purposeGuids;
        } finally {
            RequestContext.get().setIsInvokedByIndexSearch(wasInvokedByIndexSearch);
            RequestContext.get().endMetricRecord(metric);
        }
    }

    /**
     * Builds the ES DSL query for AuthPolicies with policyCategory="purpose"
     * filtered by user's username and groups.
     */
    private Map<String, Object> buildAuthPolicyQuery(PurposeUserRequest request) {
        // Build the should clause for user/group matching
        List<Map<String, Object>> shouldClauses = new ArrayList<>();

        // Always include public purposes
        shouldClauses.add(termQuery(ATTR_POLICY_GROUPS, PUBLIC_GROUP));

        // Add user's groups
        if (CollectionUtils.isNotEmpty(request.getGroups())) {
            shouldClauses.add(termsQuery(ATTR_POLICY_GROUPS, request.getGroups()));
        }

        // Add username
        shouldClauses.add(termQuery(ATTR_POLICY_USERS, request.getUsername()));

        // Build the filter clause
        List<Map<String, Object>> filterClauses = new ArrayList<>();
        filterClauses.add(termQuery(ATTR_STATE, "ACTIVE"));
        filterClauses.add(termQuery(ATTR_TYPE_NAME, TYPE_AUTH_POLICY));
        filterClauses.add(termQuery(ATTR_POLICY_CATEGORY, POLICY_CATEGORY_PURPOSE));

        // Add the should clause with minimum_should_match
        Map<String, Object> shouldBool = new LinkedHashMap<>();
        shouldBool.put("should", shouldClauses);
        shouldBool.put("minimum_should_match", 1);
        filterClauses.add(Collections.singletonMap("bool", shouldBool));

        // Build the full query
        Map<String, Object> boolQuery = new LinkedHashMap<>();
        boolQuery.put("filter", Collections.singletonMap("bool",
                Collections.singletonMap("must", filterClauses)));

        Map<String, Object> dsl = new LinkedHashMap<>();
        dsl.put("size", getMaxPolicyFetchSize());
        dsl.put("query", Collections.singletonMap("bool", boolQuery));

        if (LOG.isDebugEnabled()) {
            try {
                LOG.debug("AuthPolicy query DSL: {}", mapper.writeValueAsString(dsl));
            } catch (JsonProcessingException e) {
                LOG.debug("AuthPolicy query DSL: {}", dsl);
            }
        }

        return dsl;
    }

    /**
     * Extracts the Purpose GUID from the accessControl relationship attribute.
     */
    @SuppressWarnings("unchecked")
    private String extractPurposeGuid(AtlasEntityHeader policy) {
        if (policy == null) {
            return null;
        }

        Object accessControl = policy.getAttribute(ATTR_ACCESS_CONTROL);
        if (accessControl == null) {
            LOG.debug("accessControl is null for policy: {}, available attrs: {}",
                    policy.getGuid(), policy.getAttributes() != null ? policy.getAttributes().keySet() : "null");
            return null;
        }

        // accessControl can be AtlasObjectId, Map, or AtlasEntityHeader
        if (accessControl instanceof AtlasObjectId) {
            return ((AtlasObjectId) accessControl).getGuid();
        } else if (accessControl instanceof Map) {
            Map<String, Object> acMap = (Map<String, Object>) accessControl;
            Object guid = acMap.get("guid");
            return guid != null ? guid.toString() : null;
        } else if (accessControl instanceof AtlasEntityHeader) {
            return ((AtlasEntityHeader) accessControl).getGuid();
        }

        LOG.warn("Unexpected accessControl type: {} for policy: {}",
                accessControl.getClass().getName(), policy.getGuid());
        return null;
    }

    /**
     * Applies pagination to the list of GUIDs.
     */
    private List<String> applyPagination(List<String> guids, int offset, int limit) {
        if (offset >= guids.size()) {
            return Collections.emptyList();
        }

        int endIndex = Math.min(offset + limit, guids.size());
        return guids.subList(offset, endIndex);
    }

    /**
     * Fetches Purpose entity details by their GUIDs.
     */
    private List<AtlasEntityHeader> fetchPurposeDetails(List<String> guids, Set<String> attributes)
            throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder metric = RequestContext.get().startMetricRecord("fetchPurposeDetails");
        boolean wasInvokedByIndexSearch = RequestContext.get().isInvokedByIndexSearch();

        try {
            // Build the DSL query for Purposes
            Map<String, Object> dsl = buildPurposeQuery(guids);

            // Create IndexSearchParams
            IndexSearchParams searchParams = new IndexSearchParams();
            searchParams.setDsl(dsl);

            // Set default attributes if not specified
            if (CollectionUtils.isEmpty(attributes)) {
                attributes = getDefaultPurposeAttributes();
            }
            searchParams.setAttributes(attributes);
            searchParams.setSuppressLogs(true);

            // Set index search context flag (required for proper attribute processing)
            RequestContext.get().setIsInvokedByIndexSearch(true);

            // Execute the query with bulk fetching enabled
            AtlasSearchResult searchResult = discoveryService.directIndexSearch(searchParams, true);

            if (searchResult == null || CollectionUtils.isEmpty(searchResult.getEntities())) {
                LOG.warn("No Purpose entities found for GUIDs: {}", guids);
                return Collections.emptyList();
            }

            // Build map for ordered lookup
            Map<String, AtlasEntityHeader> purposeMap = new HashMap<>();
            for (AtlasEntityHeader entity : searchResult.getEntities()) {
                purposeMap.put(entity.getGuid(), entity);
            }

            // Maintain the order of input GUIDs
            List<AtlasEntityHeader> purposes = new ArrayList<>(guids.size());
            for (String guid : guids) {
                AtlasEntityHeader purpose = purposeMap.get(guid);
                if (purpose != null) {
                    purposes.add(purpose);
                }
            }

            return purposes;
        } finally {
            RequestContext.get().setIsInvokedByIndexSearch(wasInvokedByIndexSearch);
            RequestContext.get().endMetricRecord(metric);
        }
    }

    /**
     * Builds the ES DSL query for fetching Purpose entities by GUIDs.
     */
    private Map<String, Object> buildPurposeQuery(List<String> guids) {
        List<Map<String, Object>> filterClauses = new ArrayList<>();
        filterClauses.add(termQuery(ATTR_STATE, "ACTIVE"));
        filterClauses.add(termQuery(ATTR_TYPE_NAME, TYPE_PURPOSE));
        filterClauses.add(termsQuery(ATTR_GUID, guids));

        Map<String, Object> boolQuery = new LinkedHashMap<>();
        boolQuery.put("filter", Collections.singletonMap("bool",
                Collections.singletonMap("must", filterClauses)));

        Map<String, Object> dsl = new LinkedHashMap<>();
        dsl.put("size", guids.size());
        dsl.put("query", Collections.singletonMap("bool", boolQuery));

        return dsl;
    }

    /**
     * Returns the default set of attributes to fetch for Purpose entities.
     */
    private Set<String> getDefaultPurposeAttributes() {
        return new HashSet<>(Arrays.asList(
                "name",
                "displayName",
                "description",
                "qualifiedName",
                "purposeClassifications",
                "isAccessControlEnabled",
                "__createdBy",
                "__modifiedBy",
                "__timestamp",
                "__modificationTimestamp"
        ));
    }

    // Helper methods for building ES query clauses

    private Map<String, Object> termQuery(String field, String value) {
        return Collections.singletonMap("term", Collections.singletonMap(field, value));
    }

    private Map<String, Object> termsQuery(String field, List<String> values) {
        return Collections.singletonMap("terms", Collections.singletonMap(field, values));
    }
}
