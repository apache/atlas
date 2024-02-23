package org.apache.atlas.authorizer.authorizers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.AccessResult;
import org.apache.atlas.authorizer.JsonToElasticsearchQuery;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.DENY_POLICY_NAME_SUFFIX;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.MAX_CLAUSE_LIMIT;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;
import static org.apache.atlas.authorizer.authorizers.AuthorizerCommon.getMap;
import static org.apache.atlas.authorizer.authorizers.AuthorizerCommon.isResourceMatch;
import static org.apache.atlas.authorizer.authorizers.EntityAuthorizer.validateFilterCriteriaWithEntity;
import static org.apache.atlas.authorizer.authorizers.ListAuthorizer.getPolicySuffix;
import static org.apache.atlas.repository.Constants.QUALIFIED_NAME;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getLowLevelClient;

public class RelationshipAuthorizer {

    private static final Logger LOG = LoggerFactory.getLogger(RelationshipAuthorizer.class);

    private static List<String> RELATIONSHIP_ENDS = new ArrayList<String>() {{
        add("end-one");
        add("end-two");
    }};

    public static AccessResult isAccessAllowedInMemory(String action, String relationshipType, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) throws AtlasBaseException {
        AccessResult result;

        result = checkRelationshipAccessAllowedInMemory(action, relationshipType, endOneEntity, endTwoEntity, POLICY_TYPE_DENY);
        if (result.isAllowed()) {
            result.setAllowed(false);
            return result;
        }

        return checkRelationshipAccessAllowedInMemory(action, relationshipType, endOneEntity, endTwoEntity, POLICY_TYPE_ALLOW);
    }

    private static AccessResult checkRelationshipAccessAllowedInMemory(String action, String relationshipType, AtlasEntityHeader endOneEntity,
                                                         AtlasEntityHeader endTwoEntity, String policyType) throws AtlasBaseException {
        //Relationship add, update, remove access check in memory
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("checkRelationshipAccessAllowedInMemory."+policyType);
        AccessResult result = new AccessResult();

        try {
            List<RangerPolicy> policies = PoliciesStore.getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action), policyType);
            if (!policies.isEmpty()) {
                ObjectMapper mapper = new ObjectMapper();
                AtlasVertex oneVertex = AtlasGraphUtilsV2.findByGuid(endOneEntity.getGuid());
                AtlasVertex twoVertex = AtlasGraphUtilsV2.findByGuid(endTwoEntity.getGuid());

                for (RangerPolicy policy : policies) {
                    String filterCriteria = policy.getPolicyFilterCriteria();

                    boolean eval = false;
                    JsonNode filterCriteriaNode = null;
                    try {
                        filterCriteriaNode = mapper.readTree(filterCriteria);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    if (filterCriteriaNode != null && filterCriteriaNode.get("endOneEntity") != null) {
                        JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("endOneEntity");
                        eval = validateFilterCriteriaWithEntity(entityFilterCriteriaNode, endOneEntity, oneVertex);

                        if (eval) {
                            entityFilterCriteriaNode = filterCriteriaNode.get("endTwoEntity");
                            eval = validateFilterCriteriaWithEntity(entityFilterCriteriaNode, endTwoEntity, twoVertex);
                        }
                    }
                    //ret = ret || eval;
                    if (eval) {
                        result.setAllowed(true);
                        result.setPolicyId(policy.getGuid());
                        break;
                    }
                }
            }

            return result;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
    }

    public static AccessResult isRelationshipAccessAllowed(String action, AtlasEntityHeader endOneEntity, AtlasEntityHeader endTwoEntity) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("RelationshipAuthorizer.isRelationshipAccessAllowed");
        AccessResult result = new AccessResult();

        //Relationship update, remove access check with ES query
        if (endOneEntity == null || endTwoEntity == null) {
            return result;
        }

        try {
            Map<String, Object> dsl = getElasticsearchDSLForRelationshipActions(Arrays.asList(action), endOneEntity, endTwoEntity);
            ObjectMapper mapper = new ObjectMapper();
            String dslString = mapper.writeValueAsString(dsl);
            RestClient restClient = getLowLevelClient();
            AtlasElasticsearchQuery elasticsearchQuery = new AtlasElasticsearchQuery("janusgraph_vertex_index", restClient);
            Map<String, Object> elasticsearchResult = elasticsearchQuery.runQueryWithLowLevelClient(dslString);
            LOG.info(dslString);
            Integer count = null;
            if (elasticsearchResult!=null) {
                count = (Integer) elasticsearchResult.get("total");
            }
            if (count != null && count == 2) {
                List<Map<String, Object>> docs = (List<Map<String, Object>>) elasticsearchResult.get("data");
                List<String> matchedClausesEndOne = new ArrayList<>();
                List<String> matchedClausesEndTwo = new ArrayList<>();
                for (Map<String, Object> doc : docs) {
                    List<String> matched_queries = (List<String>) doc.get("matched_queries");
                    if (matched_queries != null && !matched_queries.isEmpty()) {
                        Map<String, Object> source = (Map<String, Object>) doc.get("_source");
                        String guid = (String) source.get("__guid");
                        if (endOneEntity.getGuid().equals(guid)) {
                            for (String matched_query : matched_queries) {
                                if (matched_query.equals("tag-clause")) {
                                    matchedClausesEndOne.add("tag-clause");
                                } else if (matched_query.startsWith("end-one-")) {
                                    matchedClausesEndOne.add(matched_query.substring(8));
                                }
                            }
                        } else {
                            for (String matched_query : matched_queries) {
                                if (matched_query.equals("tag-clause")) {
                                    matchedClausesEndTwo.add("tag-clause");
                                } else if (matched_query.startsWith("end-two-")) {
                                    matchedClausesEndTwo.add(matched_query.substring(8));
                                }
                            }
                        }
                    }
                }
                List<String> common = (List<String>) CollectionUtils.intersection(matchedClausesEndOne, matchedClausesEndTwo);
                if (!common.isEmpty()) {
                    Optional<String> denied = common.stream().filter(x -> x.endsWith(DENY_POLICY_NAME_SUFFIX)).findFirst();

                    if (denied.isPresent()) {
                        result.setPolicyId(denied.get().split("_")[0]);
                    } else {
                        result.setAllowed(true);
                        result.setPolicyId(common.get(0));
                    }
                }

                /*if (arrayListContains(matchedClausesEndOne, matchedClausesEndTwo)) {
                    result.setAllowed(true);
                    return result;
                }*/
            }
            LOG.info(dslString);
        } catch (JsonProcessingException e) {
            return result;
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }
        return result;
    }

    private static Map<String, Object> getElasticsearchDSLForRelationshipActions(List<String> actions, AtlasEntityHeader endOneEntity,
                                                                                AtlasEntityHeader endTwoEntity) throws JsonProcessingException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("RelationshipAuthorizer.getElasticsearchDSLForRelationshipActions");

        List<Map<String, Object>> policiesClauses = new ArrayList<>();

        List<RangerPolicy> abacPolicies = PoliciesStore.getRelevantPolicies(null, null, "atlas_abac", actions, null);
        List<Map<String, Object>> abacPoliciesClauses = getDSLForRelationshipAbacPolicies(abacPolicies);

        policiesClauses.addAll(abacPoliciesClauses);

        List<Map<String, Object>> clauses = new ArrayList<>();

        Map<String, Object> policiesBoolClause = new HashMap<>();
        if (policiesClauses.isEmpty()) {
            policiesBoolClause.put("must_not", getMap("match_all", new HashMap<>()));
        } else {
            //policiesBoolClause.put("should", policiesClauses);
            if (policiesClauses.size() > MAX_CLAUSE_LIMIT) {
                List<Map<String, Object>> splittedShould = new ArrayList<>();
                List<List<Map<String, Object>>> partitionedShouldClause = Lists.partition(policiesClauses, MAX_CLAUSE_LIMIT);

                for (List<Map<String, Object>> chunk : partitionedShouldClause) {
                    splittedShould.add(getMap("bool", getMap("should", chunk)));
                }
                policiesBoolClause.put("should", splittedShould);

            } else {
                policiesBoolClause.put("should", policiesClauses);
            }

            policiesBoolClause.put("minimum_should_match", 1);
        }
        clauses.add(getMap("bool", policiesBoolClause));

        Map<String, Object> entitiesBoolClause = new HashMap<>();
        List<Map<String, Object>> entityClauses = new ArrayList<>();

        if (endOneEntity.getGuid() != null && endTwoEntity.getGuid() != null) {
            entityClauses.add(getMap("term", getMap("__guid", endOneEntity.getGuid())));
            entityClauses.add(getMap("term", getMap("__guid", endTwoEntity.getGuid())));
        } else {
            //In case of evaluator API, qualifiedName can be * leading to not finding entity in store hence null GUIDs
            //Use typeName & qualifiedName to form entity clauses
            List<Map<String, Object>> entityOneClauses = new ArrayList<>();
            entityOneClauses.add(getMap("wildcard", getMap("__typeName.keyword", endOneEntity.getTypeName())));
            entityOneClauses.add(getMap("wildcard", getMap("qualifiedName", endOneEntity.getAttribute(QUALIFIED_NAME))));

            List<Map<String, Object>> entityTwoClauses = new ArrayList<>();
            entityTwoClauses.add(getMap("wildcard", getMap("__typeName.keyword", endTwoEntity.getTypeName())));
            entityTwoClauses.add(getMap("wildcard", getMap("qualifiedName", endTwoEntity.getAttribute(QUALIFIED_NAME))));

            entityClauses.add(getMap("bool", getMap("must", entityOneClauses)));
            entityClauses.add(getMap("bool", getMap("must", entityTwoClauses)));
        }

        entitiesBoolClause.put("should", entityClauses);
        entitiesBoolClause.put("minimum_should_match", 1);
        clauses.add(getMap("bool", entitiesBoolClause));

        Map<String, Object> boolClause = new HashMap<>();
        boolClause.put("filter", clauses);


        RequestContext.get().endMetricRecord(recorder);
        return getMap("query", getMap("bool", boolClause));
    }

    private static List<Map<String, Object>> getDSLForRelationshipAbacPolicies(List<RangerPolicy> policies) throws JsonProcessingException {
        List<Map<String, Object>> shouldClauses = new ArrayList<>();
        for (RangerPolicy policy : policies) {
            if ("RELATIONSHIP".equals(policy.getPolicyResourceCategory())) {
                String filterCriteria = policy.getPolicyFilterCriteria();
                ObjectMapper mapper = new ObjectMapper();
                JsonNode filterCriteriaNode = mapper.readTree(filterCriteria);

                String suffix = getPolicySuffix(policy);
                for (String relationshipEnd : RELATIONSHIP_ENDS) {
                    JsonNode endFilterCriteriaNode = filterCriteriaNode.get(relationshipEnd.equals("end-one")  ? "endOneEntity" : "endTwoEntity");
                    JsonNode dsl = JsonToElasticsearchQuery.convertJsonToQuery(endFilterCriteriaNode, mapper);
                    String DslBase64 = Base64.getEncoder().encodeToString(dsl.toString().getBytes());
                    String clauseName = relationshipEnd + "-" + policy.getGuid() + suffix;
                    Map<String, Object> boolMap = new HashMap<>();
                    boolMap.put("_name", clauseName);
                    boolMap.put("filter", getMap("wrapper", getMap("query", DslBase64)));

                    shouldClauses.add(getMap("bool", boolMap));
                }
            }
        }
        return shouldClauses;
    }
}
