package org.apache.atlas.authorizer.authorizers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.AccessResult;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchQuery;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.DENY_POLICY_NAME_SUFFIX;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;
import static org.apache.atlas.authorizer.authorizers.AuthorizerCommon.getMap;
import static org.apache.atlas.authorizer.authorizers.AuthorizerCommon.isResourceMatch;
import static org.apache.atlas.repository.graphdb.janus.AtlasElasticsearchDatabase.getLowLevelClient;

public class EntityAuthorizer {

    private static final Logger LOG = LoggerFactory.getLogger(EntityAuthorizer.class);

    public static AccessResult isAccessAllowedInMemory(AtlasEntityHeader entity, String action) {
        AccessResult result;

        result = isAccessAllowedInMemory(entity, action, POLICY_TYPE_DENY);
        if (result.isAllowed()) {
            result.setAllowed(false);
            return result;
        }

        return isAccessAllowedInMemory(entity, action, POLICY_TYPE_ALLOW);
    }

    private static AccessResult isAccessAllowedInMemory(AtlasEntityHeader entity, String action, String policyType) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("isAccessAllowedInMemory."+policyType);
        AccessResult result;

        List<RangerPolicy> policies = PoliciesStore.getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action), policyType);
        result = evaluateABACPoliciesInMemory(policies, entity);

        RequestContext.get().endMetricRecord(recorder);
        return result;
    }

    private static AccessResult evaluateABACPoliciesInMemory(List<RangerPolicy> abacPolicies, AtlasEntityHeader entity) {
        AccessResult result = new AccessResult();

        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(entity.getGuid());
        ObjectMapper mapper = new ObjectMapper();

        for (RangerPolicy policy : abacPolicies) {
            String filterCriteria = policy.getPolicyFilterCriteria();

            boolean matched = false;
            JsonNode filterCriteriaNode = null;
            try {
                filterCriteriaNode = mapper.readTree(filterCriteria);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            if (filterCriteriaNode != null && filterCriteriaNode.get("entity") != null) {
                JsonNode entityFilterCriteriaNode = filterCriteriaNode.get("entity");
                matched = validateFilterCriteriaWithEntity(entityFilterCriteriaNode, entity, vertex);
            }
            if (matched) {
                LOG.info("Matched with policy {}", policy.getGuid());
                result.setAllowed(true);
                result.setPolicyId(policy.getGuid());
                return result;
            }
        }
        return result;
    }

    public static boolean validateFilterCriteriaWithEntity(JsonNode data, AtlasEntityHeader entity, AtlasVertex vertex) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateFilterCriteriaWithEntity");
        String condition = data.get("condition").asText();
        JsonNode criterion = data.get("criterion");

        if (criterion.size() == 0) {
            return false;
        }
        boolean result = true;

        for (JsonNode crit : criterion) {

            result = true;
            if (condition.equals("OR")) {
                result = false;
            }
            boolean evaluation = false;

            if (crit.has("condition")) {
                evaluation = validateFilterCriteriaWithEntity(crit, entity, vertex);
            } else {
                evaluation = evaluateFilterCriteriaInMemory(crit, entity, vertex);
            }

            if (condition.equals("AND")) {
                if (!evaluation) {
                    return false;
                }
                result = true;
            } else {
                result = result || evaluation;
            }
        }

        RequestContext.get().endMetricRecord(recorder);
        return result;
    }

    private static boolean evaluateFilterCriteriaInMemory(JsonNode crit, AtlasEntityHeader entity, AtlasVertex vertex) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("evaluateFilterCriteria");

        String attributeName = crit.get("attributeName").asText();

        if (attributeName.endsWith(".text")) {
            attributeName = attributeName.replace(".text", "");
        } else if (attributeName.endsWith(".keyword")) {
            attributeName = attributeName.replace(".keyword", "");
        }

        List<String> entityAttributeValues = new ArrayList<>();

        switch (attributeName) {
            case "__traitNames":
                List<AtlasClassification> tags = entity.getClassifications();
                if (tags != null) {
                    for (AtlasClassification tag: tags) {
                        if (StringUtils.isEmpty(tag.getEntityGuid()) || tag.getEntityGuid().equals(entity.getGuid())) {
                            entityAttributeValues.add(tag.getTypeName());
                        }
                    }
                }
                break;

            case "__propagatedTraitNames":
                tags = entity.getClassifications();
                if (tags != null) {
                    for (AtlasClassification tag: tags) {
                        if (StringUtils.isNotEmpty(tag.getEntityGuid()) && !tag.getEntityGuid().equals(entity.getGuid())) {
                            entityAttributeValues.add(tag.getTypeName());
                        }
                    }
                }
                break;

            default:
                Object attrValue = entity.getAttribute(attributeName);
                if (attrValue != null) {
                    if (attrValue instanceof Collection) {
                        entityAttributeValues.addAll((Collection<? extends String>) entity.getAttribute(attributeName));
                    } else {
                        entityAttributeValues.add((String) entity.getAttribute(attributeName));
                    }
                } else {
                    // try fetching from vertex
                    if (vertex != null) {
                        entityAttributeValues.addAll(vertex.getPropertyValues(attributeName, String.class));
                    }
                }
        }

        String operator = crit.get("operator").asText();
        String attributeValue = crit.get("attributeValue").asText();

        switch (operator) {
            case "EQUALS":
                if (entityAttributeValues.contains(attributeValue)) {
                    return true;
                }
                break;
            case "STARTS_WITH":
                if (AuthorizerCommon.listStartsWith(attributeValue, entityAttributeValues)) {
                    return true;
                }
                break;
            case "LIKE":
                if (AuthorizerCommon.listMatchesWith(attributeValue, entityAttributeValues)) {
                    return true;
                }
                break;
            case "ENDS_WITH":
                if (AuthorizerCommon.listEndsWith(attributeValue, entityAttributeValues)) {
                    return true;
                }
                break;
            case "NOT_EQUALS":
                if (!entityAttributeValues.contains(attributeValue)) {
                    return true;
                }
                break;
/*
            case "IN":
                break;
            case "NOT_IN":
                break;
*/

            default: LOG.warn("Found unknown operator {}", operator);
        }

        RequestContext.get().endMetricRecord(recorder);
        return false;
    }

    public static AccessResult isAccessAllowed(String guid, String action) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("EntityAuthorizer.isAccessAllowed");
        AccessResult result = new AccessResult();

        if (guid == null) {
            return result;
        }
        List<Map<String, Object>> filterClauseList = new ArrayList<>();
        Map<String, Object> policiesDSL = getElasticsearchDSL(null, null, true, Arrays.asList(action));
        filterClauseList.add(policiesDSL);
        filterClauseList.add(getMap("term", getMap("__guid", guid)));
        Map<String, Object> dsl = getMap("query", getMap("bool", getMap("filter", filterClauseList)));

        result = runESQueryAndEvaluateAccess(dsl);

        RequestContext.get().endMetricRecord(recorder);
        return result;
    }

    public static AccessResult isAccessAllowedEvaluator(String entityTypeName, String entityQualifiedName, String action) throws AtlasBaseException {

        List<Map<String, Object>> filterClauseList = new ArrayList<>();
        Map<String, Object> policiesDSL = getElasticsearchDSL(null, null, true, Arrays.asList(action));
        filterClauseList.add(policiesDSL);
        filterClauseList.add(getMap("wildcard", getMap("__typeName.keyword", entityTypeName)));
        if (entityQualifiedName != null)
            filterClauseList.add(getMap("wildcard", getMap("qualifiedName", entityQualifiedName)));
        Map<String, Object> dsl = getMap("query", getMap("bool", getMap("filter", filterClauseList)));

        AccessResult result = runESQueryAndEvaluateAccess(dsl);

        return result;
    }

    private static AccessResult runESQueryAndEvaluateAccess(Map<String, Object> dsl) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("runESQueryAndEvaluateAccess");
        AccessResult result = new AccessResult();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> response = null;

        try {

            try {
                String dslString = mapper.writeValueAsString(dsl);
                response = runElasticsearchQuery(dslString);
                LOG.info(dslString);
            } catch (JsonProcessingException | AtlasBaseException e) {
                e.printStackTrace();
            }

            if (response != null) {
                Integer count = (Integer) response.get("total");
                if (count != null && count > 0) {
                    String policyId = null;
                    List<Map<String, Object>> docs = (List<Map<String, Object>>) response.get("data");

                    for (Map<String, Object> doc : docs) {
                        List<String> matched_queries = (List<String>) doc.get("matched_queries");
                        if (CollectionUtils.isNotEmpty(matched_queries)) {
                            Optional<String> denied = matched_queries.stream().filter(x -> x.endsWith(DENY_POLICY_NAME_SUFFIX)).findFirst();

                            if (denied.isPresent()) {
                                result.setPolicyId(denied.get().split("_")[0]);
                            } else {
                                result.setAllowed(true);
                                result.setPolicyId(matched_queries.get(0));
                            }
                            return result;
                        } else {
                            throw new AtlasBaseException("Failed to extract matched policy guid");
                        }
                    }

                    return result;
                }
            }
        } finally {
            RequestContext.get().endMetricRecord(recorder);
        }

        return result;
    }

    private static Map<String, Object> getElasticsearchDSL(String persona, String purpose,
                                                          boolean requestMatchedPolicyId, List<String> actions) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("EntityAuthorizer.getElasticsearchDSL");
        Map<String, Object> dsl = ListAuthorizer.getElasticsearchDSLForPolicyType(persona, purpose, actions, requestMatchedPolicyId, null);

        List<Map<String, Object>> finaDsl = new ArrayList<>();
        if (dsl != null) {
            finaDsl.add(dsl);
        }

        RequestContext.get().endMetricRecord(recorder);
        return getMap("bool", getMap("filter", getMap("bool", getMap("should", finaDsl))));
    }

    private static Integer getCountFromElasticsearch(String query) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("EntityAuthorizer.getCountFromElasticsearch");

        Map<String, Object> elasticsearchResult = runElasticsearchQuery(query);
        Integer count = null;
        if (elasticsearchResult!=null) {
            count = (Integer) elasticsearchResult.get("total");
        }
        RequestContext.get().endMetricRecord(recorder);
        return count;
    }

    private static Map<String, Object> runElasticsearchQuery(String query) throws AtlasBaseException {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("EntityAuthorizer.runElasticsearchQuery");
        RestClient restClient = getLowLevelClient();
        AtlasElasticsearchQuery elasticsearchQuery = new AtlasElasticsearchQuery("janusgraph_vertex_index", restClient);

        Map<String, Object> elasticsearchResult = elasticsearchQuery.runQueryWithLowLevelClient(query);
        RequestContext.get().endMetricRecord(recorder);
        return elasticsearchResult;
    }
}
