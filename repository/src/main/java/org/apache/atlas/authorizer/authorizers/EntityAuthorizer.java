package org.apache.atlas.authorizer.authorizers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.AccessResult;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;
import static org.apache.atlas.authorizer.authorizers.AuthorizerCommon.isResourceMatch;

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
                matched = validateEntityFilterCriteria(entityFilterCriteriaNode, entity, vertex);
            }
            if (matched) {
                //LOG.info("Matched with policy {}", policy.getGuid());
                result.setAllowed(true);
                result.setPolicyId(policy.getGuid());
                return result;
            }
        }
        return result;
    }

    public static boolean validateEntityFilterCriteria(JsonNode data, AtlasEntityHeader entity, AtlasVertex vertex) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateEntityFilterCriteria");
        String condition = data.get("condition").asText();
        JsonNode criterion = data.get("criterion");

        if (criterion.size() == 0) {
            return false;
        }
        boolean result = true;

        for (JsonNode crit : criterion) {
            result = !condition.equals("OR");

            boolean evaluation = false;

            if (crit.has("condition")) {
                evaluation = validateEntityFilterCriteria(crit, entity, vertex);
            } else {
                evaluation = evaluateFilterCriteriaInMemory(crit, entity, vertex);
            }

            if (condition.equals("AND")) {
                if (!evaluation) {
                    // One of the condition in AND is false, return false
                    return false;
                }
                result = true;
            } else {
                if (evaluation) {
                    // One of the condition in OR is true, return true
                    return true;
                }
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

        Object attrValue = entity.getAttribute(attributeName);
        if (attrValue != null) {
            if (attrValue instanceof Collection) {
                entityAttributeValues.addAll((Collection<? extends String>) attrValue);
            } else {
                entityAttributeValues.add((String) attrValue);
            }
        } else {
            // try fetching from vertex
            if (vertex != null) {
                entityAttributeValues.addAll(vertex.getPropertyValues(attributeName, String.class));
            }
        }

        if (entityAttributeValues.size() == 0) {
            switch (attributeName) {
                case "__traitNames":
                    List<AtlasClassification> tags = entity.getClassifications();
                    if (tags != null) {
                        for (AtlasClassification tag : tags) {
                            if (StringUtils.isEmpty(tag.getEntityGuid()) || tag.getEntityGuid().equals(entity.getGuid())) {
                                entityAttributeValues.add(tag.getTypeName());
                            }
                        }
                    }
                    break;

                case "__propagatedTraitNames":
                    tags = entity.getClassifications();
                    if (tags != null) {
                        for (AtlasClassification tag : tags) {
                            if (StringUtils.isNotEmpty(tag.getEntityGuid()) && !tag.getEntityGuid().equals(entity.getGuid())) {
                                entityAttributeValues.add(tag.getTypeName());
                            }
                        }
                    }
                    break;

                default:
                    LOG.warn("Value for attribute {} not found", attributeName);
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
}
