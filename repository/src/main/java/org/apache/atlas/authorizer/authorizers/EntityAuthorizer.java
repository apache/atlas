package org.apache.atlas.authorizer.authorizers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAccessResult;
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
import java.util.Set;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;

public class EntityAuthorizer {

    private static final Logger LOG = LoggerFactory.getLogger(EntityAuthorizer.class);
    private static final PoliciesStore policiesStore = PoliciesStore.getInstance();

    public static AtlasAccessResult isAccessAllowedInMemory(AtlasEntityHeader entity, String action) {

        AtlasAccessResult denyResult = isAccessAllowedInMemory(entity, action, POLICY_TYPE_DENY);
        if (denyResult.isAllowed() && denyResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
            return new AtlasAccessResult(false, denyResult.getPolicyId(), denyResult.getPolicyPriority());
        }

        AtlasAccessResult allowResult = isAccessAllowedInMemory(entity, action, POLICY_TYPE_ALLOW);
        if (allowResult.isAllowed() && allowResult.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
            return allowResult;
        }

        if (denyResult.isAllowed() && !"-1".equals(denyResult.getPolicyId())) {
            // explicit deny
            return new AtlasAccessResult(false, denyResult.getPolicyId(), denyResult.getPolicyPriority());
        } else {
            return allowResult;
        }
    }

    private static AtlasAccessResult isAccessAllowedInMemory(AtlasEntityHeader entity, String action, String policyType) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("isAccessAllowedInMemory."+policyType);
        AtlasAccessResult result;

        List<RangerPolicy> policies = policiesStore.getRelevantPolicies(null, null, "atlas_abac", Arrays.asList(action), policyType);
        result = evaluateABACPoliciesInMemory(policies, entity);

        RequestContext.get().endMetricRecord(recorder);
        return result;
    }

    private static AtlasAccessResult evaluateABACPoliciesInMemory(List<RangerPolicy> abacPolicies, AtlasEntityHeader entity) {
        AtlasAccessResult result = new AtlasAccessResult(false);

        AtlasVertex vertex = AtlasGraphUtilsV2.findByGuid(entity.getGuid());

        for (RangerPolicy policy : abacPolicies) {
            boolean matched = false;
            JsonNode entityFilterCriteriaNode = policy.getPolicyParsedFilterCriteria("entity");
            if (entityFilterCriteriaNode != null) {
                matched = validateEntityFilterCriteria(entityFilterCriteriaNode, entity, vertex);
            }
            if (matched) {
                // result here only means that a matching policy is found, allow and deny needs to be handled by caller
                result = new AtlasAccessResult(true, policy.getGuid(), policy.getPolicyPriority());
                if (policy.getPolicyPriority() == RangerPolicy.POLICY_PRIORITY_OVERRIDE) {
                    return result;
                }
            }
        }
        return result;
    }

    public static boolean validateEntityFilterCriteria(JsonNode data, AtlasEntityHeader entity, AtlasVertex vertex) {
        AtlasPerfMetrics.MetricRecorder recorder = RequestContext.get().startMetricRecord("validateEntityFilterCriteria");
        String condition = data.get("condition").asText();
        JsonNode criterion = data.get("criterion");

        if (criterion == null || !criterion.isArray() || criterion.isEmpty() ) {
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

        List<String> entityAttributeValues = getAttributeValue(entity, attributeName, vertex);
        if (entityAttributeValues.isEmpty()) {
            entityAttributeValues = handleSpecialAttributes(entity, attributeName);
        }

        JsonNode attributeValueNode = crit.get("attributeValue");
        String attributeValue = attributeValueNode.asText();
        String operator = crit.get("operator").asText();

        // incase attributeValue is an array
        List<String> attributeValues = new ArrayList<>();
        if (attributeValueNode.isArray()) {
            attributeValueNode.elements().forEachRemaining(node -> attributeValues.add(node.asText()));
        }

        switch (operator) {
            case "EQUALS":
                if (entityAttributeValues.contains(attributeValue)) {
                    return true;
                }
                break;
            case "STARTS_WITH":
                if (AuthorizerCommonUtil.listStartsWith(attributeValue, entityAttributeValues)) {
                    return true;
                }
                break;
            case "LIKE":
                if (AuthorizerCommonUtil.listMatchesWith(attributeValue, entityAttributeValues)) {
                    return true;
                }
                break;
            case "ENDS_WITH":
                if (AuthorizerCommonUtil.listEndsWith(attributeValue, entityAttributeValues)) {
                    return true;
                }
                break;
            case "NOT_EQUALS":
                if (!entityAttributeValues.contains(attributeValue)) {
                    return true;
                }
                break;
            case "IN":
                if (AuthorizerCommonUtil.arrayListContains(attributeValues, entityAttributeValues)) {
                    return true;
                }
                break;
            case "NOT_IN":
                if (!AuthorizerCommonUtil.arrayListContains(attributeValues, entityAttributeValues)) {
                    return true;
                }
                break;

            default: LOG.warn("Found unknown operator {}", operator);
        }

        RequestContext.get().endMetricRecord(recorder);
        return false;
    }

    private static List<String> handleSpecialAttributes(AtlasEntityHeader entity, String attributeName) {
        List<String> entityAttributeValues = new ArrayList<>();

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

            case "__typeName":
                String typeName = entity.getTypeName();
                Set<String> allValidTypes = AuthorizerCommonUtil.getTypeAndSupertypesList(typeName);
                entityAttributeValues.addAll(allValidTypes);
                break;

            default:
                LOG.warn("Value for attribute {} not found", attributeName);
        }

        return entityAttributeValues;
    }

    private static List<String> getAttributeValue(AtlasEntityHeader entity, String attributeName, AtlasVertex vertex) {
        List<String> entityAttributeValues = new ArrayList<>();

        List<String> relatedAttributes = getRelatedAttributes(attributeName);
        for (String relatedAttribute : relatedAttributes) {
            Object attrValue = entity.getAttribute(relatedAttribute);
            if (attrValue != null) {
                if (attrValue instanceof Collection) {
                    entityAttributeValues.addAll((Collection<? extends String>) attrValue);
                } else {
                    entityAttributeValues.add(String.valueOf(attrValue));
                }
            } else if (vertex != null) {
                // try fetching from vertex
                Collection<?> values = vertex.getPropertyValues(relatedAttribute, String.class);
                for (Object value : values) {
                    entityAttributeValues.add(String.valueOf(value));
                }
            }
        }
        return entityAttributeValues;
    }

    private static List<String> getRelatedAttributes(String attributeName) {
        List<String> relatedAttributes = new ArrayList<>();

        // Define sets of related attributes
        List<Set<String>> relatedAttributeSets = Arrays.asList(
                Set.of("ownerUsers", "ownerGroups"),
                Set.of("__traitNames", "__propagatedTraitNames")
        );

        // Check if attributeName exists in any set and add the entire set
        for (Set<String> attributeSet : relatedAttributeSets) {
            if (attributeSet.contains(attributeName)) {
                relatedAttributes.addAll(attributeSet);
                break;
            }
        }

        return relatedAttributes;
    }
}
