package org.apache.atlas.authorizer.authorizers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorize.AtlasAccessResult;
import org.apache.atlas.authorizer.store.PoliciesStore;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.plugin.model.RangerPolicy;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_ALLOW;
import static org.apache.atlas.authorizer.ABACAuthorizerUtils.POLICY_TYPE_DENY;
import static org.apache.atlas.repository.Constants.ACTION_READ;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_TAGS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_EQUALS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_ENDS_WITH;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NOT_EQUALS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_STARTS_WITH;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_IN;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NOT_IN;
import static org.apache.atlas.repository.util.AccessControlUtils.ATTR_QUALIFIED_NAME;

public class EntityAuthorizer {

    private static final Logger LOG = LoggerFactory.getLogger(EntityAuthorizer.class);
    private static final PoliciesStore policiesStore = PoliciesStore.getInstance();
    private static final List<Set<String>> relatedAttributeSets = Arrays.asList(
            Set.of("ownerUsers", "ownerGroups"),
            Set.of("__traitNames", "__propagatedTraitNames")
    );

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
        result = evaluateABACPoliciesInMemory(policies, entity, action);

        RequestContext.get().endMetricRecord(recorder);
        return result;
    }

    private static AtlasAccessResult evaluateABACPoliciesInMemory(List<RangerPolicy> abacPolicies, AtlasEntityHeader entity, String action) {
        AtlasAccessResult result = new AtlasAccessResult(false);

        if (CollectionUtils.isEmpty(abacPolicies)) {
            return result;
        }

        // don't need to fetch vertex for indexsearch response scrubbing as it already has the required attributes
        // setting vertex to null here as usage is already with a check for null possibility
        AtlasVertex vertex =  entity.getDocId() == null || !ACTION_READ.equals(action) ? AtlasGraphUtilsV2.findByGuid(entity.getGuid()) : null;

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
        entityAttributeValues.addAll(handleSpecialAttributes(entity, attributeName));
        if (entityAttributeValues.isEmpty()) {
            LOG.warn("Value for attribute {} not found for {}:{}", attributeName, entity.getTypeName(), entity.getAttribute(ATTR_QUALIFIED_NAME));
        }

        JsonNode attributeValueNode = crit.get("attributeValue");
        String operator = crit.get("operator").asText();

        if (ATTR_TAGS.contains(attributeName)) { // handling tag values separately to incorporate multiple values requirement
            return evaluateTagFilterCriteria(attributeName, attributeValueNode, operator, entityAttributeValues);
        }

        List<String> attributeValues = new ArrayList<>();
        if (attributeValueNode.isArray()) {
            attributeValueNode.elements().forEachRemaining(node -> attributeValues.add(node.asText()));
        } else {
            attributeValues.add(attributeValueNode.asText());
        }

        switch (operator) {
            case POLICY_FILTER_CRITERIA_EQUALS -> {
                return new HashSet<>(entityAttributeValues).containsAll(attributeValues);
            }
            case POLICY_FILTER_CRITERIA_STARTS_WITH -> {
                for (String value : attributeValues) {
                    if (AuthorizerCommonUtil.listStartsWith(value, entityAttributeValues)) {
                        return true;
                    }
                }
            }
            case POLICY_FILTER_CRITERIA_ENDS_WITH -> {
                for (String value : attributeValues) {
                    if (AuthorizerCommonUtil.listEndsWith(value, entityAttributeValues)) {
                        return true;
                    }
                }
            }
            case POLICY_FILTER_CRITERIA_NOT_EQUALS -> {
                return Collections.disjoint(entityAttributeValues, attributeValues);
            }
            case POLICY_FILTER_CRITERIA_IN -> {
                if (AuthorizerCommonUtil.arrayListContains(attributeValues, entityAttributeValues)) {
                    return true;
                }
            }
            case POLICY_FILTER_CRITERIA_NOT_IN -> {
                if (!AuthorizerCommonUtil.arrayListContains(attributeValues, entityAttributeValues)) {
                    return true;
                }
            }
            default -> LOG.warn("Found unknown operator {}", operator);
        }

        RequestContext.get().endMetricRecord(recorder);
        return false;
    }

    private static List<String> handleSpecialAttributes(AtlasEntityHeader entity, String attributeName) {
        List<String> entityAttributeValues = new ArrayList<>();

        switch (attributeName) {
            case "__traitNames" -> {
                List<AtlasClassification> tags = entity.getClassifications();
                if (tags != null) {
                    for (AtlasClassification tag : tags) {
                        if (StringUtils.isEmpty(tag.getEntityGuid()) || tag.getEntityGuid().equals(entity.getGuid())) {
                            entityAttributeValues.add(tag.getTypeName());
                            entityAttributeValues.addAll(extractTagAttachmentValues(tag));
                        }
                    }
                }
            }
            case "__propagatedTraitNames" -> {
                List<AtlasClassification> tags = entity.getClassifications();
                if (tags != null) {
                    for (AtlasClassification tag : tags) {
                        if (StringUtils.isNotEmpty(tag.getEntityGuid()) && !tag.getEntityGuid().equals(entity.getGuid())) {
                            entityAttributeValues.add(tag.getTypeName());
                            entityAttributeValues.addAll(extractTagAttachmentValues(tag));
                        }
                    }
                }
            }
            case "__typeName" -> {
                String typeName = entity.getTypeName();
                Set<String> allValidTypes = AuthorizerCommonUtil.getTypeAndSupertypesList(typeName);
                entityAttributeValues.addAll(allValidTypes);
            }
        }

        return entityAttributeValues;
    }

    @SuppressWarnings("unchecked")
    private static List<String> extractTagAttachmentValues(AtlasClassification tag) {
        String tagTypeName = tag.getTypeName();
        List<String> tagAttachmentValues = new ArrayList<>();

        if (tag.getAttributes() == null || tag.getAttributes().isEmpty()) {
            LOG.warn("ABAC_AUTH: Tag attributes are null or empty, tag={}", tagTypeName);
            return tagAttachmentValues;
        }

        for (String attrName : tag.getAttributes().keySet()) {
            Object attrRawValue = tag.getAttribute(attrName);

            // Skip non-collection attributes (e.g., primitive string attributes on the tag)
            if (!(attrRawValue instanceof Collection<?>)) {
                continue;
            }

            Collection<?> attrValues = (Collection<?>) attrRawValue;
            for (Object element : attrValues) {
                // Tag struct attributes arrive as Map (LinkedHashMap from Jackson / HashMap from TagAttributeMapper)
                // with "typeName" and "attributes" keys, not as AtlasStruct instances
                Map<String, Object> attrValueAttributes = getStructAttributes(element);
                if (attrValueAttributes == null || attrValueAttributes.isEmpty()) {
                    LOG.warn("ABAC_AUTH: Tag attribute value is null, tag={}, attribute={}", tagTypeName, attrName);
                    continue;
                }

                Object sourceTagValueRaw = attrValueAttributes.get("sourceTagValue");
                if (!(sourceTagValueRaw instanceof List<?>)) {
                    LOG.warn("ABAC_AUTH: Tag attribute's sourceTagValue attribute is empty, tag={}, attribute={}.sourceTagValue", tagTypeName, attrName);
                    continue;
                }

                List<?> sourceTagValues = (List<?>) sourceTagValueRaw;
                for (Object item : sourceTagValues) {
                    Map<String, Object> itemAttrs = getStructAttributes(item);
                    if (itemAttrs == null) {
                        continue;
                    }
                    String key = itemAttrs.get("tagAttachmentKey") == null ? "" : itemAttrs.get("tagAttachmentKey").toString();
                    String value = itemAttrs.get("tagAttachmentValue") == null ? "" : itemAttrs.get("tagAttachmentValue").toString();
                    tagAttachmentValues.add(AuthorizerCommonUtil.tagKeyValueRepr(tagTypeName, key, value));
                }
            }
        }

        if (tagAttachmentValues.isEmpty()) {
            tagAttachmentValues.add(tagTypeName + ".="); // to support tag with no attachment values
        }
        LOG.info("ABAC_AUTH: Tag attachment values for tag={} value={}", tagTypeName, tagAttachmentValues);

        return tagAttachmentValues;
    }

    /**
     * Extracts attributes from a struct-like object, handling both Map representations
     * (from Jackson deserialization / TagAttributeMapper) and AtlasStruct instances.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> getStructAttributes(Object obj) {
        if (obj instanceof AtlasStruct struct) {
            return struct.getAttributes();
        }
        if (obj instanceof Map<?, ?> map) {
            Object attributes = map.get("attributes");
            if (attributes instanceof Map<?, ?>) {
                return (Map<String, Object>) attributes;
            }
            // If no "attributes" wrapper, treat the map itself as attributes
            return (Map<String, Object>) map;
        }
        return null;
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

    public static List<String> getRelatedAttributes(String attributeName) {
        List<String> relatedAttributes = new ArrayList<>();

        // Check if attributeName exists in any set and add the entire set
        for (Set<String> attributeSet : relatedAttributeSets) {
            if (attributeSet.contains(attributeName)) {
                relatedAttributes.addAll(attributeSet);
                break;
            }
        }
        if (relatedAttributes.isEmpty()) {
            relatedAttributes.add(attributeName);
        }

        return relatedAttributes;
    }

    private static List<String> getRequiredTagValues(JsonNode tagValueNode) {
        List<String> requiredTagValues = new ArrayList<>();
        if (AuthorizerCommonUtil.isTagKeyValueFormat(tagValueNode)) {
            String tagName = tagValueNode.get("name").asText();

            JsonNode valuesNode = tagValueNode.get("tagValues");
            if (valuesNode != null && valuesNode.isArray()) {
                for (JsonNode valueNode : valuesNode) {
                    String key = valueNode.get("key") == null ? null : valueNode.get("key").asText();
                    String value = valueNode.get("consolidatedValue") == null ? null : valueNode.get("consolidatedValue").asText();
                    requiredTagValues.add(AuthorizerCommonUtil.tagKeyValueRepr(tagName, key, value));
                }
            } else {
                LOG.warn("Invalid tag values format for tag: {}", tagName);
            }

            if (requiredTagValues.isEmpty()) {
                requiredTagValues.add(tagName);
            }
        } else {
            requiredTagValues.add(tagValueNode.asText());
        }
        return requiredTagValues;
    }

    private static boolean evaluateTagFilterCriteria(String attributeName, JsonNode attributeValueNode, String operator, List<String> entityAttributeValues) {
        List<List<String>> attributeValues = new ArrayList<>();
        if (attributeValueNode.isArray()) {
            for (JsonNode node : attributeValueNode) {
                attributeValues.add(getRequiredTagValues(node));
            }
        } else {
            attributeValues.add(getRequiredTagValues(attributeValueNode));
        }

        // no support required for starts_with and ends_with for tags
        boolean result = false;
        switch(operator) {
            case POLICY_FILTER_CRITERIA_EQUALS -> {
                for (List<String> tagValues : attributeValues) {
                    if (!AuthorizerCommonUtil.arrayListContains(entityAttributeValues, tagValues)) {
                        return false;
                    }
                }
                result = true;
            }
            case POLICY_FILTER_CRITERIA_NOT_EQUALS, POLICY_FILTER_CRITERIA_NOT_IN -> {
                for (List<String> tagValues : attributeValues) {
                    if (AuthorizerCommonUtil.arrayListContains(entityAttributeValues, tagValues)) {
                        return false;
                    }
                }
                result = true;
            }
            case POLICY_FILTER_CRITERIA_IN -> {
                for (List<String> tagValues : attributeValues) {
                    if (AuthorizerCommonUtil.arrayListContains(entityAttributeValues, tagValues)) {
                        return true;
                    }
                }
            }
        }
        return result;
    }
}
