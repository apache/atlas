package org.apache.atlas.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.authorizer.authorizers.EntityAuthorizer;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

import static org.apache.atlas.authorizer.authorizers.AuthorizerCommonUtil.isTagKeyValueFormat;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_AND;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_OR;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_EQUALS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NOT_EQUALS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_STARTS_WITH;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_ENDS_WITH;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_IN;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NOT_IN;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NEGATIVE_OPS;


public class JsonToElasticsearchQuery {
    private static final Logger LOG = LoggerFactory.getLogger(JsonToElasticsearchQuery.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String FILTER_CRITERIA_CONDITION = "condition";

    private static JsonNode convertConditionToQuery(String condition) {
        if (condition.equals(POLICY_FILTER_CRITERIA_AND)) {
            return mapper.createObjectNode().set("bool", mapper.createObjectNode().set("filter", mapper.createArrayNode()));
        } else if (condition.equals(POLICY_FILTER_CRITERIA_OR)) {
            return mapper.createObjectNode()
                    .set("bool", mapper.createObjectNode()
                    .set("should", mapper.createArrayNode()));
        } else {
            throw new IllegalArgumentException("Unsupported condition: " + condition);
        }
    }

    public static JsonNode convertJsonToQuery(JsonNode data) {
        AtlasPerfMetrics.MetricRecorder convertJsonToQueryMetrics = RequestContext.get().startMetricRecord("convertJsonToQuery");
        String condition = data.get("condition").asText();
        JsonNode criterion = data.get("criterion");

        JsonNode query = convertConditionToQuery(condition);

        for (JsonNode crit : criterion) {
            if (crit.has("condition")) {
                JsonNode nestedQuery = convertJsonToQuery(crit);
                if (condition.equals("AND")) {
                    ((ArrayNode) query.get("bool").get("filter")).add(nestedQuery);
                } else {
                    ((ArrayNode) query.get("bool").get("should")).add(nestedQuery);
                }
            } else {
                String operator = crit.get("operator").asText();
                String attributeName = crit.get("attributeName").asText();
                JsonNode attributeValueNode = crit.get("attributeValue");
                
                List<String> relatedAttributes = EntityAuthorizer.getRelatedAttributes(attributeName);

                ArrayNode queryArray = ((ArrayNode) query.get("bool").get(getConditionClause(condition)));
                JsonNode attributeQuery;
                if (relatedAttributes.size() > 1) { // handle attributes with multiple related attributes
                    String relatedAttrCondition = POLICY_FILTER_CRITERIA_NEGATIVE_OPS.contains(operator)
                            ? POLICY_FILTER_CRITERIA_AND
                            : POLICY_FILTER_CRITERIA_OR;
                    attributeQuery = convertConditionToQuery(relatedAttrCondition);
                    for (String relatedAttribute : relatedAttributes) {
                        JsonNode relatedAttributeQuery = createAttributeQuery(operator, relatedAttribute, attributeValueNode);
                        ((ArrayNode) attributeQuery.get("bool").get(getConditionClause(relatedAttrCondition))).add(relatedAttributeQuery);
                    }
                } else {
                    attributeQuery = createAttributeQuery(operator, attributeName, attributeValueNode);
                }

                if (attributeQuery != null) queryArray.add(attributeQuery);
            }
        }
        RequestContext.get().endMetricRecord(convertJsonToQueryMetrics);
        return query;
    }

    private static JsonNode createAttributeQuery(String operator, String attributeName, JsonNode attributeValueNode) {
        ObjectNode queryNode = mapper.createObjectNode();
        String attributeValue = attributeValueNode.asText();

        // handle special attribute value requirement like tag key value
        JsonNode valueToCheck = attributeValueNode.isArray() ? attributeValueNode.get(0) : attributeValueNode;
        if (isTagKeyValueFormat(valueToCheck)) {
            return createQueryWithOperatorForTag(operator, attributeName, attributeValueNode);
        }

        switch (operator) {
            case POLICY_FILTER_CRITERIA_EQUALS:
                if (attributeValueNode.isArray()) {
                    ArrayNode filterArray = queryNode.putObject("bool").putArray("filter");
                    for (JsonNode valueNode : attributeValueNode) {
                        filterArray.addObject().putObject("term").put(attributeName, valueNode.asText());
                    }
                } else {
                    queryNode.putObject("term").put(attributeName, attributeValue);
                }
                break;

            case POLICY_FILTER_CRITERIA_NOT_EQUALS:
                if (attributeValueNode.isArray()) { // same as not_in operator
                    queryNode.putObject("bool").putObject("must_not").putObject("terms").set(attributeName, attributeValueNode);
                } else {
                    queryNode.putObject("bool").putObject("must_not").putObject("term").put(attributeName, attributeValue);
                }
                break;

            case POLICY_FILTER_CRITERIA_STARTS_WITH:
                queryNode.putObject("prefix").put(attributeName, attributeValue);
                break;

            case POLICY_FILTER_CRITERIA_ENDS_WITH:
                queryNode.putObject("wildcard").put(attributeName, "*" + attributeValue);
                break;

            case POLICY_FILTER_CRITERIA_IN:
                queryNode.putObject("terms").set(attributeName, attributeValueNode);
                break;

            case POLICY_FILTER_CRITERIA_NOT_IN:
                queryNode.putObject("bool").putObject("must_not").putObject("terms").set(attributeName, attributeValueNode);
                break;

            default: LOG.warn("Found unknown operator {}", operator);
        }
        return queryNode;
    }

    // Repeating some code for tag key-value pairs query creation to avoid complexity in the main query creation logic
    // This method can potentially be merged with createAttributeQuery if needed
    public static JsonNode createQueryWithOperatorForTag(String operator, String attributeName, JsonNode attributeValueNode) {
        ObjectNode queryNode = mapper.createObjectNode();
        
        if (!isTagKeyValueFormat(attributeValueNode)) {
            return null;
        }

        switch (operator) {
            case POLICY_FILTER_CRITERIA_EQUALS:
                if (attributeValueNode.isArray()) {
                    ArrayNode filterArray = queryNode.putObject("bool").putArray("filter");
                    for (JsonNode valueNode : attributeValueNode) {
                        filterArray.add(createDSLForTagKeyValue(attributeName, valueNode));
                    }
                } else {
                    return createDSLForTagKeyValue(attributeName, attributeValueNode);
                }
                break;

            case POLICY_FILTER_CRITERIA_NOT_EQUALS:
                ObjectNode mustNotNode = queryNode.putObject("bool").putObject("must_not");
                if (attributeValueNode.isArray()) {
                    ArrayNode shouldArray = mustNotNode.putArray("should");
                    for (JsonNode valueNode : attributeValueNode) {
                        shouldArray.add(createDSLForTagKeyValue(attributeName, valueNode));
                    }
                } else {
                    mustNotNode.setAll((ObjectNode) createDSLForTagKeyValue(attributeName, attributeValueNode));
                }
                break;

            case POLICY_FILTER_CRITERIA_IN:
                ArrayNode shouldArray = queryNode.putObject("bool").putArray("should");
                if (attributeValueNode.isArray()) {
                    for (JsonNode valueNode : attributeValueNode) {
                        shouldArray.add(createDSLForTagKeyValue(attributeName, valueNode));
                    }
                } else {
                    shouldArray.add(createDSLForTagKeyValue(attributeName, attributeValueNode));
                }
                break;

            case POLICY_FILTER_CRITERIA_NOT_IN:
                ObjectNode notInMustNot = queryNode.putObject("bool").putObject("must_not");
                ArrayNode notInShouldArray = notInMustNot.putArray("should");
                if (attributeValueNode.isArray()) {
                    for (JsonNode valueNode : attributeValueNode) {
                        notInShouldArray.add(createDSLForTagKeyValue(attributeName, valueNode));
                    }
                } else {
                    notInShouldArray.add(createDSLForTagKeyValue(attributeName, attributeValueNode));
                }
                break;

            default: LOG.warn("Found unknown operator {}", operator);
        }
        return queryNode;
    }

    /*
        This should produce something like
        {
            "bool": {
                "filter": [
                    {
                        "term": {"__traitNames": "tag"}
                    },
                    {
                        "span_near": {
                            "clauses": [
                                {"span_term": {"__classificationsText.text": "tagAttachmentValue"}},
                                {"span_term": {"__classificationsText.text": "value"}},
                                {"span_term": {"__classificationsText.text": "tagAttachmentKey"}}
                            ],
                            "in_order": true,
                            "slop": 0
                        }
                    }
                ]
            }
        }
     */
    public static JsonNode createDSLForTagKeyValue(String attributeName, JsonNode tagKeyValue) {
        String tag = tagKeyValue.get("tag").asText();
        String key = tagKeyValue.get("key").asText();
        String value = tagKeyValue.get("value").asText();

        ObjectNode queryNode = mapper.createObjectNode();
        ArrayNode filterArray = queryNode.putObject("bool").putArray("filter");

        // Add term query for tag name match
        ObjectNode tagTermQuery = mapper.createObjectNode();
        tagTermQuery.putObject("term").put(attributeName, tag);
        filterArray.add(tagTermQuery);

        // Add span_near query for key-value pair
        ArrayNode clausesArray = mapper.createArrayNode();

        // Create span_term for left side of the tag
        ObjectNode tagClause = mapper.createObjectNode();
        ObjectNode tagSpanTerm = mapper.createObjectNode();
        tagSpanTerm.put("__classificationsText.text", "tagAttachmentValue");
        tagClause.set("span_term", tagSpanTerm);
        clausesArray.add(tagClause);
        
        // Create span_term for value
        ObjectNode keyClause = mapper.createObjectNode();
        ObjectNode keySpanTerm = mapper.createObjectNode();
        keySpanTerm.put("__classificationsText.text", value);
        keyClause.set("span_term", keySpanTerm);
        clausesArray.add(keyClause);
        
        // Create span_term for right side of the tag
        ObjectNode valueClause = mapper.createObjectNode();
        ObjectNode valueSpanTerm = mapper.createObjectNode();
        valueSpanTerm.put("__classificationsText.text", "tagAttachmentKey");
        valueClause.set("span_term", valueSpanTerm);
        clausesArray.add(valueClause);

        // Skipping clause for key to keep the DSL consistent with the FE query
        
        ObjectNode spanNearNode = mapper.createObjectNode();
        spanNearNode.set("clauses", clausesArray);
        spanNearNode.put("in_order", true);
        spanNearNode.put("slop", 0);
        
        ObjectNode spanNearQuery = mapper.createObjectNode();
        spanNearQuery.set("span_near", spanNearNode);
        filterArray.add(spanNearQuery);
        
        return queryNode;
    }

    public static JsonNode parseFilterJSON(String policyFilterCriteria, String rootKey) {
        JsonNode filterCriteriaNode = null;
        if (!StringUtils.isEmpty(policyFilterCriteria)) {
            try {
                filterCriteriaNode = mapper.readTree(policyFilterCriteria);
            } catch (JsonProcessingException e) {
                LOG.error("ABAC_AUTH: parsing filterCriteria failed, filterCriteria={}", policyFilterCriteria);
            }
        }

        if (filterCriteriaNode != null) {
            return StringUtils.isNotEmpty(rootKey) ? filterCriteriaNode.get(rootKey) : filterCriteriaNode;
        }
        return null;
    }

    private static String getConditionClause(String condition) {
        return POLICY_FILTER_CRITERIA_AND.equals(condition) ? "filter" : "should";
    }
}
