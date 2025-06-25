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

import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_AND;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_OR;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_EQUALS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NOT_EQUALS;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_STARTS_WITH;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_ENDS_WITH;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_IN;
import static org.apache.atlas.repository.util.AccessControlUtils.POLICY_FILTER_CRITERIA_NOT_IN;


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

                ArrayNode queryArray = ((ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should"));
                JsonNode attributeQuery;
                if (relatedAttributes.size() > 1) { // handle attributes with multiple related attributes
                    attributeQuery = convertConditionToQuery("OR");
                    for (String relatedAttribute : relatedAttributes) {
                        JsonNode relatedAttributeQuery = createAttributeQuery(operator, relatedAttribute, attributeValueNode);
                        ((ArrayNode) attributeQuery.get("bool").get("should")).add(relatedAttributeQuery);
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
        switch (operator) {
            case POLICY_FILTER_CRITERIA_EQUALS:
                queryNode.putObject("term").put(attributeName, attributeValue);
                break;

            case POLICY_FILTER_CRITERIA_NOT_EQUALS:
                queryNode.putObject("bool").putObject("must_not").putObject("term").put(attributeName, attributeValue);
                break;

            case POLICY_FILTER_CRITERIA_STARTS_WITH:
                queryNode.putObject("prefix").put(attributeName, attributeValue + "*");
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
}
