package org.apache.atlas.authorizer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.atlas.RequestContext;
import org.apache.atlas.utils.AtlasPerfMetrics;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonToElasticsearchQuery {
    private static final Logger LOG = LoggerFactory.getLogger(JsonToElasticsearchQuery.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static JsonNode convertConditionToQuery(String condition, JsonNode criterion) {
        if (condition.equals("AND")) {
            return mapper.createObjectNode().set("bool", mapper.createObjectNode().set("filter", mapper.createArrayNode()));
        } else if (condition.equals("OR")) {
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

        JsonNode query = convertConditionToQuery(condition, criterion);

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
                String attributeValue = crit.get("attributeValue").asText();

                switch (operator) {
                    case "EQUALS":
                        ObjectNode termNode = ((ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                        termNode.putObject("term").put(attributeName, attributeValue);
                        break;

                    case "NOT_EQUALS":
                        termNode = ((ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                        termNode.putObject("bool").putObject("must_not").putObject("term").put(attributeName, attributeValue);
                        break;

                    case "STARTS_WITH":
                        ObjectNode wildcardNode = ((ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                        wildcardNode.putObject("prefix").put(attributeName, attributeValue + "*");
                        break;

                    case "ENDS_WITH":
                        wildcardNode = ((ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                        wildcardNode.putObject("wildcard").put(attributeName, "*" + attributeValue);
                        break;

                    case "IN":
                        ObjectNode termsNode = ((ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                        termsNode.putObject("terms").set(attributeName, crit.get("attributeValue"));
                        break;

                    case "NOT_IN":
                        termsNode = ((ArrayNode) query.get("bool").get(condition.equals("AND") ? "filter" : "should")).addObject();
                        termsNode.putObject("bool").putObject("must_not").putObject("terms").put(attributeName, crit.get("attributeValue"));
                        break;

                    default: LOG.warn("Found unknown operator {}", operator);
                }
            }
        }
        RequestContext.get().endMetricRecord(convertJsonToQueryMetrics);
        return query;
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
            return filterCriteriaNode.get(rootKey);
        }
        return null;
    }
}
