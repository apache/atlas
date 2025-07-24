package org.apache.atlas.authorizer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;

public class JsonToElasticsearchQueryTest extends TestCase {
    
    private ObjectMapper mapper = new ObjectMapper();

    public void testConvertJsonToQuery() throws Exception {
        // Test JSON with nested conditions and various operators along with WithRelatedAttributes
        String json = "{\"policyFilterCriteria\":{\"condition\":\"AND\",\"criterion\":[{\"attributeName\":\"__traitNames\",\"attributeValue\":\"test_trait\",\"operator\":\"EQUALS\"},{\"condition\":\"OR\",\"criterion\":[{\"attributeName\":\"ownerUsers\",\"attributeValue\":\"username1\",\"operator\":\"EQUALS\"},{\"attributeName\":\"certificationStatus\",\"attributeValue\":[\"VERIFIED\",\"DRAFT\"],\"operator\":\"IN\"},{\"attributeName\":\"name\",\"attributeValue\":\"test-name\",\"operator\":\"NOT_EQUALS\"}]}]}}";
        
        // Parse JSON and extract policyFilterCriteria
        JsonNode rootNode = mapper.readTree(json);
        JsonNode policyFilterCriteria = rootNode.get("policyFilterCriteria");
        
        // Convert to Elasticsearch query
        JsonNode query = JsonToElasticsearchQuery.convertJsonToQuery(policyFilterCriteria);
        
        // Verify the basic structure
        assertNotNull("Query should not be null", query);
        assertTrue("Query should have 'bool' field", query.has("bool"));
        assertTrue("Query should have 'bool.filter' field", query.get("bool").has("filter"));
        
        JsonNode filterArray = query.get("bool").get("filter");
        assertTrue("Filter should be an array", filterArray.isArray());
        assertEquals("Should have 2 filter conditions (trait + nested OR)", 2, filterArray.size());
        
        // Test the first condition (__traitNames EQUALS test_trait)
        JsonNode firstCondition = filterArray.get(0);
        // Since __traitNames has related attributes, it should be wrapped in an OR query
        assertTrue("First condition should have bool.should for related attributes", 
                  firstCondition.has("bool") && firstCondition.get("bool").has("should"));
        
        // Test the second condition (nested OR)
        JsonNode secondCondition = filterArray.get(1);
        assertTrue("Second condition should be a bool query", secondCondition.has("bool"));
        assertTrue("Second condition should have 'should' for OR logic", secondCondition.get("bool").has("should"));
        
        JsonNode shouldArray = secondCondition.get("bool").get("should");
        assertTrue("Should array must be an array", shouldArray.isArray());
        assertEquals("Should have 3 OR conditions", 3, shouldArray.size());
        
        // Print the generated query for manual inspection
        System.out.println("Generated Elasticsearch Query:");
        System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(query));
    }
    
    public void testSimpleEqualsQuery() throws Exception {
        // Test a simple EQUALS condition
        String json = "{\"condition\":\"AND\",\"criterion\":[{\"attributeName\":\"name\",\"attributeValue\":\"test-name\",\"operator\":\"EQUALS\"}]}";
        
        JsonNode filterCriteria = mapper.readTree(json);
        JsonNode query = JsonToElasticsearchQuery.convertJsonToQuery(filterCriteria);
        
        assertNotNull("Query should not be null", query);
        assertTrue("Query should have 'bool' field", query.has("bool"));
        assertTrue("Query should have 'bool.filter' field", query.get("bool").has("filter"));
        
        JsonNode filterArray = query.get("bool").get("filter");
        assertEquals("Should have 1 filter condition", 1, filterArray.size());
        
        JsonNode condition = filterArray.get(0);
        assertTrue("Should have term query for EQUALS", condition.has("term"));
        assertTrue("Term query should have name field", condition.get("term").has("name"));
        assertEquals("Should match test-name", "test-name", condition.get("term").get("name").asText());
    }
    
    public void testInOperator() throws Exception {
        // Test IN operator with array values
        String json = "{\"condition\":\"AND\",\"criterion\":[{\"attributeName\":\"status\",\"attributeValue\":[\"ACTIVE\",\"VERIFIED\"],\"operator\":\"IN\"}]}";
        
        JsonNode filterCriteria = mapper.readTree(json);
        JsonNode query = JsonToElasticsearchQuery.convertJsonToQuery(filterCriteria);
        
        assertNotNull("Query should not be null", query);
        JsonNode filterArray = query.get("bool").get("filter");
        assertEquals("Should have 1 filter condition", 1, filterArray.size());
        
        JsonNode condition = filterArray.get(0);
        assertTrue("Should have terms query for IN", condition.has("terms"));
        assertTrue("Terms query should have status field", condition.get("terms").has("status"));
        
        JsonNode valuesArray = condition.get("terms").get("status");
        assertTrue("Values should be an array", valuesArray.isArray());
        assertEquals("Should have 2 values", 2, valuesArray.size());
    }
    
    public void testNotEqualsOperator() throws Exception {
        // Test NOT_EQUALS operator
        String json = "{\"condition\":\"AND\",\"criterion\":[{\"attributeName\":\"status\",\"attributeValue\":\"DELETED\",\"operator\":\"NOT_EQUALS\"}]}";
        
        JsonNode filterCriteria = mapper.readTree(json);
        JsonNode query = JsonToElasticsearchQuery.convertJsonToQuery(filterCriteria);
        
        assertNotNull("Query should not be null", query);
        JsonNode filterArray = query.get("bool").get("filter");
        assertEquals("Should have 1 filter condition", 1, filterArray.size());
        
        JsonNode condition = filterArray.get(0);
        assertTrue("Should have bool.must_not for NOT_EQUALS", condition.has("bool"));
        assertTrue("Should have must_not clause", condition.get("bool").has("must_not"));
        
        JsonNode mustNotClause = condition.get("bool").get("must_not");
        assertTrue("Must not should have term query", mustNotClause.has("term"));
        assertEquals("Should exclude DELETED", "DELETED", mustNotClause.get("term").get("status").asText());
    }
}