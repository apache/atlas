package org.apache.atlas.discovery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for new optimization rules added in MS-720:
 * - TautologyElimination: should[NOT X, X] → remove
 * - ShouldMustNotRewrite: should[must_not[A], must[A,B]] → must_not[A AND NOT B]
 * - HighlightRemoval: remove highlight when no text query
 * - MustNotTermsMerging: merge duplicate must_not terms on same field
 */
public class ElasticsearchDslOptimizerNewRulesTest {

    private ElasticsearchDslOptimizer optimizer;
    private ObjectMapper mapper;

    @BeforeEach
    public void setUp() {
        optimizer = ElasticsearchDslOptimizer.getInstance();
        mapper = new ObjectMapper();
    }

    // =========================================================================
    // TautologyElimination tests
    // =========================================================================

    @Test
    public void testTautologyElimination_shouldNotXX_removed() throws Exception {
        // should[must_not[DQR], filter[DQR]] is a tautology — every doc matches
        String input = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"bool\": {\n" +
                "          \"must\": [\n" +
                "            { \"term\": { \"__state\": \"ACTIVE\" } },\n" +
                "            {\n" +
                "              \"bool\": {\n" +
                "                \"should\": [\n" +
                "                  { \"bool\": { \"must_not\": [{ \"term\": { \"__typeName.keyword\": \"DataQualityRule\" } }] } },\n" +
                "                  { \"bool\": { \"filter\": { \"term\": { \"__typeName.keyword\": \"DataQualityRule\" } } } }\n" +
                "                ]\n" +
                "              }\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ElasticsearchDslOptimizer.OptimizationResult result = optimizer.optimizeQuery(input);
        JsonNode optimized = mapper.readTree(result.getOptimizedQuery());

        // The tautological should clause should be removed from the must array
        assertTrue(result.getMetrics().appliedRules.contains("TautologyElimination"),
                "TautologyElimination rule should have been applied");

        // __state ACTIVE should still be present
        String optimizedStr = result.getOptimizedQuery();
        assertTrue(optimizedStr.contains("ACTIVE"), "ACTIVE filter should be preserved");

        // DataQualityRule tautology should be gone
        assertFalse(optimizedStr.contains("DataQualityRule"),
                "DataQualityRule tautology should be removed");
    }

    @Test
    public void testTautologyElimination_nonTautology_preserved() throws Exception {
        // should with 3 clauses is NOT a tautology
        String input = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"bool\": {\n" +
                "            \"should\": [\n" +
                "              { \"term\": { \"type\": \"A\" } },\n" +
                "              { \"term\": { \"type\": \"B\" } },\n" +
                "              { \"term\": { \"type\": \"C\" } }\n" +
                "            ]\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ElasticsearchDslOptimizer.OptimizationResult result = optimizer.optimizeQuery(input);
        JsonNode optimized = mapper.readTree(result.getOptimizedQuery());

        // Should still contain all three terms
        String optimizedStr = result.getOptimizedQuery();
        assertTrue(optimizedStr.contains("\"A\""), "Term A should be preserved");
        assertTrue(optimizedStr.contains("\"B\""), "Term B should be preserved");
        assertTrue(optimizedStr.contains("\"C\""), "Term C should be preserved");
    }

    // =========================================================================
    // ShouldMustNotRewrite tests
    // =========================================================================

    @Test
    public void testShouldMustNotRewrite_procedurePattern() throws Exception {
        // should[must_not[Procedure], must[Procedure, snowflake/mssql]]
        // → must_not[Procedure AND NOT(snowflake/mssql)]
        String input = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"bool\": {\n" +
                "          \"must\": [\n" +
                "            { \"term\": { \"__state\": \"ACTIVE\" } },\n" +
                "            {\n" +
                "              \"bool\": {\n" +
                "                \"should\": [\n" +
                "                  { \"bool\": { \"must_not\": [{ \"term\": { \"__typeName.keyword\": \"Procedure\" } }] } },\n" +
                "                  {\n" +
                "                    \"bool\": {\n" +
                "                      \"must\": [\n" +
                "                        { \"term\": { \"__typeName.keyword\": \"Procedure\" } },\n" +
                "                        { \"terms\": { \"connectorName\": [\"snowflake\", \"mssql\"] } }\n" +
                "                      ]\n" +
                "                    }\n" +
                "                  }\n" +
                "                ]\n" +
                "              }\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ElasticsearchDslOptimizer.OptimizationResult result = optimizer.optimizeQuery(input);
        JsonNode optimized = mapper.readTree(result.getOptimizedQuery());

        assertTrue(result.getMetrics().appliedRules.contains("ShouldMustNotRewrite"),
                "ShouldMustNotRewrite rule should have been applied. Applied: " + result.getMetrics().appliedRules);

        // The rewritten query should have must_not containing the Procedure term
        String optimizedStr = result.getOptimizedQuery();
        assertTrue(optimizedStr.contains("must_not"), "Should contain must_not");
        assertTrue(optimizedStr.contains("Procedure"), "Should still reference Procedure");
        assertTrue(optimizedStr.contains("snowflake"), "Should still reference snowflake");
        assertTrue(optimizedStr.contains("mssql"), "Should still reference mssql");

        // Should NOT have a "should" clause anymore (the whole pattern was rewritten)
        assertFalse(optimizedStr.contains("\"should\""),
                "Should clause should be rewritten to must_not pattern");
    }

    // =========================================================================
    // HighlightRemoval tests
    // =========================================================================

    @Test
    public void testHighlightRemoval_noTextQuery() throws Exception {
        String input = "{\n" +
                "  \"size\": 20,\n" +
                "  \"highlight\": {\n" +
                "    \"fields\": { \"name\": {}, \"displayName\": {} }\n" +
                "  },\n" +
                "  \"query\": {\n" +
                "    \"function_score\": {\n" +
                "      \"query\": {\n" +
                "        \"bool\": {\n" +
                "          \"filter\": [\n" +
                "            { \"term\": { \"__state\": \"ACTIVE\" } }\n" +
                "          ]\n" +
                "        }\n" +
                "      },\n" +
                "      \"functions\": [\n" +
                "        { \"filter\": { \"term\": { \"__typeName.keyword\": \"Table\" } }, \"weight\": 5 }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ElasticsearchDslOptimizer.OptimizationResult result = optimizer.optimizeQuery(input);

        assertTrue(result.getMetrics().appliedRules.contains("HighlightRemoval"),
                "HighlightRemoval rule should have been applied");

        assertFalse(result.getOptimizedQuery().contains("highlight"),
                "Highlight should be removed when no text query present");
    }

    @Test
    public void testHighlightRemoval_preservedWithMatchQuery() throws Exception {
        String input = "{\n" +
                "  \"size\": 20,\n" +
                "  \"highlight\": {\n" +
                "    \"fields\": { \"name\": {} }\n" +
                "  },\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must\": [\n" +
                "        { \"match\": { \"name\": \"customer_table\" } }\n" +
                "      ],\n" +
                "      \"filter\": [\n" +
                "        { \"term\": { \"__state\": \"ACTIVE\" } }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ElasticsearchDslOptimizer.OptimizationResult result = optimizer.optimizeQuery(input);

        assertTrue(result.getOptimizedQuery().contains("highlight"),
                "Highlight should be preserved when match query is present");
    }

    @Test
    public void testHighlightRemoval_preservedWithNestedMatch() throws Exception {
        // Match query nested inside function_score
        String input = "{\n" +
                "  \"size\": 20,\n" +
                "  \"highlight\": {\n" +
                "    \"fields\": { \"name\": {} }\n" +
                "  },\n" +
                "  \"query\": {\n" +
                "    \"function_score\": {\n" +
                "      \"query\": {\n" +
                "        \"bool\": {\n" +
                "          \"must\": [\n" +
                "            { \"match\": { \"name\": \"test\" } }\n" +
                "          ]\n" +
                "        }\n" +
                "      },\n" +
                "      \"functions\": []\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ElasticsearchDslOptimizer.OptimizationResult result = optimizer.optimizeQuery(input);

        assertTrue(result.getOptimizedQuery().contains("highlight"),
                "Highlight should be preserved when match query is nested in function_score");
    }

    // =========================================================================
    // MustNotTermsMerging tests
    // =========================================================================

    @Test
    public void testMustNotTermsMerging_sameField() throws Exception {
        String input = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": [\n" +
                "        { \"term\": { \"__state\": \"ACTIVE\" } }\n" +
                "      ],\n" +
                "      \"must_not\": [\n" +
                "        { \"terms\": { \"__typeName.keyword\": [\"DbtColumnProcess\", \"BIProcess\"] } },\n" +
                "        { \"terms\": { \"__typeName.keyword\": [\"MCIncident\", \"AnomaloCheck\"] } }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ElasticsearchDslOptimizer.OptimizationResult result = optimizer.optimizeQuery(input);
        JsonNode optimized = mapper.readTree(result.getOptimizedQuery());

        // The terms should be merged (by either MustNotTermsMerging or MultipleTermsConsolidation)
        JsonNode mustNot = optimized.at("/query/bool/must_not");
        assertTrue(mustNot.isArray(), "must_not should be an array");
        assertEquals(1, mustNot.size(), "Should have exactly 1 merged terms entry");

        JsonNode mergedTerms = mustNot.get(0).get("terms").get("__typeName.keyword");
        assertEquals(4, mergedTerms.size(), "Should have all 4 type names merged");
    }

    @Test
    public void testMustNotTermsMerging_differentFields_preserved() throws Exception {
        String input = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must_not\": [\n" +
                "        { \"terms\": { \"__typeName.keyword\": [\"Process\"] } },\n" +
                "        { \"terms\": { \"connectorName\": [\"bigquery\"] } }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ElasticsearchDslOptimizer.OptimizationResult result = optimizer.optimizeQuery(input);
        JsonNode optimized = mapper.readTree(result.getOptimizedQuery());

        // Different fields should NOT be merged — should still have 2 entries
        JsonNode mustNot = optimized.at("/query/bool/must_not");
        assertTrue(mustNot.isArray(), "must_not should be an array");
        assertEquals(2, mustNot.size(), "Different field terms should not be merged");
    }

    @Test
    public void testMustNotTermsMerging_deduplicatesValues() throws Exception {
        String input = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"must_not\": [\n" +
                "        { \"terms\": { \"__typeName.keyword\": [\"A\", \"B\"] } },\n" +
                "        { \"terms\": { \"__typeName.keyword\": [\"B\", \"C\"] } }\n" +
                "      ]\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ElasticsearchDslOptimizer.OptimizationResult result = optimizer.optimizeQuery(input);
        JsonNode optimized = mapper.readTree(result.getOptimizedQuery());

        JsonNode mergedTerms = optimized.at("/query/bool/must_not/0/terms/__typeName.keyword");
        assertEquals(3, mergedTerms.size(), "Should deduplicate: A, B, C (not A, B, B, C)");
    }

    // =========================================================================
    // Full customer DSL test (combines all optimizations)
    // =========================================================================

    @Test
    public void testFullCustomerDsl_allOptimizationsApplied() throws Exception {
        // Minimal reproduction of the customer query with all patterns
        String input = "{\n" +
                "  \"from\": 0,\n" +
                "  \"size\": 20,\n" +
                "  \"track_total_hits\": true,\n" +
                "  \"highlight\": {\n" +
                "    \"fields\": { \"name\": {}, \"displayName\": {} }\n" +
                "  },\n" +
                "  \"query\": {\n" +
                "    \"function_score\": {\n" +
                "      \"query\": {\n" +
                "        \"bool\": {\n" +
                "          \"filter\": {\n" +
                "            \"bool\": {\n" +
                "              \"must\": [\n" +
                "                { \"term\": { \"connectionQualifiedName\": \"default/bigquery/123\" } },\n" +
                "                { \"term\": { \"__state\": \"ACTIVE\" } },\n" +
                "                {\n" +
                "                  \"bool\": {\n" +
                "                    \"should\": [\n" +
                "                      { \"bool\": { \"must_not\": [{ \"term\": { \"__typeName.keyword\": \"Procedure\" } }] } },\n" +
                "                      {\n" +
                "                        \"bool\": {\n" +
                "                          \"must\": [\n" +
                "                            { \"term\": { \"__typeName.keyword\": \"Procedure\" } },\n" +
                "                            { \"terms\": { \"connectorName\": [\"snowflake\", \"mssql\"] } }\n" +
                "                          ]\n" +
                "                        }\n" +
                "                      }\n" +
                "                    ]\n" +
                "                  }\n" +
                "                },\n" +
                "                {\n" +
                "                  \"bool\": {\n" +
                "                    \"should\": [\n" +
                "                      { \"bool\": { \"must_not\": [{ \"term\": { \"__typeName.keyword\": \"DataQualityRule\" } }] } },\n" +
                "                      { \"bool\": { \"filter\": { \"term\": { \"__typeName.keyword\": \"DataQualityRule\" } } } }\n" +
                "                    ]\n" +
                "                  }\n" +
                "                }\n" +
                "              ],\n" +
                "              \"must_not\": [\n" +
                "                { \"terms\": { \"__typeName.keyword\": [\"DbtColumnProcess\", \"BIProcess\"] } },\n" +
                "                { \"terms\": { \"__typeName.keyword\": [\"MCIncident\", \"AnomaloCheck\"] } }\n" +
                "              ],\n" +
                "              \"minimum_should_match\": 1\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      },\n" +
                "      \"functions\": [\n" +
                "        { \"filter\": { \"terms\": { \"__typeName.keyword\": [\"Table\"] } }, \"weight\": 5 }\n" +
                "      ],\n" +
                "      \"boost_mode\": \"sum\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        ElasticsearchDslOptimizer.OptimizationResult result = optimizer.optimizeQuery(input);
        String optimized = result.getOptimizedQuery();

        // Verify key new rules were applied
        assertTrue(result.getMetrics().appliedRules.contains("TautologyElimination"),
                "TautologyElimination should fire (DQR tautology). Applied: " + result.getMetrics().appliedRules);
        assertTrue(result.getMetrics().appliedRules.contains("ShouldMustNotRewrite"),
                "ShouldMustNotRewrite should fire (Procedure pattern). Applied: " + result.getMetrics().appliedRules);
        assertTrue(result.getMetrics().appliedRules.contains("HighlightRemoval"),
                "HighlightRemoval should fire (no text query). Applied: " + result.getMetrics().appliedRules);
        // MustNotTermsMerging may be handled by MultipleTermsConsolidation (which runs earlier)

        // DataQualityRule tautology should be gone
        assertFalse(optimized.contains("DataQualityRule"),
                "DataQualityRule tautology should be eliminated");

        // Highlight should be removed (no text query)
        assertFalse(optimized.contains("highlight"),
                "Highlight should be removed");

        // Core filters should be preserved
        assertTrue(optimized.contains("ACTIVE"), "ACTIVE filter preserved");
        assertTrue(optimized.contains("default/bigquery/123"), "Connection filter preserved");
        assertTrue(optimized.contains("Procedure"), "Procedure reference preserved");
        assertTrue(optimized.contains("snowflake"), "snowflake reference preserved");
        assertTrue(optimized.contains("function_score"), "function_score preserved");

        // Verify valid JSON
        JsonNode parsed = mapper.readTree(optimized);
        assertNotNull(parsed, "Optimized query should be valid JSON");
        assertTrue(parsed.has("query"), "Should have query element");
        assertTrue(parsed.has("size"), "Should preserve size");
        assertTrue(parsed.has("from"), "Should preserve from");
    }
}
