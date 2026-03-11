package org.apache.atlas.discovery;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Generic Elasticsearch DSL Query Optimizer
 *
 * Optimizes queries through multiple optimization pipelines.
 * Reports only rules that actually help optimize (make changes).
 *
 * Available optimization rules:
 * 0. Structure simplification (flatten nested bools)
 * 1. Empty bool elimination
 * 2. Nested bool elimination (flatten structures first)
 * 3. QualifiedName hierarchy optimization (UI contains + default/* patterns) - RUNS EARLY
 * 4. Multiple terms consolidation (should/filter/must_not only, NOT must)
 * 5. Array deduplication
 * 6. Multi-match consolidation (removed it to not worry about boosting)
 * 7. Regexp simplification
 * 8. Aggregation optimization
 * 9. Multiple terms consolidation (should/filter/must_not only, NOT must)
 * 10. Wildcard consolidation (2+ wildcards, with 1000-char regexp splitting)
 * 11. Regexp consolidation (consolidate regexp patterns after wildcard conversion)
 * 12. Must_not consolidation (consolidate bool.must_not wrappers in must arrays)
 * 13. Filter structure optimization (scoring-safe)
 * 14. Bool flattening (must+must_not to filter+must_not)
 * 15. Duplicate removal and consolidation
 * 16. Context-aware filter optimization (safe must->filter in function_score)
 * 17. Function score optimization
 * 18. Duplicate filter removal
 *
 * Performance Features:
 * - UI "Contains" optimization: *default/path* → __qualifiedNameHierarchy term query (MAJOR)
 * - Must_not consolidation with semantic preservation (De Morgan's Law)
 * - Wildcard to regexp consolidation (2+ wildcards → single regexp)
 * - Only reports rules that made actual changes
 * - Detects queries that don't need Elasticsearch execution
 * - Splits large regexp patterns to avoid 1000-char limits
 * - Optimizes default/* patterns to __qualifiedNameHierarchy
 */
public class ElasticsearchDslOptimizer {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchDslOptimizer.class);
    private final ObjectMapper objectMapper;
    private final List<OptimizationRule> optimizationRules;
    private final OptimizationMetrics metrics;
    private static final ElasticsearchDslOptimizer INSTANCE = new ElasticsearchDslOptimizer();

    /**
     * Checks if a wildcard pattern or field value has special characters or flags that should prevent consolidation
     * @param pattern The pattern string to check
     * @param fieldValue The JsonNode containing the field value (for checking flags)
     * @return true if consolidation should be skipped
     */
    private static boolean hasSpecialCharacters(String pattern, JsonNode fieldValue) {
        // Check for case-insensitive flag or other flags
        if (fieldValue != null && fieldValue.isObject() && 
            (fieldValue.has("case_insensitive") || fieldValue.has("flags"))) {
            return true;
        }

        // Check for null or empty pattern
        if (pattern == null || pattern.isEmpty()) {
            return false;
        }
        
        // Check for special patterns like "?*" that have specific meaning
        if (pattern.contains("?*") || pattern.contains("*?")) {
            return true;
        }
        
        // Check for multiple consecutive special characters
        boolean lastWasSpecial = false;
        for (char c : pattern.toCharArray()) {
            boolean isSpecial = (c == '?' || c == '*');
            if (isSpecial && lastWasSpecial) {
                return true;
            }
            lastWasSpecial = isSpecial;
        }
        
        return false;
    }

    public ElasticsearchDslOptimizer() {
        this.objectMapper = new ObjectMapper();
        this.optimizationRules = Arrays.asList(
                new StructureSimplificationRule(),
                new EmptyBoolEliminationRule(),
                new NestedBoolEliminationRule(), // Flatten structures first
                new QualifiedNameHierarchyRule(), // Move EARLY - before other rules interfere
                new MultipleTermsConsolidationRule(),
                new ArrayDeduplicationRule(),
                new RegexpSimplificationRule(),
                new AggregationOptimizationRule(),
                new MultipleTermsConsolidationRule(), // Second consolidation after hierarchy conversion
                new WildcardConsolidationRule(), // Now runs after hierarchy transformation
                new RegexpConsolidationRule(), // Consolidate regexp patterns after wildcard conversion
                new MustNotConsolidationRule(), // Consolidate bool.must_not wrappers in must arrays
                new FilterStructureOptimizationRule(),
                new BoolFlatteningRule(),
                new DuplicateRemovalRule(),
                new FilterContextRule(),
                new FunctionScoreOptimizationRule(),
                new DuplicateFilterRemovalRule()
        );
        this.metrics = new OptimizationMetrics();
    }

    public static ElasticsearchDslOptimizer getInstance() {
        return INSTANCE;
    }

    /**
     * Rule 16: Nested Bool Elimination
     * Eliminates redundant nested bool wrappers - handles both object and array filter types
     * Runs as final optimization to prevent interference from other rules
     */
    private class NestedBoolEliminationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "NestedBoolElimination";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::eliminateNestedBools);
        }

        private JsonNode eliminateNestedBools(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                // Handle bool.filter.bool pattern - filter can be object or array
                if (boolNode.has("filter")) {
                    JsonNode filterNode = boolNode.get("filter");

                    // Case 1: filter is an object with nested bool
                    if (filterNode.isObject() && filterNode.has("bool")) {
                        JsonNode innerBool = filterNode.get("bool");

                        // If outer bool only has filter and inner bool only has filter, merge them
                        if (boolNode.size() == 1 && innerBool.has("filter") && innerBool.size() == 1) {
                            JsonNode innerFilter = innerBool.get("filter");
                            boolNode.set("filter", innerFilter);
                        }
                        // If inner bool has multiple clauses, promote them to outer level
                        else if (innerBool.has("filter")) {
                            JsonNode innerFilter = innerBool.get("filter");
                            boolNode.set("filter", innerFilter);

                            // Promote other clauses from inner bool to outer bool
                            for (String clause : Arrays.asList("must", "should", "must_not", "minimum_should_match")) {
                                if (innerBool.has(clause)) {
                                    boolNode.set(clause, innerBool.get(clause));
                                }
                            }
                        }
                    }
                    // Case 2: filter is an array - check if any items are redundant bool wrappers
                    else if (filterNode.isArray()) {
                        ArrayNode filterArray = (ArrayNode) filterNode;
                        ArrayNode optimizedFilterArray = eliminateNestedBoolsInArray(filterArray);
                        if (!optimizedFilterArray.equals(filterArray)) {
                            boolNode.set("filter", optimizedFilterArray);
                        }
                    }
                }

                // Also handle other clause types that might have nested bools
                for (String clause : Arrays.asList("must", "should", "must_not")) {
                    if (boolNode.has(clause) && boolNode.get(clause).isArray()) {
                        ArrayNode clauseArray = (ArrayNode) boolNode.get(clause);
                        ArrayNode optimizedArray = eliminateNestedBoolsInArray(clauseArray);
                        if (!optimizedArray.equals(clauseArray)) {
                            boolNode.set(clause, optimizedArray);
                        }
                    }
                }
            }

            return objectNode;
        }

        private ArrayNode eliminateNestedBoolsInArray(ArrayNode array) {
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode item : array) {
                if (item.has("bool")) {
                    JsonNode boolContent = item.get("bool");

                    // If bool has only a single should clause with one item, flatten it
                    if (boolContent.has("should") && boolContent.size() == 1) {
                        JsonNode shouldClause = boolContent.get("should");
                        if (shouldClause.isArray() && shouldClause.size() == 1) {
                            result.add(shouldClause.get(0));
                            continue;
                        }
                    }

                    // If bool has only a single must clause with one item, flatten it
                    if (boolContent.has("must") && boolContent.size() == 1) {
                        JsonNode mustClause = boolContent.get("must");
                        if (mustClause.isArray() && mustClause.size() == 1) {
                            result.add(mustClause.get(0));
                            continue;
                        }
                    }

                    // If bool has only a single filter clause, extract its contents
                    if (boolContent.has("filter") && boolContent.size() == 1) {
                        JsonNode filterClause = boolContent.get("filter");
                        if (filterClause.isArray()) {
                            // Add all items from the filter array
                            for (JsonNode filterItem : filterClause) {
                                result.add(filterItem);
                            }
                            continue;
                        } else {
                            // Single filter item
                            result.add(filterClause);
                            continue;
                        }
                    }
                }

                result.add(item);
            }

            return result;
        }
    }

    /**
     * Rule 10: Filter Structure Optimization - SCORING-SAFE VERSION
     * Reorganizes nested filter structures into cleaner arrays
     * CRITICAL FIX: Preserves all existing filters when consolidating bool clauses
     * SCORING PRESERVATION: Never moves must clauses to filter context as this breaks scoring semantics
     */
    private class FilterStructureOptimizationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "FilterStructureOptimization";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeFilterStructure);
        }

        private JsonNode optimizeFilterStructure(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                // CRITICAL FIX: Handle bool queries with both 'filter' and 'must' clauses
                if (boolNode.has("filter") || boolNode.has("must")) {
                    ArrayNode consolidatedFilters = objectMapper.createArrayNode();

                    // Add existing filter clauses
                    if (boolNode.has("filter")) {
                        JsonNode filterNode = boolNode.get("filter");
                        if (filterNode.isArray()) {
                            // Filter is already an array, add all items
                            for (JsonNode filterItem : filterNode) {
                                consolidatedFilters.add(filterItem);
                            }
                        } else if (filterNode.isObject()) {
                            // Filter is an object, add it directly
                            consolidatedFilters.add(filterNode);
                        }
                    }

                    // IMPORTANT: Do NOT move must clauses to filter context as this breaks scoring semantics
                    // Must clauses contribute to document scoring while filter clauses do not
                    // Preserve must clauses in their original context to maintain search relevance

                    // Set the consolidated filter array
                    if (consolidatedFilters.size() > 0) {
                        boolNode.set("filter", consolidatedFilters);
                    }

                    // Preserve other bool clauses (should, must_not, minimum_should_match)
                    // These remain unchanged
                }

                // Handle nested bool structures in filter objects
                if (boolNode.has("filter") && boolNode.get("filter").isObject()) {
                    JsonNode filterObj = boolNode.get("filter");

                    if (filterObj.has("bool")) {
                        // Extract all clauses from nested bool and flatten them
                        JsonNode nestedBool = filterObj.get("bool");
                        ArrayNode flattenedFilters = extractAllClausesFromBool(nestedBool);

                        if (flattenedFilters.size() > 0) {
                            boolNode.set("filter", flattenedFilters);
                        }
                    }
                }
            }

            return objectNode;
        }

        /**
         * CRITICAL FIX: Extract ALL clauses from a bool query, preserving semantics
         */
        private ArrayNode extractAllClausesFromBool(JsonNode boolNode) {
            ArrayNode result = objectMapper.createArrayNode();

            // Extract must clauses (these become filters in filter context)
            if (boolNode.has("must")) {
                JsonNode mustNode = boolNode.get("must");
                if (mustNode.isArray()) {
                    for (JsonNode mustItem : mustNode) {
                        result.add(mustItem);
                    }
                } else {
                    result.add(mustNode);
                }
            }

            // Extract filter clauses
            if (boolNode.has("filter")) {
                JsonNode filterNode = boolNode.get("filter");
                if (filterNode.isArray()) {
                    for (JsonNode filterItem : filterNode) {
                        result.add(filterItem);
                    }
                } else {
                    result.add(filterNode);
                }
            }

            // Handle should clauses - these need special treatment
            if (boolNode.has("should")) {
                ObjectNode shouldWrapper = objectMapper.createObjectNode();
                ObjectNode shouldBool = objectMapper.createObjectNode();
                shouldBool.set("should", boolNode.get("should"));

                // Preserve minimum_should_match if present
                if (boolNode.has("minimum_should_match")) {
                    shouldBool.set("minimum_should_match", boolNode.get("minimum_should_match"));
                }

                shouldWrapper.set("bool", shouldBool);
                result.add(shouldWrapper);
            }

            // Handle must_not clauses
            if (boolNode.has("must_not")) {
                ObjectNode mustNotWrapper = objectMapper.createObjectNode();
                ObjectNode mustNotBool = objectMapper.createObjectNode();
                mustNotBool.set("must_not", boolNode.get("must_not"));
                mustNotWrapper.set("bool", mustNotBool);
                result.add(mustNotWrapper);
            }

            return result;
        }
    }

    /**
     * Main optimization method - applies all optimization rules
     */
    public OptimizationResult optimizeQuery(String queryJson) {
        long startTime = System.currentTimeMillis();
        String queryHash = String.valueOf(queryJson.hashCode());

        log.debug("Starting query optimization for query hash: {}", queryHash);

        try {
            JsonNode query = objectMapper.readTree(queryJson);
            JsonNode originalQuery = query.deepCopy();

            metrics.startOptimization(query);

            // Apply optimization rules in sequence
            boolean shouldSkipExecution = false;
            String skipReason = null;

            for (OptimizationRule rule : optimizationRules) {
                JsonNode beforeRule = query.deepCopy();
                query = rule.apply(query);

                // IMPROVED: Only record rules that actually made changes
                if (!beforeRule.equals(query)) {
                    metrics.recordRuleApplication(rule.getName());
                    log.debug("Rule '{}' OPTIMIZED query for hash: {} (made changes)", rule.getName(), queryHash);
                } else {
                    log.debug("Rule '{}' applied but made no changes for hash: {}", rule.getName(), queryHash);
                }
            }

            // CRITICAL: Validate no invalid bool->bool nesting was created
            validateNoBoolNesting(query);

            String optimizedQueryJson = objectMapper.writeValueAsString(query);
            OptimizationMetrics.Result result = metrics.finishOptimization(originalQuery, query);

            long totalTime = System.currentTimeMillis() - startTime;

            // Log optimization results
            log.info("Query optimization completed for hash: {} in {}ms", queryHash, totalTime);
            log.info("Size reduction: {}%, Nesting reduction: {}%",
                    String.format("%.1f", result.getSizeReduction()),
                    String.format("%.1f", result.getNestingReduction()));
            if (result.appliedRules.isEmpty()) {
                log.info("No optimization rules helped (query already optimal)");
            } else {
                log.info("Rules that helped optimize: {}", String.join(", ", result.appliedRules));
            }

            // Add comprehensive optimization metrics to MDC
            MDC.put("optimization.query_hash", queryHash);
            MDC.put("optimization.original_size", String.valueOf(result.originalSize));
            MDC.put("optimization.optimized_size", String.valueOf(result.optimizedSize));
            MDC.put("optimization.original_nesting", String.valueOf(result.originalNesting));
            MDC.put("optimization.optimized_nesting", String.valueOf(result.optimizedNesting));
            MDC.put("optimization.total_time_ms", String.valueOf(totalTime));
            MDC.put("optimization.rules_that_helped", String.join("|", result.appliedRules));
            MDC.put("optimization.helpful_rules_count", String.valueOf(result.appliedRules.size()));

            OptimizationResult optimizationResult = new OptimizationResult(optimizedQueryJson, result);
            optimizationResult.setOriginalQuery(queryJson);

            // Set skip execution flags if detected
            if (shouldSkipExecution) {
                optimizationResult.setShouldSkipExecution(true);
                optimizationResult.setSkipExecutionReason(skipReason);

                // Add skip execution info to MDC for ClickHouse tracking
                MDC.put("execution.skip", "true");
                MDC.put("execution.skip_reason", skipReason);

                log.warn("🚫 EXECUTION SKIP DETECTED: {}", skipReason);
            } else {
                MDC.put("execution.skip", "false");
            }

            return optimizationResult;

        } catch (Exception e) {
            long totalTime = System.currentTimeMillis() - startTime;
            log.error("Failed to optimize query hash: {} after {}ms: {}", queryHash, totalTime, e.getMessage(), e);

            // Add error details to MDC
            MDC.put("optimization.query_hash", queryHash);
            MDC.put("optimization.status", "ERROR");
            MDC.put("optimization.error", e.getClass().getSimpleName());
            MDC.put("optimization.error_message", e.getMessage());
            MDC.put("optimization.total_time_ms", String.valueOf(totalTime));

            throw new RuntimeException("Failed to optimize query", e);
        }
    }

    /**
     * Validates that no invalid bool->bool nesting exists in the query
     * This prevents Elasticsearch parsing errors
     */
    private void validateNoBoolNesting(JsonNode node) {
        if (node.isObject()) {
            // Check if this is a bool query with invalid bool nesting
            if (node.has("bool")) {
                JsonNode boolNode = node.get("bool");
                if (boolNode.has("bool")) {
                    String invalidStructure = boolNode.toString();
                    log.error("CRITICAL: Invalid Elasticsearch syntax detected - bool query cannot contain another bool field directly");
                    log.error("Invalid structure: {}", invalidStructure);
                    throw new IllegalArgumentException("Invalid Elasticsearch syntax: bool query cannot contain another bool field directly. Found: bool.bool");
                }

                // Recursively check bool clauses
                for (String clause : Arrays.asList("must", "should", "filter", "must_not")) {
                    if (boolNode.has(clause)) {
                        JsonNode clauseNode = boolNode.get(clause);
                        if (clauseNode.isArray()) {
                            for (JsonNode item : clauseNode) {
                                validateNoBoolNesting(item);
                            }
                        } else {
                            validateNoBoolNesting(clauseNode);
                        }
                    }
                }
            } else {
                // Recursively check all child nodes
                for (JsonNode child : node) {
                    validateNoBoolNesting(child);
                }
            }
        } else if (node.isArray()) {
            for (JsonNode item : node) {
                validateNoBoolNesting(item);
            }
        }
    }

    /**
     * Optimizes query with validation-based safety net
     * Falls back to original query if optimization validation fails
     */
    public OptimizationResult optimizeQueryWithValidation(String originalQuery) {
        long startTime = System.currentTimeMillis();
        String queryHash = String.valueOf(originalQuery.hashCode());

        log.debug("Starting optimization with validation for query hash: {}", queryHash);

        try {
            OptimizationResult result = optimizeQuery(originalQuery);
            long optimizationTime = System.currentTimeMillis() - startTime;

            // CRITICAL: Check for execution skip first - bypass validation if execution should be skipped
            if (result.shouldSkipExecution()) {
                log.warn("SKIP EXECUTION detected during validation optimization for query hash: {}", queryHash);

                // Add skip execution info to MDC
                MDC.put("query.hash", queryHash);
                MDC.put("query.original_length", String.valueOf(originalQuery.length()));
                MDC.put("validation.status", "SKIPPED_EXECUTION");
                MDC.put("validation.skip_reason", result.getSkipExecutionReason());
                MDC.put("execution.recommendation", "SKIP");

                return result; // Return with skip flag set
            }

            // Validate the optimization
            if (isOptimizationValid(originalQuery, result.getOptimizedQuery())) {
                result.setValidationPassed(true);

                log.info("Query optimization validation PASSED for query hash: {} in {}ms", queryHash, optimizationTime);
                log.debug("Original query length: {}, Optimized query length: {}",
                        originalQuery.length(), result.getOptimizedQuery().length());

                // Add successful optimization to MDC
                MDC.put("query.hash", queryHash);
                MDC.put("query.original_length", String.valueOf(originalQuery.length()));
                MDC.put("query.optimized_length", String.valueOf(result.getOptimizedQuery().length()));
                MDC.put("validation.status", "PASSED");
                MDC.put("validation.time_ms", String.valueOf(optimizationTime));

                return result;
            } else {
                log.warn("Query optimization validation FAILED for query hash: {} - falling back to original query", queryHash);

                // Validation failed - return original query with warning
                OptimizationResult fallbackResult = new OptimizationResult();
                fallbackResult.setOriginalQuery(originalQuery);
                fallbackResult.setOptimizedQuery(originalQuery); // Fallback to original
                fallbackResult.setValidationPassed(false);
                fallbackResult.setValidationFailureReason("Optimization validation failed - falling back to original query");

                // Add validation failure to MDC
                MDC.put("query.hash", queryHash);
                MDC.put("query.original_length", String.valueOf(originalQuery.length()));
                MDC.put("validation.status", "FAILED");
                MDC.put("validation.failure_reason", "validation_check_failed");
                MDC.put("fallback.used", "true");

                log.warn("Query optimization failed validation - using original query as fallback");

                return fallbackResult;
            }
        } catch (Exception e) {
            long totalTime = System.currentTimeMillis() - startTime;
            log.error("Query optimization threw exception for query hash: {} after {}ms: {}",
                    queryHash, totalTime, e.getMessage(), e);

            // Optimization threw exception - return original query
            OptimizationResult fallbackResult = new OptimizationResult();
            fallbackResult.setOriginalQuery(originalQuery);
            fallbackResult.setOptimizedQuery(originalQuery); // Fallback to original
            fallbackResult.setValidationPassed(false);
            fallbackResult.setValidationFailureReason("Optimization exception: " + e.getMessage());

            // Add exception details to MDC
            MDC.put("query.hash", queryHash);
            MDC.put("query.original_length", String.valueOf(originalQuery.length()));
            MDC.put("validation.status", "EXCEPTION");
            MDC.put("validation.exception", e.getClass().getSimpleName());
            MDC.put("validation.exception_message", e.getMessage());
            MDC.put("fallback.used", "true");

            return fallbackResult;
        }
    }

    /**
     * Validates optimization using the same logic as the test suite
     */
    private boolean isOptimizationValid(String originalQuery, String optimizedQuery) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode original = mapper.readTree(originalQuery);
            JsonNode optimized = mapper.readTree(optimizedQuery);

            // Lightweight validation checks (subset of test validations)
            return validateBasicStructure(original, optimized) &&
                    validateFieldPreservation(original, optimized) &&
                    validateCriticalClauses(original, optimized);

        } catch (Exception e) {
            return false; // If we can't validate, don't use optimization
        }
    }

    private boolean validateBasicStructure(JsonNode original, JsonNode optimized) {
        if( original == null || optimized == null) {
            return false; // Can't validate null queries
        }

        if (!original.has("query") || !optimized.has("query")) {
            return false;
        }

        // Ensure both have query sections if either has one
        if (original.has("query") != optimized.has("query")) {
            return false;
        }

        // If neither has query section, that's OK for some aggregation-only queries
        if (!original.has("query") && !optimized.has("query")) {
            return true;
        }

        // Ensure critical fields are preserved
        for (String field : Arrays.asList("size", "from", "sort", "_source", "track_total_hits")) {
            if (original.has(field)) {
                if (!optimized.has(field) || !original.get(field).equals(optimized.get(field))) {
                    return false;
                }
            }
        }

        return true;
    }

    private boolean validateFieldPreservation(JsonNode original, JsonNode optimized) {
        Set<String> originalFields = extractAllFieldNames(original);
        Set<String> optimizedFields = extractAllFieldNames(optimized);

        // Allow for valid transformations like qualifiedName -> __qualifiedNameHierarchy
        for (String field : originalFields) {
            if (!optimizedFields.contains(field) && !isValidFieldTransformation(field, optimizedFields)) {
                return false;
            }
        }

        return true;
    }

    private boolean validateCriticalClauses(JsonNode original, JsonNode optimized) {
        // Ensure total number of conditions is preserved (allowing for consolidation)
        int originalConditions = countTotalConditions(original);
        int optimizedConditions = countTotalConditions(optimized);

        // Count specific types of conditions that might be consolidated
        int originalWildcards = countWildcardConditions(original);
        int optimizedWildcards = countWildcardConditions(optimized);
        int optimizedRegexps = countRegexpConditions(optimized);

        // Special handling for wildcard consolidation scenarios
        if (originalWildcards > 3) {
            // Calculate effective condition preservation for wildcard consolidation
            // Each regexp can represent multiple wildcards, so we need to account for this
            int wildcardToRegexpConversion = Math.max(0, originalWildcards - optimizedWildcards);
            int effectiveRegexpValue = optimizedRegexps * Math.max(1, wildcardToRegexpConversion / Math.max(1, optimizedRegexps));
            int adjustedOptimizedConditions = optimizedConditions + effectiveRegexpValue - optimizedRegexps;

            // For wildcard consolidation, be much more permissive
            if (optimizedRegexps > 0 && wildcardToRegexpConversion > 0) {
                return adjustedOptimizedConditions >= (originalConditions * 0.2); // Allow 80% consolidation for wildcards
            }
        }

        // Allow for reasonable consolidation but not major losses
        return optimizedConditions >= (originalConditions * 0.8); // Allow 20% consolidation
    }

    private int countWildcardConditions(JsonNode query) {
        return countSpecificConditionsRecursive(query, "wildcard");
    }

    private int countRegexpConditions(JsonNode query) {
        return countSpecificConditionsRecursive(query, "regexp");
    }

    private int countSpecificConditionsRecursive(JsonNode node, String conditionType) {
        if (node == null) return 0;

        int count = 0;

        if (node.isObject()) {
            // Count specific condition type
            if (node.has(conditionType)) {
                count++;
            }

            // Recursively count in children
            for (JsonNode child : node) {
                count += countSpecificConditionsRecursive(child, conditionType);
            }
        } else if (node.isArray()) {
            for (JsonNode arrayItem : node) {
                count += countSpecificConditionsRecursive(arrayItem, conditionType);
            }
        }

        return count;
    }

    private Set<String> extractAllFieldNames(JsonNode query) {
        Set<String> fieldNames = new HashSet<>();
        extractFieldNamesRecursive(query, fieldNames);
        return fieldNames;
    }

    private void extractFieldNamesRecursive(JsonNode node, Set<String> fieldNames) {
        if (node == null) return;

        if (node.isObject()) {
            // Extract field names from query types
            if (node.has("term")) {
                node.get("term").fieldNames().forEachRemaining(fieldNames::add);
            } else if (node.has("terms")) {
                node.get("terms").fieldNames().forEachRemaining(fieldNames::add);
            } else if (node.has("range")) {
                node.get("range").fieldNames().forEachRemaining(fieldNames::add);
            } else if (node.has("wildcard")) {
                node.get("wildcard").fieldNames().forEachRemaining(fieldNames::add);
            } else if (node.has("match")) {
                node.get("match").fieldNames().forEachRemaining(fieldNames::add);
            } else if (node.has("regexp")) {
                node.get("regexp").fieldNames().forEachRemaining(fieldNames::add);
            } else if (node.has("prefix")) {
                node.get("prefix").fieldNames().forEachRemaining(fieldNames::add);
            } else if (node.has("exists")) {
                JsonNode existsNode = node.get("exists");
                if (existsNode.has("field")) {
                    fieldNames.add(existsNode.get("field").asText());
                }
            }

            // Recursively check children
            for (JsonNode child : node) {
                extractFieldNamesRecursive(child, fieldNames);
            }
        } else if (node.isArray()) {
            for (JsonNode arrayItem : node) {
                extractFieldNamesRecursive(arrayItem, fieldNames);
            }
        }
    }

    private boolean isValidFieldTransformation(String originalField, Set<String> optimizedFields) {
        // ANY field ending with qualified name patterns can transform to __qualifiedNameHierarchy
        boolean isOriginalQualifiedName = originalField.endsWith("qualifiedName") || originalField.endsWith("QualifiedName");
        if (isOriginalQualifiedName && optimizedFields.contains("__qualifiedNameHierarchy")) {
            return true;
        }

        // Reverse transformations (when __qualifiedNameHierarchy is in original but not optimized)
        if (originalField.equals("__qualifiedNameHierarchy")) {
            // Check if any field ending with qualified name patterns exists in optimized
            for (String optimizedField : optimizedFields) {
                boolean isOptimizedQualifiedName = optimizedField.endsWith("qualifiedName") || optimizedField.endsWith("QualifiedName");
                if (isOptimizedQualifiedName) {
                    return true;
                }
            }
        }

        // Wildcard consolidation: field remains the same but query type changes wildcard->regexp
        // This is handled at the condition level, not field level, so we preserve all fields
        if (optimizedFields.contains(originalField)) {
            return true;
        }

        return false;
    }

    private int countTotalConditions(JsonNode query) {
        return countConditionsRecursive(query);
    }

    private int countConditionsRecursive(JsonNode node) {
        if (node == null) return 0;

        int count = 0;

        if (node.isObject()) {
            // Count leaf conditions
            if (node.has("term") || node.has("terms") || node.has("range") ||
                    node.has("wildcard") || node.has("match") || node.has("exists") ||
                    node.has("regexp") || node.has("prefix")) {
                count++;
            }

            // Recursively count in children
            for (JsonNode child : node) {
                count += countConditionsRecursive(child);
            }
        } else if (node.isArray()) {
            for (JsonNode arrayItem : node) {
                count += countConditionsRecursive(arrayItem);
            }
        }

        return count;
    }

    /**
     * Rule 1: Structure Simplification
     * Flattens unnecessary nested bool queries and removes single-item wrappers
     */
    private class StructureSimplificationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "StructureSimplification";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeBoolStructure);
        }

        private JsonNode optimizeBoolStructure(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Optimize bool queries
            if (objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                // Flatten nested bool structures
                for (String clause : Arrays.asList("must", "should", "filter", "must_not")) {
                    if (boolNode.has(clause)) {
                        JsonNode clauseNode = boolNode.get(clause);

                        // Handle single objects that are not arrays
                        if (clauseNode.isObject() && !clauseNode.isArray()) {
                            // Convert single object to array for consistent processing
                            ArrayNode arrayNode = objectMapper.createArrayNode();
                            arrayNode.add(clauseNode);
                            boolNode.set(clause, arrayNode);
                            clauseNode = arrayNode;
                        }

                        if (clauseNode.isArray()) {
                            ArrayNode array = (ArrayNode) clauseNode;

                            // Remove empty arrays
                            if (array.size() == 0) {
                                boolNode.remove(clause);
                                continue;
                            }

                            // Flatten deeply nested bool structures
                            ArrayNode flattenedArray = flattenNestedBoolArray(array, clause);
                            if (flattenedArray.size() != array.size() || !flattenedArray.equals(array)) {
                                boolNode.set(clause, flattenedArray);
                            }
                        }
                    }
                }

                // Convert filter object to filter array if needed
                if (boolNode.has("filter") && boolNode.get("filter").isObject() && !boolNode.get("filter").isArray()) {
                    JsonNode filterObj = boolNode.get("filter");
                    if (filterObj.has("bool")) {
                        // Extract content from nested bool and flatten
                        JsonNode innerBool = filterObj.get("bool");
                        if (innerBool.has("must") && innerBool.get("must").isArray()) {
                            ArrayNode mustArray = (ArrayNode) innerBool.get("must");
                            ArrayNode flattenedFilters = flattenFilterStructure(mustArray);
                            boolNode.set("filter", flattenedFilters);
                        }
                    }
                }
            }

            return objectNode;
        }

        private ArrayNode flattenNestedBoolArray(ArrayNode array, String clauseType) {
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode item : array) {
                if (item.has("bool")) {
                    JsonNode boolContent = item.get("bool");

                    // If this bool only has the same clause type, flatten it
                    if (boolContent.has(clauseType) && boolContent.size() == 1) {
                        JsonNode innerClause = boolContent.get(clauseType);
                        if (innerClause.isArray()) {
                            for (JsonNode innerItem : innerClause) {
                                result.add(innerItem);
                            }
                        } else {
                            result.add(innerClause);
                        }
                    } else {
                        result.add(item);
                    }
                } else {
                    result.add(item);
                }
            }

            return result;
        }

        private ArrayNode flattenFilterStructure(ArrayNode mustArray) {
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode mustItem : mustArray) {
                if (mustItem.has("bool") && mustItem.get("bool").has("must")) {
                    JsonNode innerMust = mustItem.get("bool").get("must");
                    if (innerMust.isArray()) {
                        for (JsonNode innerItem : innerMust) {
                            result.add(innerItem);
                        }
                    } else {
                        result.add(innerMust);
                    }
                } else {
                    result.add(mustItem);
                }
            }

            return result;
        }
    }

    /**
     * Rule 2: Empty Bool Elimination
     * Removes empty bool query objects that provide no filtering logic
     */
    private class EmptyBoolEliminationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "EmptyBoolElimination";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::removeEmptyBoolQueries);
        }

        private JsonNode removeEmptyBoolQueries(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Check for empty bool objects
            if (objectNode.has("bool")) {
                JsonNode boolNode = objectNode.get("bool");

                if (boolNode.isObject() && boolNode.size() == 0) {
                    // Return match_all query instead of empty object
                    ObjectNode matchAll = objectMapper.createObjectNode();
                    matchAll.set("match_all", objectMapper.createObjectNode());
                    return matchAll;
                }
            }

            // Remove empty bool objects from arrays
            if (objectNode.has("bool")) {
                ObjectNode boolObject = (ObjectNode) objectNode.get("bool");

                for (String clause : Arrays.asList("must", "should", "filter", "must_not")) {
                    if (boolObject.has(clause) && boolObject.get(clause).isArray()) {
                        ArrayNode array = (ArrayNode) boolObject.get(clause);
                        ArrayNode filteredArray = objectMapper.createArrayNode();

                        for (JsonNode item : array) {
                            // Skip empty bool objects
                            if (!(item.has("bool") && item.get("bool").size() == 0)) {
                                filteredArray.add(item);
                            }
                        }

                        if (filteredArray.size() != array.size()) {
                            boolObject.set(clause, filteredArray);
                        }
                    }
                }
            }

            return objectNode;
        }
    }

    /**
     * Rule 3: Multiple Terms Consolidation
     * Merges multiple terms queries on the same field into a single terms query
     */
    private class MultipleTermsConsolidationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "MultipleTermsConsolidation";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::consolidateMultipleTermsQueries);
        }

        private JsonNode consolidateMultipleTermsQueries(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                // Consolidate terms queries ONLY in should, filter, must_not clauses
                // IMPORTANT: Do NOT consolidate 'must' clauses as this changes AND to OR semantics!

                // Check if there are terms in 'must' clauses and log why we skip them
                if (boolNode.has("must") && boolNode.get("must").isArray()) {
                    ArrayNode mustArray = (ArrayNode) boolNode.get("must");
                    int mustTermCount = 0;
                    for (JsonNode clause : mustArray) {
                        if (clause.has("term")) mustTermCount++;
                    }
                    if (mustTermCount > 1) {
                        log.debug("MultipleTermsConsolidation: Skipping {} term queries in 'must' clause to preserve AND semantics", mustTermCount);
                    }
                }

                for (String clause : Arrays.asList("should", "filter", "must_not")) {
                    if (boolNode.has(clause) && boolNode.get(clause).isArray()) {
                        ArrayNode array = (ArrayNode) boolNode.get(clause);
                        ArrayNode consolidatedArray = consolidateTermsInArray(array);

                        if (consolidatedArray.size() != array.size()) {
                            boolNode.set(clause, consolidatedArray);
                        }
                    }
                }
            }

            return objectNode;
        }

        private ArrayNode consolidateTermsInArray(ArrayNode array) {
            Map<String, Set<String>> termsByField = new HashMap<>();
            ArrayNode nonTermsQueries = objectMapper.createArrayNode();

            // Group terms queries by field
            for (JsonNode item : array) {
                if (item.has("terms")) {
                    JsonNode termsNode = item.get("terms");
                    Iterator<String> fieldNames = termsNode.fieldNames();
                    if (fieldNames.hasNext()) {
                        String field = fieldNames.next();
                        JsonNode valuesNode = termsNode.get(field);

                        if (valuesNode.isArray()) {
                            Set<String> values = new LinkedHashSet<>();
                            for (JsonNode value : valuesNode) {
                                values.add(value.asText());
                            }
                            termsByField.computeIfAbsent(field, k -> new LinkedHashSet<>()).addAll(values);
                        }
                    }
                } else if (item.has("term")) {
                    // Convert single term to terms
                    JsonNode termNode = item.get("term");
                    Iterator<String> fieldNames = termNode.fieldNames();
                    if (fieldNames.hasNext()) {
                        String field = fieldNames.next();
                        String value = termNode.get(field).asText();
                        termsByField.computeIfAbsent(field, k -> new LinkedHashSet<>()).add(value);
                    }
                } else {
                    nonTermsQueries.add(item);
                }
            }

            // Create consolidated terms queries
            ArrayNode result = objectMapper.createArrayNode();

            // Add consolidated terms queries
            for (Map.Entry<String, Set<String>> entry : termsByField.entrySet()) {
                String field = entry.getKey();
                Set<String> allValues = entry.getValue();

                ObjectNode termsQuery = objectMapper.createObjectNode();
                ObjectNode termsObject = objectMapper.createObjectNode();
                ArrayNode valuesArray = objectMapper.createArrayNode();
                allValues.forEach(valuesArray::add);
                termsObject.set(field, valuesArray);
                termsQuery.set("terms", termsObject);

                result.add(termsQuery);
            }

            // Add non-terms queries back
            for (JsonNode nonTerms : nonTermsQueries) {
                result.add(nonTerms);
            }

            return result;
        }
    }

    /**
     * Rule 4: Array Deduplication
     * Removes duplicate values from terms arrays and other array fields
     */
    private class ArrayDeduplicationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "ArrayDeduplication";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::deduplicateArrays);
        }

        private JsonNode deduplicateArrays(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Deduplicate terms arrays
            if (objectNode.has("terms")) {
                JsonNode termsNode = objectNode.get("terms");
                if (termsNode.isObject()) {
                    ObjectNode termsObject = (ObjectNode) termsNode;
                    Iterator<String> fieldNames = termsObject.fieldNames();
                    List<String> fieldsToUpdate = new ArrayList<>();
                    Map<String, ArrayNode> newArrays = new HashMap<>();

                    fieldNames.forEachRemaining(fieldName -> {
                        JsonNode arrayNode = termsObject.get(fieldName);
                        if (arrayNode.isArray()) {
                            ArrayNode deduplicatedArray = deduplicateArray((ArrayNode) arrayNode);
                            if (deduplicatedArray.size() != arrayNode.size()) {
                                fieldsToUpdate.add(fieldName);
                                newArrays.put(fieldName, deduplicatedArray);
                            }
                        }
                    });

                    // Update arrays that had duplicates
                    for (String field : fieldsToUpdate) {
                        termsObject.set(field, newArrays.get(field));
                    }
                }
            }

            return objectNode;
        }

        private ArrayNode deduplicateArray(ArrayNode arrayNode) {
            Set<String> seen = new LinkedHashSet<>(); // Preserve order
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode item : arrayNode) {
                String value = item.asText();
                if (!seen.contains(value)) {
                    seen.add(value);
                    result.add(item);
                }
            }

            return result;
        }
    }

    /**
     * Rule 6: Regexp Simplification
     * Simplifies overly complex regexp patterns
     */
    private class RegexpSimplificationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "RegexpSimplification";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::simplifyRegexp);
        }

        private JsonNode simplifyRegexp(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("regexp")) {
                JsonNode regexpNode = objectNode.get("regexp");
                if (regexpNode.isObject()) {
                    ObjectNode regexpObject = (ObjectNode) regexpNode;
                    Iterator<String> fieldNames = regexpObject.fieldNames();
                    List<String> fieldsToUpdate = new ArrayList<>();
                    Map<String, String> newPatterns = new HashMap<>();

                    fieldNames.forEachRemaining(fieldName -> {
                        JsonNode valueNode = regexpObject.get(fieldName);
                        String pattern;

                        if (valueNode.isObject() && valueNode.has("value")) {
                            pattern = valueNode.get("value").asText();
                        } else if (valueNode.isTextual()) {
                            pattern = valueNode.asText();
                        } else {
                            return;
                        }

                        String simplifiedPattern = simplifyRegexpPattern(pattern);
                        if (!simplifiedPattern.equals(pattern)) {
                            fieldsToUpdate.add(fieldName);
                            newPatterns.put(fieldName, simplifiedPattern);
                        }
                    });

                    // Update patterns that were simplified
                    for (String field : fieldsToUpdate) {
                        JsonNode valueNode = regexpObject.get(field);
                        String newPattern = newPatterns.get(field);

                        if (valueNode.isObject()) {
                            ((ObjectNode) valueNode).put("value", newPattern);
                        } else {
                            regexpObject.put(field, newPattern);
                        }
                    }
                }
            }

            return objectNode;
        }

        private String simplifyRegexpPattern(String pattern) {
            if (pattern == null || pattern.isEmpty()) {
                return pattern;
            }
            
            try {
                String simplified = pattern;
                
                // 1. FIXED: Simplify overly complex character classes with correct escaping
                // Convert [a-zA-Z0-9_]* to [\w]* (but be conservative)
                if (simplified.contains("[a-zA-Z0-9")) {
                    // Only replace if it's a complete character class
                    simplified = simplified.replaceAll("\\[a-zA-Z0-9_\\]\\*", "[\\\\w]*");
                    simplified = simplified.replaceAll("\\[a-zA-Z0-9_\\-\\]\\*", "[\\\\w\\\\-]*");
                }
                
                // 2. FIXED: Remove redundant .* patterns with proper bounds checking
                // Pattern like "prefix.*.*suffix" becomes "prefix.*suffix"
                while (simplified.contains(".*.*")) {
                    simplified = simplified.replace(".*.*", ".*");
                }
                
                // 3. NEW: Simplify redundant anchoring
                // Pattern like "^.*" at start is redundant (unless it's the whole pattern)
                if (simplified.startsWith("^.*") && simplified.length() > 3) {
                    simplified = simplified.substring(1); // Remove redundant ^
                }
                
                // 4. NEW: Simplify redundant ending
                // Pattern like ".*$" at end is often redundant
                if (simplified.endsWith(".*$") && simplified.length() > 3) {
                    simplified = simplified.substring(0, simplified.length() - 1); // Remove redundant $
                }
                
                // 5. VALIDATE: Ensure the simplified pattern is a valid regex
                if (!isValidRegexPattern(simplified)) {
                    log.warn("RegexpSimplification: Simplified pattern '{}' is invalid, returning original: '{}'", simplified, pattern);
                    return pattern; // Fallback to original if simplified version is invalid
                }
                
                // 6. CONSERVATIVE: Only apply simplifications that are guaranteed safe
                // Don't modify patterns that might change semantics
                
                log.debug("RegexpSimplification: '{}' -> '{}' (validated)", pattern, simplified);
                return simplified;
                
            } catch (Exception e) {
                log.warn("Failed to simplify regexp pattern '{}': {}", pattern, e.getMessage());
                // SAFETY: Return original pattern if simplification fails
                return pattern;
            }
        }
        
        /**
         * 🔍 REGEX VALIDATION: Multi-level validation for regex patterns
         * 
         * 1. Java Pattern validation (fast, catches syntax errors)
         * 2. Elasticsearch-specific validation (if available)
         * 3. Lucene RegExp validation (most accurate for ES)
         */
        private boolean isValidRegexPattern(String pattern) {
            if (pattern == null || pattern.isEmpty()) {
                return false;
            }
            
            // LEVEL 1: Java Pattern validation (fast, catches most issues)
            try {
                java.util.regex.Pattern.compile(pattern);
            } catch (java.util.regex.PatternSyntaxException e) {
                log.debug("Invalid Java regex pattern '{}': {}", pattern, e.getMessage());
                return false;
            } catch (Exception e) {
                log.debug("Unexpected error in Java regex validation '{}': {}", pattern, e.getMessage());
                return false;
            }
            
            // LEVEL 2: Lucene RegExp validation (more ES-specific)
            try {
                // Use Lucene's RegExp class which is what Elasticsearch uses internally
                // This is more accurate than Java Pattern for ES compatibility
                org.apache.lucene.util.automaton.RegExp luceneRegexp = 
                    new org.apache.lucene.util.automaton.RegExp(pattern);
                
                // Try to convert to automaton (this validates the pattern)
                luceneRegexp.toAutomaton();
                
                log.debug("Regex pattern '{}' validated successfully with Lucene RegExp", pattern);
                return true;
                
            } catch (IllegalArgumentException e) {
                log.debug("Invalid Lucene regex pattern '{}': {}", pattern, e.getMessage());
                return false;
            } catch (Exception e) {
                log.debug("Unexpected error in Lucene regex validation '{}': {}", pattern, e.getMessage());
                // If Lucene validation fails but Java validation passed, allow it
                // This handles edge cases where Lucene is more strict
                log.debug("Falling back to Java regex validation for pattern '{}'", pattern);
                return true;
            }
        }
    }

    /**
     * Rule 7: Aggregation Optimization
     * Optimizes repetitive aggregation patterns
     */
    private class AggregationOptimizationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "AggregationOptimization";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeAggregations);
        }

        private JsonNode optimizeAggregations(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("aggs")) {
                JsonNode aggsNode = objectNode.get("aggs");
                if (aggsNode.isObject() && aggsNode.size() > 10) {
                    ObjectNode optimizedAggs = optimizeRepetitiveAggregations((ObjectNode) aggsNode);
                    objectNode.set("aggs", optimizedAggs);
                }
            }

            return objectNode;
        }

        private ObjectNode optimizeRepetitiveAggregations(ObjectNode aggsNode) {
            ObjectNode result = objectMapper.createObjectNode();

            Iterator<String> fieldNames = aggsNode.fieldNames();
            fieldNames.forEachRemaining(aggName -> {
                JsonNode aggConfig = aggsNode.get(aggName);
                JsonNode optimizedAgg = optimizeSingleAggregation(aggConfig);
                result.set(aggName, optimizedAgg);
            });

            return result;
        }

        private JsonNode optimizeSingleAggregation(JsonNode aggConfig) {
            if (aggConfig.has("filter") && aggConfig.get("filter").has("bool")) {
                ObjectNode optimized = (ObjectNode) aggConfig.deepCopy();
                JsonNode filterBool = optimized.get("filter").get("bool");

                if (filterBool.has("should") && filterBool.get("should").isArray()) {
                    ArrayNode shouldArray = (ArrayNode) filterBool.get("should");
                    ArrayNode optimizedShould = objectMapper.createArrayNode();

                    for (JsonNode shouldClause : shouldArray) {
                        optimizedShould.add(shouldClause);
                    }

                    ((ObjectNode) filterBool).set("should", optimizedShould);
                }

                return optimized;
            }

            return aggConfig;
        }
    }

    /**
     * Rule 8: Wildcard Consolidation - SIMPLIFIED ROBUST VERSION
     * Groups multiple wildcard queries into regexp patterns anywhere in the query tree
     */
    private class WildcardConsolidationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "WildcardConsolidation";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::consolidateWildcardsInNode);
        }

        private JsonNode consolidateWildcardsInNode(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Check bool clause types that allow wildcard consolidation
            for (String clauseType : Arrays.asList("should", "filter", "must_not")) {
                if (objectNode.has("bool") && objectNode.get("bool").has(clauseType)) {
                    JsonNode clauseNode = objectNode.get("bool").get(clauseType);

                    if (clauseNode.isArray()) {
                        ArrayNode clauseArray = (ArrayNode) clauseNode;

                        Map<String, List<JsonNode>> directWildcardsByField = new HashMap<>();
                        Map<String, List<JsonNode>> nestedWildcardsByField = new HashMap<>();
                        List<JsonNode> nonWildcards = new ArrayList<>();

                        for (JsonNode clause : clauseArray) {
                            if (clause.has("wildcard")) {
                                // Check for special characters before consolidation
                                JsonNode wildcardNode = clause.get("wildcard");
                                Iterator<String> fieldNames = wildcardNode.fieldNames();
                                if (fieldNames.hasNext()) {
                                    String field = fieldNames.next();
                                    JsonNode fieldValue = wildcardNode.get(field);
                                    String pattern = fieldValue.isObject() && fieldValue.get("value") != null ? fieldValue.get("value").asText() : fieldValue.asText();
                                    
                                    // Skip consolidation if pattern has special characters
                                    if (hasSpecialCharacters(pattern, fieldValue)) {
                                        nonWildcards.add(clause);
                                        continue;
                                    }
                                    
                                    directWildcardsByField.computeIfAbsent(field, k -> new ArrayList<>()).add(clause);
                                }
                            } else if (clause.has("bool") && isSimpleWildcardWrapper(clause)) {
                                JsonNode nestedWildcard = extractWildcardFromSimpleWrapper(clause);
                                if (nestedWildcard != null) {
                                    JsonNode wildcardNode = nestedWildcard.get("wildcard");
                                    Iterator<String> fieldNames = wildcardNode.fieldNames();
                                    if (fieldNames.hasNext()) {
                                        String field = fieldNames.next();
                                        JsonNode fieldValue = wildcardNode.get(field);
                                        String pattern = fieldValue.isObject() && fieldValue.get("value") != null ? fieldValue.get("value").asText() : fieldValue.asText();
                                        
                                        // Skip consolidation if pattern has special characters
                                        if (hasSpecialCharacters(pattern, fieldValue)) {
                                            nonWildcards.add(clause);
                                            continue;
                                        }
                                        
                                        nestedWildcardsByField.computeIfAbsent(field, k -> new ArrayList<>()).add(clause);
                                    }
                                } else {
                                    nonWildcards.add(clause);
                                }
                            } else {
                                nonWildcards.add(clause);
                            }
                        }

                        // Consolidate both direct and nested wildcards
                        boolean hasConsolidation = false;
                        ArrayNode newArray = objectMapper.createArrayNode();

                        // Add non-wildcard clauses first
                        for (JsonNode nonWildcard : nonWildcards) {
                            newArray.add(nonWildcard);
                        }

                        // Process direct wildcards
                        hasConsolidation |= processWildcardGroup(directWildcardsByField, newArray, false);

                        // Process nested wildcards (preserve the bool wrapper context)
                        hasConsolidation |= processWildcardGroup(nestedWildcardsByField, newArray, true);

                        // Replace the array if we made consolidations
                        if (hasConsolidation) {
                            ((ObjectNode) objectNode.get("bool")).set(clauseType, newArray);
                        }
                    }
                }
            }

            return objectNode;
        }

        /**
         * Checks if a bool node is a simple wrapper around a single wildcard query
         */
        private boolean isSimpleWildcardWrapper(JsonNode boolWrapper) {
            if (!boolWrapper.has("bool")) return false;

            JsonNode bool = boolWrapper.get("bool");

            // Check for single-clause bool with one wildcard
            int clauseCount = 0;
            JsonNode targetClause = null;

            for (String clauseType : Arrays.asList("must", "should", "filter", "must_not")) {
                if (bool.has(clauseType)) {
                    clauseCount++;
                    targetClause = bool.get(clauseType);
                }
            }

            // Must be exactly one clause type
            if (clauseCount != 1 || targetClause == null) return false;

            // Check if the clause contains exactly one wildcard
            if (targetClause.isArray()) {
                ArrayNode array = (ArrayNode) targetClause;
                return array.size() == 1 && array.get(0).has("wildcard");
            } else {
                return targetClause.has("wildcard");
            }
        }

        /**
         * Extracts the wildcard query from a simple bool wrapper
         */
        private JsonNode extractWildcardFromSimpleWrapper(JsonNode boolWrapper) {
            JsonNode bool = boolWrapper.get("bool");

            for (String clauseType : Arrays.asList("must", "should", "filter", "must_not")) {
                if (bool.has(clauseType)) {
                    JsonNode clause = bool.get(clauseType);
                    if (clause.isArray()) {
                        ArrayNode array = (ArrayNode) clause;
                        if (array.size() == 1 && array.get(0).has("wildcard")) {
                            return array.get(0);
                        }
                    } else if (clause.has("wildcard")) {
                        return clause;
                    }
                }
            }
            return null;
        }

        /**
         * Processes a group of wildcards (either direct or nested) for consolidation
         */
        private boolean processWildcardGroup(Map<String, List<JsonNode>> wildcardsByField,
                                             ArrayNode targetArray,
                                             boolean preserveWrapper) {
            boolean hasConsolidation = false;

            for (Map.Entry<String, List<JsonNode>> entry : wildcardsByField.entrySet()) {
                String field = entry.getKey();
                List<JsonNode> wildcards = entry.getValue();

                // Extract patterns and create regexp
                List<String> patterns = new ArrayList<>();
                String wrapperContext = null;  // For preserving must_not, should, etc.

                for (JsonNode wildcardContainer : wildcards) {
                    JsonNode wildcardNode;
                    if (wildcardContainer.has("wildcard")) {
                        wildcardNode = wildcardContainer.get("wildcard");
                    } else {
                        // Extract from nested bool
                        wildcardNode = extractWildcardFromSimpleWrapper(wildcardContainer).get("wildcard");
                        if (wrapperContext == null && preserveWrapper) {
                            // Determine the wrapper context (must_not, should, etc.)
                            JsonNode bool = wildcardContainer.get("bool");
                            for (String clauseType : Arrays.asList("must", "should", "filter", "must_not")) {
                                if (bool.has(clauseType)) {
                                    wrapperContext = clauseType;
                                    break;
                                }
                            }
                        }
                    }

                    JsonNode fieldValue = wildcardNode.get(field);
                    
                    String pattern = fieldValue.isObject() && fieldValue.get("value") != null ? fieldValue.get("value").asText() : fieldValue.asText();
                    
                    // Skip consolidation if pattern has special characters or flags
                    if (hasSpecialCharacters(pattern, fieldValue)) {
                        return false;
                    }
                    patterns.add(pattern);
                }

                // ENHANCED: Handle 1000+ character regexp splitting
                List<String> regexpPatterns = createRegexpPatterns(patterns);

                if (regexpPatterns.size() == 1) {
                    // Single regexp - use existing logic
                    ObjectNode regexpNode = objectMapper.createObjectNode();
                    ObjectNode regexpQuery = objectMapper.createObjectNode();
                    regexpQuery.put(field, regexpPatterns.get(0));
                    regexpNode.set("regexp", regexpQuery);

                    // Wrap in the same context if needed (preserve must_not, should, etc.)
                    if (preserveWrapper && wrapperContext != null) {
                        ObjectNode wrapperNode = objectMapper.createObjectNode();
                        ObjectNode boolNode = objectMapper.createObjectNode();
                        ArrayNode clauseArray = objectMapper.createArrayNode();
                        clauseArray.add(regexpNode);
                        boolNode.set(wrapperContext, clauseArray);
                        wrapperNode.set("bool", boolNode);
                        targetArray.add(wrapperNode);
                    } else {
                        targetArray.add(regexpNode);
                    }
                } else {
                    // Multiple regexps - create bool.should to OR them together
                    ArrayNode shouldArray = objectMapper.createArrayNode();

                    for (String regexpPattern : regexpPatterns) {
                        ObjectNode regexpNode = objectMapper.createObjectNode();
                        ObjectNode regexpQuery = objectMapper.createObjectNode();
                        regexpQuery.put(field, regexpPattern);
                        regexpNode.set("regexp", regexpQuery);
                        shouldArray.add(regexpNode);
                    }

                    // Create bool.should wrapper for multiple regexps
                    ObjectNode multiRegexpBool = objectMapper.createObjectNode();
                    ObjectNode multiRegexpBoolContent = objectMapper.createObjectNode();
                    multiRegexpBoolContent.set("should", shouldArray);
                    multiRegexpBool.set("bool", multiRegexpBoolContent);

                    // Wrap in the same context if needed (preserve must_not, should, etc.)
                    if (preserveWrapper && wrapperContext != null) {
                        ObjectNode wrapperNode = objectMapper.createObjectNode();
                        ObjectNode boolNode = objectMapper.createObjectNode();
                        ArrayNode clauseArray = objectMapper.createArrayNode();
                        clauseArray.add(multiRegexpBool);
                        boolNode.set(wrapperContext, clauseArray);
                        wrapperNode.set("bool", boolNode);
                        targetArray.add(wrapperNode);
                    } else {
                        targetArray.add(multiRegexpBool);
                    }

                    log.debug("WildcardConsolidation: Split large regexp into {} parts for field '{}' due to 1000-char limit",
                            regexpPatterns.size(), field);
                }

                hasConsolidation = true;

                log.debug("WildcardConsolidation: Consolidated {} wildcards for field '{}' into regexp",
                        wildcards.size(), field);
            }

            return hasConsolidation;
        }

        /**
         * Creates regexp patterns with automatic splitting when exceeding 1000 characters
         * @param patterns List of wildcard patterns to convert
         * @return List of regexp patterns (1 if under limit, multiple if split needed)
         */
        private List<String> createRegexpPatterns(List<String> patterns) {
            if (patterns.isEmpty()) return Arrays.asList(".*");

            // Remove asterisks and clean patterns for regexp
            List<String> cleanPatterns = patterns.stream()
                    .map(p -> p.replaceAll("\\*", ""))
                    .filter(p -> !p.isEmpty())
                    .collect(Collectors.toList());

            if (cleanPatterns.isEmpty()) return Arrays.asList(".*");

            // Escape patterns for regexp usage
            List<String> escapedPatterns = cleanPatterns.stream()
                    .map(this::escapeRegexSpecialChars)
                    .collect(Collectors.toList());

            // Try to create a single regexp pattern
            String allAlternatives = escapedPatterns.stream().collect(Collectors.joining("|"));
            String singlePattern = ".*(" + allAlternatives + ").*";

            // If single pattern is under 1000 chars, return it
            if (singlePattern.length() <= 1000) {
                log.debug("WildcardConsolidation: Created single regexp pattern ({} chars)", singlePattern.length());
                return Arrays.asList(singlePattern);
            }

            // SPLIT LOGIC: Pattern exceeds 1000 chars, need to split
            log.debug("WildcardConsolidation: Pattern exceeds 1000 chars ({}), splitting...", singlePattern.length());

            List<String> resultPatterns = new ArrayList<>();
            List<String> currentGroup = new ArrayList<>();
            int currentLength = 6; // Start with ".*().*" base length

            for (String escapedPattern : escapedPatterns) {
                int patternLength = escapedPattern.length() + 1; // +1 for "|" separator

                // Check if adding this pattern would exceed the limit
                if (currentLength + patternLength > 995) { // Leave 5 chars buffer
                    // Finalize current group if it has patterns
                    if (!currentGroup.isEmpty()) {
                        String groupAlternatives = currentGroup.stream().collect(Collectors.joining("|"));
                        String groupPattern = ".*(" + groupAlternatives + ").*";
                        resultPatterns.add(groupPattern);
                        log.debug("WildcardConsolidation: Created split regexp part {} ({} chars, {} patterns)",
                                resultPatterns.size(), groupPattern.length(), currentGroup.size());
                    }

                    // Start new group
                    currentGroup = new ArrayList<>();
                    currentLength = 6; // Reset to base length
                }

                currentGroup.add(escapedPattern);
                currentLength += patternLength;
            }

            // Add final group if it has patterns
            if (!currentGroup.isEmpty()) {
                String groupAlternatives = currentGroup.stream().collect(Collectors.joining("|"));
                String groupPattern = ".*(" + groupAlternatives + ").*";
                resultPatterns.add(groupPattern);
                log.debug("WildcardConsolidation: Created final split regexp part {} ({} chars, {} patterns)",
                        resultPatterns.size(), groupPattern.length(), currentGroup.size());
            }

            log.info("WildcardConsolidation: Split {} patterns into {} regexp queries to stay under 1000-char limit",
                    patterns.size(), resultPatterns.size());

            return resultPatterns;
        }

        /**
         * Legacy method for backward compatibility - returns first pattern only
         */
        private String createRegexpPattern(List<String> patterns) {
            List<String> regexpPatterns = createRegexpPatterns(patterns);
            return regexpPatterns.isEmpty() ? ".*" : regexpPatterns.get(0);
        }

        private String escapeRegexSpecialChars(String input) {
            return input.replaceAll("([\\[\\]\\\\^(){}*+?|$.])", "\\\\$1");
        }
    }

    /**
     * Rule 8.5: Regexp Consolidation
     * Consolidates multiple regexp queries on the same field into a single regexp with OR patterns
     */
    private class RegexpConsolidationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "RegexpConsolidation";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::consolidateRegexpInNode);
        }

        private JsonNode consolidateRegexpInNode(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Check bool clause types that allow regexp consolidation
            // Safe for should, filter, must_not (same logic as wildcard consolidation)
            for (String clauseType : Arrays.asList("should", "filter", "must_not")) {
                if (objectNode.has("bool") && objectNode.get("bool").has(clauseType)) {
                    JsonNode clauseNode = objectNode.get("bool").get(clauseType);

                    if (clauseNode.isArray()) {
                        ArrayNode clauseArray = (ArrayNode) clauseNode;
                        
                        // Group regexp queries by field
                        Map<String, List<JsonNode>> regexpsByField = new HashMap<>();
                        List<JsonNode> nonRegexps = new ArrayList<>();

                        for (JsonNode clause : clauseArray) {
                            if (clause.has("regexp")) {
                                // Direct regexp query
                                JsonNode regexpNode = clause.get("regexp");
                                Iterator<String> fieldNames = regexpNode.fieldNames();
                                if (fieldNames.hasNext()) {
                                    String field = fieldNames.next();
                                    regexpsByField.computeIfAbsent(field, k -> new ArrayList<>()).add(clause);
                                }
                            } else if (clause.has("bool") && isSimpleRegexpWrapper(clause)) {
                                // Regexp wrapped in simple bool (e.g., {"bool": {"must_not": [{"regexp": {...}}]}})
                                JsonNode regexpClause = extractRegexpFromSimpleWrapper(clause);
                                if (regexpClause != null && regexpClause.has("regexp")) {
                                    JsonNode regexpNode = regexpClause.get("regexp");
                                    Iterator<String> fieldNames = regexpNode.fieldNames();
                                    if (fieldNames.hasNext()) {
                                        String field = fieldNames.next();
                                        regexpsByField.computeIfAbsent(field, k -> new ArrayList<>()).add(clause); // Store the wrapper
                                    }
                                } else {
                                    nonRegexps.add(clause);
                                }
                            } else {
                                nonRegexps.add(clause);
                            }
                        }

                        // Consolidate regexp groups
                        boolean hasConsolidation = false;
                        ArrayNode newArray = objectMapper.createArrayNode();
                        
                        // Add non-regexp clauses first
                        for (JsonNode nonRegexp : nonRegexps) {
                            newArray.add(nonRegexp);
                        }

                        // Process each regexp field group
                        for (Map.Entry<String, List<JsonNode>> entry : regexpsByField.entrySet()) {
                            String field = entry.getKey();
                            List<JsonNode> regexps = entry.getValue();

                            if (regexps.size() >= 2) {
                                // Extract patterns (handle both direct and wrapped regexps)
                                List<String> patterns = new ArrayList<>();
                                String wrapperContext = null; // For preserving must_not, should, etc.
                                
                                for (JsonNode regexpContainer : regexps) {
                                    JsonNode regexpClause;
                                    if (regexpContainer.has("regexp")) {
                                        // Direct regexp
                                        regexpClause = regexpContainer;
                                    } else {
                                        // Extract from bool wrapper
                                        regexpClause = extractRegexpFromSimpleWrapper(regexpContainer);
                                        if (wrapperContext == null) {
                                            // Determine the wrapper context (must_not, should, etc.)
                                            JsonNode bool = regexpContainer.get("bool");
                                            for (String esClauseType : Arrays.asList("must", "should", "filter", "must_not")) {
                                                if (bool.has(esClauseType)) {
                                                    wrapperContext = esClauseType;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    
                                    if (regexpClause != null && regexpClause.has("regexp")) {
                                        String pattern = regexpClause.get("regexp").get(field).asText();
                                        patterns.add(pattern);
                                    }
                                }

                                // Create consolidated regexp with OR patterns
                                List<String> consolidatedPatterns = createConsolidatedRegexpPatterns(patterns);
                                
                                for (String consolidatedPattern : consolidatedPatterns) {
                                    ObjectNode consolidatedRegexp;
                                    
                                    if (wrapperContext != null) {
                                        // Preserve the bool wrapper context (e.g., must_not)
                                        consolidatedRegexp = objectMapper.createObjectNode();
                                        ObjectNode boolWrapper = objectMapper.createObjectNode();
                                        ArrayNode wrapperArray = objectMapper.createArrayNode();
                                        
                                        ObjectNode regexpQuery = objectMapper.createObjectNode();
                                        ObjectNode regexpContent = objectMapper.createObjectNode();
                                        regexpContent.put(field, consolidatedPattern);
                                        regexpQuery.set("regexp", regexpContent);
                                        wrapperArray.add(regexpQuery);
                                        
                                        boolWrapper.set(wrapperContext, wrapperArray);
                                        consolidatedRegexp.set("bool", boolWrapper);
                                    } else {
                                        // Direct regexp without wrapper
                                        consolidatedRegexp = objectMapper.createObjectNode();
                                        ObjectNode regexpContent = objectMapper.createObjectNode();
                                        regexpContent.put(field, consolidatedPattern);
                                        consolidatedRegexp.set("regexp", regexpContent);
                                    }
                                    
                                    newArray.add(consolidatedRegexp);
                                }

                                hasConsolidation = true;
                                log.debug("RegexpConsolidation: Consolidated {} regexp patterns on field '{}' into {} pattern(s)", 
                                         regexps.size(), field, consolidatedPatterns.size());
                            } else {
                                // Keep single regexp as-is
                                for (JsonNode regexp : regexps) {
                                    newArray.add(regexp);
                                }
                            }
                        }

                        // Update the clause array if consolidation occurred
                        if (hasConsolidation) {
                            ((ObjectNode) objectNode.get("bool")).set(clauseType, newArray);
                        }
                    }
                }
            }

            return objectNode;
        }

        /**
         * Creates consolidated regexp patterns from multiple individual patterns
         * Handles the 1000-character limit by splitting if necessary
         */
        private List<String> createConsolidatedRegexpPatterns(List<String> patterns) {
            if (patterns.isEmpty()) return new ArrayList<>();
            if (patterns.size() == 1) return patterns;

            // Create OR pattern: (pattern1|pattern2|pattern3)
            String consolidatedPattern = "(" + String.join("|", patterns) + ")";
            
            // Check if pattern exceeds Elasticsearch's 1000-character limit
            if (consolidatedPattern.length() <= 1000) {
                return Arrays.asList(consolidatedPattern);
            }

            // Split into multiple patterns if too long
            log.debug("RegexpConsolidation: Pattern exceeds 1000 chars ({}), splitting", consolidatedPattern.length());
            
            List<String> result = new ArrayList<>();
            List<String> currentBatch = new ArrayList<>();
            int currentLength = 2; // Account for opening parenthesis and initial pattern

            for (String pattern : patterns) {
                int additionalLength = pattern.length() + 1; // +1 for the "|" separator
                
                if (currentLength + additionalLength > 1000 && !currentBatch.isEmpty()) {
                    // Create regexp from current batch
                    String batchPattern = currentBatch.size() == 1 ? 
                        currentBatch.get(0) : 
                        "(" + String.join("|", currentBatch) + ")";
                    result.add(batchPattern);
                    
                    // Start new batch
                    currentBatch.clear();
                    currentLength = 2;
                }
                
                currentBatch.add(pattern);
                currentLength += additionalLength;
            }

            // Add final batch
            if (!currentBatch.isEmpty()) {
                String batchPattern = currentBatch.size() == 1 ? 
                    currentBatch.get(0) : 
                    "(" + String.join("|", currentBatch) + ")";
                result.add(batchPattern);
            }

            return result;
        }
        
        /**
         * Checks if a bool wrapper contains a single regexp query (similar to wildcard wrapper logic)
         */
        private boolean isSimpleRegexpWrapper(JsonNode boolWrapper) {
            if (!boolWrapper.has("bool")) return false;

            JsonNode bool = boolWrapper.get("bool");

            // Check for single-clause bool with one regexp
            int clauseCount = 0;
            JsonNode targetClause = null;

            for (String clauseType : Arrays.asList("must", "should", "filter", "must_not")) {
                if (bool.has(clauseType)) {
                    clauseCount++;
                    targetClause = bool.get(clauseType);
                }
            }

            // Must be exactly one clause type
            if (clauseCount != 1 || targetClause == null) return false;

            // Check if the clause contains exactly one regexp
            if (targetClause.isArray()) {
                ArrayNode array = (ArrayNode) targetClause;
                return array.size() == 1 && array.get(0).has("regexp");
            } else {
                return targetClause.has("regexp");
            }
        }

        /**
         * Extracts the regexp query from a simple bool wrapper
         */
        private JsonNode extractRegexpFromSimpleWrapper(JsonNode boolWrapper) {
            JsonNode bool = boolWrapper.get("bool");

            for (String clauseType : Arrays.asList("must", "should", "filter", "must_not")) {
                if (bool.has(clauseType)) {
                    JsonNode clause = bool.get(clauseType);
                    if (clause.isArray()) {
                        ArrayNode array = (ArrayNode) clause;
                        if (array.size() == 1 && array.get(0).has("regexp")) {
                            return array.get(0);
                        }
                    } else if (clause.has("regexp")) {
                        return clause;
                    }
                }
            }
            return null;
        }
    }

    /**
     * Rule 8.6: Must_Not Consolidation Rule  
     * Consolidates multiple bool.must_not wrappers in must arrays into a single consolidated must_not
     * Specifically handles patterns like:
     * [{"bool": {"must_not": [{"regexp": ...}]}}, {"bool": {"must_not": [{"regexp": ...}]}}]
     * Into a single consolidated structure
     */
    private class MustNotConsolidationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "MustNotConsolidation";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::consolidateMustNotWrappersInNode);
        }

        private JsonNode consolidateMustNotWrappersInNode(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Look specifically for bool.must arrays that contain multiple bool.must_not wrappers
            if (objectNode.has("bool")) {
                JsonNode boolNode = objectNode.get("bool");
                if (boolNode.has("must") && boolNode.get("must").isArray()) {
                    ArrayNode mustArray = (ArrayNode) boolNode.get("must");
                    
                    // Group must_not wrappers by the field they operate on
                    Map<String, List<JsonNode>> mustNotWrappersByField = new HashMap<>();
                    List<JsonNode> nonMustNotItems = new ArrayList<>();
                    
                    for (JsonNode mustItem : mustArray) {
                        if (isMustNotWrapper(mustItem)) {
                            // Extract the inner condition (regexp/wildcard)
                            JsonNode innerCondition = extractInnerConditionFromMustNotWrapper(mustItem);
                            if (innerCondition != null) {
                                String field = extractFieldFromCondition(innerCondition);
                                if (field != null) {
                                    mustNotWrappersByField.computeIfAbsent(field, k -> new ArrayList<>()).add(mustItem);
                                    continue;
                                }
                            }
                        }
                        nonMustNotItems.add(mustItem);
                    }
                    
                    // Check if we have multiple must_not wrappers for any field
                    boolean hasConsolidation = false;
                    ArrayNode newMustArray = objectMapper.createArrayNode();
                    
                    // Add non-must_not items first
                    for (JsonNode nonMustNot : nonMustNotItems) {
                        newMustArray.add(nonMustNot);
                    }
                    
                    // Process each field group
                    for (Map.Entry<String, List<JsonNode>> entry : mustNotWrappersByField.entrySet()) {
                        String field = entry.getKey();
                        List<JsonNode> wrappers = entry.getValue();
                        
                        if (wrappers.size() >= 2) {
                            // Check for case-insensitive wildcards first
                            for (JsonNode wrapper : wrappers) {
                                JsonNode innerCondition = extractInnerConditionFromMustNotWrapper(wrapper);
                                if (innerCondition != null && innerCondition.has("wildcard")) {
                                    JsonNode wildcardNode = innerCondition.get("wildcard");
                                    if (wildcardNode.has(field)) {
                                        JsonNode fieldValue = wildcardNode.get(field);
                                        String pattern = fieldValue.isObject() && fieldValue.get("value") != null ?
                                            fieldValue.get("value").asText() : fieldValue.asText();

                                        if (hasSpecialCharacters(pattern, fieldValue)) {
                                            // Skip consolidation for special patterns or flags
                                            newMustArray.add(wrapper);
                                            continue;
                                        }
                                    }
                                }
                            }

                            // Extract patterns from non-case-insensitive wildcards
                            List<String> patterns = new ArrayList<>();
                            for (JsonNode wrapper : wrappers) {
                                JsonNode innerCondition = extractInnerConditionFromMustNotWrapper(wrapper);
                                if (innerCondition != null && innerCondition.has("wildcard")) {
                                    JsonNode wildcardNode = innerCondition.get("wildcard");
                                    if (wildcardNode.has(field)) {
                                        JsonNode fieldValue = wildcardNode.get(field);
                                        String pattern = fieldValue.isObject() && fieldValue.get("value") != null ?
                                            fieldValue.get("value").asText() : fieldValue.asText();

                                        if (hasSpecialCharacters(pattern, fieldValue)) {
                                            // Skip special patterns and flags
                                            continue;
                                        }
                                    }
                                }
                                String pattern = extractPatternFromCondition(innerCondition);
                                if (pattern != null) {
                                    patterns.add(pattern);
                                }
                            }
                            
                            // Create consolidated regexp with OR patterns
                            if (CollectionUtils.isNotEmpty(patterns)) {
                                String consolidatedPattern = "(" + String.join("|", patterns) + ")";

                                // Create new consolidated must_not wrapper
                                ObjectNode consolidatedWrapper = objectMapper.createObjectNode();
                                ObjectNode boolWrapper = objectMapper.createObjectNode();
                                ArrayNode mustNotArray = objectMapper.createArrayNode();

                                ObjectNode regexpQuery = objectMapper.createObjectNode();
                                ObjectNode regexpContent = objectMapper.createObjectNode();
                                regexpContent.put(field, consolidatedPattern);
                                regexpQuery.set("regexp", regexpContent);
                                mustNotArray.add(regexpQuery);

                                boolWrapper.set("must_not", mustNotArray);
                                consolidatedWrapper.set("bool", boolWrapper);

                                newMustArray.add(consolidatedWrapper);
                                hasConsolidation = true;

                                log.debug("MustNotConsolidation: Consolidated {} must_not wrappers on field '{}' into single regexp",
                                        wrappers.size(), field);
                            }
                        } else {
                            // Keep single wrapper as-is
                            for (JsonNode wrapper : wrappers) {
                                newMustArray.add(wrapper);
                            }
                        }
                    }
                    
                    // Update the must array if consolidation occurred
                    if (hasConsolidation) {
                        ((ObjectNode) boolNode).set("must", newMustArray);
                    }
                }
            }

            return objectNode;
        }
        
        private boolean isMustNotWrapper(JsonNode node) {
            if (!node.has("bool")) return false;
            
            JsonNode bool = node.get("bool");
            
            // Check if it's a simple bool with only must_not clause
            int clauseCount = 0;
            for (String clauseType : Arrays.asList("must", "should", "filter", "must_not")) {
                if (bool.has(clauseType)) {
                    clauseCount++;
                }
            }
            
            return clauseCount == 1 && bool.has("must_not");
        }
        
        private JsonNode extractInnerConditionFromMustNotWrapper(JsonNode mustNotWrapper) {
            if (!mustNotWrapper.has("bool")) return null;
            
            JsonNode mustNotArray = mustNotWrapper.get("bool").get("must_not");
            if (mustNotArray.isArray() && mustNotArray.size() == 1) {
                return mustNotArray.get(0);
            }
            return null;
        }
        
        private String extractFieldFromCondition(JsonNode condition) {
            if (condition == null) return null;
            
            // Handle regexp, wildcard, term, etc.
            for (String queryType : Arrays.asList("regexp", "wildcard", "term", "terms")) {
                if (condition.has(queryType)) {
                    JsonNode queryContent = condition.get(queryType);
                    Iterator<String> fieldNames = queryContent.fieldNames();
                    if (fieldNames.hasNext()) {
                        return fieldNames.next();
                    }
                }
            }
            return null;
        }
        
        private String extractPatternFromCondition(JsonNode condition) {
            if (condition == null) return null;
            
            // Handle regexp, wildcard patterns
            for (String queryType : Arrays.asList("regexp", "wildcard")) {
                if (condition.has(queryType)) {
                    JsonNode queryContent = condition.get(queryType);
                    Iterator<String> fieldNames = queryContent.fieldNames();
                    if (fieldNames.hasNext()) {
                        String field = fieldNames.next();
                        return queryContent.get(field).asText();
                    }
                }
            }
            return null;
        }
    }

    /**
     * Rule 9: QualifiedName Hierarchy Optimization
     * Converts suffix wildcards to qualifiedNameHierarchy term queries and database term queries
     */
    private class QualifiedNameHierarchyRule implements OptimizationRule {

        @Override
        public String getName() {
            return "QualifiedNameHierarchy";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeQualifiedNameWildcards);
        }

        private JsonNode optimizeQualifiedNameWildcards(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("wildcard")) {
                JsonNode wildcardNode = objectNode.get("wildcard");

                // ROBUST CHECK: Handle ANY field ending with qualified name patterns (both cases)
                Iterator<String> fieldNames = wildcardNode.fieldNames();
                while (fieldNames.hasNext()) {
                    String currentField = fieldNames.next();

                    // Support both "qualifiedName" and "QualifiedName" patterns for maximum compatibility
                    boolean isQualifiedNameField = currentField.endsWith("qualifiedName") ||
                            currentField.endsWith("QualifiedName");

                    if (isQualifiedNameField) {
                        String pattern = wildcardNode.get(currentField).asText();

                        log.debug("QualifiedNameHierarchyRule: Checking field '{}' with pattern '{}'", currentField, pattern);

                        // PRIORITY 1: UI "Contains" Optimization for patterns like "*default/something*"
                        // This catches UI-generated wildcard queries from "contains" searches
                        if (pattern.startsWith("*") && pattern.endsWith("*") && pattern.contains("default/")) {
                            // Extract the core value by removing prefix * and suffix *
                            String coreValue = pattern.substring(1, pattern.length() - 1);
                            
                            // If the core value starts with "default/", convert to efficient term query
                            if (coreValue.startsWith("default/")) {
                                log.info("QualifiedNameHierarchyRule: UI Contains optimization - converting wildcard '*{}*' to term query on __qualifiedNameHierarchy", coreValue);
                                
                                // Create term query with __qualifiedNameHierarchy for much better performance
                                ObjectNode termNode = objectMapper.createObjectNode();
                                ObjectNode termQuery = objectMapper.createObjectNode();
                                termQuery.put("__qualifiedNameHierarchy", coreValue);
                                termNode.set("term", termQuery);
                                return termNode;
                            }
                        }

                        // NEW: Special handling for default/*/*/*/* patterns - convert to terms query
                        if (pattern.startsWith("default/") && pattern.endsWith("*")) {
                            String pathWithoutTrailingWildcard = pattern.substring(0, pattern.length() - 1);

                            log.debug("QualifiedNameHierarchyRule: Found default/*/*/* pattern, converting to terms query: '{}'", pathWithoutTrailingWildcard);

                            // Create terms query with __qualifiedNameHierarchy for better performance
                            ObjectNode termsNode = objectMapper.createObjectNode();
                            ObjectNode termsQuery = objectMapper.createObjectNode();
                            ArrayNode termsArray = objectMapper.createArrayNode();
                            termsArray.add(pathWithoutTrailingWildcard);
                            termsQuery.set("__qualifiedNameHierarchy", termsArray);
                            termsNode.set("terms", termsQuery);
                            return termsNode;
                        }

                        // EXISTING: Handle suffix wildcards for simple prefixes
                        if (pattern.endsWith("*") && !pattern.startsWith("*") &&
                                pattern.length() > 1 && pattern.contains("/")) {

                            String prefix = pattern.substring(0, pattern.length() - 1);
                            if (!prefix.contains("*")) {
                                log.debug("QualifiedNameHierarchyRule: Transforming {} to __qualifiedNameHierarchy with prefix '{}'",
                                        currentField, prefix);
                                //remove last char from prefix if it is *
                                if (prefix.length() > 1 && prefix.endsWith("/")) {
                                    prefix = prefix.substring(0, prefix.length() - 1);
                                }

                                // Create term query with __qualifiedNameHierarchy
                                ObjectNode termNode = objectMapper.createObjectNode();
                                ObjectNode termQuery = objectMapper.createObjectNode();
                                termQuery.put("__qualifiedNameHierarchy", prefix);
                                termNode.set("term", termQuery);
                                return termNode;
                            } else {
                                log.debug("QualifiedNameHierarchyRule: Skipping {} - prefix contains '*': '{}'", currentField, prefix);
                            }
                        } else {
                            log.debug("QualifiedNameHierarchyRule: Skipping {} - pattern doesn't match criteria: '{}'", currentField, pattern);
                        }
                    }
                }
            }

            return objectNode;
        }
    }

    /**
     * Rule 12: Duplicate Removal
     * Removes duplicate filters and consolidates similar clauses
     */
    private class DuplicateRemovalRule implements OptimizationRule {

        @Override
        public String getName() {
            return "DuplicateRemoval";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::removeDuplicates);
        }

        private JsonNode removeDuplicates(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                for (String clause : Arrays.asList("must", "should", "filter", "must_not")) {
                    if (boolNode.has(clause) && boolNode.get(clause).isArray()) {
                        ArrayNode array = (ArrayNode) boolNode.get(clause);
                        ArrayNode deduplicated = removeDuplicateFromArray(array);
                        if (deduplicated.size() != array.size()) {
                            boolNode.set(clause, deduplicated);
                        }
                    }
                }
            }

            return objectNode;
        }

        private ArrayNode removeDuplicateFromArray(ArrayNode array) {
            Set<String> seen = new LinkedHashSet<>();
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode item : array) {
                String itemString = item.toString();
                if (!seen.contains(itemString)) {
                    seen.add(itemString);
                    result.add(item);
                }
            }

            return result;
        }
    }

    /**
     * Rule 13: Context-Aware Filter Optimization
     * Intelligently moves must clauses to filter context when safe to do so
     *
     * SAFE CASES:
     * 1. Inside function_score queries (scoring handled by function_score)
     * 2. When explicit filter-only context is detected
     *
     * PRESERVES SCORING:
     * - Regular bool queries keep must clauses for scoring
     * - Only optimizes when scoring semantics won't be affected
     */
    private class FilterContextRule implements OptimizationRule {

        @Override
        public String getName() {
            return "ContextAwareFilterOptimization";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimizeWithContext(query.deepCopy(), this::optimizeFilterContext, false);
        }

        private JsonNode optimizeFilterContext(JsonNode node, boolean isInFunctionScore) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Detect if we're entering a function_score context
            if (objectNode.has("function_score")) {
                JsonNode functionScoreQuery = objectNode.get("function_score").get("query");
                if (functionScoreQuery != null) {
                    // Recursively optimize the inner query in function_score context
                    JsonNode optimizedInnerQuery = optimizeFilterContext(functionScoreQuery, true);
                    ((ObjectNode) objectNode.get("function_score")).set("query", optimizedInnerQuery);
                }
                return objectNode;
            }

            // Only optimize must->filter when in function_score context or other safe contexts
            if (isInFunctionScore && objectNode.has("bool")) {
                ObjectNode boolNode = (ObjectNode) objectNode.get("bool");

                if (boolNode.has("must") && boolNode.get("must").isArray()) {
                    ArrayNode mustArray = (ArrayNode) boolNode.get("must");
                    ArrayNode newMustArray = objectMapper.createArrayNode();
                    ArrayNode filterArray = boolNode.has("filter") && boolNode.get("filter").isArray()
                            ? (ArrayNode) boolNode.get("filter").deepCopy()
                            : objectMapper.createArrayNode();

                    for (JsonNode clause : mustArray) {
                        // In function_score context, it's safe to move filter-like clauses to filter context
                        if (isFilterCandidate(clause)) {
                            filterArray.add(clause);
                            // Optimization is tracked at rule level by recordRuleApplication
                        } else {
                            // Keep complex scoring clauses in must (like match, multi_match with boost)
                            newMustArray.add(clause);
                        }
                    }

                    // Update the bool query
                    if (newMustArray.size() > 0) {
                        boolNode.set("must", newMustArray);
                    } else {
                        boolNode.remove("must");
                    }

                    if (filterArray.size() > 0) {
                        boolNode.set("filter", filterArray);
                    }
                }
            }

            return objectNode;
        }

        private boolean isFilterCandidate(JsonNode clause) {
            // These query types don't contribute meaningful scoring and are safe to move to filter context
            return clause.has("term") || clause.has("terms") || clause.has("range") ||
                    clause.has("exists") || clause.has("regexp") || clause.has("wildcard") ||
                    clause.has("ids") || clause.has("prefix");
        }

        /**
         * Enhanced traversal that tracks function_score context and skips aggregations
         */
        private JsonNode traverseAndOptimizeWithContext(JsonNode node,
                                                        java.util.function.BiFunction<JsonNode, Boolean, JsonNode> optimizer,
                                                        boolean isInFunctionScore) {
            return traverseAndOptimizeWithAggContext(node, optimizer, isInFunctionScore, false);
        }

        private JsonNode traverseAndOptimizeWithAggContext(JsonNode node,
                                                           java.util.function.BiFunction<JsonNode, Boolean, JsonNode> optimizer,
                                                           boolean isInFunctionScore, boolean inAggregationContext) {

            if (node.isObject()) {
                ObjectNode objectNode = (ObjectNode) node.deepCopy();

                // Check if this node introduces function_score context
                boolean newFunctionScoreContext = isInFunctionScore || objectNode.has("function_score");

                // Apply optimization with context ONLY if not in aggregation
                if (!inAggregationContext) {
                    objectNode = (ObjectNode) optimizer.apply(objectNode, newFunctionScoreContext);
                } else {
                    log.debug("FilterContextRule: Skipping optimization in aggregation context");
                }

                // Recursively traverse children with context
                Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();

                    // Check if we're entering an aggregation context
                    boolean childInAggContext = inAggregationContext || field.getKey().equals("aggs");

                    JsonNode optimizedChild = traverseAndOptimizeWithAggContext(
                            field.getValue(), optimizer, newFunctionScoreContext, childInAggContext);
                    objectNode.set(field.getKey(), optimizedChild);
                }

                return objectNode;
            } else if (node.isArray()) {
                ArrayNode arrayNode = objectMapper.createArrayNode();
                for (JsonNode item : node) {
                    JsonNode optimizedItem = traverseAndOptimizeWithAggContext(
                            item, optimizer, isInFunctionScore, inAggregationContext);
                    arrayNode.add(optimizedItem);
                }
                return arrayNode;
            }

            return node;
        }
    }

    /**
     * Rule 14: Function Score Optimization
     * Optimizes function_score queries by removing duplicates and reordering
     */
    private class FunctionScoreOptimizationRule implements OptimizationRule {

        @Override
        public String getName() {
            return "FunctionScoreOptimization";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::optimizeFunctionScore);
        }

        private JsonNode optimizeFunctionScore(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("function_score") && objectNode.get("function_score").has("functions")) {
                ArrayNode functions = (ArrayNode) objectNode.get("function_score").get("functions");

                List<JsonNode> uniqueFunctions = new ArrayList<>();
                Set<String> seenFilters = new LinkedHashSet<>();

                for (JsonNode function : functions) {
                    String filterString = function.has("filter") ? function.get("filter").toString() : "no_filter";
                    if (!seenFilters.contains(filterString)) {
                        seenFilters.add(filterString);
                        uniqueFunctions.add(function);
                    }
                }

                // Sort by weight (descending)
                uniqueFunctions.sort((a, b) -> {
                    double weightA = a.has("weight") ? a.get("weight").asDouble(1.0) : 1.0;
                    double weightB = b.has("weight") ? b.get("weight").asDouble(1.0) : 1.0;
                    return Double.compare(weightB, weightA);
                });

                if (uniqueFunctions.size() != functions.size()) {
                    ArrayNode optimizedFunctions = objectMapper.createArrayNode();
                    uniqueFunctions.forEach(optimizedFunctions::add);
                    ((ObjectNode) objectNode.get("function_score")).set("functions", optimizedFunctions);
                }
            }

            return objectNode;
        }
    }

    /**
     * Rule 15: Duplicate Filter Removal
     * Removes duplicate filters between main query and function_score
     */
    private class DuplicateFilterRemovalRule implements OptimizationRule {

        @Override
        public String getName() {
            return "DuplicateFilterRemoval";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::removeDuplicateFilters);
        }

        private JsonNode removeDuplicateFilters(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            if (objectNode.has("function_score")) {
                JsonNode functionScore = objectNode.get("function_score");

                if (functionScore.has("query") && functionScore.has("functions")) {
                    Set<String> mainQueryFilters = extractMainQueryFilters(functionScore.get("query"));
                    ArrayNode functions = (ArrayNode) functionScore.get("functions");

                    ArrayNode optimizedFunctions = removeDuplicateFunctionFilters(functions, mainQueryFilters);

                    if (optimizedFunctions.size() != functions.size()) {
                        ((ObjectNode) functionScore).set("functions", optimizedFunctions);
                    }
                }
            }

            return objectNode;
        }

        private Set<String> extractMainQueryFilters(JsonNode mainQuery) {
            Set<String> filters = new HashSet<>();

            if (mainQuery.has("bool")) {
                JsonNode boolQuery = mainQuery.get("bool");

                if (boolQuery.has("filter")) {
                    addFiltersFromNode(boolQuery.get("filter"), filters);
                }
                if (boolQuery.has("must")) {
                    addFiltersFromNode(boolQuery.get("must"), filters);
                }
            }

            return filters;
        }

        private void addFiltersFromNode(JsonNode node, Set<String> filters) {
            if (node.isArray()) {
                for (JsonNode item : node) {
                    addFiltersFromNode(item, filters);
                }
            } else if (node.isObject()) {
                if (node.has("bool")) {
                    JsonNode boolNode = node.get("bool");
                    if (boolNode.has("must")) {
                        addFiltersFromNode(boolNode.get("must"), filters);
                    }
                    if (boolNode.has("filter")) {
                        addFiltersFromNode(boolNode.get("filter"), filters);
                    }
                } else if (node.has("term")) {
                    JsonNode termNode = node.get("term");
                    Iterator<String> fieldNames = termNode.fieldNames();
                    fieldNames.forEachRemaining(field -> {
                        String value = termNode.get(field).asText();
                        filters.add("term:" + field + ":" + value);
                    });
                } else if (node.has("terms")) {
                    JsonNode termsNode = node.get("terms");
                    Iterator<String> fieldNames = termsNode.fieldNames();
                    fieldNames.forEachRemaining(field -> {
                        String values = termsNode.get(field).toString();
                        filters.add("terms:" + field + ":" + values);
                    });
                }
            }
        }

        private ArrayNode removeDuplicateFunctionFilters(ArrayNode functions, Set<String> mainQueryFilters) {
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode function : functions) {
                if (function.has("filter")) {
                    String functionFilterKey = extractFilterKey(function.get("filter"));

                    if (!mainQueryFilters.contains(functionFilterKey)) {
                        result.add(function);
                    }
                } else {
                    result.add(function);
                }
            }

            return result;
        }

        private String extractFilterKey(JsonNode filter) {
            if (filter.has("match")) {
                JsonNode matchNode = filter.get("match");
                Iterator<String> fieldNames = matchNode.fieldNames();
                if (fieldNames.hasNext()) {
                    String field = fieldNames.next();
                    String value = matchNode.get(field).asText();
                    return "term:" + field + ":" + value;
                }
            } else if (filter.has("term")) {
                JsonNode termNode = filter.get("term");
                Iterator<String> fieldNames = termNode.fieldNames();
                if (fieldNames.hasNext()) {
                    String field = fieldNames.next();
                    String value = termNode.get(field).asText();
                    return "term:" + field + ":" + value;
                }
            } else if (filter.has("terms")) {
                JsonNode termsNode = filter.get("terms");
                Iterator<String> fieldNames = termsNode.fieldNames();
                if (fieldNames.hasNext()) {
                    String field = fieldNames.next();
                    String values = termsNode.get(field).toString();
                    return "terms:" + field + ":" + values;
                }
            }

            return filter.toString();
        }
    }

    /**
     * Rule 12: Bool Flattening
     * Flattens bool queries that contain only must and must_not clauses.
     * Converts filter.bool.must to filter array and preserves must_not clauses.
     *
     * Example:
     * filter: { bool: { must: [...], must_not: [...] } }
     * becomes:
     * bool: { filter: [...], must_not: [...] }
     */
    private class BoolFlatteningRule implements OptimizationRule {

        @Override
        public String getName() {
            return "BoolFlattening";
        }

        @Override
        public JsonNode apply(JsonNode query) {
            return traverseAndOptimize(query.deepCopy(), this::flattenBoolStructures);
        }

        private JsonNode flattenBoolStructures(JsonNode node) {
            if (!node.isObject()) return node;

            ObjectNode objectNode = (ObjectNode) node;

            // Look for filter.bool patterns that can be flattened
            if (objectNode.has("filter")) {
                JsonNode filterNode = objectNode.get("filter");

                // Case 1: filter is an object with a bool
                if (filterNode.isObject() && filterNode.has("bool")) {
                    JsonNode boolNode = filterNode.get("bool");

                    log.debug("BoolFlattening: Found filter.bool structure to analyze: {}", boolNode.toString());

                    // Check if this bool has only must and must_not (no should, no existing filter)
                    if (canFlattenBool(boolNode)) {
                        log.debug("BoolFlattening: Bool structure can be flattened");
                        JsonNode optimizedNode = createFlattenedBool(boolNode, objectNode);
                        if (optimizedNode != null) {
                            log.debug("BoolFlattening: Successfully flattened structure");
                            return optimizedNode;
                        } else {
                            log.warn("BoolFlattening: createFlattenedBool returned null");
                        }
                    } else {
                        log.debug("BoolFlattening: Bool structure cannot be flattened (has should/filter clauses)");
                    }
                }

                // Case 2: filter is an array with nested bool patterns
                else if (filterNode.isArray()) {
                    ArrayNode filterArray = (ArrayNode) filterNode;
                    if (filterArray.size() == 1) {
                        JsonNode singleFilter = filterArray.get(0);
                        if (singleFilter.has("bool")) {
                            JsonNode boolNode = singleFilter.get("bool");
                            if (canFlattenBool(boolNode)) {
                                JsonNode optimizedNode = createFlattenedBool(boolNode, objectNode);
                                if (optimizedNode != null) {
                                    return optimizedNode;
                                }
                            }
                        }
                    }
                }
            }

            return objectNode;
        }

        private boolean canFlattenBool(JsonNode boolNode) {
            // Can flatten if:
            // 1. Has must clauses (the positive conditions)
            // 2. May have must_not clauses (the negative conditions)
            // 3. No should clauses (would change scoring semantics)
            // 4. No existing filter clauses (would complicate merging)
            return boolNode.has("must") &&
                    !boolNode.has("should") &&
                    !boolNode.has("filter");
        }

        private JsonNode createFlattenedBool(JsonNode originalBool, ObjectNode parentNode) {
            try {
                // FIXED: Instead of creating a new wrapper, modify the existing parent bool structure
                ObjectNode resultNode = parentNode.deepCopy();

                // Extract flattened filter array from the nested bool.must
                ArrayNode flattenedFilter = objectMapper.createArrayNode();
                if (originalBool.has("must")) {
                    JsonNode mustNode = originalBool.get("must");
                    if (mustNode.isArray()) {
                        for (JsonNode mustItem : mustNode) {
                            flattenedFilter.add(mustItem);
                        }
                    } else {
                        flattenedFilter.add(mustNode);
                    }
                }

                // Set the flattened filter array directly on the parent bool
                resultNode.set("filter", flattenedFilter);

                // If original bool had must_not, add it to the parent bool level
                if (originalBool.has("must_not")) {
                    resultNode.set("must_not", originalBool.get("must_not"));
                }

                // Copy any other properties from original bool (like minimum_should_match, boost)
                originalBool.fieldNames().forEachRemaining(fieldName -> {
                    if (!fieldName.equals("must") && !fieldName.equals("must_not") && !fieldName.equals("filter")) {
                        resultNode.set(fieldName, originalBool.get(fieldName));
                    }
                });

                log.debug("BoolFlattening: Flattened nested filter.bool.must structure");

                return resultNode;

            } catch (Exception e) {
                log.error("Error in BoolFlattening createFlattenedBool: {}", e.getMessage(), e);
                // If anything goes wrong, return null to skip optimization
                return null;
            }
        }
    }

    // Helper classes and interfaces

    private interface OptimizationRule {
        String getName();
        JsonNode apply(JsonNode query);
    }

    private static class WildcardPattern {
        final String field;
        final String pattern;

        WildcardPattern(String field, String pattern) {
            this.field = field;
            this.pattern = pattern;
        }

        String extractPattern() {
            return pattern.replaceAll("\\*", "");
        }
    }

    // Helper class for multi-match optimization
    private static class MultiMatchQuery {
        final JsonNode originalNode;
        final String queryText;
        final List<String> fields;
        final double boost;

        MultiMatchQuery(JsonNode node, String queryText) {
            this.originalNode = node;
            this.queryText = queryText;
            this.fields = extractFields(node);
            this.boost = extractBoost(node);
        }

        private List<String> extractFields(JsonNode node) {
            List<String> fields = new ArrayList<>();
            JsonNode multiMatch = node.get("multi_match");
            if (multiMatch.has("fields") && multiMatch.get("fields").isArray()) {
                for (JsonNode field : multiMatch.get("fields")) {
                    fields.add(field.asText());
                }
            }
            return fields;
        }

        private double extractBoost(JsonNode node) {
            JsonNode multiMatch = node.get("multi_match");
            if (multiMatch.has("boost")) {
                return multiMatch.get("boost").asDouble();
            }
            return 1.0;
        }

        double getTotalBoost() {
            return boost * fields.size();
        }

        int getFieldCount() {
            return fields.size();
        }
    }

    private JsonNode traverseAndOptimize(JsonNode node, java.util.function.Function<JsonNode, JsonNode> optimizer) {
        return traverseAndOptimizeWithContext(node, optimizer, false);
    }

    private JsonNode traverseAndOptimizeWithContext(JsonNode node, java.util.function.Function<JsonNode, JsonNode> optimizer, boolean inAggregationContext) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;

            // First optimize children
            Iterator<String> fieldNames = objectNode.fieldNames();
            List<String> fieldsToUpdate = new ArrayList<>();
            Map<String, JsonNode> newValues = new HashMap<>();

            fieldNames.forEachRemaining(fieldName -> {
                JsonNode childNode = objectNode.get(fieldName);

                // SKIP OPTIMIZATION IN AGGREGATIONS: Check if we're entering an aggregation context
                boolean childInAggContext = inAggregationContext || fieldName.equals("aggs");

                if (childInAggContext) {
                    log.debug("Skipping optimization in aggregation context for field: {}", fieldName);
                    // In aggregation context, just preserve the structure without optimization
                    JsonNode preservedChild = preserveAggregationStructure(childNode);
                    if (preservedChild != childNode) {
                        fieldsToUpdate.add(fieldName);
                        newValues.put(fieldName, preservedChild);
                    }
                } else {
                    // Normal optimization for non-aggregation context
                    JsonNode optimizedChild = traverseAndOptimizeWithContext(childNode, optimizer, false);
                    if (optimizedChild != childNode) {
                        fieldsToUpdate.add(fieldName);
                        newValues.put(fieldName, optimizedChild);
                    }
                }
            });

            // Update modified fields
            for (String field : fieldsToUpdate) {
                objectNode.set(field, newValues.get(field));
            }

            // Then optimize this node ONLY if not in aggregation context
            if (inAggregationContext) {
                log.debug("Preserving node structure in aggregation context");
                return objectNode;
            } else {
                return optimizer.apply(objectNode);
            }
        } else if (node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            ArrayNode optimizedArray = objectMapper.createArrayNode();

            for (JsonNode child : arrayNode) {
                if (inAggregationContext) {
                    // Preserve array items in aggregation context
                    optimizedArray.add(preserveAggregationStructure(child));
                } else {
                    optimizedArray.add(traverseAndOptimizeWithContext(child, optimizer, false));
                }
            }

            return optimizedArray;
        }

        return node;
    }

    /**
     * Preserves aggregation structure without applying optimization rules
     * Recursively preserves nested aggregation structures
     */
    private JsonNode preserveAggregationStructure(JsonNode node) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            ObjectNode result = objectNode.deepCopy();

            // Recursively preserve all child structures in aggregation context
            Iterator<String> fieldNames = result.fieldNames();
            List<String> fieldsToUpdate = new ArrayList<>();
            Map<String, JsonNode> newValues = new HashMap<>();

            fieldNames.forEachRemaining(fieldName -> {
                JsonNode childNode = result.get(fieldName);
                JsonNode preservedChild = preserveAggregationStructure(childNode);
                if (preservedChild != childNode) {
                    fieldsToUpdate.add(fieldName);
                    newValues.put(fieldName, preservedChild);
                }
            });

            // Update any modified fields
            for (String field : fieldsToUpdate) {
                result.set(field, newValues.get(field));
            }

            return result;
        } else if (node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            ArrayNode result = objectMapper.createArrayNode();

            for (JsonNode child : arrayNode) {
                result.add(preserveAggregationStructure(child));
            }

            return result;
        }

        return node;
    }

    /**
     * Optimization metrics and results
     */
    public static class OptimizationMetrics {
        private long startTime;
        private int originalSize;
        private int originalNesting;
        private final List<String> appliedRules = new ArrayList<>();

        void startOptimization(JsonNode query) {
            startTime = System.currentTimeMillis();
            originalSize = query.toString().length();
            originalNesting = calculateNestingDepth(query);
            appliedRules.clear();
        }

        void recordRuleApplication(String ruleName) {
            appliedRules.add(ruleName);
        }

        Result finishOptimization(JsonNode original, JsonNode optimized) {
            long duration = System.currentTimeMillis() - startTime;
            int optimizedSize = optimized.toString().length();
            int optimizedNesting = calculateNestingDepth(optimized);

            return new Result(
                    originalSize, optimizedSize,
                    originalNesting, optimizedNesting,
                    duration, new ArrayList<>(appliedRules)
            );
        }

        private int calculateNestingDepth(JsonNode node) {
            if (!node.isContainerNode()) return 0;

            int maxDepth = 0;
            if (node.isObject()) {
                for (JsonNode child : node) {
                    maxDepth = Math.max(maxDepth, calculateNestingDepth(child));
                }
            } else if (node.isArray()) {
                for (JsonNode child : node) {
                    maxDepth = Math.max(maxDepth, calculateNestingDepth(child));
                }
            }

            return maxDepth + 1;
        }

        public static class Result {
            public final int originalSize;
            public final int optimizedSize;
            public final int originalNesting;
            public final int optimizedNesting;
            public final long optimizationTime;
            public final List<String> appliedRules;

            Result(int originalSize, int optimizedSize, int originalNesting,
                   int optimizedNesting, long optimizationTime, List<String> appliedRules) {
                this.originalSize = originalSize;
                this.optimizedSize = optimizedSize;
                this.originalNesting = originalNesting;
                this.optimizedNesting = optimizedNesting;
                this.optimizationTime = optimizationTime;
                this.appliedRules = new ArrayList<>(appliedRules);
            }

            public double getSizeReduction() {
                if (originalSize == 0) return 0.0;
                return ((double) (originalSize - optimizedSize) / originalSize) * 100;
            }

            public double getNestingReduction() {
                if (originalNesting == 0) return 0.0;
                return ((double) (originalNesting - optimizedNesting) / originalNesting) * 100;
            }
        }
    }

    public static class OptimizationResult {
        private String optimizedQuery;
        private OptimizationMetrics.Result metrics;
        private String originalQuery;
        private boolean validationPassed = true;
        private String validationFailureReason;
        private boolean shouldSkipExecution = false;
        private String skipExecutionReason;

        OptimizationResult(String optimizedQuery, OptimizationMetrics.Result metrics) {
            this.optimizedQuery = optimizedQuery;
            this.metrics = metrics;
        }

        // Constructor for validation failure cases
        public OptimizationResult() {
            this.optimizedQuery = null;
            this.metrics = null;
        }

        public OptimizationMetrics.Result getMetrics() {
            return metrics;
        }

        public void setOptimizedQuery(String optimizedQuery) {
            this.optimizedQuery = optimizedQuery;
        }

        public String getOptimizedQuery() {
            return optimizedQuery;
        }

        public void setOriginalQuery(String originalQuery) {
            this.originalQuery = originalQuery;
        }

        public void setValidationPassed(boolean validationPassed) {
            this.validationPassed = validationPassed;
        }

        public void setValidationFailureReason(String validationFailureReason) {
            this.validationFailureReason = validationFailureReason;
        }

        public boolean isValidationPassed() {
            return validationPassed;
        }

        public String getValidationFailureReason() {
            return validationFailureReason;
        }

        public boolean shouldSkipExecution() {
            return shouldSkipExecution;
        }

        public void setShouldSkipExecution(boolean shouldSkipExecution) {
            this.shouldSkipExecution = shouldSkipExecution;
        }

        public String getSkipExecutionReason() {
            return skipExecutionReason;
        }

        public void setSkipExecutionReason(String skipExecutionReason) {
            this.skipExecutionReason = skipExecutionReason;
        }

        public void logOptimizationSummary() {
            log.info("=== Optimization Summary ===");

            // CRITICAL: Check for execution skip first
            if (shouldSkipExecution) {
                log.warn("🚫 EXECUTION SHOULD BE SKIPPED: {}", skipExecutionReason);
                log.warn("⚠️  This query would return no results and provides no aggregations");
                log.warn("💡 Recommendation: Skip Elasticsearch execution entirely to save resources");

                // Add to MDC for ClickHouse tracking - execution skip
                MDC.put("optimization.execution_skip", "true");
                MDC.put("optimization.skip_reason", skipExecutionReason);
                MDC.put("optimization.recommendation", "SKIP_EXECUTION");

                return; // Don't log other optimization details if execution should be skipped
            }

            if (validationPassed && metrics != null) {
                log.info("Size reduction: {}%", String.format("%.1f", metrics.getSizeReduction()));
                log.info("Nesting reduction: {}%", String.format("%.1f", metrics.getNestingReduction()));
                log.info("Optimization time: {}ms", metrics.optimizationTime);
                if (metrics.appliedRules.isEmpty()) {
                    log.info("🔍 No optimization rules helped (query was already optimal)");
                } else {
                    log.info("🔧 Rules that helped: {}", String.join(", ", metrics.appliedRules));
                    log.info("📊 Total helpful rules: {}", metrics.appliedRules.size());
                }
                log.info("Validation: PASSED");
                log.info("✅ Execute this optimized query on Elasticsearch");

                // Add to MDC for ClickHouse tracking
                MDC.put("optimization.size_reduction", String.format("%.1f", metrics.getSizeReduction()));
                MDC.put("optimization.nesting_reduction", String.format("%.1f", metrics.getNestingReduction()));
                MDC.put("optimization.time_ms", String.valueOf(metrics.optimizationTime));
                MDC.put("optimization.rules_that_helped", String.join(", ", metrics.appliedRules));
                MDC.put("optimization.validation_status", "PASSED");
                MDC.put("optimization.optimized_query", optimizedQuery);
                MDC.put("optimization.execution_skip", "false");
                MDC.put("optimization.recommendation", "EXECUTE_OPTIMIZED");

                log.info("Query optimization completed successfully - metrics added to MDC");
            } else {
                log.warn("Validation: FAILED - {}", validationFailureReason);
                log.warn("Using original query as fallback");
                log.warn("⚠️  Execute original query on Elasticsearch (optimization failed)");

                // Add to MDC for ClickHouse tracking - validation failure
                MDC.put("optimization.validation_status", "FAILED");
                MDC.put("optimization.failure_reason", validationFailureReason);
                MDC.put("optimization.fallback_used", "true");
                MDC.put("optimization.execution_skip", "false");
                MDC.put("optimization.recommendation", "EXECUTE_ORIGINAL");

                log.warn("Query optimization failed validation - fallback to original query");
            }
        }
    }

    public static void main(String[] args) {
        ElasticsearchDslOptimizer elasticsearchDslOptimizer = new ElasticsearchDslOptimizer();
        // get all json files from a directory
        File dir = new File("/Users/sriram.aravamuthan/Documents/Notes/TestDSLRewrite/test");
        File[] files = dir.listFiles((d, name) -> name.endsWith(".json"));
        if (files != null) {
            for (File file : files) {
                try {
                    BufferedInputStream bufferedInputStream = IOUtils.buffer(new FileInputStream(file));
                    //convert to bufferedInputStream to string
                    String jsonString = IOUtils.toString(bufferedInputStream, StandardCharsets.UTF_8);
                    OptimizationResult result = elasticsearchDslOptimizer.optimizeQuery(jsonString);

                    // NEW: Check for skip execution recommendation
                    if (result.shouldSkipExecution()) {
                        System.out.println("🚫 SKIP EXECUTION for " + file.getName() + ": " + result.getSkipExecutionReason());
                        continue; // Don't write optimized file if execution should be skipped
                    }

                    //result.printOptimizationSummary();
                    //write the output to another file in same directory with file name as <original_file_name>_optimized.json
                    String outputFileName = file.getName().replace(".json", "_optimized.json");
                    File outputFile = new File(dir, outputFileName);
                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                        writer.write(result.getOptimizedQuery());
                    }
                    System.out.println("Optimized query written to: " + outputFile.getAbsolutePath());
                } catch (IOException e) {
                    System.err.println("Failed to read file " + file.getName() + ": " + e.getMessage());
                }
            }
        } else {
            System.out.println("No JSON files found in the specified directory.");
        }
    }

}