/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.opensearch.OpenSearchConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.Constants.CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.Constants.CUSTOM_ATTRIBUTES_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.LABELS_PROPERTY_KEY;
import static org.apache.atlas.repository.Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY;
import static org.apache.atlas.repository.graphdb.janus.AtlasSolrQueryBuilder.CUSTOM_ATTR_SEPARATOR;

/**
 * Builds an OpenSearch Query DSL {@code query} clause from the same Atlas inputs as
 * {@link AtlasSolrQueryBuilder} (free-text query, entity-type filter, exclude-deleted flag,
 * {@link FilterCriteria} tree). Used for OpenSearch discovery aggregations/suggestions filters.
 */
public class AtlasOpenSearchQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasOpenSearchQueryBuilder.class);

    private Set<AtlasEntityType> entityTypes;
    private String               queryString;
    private FilterCriteria       criteria;
    private boolean              excludeDeletedEntities;
    private boolean              includeSubtypes;
    private Map<String, String>  indexFieldNameCache;
    private Map<String, Integer> searchWeights;
    private Set<String>          classificationTypeNames;

    public AtlasOpenSearchQueryBuilder withEntityTypes(Set<AtlasEntityType> searchForEntityTypes) {
        this.entityTypes = searchForEntityTypes;

        return this;
    }

    public AtlasOpenSearchQueryBuilder withQueryString(String queryString) {
        this.queryString = queryString;

        return this;
    }

    public AtlasOpenSearchQueryBuilder withCriteria(FilterCriteria criteria) {
        this.criteria = criteria;

        return this;
    }

    public AtlasOpenSearchQueryBuilder withExcludedDeletedEntities(boolean excludeDeletedEntities) {
        this.excludeDeletedEntities = excludeDeletedEntities;

        return this;
    }

    public AtlasOpenSearchQueryBuilder withIncludeSubTypes(boolean includeSubTypes) {
        this.includeSubtypes = includeSubTypes;

        return this;
    }

    public AtlasOpenSearchQueryBuilder withCommonIndexFieldNames(Map<String, String> indexFieldNameCache) {
        this.indexFieldNameCache = indexFieldNameCache;

        return this;
    }

    public AtlasOpenSearchQueryBuilder withSearchWeights(Map<String, Integer> searchWeights) {
        this.searchWeights = searchWeights;

        return this;
    }

    public AtlasOpenSearchQueryBuilder withClassificationTypeNames(Set<String> classificationTypeNames) {
        this.classificationTypeNames = classificationTypeNames;

        return this;
    }

    /**
     * @return OpenSearch Query DSL {@code query} clause for discovery (quick search hits and aggregations)
     */
    public Map<String, Object> buildDiscoveryQuery() throws AtlasBaseException {
        List<Map<String, Object>> mustClauses    = new ArrayList<>();
        List<Map<String, Object>> filterClauses  = new ArrayList<>();
        List<Map<String, Object>> mustNotClauses = new ArrayList<>();

        if (StringUtils.isNotEmpty(queryString)) {
            Map<String, Object> textClause = buildFreeTextClause(queryString.trim());

            if (textClause != null) {
                mustClauses.add(textClause);
            }
        }

        if (excludeDeletedEntities) {
            String rawStateFieldName = indexFieldNameCache != null
                    ? indexFieldNameCache.get(Constants.STATE_PROPERTY_KEY) : null;

            if (StringUtils.isEmpty(rawStateFieldName)) {
                throw new AtlasBaseException(String.format("There is no index field name defined for attribute '%s'",
                        Constants.STATE_PROPERTY_KEY));
            }

            // Exact-match on __state must target the keyword field when the mapping uses a text+keyword subfield;
            // an analyzed text field would index "DELETED" lower-cased and miss the term filter.
            mustNotClauses.add(singleKeyMap("term", singleKeyMap(toKeywordField(rawStateFieldName), AtlasEntity.Status.DELETED.name())));
        }

        if (CollectionUtils.isNotEmpty(entityTypes)) {
            filterClauses.add(buildEntityTypeClause());
        }

        if (CollectionUtils.isNotEmpty(classificationTypeNames)) {
            Map<String, Object> classificationClause = buildClassificationTypeClause();

            if (classificationClause != null) {
                filterClauses.add(classificationClause);
            }
        }

        if (criteria != null) {
            Map<String, Object> criteriaClause = null;

            if (canPushEntityFilterToIndex(criteria, false)) {
                criteriaClause = buildCriteria(criteria);
            } else {
                LOG.debug("Deferring entire entity filter to downstream graph/in-memory filtering "
                        + "(OR group contains a non-indexed attribute or TIME_RANGE; see SearchProcessor#canApplyIndexFilter)");
            }

            if (criteriaClause != null) {
                filterClauses.add(criteriaClause);
            }
        }

        if (mustClauses.isEmpty() && filterClauses.isEmpty() && mustNotClauses.isEmpty()) {
            return singleKeyMap("match_all", new HashMap<>());
        }

        Map<String, Object> bool = new HashMap<>();

        if (!mustClauses.isEmpty()) {
            bool.put("must", mustClauses);
        }

        if (!filterClauses.isEmpty()) {
            bool.put("filter", filterClauses);
        }

        if (!mustNotClauses.isEmpty()) {
            bool.put("must_not", mustNotClauses);
        }

        return singleKeyMap("bool", bool);
    }

    /**
     * <p>Three cases, matching how {@code EntityDiscoveryService.quickSearch()} shapes the incoming query:
     * <ul>
     *   <li><b>Trailing wildcard</b> ({@code "customer*"}) — the implicit "search as you type" shape
     *       Use {@code multi_match} with {@code type=bool_prefix}: the query text is analyzed like a normal {@code match} (so case/Unicode
     *       normalization matches how the field was indexed), and the last analyzed term is treated as a prefix.
     *   <li><b>Explicit wildcard</b> ({@code "cus*tomer"}, {@code "?ustomer"}) — a user-typed Lucene-style wildcard
     *       that was NOT auto-appended (only single, punctuation-free words get the implicit trailing {@code *};
     *       see {@code AtlasStructType.AtlasAttribute#hastokenizeChar}). {@code multi_match} never interprets
     *       {@code *}/{@code ?} as wildcards (its query text is always literal, analyzed text), so genuine
     *       wildcard-pattern matching requires {@code query_string} — used here as a single clause carrying the
     *       full weighted {@code fields} list (not one clause per field), preserving the same no-fan-out property.</li>
     *   <li><b>Plain text</b> (anything else, including multi-word queries like {@code "customer purchase"} or
     *       punctuation) — {@code multi_match} with no explicit type (OpenSearch's default, {@code best_fields}),
     *       which ORs the query's terms per field and takes the best-scoring field.</li>
     * </ul>
     */
    private Map<String, Object> buildFreeTextClause(String trimmedQuery) {
        List<String> weightedFields = buildWeightedFieldList();

        boolean trailingWildcard = trimmedQuery.endsWith("*")
                && trimmedQuery.indexOf('*') == trimmedQuery.length() - 1
                && trimmedQuery.indexOf('?') < 0;

        if (trailingWildcard) {
            String prefixTerm = trimmedQuery.substring(0, trimmedQuery.length() - 1);

            return buildMultiMatchClause(prefixTerm, weightedFields, "bool_prefix");
        }

        if (containsWildcard(trimmedQuery)) {
            Map<String, Object> queryStringParams = new HashMap<>();

            queryStringParams.put("query", escapeFreeTextQuery(trimmedQuery));
            queryStringParams.put("default_operator", "AND");

            if (!weightedFields.isEmpty()) {
                queryStringParams.put("fields", weightedFields);
            }

            return singleKeyMap("query_string", queryStringParams);
        }

        return buildMultiMatchClause(trimmedQuery, weightedFields, null);
    }

    private Map<String, Object> buildMultiMatchClause(String queryText, List<String> fields, String type) {
        Map<String, Object> multiMatch = new HashMap<>();

        multiMatch.put("query", queryText);

        if (!fields.isEmpty()) {
            multiMatch.put("fields", fields);
        }

        if (StringUtils.isNotEmpty(type)) {
            multiMatch.put("type", type);
        }

        return singleKeyMap("multi_match", multiMatch);
    }

    /**
     * Every {@code searchWeights} entry rendered as a compact {@code "field^boost"} string, plus the catch-all {@code all}
     * field with its default boost of 1 — always included so the fields list is never empty even when no per-attribute weights
     * are configured yet. Every valid entry in {@code searchWeights} is included;
     */
    private List<String> buildWeightedFieldList() {
        List<String> weightedFields = new ArrayList<>();

        weightedFields.add(OpenSearchConstants.CUSTOM_ALL_FIELD);

        if (searchWeights != null) {
            for (Map.Entry<String, Integer> entry : searchWeights.entrySet()) {
                if (StringUtils.isNotEmpty(entry.getKey()) && entry.getValue() != null) {
                    weightedFields.add(toOsField(entry.getKey()) + "^" + entry.getValue());
                }
            }
        }

        return weightedFields;
    }

    static boolean containsWildcard(String query) {
        if (StringUtils.isEmpty(query)) {
            return false;
        }

        for (int i = 0; i < query.length(); i++) {
            char c = query.charAt(i);

            if (c == '\\' && i + 1 < query.length()) {
                i++;

                continue;
            }

            if (c == '*' || c == '?') {
                return true;
            }
        }

        return false;
    }

    private static String escapeFreeTextQuery(String query) {
        if (StringUtils.isEmpty(query)) {
            return query;
        }

        return AtlasAttribute.escapeIndexQueryValue(query, containsWildcard(query));
    }

    private Map<String, Object> buildClassificationTypeClause() {
        // terms on classification-name fields is an exact-match filter; resolve to the keyword subfield when mapped.
        String classIndexFieldName      = toKeywordField(indexFieldNameCache.get(CLASSIFICATION_NAMES_KEY));
        String propagatedIndexFieldName = toKeywordField(indexFieldNameCache.get(PROPAGATED_CLASSIFICATION_NAMES_KEY));

        if (StringUtils.isEmpty(classIndexFieldName) || StringUtils.isEmpty(propagatedIndexFieldName)) {
            LOG.warn("Missing index field names for classification filters; skipping OpenSearch classification filter.");

            return null;
        }

        List<String> types = new ArrayList<>(classificationTypeNames);

        List<Map<String, Object>> shouldClauses = new ArrayList<>();

        shouldClauses.add(singleKeyMap("terms", singleKeyMap(classIndexFieldName, types)));
        shouldClauses.add(singleKeyMap("terms", singleKeyMap(propagatedIndexFieldName, types)));

        return singleKeyMap("bool", singleKeyMap("should", shouldClauses));
    }

    /**
     * @return OpenSearch Query DSL {@code query} clause for legacy callers; prefer {@link #buildDiscoveryQuery()}.
     */
    public Map<String, Object> build() throws AtlasBaseException {
        List<Map<String, Object>> mustClauses    = new ArrayList<>();
        List<Map<String, Object>> mustNotClauses = new ArrayList<>();

        if (StringUtils.isNotEmpty(queryString)) {
            LOG.debug("Initial query string is {}.", queryString);

            Map<String, Object> queryStringParams = new HashMap<>();

            queryStringParams.put("query", escapeFreeTextQuery(queryString.trim()));
            queryStringParams.put("default_operator", "AND");

            mustClauses.add(singleKeyMap("query_string", queryStringParams));
        }

        if (excludeDeletedEntities) {
            String rawStateFieldName = indexFieldNameCache != null
                    ? indexFieldNameCache.get(Constants.STATE_PROPERTY_KEY) : null;

            if (StringUtils.isEmpty(rawStateFieldName)) {
                throw new AtlasBaseException(String.format("There is no index field name defined for attribute '%s'",
                        Constants.STATE_PROPERTY_KEY));
            }

            mustNotClauses.add(singleKeyMap("term", singleKeyMap(toKeywordField(rawStateFieldName), AtlasEntity.Status.DELETED.name())));
        }

        if (CollectionUtils.isNotEmpty(entityTypes)) {
            mustClauses.add(buildEntityTypeClause());
        }

        if (criteria != null) {
            Map<String, Object> criteriaClause = buildCriteria(criteria);

            if (criteriaClause != null) {
                mustClauses.add(criteriaClause);
            }
        }

        if (mustClauses.isEmpty() && mustNotClauses.isEmpty()) {
            return singleKeyMap("match_all", new HashMap<>());
        }

        Map<String, Object> bool = new HashMap<>();

        if (!mustClauses.isEmpty()) {
            bool.put("must", mustClauses);
        }

        if (!mustNotClauses.isEmpty()) {
            bool.put("must_not", mustNotClauses);
        }

        return singleKeyMap("bool", bool);
    }

    private Map<String, Object> buildEntityTypeClause() {
        // terms on __typeName is an exact-match filter — resolve to the keyword subfield when the mapping requires it.
        String typeIndexFieldName = toKeywordField(indexFieldNameCache.get(Constants.ENTITY_TYPE_PROPERTY_KEY));
        Set<String> typesToSearch = new HashSet<>();

        for (AtlasEntityType type : entityTypes) {
            if (includeSubtypes) {
                typesToSearch.addAll(type.getTypeAndAllSubTypes());
            } else {
                typesToSearch.add(type.getTypeName());
            }
        }

        return singleKeyMap("terms", singleKeyMap(typeIndexFieldName, new ArrayList<>(typesToSearch)));
    }

    /**
     * Mirrors {@link org.apache.atlas.discovery.SearchProcessor#canApplyIndexFilter} for attributes that have no
     * mixed-index field and for {@code TIME_RANGE} under {@code OR}
     * {@code NOT_CONTAINS} on an attribute whose mixed-index field is analyzed {@code text} without a keyword
     * subfield (and not a native keyword field) gets the exact same treatment: OpenSearch's default analyzer
     * lowercases stored tokens, so a case-sensitive {@code must_not}/{@code wildcard} built against that field can
     * produce a false-positive exclusion whenever the (case-preserved) query value happens to match the lowercased
     * token even though the raw attribute value does not case-sensitively contain it (see
     * {@link #buildOperatorClause}'s {@code NOT_CONTAINS} case) — and, unlike inclusion operators, a candidate
     * {@code must_not} wrongly drops can never be recovered by {@code EntitySearchProcessor}'s downstream
     * in-memory predicate.
     */
    private boolean canPushEntityFilterToIndex(FilterCriteria criteria, boolean insideOrCondition) throws AtlasBaseException {
        List<FilterCriteria> criterion = criteria.getCriterion();
        FilterCriteria.Condition filterCondition = criteria.getCondition();

        if (filterCondition != null && CollectionUtils.isNotEmpty(criterion)) {
            boolean insideOr = insideOrCondition || filterCondition == FilterCriteria.Condition.OR;

            for (FilterCriteria child : criterion) {
                if (!canPushEntityFilterToIndex(child, insideOr)) {
                    return false;
                }
            }

            return true;
        }

        if (StringUtils.isNotEmpty(criteria.getAttributeName())) {
            if (criteria.getOperator() == Operator.TIME_RANGE) {
                return !insideOrCondition;
            }

            for (AtlasEntityType type : entityTypes) {
                String rawIndexFieldName = resolveIndexFieldName(type, criteria.getAttributeName());

                if (insideOrCondition && rawIndexFieldName == null) {
                    return false;
                }

                if (criteria.getOperator() == Operator.NOT_CONTAINS && rawIndexFieldName != null
                        && isAnalyzedOnlyField(rawIndexFieldName)) {
                    return !insideOrCondition;
                }
            }
        }

        return true;
    }

    private Map<String, Object> buildCriteria(FilterCriteria criteria) throws AtlasBaseException {
        List<FilterCriteria> criterion = criteria.getCriterion();

        if (StringUtils.isNotEmpty(criteria.getAttributeName()) && CollectionUtils.isEmpty(criterion)) {
            return buildLeafCriteria(criteria);
        } else if (CollectionUtils.isNotEmpty(criterion)) {
            List<Map<String, Object>> childClauses = new ArrayList<>();

            for (FilterCriteria childCriteria : criterion) {
                Map<String, Object> childClause = buildCriteria(childCriteria);

                if (childClause != null) {
                    childClauses.add(childClause);
                }
            }

            if (childClauses.isEmpty()) {
                return null;
            }

            String  condition = criteria.getCondition() != null ? criteria.getCondition().name() : FilterCriteria.Condition.AND.name();
            boolean isAnd     = FilterCriteria.Condition.AND.name().equalsIgnoreCase(condition);

            return singleKeyMap("bool", singleKeyMap(isAnd ? "must" : "should", childClauses));
        }

        return null;
    }

    private Map<String, Object> buildLeafCriteria(FilterCriteria criteria) throws AtlasBaseException {
        String   attributeName  = criteria.getAttributeName();
        String   attributeValue = criteria.getAttributeValue();
        Operator operator       = criteria.getOperator();

        List<Map<String, Object>> orClauses       = new ArrayList<>();
        Set<String>               indexAttributes = new HashSet<>();

        if (operator == Operator.TIME_RANGE) {
            for (AtlasEntityType type : entityTypes) {
                resolveIndexFieldName(type, attributeName);
            }

            // Solr/Atlas quick-search builders do not translate TIME_RANGE into the weighted index query either;
            // SearchProcessor#processDateRange + EntitySearchProcessor in-memory filtering apply timerange on each
            // result page (FreeTextSearchProcessor loops until enough matches pass the full entity filter).
            LOG.debug("Skipping TIME_RANGE entity filter on attribute '{}' (deferring to EntitySearchProcessor)",
                    attributeName);

            return null;
        }

        for (AtlasEntityType type : entityTypes) {
            String rawIndexAttributeName = resolveIndexFieldName(type, attributeName);

            if (rawIndexAttributeName == null) {
                // Same contract as SearchProcessor#toIndexQuery: non-indexed criteria are not pushed into the
                // mixed-index query; EntitySearchProcessor applies them after the index returns candidate entities.
                LOG.debug("Skipping non-index entity filter attribute '{}' for type '{}' (operator={})",
                        attributeName, type.getTypeName(), operator);

                continue;
            }

            String indexAttributeName = toOsField(rawIndexAttributeName);

            if (!indexAttributes.contains(indexAttributeName)) {
                indexAttributes.add(indexAttributeName);

                if (attributeName.equals(CUSTOM_ATTRIBUTES_PROPERTY_KEY)) {
                    Map<String, Object> customAttributesClause =
                            buildCustomAttributesFilterClause(rawIndexAttributeName, operator, attributeValue);

                    if (customAttributesClause != null) {
                        orClauses.add(customAttributesClause);
                    }

                    continue;
                }

                if (attributeValue != null) {
                    attributeValue = attributeValue.trim();
                }

                boolean                          replaceWildcardChar = false;
                AtlasStructDef.AtlasAttributeDef def                 = type.getAttributeDef(attributeName);

                if (!isPipeSeparatedSystemAttribute(attributeName) && isWildCardOperator(operator)
                        && def.getTypeName().equalsIgnoreCase(AtlasBaseTypeDef.ATLAS_TYPE_STRING)) {
                    if (def.getIndexType() == null && AtlasAttribute.hastokenizeChar(attributeValue)) {
                        replaceWildcardChar = true;
                    }
                }

                Map<String, Object> clause = buildOperatorClause(rawIndexAttributeName, operator, attributeValue, replaceWildcardChar);

                if (clause != null) {
                    orClauses.add(clause);
                }
            }
        }

        if (orClauses.isEmpty()) {
            return null;
        }

        return orClauses.size() == 1 ? orClauses.get(0) : singleKeyMap("bool", singleKeyMap("should", orClauses));
    }

    /**
     * User-defined attributes are stored as JSON in an analyzed {@code text} field. Solr uses a quoted fragment
     * with {@code term}; OpenSearch must match the JSON substring via {@code match_phrase} on the analyzed field.
     */
    private Map<String, Object> buildCustomAttributesFilterClause(String rawIndexFieldName, Operator operator,
                                                                    String attributeValue) {
        if (operator == null || StringUtils.isEmpty(attributeValue)) {
            return null;
        }

        String jsonFragment = getCustomAttributeJsonFragment(attributeValue.trim());

        if (jsonFragment == null) {
            return null;
        }

        String analyzedField = toOsField(rawIndexFieldName);
        Map<String, Object> matchPhraseParams = new HashMap<>();

        matchPhraseParams.put("query", jsonFragment);

        Map<String, Object> matchPhrase = singleKeyMap("match_phrase", singleKeyMap(analyzedField, matchPhraseParams));

        if (operator == Operator.NOT_CONTAINS || operator == Operator.NEQ) {
            return singleKeyMap("bool", singleKeyMap("must_not", matchPhrase));
        }

        if (operator == Operator.CONTAINS || operator == Operator.EQ) {
            return matchPhrase;
        }

        return null;
    }

    private static String getCustomAttributeJsonFragment(String attributeValue) {
        if (StringUtils.isEmpty(attributeValue)) {
            return null;
        }

        int separatorIdx = attributeValue.indexOf(CUSTOM_ATTR_SEPARATOR);

        if (separatorIdx < 0) {
            return attributeValue;
        }

        String key   = attributeValue.substring(0, separatorIdx).trim();
        String value = attributeValue.substring(separatorIdx + 1).trim();

        if (StringUtils.isEmpty(key)) {
            return null;
        }

        return String.format("\"%s\":\"%s\"", key, value);
    }

    private Map<String, Object> buildOperatorClause(String rawIndexFieldName, Operator operator, String attributeValue,
                                                    boolean replaceWildCard) throws AtlasBaseException {
        if (operator == null) {
            return null;
        }

        // Free-text/wildcard/range operators target the analyzed (default) physical field; EQ/NEQ use
        // {@link #buildExactMatchClause} (keyword term vs analyzed match_phrase).
        String indexFieldName = toOsField(rawIndexFieldName);

        switch (operator) {
            case EQ:
                return buildExactMatchClause(rawIndexFieldName, attributeValue);
            case NEQ:
                return singleKeyMap("bool", singleKeyMap("must_not", buildExactMatchClause(rawIndexFieldName, attributeValue)));
            case STARTS_WITH:
                return wildcardClause(indexFieldName, toWildcardPattern(attributeValue, replaceWildCard, false, true), true);
            case ENDS_WITH:
                return wildcardClause(indexFieldName, toWildcardPattern(attributeValue, replaceWildCard, true, false), true);
            case CONTAINS:
                return wildcardClause(indexFieldName, toWildcardPattern(attributeValue, replaceWildCard, true, true), true);
            case NOT_CONTAINS:
                if (isAnalyzedOnlyField(rawIndexFieldName)) {
                    LOG.debug("Skipping NOT_CONTAINS entity filter on analyzed-only attribute (index field '{}'); "
                            + "deferring to EntitySearchProcessor to avoid a false-positive must_not exclusion", rawIndexFieldName);

                    return null;
                }

                return singleKeyMap("bool", singleKeyMap("must_not", wildcardClause(indexFieldName,
                        toWildcardPattern(attributeValue, replaceWildCard, true, true), false)));
            case IS_NULL:
                return singleKeyMap("bool", singleKeyMap("must_not", singleKeyMap("exists", singleKeyMap("field", indexFieldName))));
            case NOT_NULL:
                return singleKeyMap("exists", singleKeyMap("field", indexFieldName));
            case LT:
                return singleKeyMap("range", singleKeyMap(indexFieldName, singleKeyMap("lt", attributeValue)));
            case GT:
                return singleKeyMap("range", singleKeyMap(indexFieldName, singleKeyMap("gt", attributeValue)));
            case LTE:
                return singleKeyMap("range", singleKeyMap(indexFieldName, singleKeyMap("lte", attributeValue)));
            case GTE:
                return singleKeyMap("range", singleKeyMap(indexFieldName, singleKeyMap("gte", attributeValue)));
            case IN:
            case LIKE:
            case CONTAINS_ANY:
            case CONTAINS_ALL:
            default:
                // Operator parity with the Solr quick-search (AtlasSolrQueryBuilder)
                String msg = String.format("%s is not supported operation.", operator.getSymbol());

                LOG.error(msg);

                throw new AtlasBaseException(msg);
        }
    }

    /**
     * Exact equality for entity/BM filters in the OpenSearch quick-search path. Fields with a registered
     * {@code .keyword} subfield (or native {@code __s_} keyword string-index fields) use {@code term}; analyzed
     * {@code text} fields without a keyword subfield use {@code match_phrase}
     */
    private static Map<String, Object> buildExactMatchClause(String rawIndexFieldName, String attributeValue) {
        if (AtlasOpenSearchIndexClient.usesKeywordSubfield(rawIndexFieldName)) {
            return singleKeyMap("term", singleKeyMap(toKeywordField(rawIndexFieldName), attributeValue));
        }

        if (isNativeKeywordStringIndexField(rawIndexFieldName)) {
            return singleKeyMap("term", singleKeyMap(toOsField(rawIndexFieldName), attributeValue));
        }

        Map<String, Object> matchPhraseParams = new HashMap<>();

        matchPhraseParams.put("query", attributeValue);

        return singleKeyMap("match_phrase", singleKeyMap(toOsField(rawIndexFieldName), matchPhraseParams));
    }

    private static boolean isNativeKeywordStringIndexField(String rawIndexFieldName) {
        return StringUtils.isNotEmpty(rawIndexFieldName)
                && rawIndexFieldName.contains(String.valueOf(AtlasAttribute.VERTEX_PROPERTY_PREFIX_STRING_INDEX_TYPE));
    }

    /**
     * @return {@code true} when {@code rawIndexFieldName} has no case-preserving representation available (no
     * registered {@code .keyword} subfield and not a native {@code __s_} keyword field) -- i.e. the only physical
     * OpenSearch field for it is analyzed {@code text}, whose default analyzer lowercases every stored token.
     */
    private static boolean isAnalyzedOnlyField(String rawIndexFieldName) {
        return !AtlasOpenSearchIndexClient.usesKeywordSubfield(rawIndexFieldName)
                && !isNativeKeywordStringIndexField(rawIndexFieldName);
    }

    /**
     * OpenSearch's default (standard) analyzer lowercases every token of the {@code text} field that
     * {@code STARTS_WITH}/{@code ENDS_WITH}/{@code CONTAINS} target (see {@link #buildOperatorClause}, which
     * deliberately keeps wildcard operators on the analyzed field.
     * <p>
     * Without an explicit {@code case_insensitive} flag, OpenSearch's {@code wildcard} query compares the pattern
     * byte-for-byte against the (lowercased) indexed tokens, so a mixed-case {@code attributeValue} like
     * {@code "High"} never matches the stored token {@code "high"} and OpenSearch drops the entity from its
     * candidate page entirely -- before {@code EntitySearchProcessor}, which is unconditionally chained after
     * {@code FreeTextSearchProcessor} whenever a {@code typeName} is present (see {@code SearchContext}), gets a
     * chance to apply its own case-sensitive Java predicate (
     * {@code SearchPredicateUtil#getContainsPredicate}/{@code getStartsWithPredicate}/{@code getEndsWithPredicate},
     * evaluated against the raw, un-analyzed attribute value) to confirm or reject it.
     * <p>
     * {@code caseInsensitive} widens the OpenSearch candidate match to a superset of the true (case-sensitive)
     * matches -- exactly mirroring Solr's own multi-term-analyzer normalization -- and relies on that
     * always-present downstream in-memory predicate to narrow back down to Atlas's case-sensitive contract, the
     * same two-step "index narrows candidates, in-memory predicate decides" pattern the no-free-text-query
     * {@code EntitySearchProcessor}-only path already uses for every operator. This widening must only be applied
     * to inclusion clauses (queried directly, or wrapped in {@code must}/{@code filter}); see {@link
     * #buildOperatorClause}'s {@code NOT_CONTAINS} case for why the negated ({@code must_not}) wildcard must stay
     * case-sensitive.
     */
    private static Map<String, Object> wildcardClause(String indexFieldName, String pattern, boolean caseInsensitive) {
        if (!caseInsensitive) {
            return singleKeyMap("wildcard", singleKeyMap(indexFieldName, pattern));
        }

        Map<String, Object> params = new HashMap<>();

        params.put("value", pattern);
        params.put("case_insensitive", true);

        return singleKeyMap("wildcard", singleKeyMap(indexFieldName, params));
    }

    /**
     * OpenSearch wildcard patterns use {@code *} and {@code ?}; do not apply Solr quote/escape helpers.
     */
    private static String toWildcardPattern(String attributeValue, boolean replaceWildCard, boolean prefixStar, boolean suffixStar) {
        if (attributeValue == null) {
            return replaceWildCard ? "" : (prefixStar ? "*" : "") + (suffixStar ? "*" : "");
        }

        String escaped = escapeOpenSearchWildcard(attributeValue);

        if (replaceWildCard) {
            return escaped;
        }

        StringBuilder sb = new StringBuilder();

        if (prefixStar) {
            sb.append('*');
        }

        sb.append(escaped);

        if (suffixStar) {
            sb.append('*');
        }

        return sb.toString();
    }

    private static String escapeOpenSearchWildcard(String value) {
        StringBuilder sb = new StringBuilder(value.length());

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);

            if (c == '*' || c == '?' || c == '\\') {
                sb.append('\\');
            }

            sb.append(c);
        }

        return sb.toString();
    }

    private static boolean isPipeSeparatedSystemAttribute(String attrName) {
        return StringUtils.equals(attrName, CLASSIFICATION_NAMES_KEY) ||
                StringUtils.equals(attrName, PROPAGATED_CLASSIFICATION_NAMES_KEY) ||
                StringUtils.equals(attrName, LABELS_PROPERTY_KEY) ||
                StringUtils.equals(attrName, CUSTOM_ATTRIBUTES_PROPERTY_KEY);
    }

    private static boolean isWildCardOperator(Operator operator) {
        return operator == Operator.CONTAINS ||
                operator == Operator.STARTS_WITH ||
                operator == Operator.ENDS_WITH ||
                operator == Operator.NOT_CONTAINS;
    }

    /**
     * @return mixed-index physical field name, or {@code null} when the attribute exists on the type but is not indexed
     */
    private String resolveIndexFieldName(AtlasEntityType type, String attrName) throws AtlasBaseException {
        AtlasAttribute ret = type.getAttribute(attrName);

        if (ret == null) {
            throw new AtlasBaseException(String.format("Received unknown attribute '%s' for type '%s'.", attrName, type.getTypeName()));
        }

        return ret.getIndexFieldName();
    }

    static String toOsField(String indexFieldName) {
        return AtlasOpenSearchIndexClient.toOpenSearchFieldName(indexFieldName);
    }

    /**
     * Resolves the physical OpenSearch field for exact-match ({@code term}/{@code terms}) operations, appending the
     * {@code .keyword} subfield when the field was registered as text+keyword (see
     * {@link AtlasOpenSearchIndexClient#toOpenSearchTermsFieldName(String)}). For fields mapped as native keyword or
     * as analyzed text without a keyword subfield, the bare field is returned (preserving prior behavior).
     */
    static String toKeywordField(String indexFieldName) {
        return AtlasOpenSearchIndexClient.toOpenSearchTermsFieldName(indexFieldName);
    }

    private static Map<String, Object> singleKeyMap(String key, Object value) {
        Map<String, Object> ret = new HashMap<>();

        ret.put(key, value);

        return ret;
    }
}
