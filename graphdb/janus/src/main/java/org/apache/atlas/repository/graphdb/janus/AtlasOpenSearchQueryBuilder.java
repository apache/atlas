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
import static org.apache.atlas.repository.graphdb.janus.AtlasSolrQueryBuilder.CUSTOM_ATTR_SEARCH_FORMAT;
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
     * @return OpenSearch Query DSL {@code query} clause for discovery (quick search hits and aggregations):
     * Solr edismax-style {@code dis_max} over weighted fields for plain terms and wildcards; structural
     * filters in {@code filter}.
     */
    public Map<String, Object> buildDiscoveryQuery() throws AtlasBaseException {
        return buildWeightedQuickSearch();
    }

    /**
     * @return OpenSearch Query DSL {@code query} clause for weighted quick search: Solr edismax-style
     * {@code dis_max} when weights are configured (or {@code query_string} fallback); structural filters
     * in {@code filter}.
     */
    public Map<String, Object> buildWeightedQuickSearch() throws AtlasBaseException {
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
            String stateIndexFieldName = indexFieldNameCache != null
                    ? toOsField(indexFieldNameCache.get(Constants.STATE_PROPERTY_KEY)) : null;

            if (StringUtils.isEmpty(stateIndexFieldName)) {
                throw new AtlasBaseException(String.format("There is no index field name defined for attribute '%s'",
                        Constants.STATE_PROPERTY_KEY));
            }

            mustNotClauses.add(singleKeyMap("term", singleKeyMap(stateIndexFieldName, AtlasEntity.Status.DELETED.name())));
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
            Map<String, Object> criteriaClause = buildCriteria(criteria);

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

    private Map<String, Object> buildFreeTextClause(String trimmedQuery) {
        if (searchWeights != null && !searchWeights.isEmpty()) {
            List<Map<String, Object>> disMaxQueries = buildDisMaxWeightedQueries(trimmedQuery);

            if (!disMaxQueries.isEmpty()) {
                Map<String, Object> disMax = new HashMap<>();

                disMax.put("queries", disMaxQueries);
                disMax.put("tie_breaker", 0.0);

                return singleKeyMap("dis_max", disMax);
            }
        }

        LOG.debug("No search weights configured; using query_string fallback for quick search.");

        return buildWeightedQueryStringClause(trimmedQuery);
    }

    /**
     * One dis_max sub-query per weighted index field — mirrors Solr edismax {@code qf=name^10 owner^3}.
     */
    private List<Map<String, Object>> buildDisMaxWeightedQueries(String trimmedQuery) {
        List<Map<String, Object>> queries = new ArrayList<>();

        if (searchWeights == null || searchWeights.isEmpty()) {
            return queries;
        }

        boolean trailingWildcard = trimmedQuery.endsWith("*")
                && trimmedQuery.indexOf('*') == trimmedQuery.length() - 1
                && trimmedQuery.indexOf('?') < 0;
        String prefixTerm = trailingWildcard ? trimmedQuery.substring(0, trimmedQuery.length() - 1) : trimmedQuery;

        for (Map.Entry<String, Integer> entry : searchWeights.entrySet()) {
            if (StringUtils.isEmpty(entry.getKey()) || entry.getValue() == null) {
                continue;
            }

            String fieldName = toOsField(entry.getKey());
            float  boost     = entry.getValue().floatValue();

            Map<String, Object> fieldQuery = new HashMap<>();

            fieldQuery.put("query", prefixTerm);
            fieldQuery.put("boost", boost);

            if (trailingWildcard) {
                Map<String, Object> prefixParams = new HashMap<>();

                prefixParams.put("value", prefixTerm);
                prefixParams.put("boost", boost);
                queries.add(singleKeyMap("prefix", singleKeyMap(fieldName, prefixParams)));
            } else if (containsWildcard(trimmedQuery)) {
                fieldQuery.put("query", trimmedQuery);
                fieldQuery.put("default_field", fieldName);
                fieldQuery.put("default_operator", "AND");
                queries.add(singleKeyMap("query_string", fieldQuery));
            } else if (containsQueryStringMetacharacters(trimmedQuery)) {
                Map<String, Object> qsParams = new HashMap<>();

                qsParams.put("query", escapeFreeTextQuery(trimmedQuery));
                qsParams.put("default_field", fieldName);
                qsParams.put("default_operator", "AND");
                qsParams.put("boost", boost);
                queries.add(singleKeyMap("query_string", qsParams));
            } else {
                queries.add(singleKeyMap("match", singleKeyMap(fieldName, fieldQuery)));
            }
        }

        if (!trailingWildcard && !containsWildcard(trimmedQuery)) {
            Map<String, Object> allFieldQuery = new HashMap<>();

            allFieldQuery.put("query", trimmedQuery);
            queries.add(singleKeyMap("match", singleKeyMap(OpenSearchConstants.CUSTOM_ALL_FIELD, allFieldQuery)));
        }

        return queries;
    }

    private Map<String, Object> buildWeightedQueryStringClause(String trimmedQuery) {
        Map<String, Object> queryStringParams = new HashMap<>();

        queryStringParams.put("query", escapeFreeTextQuery(trimmedQuery));
        queryStringParams.put("default_operator", "AND");

        List<String> weightedFields = buildWeightedFieldList();

        if (!weightedFields.isEmpty()) {
            queryStringParams.put("fields", weightedFields);
        }

        return singleKeyMap("query_string", queryStringParams);
    }

    private List<String> buildWeightedFieldList() {
        List<String> weightedFields = new ArrayList<>();

        if (searchWeights == null || searchWeights.isEmpty()) {
            return weightedFields;
        }

        for (Map.Entry<String, Integer> entry : searchWeights.entrySet()) {
            if (StringUtils.isNotEmpty(entry.getKey()) && entry.getValue() != null) {
                weightedFields.add(toOsField(entry.getKey()) + "^" + entry.getValue());
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

    private static boolean containsQueryStringMetacharacters(String query) {
        if (StringUtils.isEmpty(query) || containsWildcard(query)) {
            return false;
        }

        for (int i = 0; i < query.length(); i++) {
            char c = query.charAt(i);

            if (c == '\\' && i + 1 < query.length()) {
                i++;

                continue;
            }

            switch (c) {
                case ':':
                case '+':
                case '!':
                case '(':
                case ')':
                case '{':
                case '}':
                case '[':
                case ']':
                case '^':
                case '"':
                case '~':
                case '/':
                case '&':
                case '|':
                    return true;
                default:
                    break;
            }
        }

        return false;
    }

    private Map<String, Object> buildClassificationTypeClause() {
        String classIndexFieldName     = toOsField(indexFieldNameCache.get(CLASSIFICATION_NAMES_KEY));
        String propagatedIndexFieldName = toOsField(indexFieldNameCache.get(PROPAGATED_CLASSIFICATION_NAMES_KEY));

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
            String stateIndexFieldName = indexFieldNameCache != null
                    ? toOsField(indexFieldNameCache.get(Constants.STATE_PROPERTY_KEY)) : null;

            if (StringUtils.isEmpty(stateIndexFieldName)) {
                throw new AtlasBaseException(String.format("There is no index field name defined for attribute '%s'",
                        Constants.STATE_PROPERTY_KEY));
            }

            mustNotClauses.add(singleKeyMap("term", singleKeyMap(stateIndexFieldName, AtlasEntity.Status.DELETED.name())));
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
        String typeIndexFieldName = toOsField(indexFieldNameCache.get(Constants.ENTITY_TYPE_PROPERTY_KEY));
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

        for (AtlasEntityType type : entityTypes) {
            String indexAttributeName = toOsField(getIndexAttributeName(type, attributeName));

            if (!indexAttributes.contains(indexAttributeName)) {
                indexAttributes.add(indexAttributeName);

                if (attributeName.equals(CUSTOM_ATTRIBUTES_PROPERTY_KEY)) {
                    if (operator.equals(Operator.CONTAINS)) {
                        operator = Operator.EQ;
                    } else if (operator.equals(Operator.NOT_CONTAINS)) {
                        operator = Operator.NEQ;
                    }

                    attributeValue = getIndexQueryAttributeValue(attributeValue);
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

                Map<String, Object> clause = buildOperatorClause(indexAttributeName, operator, attributeValue, replaceWildcardChar);

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

    private Map<String, Object> buildOperatorClause(String indexFieldName, Operator operator, String attributeValue,
                                                    boolean replaceWildCard) throws AtlasBaseException {
        if (operator == null) {
            return null;
        }

        switch (operator) {
            case EQ:
                return singleKeyMap("term", singleKeyMap(indexFieldName, attributeValue));
            case NEQ:
                return singleKeyMap("bool", singleKeyMap("must_not", singleKeyMap("term", singleKeyMap(indexFieldName, attributeValue))));
            case STARTS_WITH:
                return wildcardClause(indexFieldName, toWildcardPattern(attributeValue, replaceWildCard, true, false));
            case ENDS_WITH:
                return wildcardClause(indexFieldName, toWildcardPattern(attributeValue, replaceWildCard, false, true));
            case CONTAINS:
                return wildcardClause(indexFieldName, toWildcardPattern(attributeValue, replaceWildCard, true, true));
            case NOT_CONTAINS:
                return singleKeyMap("bool", singleKeyMap("must_not", wildcardClause(indexFieldName,
                        toWildcardPattern(attributeValue, replaceWildCard, true, true))));
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
                String msg = String.format("%s is not supported operation.", operator.getSymbol());

                LOG.error(msg);

                throw new AtlasBaseException(msg);
        }
    }

    private static Map<String, Object> wildcardClause(String indexFieldName, String pattern) {
        return singleKeyMap("wildcard", singleKeyMap(indexFieldName, pattern));
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

    private static String getIndexQueryAttributeValue(String attributeValue) {
        if (StringUtils.isNotEmpty(attributeValue)) {
            int    separatorIdx = attributeValue.indexOf(CUSTOM_ATTR_SEPARATOR);
            String key          = separatorIdx != -1 ? attributeValue.substring(0, separatorIdx) : null;

            if (key != null) {
                String value = attributeValue.substring(separatorIdx + 1);

                return String.format(CUSTOM_ATTR_SEARCH_FORMAT, key, value);
            }
        }

        return attributeValue;
    }

    private String getIndexAttributeName(AtlasEntityType type, String attrName) throws AtlasBaseException {
        AtlasAttribute ret = type.getAttribute(attrName);

        if (ret == null) {
            throw new AtlasBaseException(String.format("Received unknown attribute '%s' for type '%s'.", attrName, type.getTypeName()));
        }

        String indexFieldName = ret.getIndexFieldName();

        if (indexFieldName == null) {
            throw new AtlasBaseException(String.format("Received non-index attribute %s for type %s.", attrName, type.getTypeName()));
        }

        return indexFieldName;
    }

    static String toOsField(String indexFieldName) {
        return AtlasOpenSearchDiscoveryClient.toOpenSearchFieldName(indexFieldName);
    }

    private static Map<String, Object> singleKeyMap(String key, Object value) {
        Map<String, Object> ret = new HashMap<>();

        ret.put(key, value);

        return ret;
    }
}
