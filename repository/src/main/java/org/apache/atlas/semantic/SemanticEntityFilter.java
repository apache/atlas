/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.semantic;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.discovery.SearchProcessor;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasBusinessMetadataType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.lang3.CharUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The entityFilters of a semantic search, resolved against the type registry. OpenSearch applies the index clause
 * inside the k-NN query: exactly on keyword, number and date fields, loosely on analyzed text.
 * {@link #matches} then checks every condition on the vertex with basic search's in-memory predicates.
 */
public final class SemanticEntityFilter {
    private static final int                 MAX_TOKEN_LENGTH = 255; // standard analyzer: longer words are split
    private static final Map<String, Object> MATCH_ALL        = Collections.singletonMap("match_all", Collections.emptyMap());

    // system attributes every entity has: name -> vertex index field
    private static final Map<String, Map.Entry<String, FieldKind>> SYSTEM_ATTRIBUTE_FIELDS = new HashMap<>();

    static {
        SYSTEM_ATTRIBUTE_FIELDS.put(Constants.GUID_PROPERTY_KEY, new SimpleImmutableEntry<>(Constants.GUID_PROPERTY_KEY, FieldKind.TEXT));
        SYSTEM_ATTRIBUTE_FIELDS.put(Constants.ENTITY_TYPE_PROPERTY_KEY, new SimpleImmutableEntry<>(OpenSearchSemanticStore.TYPE_NAME_KEYWORD_FIELD, FieldKind.KEYWORD));
        SYSTEM_ATTRIBUTE_FIELDS.put(Constants.STATE_PROPERTY_KEY, new SimpleImmutableEntry<>(OpenSearchSemanticStore.STATE_KEYWORD_FIELD, FieldKind.KEYWORD));
        SYSTEM_ATTRIBUTE_FIELDS.put(Constants.TIMESTAMP_PROPERTY_KEY, new SimpleImmutableEntry<>(Constants.TIMESTAMP_PROPERTY_KEY, FieldKind.DATE));
        SYSTEM_ATTRIBUTE_FIELDS.put(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, new SimpleImmutableEntry<>(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, FieldKind.DATE));
        SYSTEM_ATTRIBUTE_FIELDS.put(Constants.CLASSIFICATION_NAMES_KEY, new SimpleImmutableEntry<>(Constants.CLASSIFICATION_NAMES_KEY, FieldKind.NAME_LIST));
        SYSTEM_ATTRIBUTE_FIELDS.put(Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY, new SimpleImmutableEntry<>(Constants.PROPAGATED_CLASSIFICATION_NAMES_KEY, FieldKind.NAME_LIST));
        SYSTEM_ATTRIBUTE_FIELDS.put(Constants.LABELS_PROPERTY_KEY, new SimpleImmutableEntry<>(Constants.LABELS_PROPERTY_KEY, FieldKind.NAME_LIST));
    }

    private final Map<String, Object> indexClause;
    private final Predicate           exactMatch;

    private SemanticEntityFilter(Map<String, Object> indexClause, Predicate exactMatch) {
        this.indexClause = indexClause;
        this.exactMatch  = exactMatch;
    }

    /**
     * Validates the filters (400 on anything semantic search can't filter on).
     * Like basic search with _ALL_ENTITY_TYPES, filters without typeName can only use system attributes and business metadata.
     */
    public static SemanticEntityFilter resolve(AtlasTypeRegistry typeRegistry, String typeName, FilterCriteria criteria) throws AtlasBaseException {
        AtlasEntityType entityType = StringUtils.isBlank(typeName) ? null : typeRegistry.getEntityTypeByName(typeName);

        return resolve(typeRegistry, entityType, criteria);
    }

    /** the OpenSearch query clause on the vertex index fields */
    public Map<String, Object> getIndexClause() {
        return indexClause;
    }

    public boolean matches(AtlasVertex vertex) {
        return exactMatch.evaluate(vertex);
    }

    private static SemanticEntityFilter resolve(AtlasTypeRegistry typeRegistry, AtlasEntityType entityType, FilterCriteria criteria) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(criteria.getCriterion())) {
            List<Map<String, Object>> childClauses    = new ArrayList<>();
            List<Predicate>           childPredicates = new ArrayList<>();
            for (FilterCriteria child : criteria.getCriterion()) {
                SemanticEntityFilter resolved = resolve(typeRegistry, entityType, child);
                childClauses.add(resolved.indexClause);
                childPredicates.add(resolved.exactMatch);
            }

            if (criteria.getCondition() == FilterCriteria.Condition.OR) {
                return new SemanticEntityFilter(anyClause(childClauses), PredicateUtils.anyPredicate(childPredicates));
            }
            return new SemanticEntityFilter(Collections.singletonMap("bool", Collections.singletonMap("filter", childClauses)), PredicateUtils.allPredicate(childPredicates));
        }

        String   attributeName = criteria.getAttributeName();
        Operator operator      = criteria.getOperator();
        String   value         = criteria.getAttributeValue();

        if (operator == null || StringUtils.isBlank(value) || (operator == Operator.IN && inValues(value).isEmpty())) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "entityFilters operator or attributeValue for " + attributeName);
        }

        if (operator == Operator.TIME_RANGE) {
            // a preset such as LAST_7_DAYS, or "start,end" in epoch millis
            String[] range = StringUtils.split(SearchProcessor.processDateRange(criteria).getAttributeValue(), ',');
            if (range.length != 2 || !NumberUtils.isDigits(range[0].trim()) || !NumberUtils.isDigits(range[1].trim())) {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "entityFilters timerange value for " + attributeName);
            }
            value = range[0].trim() + "," + range[1].trim();
        }

        Field   field   = resolveField(typeRegistry, entityType, attributeName);
        boolean numeric = operator == Operator.TIME_RANGE || NumberUtils.isParsable(value);

        if (!field.kind.operators.contains(operator) || (field.kind.numeric && !numeric)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "entityFilters operator " + operator.getSymbol()
                    + " with value '" + value + "' is not supported by semantic search for attribute " + attributeName);
        }

        return new SemanticEntityFilter(indexClause(field, operator, value), exactMatch(field, operator, value));
    }

    // analyzed text is matched loosely (every exact match included): matches() checks the exact value
    private static Map<String, Object> indexClause(Field field, Operator operator, String value) {
        String  name   = field.indexField;
        boolean phrase = field.kind == FieldKind.TEXT || field.kind == FieldKind.NAME_LIST;

        switch (operator) {
            case EQ:
                return equalsClause(name, value, phrase);
            case NEQ:
                // excluding the phrase would also exclude longer values that contain it
                return phrase ? MATCH_ALL : Collections.singletonMap("bool", Collections.singletonMap("must_not", Collections.singletonList(equalsClause(name, value, false))));
            case STARTS_WITH:
                return phrase ? firstWordPrefixClause(name, value) : Collections.singletonMap("prefix", Collections.singletonMap(name, value));
            case IN:
                if (phrase) {
                    List<Map<String, Object>> clauses = new ArrayList<>();
                    for (String v : inValues(value)) {
                        clauses.add(equalsClause(name, v, true));
                    }
                    return anyClause(clauses);
                }
                return Collections.singletonMap("terms", Collections.singletonMap(name, new ArrayList<>(inValues(value))));
            case LT:
            case LTE:
            case GT:
            case GTE:
                return Collections.singletonMap("range", Collections.singletonMap(name, Collections.singletonMap(operator.name().toLowerCase(Locale.ROOT), value)));
            case TIME_RANGE: {
                // "start,end" in epoch millis, both inclusive
                String[]            range  = StringUtils.split(value, ',');
                Map<String, Object> bounds = new HashMap<>();
                bounds.put("gte", range[0]);
                bounds.put("lte", range[1]);
                return Collections.singletonMap("range", Collections.singletonMap(name, bounds));
            }
            default:
                throw new IllegalArgumentException("Unsupported semantic search operator: " + operator);
        }
    }

    private static Map<String, Object> equalsClause(String field, String value, boolean phrase) {
        return Collections.singletonMap(phrase ? "match_phrase" : "term", Collections.singletonMap(field, value));
    }

    private static Map<String, Object> anyClause(List<Map<String, Object>> clauses) {
        Map<String, Object> bool = new HashMap<>();
        bool.put("should", clauses);
        bool.put("minimum_should_match", 1);
        return Collections.singletonMap("bool", bool);
    }

    // The standard analyzer lowercases and never splits a run of ASCII letters and digits (up to its max token length),
    // so the first word of a value starting with the prefix starts with the prefix's leading run.
    private static Map<String, Object> firstWordPrefixClause(String field, String prefix) {
        int end = 0;
        while (end < prefix.length() && end < MAX_TOKEN_LENGTH && CharUtils.isAsciiAlphanumeric(prefix.charAt(end))) {
            end++;
        }
        if (end == 0) {
            return MATCH_ALL;
        }
        return Collections.singletonMap("prefix", Collections.singletonMap(field, prefix.substring(0, end).toLowerCase(Locale.ROOT)));
    }

    // "in" takes a comma-separated list, e.g. "sales_db,hr_db"
    private static Set<String> inValues(String value) {
        Set<String> ret = new LinkedHashSet<>();
        for (String v : StringUtils.split(StringUtils.defaultString(value), ',')) {
            if (StringUtils.isNotBlank(v)) {
                ret.add(v.trim());
            }
        }
        return ret;
    }

    private static Predicate exactMatch(Field field, Operator operator, String value) {
        // basic search's in-memory "in" expects a collection, not the comma-separated value
        if (operator == Operator.IN) {
            List<Predicate> any = new ArrayList<>();
            for (String v : inValues(value)) {
                any.add(exactMatch(field, Operator.EQ, v));
            }
            return PredicateUtils.anyPredicate(any);
        }

        if (field.kind == FieldKind.NAME_LIST) {
            FilterCriteria names = SearchProcessor.processPipeSeperatedSystemAttribute(field.attribute.getName(), operator, value);
            operator = names.getOperator();
            value    = names.getAttributeValue();
        }

        try {
            Predicate ret = SearchProcessor.toInMemoryPredicate(field.attribute.getDefinedInType(), field.attribute.getName(), operator, value);
            return ret != null ? ret : PredicateUtils.falsePredicate();
        } catch (NumberFormatException e) {
            // e.g. a decimal on an integer attribute: OpenSearch matches nothing either
            return PredicateUtils.falsePredicate();
        }
    }

    // as in basic search: system attributes and business metadata ("<name>.<attribute>") always, entity attributes only with typeName
    private static Field resolveField(AtlasTypeRegistry typeRegistry, AtlasEntityType entityType, String attributeName) throws AtlasBaseException {
        Map.Entry<String, FieldKind> systemField = SYSTEM_ATTRIBUTE_FIELDS.get(attributeName);
        if (systemField != null) {
            return new Field(systemField.getKey(), systemField.getValue(), AtlasEntityType.getEntityRoot().getAttribute(attributeName));
        }

        AtlasBusinessMetadataType businessMetadataType = typeRegistry.getBusinessMetadataTypeByName(StringUtils.substringBefore(attributeName, "."));
        AtlasAttribute            attribute;

        if (businessMetadataType != null && attributeName.contains(".")) {
            attribute = businessMetadataType.getAttribute(StringUtils.substringAfter(attributeName, "."));
            if (attribute == null) {
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_ATTRIBUTE, attributeName, businessMetadataType.getTypeName());
            }
        } else if (entityType != null) {
            attribute = entityType.getAttribute(attributeName);
            if (attribute == null) {
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_ATTRIBUTE, attributeName, entityType.getTypeName());
            }
        } else {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "entityFilters attribute " + attributeName
                    + ": without typeName only system attributes and business metadata (<name>.<attribute>) can be filtered");
        }

        FieldKind kind = fieldKind(attribute);
        if (attribute.getIndexFieldName() == null || kind == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "entityFilters attribute " + attributeName
                    + " has no index field semantic search can filter on (supported: strings, numbers, dates and system attributes)");
        }
        return new Field(attribute.getIndexFieldName(), kind, attribute);
    }

    // same mapping rules as GraphBackedSearchIndexer: strings are keyword fields (indexType STRING) or analyzed text;
    // booleans, enums and big numbers have no usable field (null)
    private static FieldKind fieldKind(AtlasAttribute attribute) {
        switch (attribute.getTypeName()) {
            case AtlasBaseTypeDef.ATLAS_TYPE_STRING:
                return attribute.getIndexType() == AtlasAttributeDef.IndexType.STRING ? FieldKind.KEYWORD : FieldKind.TEXT;
            case AtlasBaseTypeDef.ATLAS_TYPE_DATE:
                return FieldKind.DATE;
            case AtlasBaseTypeDef.ATLAS_TYPE_BYTE:
            case AtlasBaseTypeDef.ATLAS_TYPE_SHORT:
            case AtlasBaseTypeDef.ATLAS_TYPE_INT:
            case AtlasBaseTypeDef.ATLAS_TYPE_LONG:
            case AtlasBaseTypeDef.ATLAS_TYPE_FLOAT:
            case AtlasBaseTypeDef.ATLAS_TYPE_DOUBLE:
                return FieldKind.NUMBER;
            default:
                return null;
        }
    }

    private enum FieldKind {
        KEYWORD(false, Operator.EQ, Operator.NEQ, Operator.STARTS_WITH, Operator.IN),
        // analyzed text: matched loosely by OpenSearch
        TEXT(false, Operator.EQ, Operator.NEQ, Operator.STARTS_WITH, Operator.IN),
        // "|a|b|" lists of classification names or labels: each name is one phrase
        NAME_LIST(false, Operator.EQ, Operator.NEQ, Operator.IN),
        NUMBER(true, Operator.EQ, Operator.NEQ, Operator.LT, Operator.LTE, Operator.GT, Operator.GTE),
        DATE(true, Operator.EQ, Operator.NEQ, Operator.LT, Operator.LTE, Operator.GT, Operator.GTE, Operator.TIME_RANGE);

        final boolean       numeric;
        final Set<Operator> operators;

        FieldKind(boolean numeric, Operator first, Operator... rest) {
            this.numeric   = numeric;
            this.operators = EnumSet.of(first, rest);
        }
    }

    private static final class Field {
        final String         indexField;
        final FieldKind      kind;
        final AtlasAttribute attribute;

        Field(String indexField, FieldKind kind, AtlasAttribute attribute) {
            this.indexField = indexField;
            this.kind       = kind;
            this.attribute  = attribute;
        }
    }
}
