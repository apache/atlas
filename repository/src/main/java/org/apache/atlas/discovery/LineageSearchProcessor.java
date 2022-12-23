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

package org.apache.atlas.discovery;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.*;
import org.apache.atlas.util.SearchPredicateUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.Predicate;
import org.apache.commons.collections.PredicateUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import static org.apache.atlas.repository.Constants.ATTRIBUTE_VALUE_DELIMITER;
import static org.apache.atlas.util.SearchPredicateUtil.*;
import static org.apache.atlas.util.SearchPredicateUtil.getInRangePredicateGenerator;

public class LineageSearchProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(LineageSearchProcessor.class);

    private static final Map<SearchParameters.Operator, SearchPredicateUtil.ElementAttributePredicateGenerator> OPERATOR_PREDICATE_MAP = new HashMap<>();

    static
    {
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.LT, getLTPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.GT, getGTPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.LTE, getLTEPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.GTE, getGTEPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.EQ, getEQPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.NEQ, getNEQPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.IN, getINPredicateGenerator()); // this should be a list of quoted strings
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.LIKE, getLIKEPredicateGenerator()); // this should be regex pattern
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.STARTS_WITH, getStartsWithPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.ENDS_WITH, getEndsWithPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.CONTAINS, getContainsPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.NOT_CONTAINS, getNotContainsPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.IS_NULL, getIsNullPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.NOT_NULL, getNotNullPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.NOT_EMPTY, getNotEmptyPredicateGenerator());
        OPERATOR_PREDICATE_MAP.put(SearchParameters.Operator.TIME_RANGE, getInRangePredicateGenerator());
    }

    protected Predicate constructInMemoryPredicate(AtlasTypeRegistry typeRegistry, SearchParameters.FilterCriteria filterCriteria) {
        Predicate ret = null;
        if (filterCriteria != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Processing Filters");
            }
            final List<AtlasEntityType> entityTypes = new ArrayList<>(typeRegistry.getAllEntityTypes());
            Set<String> allAttributes = new HashSet<>();
            getAllAttributes(filterCriteria, allAttributes);

            ret = toInMemoryPredicate(entityTypes, filterCriteria, allAttributes);
        }
        return ret;
    }

    protected Predicate constructInMemoryPredicate(AtlasTypeRegistry typeRegistry, List<SearchParameters.FilterCriteria> filterCriteriaList) {
        Predicate ret = null;
        if (filterCriteriaList != null && ! filterCriteriaList.isEmpty()) {
            for (SearchParameters.FilterCriteria filterCriteria : filterCriteriaList) {
                if (filterCriteria != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Processing Filters");
                    }
                    final List<AtlasEntityType> entityTypes = new ArrayList<>(typeRegistry.getAllEntityTypes());
                    Set<String> allAttributes = new HashSet<>();
                    getAllAttributes(filterCriteria, allAttributes);

                    ret = toInMemoryPredicate(entityTypes, filterCriteria, allAttributes);
                }
            }
        }
        return ret;
    }

    private Predicate toInMemoryPredicate(List<? extends AtlasStructType> structTypes, SearchParameters.FilterCriteria criteria,
                                          Set<String> allAttributes) {

        Set<String> filterAttributes = new HashSet<>();
        filterAttributes.addAll(allAttributes);

        if (criteria.getCondition() != null && CollectionUtils.isNotEmpty(criteria.getCriterion())) {
            List<Predicate> predicates = new ArrayList<>();

            for (SearchParameters.FilterCriteria filterCriteria : criteria.getCriterion()) {
                Predicate predicate = toInMemoryPredicate(structTypes, filterCriteria, filterAttributes);

                if (predicate != null) {
                    predicates.add(predicate);
                }
            }

            if (CollectionUtils.isNotEmpty(predicates)) {
                if (criteria.getCondition() == SearchParameters.FilterCriteria.Condition.AND) {
                    return PredicateUtils.allPredicate(predicates);
                } else {
                    return PredicateUtils.anyPredicate(predicates);
                }
            }
        } else if (StringUtils.isNotEmpty(criteria.getAttributeName())) {
            try {
                ArrayList<Predicate> predicates = new ArrayList<>();

                for (AtlasStructType structType : structTypes) {
                    AtlasStructType.AtlasAttribute attribute = structType.getAttribute(criteria.getAttributeName());
                    if (attribute == null) {
                        continue;
                    }
                    String name = structType.getVertexPropertyName(criteria.getAttributeName());

                    if (filterAttributes.contains(name)) {
                        String attrName = criteria.getAttributeName();
                        String attrValue = criteria.getAttributeValue();
                        SearchParameters.Operator operator = criteria.getOperator();

                        predicates.add(toInMemoryPredicate(structType, attrName, operator, attrValue));
                        filterAttributes.remove(name);
                        break;
                    }

                }

                if (CollectionUtils.isNotEmpty(predicates)) {
                    if (predicates.size() > 1) {
                        return PredicateUtils.anyPredicate(predicates);
                    } else {
                        return predicates.iterator().next();
                    }
                }
            } catch (AtlasBaseException e) {
                LOG.warn(e.getMessage());
            }
        }
        return null;
    }

    private Predicate toInMemoryPredicate(AtlasStructType type, String attrName, SearchParameters.Operator op, String attrVal) {
        Predicate ret = null;
        AtlasStructType.AtlasAttribute attribute = type.getAttribute(attrName);
        SearchPredicateUtil.ElementAttributePredicateGenerator predicate = OPERATOR_PREDICATE_MAP.get(op);

        if (attribute != null && predicate != null) {
            final AtlasType attrType = attribute.getAttributeType();
            final Class     attrClass;
            final Object    attrValue;
            Object attrValue2 = null;

            // Some operators support null comparison, thus the parsing has to be conditional
            switch (attrType.getTypeName()) {
                case AtlasBaseTypeDef.ATLAS_TYPE_STRING:
                    attrClass = String.class;
                    attrValue = attrVal;
                    break;
                case AtlasBaseTypeDef.ATLAS_TYPE_SHORT:
                    attrClass = Short.class;
                    attrValue = StringUtils.isEmpty(attrVal) ? null : Short.parseShort(attrVal);
                    break;
                case AtlasBaseTypeDef.ATLAS_TYPE_INT:
                    attrClass = Integer.class;
                    attrValue = StringUtils.isEmpty(attrVal) ? null : Integer.parseInt(attrVal);
                    break;
                case AtlasBaseTypeDef.ATLAS_TYPE_BIGINTEGER:
                    attrClass = BigInteger.class;
                    attrValue = StringUtils.isEmpty(attrVal) ? null : new BigInteger(attrVal);
                    break;
                case AtlasBaseTypeDef.ATLAS_TYPE_BOOLEAN:
                    attrClass = Boolean.class;
                    attrValue = StringUtils.isEmpty(attrVal) ? null : Boolean.parseBoolean(attrVal);
                    break;
                case AtlasBaseTypeDef.ATLAS_TYPE_BYTE:
                    attrClass = Byte.class;
                    attrValue = StringUtils.isEmpty(attrVal) ? null : Byte.parseByte(attrVal);
                    break;
                case AtlasBaseTypeDef.ATLAS_TYPE_LONG:
                case AtlasBaseTypeDef.ATLAS_TYPE_DATE:
                    attrClass = Long.class;
                    String rangeStart = "";
                    String rangeEnd   = "";
                    if (op == SearchParameters.Operator.TIME_RANGE) {
                        String[] parts = attrVal.split(ATTRIBUTE_VALUE_DELIMITER);
                        if (parts.length == 2) {
                            rangeStart = parts[0];
                            rangeEnd   = parts[1];
                        }
                    }
                    if (StringUtils.isNotEmpty(rangeStart) && StringUtils.isNotEmpty(rangeEnd)) {
                        attrValue  = Long.parseLong(rangeStart);
                        attrValue2 = Long.parseLong(rangeEnd);
                    } else {
                        attrValue = StringUtils.isEmpty(attrVal) ? null : Long.parseLong(attrVal);
                    }
                    break;
                case AtlasBaseTypeDef.ATLAS_TYPE_FLOAT:
                    attrClass = Float.class;
                    attrValue = StringUtils.isEmpty(attrVal) ? null : Float.parseFloat(attrVal);
                    break;
                case AtlasBaseTypeDef.ATLAS_TYPE_DOUBLE:
                    attrClass = Double.class;
                    attrValue = StringUtils.isEmpty(attrVal) ? null : Double.parseDouble(attrVal);
                    break;
                case AtlasBaseTypeDef.ATLAS_TYPE_BIGDECIMAL:
                    attrClass = BigDecimal.class;
                    attrValue = StringUtils.isEmpty(attrVal) ? null : new BigDecimal(attrVal);
                    break;
                default:
                    if (attrType instanceof AtlasEnumType) {
                        attrClass = String.class;
                    } else if (attrType instanceof AtlasArrayType) {
                        attrClass = List.class;
                    } else {
                        attrClass = Object.class;
                    }

                    attrValue = attrVal;
                    break;
            }

            String vertexPropertyName = attribute.getVertexPropertyName();
            if (attrValue != null && attrValue2 != null) {
                ret = predicate.generatePredicate(StringUtils.isEmpty(vertexPropertyName) ? attribute.getQualifiedName() : vertexPropertyName,
                        attrValue, attrValue2, attrClass);
            } else {
                ret = predicate.generatePredicate(
                        StringUtils.isEmpty(vertexPropertyName) ? attribute.getQualifiedName() : vertexPropertyName,
                        attrValue, attrClass);
            }
        }

        return ret;
    }

    private Set<String> getAllAttributes(SearchParameters.FilterCriteria filterCriteria, Set<String> allAttributes) {
        if (filterCriteria == null) {
            return null;
        }

        SearchParameters.FilterCriteria.Condition filterCondition = filterCriteria.getCondition();
        List<SearchParameters.FilterCriteria> criterion = filterCriteria.getCriterion();

        if (filterCondition != null && CollectionUtils.isNotEmpty(criterion)) {
            for (SearchParameters.FilterCriteria criteria : criterion) {
                getAllAttributes(criteria, allAttributes);
            }
        } else if (StringUtils.isNotEmpty(filterCriteria.getAttributeName())) {
            allAttributes.add(filterCriteria.getAttributeName());
        }
        return allAttributes;
    }
}
