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
package org.apache.atlas.repository.graphdb.janus;

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AtlasSolrQueryBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasSolrQueryBuilder.class);

    private AtlasEntityType     entityType;
    private String              queryString;
    private FilterCriteria      criteria;
    private boolean             excludeDeletedEntities;
    private boolean             includeSubtypes;
    private Map<String, String> indexFieldNameCache;


    public AtlasSolrQueryBuilder() {
    }

    public AtlasSolrQueryBuilder withEntityType(AtlasEntityType searchForEntityType) {
        this.entityType = searchForEntityType;

        return this;
    }

    public AtlasSolrQueryBuilder withQueryString(String queryString) {
        this.queryString = queryString;

        return this;
    }

    public AtlasSolrQueryBuilder withCriteria(FilterCriteria criteria) {
        this.criteria = criteria;

        return this;
    }

    public AtlasSolrQueryBuilder withExcludedDeletedEntities(boolean excludeDeletedEntities) {
        this.excludeDeletedEntities = excludeDeletedEntities;

        return this;
    }

    public AtlasSolrQueryBuilder withIncludeSubTypes(boolean includeSubTypes) {
        this.includeSubtypes = includeSubTypes;

        return this;
    }

    public AtlasSolrQueryBuilder withCommonIndexFieldNames(Map<String, String> indexFieldNameCache) {
        this.indexFieldNameCache = indexFieldNameCache;

        return this;
    }

    public String build() throws AtlasBaseException {
        StringBuilder queryBuilder = new StringBuilder();
        boolean       isAndNeeded  = false;

        if (queryString != null ) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Initial query string is {}.", queryString);
            }

            queryBuilder.append("+").append(queryString.trim()).append(" ");

            isAndNeeded = true;
        }

        if (excludeDeletedEntities) {
            if (isAndNeeded) {
                queryBuilder.append(" AND ");
            }

            dropDeletedEntities(queryBuilder);

            isAndNeeded = true;
        }

        if (entityType != null) {
            if (isAndNeeded) {
                queryBuilder.append(" AND ");
            }

            buildForEntityType(queryBuilder);

            isAndNeeded = true;
        }

        if (criteria != null) {
            StringBuilder attrFilterQueryBuilder = new StringBuilder();

            withCriteria(attrFilterQueryBuilder, criteria);

            if (attrFilterQueryBuilder.length() != 0) {
                if (isAndNeeded) {
                    queryBuilder.append(" AND ");
                }

                queryBuilder.append(" ").append(attrFilterQueryBuilder.toString());
            }
        }

        return queryBuilder.toString();
    }

    private void buildForEntityType(StringBuilder queryBuilder) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Search is being done for entities of type {}", entityType.getTypeName());
        }

        String typeIndexFieldName = indexFieldNameCache.get(Constants.ENTITY_TYPE_PROPERTY_KEY);

        queryBuilder.append(" +")
                    .append(typeIndexFieldName)
                    .append(":(")
                    .append(entityType.getTypeName())
                    .append(" ");

        if (includeSubtypes) {
            Set<String> allSubTypes = entityType.getAllSubTypes();

            if(allSubTypes.size() != 0 ) {
                for(String subTypeName: allSubTypes) {
                    queryBuilder.append(subTypeName).append(" ");
                }
            }
        }

        queryBuilder.append(" ) ");
    }

    private void dropDeletedEntities(StringBuilder queryBuilder) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("excluding the deleted entities.");
        }

        String indexFieldName = indexFieldNameCache.get(Constants.STATE_PROPERTY_KEY);

        if (indexFieldName == null) {
            String msg = String.format("There is no index field name defined for attribute '%s' for entity '%s'",
                                       Constants.STATE_PROPERTY_KEY,
                                       entityType.getTypeName());

            LOG.error(msg);

            throw new AtlasBaseException(msg);
        }

        queryBuilder.append(" -").append(indexFieldName).append(":").append(AtlasEntity.Status.DELETED.name());
    }

    private  AtlasSolrQueryBuilder withCriteria(StringBuilder queryBuilder, FilterCriteria criteria) throws AtlasBaseException {
        List<FilterCriteria> criterion = criteria.getCriterion();

        if(criterion == null || CollectionUtils.isEmpty(criteria.getCriterion())) { // no child criterion
            withPropertyCondition(queryBuilder, criteria.getAttributeName(), criteria.getOperator(), criteria.getAttributeValue());
        } else {
            beginCriteria(queryBuilder);

            for (Iterator<FilterCriteria> iterator = criterion.iterator(); iterator.hasNext(); ) {
                FilterCriteria childCriteria = iterator.next();

                withCriteria(queryBuilder, childCriteria);

                if (iterator.hasNext()) {
                    withCondition(queryBuilder, criteria.getCondition().name());
                }
            }

            endCriteria(queryBuilder);
        }

        return this;
    }

    private void withPropertyCondition(StringBuilder queryBuilder, String attributeName, Operator operator, String attributeValue) throws AtlasBaseException {
        if (StringUtils.isNotEmpty(attributeName) && operator != null) {
            if (attributeValue != null) {
                attributeValue = attributeValue.trim();
            }

            AtlasAttribute attribute = entityType.getAttribute(attributeName);

            if (attribute == null) {
                String msg = String.format("Received unknown attribute '%s' for type '%s'.", attributeName, entityType.getTypeName());

                LOG.error(msg);

                throw new AtlasBaseException(msg);
            }

            String indexFieldName = attribute.getIndexFieldName();

            if (indexFieldName == null) {
                String msg = String.format("Received non-index attribute %s for type %s.", attributeName, entityType.getTypeName());

                LOG.error(msg);

                throw new AtlasBaseException(msg);
            }

            switch (operator) {
                case EQ:
                    withEqual(queryBuilder, indexFieldName, attributeValue);
                    break;
                case NEQ:
                    withNotEqual(queryBuilder, indexFieldName, attributeValue);
                    break;
                case STARTS_WITH:
                    withStartsWith(queryBuilder, indexFieldName, attributeValue);
                    break;
                case ENDS_WITH:
                    withEndsWith(queryBuilder, indexFieldName, attributeValue);
                    break;
                case CONTAINS:
                    withContains(queryBuilder, indexFieldName, attributeValue);
                    break;
                case IS_NULL:
                    withIsNull(queryBuilder, indexFieldName);
                    break;
                case NOT_NULL:
                    withIsNotNull(queryBuilder, indexFieldName);
                    break;
                case LT:
                    withLessthan(queryBuilder, indexFieldName, attributeValue);
                    break;
                case GT:
                    withGreaterThan(queryBuilder, indexFieldName, attributeValue);
                    break;
                case LTE:
                    withLessthanOrEqual(queryBuilder, indexFieldName, attributeValue);
                    break;
                case GTE:
                    withGreaterThanOrEqual(queryBuilder, indexFieldName, attributeValue);
                    break;
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
    }

    private void beginCriteria(StringBuilder queryBuilder) {
        queryBuilder.append("( ");
    }

    private void endCriteria(StringBuilder queryBuilder) {
        queryBuilder.append(" )");
    }

    private void withEndsWith(StringBuilder queryBuilder, String indexFieldName, String attributeValue) {
        queryBuilder.append("+").append(indexFieldName).append(":*").append(attributeValue).append(" ");
    }

    private void withStartsWith(StringBuilder queryBuilder, String indexFieldName, String attributeValue) {
        queryBuilder.append("+").append(indexFieldName).append(":").append(attributeValue).append("* ");
    }

    private void withNotEqual(StringBuilder queryBuilder, String indexFieldName, String attributeValue) {
        queryBuilder.append("-").append(indexFieldName).append(":").append(attributeValue).append(" ");
    }

    private void withEqual(StringBuilder queryBuilder, String indexFieldName, String attributeValue) {
        queryBuilder.append("+").append(indexFieldName).append(":").append(attributeValue).append(" ");
    }

    private void withGreaterThan(StringBuilder queryBuilder, String indexFieldName, String attributeValue) {
        //{ == exclusive
        //] == inclusive
        //+__timestamp_l:{<attributeValue> TO *]
        queryBuilder.append("+").append(indexFieldName).append(":{ ").append(attributeValue).append(" TO * ] ");
    }

    private void withGreaterThanOrEqual(StringBuilder queryBuilder, String indexFieldName, String attributeValue) {
        //[ == inclusive
        //] == inclusive
        //+__timestamp_l:[<attributeValue> TO *]
        queryBuilder.append("+").append(indexFieldName).append(":[ ").append(attributeValue).append(" TO * ] ");
    }

    private void withLessthan(StringBuilder queryBuilder, String indexFieldName, String attributeValue) {
        //[ == inclusive
        //} == exclusive
        //+__timestamp_l:[* TO <attributeValue>}
        queryBuilder.append("+").append(indexFieldName).append(":[ * TO").append(attributeValue).append("} ");
    }

    private void withLessthanOrEqual(StringBuilder queryBuilder, String indexFieldName, String attributeValue) {
        //[ == inclusive
        //[ == inclusive
        //+__timestamp_l:[* TO <attributeValue>]
        queryBuilder.append("+").append(indexFieldName).append(":[ * TO ").append(attributeValue).append(" ] ");
    }

    private void withContains(StringBuilder queryBuilder, String indexFieldName, String attributeValue) {
        queryBuilder.append("+").append(indexFieldName).append(":*").append(attributeValue).append("* ");
    }

    private void withIsNull(StringBuilder queryBuilder, String indexFieldName) {
        queryBuilder.append("-").append(indexFieldName).append(":*").append(" ");
    }

    private void withIsNotNull(StringBuilder queryBuilder, String indexFieldName) {
        queryBuilder.append("+").append(indexFieldName).append(":*").append(" ");
    }

    private void withCondition(StringBuilder queryBuilder, String condition) {
        queryBuilder.append(" ").append(condition).append(" ");
    }
}
