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

import org.apache.atlas.discovery.SearchPipeline.IndexResultType;
import org.apache.atlas.discovery.SearchPipeline.PipelineContext;
import org.apache.atlas.discovery.SearchPipeline.PipelineStep;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.type.*;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.atlas.model.discovery.SearchParameters.*;

@Component
public class SolrStep implements PipelineStep {
    private static final Logger LOG = LoggerFactory.getLogger(SolrStep.class);

    private static final Pattern STRAY_AND_PATTERN     = Pattern.compile("(AND\\s+)+\\)");
    private static final Pattern STRAY_OR_PATTERN      = Pattern.compile("(OR\\s+)+\\)");
    private static final Pattern STRAY_ELIPSIS_PATTERN = Pattern.compile("(\\(\\s*)\\)");
    private static final String  AND_STR         = " AND ";
    private static final String  EMPTY_STRING    = "";
    private static final String  SPACE_STRING    = " ";
    private static final String  BRACE_OPEN_STR  = "( ";
    private static final String  BRACE_CLOSE_STR = " )";

    private static final Map<Operator, String> operatorMap = new HashMap<>();

    static
    {
        operatorMap.put(Operator.LT,"v.\"%s\": [* TO %s}");
        operatorMap.put(Operator.GT,"v.\"%s\": {%s TO *]");
        operatorMap.put(Operator.LTE,"v.\"%s\": [* TO %s]");
        operatorMap.put(Operator.GTE,"v.\"%s\": [%s TO *]");
        operatorMap.put(Operator.EQ,"v.\"%s\": %s");
        operatorMap.put(Operator.NEQ,"v.\"%s\": (NOT %s)");
        operatorMap.put(Operator.IN, "v.\"%s\": (%s)");
        operatorMap.put(Operator.LIKE, "v.\"%s\": (%s)");
        operatorMap.put(Operator.STARTS_WITH, "v.\"%s\": (%s*)");
        operatorMap.put(Operator.ENDS_WITH, "v.\"%s\": (*%s)");
        operatorMap.put(Operator.CONTAINS, "v.\"%s\": (*%s*)");
    }

    private final AtlasGraph graph;

    @Inject
    public SolrStep(AtlasGraph graph) {
        this.graph = graph;
    }

    @Override
    public void execute(PipelineContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SolrStep.execute({})", context);
        }

        if (context == null) {
            throw new AtlasBaseException("Can't start search without any context");
        }

        SearchParameters searchParameters = context.getSearchParameters();

        final Iterator<AtlasIndexQuery.Result> result;

        if (StringUtils.isNotEmpty(searchParameters.getQuery())) {
            result = executeAgainstFulltextIndex(context);
        } else {
            result = executeAgainstVertexIndex(context);
        }

        context.setIndexResultsIterator(result);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== SolrStep.execute({})", context);
        }
    }

    private Iterator<AtlasIndexQuery.Result> executeAgainstFulltextIndex(PipelineContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SolrStep.executeAgainstFulltextIndex()");
        }

        final Iterator<AtlasIndexQuery.Result> ret;

        AtlasIndexQuery query = context.getIndexQuery("FULLTEXT");

        if (query == null) {
            // Compute only once
            SearchParameters searchParameters = context.getSearchParameters();
            String           indexQuery       = String.format("v.\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, searchParameters.getQuery());

            query = graph.indexQuery(Constants.FULLTEXT_INDEX, indexQuery);

            context.cacheIndexQuery("FULLTEXT", query);
        }

        context.setIndexResultType(IndexResultType.TEXT);

        ret = query.vertices(context.getCurrentOffset(), context.getMaxLimit());

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== SolrStep.executeAgainstFulltextIndex()");
        }

        return ret;
    }

    private Iterator<AtlasIndexQuery.Result> executeAgainstVertexIndex(PipelineContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> SolrStep.executeAgainstVertexIndex()");
        }

        final Iterator<AtlasIndexQuery.Result> ret;

        SearchParameters searchParameters = context.getSearchParameters();
        AtlasIndexQuery  query            = context.getIndexQuery("VERTEX_INDEX");

        if (query == null) {
            StringBuilder solrQuery = new StringBuilder();

            // If tag is specified then let's start processing using tag and it's attributes, entity filters will
            // be pushed to Gremlin
            if (context.getClassificationType() != null) {
                context.setIndexResultType(IndexResultType.TAG);

                constructTypeTestQuery(solrQuery, context.getClassificationType().getTypeAndAllSubTypes());
                constructFilterQuery(solrQuery, context.getClassificationType(), searchParameters.getTagFilters(), context);
            } else if (context.getEntityType() != null) {
                context.setIndexResultType(IndexResultType.ENTITY);

                constructTypeTestQuery(solrQuery, context.getEntityType().getTypeAndAllSubTypes());
                constructFilterQuery(solrQuery, context.getEntityType(), searchParameters.getEntityFilters(), context);

                // Set the status flag
                if (searchParameters.getExcludeDeletedEntities()) {
                    if (solrQuery.length() > 0) {
                        solrQuery.append(" AND ");
                    }

                    solrQuery.append("v.\"__state\":").append("ACTIVE");
                }
            }

            // No query was formed, doesn't make sense to do anything beyond this point
            if (solrQuery.length() > 0) {
                String validSolrQuery = STRAY_AND_PATTERN.matcher(solrQuery).replaceAll(")");
                validSolrQuery = STRAY_OR_PATTERN.matcher(validSolrQuery).replaceAll(")");
                validSolrQuery = STRAY_ELIPSIS_PATTERN.matcher(validSolrQuery).replaceAll(EMPTY_STRING);

                query = graph.indexQuery(Constants.VERTEX_INDEX, validSolrQuery);
                context.cacheIndexQuery("VERTEX_INDEX", query);
            }
        }

        // Execute solr query and return the index results in the context
        if (query != null) {
            ret = query.vertices(context.getCurrentOffset(), context.getMaxLimit());
        } else {
            ret = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== SolrStep.executeAgainstVertexIndex()");
        }

        return ret;
    }

    private void constructTypeTestQuery(StringBuilder solrQuery, Set<String> typeAndAllSubTypes) {
        String typeAndSubtypesString = StringUtils.join(typeAndAllSubTypes, SPACE_STRING);

        solrQuery.append("v.\"__typeName\": (")
                .append(typeAndSubtypesString)
                .append(")");
    }

    private void constructFilterQuery(StringBuilder solrQuery, AtlasStructType type, FilterCriteria filterCriteria, PipelineContext context) {
        if (filterCriteria != null) {
            LOG.debug("Processing Filters");

            String filterQuery = toSolrQuery(type, filterCriteria, context);

            if (StringUtils.isNotEmpty(filterQuery)) {
                solrQuery.append(AND_STR).append(filterQuery);
            }
        }
    }

    private String toSolrQuery(AtlasStructType type, FilterCriteria criteria, PipelineContext context) {
        return toSolrQuery(type, criteria, context, new StringBuilder());
    }

    private String toSolrQuery(AtlasStructType type, FilterCriteria criteria, PipelineContext context, StringBuilder sb) {
        if (criteria.getCondition() != null && CollectionUtils.isNotEmpty(criteria.getCriterion())) {
            StringBuilder nestedExpression = new StringBuilder();

            for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                String nestedQuery = toSolrQuery(type, filterCriteria, context);

                if (StringUtils.isNotEmpty(nestedQuery)) {
                    if (nestedExpression.length() > 0) {
                        nestedExpression.append(SPACE_STRING).append(criteria.getCondition()).append(SPACE_STRING);
                    }

                    nestedExpression.append(nestedQuery);
                }
            }

            return nestedExpression.length() > 0 ? sb.append(BRACE_OPEN_STR).append(nestedExpression.toString()).append(BRACE_CLOSE_STR).toString() : EMPTY_STRING;
        } else {
            return toSolrExpression(type, criteria.getAttributeName(), criteria.getOperator(), criteria.getAttributeValue(), context);
        }
    }

    private String toSolrExpression(AtlasStructType type, String attrName, Operator op, String attrVal, PipelineContext context) {
        String ret = EMPTY_STRING;

        try {
            String    indexKey      = type.getQualifiedAttributeName(attrName);
            AtlasType attributeType = type.getAttributeType(attrName);

            switch (context.getIndexResultType()) {
                case TAG:
                    context.addTagSearchAttribute(indexKey);
                    break;

                case ENTITY:
                    context.addEntitySearchAttribute(indexKey);
                    break;

                default:
                    // Do nothing
            }

            if (attributeType != null && AtlasTypeUtil.isBuiltInType(attributeType.getTypeName()) && context.getIndexedKeys().contains(indexKey)) {
                if (operatorMap.get(op) != null) {
                    // If there's a chance of multi-value then we need some additional processing here
                    switch (context.getIndexResultType()) {
                        case TAG:
                            context.addProcessedTagAttribute(indexKey);
                            break;

                        case ENTITY:
                            context.addProcessedEntityAttribute(indexKey);
                            break;
                    }

                    ret = String.format(operatorMap.get(op), indexKey, attrVal);
                }
            }
        } catch (AtlasBaseException ex) {
            LOG.warn(ex.getMessage());
        }

        return ret;
    }
}
