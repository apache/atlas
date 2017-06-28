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
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria.Condition;
import org.apache.atlas.model.discovery.SearchParameters.Operator;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.AtlasGraphUtilsV1;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasPerfTracer;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.discovery.SearchPipeline.IndexResultType;
import static org.apache.atlas.discovery.SearchPipeline.PipelineContext;
import static org.apache.atlas.discovery.SearchPipeline.PipelineStep;
import static org.apache.atlas.repository.graphdb.AtlasGraphQuery.ComparisionOperator;
import static org.apache.atlas.repository.graphdb.AtlasGraphQuery.MatchingOperator;

@Component
public class GremlinStep implements PipelineStep {
    private static final Logger LOG      = LoggerFactory.getLogger(GremlinStep.class);
    private static final Logger PERF_LOG = AtlasPerfTracer.getPerfLogger("GremlinSearchStep");

    private final AtlasGraph        graph;
    private final AtlasTypeRegistry typeRegistry;

    enum GremlinFilterQueryType { TAG, ENTITY }

    @Inject
    public GremlinStep(AtlasGraph graph, AtlasTypeRegistry typeRegistry) {
        this.graph        = graph;
        this.typeRegistry = typeRegistry;
    }

    @Override
    public void execute(PipelineContext context) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> GremlinStep.execute({})", context);
        }

        if (context == null) {
            throw new AtlasBaseException("Can't start search without any context");
        }

        AtlasPerfTracer perf = null;

        if (AtlasPerfTracer.isPerfTraceEnabled(PERF_LOG)) {
            perf = AtlasPerfTracer.getPerfTracer(PERF_LOG, "GremlinSearchStep.execute(" + context +  ")");
        }

        final Iterator<AtlasVertex> result;

        if (context.hasIndexResults()) {
            // We have some results from the indexed step, let's proceed accordingly
            if (context.getIndexResultType() == IndexResultType.TAG) {
                // Index search was done on tag and filters
                if (context.isTagProcessingComplete()) {
                    LOG.debug("GremlinStep.execute(): index has completely processed tag, further TAG filtering not needed");

                    Set<String> taggedVertexGUIDs = new HashSet<>();

                    Iterator<AtlasIndexQuery.Result> tagVertexIterator = context.getIndexResultsIterator();

                    while (tagVertexIterator.hasNext()) {
                        // Find out which Vertex has this outgoing edge
                        AtlasVertex         vertex = tagVertexIterator.next().getVertex();
                        Iterable<AtlasEdge> edges  = vertex.getEdges(AtlasEdgeDirection.IN);

                        for (AtlasEdge edge : edges) {
                            String guid = AtlasGraphUtilsV1.getIdFromVertex(edge.getOutVertex());

                            taggedVertexGUIDs.add(guid);
                        }
                    }

                    // No entities are tagged  (actually this check is already done)
                    if (!taggedVertexGUIDs.isEmpty()) {
                        result = processEntity(taggedVertexGUIDs, context);
                    } else {
                        result = null;
                    }
                } else {
                    result = processTagAndEntity(Collections.<String>emptySet(), context);
                }
            } else if (context.getIndexResultType() == IndexResultType.TEXT) {
                // Index step processed full-text;
                Set<String> entityIDs = getVertexIDs(context.getIndexResultsIterator());

                result = processTagAndEntity(entityIDs, context);
            } else if (context.getIndexResultType() == IndexResultType.ENTITY) {
                // Index step processed entity and it's filters; tag filter wouldn't be set
                Set<String> entityIDs = getVertexIDs(context.getIndexResultsIterator());

                result = processEntity(entityIDs, context);
            } else {
                result = null;
            }
        } else {
            // No index results, need full processing in Gremlin
            if (context.getClassificationType() != null) {
                // Process tag and filters first, then entity filters
                result = processTagAndEntity(Collections.<String>emptySet(), context);
            } else {
                result = processEntity(Collections.<String>emptySet(), context);
            }
        }

        context.setGremlinResultIterator(result);

        AtlasPerfTracer.log(perf);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== GremlinStep.execute({})", context);
        }
    }

    private Iterator<AtlasVertex> processEntity(Set<String> entityGUIDs, PipelineContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> GremlinStep.processEntity(entityGUIDs={})", entityGUIDs);
        }

        final Iterator<AtlasVertex> ret;

        SearchParameters searchParameters = context.getSearchParameters();
        AtlasEntityType  entityType       = context.getEntityType();

        if (entityType != null) {
            AtlasGraphQuery entityFilterQuery = context.getGraphQuery("ENTITY_FILTER");

            if (entityFilterQuery == null) {
                entityFilterQuery = graph.query().in(Constants.TYPE_NAME_PROPERTY_KEY, entityType.getTypeAndAllSubTypes());

                if (searchParameters.getEntityFilters() != null) {
                    toGremlinFilterQuery(GremlinFilterQueryType.ENTITY, entityType, searchParameters.getEntityFilters(), entityFilterQuery, context);
                }

                if (searchParameters.getExcludeDeletedEntities()) {
                    entityFilterQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
                }

                context.cacheGraphQuery("ENTITY_FILTER", entityFilterQuery);
            }

            // Now get all vertices
            if (CollectionUtils.isEmpty(entityGUIDs)) {
                ret = entityFilterQuery.vertices(context.getCurrentOffset(), context.getMaxLimit()).iterator();
            } else {
                AtlasGraphQuery guidQuery = graph.query().in(Constants.GUID_PROPERTY_KEY, entityGUIDs);

                if (entityFilterQuery != null) {
                    guidQuery.addConditionsFrom(entityFilterQuery);
                } else if (searchParameters.getExcludeDeletedEntities()) {
                    guidQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
                }

                ret = guidQuery.vertices(context.getMaxLimit()).iterator();
            }
        } else if (CollectionUtils.isNotEmpty(entityGUIDs)) {
            AtlasGraphQuery guidQuery = graph.query().in(Constants.GUID_PROPERTY_KEY, entityGUIDs);

            if (searchParameters.getExcludeDeletedEntities()) {
                guidQuery.has(Constants.STATE_PROPERTY_KEY, "ACTIVE");
            }

            Iterable<AtlasVertex> vertices = guidQuery.vertices(context.getMaxLimit());

            ret = vertices.iterator();
        } else {
            ret = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== GremlinStep.processEntity(entityGUIDs={})", entityGUIDs);
        }

        return ret;
    }

    private Iterator<AtlasVertex> processTagAndEntity(Set<String> entityGUIDs, PipelineContext context) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> GremlinStep.processTagAndEntity(entityGUIDs={})", entityGUIDs);
        }

        final Iterator<AtlasVertex> ret;

        AtlasClassificationType classificationType = context.getClassificationType();

        if (classificationType != null) {
            AtlasGraphQuery  tagVertexQuery = context.getGraphQuery("TAG_VERTEX");

            if (tagVertexQuery == null) {
                tagVertexQuery = graph.query().in(Constants.TYPE_NAME_PROPERTY_KEY, classificationType.getTypeAndAllSubTypes());

                SearchParameters searchParameters = context.getSearchParameters();

                // Do tag filtering first as it'll return a smaller subset of vertices
                if (searchParameters.getTagFilters() != null) {
                    toGremlinFilterQuery(GremlinFilterQueryType.TAG, classificationType, searchParameters.getTagFilters(), tagVertexQuery, context);
                }

                context.cacheGraphQuery("TAG_VERTEX", tagVertexQuery);
            }

            if (tagVertexQuery != null) {
                Set<String> taggedVertexGuids = new HashSet<>();
                // Now get all vertices after adjusting offset for each iteration
                LOG.debug("Firing TAG query");

                Iterator<AtlasVertex> tagVertexIterator = tagVertexQuery.vertices(context.getCurrentOffset(), context.getMaxLimit()).iterator();

                while (tagVertexIterator.hasNext()) {
                    // Find out which Vertex has this outgoing edge
                    Iterable<AtlasEdge> edges = tagVertexIterator.next().getEdges(AtlasEdgeDirection.IN);
                    for (AtlasEdge edge : edges) {
                        String guid = AtlasGraphUtilsV1.getIdFromVertex(edge.getOutVertex());
                        taggedVertexGuids.add(guid);
                    }
                }

                entityGUIDs = taggedVertexGuids;
            }
        }

        if (!entityGUIDs.isEmpty()) {
            ret = processEntity(entityGUIDs, context);
        } else {
            ret = null;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== GremlinStep.processTagAndEntity(entityGUIDs={})", entityGUIDs);
        }

        return ret;
    }

    private Set<String> getVertexIDs(Iterator<AtlasIndexQuery.Result> idxResultsIterator) {
        Set<String> guids = new HashSet<>();
        while (idxResultsIterator.hasNext()) {
            AtlasVertex vertex = idxResultsIterator.next().getVertex();
            String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);
            guids.add(guid);
        }
        return guids;
    }

    private Set<String> getVertexIDs(Iterable<AtlasVertex> vertices) {
        Set<String> guids = new HashSet<>();
        for (AtlasVertex vertex : vertices) {
            String guid = AtlasGraphUtilsV1.getIdFromVertex(vertex);
            guids.add(guid);
        }
        return guids;
    }

    private AtlasGraphQuery toGremlinFilterQuery(GremlinFilterQueryType queryType, AtlasStructType type, FilterCriteria criteria,
                                                 AtlasGraphQuery query, PipelineContext context) {
        if (criteria.getCondition() != null) {
            if (criteria.getCondition() == Condition.AND) {
                for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                    AtlasGraphQuery nestedQuery = toGremlinFilterQuery(queryType, type, filterCriteria, graph.query(), context);
                    query.addConditionsFrom(nestedQuery);
                }
            } else {
                List<AtlasGraphQuery> orConditions = new LinkedList<>();

                for (FilterCriteria filterCriteria : criteria.getCriterion()) {
                    AtlasGraphQuery nestedQuery = toGremlinFilterQuery(queryType, type, filterCriteria, graph.query(), context);
                    // FIXME: Something might not be right here as the queries are getting overwritten sometimes
                    orConditions.add(graph.query().createChildQuery().addConditionsFrom(nestedQuery));
                }

                if (!orConditions.isEmpty()) {
                    query.or(orConditions);
                }
            }
        } else {
            String   attrName  = criteria.getAttributeName();
            String   attrValue = criteria.getAttributeValue();
            Operator operator  = criteria.getOperator();

            try {
                // If attribute belongs to supertype then adjust the name accordingly
                final String  qualifiedAttributeName;
                final boolean attrProcessed;

                if (queryType == GremlinFilterQueryType.TAG) {
                    qualifiedAttributeName = type.getQualifiedAttributeName(attrName);
                    attrProcessed          = context.hasProcessedTagAttribute(qualifiedAttributeName);
                } else {
                    qualifiedAttributeName = type.getQualifiedAttributeName(attrName);
                    attrProcessed          = context.hasProcessedEntityAttribute(qualifiedAttributeName);
                }

                // Check if the qualifiedAttribute has been processed
                if (!attrProcessed) {
                    switch (operator) {
                        case LT:
                            query.has(qualifiedAttributeName, ComparisionOperator.LESS_THAN, attrValue);
                            break;
                        case LTE:
                            query.has(qualifiedAttributeName, ComparisionOperator.LESS_THAN_EQUAL, attrValue);
                            break;
                        case GT:
                            query.has(qualifiedAttributeName, ComparisionOperator.GREATER_THAN, attrValue);
                            break;
                        case GTE:
                            query.has(qualifiedAttributeName, ComparisionOperator.GREATER_THAN_EQUAL, attrValue);
                            break;
                        case EQ:
                            query.has(qualifiedAttributeName, ComparisionOperator.EQUAL, attrValue);
                            break;
                        case NEQ:
                            query.has(qualifiedAttributeName, ComparisionOperator.NOT_EQUAL, attrValue);
                            break;
                        case LIKE:
                            // TODO: Maybe we need to validate pattern
                            query.has(qualifiedAttributeName, MatchingOperator.REGEX, getLikeRegex(attrValue));
                            break;
                        case CONTAINS:
                            query.has(qualifiedAttributeName, MatchingOperator.REGEX, getContainsRegex(attrValue));
                            break;
                        case STARTS_WITH:
                            query.has(qualifiedAttributeName, MatchingOperator.PREFIX, attrValue);
                            break;
                        case ENDS_WITH:
                            query.has(qualifiedAttributeName, MatchingOperator.REGEX, getSuffixRegex(attrValue));
                        case IN:
                            LOG.warn("{}: unsupported operator. Ignored", operator);
                            break;
                    }
                }
            } catch (AtlasBaseException e) {
                LOG.error("toGremlinFilterQuery(): failed for attrName=" + attrName + "; operator=" + operator + "; attrValue=" + attrValue, e);
            }
        }

        return query;
    }

    private String getContainsRegex(String attributeValue) {
        return ".*" + attributeValue + ".*";
    }

    private String getSuffixRegex(String attributeValue) {
        return ".*" + attributeValue;
    }

    private String getLikeRegex(String attributeValue) { return ".*" + attributeValue + ".*"; }
}
