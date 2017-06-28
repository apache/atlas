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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasConfiguration;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.discovery.graph.DefaultGraphPersistenceStrategy;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasFullTextResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.discovery.AtlasSearchResult.AttributeSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntity.Status;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.query.Expressions.AliasExpression;
import org.apache.atlas.query.Expressions.Expression;
import org.apache.atlas.query.Expressions.SelectExpression;
import org.apache.atlas.query.GremlinQuery;
import org.apache.atlas.query.GremlinTranslator;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.query.QueryParser;
import org.apache.atlas.query.QueryProcessor;
import org.apache.atlas.query.SelectExpressionHelper;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery.Result;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v1.EntityGraphRetriever;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scala.Option;
import scala.util.Either;
import scala.util.parsing.combinator.Parsers.NoSuccess;

import javax.inject.Inject;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.AtlasErrorCode.CLASSIFICATION_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.DISCOVERY_QUERY_FAILED;
import static org.apache.atlas.AtlasErrorCode.UNKNOWN_TYPENAME;

@Component
public class EntityDiscoveryService implements AtlasDiscoveryService {
    private static final Logger LOG = LoggerFactory.getLogger(EntityDiscoveryService.class);

    private final AtlasGraph                      graph;
    private final DefaultGraphPersistenceStrategy graphPersistenceStrategy;
    private final EntityGraphRetriever            entityRetriever;
    private final AtlasGremlinQueryProvider       gremlinQueryProvider;
    private final AtlasTypeRegistry               typeRegistry;
    private final SearchPipeline                  searchPipeline;
    private final int                             maxResultSetSize;
    private final int                             maxTypesCountInIdxQuery;
    private final int                             maxTagsCountInIdxQuery;

    @Inject
    EntityDiscoveryService(MetadataRepository metadataRepository, AtlasTypeRegistry typeRegistry,
                           AtlasGraph graph, SearchPipeline searchPipeline) throws AtlasException {
        this.graph                    = graph;
        this.graphPersistenceStrategy = new DefaultGraphPersistenceStrategy(metadataRepository);
        this.entityRetriever          = new EntityGraphRetriever(typeRegistry);
        this.gremlinQueryProvider     = AtlasGremlinQueryProvider.INSTANCE;
        this.typeRegistry             = typeRegistry;
        this.searchPipeline           = searchPipeline;

        this.maxResultSetSize         = ApplicationProperties.get().getInt(Constants.INDEX_SEARCH_MAX_RESULT_SET_SIZE, 150);
        this.maxTypesCountInIdxQuery  = ApplicationProperties.get().getInt(Constants.INDEX_SEARCH_MAX_TYPES_COUNT, 10);
        this.maxTagsCountInIdxQuery   = ApplicationProperties.get().getInt(Constants.INDEX_SEARCH_MAX_TAGS_COUNT, 10);
    }

    @Override
    @GraphTransaction
    public AtlasSearchResult searchUsingDslQuery(String dslQuery, int limit, int offset) throws AtlasBaseException {
        AtlasSearchResult ret = new AtlasSearchResult(dslQuery, AtlasQueryType.DSL);
        GremlinQuery gremlinQuery = toGremlinQuery(dslQuery, limit, offset);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing DSL query: {}", dslQuery);
        }

        Object result = graph.executeGremlinScript(gremlinQuery.queryStr(), false);

        if (result instanceof List && CollectionUtils.isNotEmpty((List)result)) {
            List   queryResult  = (List) result;
            Object firstElement = queryResult.get(0);

            if (firstElement instanceof AtlasVertex) {
                for (Object element : queryResult) {
                    if (element instanceof AtlasVertex) {
                        ret.addEntity(entityRetriever.toAtlasEntityHeader((AtlasVertex)element));
                    } else {
                        LOG.warn("searchUsingDslQuery({}): expected an AtlasVertex; found unexpected entry in result {}", dslQuery, element);
                    }
                }
            } else if (firstElement instanceof Map &&
                       (((Map)firstElement).containsKey("theInstance") || ((Map)firstElement).containsKey("theTrait"))) {
                for (Object element : queryResult) {
                    if (element instanceof Map) {
                        Map map = (Map)element;

                        if (map.containsKey("theInstance")) {
                            Object value = map.get("theInstance");

                            if (value instanceof List && CollectionUtils.isNotEmpty((List)value)) {
                                Object entry = ((List)value).get(0);

                                if (entry instanceof AtlasVertex) {
                                    ret.addEntity(entityRetriever.toAtlasEntityHeader((AtlasVertex)entry));
                                }
                            }
                        }
                    } else {
                        LOG.warn("searchUsingDslQuery({}): expected a trait result; found unexpected entry in result {}", dslQuery, element);
                    }
                }
            } else if (gremlinQuery.hasSelectList()) {
                ret.setAttributes(toAttributesResult(queryResult, gremlinQuery));
            }
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasSearchResult searchUsingFullTextQuery(String fullTextQuery, boolean excludeDeletedEntities, int limit, int offset)
                                                      throws AtlasBaseException {
        AtlasSearchResult ret      = new AtlasSearchResult(fullTextQuery, AtlasQueryType.FULL_TEXT);
        QueryParams       params   = validateSearchParams(limit, offset);
        AtlasIndexQuery   idxQuery = toAtlasIndexQuery(fullTextQuery);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing Full text query: {}", fullTextQuery);
        }
        ret.setFullTextResult(getIndexQueryResults(idxQuery, params, excludeDeletedEntities));

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasSearchResult searchUsingBasicQuery(String query, String typeName, String classification, String attrName,
                                                   String attrValuePrefix, boolean excludeDeletedEntities, int limit,
                                                   int offset) throws AtlasBaseException {

        AtlasSearchResult ret = new AtlasSearchResult(AtlasQueryType.BASIC);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing basic search query: {} with type: {} and classification: {}", query, typeName, classification);
        }

        final QueryParams params              = validateSearchParams(limit, offset);
        Set<String>       typeNames           = null;
        Set<String>       classificationNames = null;
        String            attrQualifiedName   = null;

        if (StringUtils.isNotEmpty(typeName)) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            if (entityType == null) {
                throw new AtlasBaseException(UNKNOWN_TYPENAME, typeName);
            }

            typeNames = entityType.getTypeAndAllSubTypes();

            ret.setType(typeName);
        }

        if (StringUtils.isNotEmpty(classification)) {
            AtlasClassificationType classificationType = typeRegistry.getClassificationTypeByName(classification);

            if (classificationType == null) {
                throw new AtlasBaseException(CLASSIFICATION_NOT_FOUND, classification);
            }

            classificationNames = classificationType.getTypeAndAllSubTypes();

            ret.setClassification(classification);
        }

        boolean isAttributeSearch  = StringUtils.isNotEmpty(attrName) || StringUtils.isNotEmpty(attrValuePrefix);
        boolean isGuidPrefixSearch = false;

        if (isAttributeSearch) {
            AtlasEntityType entityType = typeRegistry.getEntityTypeByName(typeName);

            ret.setQueryType(AtlasQueryType.ATTRIBUTE);

            if (entityType != null) {
                AtlasAttribute attribute = null;

                if (StringUtils.isNotEmpty(attrName)) {
                    attribute = entityType.getAttribute(attrName);

                    if (attribute == null) {
                        throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_ATTRIBUTE, attrName, typeName);
                    }

                } else {
                    // if attrName is null|empty iterate defaultAttrNames to get attribute value
                    final List<String> defaultAttrNames = new ArrayList<>(Arrays.asList("qualifiedName", "name"));
                    Iterator<String>   iter             = defaultAttrNames.iterator();

                    while (iter.hasNext() && attribute == null) {
                        attrName  = iter.next();
                        attribute = entityType.getAttribute(attrName);
                    }
                }

                if (attribute == null) {
                    // for guid prefix search use gremlin and nullify query to avoid using fulltext
                    // (guids cannot be searched in fulltext)
                    isGuidPrefixSearch = true;
                    query              = null;

                } else {
                    attrQualifiedName = attribute.getQualifiedName();

                    String attrQuery = String.format("%s AND (%s *)", attrName, attrValuePrefix.replaceAll("\\.", " "));

                    query = StringUtils.isEmpty(query) ? attrQuery : String.format("(%s) AND (%s)", query, attrQuery);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Executing attribute search attrName: {} and attrValue: {}", attrName, attrValuePrefix);
            }
        }

        // if query was provided, perform indexQuery and filter for typeName & classification in memory; this approach
        // results in a faster and accurate results than using CONTAINS/CONTAINS_PREFIX filter on entityText property
        if (StringUtils.isNotEmpty(query)) {
            final String idxQuery   = getQueryForFullTextSearch(query, typeName, classification);
            final int    startIdx   = params.offset();
            final int    resultSize = params.limit();
            int          resultIdx  = 0;

            for (int indexQueryOffset = 0; ; indexQueryOffset += getMaxResultSetSize()) {
                final Iterator<Result<?, ?>> qryResult = graph.indexQuery(Constants.FULLTEXT_INDEX, idxQuery, indexQueryOffset).vertices();

                if (LOG.isDebugEnabled()) {
                    LOG.debug("indexQuery: query=" + idxQuery + "; offset=" + indexQueryOffset);
                }

                if(!qryResult.hasNext()) {
                    break;
                }

                while (qryResult.hasNext()) {
                    AtlasVertex<?, ?> vertex         = qryResult.next().getVertex();
                    String            vertexTypeName = GraphHelper.getTypeName(vertex);

                    // skip non-entity vertices
                    if (StringUtils.isEmpty(vertexTypeName) || StringUtils.isEmpty(GraphHelper.getGuid(vertex))) {
                        continue;
                    }

                    if (typeNames != null && !typeNames.contains(vertexTypeName)) {
                        continue;
                    }

                    if (classificationNames != null) {
                        List<String> traitNames = GraphHelper.getTraitNames(vertex);

                        if (CollectionUtils.isEmpty(traitNames) ||
                                !CollectionUtils.containsAny(classificationNames, traitNames)) {
                            continue;
                        }
                    }

                    if (isAttributeSearch) {
                        String vertexAttrValue = vertex.getProperty(attrQualifiedName, String.class);

                        if (StringUtils.isNotEmpty(vertexAttrValue) && !vertexAttrValue.startsWith(attrValuePrefix)) {
                            continue;
                        }
                    }

                    if (skipDeletedEntities(excludeDeletedEntities, vertex)) {
                        continue;
                    }

                    resultIdx++;

                    if (resultIdx <= startIdx) {
                        continue;
                    }

                    AtlasEntityHeader header = entityRetriever.toAtlasEntityHeader(vertex);

                    ret.addEntity(header);

                    if (ret.getEntities().size() == resultSize) {
                        break;
                    }
                }

                if (ret.getEntities() != null && ret.getEntities().size() == resultSize) {
                    break;
                }
            }
        } else {
            final Map<String, Object> bindings   = new HashMap<>();
            String                    basicQuery = "g.V()";

            if (classificationNames != null) {
                bindings.put("traitNames", classificationNames);

                basicQuery += gremlinQueryProvider.getQuery(AtlasGremlinQuery.BASIC_SEARCH_CLASSIFICATION_FILTER);
            }

            if (typeNames != null) {
                bindings.put("typeNames", typeNames);

                basicQuery += gremlinQueryProvider.getQuery(AtlasGremlinQuery.BASIC_SEARCH_TYPE_FILTER);
            }

            if (excludeDeletedEntities) {
                bindings.put("state", Status.ACTIVE.toString());

                basicQuery += gremlinQueryProvider.getQuery(AtlasGremlinQuery.BASIC_SEARCH_STATE_FILTER);
            }

            if (isGuidPrefixSearch) {
                bindings.put("guid", attrValuePrefix + ".*");

                basicQuery += gremlinQueryProvider.getQuery(AtlasGremlinQuery.GUID_PREFIX_FILTER);
            }

            bindings.put("startIdx", params.offset());
            bindings.put("endIdx", params.offset() + params.limit());

            basicQuery += gremlinQueryProvider.getQuery(AtlasGremlinQuery.TO_RANGE_LIST);

            ScriptEngine scriptEngine = graph.getGremlinScriptEngine();

            try {
                Object result = graph.executeGremlinScript(scriptEngine, bindings, basicQuery, false);

                if (result instanceof List && CollectionUtils.isNotEmpty((List) result)) {
                    List queryResult = (List) result;
                    Object firstElement = queryResult.get(0);

                    if (firstElement instanceof AtlasVertex) {
                        for (Object element : queryResult) {
                            if (element instanceof AtlasVertex) {

                                ret.addEntity(entityRetriever.toAtlasEntityHeader((AtlasVertex) element));
                            } else {
                                LOG.warn("searchUsingBasicQuery({}): expected an AtlasVertex; found unexpected entry in result {}", basicQuery, element);
                            }
                        }
                    }
                }
            } catch (ScriptException e) {
                throw new AtlasBaseException(DISCOVERY_QUERY_FAILED, basicQuery);
            } finally {
                graph.releaseGremlinScriptEngine(scriptEngine);
            }
        }

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasSearchResult searchUsingBasicQuery(SearchParameters searchParameters) throws AtlasBaseException {
        AtlasSearchResult ret = new AtlasSearchResult(searchParameters);

        List<AtlasVertex> resultList = searchPipeline.run(searchParameters);

        for (AtlasVertex atlasVertex : resultList) {
            AtlasEntityHeader entity = entityRetriever.toAtlasEntityHeader(atlasVertex, searchParameters.getAttributes());

            ret.addEntity(entity);
        }

        return ret;
    }

    private String getQueryForFullTextSearch(String userKeyedString, String typeName, String classification) {
        String typeFilter          = getTypeFilter(typeRegistry, typeName, maxTypesCountInIdxQuery);
        String classficationFilter = getClassificationFilter(typeRegistry, classification, maxTagsCountInIdxQuery);

        StringBuilder queryText = new StringBuilder();

        if (! StringUtils.isEmpty(userKeyedString)) {
            queryText.append(userKeyedString);
        }

        if (! StringUtils.isEmpty(typeFilter)) {
            if (queryText.length() > 0) {
                queryText.append(" AND ");
            }

            queryText.append(typeFilter);
        }

        if (! StringUtils.isEmpty(classficationFilter)) {
            if (queryText.length() > 0) {
                queryText.append(" AND ");
            }

            queryText.append(classficationFilter);
        }

        return String.format("v.\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, queryText.toString());
    }

    private static String getClassificationFilter(AtlasTypeRegistry typeRegistry, String classificationName, int maxTypesCountInIdxQuery) {
        AtlasClassificationType classification  = typeRegistry.getClassificationTypeByName(classificationName);
        Set<String>             typeAndSubTypes = classification != null ? classification.getTypeAndAllSubTypes() : null;

        if(CollectionUtils.isNotEmpty(typeAndSubTypes) && typeAndSubTypes.size() <= maxTypesCountInIdxQuery) {
            return String.format("(%s)", StringUtils.join(typeAndSubTypes, " "));
        }

        return "";
    }

    private static String getTypeFilter(AtlasTypeRegistry typeRegistry, String typeName, int maxTypesCountInIdxQuery) {
        AtlasEntityType type            = typeRegistry.getEntityTypeByName(typeName);
        Set<String>     typeAndSubTypes = type != null ? type.getTypeAndAllSubTypes() : null;

        if(CollectionUtils.isNotEmpty(typeAndSubTypes) && typeAndSubTypes.size() <= maxTypesCountInIdxQuery) {
            return String.format("(%s)", StringUtils.join(typeAndSubTypes, " "));
        }

        return "";
    }

    private List<AtlasFullTextResult> getIndexQueryResults(AtlasIndexQuery query, QueryParams params, boolean excludeDeletedEntities) throws AtlasBaseException {
        List<AtlasFullTextResult> ret  = new ArrayList<>();
        Iterator<Result>          iter = query.vertices();

        while (iter.hasNext() && ret.size() < params.limit()) {
            Result      idxQueryResult = iter.next();
            AtlasVertex vertex         = idxQueryResult.getVertex();

            if (skipDeletedEntities(excludeDeletedEntities, vertex)) {
                continue;
            }

            String guid = vertex != null ? vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class) : null;

            if (guid != null) {
                AtlasEntityHeader entity = entityRetriever.toAtlasEntityHeader(vertex);
                Double score = idxQueryResult.getScore();
                ret.add(new AtlasFullTextResult(entity, score));
            }
        }

        return ret;
    }

    private GremlinQuery toGremlinQuery(String query, int limit, int offset) throws AtlasBaseException {
        QueryParams params = validateSearchParams(limit, offset);
        Either<NoSuccess, Expression> either = QueryParser.apply(query, params);

        if (either.isLeft()) {
            throw new AtlasBaseException(DISCOVERY_QUERY_FAILED, query);
        }

        Expression   expression      = either.right().get();
        Expression   validExpression = QueryProcessor.validate(expression);
        GremlinQuery gremlinQuery    = new GremlinTranslator(validExpression, graphPersistenceStrategy).translate();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Translated Gremlin Query: {}", gremlinQuery.queryStr());
        }

        return gremlinQuery;
    }

    private QueryParams validateSearchParams(int limitParam, int offsetParam) {
        int defaultLimit = AtlasConfiguration.SEARCH_DEFAULT_LIMIT.getInt();
        int maxLimit     = AtlasConfiguration.SEARCH_MAX_LIMIT.getInt();

        int limit = defaultLimit;
        if (limitParam > 0 && limitParam <= maxLimit) {
            limit = limitParam;
        }

        int offset = 0;
        if (offsetParam > 0) {
            offset = offsetParam;
        }

        return new QueryParams(limit, offset);
    }

    private AtlasIndexQuery toAtlasIndexQuery(String fullTextQuery) {
        String graphQuery = String.format("v.\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, fullTextQuery);
        return graph.indexQuery(Constants.FULLTEXT_INDEX, graphQuery);
    }

    private AttributeSearchResult toAttributesResult(List list, GremlinQuery query) {
        AttributeSearchResult ret = new AttributeSearchResult();
        List<String> names = new ArrayList<>();
        List<List<Object>> values = new ArrayList<>();

        // extract select attributes from gremlin query
        Option<SelectExpression> selectExpr = SelectExpressionHelper.extractSelectExpression(query.expr());
        if (selectExpr.isDefined()) {
            List<AliasExpression> aliases = selectExpr.get().toJavaList();

            if (CollectionUtils.isNotEmpty(aliases)) {
                for (AliasExpression alias : aliases) {
                    names.add(alias.alias());
                }
                ret.setName(names);
            }
        }

        for (Object mapObj : list) {
            Map map = (mapObj instanceof Map ? (Map) mapObj : null);
            if (MapUtils.isNotEmpty(map)) {
                for (Object key : map.keySet()) {
                    Object vals = map.get(key);
                    values.add((List<Object>) vals);
                }
                ret.setValues(values);
            }
        }

        return ret;
    }

    private boolean skipDeletedEntities(boolean excludeDeletedEntities, AtlasVertex<?, ?> vertex) {
        return excludeDeletedEntities && GraphHelper.getStatus(vertex) == Status.DELETED;
    }

    public int getMaxResultSetSize() {
        return maxResultSetSize;
    }

}
