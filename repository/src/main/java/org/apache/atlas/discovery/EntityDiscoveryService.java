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

import com.google.common.annotations.VisibleForTesting;
import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.SortOrder;
import org.apache.atlas.annotation.GraphTransaction;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasSearchResultScrubRequest;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasFullTextResult;
import org.apache.atlas.model.discovery.AtlasSearchResult.AtlasQueryType;
import org.apache.atlas.model.discovery.AtlasSearchResult.AttributeSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.profile.AtlasUserSavedSearch;
import org.apache.atlas.query.AtlasDSL;
import org.apache.atlas.query.GremlinQuery;
import org.apache.atlas.query.QueryParams;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery.Result;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.repository.userprofile.UserProfileService;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasBuiltInTypes.AtlasObjectIdType;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasGremlinQueryProvider;
import org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery;
import org.apache.atlas.util.SearchTracker;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.*;

import static org.apache.atlas.AtlasErrorCode.CLASSIFICATION_NOT_FOUND;
import static org.apache.atlas.AtlasErrorCode.DISCOVERY_QUERY_FAILED;
import static org.apache.atlas.AtlasErrorCode.UNKNOWN_TYPENAME;
import static org.apache.atlas.SortOrder.ASCENDING;
import static org.apache.atlas.SortOrder.DESCENDING;
import static org.apache.atlas.model.TypeCategory.ARRAY;
import static org.apache.atlas.model.TypeCategory.MAP;
import static org.apache.atlas.model.TypeCategory.OBJECT_ID_TYPE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.apache.atlas.util.AtlasGremlinQueryProvider.AtlasGremlinQuery.*;

@Component
public class EntityDiscoveryService implements AtlasDiscoveryService {
    private static final Logger LOG = LoggerFactory.getLogger(EntityDiscoveryService.class);
    private static final String DEFAULT_SORT_ATTRIBUTE_NAME = "name";

    private final AtlasGraph                      graph;
    private final EntityGraphRetriever            entityRetriever;
    private final AtlasGremlinQueryProvider       gremlinQueryProvider;
    private final AtlasTypeRegistry               typeRegistry;
    private final GraphBackedSearchIndexer        indexer;
    private final SearchTracker                   searchTracker;
    private final int                             maxResultSetSize;
    private final int                             maxTypesLengthInIdxQuery;
    private final int                             maxTagsLengthInIdxQuery;
    private final String                          indexSearchPrefix;
    private final UserProfileService              userProfileService;

    @Inject
    EntityDiscoveryService(AtlasTypeRegistry typeRegistry,
                           AtlasGraph graph, GraphBackedSearchIndexer indexer, SearchTracker searchTracker,
                           UserProfileService userProfileService) throws AtlasException {
        this.graph                    = graph;
        this.entityRetriever          = new EntityGraphRetriever(typeRegistry);
        this.indexer                  = indexer;
        this.searchTracker            = searchTracker;
        this.gremlinQueryProvider     = AtlasGremlinQueryProvider.INSTANCE;
        this.typeRegistry             = typeRegistry;
        this.maxResultSetSize         = ApplicationProperties.get().getInt(Constants.INDEX_SEARCH_MAX_RESULT_SET_SIZE, 150);
        this.maxTypesLengthInIdxQuery = ApplicationProperties.get().getInt(Constants.INDEX_SEARCH_TYPES_MAX_QUERY_STR_LENGTH, 512);
        this.maxTagsLengthInIdxQuery  = ApplicationProperties.get().getInt(Constants.INDEX_SEARCH_TAGS_MAX_QUERY_STR_LENGTH, 512);
        this.indexSearchPrefix        = AtlasGraphUtilsV2.getIndexSearchPrefix();
        this.userProfileService       = userProfileService;
    }

    @Override
    @GraphTransaction
    public AtlasSearchResult searchUsingDslQuery(String dslQuery, int limit, int offset) throws AtlasBaseException {
        AtlasSearchResult ret          = new AtlasSearchResult(dslQuery, AtlasQueryType.DSL);
        GremlinQuery      gremlinQuery = toGremlinQuery(dslQuery, limit, offset);
        String            queryStr     = gremlinQuery.queryStr();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing DSL: query={}, gremlinQuery={}", dslQuery, queryStr);
        }

        Object result = graph.executeGremlinScript(queryStr, false);

        if (result instanceof List && CollectionUtils.isNotEmpty((List)result)) {
            List   queryResult  = (List) result;
            Object firstElement = queryResult.get(0);

            if (firstElement instanceof AtlasVertex) {
                for (Object element : queryResult) {
                    if (element instanceof AtlasVertex) {
                        ret.addEntity(entityRetriever.toAtlasEntityHeaderWithClassifications((AtlasVertex)element));
                    } else {
                        LOG.warn("searchUsingDslQuery({}): expected an AtlasVertex; found unexpected entry in result {}", dslQuery, element);
                    }
                }
            } else if (gremlinQuery.hasSelectList()) {
                ret.setAttributes(toAttributesResult(queryResult, gremlinQuery));
            } else if (firstElement instanceof Map) {
                for (Object element : queryResult) {
                    if (element instanceof Map) {
                        Map map = (Map)element;

                        for (Object key : map.keySet()) {
                            Object value = map.get(key);

                            if (value instanceof List && CollectionUtils.isNotEmpty((List)value)) {
                                for (Object o : (List) value) {
                                    Object entry = o;
                                    if (entry instanceof AtlasVertex) {
                                        ret.addEntity(entityRetriever.toAtlasEntityHeader((AtlasVertex) entry));
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                LOG.warn("searchUsingDslQuery({}/{}): found unexpected entry in result {}", dslQuery, dslQuery, gremlinQuery.queryStr());
            }
        }

        scrubSearchResults(ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasSearchResult searchUsingFullTextQuery(String fullTextQuery, boolean excludeDeletedEntities, int limit, int offset)
                                                      throws AtlasBaseException {
        AtlasSearchResult ret      = new AtlasSearchResult(fullTextQuery, AtlasQueryType.FULL_TEXT);
        QueryParams       params   = QueryParams.getNormalizedParams(limit, offset);
        AtlasIndexQuery   idxQuery = toAtlasIndexQuery(fullTextQuery);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing Full text query: {}", fullTextQuery);
        }
        ret.setFullTextResult(getIndexQueryResults(idxQuery, params, excludeDeletedEntities));

        scrubSearchResults(ret);

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

        final QueryParams params              = QueryParams.getNormalizedParams(limit, offset);
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
                bindings.put("state", ACTIVE.toString());

                basicQuery += gremlinQueryProvider.getQuery(BASIC_SEARCH_STATE_FILTER);
            }

            if (isGuidPrefixSearch) {
                bindings.put("guid", attrValuePrefix + ".*");

                basicQuery += gremlinQueryProvider.getQuery(AtlasGremlinQuery.GUID_PREFIX_FILTER);
            }

            bindings.put("startIdx", params.offset());
            bindings.put("endIdx", params.offset() + params.limit());

            basicQuery += gremlinQueryProvider.getQuery(TO_RANGE_LIST);

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

        scrubSearchResults(ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasSearchResult searchWithParameters(SearchParameters searchParameters) throws AtlasBaseException {
        AtlasSearchResult ret = new AtlasSearchResult(searchParameters);
        final   QueryParams   params =  QueryParams.getNormalizedParams(searchParameters.getLimit(),searchParameters.getOffset());
        searchParameters.setLimit(params.limit());
        searchParameters.setOffset(params.offset());

        SearchContext context  = new SearchContext(searchParameters, typeRegistry, graph, indexer.getVertexIndexKeys());
        String        searchID = searchTracker.add(context); // For future cancellations

        try {
            List<AtlasVertex> resultList = context.getSearchProcessor().execute();

            // By default any attribute that shows up in the search parameter should be sent back in the response
            // If additional values are requested then the entityAttributes will be a superset of the all search attributes
            // and the explicitly requested attribute(s)
            Set<String> resultAttributes = new HashSet<>();
            Set<String> entityAttributes = new HashSet<>();

            if (CollectionUtils.isNotEmpty(searchParameters.getAttributes())) {
                resultAttributes.addAll(searchParameters.getAttributes());
            }

            if (CollectionUtils.isNotEmpty(context.getEntityAttributes())) {
                resultAttributes.addAll(context.getEntityAttributes());
            }

            AtlasEntityType entityType = context.getEntityType();
            if (entityType != null) {
                for (String resultAttribute : resultAttributes) {
                    AtlasAttribute  attribute  = entityType.getAttribute(resultAttribute);

                    if (attribute == null) {
                        attribute = entityType.getRelationshipAttribute(resultAttribute, null);
                    }

                    if (attribute != null) {
                        AtlasType attributeType = attribute.getAttributeType();

                        if (attributeType instanceof AtlasArrayType) {
                            attributeType = ((AtlasArrayType) attributeType).getElementType();
                        }

                        if (attributeType instanceof AtlasEntityType || attributeType instanceof AtlasObjectIdType) {
                            entityAttributes.add(resultAttribute);
                        }
                    }
                }
            }

            for (AtlasVertex atlasVertex : resultList) {
                AtlasEntityHeader entity = entityRetriever.toAtlasEntityHeader(atlasVertex, resultAttributes);

                if(searchParameters.getIncludeClassificationAttributes()) {
                    entity.setClassifications(entityRetriever.getAllClassifications(atlasVertex));
                }

                ret.addEntity(entity);

                // populate ret.referredEntities
                for (String entityAttribute : entityAttributes) {
                    Object attrValue = entity.getAttribute(entityAttribute);

                    if (attrValue instanceof AtlasObjectId) {
                        AtlasObjectId objId = (AtlasObjectId) attrValue;

                        if (ret.getReferredEntities() == null) {
                            ret.setReferredEntities(new HashMap<>());
                        }

                        if (!ret.getReferredEntities().containsKey(objId.getGuid())) {
                            ret.getReferredEntities().put(objId.getGuid(), entityRetriever.toAtlasEntityHeader(objId.getGuid()));
                        }
                    } else if (attrValue instanceof Collection) {
                        Collection objIds = (Collection) attrValue;

                        for (Object obj : objIds) {
                            if (obj instanceof AtlasObjectId) {
                                AtlasObjectId objId = (AtlasObjectId) obj;

                                if (ret.getReferredEntities() == null) {
                                    ret.setReferredEntities(new HashMap<>());
                                }

                                if (!ret.getReferredEntities().containsKey(objId.getGuid())) {
                                    ret.getReferredEntities().put(objId.getGuid(), entityRetriever.toAtlasEntityHeader(objId.getGuid()));
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            searchTracker.remove(searchID);
        }

        scrubSearchResults(ret);

        return ret;
    }

    @Override
    @GraphTransaction
    public AtlasSearchResult searchRelatedEntities(String guid, String relation, String sortByAttributeName, SortOrder sortOrder,
                                                   boolean excludeDeletedEntities, int limit, int offset) throws AtlasBaseException {
        AtlasSearchResult ret = new AtlasSearchResult(AtlasQueryType.RELATIONSHIP);

        if (StringUtils.isEmpty(guid) || StringUtils.isEmpty(relation)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS, "guid: '" + guid + "', relation: '" + relation + "'");
        }

        AtlasVertex     entityVertex   = entityRetriever.getEntityVertex(guid);
        String          entityTypeName = GraphHelper.getTypeName(entityVertex);
        AtlasEntityType entityType     = typeRegistry.getEntityTypeByName(entityTypeName);

        if (entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_RELATIONSHIP_TYPE, entityTypeName, guid);
        }

        AtlasAttribute attribute = entityType.getAttribute(relation);

        if (attribute == null) {
            attribute = entityType.getRelationshipAttribute(relation, null);
        }

        if (attribute != null) {
            if (attribute.isObjectRef()) {
                relation = attribute.getRelationshipEdgeLabel();
            } else {
                throw new AtlasBaseException(AtlasErrorCode.INVALID_RELATIONSHIP_ATTRIBUTE, relation, attribute.getTypeName());
            }
        }

        if (StringUtils.isEmpty(sortByAttributeName)) {
            sortByAttributeName = DEFAULT_SORT_ATTRIBUTE_NAME;
        }

        AtlasAttribute sortByAttribute = entityType.getAttribute(sortByAttributeName);

        if (sortByAttribute == null) {
            sortByAttributeName = null;
            sortOrder           = null;
        } else {
            sortByAttributeName = sortByAttribute.getQualifiedName();

            if (sortOrder == null) {
                sortOrder = ASCENDING;
            }
        }

        QueryParams  params               = QueryParams.getNormalizedParams(limit, offset);
        ScriptEngine scriptEngine         = graph.getGremlinScriptEngine();
        Bindings     bindings             = scriptEngine.createBindings();
        Set<String>  states               = getEntityStates();
        String       relatedEntitiesQuery = gremlinQueryProvider.getQuery(RELATIONSHIP_SEARCH);

        if (excludeDeletedEntities) {
            states.remove(DELETED.toString());
        }

        if (sortOrder == ASCENDING) {
            relatedEntitiesQuery += gremlinQueryProvider.getQuery(RELATIONSHIP_SEARCH_ASCENDING_SORT);
            bindings.put("sortAttributeName", sortByAttributeName);

        } else if (sortOrder == DESCENDING) {
            relatedEntitiesQuery += gremlinQueryProvider.getQuery(RELATIONSHIP_SEARCH_DESCENDING_SORT);
            bindings.put("sortAttributeName", sortByAttributeName);
        }

        relatedEntitiesQuery += gremlinQueryProvider.getQuery(TO_RANGE_LIST);

        bindings.put("g", graph);
        bindings.put("guid", guid);
        bindings.put("relation", relation);
        bindings.put("states", Collections.unmodifiableSet(states));
        bindings.put("startIdx", params.offset());
        bindings.put("endIdx", params.offset() + params.limit());

        try {
            Object result = graph.executeGremlinScript(scriptEngine, bindings, relatedEntitiesQuery, false);

            if (result instanceof List && CollectionUtils.isNotEmpty((List) result)) {
                List<?> queryResult  = (List) result;
                Object  firstElement = queryResult.get(0);

                if (firstElement instanceof AtlasVertex) {
                    List<AtlasVertex>       vertices   = (List<AtlasVertex>) queryResult;
                    List<AtlasEntityHeader> resultList = new ArrayList<>(vertices.size());

                    for (AtlasVertex vertex : vertices) {
                        resultList.add(entityRetriever.toAtlasEntityHeader(vertex));
                    }

                    ret.setEntities(resultList);
                }
            }

            if (ret.getEntities() == null) {
                ret.setEntities(new ArrayList<>());
            }
        } catch (ScriptException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Gremlin script execution failed for relationship search query: {}", relatedEntitiesQuery, e);
            }

            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Relationship search query failed");
        } finally {
            graph.releaseGremlinScriptEngine(scriptEngine);
        }

        scrubSearchResults(ret);

        return ret;
    }

    public int getMaxResultSetSize() {
        return maxResultSetSize;
    }

    private String getQueryForFullTextSearch(String userKeyedString, String typeName, String classification) {
        String typeFilter           = getTypeFilter(typeRegistry, typeName, maxTypesLengthInIdxQuery);
        String classificationFilter = getClassificationFilter(typeRegistry, classification, maxTagsLengthInIdxQuery);

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

        if (! StringUtils.isEmpty(classificationFilter)) {
            if (queryText.length() > 0) {
                queryText.append(" AND ");
            }

            queryText.append(classificationFilter);
        }

        return String.format(indexSearchPrefix + "\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, queryText.toString());
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
        QueryParams                 params       = QueryParams.getNormalizedParams(limit, offset);
        GremlinQuery                gremlinQuery = new AtlasDSL.Translator(query, typeRegistry, params.offset(), params.limit()).translate();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Translated Gremlin Query: {}", gremlinQuery.queryStr());
        }

        return gremlinQuery;
    }

    private AtlasIndexQuery toAtlasIndexQuery(String fullTextQuery) {
        String graphQuery = String.format(indexSearchPrefix + "\"%s\":(%s)", Constants.ENTITY_TEXT_PROPERTY_KEY, fullTextQuery);
        return graph.indexQuery(Constants.FULLTEXT_INDEX, graphQuery);
    }

    private AttributeSearchResult toAttributesResult(List results, GremlinQuery query) {
        AttributeSearchResult ret = new AttributeSearchResult();
        List<String> names = (List<String>) results.get(0);
        List<List<Object>> values = extractValues(results.subList(1, results.size()));

        ret.setName(names);
        ret.setValues(values);
        return ret;
    }

    private List<String> extractNames(List results) {
        List<String> names = new ArrayList<>();
        for (Object obj : results) {
            if (obj instanceof Map) {
                Map map = (Map) obj;
                if (MapUtils.isNotEmpty(map)) {
                    for (Object key : map.keySet()) {
                        names.add((String) key);
                    }
                    return names;
                }
            } else if (obj instanceof List) {
                List list = (List) obj;
                if (CollectionUtils.isNotEmpty(list)) {
                    for(Object o : list) {
                        names.add((String) o);
                    }
                }
            }
        }

        return names;
    }

    private List<List<Object>> extractValues(List results) {
        List<List<Object>> values = new ArrayList<>();

        for (Object obj : results) {
            if (obj instanceof Map) {
                Map map = (Map) obj;
                List<Object> list = new ArrayList<>();
                if (MapUtils.isNotEmpty(map)) {
                    for (Object key : map.keySet()) {
                       Object vals = map.get(key);
                       if(vals instanceof List) {
                           List l = (List) vals;
                           list.addAll(l);
                       }

                    }

                    values.add(list);
                }
            } else if (obj instanceof List) {
                List list = (List) obj;
                if (CollectionUtils.isNotEmpty(list)) {
                    values.add(list);
                }
            }
        }

        return values;
    }

    private boolean skipDeletedEntities(boolean excludeDeletedEntities, AtlasVertex<?, ?> vertex) {
        return excludeDeletedEntities && GraphHelper.getStatus(vertex) == DELETED;
    }

    private static String getClassificationFilter(AtlasTypeRegistry typeRegistry, String classificationName, int maxTypesLengthInIdxQuery) {
        AtlasClassificationType type                  = typeRegistry.getClassificationTypeByName(classificationName);
        String                  typeAndSubTypesQryStr = type != null ? type.getTypeAndAllSubTypesQryStr() : null;

        if(StringUtils.isNotEmpty(typeAndSubTypesQryStr) && typeAndSubTypesQryStr.length() <= maxTypesLengthInIdxQuery) {
            return typeAndSubTypesQryStr;
        }

        return "";
    }

    @VisibleForTesting
    static String getTypeFilter(AtlasTypeRegistry typeRegistry, String typeName, int maxTypesLengthInIdxQuery) {
        AtlasEntityType type                  = typeRegistry.getEntityTypeByName(typeName);
        String          typeAndSubTypesQryStr = type != null ? type.getTypeAndAllSubTypesQryStr() : null;

        if(StringUtils.isNotEmpty(typeAndSubTypesQryStr) && typeAndSubTypesQryStr.length() <= maxTypesLengthInIdxQuery) {
            return typeAndSubTypesQryStr;
        }

        return "";
    }

    private Set<String> getEntityStates() {
        return new HashSet<>(Arrays.asList(ACTIVE.toString(), DELETED.toString()));
    }


    @Override
    public AtlasUserSavedSearch addSavedSearch(String currentUser, AtlasUserSavedSearch savedSearch) throws AtlasBaseException {
        try {
            if (StringUtils.isEmpty(savedSearch.getOwnerName())) {
                savedSearch.setOwnerName(currentUser);
            }

            checkSavedSearchOwnership(currentUser, savedSearch);

            return userProfileService.addSavedSearch(savedSearch);
        } catch (AtlasBaseException e) {
            LOG.error("addSavedSearch({})", savedSearch, e);
            throw e;
        }
    }


    @Override
    public AtlasUserSavedSearch updateSavedSearch(String currentUser, AtlasUserSavedSearch savedSearch) throws AtlasBaseException {
        try {
            if (StringUtils.isEmpty(savedSearch.getOwnerName())) {
                savedSearch.setOwnerName(currentUser);
            }

            checkSavedSearchOwnership(currentUser, savedSearch);

            return userProfileService.updateSavedSearch(savedSearch);
        } catch (AtlasBaseException e) {
            LOG.error("updateSavedSearch({})", savedSearch, e);
            throw e;
        }
    }

    @Override
    public List<AtlasUserSavedSearch> getSavedSearches(String currentUser, String userName) throws AtlasBaseException {
        try {
            if (StringUtils.isEmpty(userName)) {
                userName = currentUser;
            } else if (!StringUtils.equals(currentUser, userName)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "invalid data");
            }

            return userProfileService.getSavedSearches(userName);
        } catch (AtlasBaseException e) {
            LOG.error("getSavedSearches({})", userName, e);
            throw e;
        }
    }

    @Override
    public AtlasUserSavedSearch getSavedSearchByGuid(String currentUser, String guid) throws AtlasBaseException {
        try {
            AtlasUserSavedSearch savedSearch = userProfileService.getSavedSearch(guid);

            checkSavedSearchOwnership(currentUser, savedSearch);

            return savedSearch;
        } catch (AtlasBaseException e) {
            LOG.error("getSavedSearchByGuid({})", guid, e);
            throw e;
        }
    }

    @Override
    public AtlasUserSavedSearch getSavedSearchByName(String currentUser, String userName, String searchName) throws AtlasBaseException {
        try {
            if (StringUtils.isEmpty(userName)) {
                userName = currentUser;
            } else if (!StringUtils.equals(currentUser, userName)) {
                throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "invalid data");
            }

            return userProfileService.getSavedSearch(userName, searchName);
        } catch (AtlasBaseException e) {
            LOG.error("getSavedSearchByName({}, {})", userName, searchName, e);
            throw e;
        }
    }

    @Override
    public void deleteSavedSearch(String currentUser, String guid) throws AtlasBaseException {
        try {
            AtlasUserSavedSearch savedSearch = userProfileService.getSavedSearch(guid);

            checkSavedSearchOwnership(currentUser, savedSearch);

            userProfileService.deleteSavedSearch(guid);
        } catch (AtlasBaseException e) {
            LOG.error("deleteSavedSearch({})", guid, e);
            throw e;
        }
    }

    @Override
    public String getDslQueryUsingTypeNameClassification(String query, String typeName, String classification) {
        String queryStr = query == null ? "" : query;

        if (StringUtils.isNotEmpty(typeName)) {
            queryStr = escapeTypeName(typeName) + " " + queryStr;
        }

        if (StringUtils.isNotEmpty(classification)) {
            // isa works with a type name only - like hive_column isa PII; it doesn't work with more complex query
            if (StringUtils.isEmpty(query)) {
                queryStr += (" isa " + classification);
            }
        }
        return queryStr;
    }

    private String escapeTypeName(String typeName) {
        String ret;

        if (StringUtils.startsWith(typeName, "`") && StringUtils.endsWith(typeName, "`")) {
            ret = typeName;
        } else {
            ret = String.format("`%s`", typeName);
        }

        return ret;
    }

    private void checkSavedSearchOwnership(String claimedOwner, AtlasUserSavedSearch savedSearch) throws AtlasBaseException {
        // block attempt to delete another user's saved-search
        if (savedSearch != null && !StringUtils.equals(savedSearch.getOwnerName(), claimedOwner)) {
            throw new AtlasBaseException(AtlasErrorCode.BAD_REQUEST, "invalid data");
        }
    }

    private void scrubSearchResults(AtlasSearchResult result) throws AtlasBaseException {
        AtlasAuthorizationUtils.scrubSearchResults(new AtlasSearchResultScrubRequest(typeRegistry, result));
    }
}
