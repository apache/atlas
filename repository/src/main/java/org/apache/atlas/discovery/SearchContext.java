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


import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.AtlasGraphUtilsV2;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.util.AtlasRepositoryConfiguration;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.atlas.model.discovery.SearchParameters.ALL_CLASSIFICATIONS;
import static org.apache.atlas.model.discovery.SearchParameters.NO_CLASSIFICATIONS;
import static org.apache.atlas.model.discovery.SearchParameters.WILDCARD_CLASSIFICATIONS;

/*
 * Search context captures elements required for performing a basic search
 * For every search request the search context will determine the execution sequence of the search processor(s) and the
 * possible chaining of processor(s)
 */
public class SearchContext {
    private static final Logger LOG      = LoggerFactory.getLogger(SearchContext.class);
    private final SearchParameters        searchParameters;
    private final AtlasTypeRegistry       typeRegistry;
    private final AtlasGraph              graph;
    private final Set<String>             indexedKeys;
    private final Set<String>             entityAttributes;
    private final AtlasEntityType         entityType;
    private final AtlasClassificationType classificationType;
    private       SearchProcessor         searchProcessor;
    private       boolean                 terminateSearch = false;
    private final Set<String>             typeAndSubTypes;
    private final Set<String>             classificationTypeAndSubTypes;
    private final String                  typeAndSubTypesQryStr;
    private final String                  classificationTypeAndSubTypesQryStr;

    public final static AtlasClassificationType MATCH_ALL_WILDCARD_CLASSIFICATION = new AtlasClassificationType(new AtlasClassificationDef(WILDCARD_CLASSIFICATIONS));
    public final static AtlasClassificationType MATCH_ALL_CLASSIFIED              = new AtlasClassificationType(new AtlasClassificationDef(ALL_CLASSIFICATIONS));
    public final static AtlasClassificationType MATCH_ALL_NOT_CLASSIFIED          = new AtlasClassificationType(new AtlasClassificationDef(NO_CLASSIFICATIONS));


    public SearchContext(SearchParameters searchParameters, AtlasTypeRegistry typeRegistry, AtlasGraph graph, Set<String> indexedKeys) throws AtlasBaseException {
        String classificationName = searchParameters.getClassification();

        this.searchParameters   = searchParameters;
        this.typeRegistry       = typeRegistry;
        this.graph              = graph;
        this.indexedKeys        = indexedKeys;
        this.entityAttributes   = new HashSet<>();
        this.entityType         = typeRegistry.getEntityTypeByName(searchParameters.getTypeName());
        this.classificationType = getClassificationType(classificationName);

        // Validate if the type name exists
        if (StringUtils.isNotEmpty(searchParameters.getTypeName()) && entityType == null) {
            throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, searchParameters.getTypeName());
        }

        // Validate if the classification exists
        if (StringUtils.isNotEmpty(classificationName) && classificationType == null) {
            throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_CLASSIFICATION, classificationName);
        }

        AtlasVertex glossaryTermVertex = getGlossaryTermVertex(searchParameters.getTermName());

        // Validate if the term exists
        if (StringUtils.isNotEmpty(searchParameters.getTermName()) && glossaryTermVertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_GLOSSARY_TERM, searchParameters.getTermName());
        }

        // Invalid attributes will raise an exception with 400 error code
        validateAttributes(entityType, searchParameters.getEntityFilters());

        // Invalid attribute will raise an exception with 400 error code
        validateAttributes(entityType, searchParameters.getSortBy());

        // Invalid attributes will raise an exception with 400 error code
        validateAttributes(classificationType, searchParameters.getTagFilters());

        if (entityType != null) {
            if (searchParameters.getIncludeSubTypes()) {
                typeAndSubTypes       = entityType.getTypeAndAllSubTypes();
                typeAndSubTypesQryStr = entityType.getTypeAndAllSubTypesQryStr();
            } else {
                typeAndSubTypes       = Collections.singleton(entityType.getTypeName());
                typeAndSubTypesQryStr = entityType.getTypeQryStr();
            }
        } else {
            typeAndSubTypes       = Collections.emptySet();
            typeAndSubTypesQryStr = "";
        }

        if (classificationType != null) {
            if (classificationType == MATCH_ALL_CLASSIFIED || classificationType == MATCH_ALL_NOT_CLASSIFIED || classificationType == MATCH_ALL_WILDCARD_CLASSIFICATION) {
                classificationTypeAndSubTypes       = Collections.emptySet();
                classificationTypeAndSubTypesQryStr = "";
            } else if (searchParameters.getIncludeSubClassifications()) {
                classificationTypeAndSubTypes       = classificationType.getTypeAndAllSubTypes();
                classificationTypeAndSubTypesQryStr = classificationType.getTypeAndAllSubTypesQryStr();
            } else {
                classificationTypeAndSubTypes       = Collections.singleton(classificationType.getTypeName());
                classificationTypeAndSubTypesQryStr = classificationType.getTypeQryStr();
            }
        } else {
            classificationTypeAndSubTypes       = Collections.emptySet();
            classificationTypeAndSubTypesQryStr = "";
        }

        if (glossaryTermVertex != null) {
            addProcessor(new TermSearchProcessor(this, getAssignedEntities(glossaryTermVertex)));
        }

        if (needFullTextProcessor()) {
            if (AtlasRepositoryConfiguration.isFreeTextSearchEnabled()) {
                LOG.debug("Using Free Text index based search.");

                addProcessor(new FreeTextSearchProcessor(this));
            } else {
                LOG.debug("Using Full Text index based search.");

                addProcessor(new FullTextSearchProcessor(this));
            }
        }

        if (needClassificationProcessor()) {
            addProcessor(new ClassificationSearchProcessor(this));
        }


        if (needEntityProcessor()) {
            addProcessor(new EntitySearchProcessor(this));
        }
    }

    public SearchParameters getSearchParameters() { return searchParameters; }

    public AtlasTypeRegistry getTypeRegistry() { return typeRegistry; }

    public AtlasGraph getGraph() { return graph; }

    public Set<String> getIndexedKeys() { return indexedKeys; }

    public Set<String> getEntityAttributes() { return entityAttributes; }

    public AtlasEntityType getEntityType() { return entityType; }

    public AtlasClassificationType getClassificationType() { return classificationType; }

    public Set<String> getEntityTypes() { return typeAndSubTypes; }

    public Set<String> getClassificationTypes() { return classificationTypeAndSubTypes; }

    public String getEntityTypesQryStr() { return typeAndSubTypesQryStr; }

    public String getClassificationTypesQryStr() { return classificationTypeAndSubTypesQryStr; }

    public SearchProcessor getSearchProcessor() { return searchProcessor; }

    public boolean includeEntityType(String entityType) {
        return typeAndSubTypes.isEmpty() || typeAndSubTypes.contains(entityType);
    }

    public boolean includeClassificationTypes(Collection<String> classificationTypes) {
        final boolean ret;

        if (classificationType == null) {
            ret = true;
        } else if (classificationType == MATCH_ALL_NOT_CLASSIFIED) {
            ret = CollectionUtils.isEmpty(classificationTypes);
        } else if (classificationType == MATCH_ALL_CLASSIFIED || classificationType == MATCH_ALL_WILDCARD_CLASSIFICATION) {
            ret = CollectionUtils.isNotEmpty(classificationTypes);
        } else {
            ret = CollectionUtils.containsAny(classificationTypeAndSubTypes, classificationTypes);
        }

        return ret;
    }

    public boolean terminateSearch() { return terminateSearch; }

    public void terminateSearch(boolean terminateSearch) { this.terminateSearch = terminateSearch; }

    public StringBuilder toString(StringBuilder sb) {
        if (sb == null) {
            sb = new StringBuilder();
        }

        sb.append("searchParameters=");

        if (searchParameters != null) {
            searchParameters.toString(sb);
        }

        return sb;
    }

    @Override
    public String toString() {
        return toString(new StringBuilder()).toString();
    }

    boolean needFullTextProcessor() {
        return StringUtils.isNotEmpty(searchParameters.getQuery());
    }

    boolean needClassificationProcessor() {
        return classificationType != null && (entityType == null || hasAttributeFilter(searchParameters.getTagFilters()));
    }

    boolean needEntityProcessor() {
        return entityType != null;
    }

    private void validateAttributes(final AtlasStructType structType, final FilterCriteria filterCriteria) throws AtlasBaseException {
        if (filterCriteria != null) {
            FilterCriteria.Condition condition = filterCriteria.getCondition();

            if (condition != null && CollectionUtils.isNotEmpty(filterCriteria.getCriterion())) {
                for (FilterCriteria criteria : filterCriteria.getCriterion()) {
                    validateAttributes(structType, criteria);
                }
            } else {
                String attributeName = filterCriteria.getAttributeName();
                validateAttributes(structType, attributeName);
            }
        }
    }

    private void validateAttributes(final AtlasStructType structType, final String... attributeNames) throws AtlasBaseException {
        for (String attributeName : attributeNames) {
            if (StringUtils.isNotEmpty(attributeName) && structType.getAttributeType(attributeName) == null) {
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_ATTRIBUTE, attributeName, structType.getTypeName());
            }
        }
    }

    private boolean hasAttributeFilter(FilterCriteria filterCriteria) {
        return filterCriteria != null &&
               (CollectionUtils.isNotEmpty(filterCriteria.getCriterion()) || StringUtils.isNotEmpty(filterCriteria.getAttributeName()));
    }

    private void addProcessor(SearchProcessor processor) {
        if (searchProcessor == null) {
            searchProcessor = processor;
        } else {
            searchProcessor.addProcessor(processor);
        }
    }

    private AtlasClassificationType getClassificationType(String classificationName) {
        AtlasClassificationType ret;

        if (StringUtils.equals(classificationName, MATCH_ALL_WILDCARD_CLASSIFICATION.getTypeName())) {
            ret = MATCH_ALL_WILDCARD_CLASSIFICATION;
        } else if (StringUtils.equals(classificationName, MATCH_ALL_CLASSIFIED.getTypeName())) {
            ret = MATCH_ALL_CLASSIFIED;
        } else if (StringUtils.equals(classificationName, MATCH_ALL_NOT_CLASSIFIED.getTypeName())) {
            ret = MATCH_ALL_NOT_CLASSIFIED;
        } else {
            ret = typeRegistry.getClassificationTypeByName(classificationName);
        }

        return ret;
    }

    private AtlasVertex getGlossaryTermVertex(String termName) {
        AtlasVertex ret = null;

        if (StringUtils.isNotEmpty(termName)) {
            AtlasEntityType termType = getTermEntityType();
            AtlasAttribute  attrName = termType.getAttribute(TermSearchProcessor.ATLAS_GLOSSARY_TERM_ATTR_QNAME);
            AtlasGraphQuery query    = graph.query().has(Constants.ENTITY_TYPE_PROPERTY_KEY, termType.getTypeName())
                                                    .has(attrName.getVertexPropertyName(), termName)
                                                    .has(Constants.STATE_PROPERTY_KEY, AtlasEntity.Status.ACTIVE.name());

            Iterator<AtlasVertex> results = query.vertices().iterator();

            ret = results.hasNext() ? results.next() : null;
        }

        return ret;
    }

    private List<AtlasVertex> getAssignedEntities(AtlasVertex glossaryTerm) {
        List<AtlasVertex>   ret      = new ArrayList<>();
        AtlasEntityType     termType = getTermEntityType();
        AtlasAttribute      attr     = termType.getRelationshipAttribute(TermSearchProcessor.ATLAS_GLOSSARY_TERM_ATTR_ASSIGNED_ENTITIES, EntityGraphRetriever.TERM_RELATION_NAME);
        Iterator<AtlasEdge> edges    = GraphHelper.getEdgesForLabel(glossaryTerm, attr.getRelationshipEdgeLabel(), attr.getRelationshipEdgeDirection());

        boolean excludeDeletedEntities = searchParameters.getExcludeDeletedEntities();
        if (edges != null) {
            while (edges.hasNext()) {
                AtlasEdge edge = edges.next();

                AtlasVertex inVertex = edge.getInVertex();
                if (excludeDeletedEntities && AtlasGraphUtilsV2.getState(inVertex) == AtlasEntity.Status.DELETED) {
                    continue;
                }
                ret.add(inVertex);
            }
        }

        return ret;
    }

    private AtlasEntityType getTermEntityType() {
        return typeRegistry.getEntityTypeByName(TermSearchProcessor.ATLAS_GLOSSARY_TERM_ENTITY_TYPE);
    }
}
