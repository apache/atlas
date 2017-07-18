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


import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.Set;


public class SearchContext {
    private final SearchParameters        searchParameters;
    private final AtlasTypeRegistry       typeRegistry;
    private final AtlasGraph              graph;
    private final Set<String>             indexedKeys;
    private final Set<String>             entityAttributes;
    private final AtlasEntityType         entityType;
    private final AtlasClassificationType classificationType;
    private       SearchProcessor         searchProcessor;
    private       boolean                 terminateSearch = false;

    public SearchContext(SearchParameters searchParameters, AtlasTypeRegistry typeRegistry, AtlasGraph graph, Set<String> indexedKeys) {
        this.searchParameters   = searchParameters;
        this.typeRegistry       = typeRegistry;
        this.graph              = graph;
        this.indexedKeys        = indexedKeys;
        this.entityAttributes   = new HashSet<>();
        this.entityType         = typeRegistry.getEntityTypeByName(searchParameters.getTypeName());
        this.classificationType = typeRegistry.getClassificationTypeByName(searchParameters.getClassification());

        if (needFullTextrocessor()) {
            addProcessor(new FullTextSearchProcessor(this));
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

    public SearchProcessor getSearchProcessor() { return searchProcessor; }

    public boolean terminateSearch() { return this.terminateSearch; }

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

    boolean needFullTextrocessor() {
        return StringUtils.isNotEmpty(searchParameters.getQuery());
    }

    boolean needClassificationProcessor() {
        return classificationType != null && (entityType == null || hasAttributeFilter(searchParameters.getTagFilters()));
    }

    boolean needEntityProcessor() {
        return entityType != null;
    }

    private boolean hasAttributeFilter(FilterCriteria filterCriteria) {
        return filterCriteria != null &&
               (CollectionUtils.isNotEmpty(filterCriteria.getCriterion()) || StringUtils.isNotEmpty(filterCriteria.getAttributeName()));
    }

    private void addProcessor(SearchProcessor processor) {
        if (this.searchProcessor == null) {
            this.searchProcessor = processor;
        } else {
            this.searchProcessor.addProcessor(processor);
        }
    }
}
