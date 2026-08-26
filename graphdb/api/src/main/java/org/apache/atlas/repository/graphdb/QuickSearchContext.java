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
package org.apache.atlas.repository.graphdb;

import org.apache.atlas.model.discovery.SearchParameters.FilterCriteria;
import org.apache.atlas.type.AtlasEntityType;

import java.util.Map;
import java.util.Set;

/**
 * Inputs for OpenSearch-native weighted quick search (C5.5.3).
 */
public class QuickSearchContext {
    private final String               queryString;
    private final FilterCriteria       filterCriteria;
    private final Set<AtlasEntityType> entityTypes;
    private final Set<String>          classificationTypeNames;
    private final Map<String, String>  indexFieldNameCache;
    private final boolean              excludeDeletedEntities;
    private final boolean              includeSubTypes;
    private final int                  offset;
    private final int                  limit;

    public QuickSearchContext(String queryString, FilterCriteria filterCriteria, Set<AtlasEntityType> entityTypes,
                              Set<String> classificationTypeNames, Map<String, String> indexFieldNameCache,
                              boolean excludeDeletedEntities, boolean includeSubTypes, int offset, int limit) {
        this.queryString              = queryString;
        this.filterCriteria           = filterCriteria;
        this.entityTypes              = entityTypes;
        this.classificationTypeNames  = classificationTypeNames;
        this.indexFieldNameCache      = indexFieldNameCache;
        this.excludeDeletedEntities   = excludeDeletedEntities;
        this.includeSubTypes          = includeSubTypes;
        this.offset                   = offset;
        this.limit                    = limit;
    }

    public String getQueryString() {
        return queryString;
    }

    public FilterCriteria getFilterCriteria() {
        return filterCriteria;
    }

    public Set<AtlasEntityType> getEntityTypes() {
        return entityTypes;
    }

    public Set<String> getClassificationTypeNames() {
        return classificationTypeNames;
    }

    public Map<String, String> getIndexFieldNameCache() {
        return indexFieldNameCache;
    }

    public boolean isExcludeDeletedEntities() {
        return excludeDeletedEntities;
    }

    public boolean isIncludeSubTypes() {
        return includeSubTypes;
    }

    public int getOffset() {
        return offset;
    }

    public int getLimit() {
        return limit;
    }
}
