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
package org.apache.atlas.model.glossary;

import org.apache.atlas.SortOrder;
import org.apache.atlas.model.annotation.AtlasJSON;

import java.io.Serializable;

/**
 * Request parameters for glossary terms/categories search (display API).
 * Mirrors Basic Search {@code SearchParameters} pagination and filter pattern for the UI table.
 */
@AtlasJSON
public class GlossarySearchParameters implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum GlossaryType {
        ALL, TERM, CATEGORY
    }

    @AtlasJSON
    public static class GlossaryFilter implements Serializable {
        private static final long serialVersionUID = 1L;

        private String name;
        private String guid;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getGuid() {
            return guid;
        }

        public void setGuid(String guid) {
            this.guid = guid;
        }
    }

    private int           limit           = 25;
    private int           offset;
    private String        sortBy          = "name";
    private SortOrder     sortOrder       = SortOrder.ASCENDING;
    private GlossaryFilter glossary;
    private GlossaryType  glossaryType;
    private String        status;
    private String        classificationContains;
    private String        searchQuery;
    private boolean       excludeDeleted  = true;

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public String getSortBy() {
        return sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public SortOrder getSortOrder() {
        return sortOrder != null ? sortOrder : SortOrder.ASCENDING;
    }

    public void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    public GlossaryFilter getGlossary() {
        return glossary;
    }

    public void setGlossary(GlossaryFilter glossary) {
        this.glossary = glossary;
    }

    public GlossaryType getGlossaryType() {
        return glossaryType != null ? glossaryType : GlossaryType.ALL;
    }

    public void setGlossaryType(GlossaryType glossaryType) {
        this.glossaryType = glossaryType;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getClassificationContains() {
        return classificationContains;
    }

    public void setClassificationContains(String classificationContains) {
        this.classificationContains = classificationContains;
    }

    public String getSearchQuery() {
        return searchQuery;
    }

    public void setSearchQuery(String searchQuery) {
        this.searchQuery = searchQuery;
    }

    public boolean getExcludeDeleted() {
        return excludeDeleted;
    }

    public void setExcludeDeleted(boolean excludeDeleted) {
        this.excludeDeleted = excludeDeleted;
    }
}
