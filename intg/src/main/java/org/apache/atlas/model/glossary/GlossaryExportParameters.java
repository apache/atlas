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
import java.util.List;

@AtlasJSON
public class GlossaryExportParameters implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum ExportFormat {
        CSV, XLSX
    }

    public enum ExportMode {
        AUDIT, IMPORT_COMPATIBLE
    }

    public enum RecordType {
        ALL, TERM, CATEGORY
    }

    private ExportFormat format;
    private ExportMode   mode;
    private RecordType   recordType;
    private String       glossaryGuid;
    private List<String> glossaryGuids;
    private String       statusContains;
    private String       classificationContains;
    private String       searchQuery;
    private boolean      excludeDeleted = true;
    private String       sortBy          = "name";
    private SortOrder    sortOrder       = SortOrder.ASCENDING;
    /** Optional shared timestamp token so CSV and XLSX exports correlate (yyyy-MM-dd_HH-mm-ss). */
    private String       exportTimestamp;

    public ExportFormat getFormat() {
        return format;
    }

    public void setFormat(ExportFormat format) {
        this.format = format;
    }

    public ExportMode getMode() {
        return mode != null ? mode : ExportMode.AUDIT;
    }

    public void setMode(ExportMode mode) {
        this.mode = mode;
    }

    public RecordType getRecordType() {
        return recordType != null ? recordType : RecordType.ALL;
    }

    public void setRecordType(RecordType recordType) {
        this.recordType = recordType;
    }

    public String getGlossaryGuid() {
        return glossaryGuid;
    }

    public void setGlossaryGuid(String glossaryGuid) {
        this.glossaryGuid = glossaryGuid;
    }

    public List<String> getGlossaryGuids() {
        return glossaryGuids;
    }

    public void setGlossaryGuids(List<String> glossaryGuids) {
        this.glossaryGuids = glossaryGuids;
    }

    public String getStatusContains() {
        return statusContains;
    }

    public void setStatusContains(String statusContains) {
        this.statusContains = statusContains;
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

    public String getSortBy() {
        return sortBy;
    }

    public void setSortBy(String sortBy) {
        this.sortBy = sortBy;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public void setSortOrder(SortOrder sortOrder) {
        this.sortOrder = sortOrder;
    }

    public String getExportTimestamp() {
        return exportTimestamp;
    }

    public void setExportTimestamp(String exportTimestamp) {
        this.exportTimestamp = exportTimestamp;
    }
}
