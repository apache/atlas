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
package org.apache.atlas.glossary;

import org.apache.atlas.model.glossary.GlossaryExportParameters;
import org.apache.atlas.model.glossary.GlossarySearchParameters;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * Maps UI / REST request bodies to {@link GlossaryExportParameters}.
 * Supports Basic Search-style {@code exportParameters} wrapper
 * ({@link GlossarySearchParameters} fields sent directly to {@code /glossary/create_file}).
 */
public final class GlossaryExportParameterMapper {
    private GlossaryExportParameterMapper() {
    }

    public static GlossaryExportParameters fromCreateFileRequest(Map<String, Object> parameterMap) {
        if (parameterMap == null || parameterMap.isEmpty()) {
            return null;
        }

        Object exportParameters = parameterMap.get("exportParameters");

        if (exportParameters != null) {
            return AtlasJson.fromLinkedHashMap(exportParameters, GlossaryExportParameters.class);
        }

        if (isSearchStyleCreateFileRequest(parameterMap)) {
            GlossarySearchParameters searchParameters = AtlasJson.fromLinkedHashMap(parameterMap, GlossarySearchParameters.class);

            return fromSearchParameters(searchParameters, resolveFormat(parameterMap));
        }

        if (parameterMap.containsKey("format") || parameterMap.containsKey("mode") || parameterMap.containsKey("recordType")) {
            return AtlasJson.fromLinkedHashMap(parameterMap, GlossaryExportParameters.class);
        }

        GlossarySearchParameters searchParameters = AtlasJson.fromLinkedHashMap(parameterMap, GlossarySearchParameters.class);

        return fromSearchParameters(searchParameters, resolveFormat(parameterMap));
    }

    /**
     * Detect UI / search-style payloads so {@code format} does not force direct
     * {@link GlossaryExportParameters}
     */
    static boolean isSearchStyleCreateFileRequest(Map<String, Object> parameterMap) {
        return parameterMap.containsKey("glossaryType")
                || parameterMap.containsKey("glossary")
                || parameterMap.containsKey("limit")
                || parameterMap.containsKey("offset")
                || parameterMap.containsKey("status");
    }

    public static GlossaryExportParameters fromSearchParameters(GlossarySearchParameters searchParameters,
                                                                  GlossaryExportParameters.ExportFormat format) {
        GlossaryExportParameters exportParameters = new GlossaryExportParameters();

        exportParameters.setFormat(format != null ? format : GlossaryExportParameters.ExportFormat.CSV);
        exportParameters.setMode(GlossaryExportParameters.ExportMode.AUDIT);
        exportParameters.setExcludeDeleted(searchParameters != null && searchParameters.getExcludeDeleted());

        if (searchParameters == null) {
            return exportParameters;
        }

        exportParameters.setSearchQuery(searchParameters.getSearchQuery());
        exportParameters.setClassificationContains(searchParameters.getClassificationContains());
        exportParameters.setStatusContains(searchParameters.getStatus());
        exportParameters.setSortBy(searchParameters.getSortBy());
        exportParameters.setSortOrder(searchParameters.getSortOrder());

        if (searchParameters.getGlossaryType() == GlossarySearchParameters.GlossaryType.TERM) {
            exportParameters.setRecordType(GlossaryExportParameters.RecordType.TERM);
        } else if (searchParameters.getGlossaryType() == GlossarySearchParameters.GlossaryType.CATEGORY) {
            exportParameters.setRecordType(GlossaryExportParameters.RecordType.CATEGORY);
        } else {
            exportParameters.setRecordType(GlossaryExportParameters.RecordType.ALL);
        }

        if (searchParameters.getGlossary() != null) {
            if (StringUtils.isNotEmpty(searchParameters.getGlossary().getGuid())) {
                exportParameters.setGlossaryGuid(searchParameters.getGlossary().getGuid());
            }
        }

        return exportParameters;
    }

    private static GlossaryExportParameters.ExportFormat resolveFormat(Map<String, Object> parameterMap) {
        Object format = parameterMap.get("format");

        if (format == null) {
            return GlossaryExportParameters.ExportFormat.CSV;
        }

        try {
            return GlossaryExportParameters.ExportFormat.valueOf(String.valueOf(format).trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            return GlossaryExportParameters.ExportFormat.CSV;
        }
    }
}
