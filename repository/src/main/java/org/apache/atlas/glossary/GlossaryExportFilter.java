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
import org.apache.atlas.model.glossary.GlossaryExportRow;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class GlossaryExportFilter {
    public List<GlossaryExportRow> apply(List<GlossaryExportRow> rows, GlossaryExportParameters parameters) {
        if (rows == null) {
            return new ArrayList<>();
        }

        return rows.stream()
                .filter(row -> matchesRecordType(row, parameters.getRecordType()))
                .filter(row -> matchesGlossary(row, parameters))
                .filter(row -> GlossaryExportUtils.containsIgnoreCase(row.getStatus(), parameters.getStatusContains()))
                .filter(row -> GlossaryExportUtils.containsIgnoreCase(row.getClassifications(), parameters.getClassificationContains()))
                .filter(row -> GlossaryExportUtils.matchesSearch(row, parameters.getSearchQuery(), isGlossaryScoped(parameters)))
                .filter(row -> matchesDeleted(row, parameters.getExcludeDeleted()))
                .collect(Collectors.toList());
    }

    private boolean matchesRecordType(GlossaryExportRow row, GlossaryExportParameters.RecordType recordType) {
        if (recordType == null || recordType == GlossaryExportParameters.RecordType.ALL) {
            return true;
        }

        if (recordType == GlossaryExportParameters.RecordType.TERM) {
            return row.getRecordType() == GlossaryExportRow.RecordTypeKind.TERM;
        }

        return row.getRecordType() == GlossaryExportRow.RecordTypeKind.CATEGORY;
    }

    private boolean matchesGlossary(GlossaryExportRow row, GlossaryExportParameters parameters) {
        if (StringUtils.isNotEmpty(parameters.getGlossaryGuid())) {
            return StringUtils.equals(row.getGlossaryGuid(), parameters.getGlossaryGuid());
        }

        if (parameters.getGlossaryGuids() != null && !parameters.getGlossaryGuids().isEmpty()) {
            return parameters.getGlossaryGuids().contains(row.getGlossaryGuid());
        }

        return true;
    }

    private boolean isGlossaryScoped(GlossaryExportParameters parameters) {
        return StringUtils.isNotEmpty(parameters.getGlossaryGuid())
                || (parameters.getGlossaryGuids() != null && !parameters.getGlossaryGuids().isEmpty());
    }

    private boolean matchesDeleted(GlossaryExportRow row, boolean excludeDeleted) {
        if (!excludeDeleted) {
            return true;
        }

        return !StringUtils.equalsIgnoreCase(row.getEntityStatus(), AtlasEntity.Status.DELETED.name());
    }
}
