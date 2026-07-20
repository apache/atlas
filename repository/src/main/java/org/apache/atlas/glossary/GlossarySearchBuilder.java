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

import org.apache.atlas.SortOrder;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.GlossaryExportParameters;
import org.apache.atlas.model.glossary.GlossaryExportRow;
import org.apache.atlas.model.glossary.GlossarySearchParameters;
import org.apache.atlas.model.glossary.GlossarySearchResult;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Builds paginated, nested JSON for glossary terms-list display.
 * Reuses {@link GlossaryExportDataCollector} and {@link GlossaryExportFilter} so search filters match export filters.
 */
public class GlossarySearchBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(GlossarySearchBuilder.class);

    private final GlossaryService glossaryService;

    public GlossarySearchBuilder(GlossaryService glossaryService) {
        this.glossaryService = glossaryService;
    }

    public GlossarySearchResult search(GlossarySearchParameters parameters) throws AtlasBaseException {
        GlossaryExportParameters exportParameters = toExportParameters(parameters);
        GlossaryExportDataCollector collector       = new GlossaryExportDataCollector(glossaryService);
        GlossaryExportFilter        filter          = new GlossaryExportFilter();

        List<GlossaryExportRow> rows = filter.apply(collector.collect(exportParameters), exportParameters);
        rows = filterByGlossaryName(rows, parameters);
        sort(rows, parameters);

        long totalCount = rows.size();
        List<GlossaryExportRow> pageRows = paginate(rows, parameters);

        LOG.debug("Glossary search matched {} rows, returning page of {}", totalCount, pageRows.size());

        return buildResult(pageRows, totalCount, parameters);
    }

    private GlossaryExportParameters toExportParameters(GlossarySearchParameters parameters) {
        GlossaryExportParameters exportParameters = new GlossaryExportParameters();

        exportParameters.setRecordType(toRecordType(parameters.getGlossaryType()));
        exportParameters.setStatusContains(parameters.getStatus());
        exportParameters.setClassificationContains(parameters.getClassificationContains());
        exportParameters.setSearchQuery(parameters.getSearchQuery());
        exportParameters.setExcludeDeleted(parameters.getExcludeDeleted());

        if (parameters.getGlossary() != null && StringUtils.isNotEmpty(parameters.getGlossary().getGuid())) {
            exportParameters.setGlossaryGuid(parameters.getGlossary().getGuid());
        }

        return exportParameters;
    }

    private GlossaryExportParameters.RecordType toRecordType(GlossarySearchParameters.GlossaryType glossaryType) {
        if (glossaryType == null || glossaryType == GlossarySearchParameters.GlossaryType.ALL) {
            return GlossaryExportParameters.RecordType.ALL;
        }

        if (glossaryType == GlossarySearchParameters.GlossaryType.TERM) {
            return GlossaryExportParameters.RecordType.TERM;
        }

        return GlossaryExportParameters.RecordType.CATEGORY;
    }

    private List<GlossaryExportRow> filterByGlossaryName(List<GlossaryExportRow> rows, GlossarySearchParameters parameters) {
        if (parameters.getGlossary() == null || StringUtils.isEmpty(parameters.getGlossary().getName())) {
            return rows;
        }

        String glossaryName = parameters.getGlossary().getName().toLowerCase(Locale.ROOT);
        List<GlossaryExportRow> ret = new ArrayList<>();

        for (GlossaryExportRow row : rows) {
            if (row.getGlossaryName() != null && row.getGlossaryName().toLowerCase(Locale.ROOT).contains(glossaryName)) {
                ret.add(row);
            }
        }

        return ret;
    }

    private void sort(List<GlossaryExportRow> rows, GlossarySearchParameters parameters) {
        Comparator<GlossaryExportRow> comparator = resolveComparator(parameters.getSortBy());
        boolean descending = parameters.getSortOrder() == SortOrder.DESCENDING;

        if (descending) {
            comparator = comparator.reversed();
        }

        rows.sort(comparator);
    }

    private Comparator<GlossaryExportRow> resolveComparator(String sortBy) {
        if (StringUtils.isBlank(sortBy)) {
            return Comparator.comparing(row -> StringUtils.defaultString(row.getName()), String.CASE_INSENSITIVE_ORDER);
        }

        switch (sortBy.toLowerCase(Locale.ROOT)) {
            case "glossaryname":
            case "glossary":
                return Comparator.comparing(row -> StringUtils.defaultString(row.getGlossaryName()), String.CASE_INSENSITIVE_ORDER);
            case "status":
                return Comparator.comparing(row -> StringUtils.defaultString(row.getStatus()), String.CASE_INSENSITIVE_ORDER);
            case "qualifiedname":
                return Comparator.comparing(row -> StringUtils.defaultString(row.getQualifiedName()), String.CASE_INSENSITIVE_ORDER);
            case "recordtype":
            case "type":
            case "glossarytype":
                return Comparator.comparing(row -> row.getRecordType() != null ? row.getRecordType().name() : "", String.CASE_INSENSITIVE_ORDER);
            case "name":
            default:
                return Comparator.comparing(row -> StringUtils.defaultString(row.getName()), String.CASE_INSENSITIVE_ORDER);
        }
    }

    private List<GlossaryExportRow> paginate(List<GlossaryExportRow> rows, GlossarySearchParameters parameters) {
        int offset = Math.max(0, parameters.getOffset());
        int limit  = parameters.getLimit();

        if (offset >= rows.size()) {
            return Collections.emptyList();
        }

        if (limit <= 0) {
            return new ArrayList<>(rows.subList(offset, rows.size()));
        }

        int end = Math.min(offset + limit, rows.size());

        return new ArrayList<>(rows.subList(offset, end));
    }

    private GlossarySearchResult buildResult(List<GlossaryExportRow> pageRows, long totalCount, GlossarySearchParameters parameters)
            throws AtlasBaseException {
        GlossarySearchResult result = new GlossarySearchResult();
        result.setApproximateCount(totalCount);

        if (CollectionUtils.isEmpty(pageRows)) {
            result.setGlossary(Collections.emptyList());
            return result;
        }

        Map<String, GlossarySearchResult.GlossaryDetail> glossaryByGuid = new LinkedHashMap<>();
        Map<String, AtlasGlossary.AtlasGlossaryExtInfo> extByGuid     = new LinkedHashMap<>();

        boolean includeTerms      = parameters.getGlossaryType() != GlossarySearchParameters.GlossaryType.CATEGORY;
        boolean includeCategories = parameters.getGlossaryType() != GlossarySearchParameters.GlossaryType.TERM;

        for (GlossaryExportRow row : pageRows) {
            AtlasGlossary.AtlasGlossaryExtInfo ext = extByGuid.computeIfAbsent(row.getGlossaryGuid(), this::loadExtInfoSafely);

            if (ext == null) {
                continue;
            }

            GlossarySearchResult.GlossaryDetail glossaryDetail = glossaryByGuid.computeIfAbsent(
                    row.getGlossaryGuid(), guid -> buildGlossaryDetail(ext));

            if (row.getRecordType() == GlossaryExportRow.RecordTypeKind.TERM && includeTerms) {
                AtlasGlossaryTerm term = findTerm(ext, row.getGuid());

                if (term != null) {
                    glossaryDetail.getTerms().add(buildTermDetail(term));
                }
            } else if (row.getRecordType() == GlossaryExportRow.RecordTypeKind.CATEGORY && includeCategories) {
                AtlasGlossaryCategory category = findCategory(ext, row.getGuid());

                if (category != null) {
                    glossaryDetail.getCategories().add(buildCategoryDetail(category));
                }
            }
        }

        result.setGlossary(new ArrayList<>(glossaryByGuid.values()));
        return result;
    }

    private AtlasGlossary.AtlasGlossaryExtInfo loadExtInfoSafely(String glossaryGuid) {
        try {
            return glossaryService.getDetailedGlossary(glossaryGuid);
        } catch (AtlasBaseException e) {
            LOG.warn("Failed to load detailed glossary for guid {}", glossaryGuid, e);
            return null;
        }
    }

    private GlossarySearchResult.GlossaryDetail buildGlossaryDetail(AtlasGlossary.AtlasGlossaryExtInfo ext) {
        GlossarySearchResult.GlossaryDetail detail = new GlossarySearchResult.GlossaryDetail();

        detail.setName(ext.getName());
        detail.setGuid(ext.getGuid());
        detail.setShortDescription(ext.getShortDescription());
        detail.setLongDescription(ext.getLongDescription());
        detail.setStatus(GlossaryExportUtils.DEFAULT_STATUS);

        if (MapUtils.isNotEmpty(ext.getAdditionalAttributes()) && ext.getAdditionalAttributes().get(GlossaryExportUtils.STATUS_ATTR) != null) {
            detail.setStatus(String.valueOf(ext.getAdditionalAttributes().get(GlossaryExportUtils.STATUS_ATTR)));
        }

        return detail;
    }

    private AtlasGlossaryTerm findTerm(AtlasGlossary.AtlasGlossaryExtInfo ext, String guid) {
        if (MapUtils.isEmpty(ext.getTermInfo())) {
            return null;
        }

        AtlasGlossaryTerm term = ext.getTermInfo().get(guid);

        if (term != null) {
            return term;
        }

        for (AtlasGlossaryTerm candidate : ext.getTermInfo().values()) {
            if (StringUtils.equals(candidate.getGuid(), guid)) {
                return candidate;
            }
        }

        return null;
    }

    private AtlasGlossaryCategory findCategory(AtlasGlossary.AtlasGlossaryExtInfo ext, String guid) {
        if (MapUtils.isEmpty(ext.getCategoryInfo())) {
            return null;
        }

        AtlasGlossaryCategory category = ext.getCategoryInfo().get(guid);

        if (category != null) {
            return category;
        }

        for (AtlasGlossaryCategory candidate : ext.getCategoryInfo().values()) {
            if (StringUtils.equals(candidate.getGuid(), guid)) {
                return candidate;
            }
        }

        return null;
    }

    private GlossarySearchResult.GlossaryTermDetail buildTermDetail(AtlasGlossaryTerm term) {
        GlossarySearchResult.GlossaryTermDetail detail = new GlossarySearchResult.GlossaryTermDetail();

        detail.setName(term.getName());
        detail.setQualifiedName(term.getQualifiedName());
        detail.setGuid(term.getGuid());
        detail.setShortDescription(term.getShortDescription());
        detail.setLongDescription(term.getLongDescription());
        detail.setStatus(GlossaryExportUtils.deriveTermStatus(term));

        if (CollectionUtils.isNotEmpty(term.getClassifications())) {
            detail.setClassifications(new ArrayList<>(term.getClassifications()));
        }

        detail.setCustomAttributes(term.getAdditionalAttributes());

        return detail;
    }

    private GlossarySearchResult.GlossaryCategoryDetail buildCategoryDetail(AtlasGlossaryCategory category) {
        GlossarySearchResult.GlossaryCategoryDetail detail = new GlossarySearchResult.GlossaryCategoryDetail();

        detail.setName(category.getName());
        detail.setQualifiedName(category.getQualifiedName());
        detail.setGuid(category.getGuid());
        detail.setShortDescription(category.getShortDescription());
        detail.setLongDescription(category.getLongDescription());
        detail.setStatus(GlossaryExportUtils.deriveCategoryStatus(category));

        if (CollectionUtils.isNotEmpty(category.getClassifications())) {
            detail.setClassifications(new ArrayList<>(category.getClassifications()));
        }

        detail.setCustomAttributes(category.getAdditionalAttributes());

        return detail;
    }
}
