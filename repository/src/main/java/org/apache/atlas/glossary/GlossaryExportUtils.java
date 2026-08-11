/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.
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
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.GlossaryExportRow;
import org.apache.atlas.model.glossary.enums.AtlasTermRelationshipStatus;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.glossary.relations.AtlasTermCategorizationHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.util.FileUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class GlossaryExportUtils {
    public static final String DEFAULT_STATUS = "ACTIVE";
    public static final String STATUS_ATTR    = "status";

    private GlossaryExportUtils() {
    }

    public static String deriveTermStatus(AtlasGlossaryTerm term) {
        if (term == null) {
            return DEFAULT_STATUS;
        }

        if (CollectionUtils.isNotEmpty(term.getCategories())) {
            for (AtlasTermCategorizationHeader category : term.getCategories()) {
                if (category.getStatus() != null) {
                    return category.getStatus().name();
                }
            }
        }

        Map<String, Object> additionalAttributes = term.getAdditionalAttributes();

        if (MapUtils.isNotEmpty(additionalAttributes) && additionalAttributes.get(STATUS_ATTR) != null) {
            return String.valueOf(additionalAttributes.get(STATUS_ATTR));
        }

        return DEFAULT_STATUS;
    }

    public static String deriveCategoryStatus(AtlasGlossaryCategory category) {
        if (category == null) {
            return DEFAULT_STATUS;
        }

        Map<String, Object> additionalAttributes = category.getAdditionalAttributes();

        if (MapUtils.isNotEmpty(additionalAttributes) && additionalAttributes.get(STATUS_ATTR) != null) {
            return String.valueOf(additionalAttributes.get(STATUS_ATTR));
        }

        return DEFAULT_STATUS;
    }

    public static String formatClassifications(List<AtlasClassification> classifications) {
        if (CollectionUtils.isEmpty(classifications)) {
            return "";
        }

        return classifications.stream()
                .map(AtlasClassification::getTypeName)
                .filter(StringUtils::isNotEmpty)
                .sorted()
                .collect(Collectors.joining(", "));
    }

    public static String formatAdditionalAttributes(Map<String, Object> additionalAttributes) {
        if (MapUtils.isEmpty(additionalAttributes)) {
            return "";
        }

        List<String> parts = new ArrayList<>();

        for (Map.Entry<String, Object> entry : additionalAttributes.entrySet()) {
            if (entry.getValue() != null) {
                parts.add(entry.getKey() + "=" + entry.getValue());
            }
        }

        return String.join("; ", parts);
    }

    public static String formatTermCategories(Set<AtlasTermCategorizationHeader> categories) {
        if (CollectionUtils.isEmpty(categories)) {
            return "";
        }

        return categories.stream()
                .map(AtlasTermCategorizationHeader::getDisplayText)
                .filter(StringUtils::isNotEmpty)
                .sorted()
                .collect(Collectors.joining(", "));
    }

    public static String formatCategoryParent(AtlasGlossaryCategory category) {
        if (category == null || category.getParentCategory() == null) {
            return "";
        }

        return StringUtils.defaultString(category.getParentCategory().getDisplayText());
    }

    public static String formatExamples(List<String> examples) {
        if (CollectionUtils.isEmpty(examples)) {
            return "";
        }

        return String.join("|", examples);
    }

    public static String formatRelatedTerms(Set<AtlasRelatedTermHeader> relatedTerms, String glossaryName) {
        if (CollectionUtils.isEmpty(relatedTerms)) {
            return "";
        }

        List<String> parts = new ArrayList<>();

        for (AtlasRelatedTermHeader header : relatedTerms) {
            String termName = StringUtils.defaultIfEmpty(header.getDisplayText(), header.getTermGuid());
            String status   = header.getStatus() != null ? header.getStatus().name() : AtlasTermRelationshipStatus.ACTIVE.name();

            parts.add(termName + FileUtils.COLON_CHARACTER + status);
        }

        return String.join("|", parts);
    }

    public static boolean containsIgnoreCase(String value, String search) {
        if (StringUtils.isEmpty(search)) {
            return true;
        }

        return StringUtils.containsIgnoreCase(StringUtils.defaultString(value), search);
    }

    public static boolean matchesSearch(GlossaryExportRow row, String searchQuery) {
        return matchesSearch(row, searchQuery, false);
    }

    public static boolean matchesSearch(GlossaryExportRow row, String searchQuery, boolean glossaryScoped) {
        if (StringUtils.isEmpty(searchQuery)) {
            return true;
        }

        if (containsIgnoreCase(row.getName(), searchQuery)
                || containsIgnoreCase(row.getShortDescription(), searchQuery)
                || containsIgnoreCase(row.getLongDescription(), searchQuery)
                || containsIgnoreCase(row.getStatus(), searchQuery)
                || containsIgnoreCase(row.getClassifications(), searchQuery)
                || containsIgnoreCase(row.getCustomAttributes(), searchQuery)
                || containsIgnoreCase(row.getRelatedCategoriesOrParent(), searchQuery)) {
            return true;
        }

        return !glossaryScoped && containsIgnoreCase(row.getGlossaryName(), searchQuery);
    }

    public static void sortRows(List<GlossaryExportRow> rows, String sortBy, SortOrder sortOrder) {
        if (CollectionUtils.isEmpty(rows)) {
            return;
        }

        Comparator<GlossaryExportRow> comparator = comparatorForSortBy(sortBy);

        if (sortOrder == SortOrder.DESCENDING) {
            comparator = comparator.reversed();
        }

        rows.sort(comparator);
    }

    private static Comparator<GlossaryExportRow> comparatorForSortBy(String sortBy) {
        if (StringUtils.equalsIgnoreCase(sortBy, "glossaryName")) {
            return Comparator.<GlossaryExportRow, String>comparing(
                    row -> StringUtils.defaultString(row.getGlossaryName()), String.CASE_INSENSITIVE_ORDER)
                    .thenComparing(row -> StringUtils.defaultString(row.getName()), String.CASE_INSENSITIVE_ORDER);
        }

        return Comparator.comparing(row -> StringUtils.defaultString(row.getName()), String.CASE_INSENSITIVE_ORDER);
    }
}
