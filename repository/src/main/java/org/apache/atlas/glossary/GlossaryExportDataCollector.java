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
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GlossaryExportDataCollector {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryExportDataCollector.class);

    private final GlossaryService glossaryService;

    public GlossaryExportDataCollector(GlossaryService glossaryService) {
        this.glossaryService = glossaryService;
    }

    public List<GlossaryExportRow> collect(GlossaryExportParameters parameters) throws AtlasBaseException {
        List<GlossaryExportRow> ret           = new ArrayList<>();
        List<String>          glossaryGuids = resolveGlossaryGuids(parameters);

        LOG.debug("Collecting glossary export rows for {} glossaries", glossaryGuids.size());

        for (String glossaryGuid : glossaryGuids) {
            AtlasGlossary.AtlasGlossaryExtInfo glossaryExt = glossaryService.getDetailedGlossary(glossaryGuid);

            if (glossaryExt == null) {
                continue;
            }

            String glossaryName = glossaryExt.getName();

            if (MapUtils.isNotEmpty(glossaryExt.getTermInfo())) {
                for (AtlasGlossaryTerm term : glossaryExt.getTermInfo().values()) {
                    ret.add(buildTermRow(term, glossaryGuid, glossaryName));
                }
            }

            if (MapUtils.isNotEmpty(glossaryExt.getCategoryInfo())) {
                for (AtlasGlossaryCategory category : glossaryExt.getCategoryInfo().values()) {
                    ret.add(buildCategoryRow(category, glossaryGuid, glossaryName));
                }
            }
        }

        LOG.debug("Collected {} glossary export rows before filtering", ret.size());

        return ret;
    }

    private List<String> resolveGlossaryGuids(GlossaryExportParameters parameters) throws AtlasBaseException {
        if (StringUtils.isNotEmpty(parameters.getGlossaryGuid())) {
            return Collections.singletonList(parameters.getGlossaryGuid());
        }

        if (CollectionUtils.isNotEmpty(parameters.getGlossaryGuids())) {
            return parameters.getGlossaryGuids();
        }

        List<AtlasGlossary> glossaries = glossaryService.getGlossaries(-1, 0, SortOrder.ASCENDING);
        List<String>        ret        = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(glossaries)) {
            for (AtlasGlossary glossary : glossaries) {
                ret.add(glossary.getGuid());
            }
        }

        return ret;
    }

    private GlossaryExportRow buildTermRow(AtlasGlossaryTerm term, String glossaryGuid, String glossaryName) {
        GlossaryExportRow row = new GlossaryExportRow();

        row.setRecordType(GlossaryExportRow.RecordTypeKind.TERM);
        row.setGuid(term.getGuid());
        row.setGlossaryGuid(glossaryGuid);
        row.setGlossaryName(glossaryName);
        row.setName(term.getName());
        row.setShortDescription(term.getShortDescription());
        row.setLongDescription(term.getLongDescription());
        row.setQualifiedName(term.getQualifiedName());
        row.setStatus(GlossaryExportUtils.deriveTermStatus(term));
        row.setClassifications(GlossaryExportUtils.formatClassifications(term.getClassifications()));
        row.setCustomAttributes(GlossaryExportUtils.formatAdditionalAttributes(term.getAdditionalAttributes()));
        row.setRelatedCategoriesOrParent(GlossaryExportUtils.formatTermCategories(term.getCategories()));
        row.setEntityStatus(AtlasEntity.Status.ACTIVE.name());
        row.setExamples(GlossaryExportUtils.formatExamples(term.getExamples()));
        row.setAbbreviation(term.getAbbreviation());
        row.setUsage(term.getUsage());
        row.setTranslationTerms(GlossaryExportUtils.formatRelatedTerms(term.getTranslationTerms(), glossaryName));
        row.setValidValuesFor(GlossaryExportUtils.formatRelatedTerms(term.getValidValuesFor(), glossaryName));
        row.setSynonyms(GlossaryExportUtils.formatRelatedTerms(term.getSynonyms(), glossaryName));
        row.setReplacedBy(GlossaryExportUtils.formatRelatedTerms(term.getReplacedBy(), glossaryName));
        row.setValidValues(GlossaryExportUtils.formatRelatedTerms(term.getValidValues(), glossaryName));
        row.setReplacementTerms(GlossaryExportUtils.formatRelatedTerms(term.getReplacementTerms(), glossaryName));
        row.setSeeAlso(GlossaryExportUtils.formatRelatedTerms(term.getSeeAlso(), glossaryName));
        row.setTranslatedTerms(GlossaryExportUtils.formatRelatedTerms(term.getTranslatedTerms(), glossaryName));
        row.setIsA(GlossaryExportUtils.formatRelatedTerms(term.getIsA(), glossaryName));
        row.setAntonyms(GlossaryExportUtils.formatRelatedTerms(term.getAntonyms(), glossaryName));
        row.setClassifies(GlossaryExportUtils.formatRelatedTerms(term.getClassifies(), glossaryName));
        row.setPreferredToTerms(GlossaryExportUtils.formatRelatedTerms(term.getPreferredToTerms(), glossaryName));
        row.setPreferredTerms(GlossaryExportUtils.formatRelatedTerms(term.getPreferredTerms(), glossaryName));

        return row;
    }

    private GlossaryExportRow buildCategoryRow(AtlasGlossaryCategory category, String glossaryGuid, String glossaryName) {
        GlossaryExportRow row = new GlossaryExportRow();

        row.setRecordType(GlossaryExportRow.RecordTypeKind.CATEGORY);
        row.setGuid(category.getGuid());
        row.setGlossaryGuid(glossaryGuid);
        row.setGlossaryName(glossaryName);
        row.setName(category.getName());
        row.setShortDescription(category.getShortDescription());
        row.setLongDescription(category.getLongDescription());
        row.setQualifiedName(category.getQualifiedName());
        row.setStatus(GlossaryExportUtils.deriveCategoryStatus(category));
        row.setClassifications(GlossaryExportUtils.formatClassifications(category.getClassifications()));
        row.setCustomAttributes(GlossaryExportUtils.formatAdditionalAttributes(category.getAdditionalAttributes()));
        row.setRelatedCategoriesOrParent(GlossaryExportUtils.formatCategoryParent(category));
        row.setEntityStatus(AtlasEntity.Status.ACTIVE.name());

        return row;
    }
}
