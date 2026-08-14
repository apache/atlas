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
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class GlossaryExportFilterTest {
    private final GlossaryExportFilter filter = new GlossaryExportFilter();

    @Test
    public void testFilterByRecordTypeTerm() {
        GlossaryExportParameters parameters = new GlossaryExportParameters();
        parameters.setRecordType(GlossaryExportParameters.RecordType.TERM);

        List<GlossaryExportRow> rows = filter.apply(sampleRows(), parameters);

        assertEquals(rows.size(), 1);
        assertEquals(rows.get(0).getName(), "BankBranch");
    }

    @Test
    public void testFilterByGlossaryGuid() {
        GlossaryExportParameters parameters = new GlossaryExportParameters();
        parameters.setGlossaryGuid("glossary-guid-1");

        List<GlossaryExportRow> rows = filter.apply(sampleRows(), parameters);

        assertEquals(rows.size(), 1);
        assertEquals(rows.get(0).getGlossaryGuid(), "glossary-guid-1");
    }

    @Test
    public void testFilterByStatusContains() {
        GlossaryExportParameters parameters = new GlossaryExportParameters();
        parameters.setStatusContains("draft");

        List<GlossaryExportRow> rows = filter.apply(sampleRows(), parameters);

        assertEquals(rows.size(), 1);
        assertEquals(rows.get(0).getStatus(), "DRAFT");
    }

    @Test
    public void testFilterBySearchQuery() {
        GlossaryExportParameters parameters = new GlossaryExportParameters();
        parameters.setSearchQuery("capital");

        List<GlossaryExportRow> rows = filter.apply(sampleRows(), parameters);

        assertEquals(rows.size(), 1);
        assertEquals(rows.get(0).getName(), "CapitalTerm");
    }

    @Test
    public void testExcludeDeleted() {
        GlossaryExportParameters parameters = new GlossaryExportParameters();
        parameters.setExcludeDeleted(true);

        List<GlossaryExportRow> rows = filter.apply(sampleRows(), parameters);

        assertEquals(rows.size(), 2);
    }

    @Test
    public void testFilterByCombinedGlossaryGuidAndSearchQuery() {
        GlossaryExportParameters parameters = new GlossaryExportParameters();
        parameters.setGlossaryGuid("glossary-guid-1");
        parameters.setRecordType(GlossaryExportParameters.RecordType.TERM);
        parameters.setSearchQuery("Bank");

        GlossaryExportRow capitalTerm = new GlossaryExportRow();
        capitalTerm.setRecordType(GlossaryExportRow.RecordTypeKind.TERM);
        capitalTerm.setGuid("term-2");
        capitalTerm.setGlossaryGuid("glossary-guid-1");
        capitalTerm.setGlossaryName("testBankingGlossary");
        capitalTerm.setName("CapitalTerm075");
        capitalTerm.setStatus("DRAFT");
        capitalTerm.setEntityStatus(AtlasEntity.Status.ACTIVE.name());

        List<GlossaryExportRow> rows = filter.apply(Arrays.asList(sampleRows().get(0), capitalTerm), parameters);

        assertEquals(rows.size(), 1);
        assertEquals(rows.get(0).getName(), "BankBranch");
    }

    private List<GlossaryExportRow> sampleRows() {
        GlossaryExportRow term = new GlossaryExportRow();
        term.setRecordType(GlossaryExportRow.RecordTypeKind.TERM);
        term.setGuid("term-1");
        term.setGlossaryGuid("glossary-guid-1");
        term.setGlossaryName("testBankingGlossary");
        term.setName("BankBranch");
        term.setStatus("ACTIVE");
        term.setEntityStatus(AtlasEntity.Status.ACTIVE.name());

        GlossaryExportRow category = new GlossaryExportRow();
        category.setRecordType(GlossaryExportRow.RecordTypeKind.CATEGORY);
        category.setGuid("cat-1");
        category.setGlossaryGuid("glossary-guid-2");
        category.setGlossaryName("testFinanceGlossary");
        category.setName("CapitalTerm");
        category.setStatus("DRAFT");
        category.setEntityStatus(AtlasEntity.Status.ACTIVE.name());

        GlossaryExportRow deleted = new GlossaryExportRow();
        deleted.setRecordType(GlossaryExportRow.RecordTypeKind.TERM);
        deleted.setGuid("term-2");
        deleted.setGlossaryGuid("glossary-guid-1");
        deleted.setGlossaryName("testBankingGlossary");
        deleted.setName("DeletedTerm");
        deleted.setStatus("ACTIVE");
        deleted.setEntityStatus(AtlasEntity.Status.DELETED.name());

        return Arrays.asList(term, category, deleted);
    }
}
