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

import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.enums.AtlasTermRelationshipStatus;
import org.apache.atlas.model.glossary.relations.AtlasTermCategorizationHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class GlossaryExportUtilsTest {
    @Test
    public void testDeriveTermStatusFromCategory() {
        AtlasGlossaryTerm term = new AtlasGlossaryTerm();

        AtlasTermCategorizationHeader header = new AtlasTermCategorizationHeader();
        header.setStatus(AtlasTermRelationshipStatus.DRAFT);

        term.setCategories(new HashSet<>(Collections.singletonList(header)));

        assertEquals(GlossaryExportUtils.deriveTermStatus(term), "DRAFT");
    }

    @Test
    public void testDeriveTermStatusFromAdditionalAttributes() {
        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("status", "Deprecated");
        term.setAdditionalAttributes(attrs);

        assertEquals(GlossaryExportUtils.deriveTermStatus(term), "Deprecated");
    }

    @Test
    public void testFormatClassifications() {
        AtlasClassification classification = new AtlasClassification();
        classification.setTypeName("PII");

        String result = GlossaryExportUtils.formatClassifications(Collections.singletonList(classification));
        assertEquals(result, "PII");
    }

    @Test
    public void testMatchesSearch() {
        org.apache.atlas.model.glossary.GlossaryExportRow row = new org.apache.atlas.model.glossary.GlossaryExportRow();
        row.setName("BankBranch");
        row.setGlossaryName("testBankingGlossary");

        assertTrue(GlossaryExportUtils.matchesSearch(row, "bank"));
    }

    @Test
    public void testMatchesSearchWhenGlossaryScopedSkipsGlossaryName() {
        org.apache.atlas.model.glossary.GlossaryExportRow row = new org.apache.atlas.model.glossary.GlossaryExportRow();
        row.setName("CapitalTerm075");
        row.setGlossaryName("testBankingGlossary");

        assertTrue(GlossaryExportUtils.matchesSearch(row, "bank"));
        assertTrue(!GlossaryExportUtils.matchesSearch(row, "bank", true));
    }
}
