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
import org.apache.atlas.model.glossary.GlossarySearchParameters;
import org.apache.atlas.model.glossary.GlossarySearchResult;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class GlossarySearchBuilderTest {
    private GlossaryService       mockGlossaryService;
    private GlossarySearchBuilder builder;

    @BeforeMethod
    public void setUp() throws AtlasBaseException {
        mockGlossaryService = Mockito.mock(GlossaryService.class);
        builder             = new GlossarySearchBuilder(mockGlossaryService);

        AtlasGlossary glossary = new AtlasGlossary();
        glossary.setGuid("glossary-guid-1");
        glossary.setName("BankGlossary");
        glossary.setShortDescription("Bank glossary");
        glossary.setLongDescription("Long description");

        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setGuid("term-guid-1");
        term.setName("BankBranch");
        term.setQualifiedName("BankBranch@BankGlossary");
        term.setShortDescription("Branch term");

        AtlasGlossaryCategory category = new AtlasGlossaryCategory();
        category.setGuid("category-guid-1");
        category.setName("BankCategory");
        category.setQualifiedName("BankCategory@BankGlossary");
        category.setShortDescription("Category desc");

        Map<String, AtlasGlossaryTerm> termInfo = new HashMap<>();
        termInfo.put(term.getGuid(), term);

        Map<String, AtlasGlossaryCategory> categoryInfo = new HashMap<>();
        categoryInfo.put(category.getGuid(), category);

        AtlasGlossary.AtlasGlossaryExtInfo extInfo = new AtlasGlossary.AtlasGlossaryExtInfo();
        extInfo.setGuid(glossary.getGuid());
        extInfo.setName(glossary.getName());
        extInfo.setShortDescription(glossary.getShortDescription());
        extInfo.setLongDescription(glossary.getLongDescription());
        extInfo.setTermInfo(termInfo);
        extInfo.setCategoryInfo(categoryInfo);

        when(mockGlossaryService.getGlossaries(-1, 0, SortOrder.ASCENDING)).thenReturn(Collections.singletonList(glossary));
        when(mockGlossaryService.getDetailedGlossary(anyString())).thenReturn(extInfo);
    }

    @Test
    public void testSearchReturnsNestedGlossaryStructure() throws AtlasBaseException {
        GlossarySearchParameters parameters = new GlossarySearchParameters();
        parameters.setLimit(25);
        parameters.setOffset(0);

        GlossarySearchResult result = builder.search(parameters);

        assertNotNull(result);
        assertEquals(result.getApproximateCount(), 2);
        assertEquals(result.getGlossary().size(), 1);
        assertEquals(result.getGlossary().get(0).getName(), "BankGlossary");
        assertEquals(result.getGlossary().get(0).getTerms().size(), 1);
        assertEquals(result.getGlossary().get(0).getCategories().size(), 1);
        assertEquals(result.getGlossary().get(0).getTerms().get(0).getName(), "BankBranch");
    }

    @Test
    public void testSearchFiltersByGlossaryTypeTerm() throws AtlasBaseException {
        GlossarySearchParameters parameters = new GlossarySearchParameters();
        parameters.setGlossaryType(GlossarySearchParameters.GlossaryType.TERM);

        GlossarySearchResult result = builder.search(parameters);

        assertEquals(result.getApproximateCount(), 1);
        assertTrue(result.getGlossary().get(0).getCategories().isEmpty());
        assertEquals(result.getGlossary().get(0).getTerms().size(), 1);
    }

    @Test
    public void testSearchPaginatesRows() throws AtlasBaseException {
        GlossarySearchParameters parameters = new GlossarySearchParameters();
        parameters.setLimit(1);
        parameters.setOffset(0);
        parameters.setSortBy("name");
        parameters.setSortOrder(SortOrder.ASCENDING);

        GlossarySearchResult result = builder.search(parameters);

        assertEquals(result.getApproximateCount(), 2);
        assertEquals(result.getGlossary().size(), 1);
        assertEquals(result.getGlossary().get(0).getTerms().size() + result.getGlossary().get(0).getCategories().size(), 1);
    }
}
