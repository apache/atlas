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

import org.apache.atlas.model.glossary.relations.AtlasRelatedCategoryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.instance.AtlasClassification;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestAtlasGlossary {
    private AtlasGlossary glossary;

    @BeforeMethod
    public void setUp() {
        glossary = new AtlasGlossary();
    }

    @Test
    public void testDefaultConstructor() {
        AtlasGlossary newGlossary = new AtlasGlossary();

        assertNull(newGlossary.getLanguage());
        assertNull(newGlossary.getUsage());
        assertNull(newGlossary.getTerms());
        assertNull(newGlossary.getCategories());
    }

    @Test
    public void testCopyConstructor() {
        AtlasGlossary original = new AtlasGlossary();
        original.setGuid("test-guid");
        original.setName("Test Glossary");
        original.setQualifiedName("test.glossary");
        original.setShortDescription("Short desc");
        original.setLongDescription("Long description");
        original.setLanguage("English");
        original.setUsage("Test usage");

        Set<AtlasRelatedTermHeader> terms = new HashSet<>();
        AtlasRelatedTermHeader termHeader = new AtlasRelatedTermHeader();
        termHeader.setTermGuid("term-guid");
        terms.add(termHeader);
        original.setTerms(terms);

        Set<AtlasRelatedCategoryHeader> categories = new HashSet<>();
        AtlasRelatedCategoryHeader categoryHeader = new AtlasRelatedCategoryHeader();
        categoryHeader.setCategoryGuid("category-guid");
        categories.add(categoryHeader);
        original.setCategories(categories);

        AtlasGlossary copy = new AtlasGlossary(original);

        assertEquals(copy.getGuid(), original.getGuid());
        assertEquals(copy.getName(), original.getName());
        assertEquals(copy.getQualifiedName(), original.getQualifiedName());
        assertEquals(copy.getShortDescription(), original.getShortDescription());
        assertEquals(copy.getLongDescription(), original.getLongDescription());
        assertEquals(copy.getLanguage(), original.getLanguage());
        assertEquals(copy.getUsage(), original.getUsage());
        assertEquals(copy.getTerms(), original.getTerms());
        assertEquals(copy.getCategories(), original.getCategories());
    }

    @Test
    public void testLanguageGetterSetter() {
        String language = "English";

        glossary.setLanguage(language);
        assertEquals(glossary.getLanguage(), language);

        glossary.setLanguage(null);
        assertNull(glossary.getLanguage());
    }

    @Test
    public void testUsageGetterSetter() {
        String usage = "Test usage description";

        glossary.setUsage(usage);
        assertEquals(glossary.getUsage(), usage);

        glossary.setUsage(null);
        assertNull(glossary.getUsage());
    }

    @Test
    public void testTermsGetterSetter() {
        Set<AtlasRelatedTermHeader> terms = new HashSet<>();
        AtlasRelatedTermHeader term1 = new AtlasRelatedTermHeader();
        term1.setTermGuid("term1-guid");
        terms.add(term1);

        glossary.setTerms(terms);
        assertEquals(glossary.getTerms(), terms);

        glossary.setTerms(null);
        assertNull(glossary.getTerms());
    }

    @Test
    public void testCategoriesGetterSetter() {
        Set<AtlasRelatedCategoryHeader> categories = new HashSet<>();
        AtlasRelatedCategoryHeader category1 = new AtlasRelatedCategoryHeader();
        category1.setCategoryGuid("category1-guid");
        categories.add(category1);

        glossary.setCategories(categories);
        assertEquals(glossary.getCategories(), categories);

        glossary.setCategories(null);
        assertNull(glossary.getCategories());
    }

    @Test
    public void testSetAttributeValidAttributes() {
        glossary.setAttribute("name", "Test Name");
        assertEquals(glossary.getName(), "Test Name");

        glossary.setAttribute("shortDescription", "Short Desc");
        assertEquals(glossary.getShortDescription(), "Short Desc");

        glossary.setAttribute("longDescription", "Long Desc");
        assertEquals(glossary.getLongDescription(), "Long Desc");

        glossary.setAttribute("language", "Spanish");
        assertEquals(glossary.getLanguage(), "Spanish");

        glossary.setAttribute("usage", "Usage info");
        assertEquals(glossary.getUsage(), "Usage info");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSetAttributeInvalidAttribute() {
        glossary.setAttribute("invalidAttribute", "value");
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testSetAttributeNullAttributeName() {
        glossary.setAttribute(null, "value");
    }

    @Test
    public void testAddTerm() {
        AtlasRelatedTermHeader term = new AtlasRelatedTermHeader();
        term.setTermGuid("term-guid");

        glossary.addTerm(term);

        assertNotNull(glossary.getTerms());
        assertTrue(glossary.getTerms().contains(term));
        assertEquals(glossary.getTerms().size(), 1);
    }

    @Test
    public void testAddTermToExistingTerms() {
        Set<AtlasRelatedTermHeader> existingTerms = new HashSet<>();
        AtlasRelatedTermHeader existingTerm = new AtlasRelatedTermHeader();
        existingTerm.setTermGuid("existing-guid");
        existingTerms.add(existingTerm);

        glossary.setTerms(existingTerms);

        AtlasRelatedTermHeader newTerm = new AtlasRelatedTermHeader();
        newTerm.setTermGuid("new-guid");

        glossary.addTerm(newTerm);

        assertEquals(glossary.getTerms().size(), 2);
        assertTrue(glossary.getTerms().contains(existingTerm));
        assertTrue(glossary.getTerms().contains(newTerm));
    }

    @Test
    public void testAddCategory() {
        AtlasRelatedCategoryHeader category = new AtlasRelatedCategoryHeader();
        category.setCategoryGuid("category-guid");

        glossary.addCategory(category);

        assertNotNull(glossary.getCategories());
        assertTrue(glossary.getCategories().contains(category));
        assertEquals(glossary.getCategories().size(), 1);
    }

    @Test
    public void testAddCategoryToExistingCategories() {
        Set<AtlasRelatedCategoryHeader> existingCategories = new HashSet<>();
        AtlasRelatedCategoryHeader existingCategory = new AtlasRelatedCategoryHeader();
        existingCategory.setCategoryGuid("existing-guid");
        existingCategories.add(existingCategory);

        glossary.setCategories(existingCategories);

        AtlasRelatedCategoryHeader newCategory = new AtlasRelatedCategoryHeader();
        newCategory.setCategoryGuid("new-guid");

        glossary.addCategory(newCategory);

        assertEquals(glossary.getCategories().size(), 2);
        assertTrue(glossary.getCategories().contains(existingCategory));
        assertTrue(glossary.getCategories().contains(newCategory));
    }

    @Test
    public void testRemoveTerm() {
        AtlasRelatedTermHeader term = new AtlasRelatedTermHeader();
        term.setTermGuid("term-guid");

        Set<AtlasRelatedTermHeader> terms = new HashSet<>();
        terms.add(term);
        glossary.setTerms(terms);

        glossary.removeTerm(term);

        assertFalse(glossary.getTerms().contains(term));
        assertEquals(glossary.getTerms().size(), 0);
    }

    @Test
    public void testRemoveTermFromEmptySet() {
        AtlasRelatedTermHeader term = new AtlasRelatedTermHeader();
        term.setTermGuid("term-guid");

        glossary.removeTerm(term);

        assertNull(glossary.getTerms());
    }

    @Test
    public void testRemoveCategory() {
        AtlasRelatedCategoryHeader category = new AtlasRelatedCategoryHeader();
        category.setCategoryGuid("category-guid");

        Set<AtlasRelatedCategoryHeader> categories = new HashSet<>();
        categories.add(category);
        glossary.setCategories(categories);

        glossary.removeCategory(category);

        assertFalse(glossary.getCategories().contains(category));
        assertEquals(glossary.getCategories().size(), 0);
    }

    @Test
    public void testRemoveCategoryFromEmptySet() {
        AtlasRelatedCategoryHeader category = new AtlasRelatedCategoryHeader();
        category.setCategoryGuid("category-guid");

        glossary.removeCategory(category);

        assertNull(glossary.getCategories());
    }

    @Test
    public void testEqualsWithSameObject() {
        assertTrue(glossary.equals(glossary));
    }

    @Test
    public void testEqualsWithNull() {
        assertFalse(glossary.equals(null));
    }

    @Test
    public void testEqualsWithDifferentClass() {
        assertFalse(glossary.equals("not a glossary"));
    }

    @Test
    public void testEqualsWithDifferentLanguage() {
        AtlasGlossary glossary1 = new AtlasGlossary();
        AtlasGlossary glossary2 = new AtlasGlossary();

        glossary1.setLanguage("English");
        glossary2.setLanguage("Spanish");

        assertFalse(glossary1.equals(glossary2));
    }

    @Test
    public void testHashCodeConsistency() {
        glossary.setLanguage("English");
        glossary.setUsage("Usage");

        int hashCode1 = glossary.hashCode();
        int hashCode2 = glossary.hashCode();

        assertEquals(hashCode1, hashCode2);
    }

    @Test
    public void testToString() {
        glossary.setLanguage("English");
        glossary.setUsage("Test usage");

        String toString = glossary.toString();

        assertNotNull(toString);
        assertTrue(toString.contains("English"));
        assertTrue(toString.contains("Test usage"));
    }

    @Test
    public void testInheritedMethods() {
        // Test inherited methods from AtlasGlossaryBaseObject
        glossary.setName("Test Glossary");
        assertEquals(glossary.getName(), "Test Glossary");

        glossary.setQualifiedName("test.glossary");
        assertEquals(glossary.getQualifiedName(), "test.glossary");

        glossary.setShortDescription("Short");
        assertEquals(glossary.getShortDescription(), "Short");

        glossary.setLongDescription("Long");
        assertEquals(glossary.getLongDescription(), "Long");
    }

    @Test
    public void testClassificationsHandling() {
        AtlasClassification classification = new AtlasClassification();
        classification.setTypeName("TestClassification");

        glossary.addClassification(classification);

        assertNotNull(glossary.getClassifications());
        assertTrue(glossary.getClassifications().contains(classification));
    }

    @Test
    public void testAtlasGlossaryExtInfoDefaultConstructor() {
        AtlasGlossary.AtlasGlossaryExtInfo extInfo = new AtlasGlossary.AtlasGlossaryExtInfo();

        assertNull(extInfo.getTermInfo());
        assertNull(extInfo.getCategoryInfo());
    }

    @Test
    public void testAtlasGlossaryExtInfoCopyConstructor() {
        AtlasGlossary originalGlossary = new AtlasGlossary();
        originalGlossary.setName("Original");
        originalGlossary.setLanguage("English");

        AtlasGlossary.AtlasGlossaryExtInfo extInfo = new AtlasGlossary.AtlasGlossaryExtInfo(originalGlossary);

        assertEquals(extInfo.getName(), "Original");
        assertEquals(extInfo.getLanguage(), "English");
    }

    @Test
    public void testAtlasGlossaryExtInfoTermInfo() {
        AtlasGlossary.AtlasGlossaryExtInfo extInfo = new AtlasGlossary.AtlasGlossaryExtInfo();

        Map<String, AtlasGlossaryTerm> termInfo = new HashMap<>();
        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setGuid("term-guid");
        termInfo.put("term-guid", term);

        extInfo.setTermInfo(termInfo);
        assertEquals(extInfo.getTermInfo(), termInfo);
    }

    @Test
    public void testAtlasGlossaryExtInfoAddTermInfo() {
        AtlasGlossary.AtlasGlossaryExtInfo extInfo = new AtlasGlossary.AtlasGlossaryExtInfo();

        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setGuid("term-guid");

        extInfo.addTermInfo(term);

        assertNotNull(extInfo.getTermInfo());
        assertTrue(extInfo.getTermInfo().containsKey("term-guid"));
        assertEquals(extInfo.getTermInfo().get("term-guid"), term);
    }

    @Test
    public void testAtlasGlossaryExtInfoCategoryInfo() {
        AtlasGlossary.AtlasGlossaryExtInfo extInfo = new AtlasGlossary.AtlasGlossaryExtInfo();

        Map<String, AtlasGlossaryCategory> categoryInfo = new HashMap<>();
        AtlasGlossaryCategory category = new AtlasGlossaryCategory();
        category.setGuid("category-guid");
        categoryInfo.put("category-guid", category);

        extInfo.setCategoryInfo(categoryInfo);
        assertEquals(extInfo.getCategoryInfo(), categoryInfo);
    }

    @Test
    public void testAtlasGlossaryExtInfoAddCategoryInfo() {
        AtlasGlossary.AtlasGlossaryExtInfo extInfo = new AtlasGlossary.AtlasGlossaryExtInfo();

        AtlasGlossaryCategory category = new AtlasGlossaryCategory();
        category.setGuid("category-guid");

        extInfo.addCategoryInfo(category);

        assertNotNull(extInfo.getCategoryInfo());
        assertTrue(extInfo.getCategoryInfo().containsKey("category-guid"));
        assertEquals(extInfo.getCategoryInfo().get("category-guid"), category);
    }

    @Test
    public void testAtlasGlossaryExtInfoEquals() {
        AtlasGlossary.AtlasGlossaryExtInfo extInfo1 = new AtlasGlossary.AtlasGlossaryExtInfo();
        AtlasGlossary.AtlasGlossaryExtInfo extInfo2 = new AtlasGlossary.AtlasGlossaryExtInfo();

        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setGuid("term-guid");

        extInfo1.addTermInfo(term);
        assertFalse(extInfo1.equals(extInfo2));
    }

    @Test
    public void testAtlasGlossaryExtInfoToString() {
        AtlasGlossary.AtlasGlossaryExtInfo extInfo = new AtlasGlossary.AtlasGlossaryExtInfo();

        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setGuid("term-guid");
        extInfo.addTermInfo(term);

        StringBuilder sb = new StringBuilder();
        extInfo.toString(sb);

        String result = sb.toString();
        assertTrue(result.contains("termInfo"));
        assertTrue(result.contains("categoryInfo"));
    }
}
