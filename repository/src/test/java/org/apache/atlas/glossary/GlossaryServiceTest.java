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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.SortOrder;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.enums.AtlasTermRelationshipStatus;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedCategoryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.repository.impexp.ZipFileResourceTestUtils;
import org.apache.atlas.repository.store.graph.AtlasEntityStore;
import org.apache.atlas.repository.store.graph.v1.AtlasEntityStream;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.utils.AtlasJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

@Guice(modules = TestModules.TestOnlyModule.class)
public class GlossaryServiceTest {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryServiceTest.class);

    @Inject
    private GlossaryService   glossaryService;
    @Inject
    private AtlasTypeDefStore typeDefStore;
    @Inject
    private AtlasTypeRegistry typeRegistry;
    @Inject
    private AtlasEntityStore entityStore;

    private AtlasGlossary     bankGlossary;
    private AtlasGlossaryTerm checkingAccount, savingsAccount, fixedRateMortgage, adjustableRateMortgage;
    private AtlasGlossaryCategory customerCategory, accountCategory, mortgageCategory;

    private AtlasEntityHeader testEntityHeader;

    @BeforeClass
    public void setupSampleGlossary() {
        try {
            ZipFileResourceTestUtils.loadAllModels("0000-Area0", typeDefStore, typeRegistry);
        } catch (AtlasBaseException | IOException e) {
            throw new SkipException("SubjectArea model loading failed");
        }
    }

    @Test(groups = "Glossary.CREATE")
    public void testCreateGlossary() {
        bankGlossary = new AtlasGlossary();
        bankGlossary.setQualifiedName("testBankingGlossary");
        bankGlossary.setDisplayName("Banking glossary");
        bankGlossary.setShortDescription("Short description");
        bankGlossary.setLongDescription("Long description");
        bankGlossary.setUsage("N/A");
        bankGlossary.setLanguage("en-US");

        try {
            AtlasGlossary created = glossaryService.createGlossary(bankGlossary);
            bankGlossary.setGuid(created.getGuid());
        } catch (AtlasBaseException e) {
            fail("Glossary creation should've succeeded", e);
        }
    }

    @Test(groups = "Glossary.CREATE", dependsOnMethods = {"testCreateGlossary"})
    public void testCreateGlossaryTerms() {
        AtlasGlossaryHeader glossaryId = new AtlasGlossaryHeader();
        glossaryId.setGlossaryGuid(bankGlossary.getGuid());

        checkingAccount = new AtlasGlossaryTerm();
        checkingAccount.setQualifiedName("chk_acc@testBankingGlossary");
        checkingAccount.setDisplayName("A checking account");
        checkingAccount.setShortDescription("Short description");
        checkingAccount.setLongDescription("Long description");
        checkingAccount.setAbbreviation("CHK");
        checkingAccount.setExamples(Arrays.asList("Personal", "Joint"));
        checkingAccount.setUsage("N/A");
        checkingAccount.setAnchor(glossaryId);

        savingsAccount = new AtlasGlossaryTerm();
        savingsAccount.setQualifiedName("sav_acc@testBankingGlossary");
        savingsAccount.setDisplayName("A savings account");
        savingsAccount.setShortDescription("Short description");
        savingsAccount.setLongDescription("Long description");
        savingsAccount.setAbbreviation("SAV");
        savingsAccount.setExamples(Arrays.asList("Personal", "Joint"));
        savingsAccount.setUsage("N/A");
        savingsAccount.setAnchor(glossaryId);

        fixedRateMortgage = new AtlasGlossaryTerm();
        fixedRateMortgage.setQualifiedName("fixed_mtg@testBankingGlossary");
        fixedRateMortgage.setDisplayName("15/30 yr mortgage");
        fixedRateMortgage.setShortDescription("Short description");
        fixedRateMortgage.setLongDescription("Long description");
        fixedRateMortgage.setAbbreviation("FMTG");
        fixedRateMortgage.setExamples(Arrays.asList("15-yr", "30-yr"));
        fixedRateMortgage.setUsage("N/A");
        fixedRateMortgage.setAnchor(glossaryId);

        adjustableRateMortgage = new AtlasGlossaryTerm();
        adjustableRateMortgage.setQualifiedName("arm_mtg@testBankingGlossary");
        adjustableRateMortgage.setDisplayName("ARM loans");
        adjustableRateMortgage.setShortDescription("Short description");
        adjustableRateMortgage.setLongDescription("Long description");
        adjustableRateMortgage.setAbbreviation("ARMTG");
        adjustableRateMortgage.setExamples(Arrays.asList("5/1", "7/1", "10/1"));
        adjustableRateMortgage.setUsage("N/A");
        adjustableRateMortgage.setAnchor(glossaryId);

        try {
            List<AtlasGlossaryTerm> terms = glossaryService.createTerms(Arrays.asList(checkingAccount, savingsAccount, fixedRateMortgage, adjustableRateMortgage));
            checkingAccount.setGuid(terms.get(0).getGuid());
            savingsAccount.setGuid(terms.get(1).getGuid());
            fixedRateMortgage.setGuid(terms.get(2).getGuid());
            adjustableRateMortgage.setGuid(terms.get(3).getGuid());
        } catch (AtlasBaseException e) {
            fail("Term creation should've succeeded", e);
        }
    }

    @Test(groups = "Glossary.CREATE", dependsOnMethods = {"testCreateGlossaryTerms"})
    public void testCreateGlossaryCategory() {
        AtlasGlossaryHeader glossaryId = new AtlasGlossaryHeader();
        glossaryId.setGlossaryGuid(bankGlossary.getGuid());

        customerCategory = new AtlasGlossaryCategory();
        customerCategory.setQualifiedName("customer@testBankingGlossary");
        customerCategory.setDisplayName("Customer category");
        customerCategory.setShortDescription("Short description");
        customerCategory.setLongDescription("Long description");
        customerCategory.setAnchor(glossaryId);

        accountCategory = new AtlasGlossaryCategory();
        accountCategory.setQualifiedName("acc@testBankingGlossary");
        accountCategory.setDisplayName("Account categorization");
        accountCategory.setShortDescription("Short description");
        accountCategory.setLongDescription("Long description");
        accountCategory.setAnchor(glossaryId);

        mortgageCategory = new AtlasGlossaryCategory();
        mortgageCategory.setQualifiedName("mtg@testBankingGlossary");
        mortgageCategory.setDisplayName("Mortgage categorization");
        mortgageCategory.setShortDescription("Short description");
        mortgageCategory.setLongDescription("Long description");
        mortgageCategory.setAnchor(glossaryId);

        try {
            List<AtlasGlossaryCategory> categories = glossaryService.createCategories(Arrays.asList(customerCategory, accountCategory, mortgageCategory));
            customerCategory.setGuid(categories.get(0).getGuid());
            accountCategory.setGuid(categories.get(1).getGuid());
            mortgageCategory.setGuid(categories.get(2).getGuid());
        } catch (AtlasBaseException e) {
            fail("Category creation should've succeeded", e);
        }
    }

    @DataProvider
    public Object[][] getAllGlossaryDataProvider() {
        return new Object[][]{
                // limit, offset, sortOrder, expected
                {1, 0, SortOrder.ASCENDING, 1},
                {5, 0, SortOrder.ASCENDING, 1},
                {10, 0, SortOrder.ASCENDING, 1},
                {1, 1, SortOrder.ASCENDING, 0},
                {5, 1, SortOrder.ASCENDING, 0},
                {10, 1, SortOrder.ASCENDING, 0},
                {1, 2, SortOrder.ASCENDING, 0},
                {5, 2, SortOrder.ASCENDING, 0},
                {10, 2, SortOrder.ASCENDING, 0},
        };
    }

    @Test(dataProvider = "getAllGlossaryDataProvider")
    public void testGetAllGlossaries(int limit, int offset, SortOrder sortOrder, int expected) {
        try {
            List<AtlasGlossary> glossaries = glossaryService.getGlossaries(limit, offset, sortOrder);
            assertEquals(glossaries.size(), expected);
        } catch (AtlasBaseException e) {
            fail("Get glossaries should've succeeded", e);
        }
    }

    @Test(groups = "Glossary.GET", dependsOnGroups = "Glossary.CREATE")
    public void testGetGlossary() {
        try {
            AtlasGlossary glossary = glossaryService.getGlossary(bankGlossary.getGuid());
            assertNotNull(glossary);
            assertEquals(glossary.getGuid(), bankGlossary.getGuid());
        } catch (AtlasBaseException e) {
            fail("Glossary fetch should've succeeded", e);
        }
    }

    @Test(groups = "Glossary.UPDATE", dependsOnGroups = "Glossary.CREATE")
    public void testUpdateGlossary() {
        try {
            bankGlossary = glossaryService.getGlossary(bankGlossary.getGuid());
            bankGlossary.setShortDescription("Updated short description");
            bankGlossary.setLongDescription("Updated long description");

            AtlasGlossary updatedGlossary = glossaryService.updateGlossary(bankGlossary);
            assertNotNull(updatedGlossary);
            assertEquals(updatedGlossary.getGuid(), bankGlossary.getGuid());
            assertEquals(updatedGlossary, bankGlossary);
            bankGlossary = updatedGlossary;
        } catch (AtlasBaseException e) {
            fail("Glossary fetch/update should've succeeded", e);
        }
    }

    @Test(dependsOnGroups = {"Glossary.GET", "Glossary.UPDATE", "Glossary.GET.postUpdate"}) // Should be the last test
    public void testDeleteGlossary() {
        try {
            glossaryService.deleteGlossary(bankGlossary.getGuid());
            try {
                glossaryService.getGlossary(bankGlossary.getGuid());
            } catch (AtlasBaseException e) {
                assertEquals(e.getAtlasErrorCode(), AtlasErrorCode.INSTANCE_GUID_DELETED);
            }
        } catch (AtlasBaseException e) {
            fail("Glossary delete should've succeeded", e);
        }
    }

    @Test(groups = "Glossary.GET", dependsOnGroups = "Glossary.CREATE")
    public void testGetGlossaryTerm() {
        for (AtlasGlossaryTerm t : Arrays.asList(checkingAccount, savingsAccount, fixedRateMortgage, adjustableRateMortgage)) {
            try {
                AtlasGlossaryTerm glossaryTerm = glossaryService.getTerm(t.getGuid());
                assertNotNull(glossaryTerm);
                assertEquals(glossaryTerm.getGuid(), t.getGuid());
            } catch (AtlasBaseException e) {
                fail("Glossary term fetching should've succeeded", e);
            }
        }
    }

    @Test(groups = "Glossary.UPDATE", dependsOnGroups = "Glossary.CREATE")
    public void testUpdateGlossaryTerm() {
        List<AtlasGlossaryTerm> glossaryTerms = new ArrayList<>();
        for (AtlasGlossaryTerm term : Arrays.asList(checkingAccount, savingsAccount, fixedRateMortgage, adjustableRateMortgage)) {
            try {
                glossaryTerms.add(glossaryService.getTerm(term.getGuid()));
            } catch (AtlasBaseException e) {
                fail("Fetch of GlossaryTerm should've succeeded", e);
            }
        }
        for (AtlasGlossaryTerm t : glossaryTerms) {
            try {
                t.setShortDescription("Updated short description");
                t.setLongDescription("Updated long description");

                AtlasGlossaryTerm updatedTerm = glossaryService.updateTerm(t);
                assertNotNull(updatedTerm);
                assertEquals(updatedTerm.getGuid(), t.getGuid());
            } catch (AtlasBaseException e) {
                fail("Glossary term fetch/update should've succeeded", e);
            }
        }
    }

    @Test(groups = "Glossary.GET", dependsOnGroups = "Glossary.CREATE")
    public void testGetGlossaryCategory() {
        for (AtlasGlossaryCategory c : Arrays.asList(customerCategory, accountCategory, mortgageCategory)) {
            try {
                AtlasGlossaryCategory glossaryCategory = glossaryService.getCategory(c.getGuid());
                assertNotNull(glossaryCategory);
                assertEquals(glossaryCategory.getGuid(), c.getGuid());
            } catch (AtlasBaseException e) {
                fail("Glossary category fetching should've succeeded", e);
            }
        }
    }

    @Test(groups = "Glossary.UPDATE", dependsOnGroups = "Glossary.CREATE")
    public void testUpdateGlossaryCategory() {
        List<AtlasGlossaryCategory> glossaryCategories = new ArrayList<>();
        for (AtlasGlossaryCategory glossaryCategory : Arrays.asList(customerCategory, accountCategory, mortgageCategory)) {
            try {
                glossaryCategories.add(glossaryService.getCategory(glossaryCategory.getGuid()));
            } catch (AtlasBaseException e) {
                fail("Category fetch should've succeeded", e);
            }
        }

        for (AtlasGlossaryCategory c : glossaryCategories) {
            try {
                AtlasGlossaryCategory glossaryCategory = glossaryService.getCategory(c.getGuid());
                glossaryCategory.setShortDescription("Updated short description");
                glossaryCategory.setLongDescription("Updated long description");

                AtlasGlossaryCategory updatedCategory = glossaryService.updateCategory(glossaryCategory);
                assertNotNull(updatedCategory);
                assertEquals(updatedCategory.getGuid(), c.getGuid());
            } catch (AtlasBaseException e) {
                fail("Glossary category fetching should've succeeded", e);
            }
        }
    }

    @Test(groups = "Glossary.UPDATE", dependsOnGroups = "Glossary.CREATE")
    public void testAddTermsToCategory() {
        assertNotNull(accountCategory);
        try {
            accountCategory = glossaryService.getCategory(accountCategory.getGuid());
        } catch (AtlasBaseException e) {
            fail("Fetch of accountCategory should've succeeded", e);
        }
        List<AtlasGlossaryTerm> terms = new ArrayList<>();
        for (AtlasGlossaryTerm term : Arrays.asList(checkingAccount, savingsAccount)) {
            try {
                terms.add(glossaryService.getTerm(term.getGuid()));
            } catch (AtlasBaseException e) {
                fail("Term fetching should've succeeded", e);
            }
        }
        for (AtlasGlossaryTerm termEntry : terms) {
            AtlasRelatedTermHeader relatedTermId = new AtlasRelatedTermHeader();
            relatedTermId.setTermGuid(termEntry.getGuid());
            relatedTermId.setStatus(AtlasTermRelationshipStatus.ACTIVE);
            relatedTermId.setSteward("UT");
            relatedTermId.setSource("UT");
            relatedTermId.setExpression("N/A");
            relatedTermId.setDescription("Categorization under account category");
            accountCategory.addTerm(relatedTermId);
        }
        try {
            AtlasGlossaryCategory updated = glossaryService.updateCategory(accountCategory);
            assertNotNull(updated.getTerms());
            assertEquals(updated.getTerms().size(), 2);
            accountCategory = updated;
        } catch (AtlasBaseException e) {
            fail("Glossary category update should've succeeded", e);
        }

        assertNotNull(accountCategory);
        try {
            accountCategory = glossaryService.getCategory(accountCategory.getGuid());
        } catch (AtlasBaseException e) {
            fail("Fetch of accountCategory should've succeeded", e);
        }

        terms.clear();
        for (AtlasGlossaryTerm term : Arrays.asList(fixedRateMortgage, adjustableRateMortgage)) {
            try {
                terms.add(glossaryService.getTerm(term.getGuid()));
            } catch (AtlasBaseException e) {
                fail("Term fetching should've succeeded", e);
            }
        }

        for (AtlasGlossaryTerm termEntry : terms) {
            AtlasRelatedTermHeader relatedTermId = new AtlasRelatedTermHeader();
            relatedTermId.setTermGuid(termEntry.getGuid());
            relatedTermId.setStatus(AtlasTermRelationshipStatus.ACTIVE);
            relatedTermId.setSteward("UT");
            relatedTermId.setSource("UT");
            relatedTermId.setExpression("N/A");
            relatedTermId.setDescription("Categorization under mortgage category");

            mortgageCategory.addTerm(relatedTermId);
        }
        try {
            AtlasGlossaryCategory updated = glossaryService.updateCategory(mortgageCategory);
            assertNotNull(updated.getTerms());
            assertEquals(updated.getTerms().size(), 2);
            mortgageCategory = updated;
        } catch (AtlasBaseException e) {
            fail("Glossary category update should've succeeded", e);
        }

    }

    @Test(groups = "Glossary.UPDATE", dependsOnGroups = "Glossary.CREATE")
    public void testAddGlossaryCategoryChildren() {
        assertNotNull(customerCategory);
        try {
            customerCategory = glossaryService.getCategory(customerCategory.getGuid());
        } catch (AtlasBaseException e) {
            fail("Fetch of accountCategory should've succeeded", e);
        }
        List<AtlasGlossaryCategory> categories = new ArrayList<>();
        for (AtlasGlossaryCategory category : Arrays.asList(accountCategory, mortgageCategory)) {
            try {
                categories.add(glossaryService.getCategory(category.getGuid()));
            } catch (AtlasBaseException e) {
                fail("Category fetch should've succeeded");
            }
        }

        for (AtlasGlossaryCategory category : categories) {
            AtlasRelatedCategoryHeader id = new AtlasRelatedCategoryHeader();
            id.setCategoryGuid(category.getGuid());
            id.setDescription("Sub-category of customer");
            customerCategory.addChild(id);
        }

        try {
            AtlasGlossaryCategory updateGlossaryCategory = glossaryService.updateCategory(customerCategory);
            assertNull(updateGlossaryCategory.getParentCategory());
            assertNotNull(updateGlossaryCategory.getChildrenCategories());
            assertEquals(updateGlossaryCategory.getChildrenCategories().size(), 2);
            customerCategory = updateGlossaryCategory;

            LOG.debug(AtlasJson.toJson(updateGlossaryCategory));
        } catch (AtlasBaseException e) {
            fail("Sub category addition should've succeeded", e);
        }
    }

    @Test(groups = "Glossary.GET.postUpdate", dependsOnGroups = "Glossary.UPDATE")
    public void testCategoryRelation() {
        AtlasGlossaryCategory parent = customerCategory;

        Map<String, List<AtlasRelatedCategoryHeader>> relatedCategories;
        try {
            relatedCategories = glossaryService.getRelatedCategories(parent.getGuid(), 0, -1, SortOrder.ASCENDING);
            assertNotNull(relatedCategories);
            assertNotNull(relatedCategories.get("children"));
            assertEquals(relatedCategories.get("children").size(), 2);
        } catch (AtlasBaseException e) {
            fail("Category fetch should've succeeded", e);
        }

        for (AtlasGlossaryCategory childCategory : Arrays.asList(accountCategory, mortgageCategory)) {
            try {
                relatedCategories = glossaryService.getRelatedCategories(childCategory.getGuid(), 0, -1, SortOrder.ASCENDING);
                assertNotNull(relatedCategories.get("parent"));
                assertEquals(relatedCategories.get("parent").size(), 1);
            } catch (AtlasBaseException e) {
                fail("Category fetch should've succeeded", e);
            }
        }
    }

    @Test(groups = "Glossary.UPDATE", dependsOnGroups = "Glossary.CREATE")
    public void testTermAssignment() {
        AtlasEntity assetEntity = new AtlasEntity("Asset");
        assetEntity.setAttribute("qualifiedName", "testAsset");
        assetEntity.setAttribute("name", "testAsset");

        try {
            EntityMutationResponse response = entityStore.createOrUpdate(new AtlasEntityStream(assetEntity), false);
            testEntityHeader = response.getFirstEntityCreated();
            assertNotNull(testEntityHeader);
        } catch (AtlasBaseException e) {
            fail("Entity creation should've succeeded", e);
        }

        try {
            glossaryService.assignTermToEntities(fixedRateMortgage.getGuid(), Arrays.asList(testEntityHeader));
        } catch (AtlasBaseException e) {
            fail("Term assignment to asset should've succeeded", e);
        }

        try {
            List<AtlasEntityHeader> assignedEntities = glossaryService.getAssignedEntities(fixedRateMortgage.getGuid(), 0, 1, SortOrder.ASCENDING);
            assertNotNull(assignedEntities);
            assertEquals(assignedEntities.size(), 1);
            Object relationGuid = assignedEntities.get(0).getAttribute("relationGuid");
            assertNotNull(relationGuid);
            testEntityHeader.setAttribute("relationGuid", relationGuid);
        } catch (AtlasBaseException e) {
            fail("Term fetch should've succeeded",e);
        }

    }

    // FIXME: The term dissociation is not working as intended.
    @Test(groups = "Glossary.UPDATE", dependsOnGroups = "Glossary.CREATE", dependsOnMethods = "testTermAssignment")
    public void testTermDissociation() {
        try {
            glossaryService.removeTermFromEntities(fixedRateMortgage.getGuid(), Arrays.asList(testEntityHeader));
            AtlasGlossaryTerm term = glossaryService.getTerm(fixedRateMortgage.getGuid());
            assertNotNull(term);
            assertNull(term.getAssignedEntities());
        } catch (AtlasBaseException e) {
            fail("Term update should've succeeded", e);
        }
    }

    @Test(groups = "Glossary.UPDATE", dependsOnGroups = "Glossary.CREATE")
    public void testTermRelation() {
        AtlasRelatedTermHeader relatedTerm = new AtlasRelatedTermHeader();
        relatedTerm.setTermGuid(savingsAccount.getGuid());
        relatedTerm.setStatus(AtlasTermRelationshipStatus.DRAFT);
        relatedTerm.setSteward("UT");
        relatedTerm.setSource("UT");
        relatedTerm.setExpression("N/A");
        relatedTerm.setDescription("Related term");

        assertNotNull(checkingAccount);
        try {
            checkingAccount = glossaryService.getTerm(checkingAccount.getGuid());
        } catch (AtlasBaseException e) {
            fail("Glossary term fetch should've been a success", e);
        }

        checkingAccount.setSeeAlso(new HashSet<>(Arrays.asList(relatedTerm)));

        try {
            AtlasGlossaryTerm updatedTerm = glossaryService.updateTerm(checkingAccount);
            assertNotNull(updatedTerm.getSeeAlso());
            assertEquals(updatedTerm.getSeeAlso().size(), 1);
        } catch (AtlasBaseException e) {
            fail("RelatedTerm association should've succeeded", e);
        }

        relatedTerm.setTermGuid(fixedRateMortgage.getGuid());

        assertNotNull(adjustableRateMortgage);
        try {
            adjustableRateMortgage = glossaryService.getTerm(adjustableRateMortgage.getGuid());
        } catch (AtlasBaseException e) {
            fail("Glossary term fetch should've been a success", e);
        }

        adjustableRateMortgage.setSeeAlso(new HashSet<>(Arrays.asList(relatedTerm)));

        try {
            AtlasGlossaryTerm updatedTerm = glossaryService.updateTerm(adjustableRateMortgage);
            assertNotNull(updatedTerm.getSeeAlso());
            assertEquals(updatedTerm.getSeeAlso().size(), 1);
        } catch (AtlasBaseException e) {
            fail("RelatedTerm association should've succeeded", e);
        }

    }

    @DataProvider
    public static Object[][] getGlossaryTermsProvider() {
        return new Object[][]{
                // offset, limit, expected
                {0, -1, 4},
                {0, 2, 2},
                {2, 5, 2},
        };
    }

    @Test(dataProvider = "getGlossaryTermsProvider", groups = "Glossary.GET.postUpdate", dependsOnGroups = "Glossary.UPDATE")
    public void testGetGlossaryTerms(int offset, int limit, int expected) {
        String    guid      = bankGlossary.getGuid();
        SortOrder sortOrder = SortOrder.ASCENDING;

        try {
            List<AtlasGlossaryTerm> glossaryTerms = glossaryService.getGlossaryTerms(guid, offset, limit, sortOrder);
            assertNotNull(glossaryTerms);
            assertEquals(glossaryTerms.size(), expected);
        } catch (AtlasBaseException e) {
            fail("Glossary term fetching should've succeeded", e);
        }
    }

    @DataProvider
    public Object[][] getGlossaryCategoriesProvider() {
        return new Object[][]{
                // offset, limit, expected
                {0, -1, 3},
                {0, 2, 2},
                {2, 5, 1},
        };
    }

    @Test(dataProvider = "getGlossaryCategoriesProvider", groups = "Glossary.GET.postUpdate", dependsOnGroups = "Glossary.UPDATE")
    public void testGetGlossaryCategories(int offset, int limit, int expected) {
        String    guid      = bankGlossary.getGuid();
        SortOrder sortOrder = SortOrder.ASCENDING;

        try {
            List<AtlasRelatedCategoryHeader> glossaryCategories = glossaryService.getGlossaryCategories(guid, offset, limit, sortOrder);
            assertNotNull(glossaryCategories);
            assertEquals(glossaryCategories.size(), expected);
        } catch (AtlasBaseException e) {
            fail("Glossary term fetching should've succeeded");
        }
    }

    @DataProvider
    public Object[][] getCategoryTermsProvider() {
        return new Object[][]{
                // offset, limit, expected
                {0, -1, 2},
                {0, 2, 2},
                {1, 5, 1},
                {2, 5, 0},
        };
    }


    @Test(dataProvider = "getCategoryTermsProvider", groups = "Glossary.GET.postUpdate", dependsOnGroups = "Glossary.UPDATE")
    public void testGetCategoryTerms(int offset, int limit, int expected) {
        for (AtlasGlossaryCategory c : Arrays.asList(accountCategory, mortgageCategory)) {
            try {
                List<AtlasRelatedTermHeader> categoryTerms = glossaryService.getCategoryTerms(c.getGuid(), offset, limit, SortOrder.ASCENDING);
                assertNotNull(categoryTerms);
                assertEquals(categoryTerms.size(), expected);
            } catch (AtlasBaseException e) {
                fail("Category term retrieval should've been a success", e);
            }
        }
    }
}