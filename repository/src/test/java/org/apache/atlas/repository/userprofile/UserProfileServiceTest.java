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
package org.apache.atlas.repository.userprofile;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.profile.AtlasUserProfile;
import org.apache.atlas.model.profile.AtlasUserSavedSearch;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.atlas.store.AtlasTypeDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.atlas.graph.GraphSandboxUtil.useLocalSolr;
import static org.apache.atlas.model.profile.AtlasUserSavedSearch.SavedSearchType.BASIC;
import static org.apache.atlas.repository.impexp.ZipFileResourceTestUtils.loadModelFromJson;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class UserProfileServiceTest {
    private UserProfileService userProfileService;
    private AtlasTypeDefStore  typeDefStore;
    private int                max_searches = 4;

    @Inject
    public void UserProfileServiceTest(AtlasTypeRegistry  typeRegistry,
                                       AtlasTypeDefStore  typeDefStore,
                                       UserProfileService userProfileService) throws IOException, AtlasBaseException {
        this.typeDefStore       = typeDefStore;
        this.userProfileService = userProfileService;

        loadModelFromJson("0010-base_model.json", typeDefStore, typeRegistry);
    }

    @AfterClass
    public void clear() throws Exception {
        AtlasGraphProvider.cleanup();

        if (useLocalSolr()) {
            LocalSolrRunner.stop();
        }
    }

    @Test
    public void filterInternalType() throws AtlasBaseException {
        SearchFilter searchFilter = new SearchFilter();
        FilterUtil.addParamsToHideInternalType(searchFilter);
        AtlasTypesDef filteredTypeDefs = typeDefStore.searchTypesDef(searchFilter);

        assertNotNull(filteredTypeDefs);
        Optional<AtlasEntityDef> anyInternal = filteredTypeDefs.getEntityDefs().stream().filter(e -> e.getSuperTypes().contains("__internal")).findAny();
        assertFalse(anyInternal.isPresent());
    }

    @Test
    public void createsNewProfile() throws AtlasBaseException {
        int i = 0;
        assertSaveLoadUserProfile(i++);
        assertSaveLoadUserProfile(i);
    }

    @Test(dependsOnMethods = { "createsNewProfile", "savesQueryForAnNonExistentUser" }, expectedExceptions = AtlasBaseException.class)
    public void atteptsToAddAlreadyExistingQueryForAnExistingUser() throws AtlasBaseException {
        SearchParameters expectedSearchParameter = getActualSearchParameters();

        for (int i = 0; i < 2; i++) {
            String userName = getIndexBasedUserName(i);

            for (int j = 0; j < max_searches; j++) {
                String queryName = getIndexBasedQueryName(j);
                AtlasUserSavedSearch expected = getDefaultSavedSearch(userName, queryName, expectedSearchParameter);
                AtlasUserSavedSearch actual = userProfileService.addSavedSearch(expected);

                assertNotNull(actual);
                assertNotNull(actual.getGuid());
                assertEquals(actual.getOwnerName(), expected.getOwnerName());
                assertEquals(actual.getName(), expected.getName());
                assertEquals(actual.getSearchType(), expected.getSearchType());
                assertEquals(actual.getSearchParameters(), expected.getSearchParameters());
            }
        }
    }

    @Test(dependsOnMethods = { "createsNewProfile", "savesQueryForAnNonExistentUser", "atteptsToAddAlreadyExistingQueryForAnExistingUser" })
    public void savesExistingQueryForAnExistingUser() throws AtlasBaseException {
        SearchParameters expectedSearchParameter = getActualSearchParameters();

        for (int i = 0; i < 2; i++) {
            String userName = getIndexBasedUserName(i);

            for (int j = 4; j < max_searches + 6; j++) {
                String queryName = getIndexBasedQueryName(j);
                AtlasUserSavedSearch actual = userProfileService.addSavedSearch(getDefaultSavedSearch(userName, queryName, expectedSearchParameter));
                assertNotNull(actual);

                AtlasUserSavedSearch savedSearch = userProfileService.getSavedSearch(userName, queryName);
                assertNotNull(savedSearch);
                assertEquals(savedSearch.getSearchParameters(), expectedSearchParameter);
            }
        }
    }

    private SearchParameters getActualSearchParameters() {
        SearchParameters sp = new SearchParameters();
        sp.setClassification("test-classification");
        sp.setQuery("g.v().has('__guid').__guid.toList()");
        sp.setLimit(10);
        sp.setTypeName("some-type");

        return sp;
    }

    @Test(dependsOnMethods = "createsNewProfile")
    public void savesQueryForAnNonExistentUser() throws AtlasBaseException {
        String expectedUserName = getIndexBasedUserName(0);
        String expectedQueryName = "testQuery";
        SearchParameters expectedSearchParam = getActualSearchParameters();
        AtlasUserSavedSearch expectedSavedSearch = getDefaultSavedSearch(expectedUserName, expectedQueryName, expectedSearchParam);

        AtlasUserSavedSearch actual = userProfileService.addSavedSearch(expectedSavedSearch);
        assertEquals(actual.getOwnerName(), expectedUserName);
        assertEquals(actual.getName(), expectedQueryName);
    }

    private AtlasUserSavedSearch getDefaultSavedSearch(String userName, String queryName, SearchParameters expectedSearchParam) {
        return new AtlasUserSavedSearch(userName, queryName,
                BASIC, expectedSearchParam);
    }

    @Test(dependsOnMethods = "createsNewProfile")
    public void savesMultipleQueriesForUser() throws AtlasBaseException {
        final String userName = getIndexBasedUserName(0);
        createUserWithSavedQueries(userName);
    }

    private void createUserWithSavedQueries(String userName) throws AtlasBaseException {
        SearchParameters actualSearchParameter = getActualSearchParameters();

        saveQueries(userName, actualSearchParameter);
        for (int i = 0; i < max_searches; i++) {
            AtlasUserSavedSearch savedSearch = userProfileService.getSavedSearch(userName, getIndexBasedQueryName(i));
            assertEquals(savedSearch.getName(), getIndexBasedQueryName(i));
            assertEquals(savedSearch.getSearchParameters(), actualSearchParameter);
        }
    }

    private void saveQueries(String userName, SearchParameters sp) throws AtlasBaseException {
        for (int i = 0; i < max_searches; i++) {
            userProfileService.addSavedSearch(getDefaultSavedSearch(userName, getIndexBasedQueryName(i), sp));
        }
    }

    @Test(dependsOnMethods = {"createsNewProfile", "savesMultipleQueriesForUser"})
    public void verifyQueryNameListForUser() throws AtlasBaseException {
        final String userName = getIndexBasedUserName(0);

        List<AtlasUserSavedSearch> list = userProfileService.getSavedSearches(userName);
        List<String> names = getIndexBasedQueryNamesList();
        for (int i = 0; i < names.size(); i++) {
            assertTrue(names.contains(list.get(i).getName()), list.get(i).getName() + " failed!");
        }
    }

    @Test(dependsOnMethods = {"createsNewProfile", "savesMultipleQueriesForUser"})
    public void verifyQueryConversionFromJSON() throws AtlasBaseException {
        List<AtlasUserSavedSearch> list = userProfileService.getSavedSearches("first-0");

        for (int i = 0; i < max_searches; i++) {
            SearchParameters sp = list.get(i).getSearchParameters();
            String json = AtlasType.toJson(sp);
            assertEquals(AtlasType.toJson(getActualSearchParameters()).replace("\n", "").replace(" ", ""), json);
        }
    }

    @Test(dependsOnMethods = {"createsNewProfile", "savesMultipleQueriesForUser", "verifyQueryConversionFromJSON"})
    public void updateSearch() throws AtlasBaseException {
        final String queryName = getIndexBasedQueryName(0);
        String userName = getIndexBasedUserName(0);
        AtlasUserSavedSearch expected = userProfileService.getSavedSearch(userName, queryName);
        assertNotNull(expected);

        SearchParameters sp = expected.getSearchParameters();
        sp.setClassification("new-classification");

        AtlasUserSavedSearch actual = userProfileService.updateSavedSearch(expected);

        assertNotNull(actual);
        assertNotNull(actual.getSearchParameters());
        assertEquals(actual.getSearchParameters().getClassification(), expected.getSearchParameters().getClassification());
    }

    @Test(dependsOnMethods = {"createsNewProfile", "savesMultipleQueriesForUser", "verifyQueryNameListForUser"}, expectedExceptions = AtlasBaseException.class)
    public void deleteUsingGuid() throws AtlasBaseException {
        final String queryName = getIndexBasedQueryName(1);
        String userName = getIndexBasedUserName(0);

        AtlasUserSavedSearch expected = userProfileService.getSavedSearch(userName, queryName);
        assertNotNull(expected);

        userProfileService.deleteSavedSearch(expected.getGuid());
        userProfileService.getSavedSearch(userName, queryName);
    }

    @Test(dependsOnMethods = {"createsNewProfile", "savesMultipleQueriesForUser", "verifyQueryNameListForUser"})
    public void deleteSavedQuery() throws AtlasBaseException {
        final String userName = getIndexBasedUserName(0);
        AtlasUserProfile expected = userProfileService.getUserProfile(userName);
        assertNotNull(expected);

        int new_max_searches = expected.getSavedSearches().size();
        String queryNameToBeDeleted = getIndexBasedQueryName(max_searches - 2);
        userProfileService.deleteSearchBySearchName(userName, queryNameToBeDeleted);

        List<AtlasUserSavedSearch> savedSearchList = userProfileService.getSavedSearches(userName);
        assertEquals(savedSearchList.size(), new_max_searches - 1);
    }
    @Test(dependsOnMethods = {"createsNewProfile", "savesMultipleQueriesForUser", "verifyQueryNameListForUser"})
    void deleteUser() throws AtlasBaseException {
        String userName = getIndexBasedUserName(1);

        userProfileService.deleteUserProfile(userName);
        try {
            userProfileService.getUserProfile(userName);
        }
        catch(AtlasBaseException ex) {
            assertEquals(ex.getAtlasErrorCode().name(), AtlasErrorCode.INSTANCE_BY_UNIQUE_ATTRIBUTE_NOT_FOUND.name());
        }
    }

    private void assertSaveLoadUserProfile(int i) throws AtlasBaseException {
        String s = String.valueOf(i);
        AtlasUserProfile expected = getAtlasUserProfile(i);

        AtlasUserProfile actual = userProfileService.saveUserProfile(expected);
        assertNotNull(actual);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getFullName(), actual.getFullName());
        assertNotNull(actual.getGuid());
    }

    public static AtlasUserProfile getAtlasUserProfile(Integer s) {
        return new AtlasUserProfile(getIndexBasedUserName(s), String.format("first-%s last-%s", s, s));
    }

    private static String getIndexBasedUserName(Integer i) {
        return String.format("first-%s", i.toString());
    }

    private static String getIndexBasedQueryName(Integer i) {
        return String.format("testQuery-%s", i.toString());
    }

    public List<String> getIndexBasedQueryNamesList() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < max_searches; i++) {
            list.add(getIndexBasedQueryName(i));
        }

        return list;
    }
}
