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
package org.apache.atlas.web.integration;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.impexp.AtlasImportRequest;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.utils.TestResourceFileUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class BasicSearchIT extends BaseResourceIT {

    @BeforeClass
    @Override
    public void setUp() throws Exception {
        super.setUp();

        String smallDatasetFileName = "hive-db-50-tables.zip";
        atlasClientV2.importData(new AtlasImportRequest(), TestResourceFileUtils.getTestFilePath(smallDatasetFileName));

        // Create a entity with name/qualified name having special characters

        // Create a test tag
        if (!atlasClientV2.typeWithNameExists("fooTag")) {
            AtlasClassificationDef testClassificationDef = AtlasTypeUtil.createTraitTypeDef("fooTag", "Test tag", "1.0", Collections.<String>emptySet());
            AtlasTypesDef          typesDef              = new AtlasTypesDef();
            typesDef.getClassificationDefs().add(testClassificationDef);
            atlasClientV2.createAtlasTypeDefs(typesDef);
        }

        try {
            atlasClientV2.getEntityByAttribute("hdfs_path", new HashMap<String, String>() {{
                put("qualifiedName", URLEncoder.encode("test$1test+ - && || ! ( ) { } [ ] ^ < > ; : \" % * ` ~", "UTF-8"));
            }});
        } catch (AtlasServiceException e) {
            AtlasEntity hdfsEntity = new AtlasEntity("hdfs_path");
            hdfsEntity.setGuid("-1");
            hdfsEntity.setAttribute("description", "1test+ - && || ! ( ) { } [ ] ^ < > ; : \" % * ` ~");
            hdfsEntity.setAttribute("name", "1test+ - && || ! ( ) { } [ ] ^ < > ; : \" % * ` ~");
            hdfsEntity.setAttribute("owner", "test");
            hdfsEntity.setAttribute("qualifiedName", "test$1test+ - && || ! ( ) { } [ ] ^ < > ; : \" % * ` ~");
            hdfsEntity.setAttribute("path", "/test/foo");

            hdfsEntity.setClassifications(new ArrayList<AtlasClassification>());
            hdfsEntity.getClassifications().add(new AtlasClassification("fooTag"));

            EntityMutationResponse entityMutationResponse = atlasClientV2.createEntity(new AtlasEntity.AtlasEntityWithExtInfo(hdfsEntity));
            if (entityMutationResponse.getCreatedEntities() != null) {
                assertEquals(entityMutationResponse.getCreatedEntities().size(), 1);
            } else if (entityMutationResponse.getUpdatedEntities() != null) {
                assertEquals(entityMutationResponse.getUpdatedEntities().size(), 1);
            } else {
                fail("Entity should've been created or updated");
            }
        }

        // Add a 5s mandatory sleep for allowing index to catch up
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            LOG.error("Sleep was interrupted. The search results might be inconsistent.");
        }
    }

    @DataProvider
    public Object[][] basicSearchJSONNames() {
        return new String[][]{
                {"search-parameters/entity-filters"},
                {"search-parameters/tag-filters"},
                {"search-parameters/combination-filters"}
        };
    }

    @Test(dataProvider = "basicSearchJSONNames")
    public void testDiscoveryWithSearchParameters(String jsonFile) {
        try {
            BasicSearchParametersWithExpectation[] testExpectations =
                    TestResourceFileUtils.readObjectFromJson(jsonFile, BasicSearchParametersWithExpectation[].class);
            assertNotNull(testExpectations);

            for (BasicSearchParametersWithExpectation testExpectation : testExpectations) {
                LOG.info("TestDescription  :{}", testExpectation.testDescription);
                LOG.info("SearchParameters :{}", testExpectation.searchParameters);

                AtlasSearchResult searchResult = atlasClientV2.facetedSearch(testExpectation.searchParameters);
                if (testExpectation.expectedCount > 0) {
                    assertNotNull(searchResult.getEntities());
                    assertEquals(searchResult.getEntities().size(), testExpectation.expectedCount);
                }
            }
        } catch (IOException | AtlasServiceException e) {
            fail(e.getMessage());
        }
    }

    @JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class BasicSearchParametersWithExpectation {
        private String           testDescription;
        private SearchParameters searchParameters;
        private int              expectedCount;

        public BasicSearchParametersWithExpectation() {
        }

        public BasicSearchParametersWithExpectation(final String testDescription, final SearchParameters searchParameters, final int expectedCount) {
            this.testDescription = testDescription;
            this.searchParameters = searchParameters;
            this.expectedCount = expectedCount;
        }

        public SearchParameters getSearchParameters() {
            return searchParameters;
        }

        public void setSearchParameters(final SearchParameters searchParameters) {
            this.searchParameters = searchParameters;
        }

        public int getExpectedCount() {
            return expectedCount;
        }

        public void setExpectedCount(final int expectedCount) {
            this.expectedCount = expectedCount;
        }

        public String getTestDescription() {
            return testDescription;
        }

        public void setTestDescription(final String testDescription) {
            this.testDescription = testDescription;
        }
    }
}
