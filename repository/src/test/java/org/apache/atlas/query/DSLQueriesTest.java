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
package org.apache.atlas.query;

import org.apache.atlas.TestModules;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.graph.GraphSandboxUtil.useLocalSolr;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Guice(modules = TestModules.TestOnlyModule.class)
public class DSLQueriesTest extends BasicTestSetup {
    private static final Logger LOG = LoggerFactory.getLogger(DSLQueriesTest.class);

    private final int DEFAULT_LIMIT = 25;
    @Inject
    private EntityDiscoveryService discoveryService;

    @BeforeClass
    public void setup() throws Exception {
        LocalSolrRunner.start();
        setupTestData();

        pollForData();
    }

    private void pollForData() throws InterruptedException {
        Object[][] basicVerificationQueries = new Object[][] {
                {"hive_db", 3},
                {"hive_process", 7},
                {"hive_table", 10},
                {"hive_column", 17},
                {"hive_storagedesc", 1},
                {"Manager", 2},
                {"Employee", 4},
        };

        int pollingAttempts = 5;
        int pollingBackoff  = 0; // in msecs

        boolean success;

        for (int attempt = 0; attempt < pollingAttempts; attempt++, pollingBackoff += attempt * 5000) {
            LOG.debug("Polling -- Attempt {}, Backoff {}", attempt, pollingBackoff);

            success = false;
            for (Object[] verificationQuery : basicVerificationQueries) {
                String query = (String) verificationQuery[0];
                int expected = (int) verificationQuery[1];

                try {
                    AtlasSearchResult result = discoveryService.searchUsingDslQuery(query, 25, 0);
                    if (result.getEntities() == null || result.getEntities().isEmpty()) {
                        LOG.warn("DSL {} returned no entities", query);
                        success = false;
                    } else if (result.getEntities().size() != expected) {
                        LOG.warn("DSL {} returned unexpected number of entities. Expected {} Actual {}", query, expected, result.getEntities().size());
                        success = false;
                    } else {
                        success = true;
                    }
                } catch (AtlasBaseException e) {
                    LOG.error("Got exception for DSL {}, errorCode: {}", query, e.getAtlasErrorCode());
                    waitOrBailout(pollingAttempts, pollingBackoff, attempt);
                }
            }
            // DSL queries were successful
            if (success) {
                LOG.info("Polling was success");
                break;
            } else {
                waitOrBailout(pollingAttempts, pollingBackoff, attempt);
            }
        }
    }

    private void waitOrBailout(final int pollingAttempts, final int pollingBackoff, final int attempt) throws InterruptedException {
        if (attempt == pollingAttempts - 1) {
            LOG.error("Polling failed after {} attempts", pollingAttempts);
            throw new SkipException("Polling for test data was unsuccessful");
        } else {
            LOG.warn("Waiting for {} before polling again", pollingBackoff);
            Thread.sleep(pollingBackoff);
        }
    }

    @AfterClass
    public void teardown() throws Exception {
        AtlasGraphProvider.cleanup();

        if (useLocalSolr()) {
            LocalSolrRunner.stop();
        }
    }

    @DataProvider(name = "comparisonQueriesProvider")
    private Object[][] comparisonQueriesProvider() {
        return new Object[][] {
                {"Person where (birthday < \"1950-01-01T02:35:58.440Z\" )", 0},
                {"Person where (birthday > \"1975-01-01T02:35:58.440Z\" )", 2},
                {"Person where (birthday >= \"1975-01-01T02:35:58.440Z\" )", 2},
                {"Person where (birthday <= \"1950-01-01T02:35:58.440Z\" )", 0},
                {"Person where (birthday = \"1975-01-01T02:35:58.440Z\" )", 0},
                {"Person where (birthday != \"1975-01-01T02:35:58.440Z\" )", 4},

                {"Person where (hasPets = true)", 2},
                {"Person where (hasPets = false)", 2},
                {"Person where (hasPets != false)", 2},
                {"Person where (hasPets != true)", 2},

                {"Person where (numberOfCars > 0)", 2},
                {"Person where (numberOfCars > 1)", 1},
                {"Person where (numberOfCars >= 1)", 2},
                {"Person where (numberOfCars < 2)", 3},
                {"Person where (numberOfCars <= 2)", 4},
                {"Person where (numberOfCars = 2)", 1},
                {"Person where (numberOfCars != 2)", 3},

                {"Person where (houseNumber > 0)", 2},
                {"Person where (houseNumber > 17)", 1},
                {"Person where (houseNumber >= 17)", 2},
                {"Person where (houseNumber < 153)", 3},
                {"Person where (houseNumber <= 153)", 4},
                {"Person where (houseNumber =  17)", 1},
                {"Person where houseNumber >= 17 or numberOfCars = 2", 2},
                {"Person where (houseNumber != 17)", 3},

                {"Person where (carMileage > 0)", 2},
                {"Person where (carMileage > 13)", 1},
                {"Person where (carMileage >= 13)", 2},
                {"Person where (carMileage < 13364)", 3},
                {"Person where (carMileage <= 13364)", 4},
                {"Person where (carMileage =  13)", 1},
                {"Person where (carMileage != 13)", 3},

                {"Person where (age > 36)", 1},
                {"Person where (age > 49)", 1},
                {"Person where (age >= 49)", 1},
                {"Person where (age < 50)", 3},
                {"Person where (age <= 35)", 2},
                {"Person where (age =  35)", 0},
                {"Person where (age != 35)", 4},
                {String.format("Person where (age <= %f)", Float.MAX_VALUE), 4},
                {"Person where (approximationOfPi > -3.4028235e+38)", 4},
        };
    }

    @Test(dataProvider = "comparisonQueriesProvider")
    public void comparison(String query, int expected) throws AtlasBaseException {
        LOG.debug(query);
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, DEFAULT_LIMIT, 0);
        assertSearchResult(searchResult, expected, query);

        AtlasSearchResult searchResult2 = discoveryService.searchUsingDslQuery(query.replace("where", " "), DEFAULT_LIMIT, 0);
        assertSearchResult(searchResult2, expected, query);
    }

    @DataProvider(name = "basicProvider")
    private Object[][] basicQueries() {
        return new Object[][]{
        };
    }

    @Test(dataProvider = "basicProvider")
    public void basic(String query, int expected) throws AtlasBaseException {
        queryAssert(query, expected, DEFAULT_LIMIT, 0);
        queryAssert(query.replace("where", " "), expected, DEFAULT_LIMIT, 0);
    }

    @DataProvider(name = "systemAttributesProvider")
    private Object[][] systemAttributesQueries() {
        return new Object[][]{
        };
    }

    @Test(dataProvider = "systemAttributesProvider")
    public void systemAttributes(String query, int expected) throws AtlasBaseException {
        queryAssert(query, expected, DEFAULT_LIMIT, 0);
    }

    private void queryAssert(String query, final int expected, final int limit, final int offset) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, limit, offset);
        assertSearchResult(searchResult, expected, query);
    }

    @DataProvider(name = "limitProvider")
    private Object[][] limitQueries() {
        return new Object[][]{
        };
    }

    @Test(dataProvider = "limitProvider")
    public void limit(String query, int expected, int limit, int offset) throws AtlasBaseException {
        queryAssert(query, expected, limit, offset);
        queryAssert(query.replace("where", " "), expected, limit, offset);
    }

    @DataProvider(name = "syntaxProvider")
    private Object[][] syntaxQueries() {
        return new Object[][]{
        };
    }

    @Test(dataProvider = "syntaxProvider")
    public void syntax(String query, int expected) throws AtlasBaseException {
        LOG.debug(query);
        queryAssert(query, expected, DEFAULT_LIMIT, 0);
        queryAssert(query.replace("where", " "), expected, DEFAULT_LIMIT, 0);
    }

    @DataProvider(name = "orderByProvider")
    private Object[][] orderByQueries() {
        return new Object[][]{
        };
    }

    @Test(dataProvider = "orderByProvider")
    public void orderBy(String query, int expected, String orderBy, boolean ascending) throws AtlasBaseException {
        LOG.debug(query);
        queryAssert(query, expected, DEFAULT_LIMIT, 0);
        queryAssert(query.replace("where", " "), expected, DEFAULT_LIMIT, 0);
    }

    @DataProvider(name = "likeQueriesProvider")
    private Object[][] likeQueries() {
        return new Object[][]{
        };
    }

    @Test(dataProvider = "likeQueriesProvider")
    public void likeQueries(String query, int expected) throws AtlasBaseException {
        LOG.debug(query);
        queryAssert(query, expected, DEFAULT_LIMIT, 0);
        queryAssert(query.replace("where", " "), expected, DEFAULT_LIMIT, 0);
    }

    @DataProvider(name = "minMaxCountProvider")
    private Object[][] minMaxCountQueries() {
        return new Object[][]{
                { "from Person select count() as 'count', max(Person.age) as 'max', min(Person.age) as 'min'",
                        new FieldValueValidator()
                                .withFieldNames("'count'", "'max'", "'min'")
                                .withExpectedValues(50, 0, 4) },
                { "from Person select count() as 'count', sum(Person.age) as 'sum'",
                        new FieldValueValidator()
                                .withFieldNames("'count'", "'sum'")
                                .withExpectedValues(4, 86) },
//                { "from hive_db groupby (owner) select min(name) orderby name limit 2 ",
//                        new FieldValueValidator()
//                                .withFieldNames("min(name)")
//                                .withExpectedValues("Logging")
//                                .withExpectedValues("Reporting") },
//                { "from hive_db groupby (owner) select min(name) orderby name desc limit 2 ",
//                        new FieldValueValidator()
//                                .withFieldNames("min(name)")
//                                .withExpectedValues("Reporting")
//                                .withExpectedValues("Sales") }
        };
    }

    @Test(dataProvider = "minMaxCountProvider")
    public void minMaxCount(String query, FieldValueValidator fv) throws AtlasBaseException {
        LOG.debug(query);
        queryAssert(query, fv);
        queryAssert(query.replace("where", " "), fv);
    }

    @DataProvider(name = "errorQueriesProvider")
    private Object[][] errorQueries() {
        return new Object[][]{
                {"`isa`"}, // Tag doesn't exist in the test data
                {"PIII"},  // same as above
                {"DBBB as d select d"}, // same as above
        };
    }

    @Test(dataProvider = "errorQueriesProvider", expectedExceptions = { AtlasBaseException.class })
    public void errorQueries(String query) throws AtlasBaseException {
        LOG.debug(query);
        discoveryService.searchUsingDslQuery(query, DEFAULT_LIMIT, 0);
    }

    private void queryAssert(String query, FieldValueValidator fv) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, DEFAULT_LIMIT, 0);
        assertSearchResult(searchResult, fv);
    }

    private void assertSearchResult(AtlasSearchResult searchResult, FieldValueValidator expected) {
        assertNotNull(searchResult);
        assertNull(searchResult.getEntities());

        assertEquals(searchResult.getAttributes().getName().size(), expected.getFieldNamesCount());
        for (int i = 0; i < searchResult.getAttributes().getName().size(); i++) {
            String s = searchResult.getAttributes().getName().get(i);
            assertEquals(s, expected.fieldNames[i]);
        }

        assertEquals(searchResult.getAttributes().getValues().size(), expected.values.size());
    }

    private void assertSearchResult(AtlasSearchResult searchResult, int expected, String query) {
        assertNotNull(searchResult);
        if(expected == 0) {
            assertTrue(searchResult.getAttributes() == null || CollectionUtils.isEmpty(searchResult.getAttributes().getValues()));
            assertNull(searchResult.getEntities(), query);
        } else if(searchResult.getEntities() != null) {
            assertEquals(searchResult.getEntities().size(), expected, query);
        } else {
            assertNotNull(searchResult.getAttributes());
            assertNotNull(searchResult.getAttributes().getValues());
            assertEquals(searchResult.getAttributes().getValues().size(), expected, query);
        }
    }

    private class FieldValueValidator {
        class ResultObject {
            Map<String, Object> fieldValues = new HashMap<>();

            public void setFieldValue(String string, Object object) {
                fieldValues.put(string, object);
            }
        }

        private String[] fieldNames;
        private List<ResultObject> values = new ArrayList<>();

        public FieldValueValidator withFieldNames(String... fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public FieldValueValidator withExpectedValues(Object... values) {
            ResultObject obj = new ResultObject();
            for (int i = 0; i < fieldNames.length; i++) {
                obj.setFieldValue(fieldNames[i], values[i]);
            }

            this.values.add(obj);
            return this;
        }

        public int getFieldNamesCount() {
            return (fieldNames != null) ? fieldNames.length : 0;
        }
    }
}
