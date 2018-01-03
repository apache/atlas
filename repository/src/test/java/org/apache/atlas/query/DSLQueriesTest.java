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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.atlas.TestModules;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.query.antlr4.AtlasDSLLexer;
import org.apache.atlas.query.antlr4.AtlasDSLParser;
import org.apache.atlas.runner.LocalSolrRunner;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class DSLQueriesTest extends BasicTestSetup {
    @Inject
    private EntityDiscoveryService discoveryService;

    @BeforeClass
    public void setup() throws Exception {
        LocalSolrRunner.start();
        setupTestData();
    }

    @AfterClass
    public void teardown() throws Exception {
        LocalSolrRunner.stop();
    }

    @DataProvider(name = "comparisonQueriesProvider")
    private Object[][] comparisonQueriesProvider() {
        return new Object[][] {
                {"Person where (birthday < \"1950-01-01T02:35:58.440Z\" )", 0},
                {"Person where (birthday > \"1975-01-01T02:35:58.440Z\" )", 2},
                {"Person where (birthday >= \"1975-01-01T02:35:58.440Z\" )", 2},
                {"Person where (birthday <= \"1950-01-01T02:35:58.440Z\" )", 0},
                {"Person where (birthday = \"1975-01-01T02:35:58.440Z\" )", 0},
                {"Person where (birthday != \"1975-01-01T02:35:58.440Z\" )", 0},

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
                {"Person where (numberOfCars != 2)", 0},

                {"Person where (houseNumber > 0)", 2},
                {"Person where (houseNumber > 17)", 1},
                {"Person where (houseNumber >= 17)", 2},
                {"Person where (houseNumber < 153)", 3},
                {"Person where (houseNumber <= 153)", 4},
                {"Person where (houseNumber =  17)", 1},
                {"Person where (houseNumber != 17)", 0},

                {"Person where (carMileage > 0)", 2},
                {"Person where (carMileage > 13)", 1},
                {"Person where (carMileage >= 13)", 2},
                {"Person where (carMileage < 13364)", 3},
                {"Person where (carMileage <= 13364)", 4},
                {"Person where (carMileage =  13)", 1},
                {"Person where (carMileage != 13)", 0},

                {"Person where (age > 36)", 1},
                {"Person where (age > 49)", 1},
                {"Person where (age >= 49)", 1},
                {"Person where (age < 50)", 3},
                {"Person where (age <= 35)", 2},
                {"Person where (age =  35)", 0},
                {"Person where (age != 35)", 0}
        };
    }

    @Test(dataProvider = "comparisonQueriesProvider")
    public void comparison(String query, int expected) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertSearchResult(searchResult, expected);
    }

    @DataProvider(name = "basicProvider")
    private Object[][] basicQueries() {
        return new Object[][]{
                {"from hive_db", 3},
                {"hive_db", 3},
                {"hive_db where hive_db.name=\"Reporting\"", 1},
                {"hive_db hive_db.name = \"Reporting\"", 3},
                {"hive_db where hive_db.name=\"Reporting\" select name, owner", 1},
                {"hive_db has name", 3},
                {"from hive_table", 10},
                {"hive_table", 10},
                {"hive_table isa Dimension", 3},
                {"hive_column where hive_column isa PII", 4},
                {"hive_column where hive_column isa PII select hive_column.qualifiedName", 4},
                {"hive_column select hive_column.qualifiedName", 17},
                {"hive_column select qualifiedName", 17},
                {"hive_column where hive_column.name=\"customer_id\"", 2},
                {"from hive_table select hive_table.qualifiedName", 10},
                {"hive_db where (name = \"Reporting\")", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1", 1},
                {"hive_db where hive_db is JdbcAccess", 0},
                {"hive_db where hive_db has name", 3},
                {"hive_db as db1 hive_table where (db1.name = \"Reporting\")", 0},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1", 1},
                {"Dimension", 5},
                {"JdbcAccess", 2},
                {"ETL", 5},
                {"Metric", 5},
                {"PII", 4},
                {"`Log Data`", 3},
                {"`isa`", 0},
                {"hive_table as t, sd, hive_column as c where t.name=\"sales_fact\" select c.name as colName, c.dataType as colType", 0},
                {"hive_table where name='sales_fact', db where name='Sales'", 1},
                {"hive_table where name='sales_fact', db where name='Reporting'", 0},
                {"DataSet where name='sales_fact'", 1},
                {"Asset where name='sales_fact'", 1}
        };
    }

    @Test(dataProvider = "basicProvider")
    public void basic(String query, int expected) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertSearchResult(searchResult, expected);
    }

    @DataProvider(name = "limitProvider")
    private Object[][] limitQueries() {
        return new Object[][]{
                {"hive_column", 17, 40, 0},
                {"hive_column limit 10", 10, 50, 0},
                {"hive_column select hive_column.qualifiedName limit 10", 10, 5, 0},
                {"hive_column select hive_column.qualifiedName limit 40 offset 10", 7, 40, 0},
                {"hive_db where name = 'Reporting' limit 10 offset 0", 1, 40, 0},
                {"hive_table where db.name = 'Reporting' limit 10", 4, 1, 0},
        };
    }

    @Test(dataProvider = "limitProvider")
    public void limit(String query, int expected, int limit, int offset) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, limit, offset);
        assertSearchResult(searchResult, expected);
    }

    @DataProvider(name = "syntaxVerifierProvider")
    private Object[][] syntaxVerifierQueries() {
        return new Object[][]{
                {"hive_column  limit 10 ", 10},
                {"hive_column select hive_column.qualifiedName limit 10 ", 10},
                {"from hive_db", 3},
                {"from hive_db limit 2", 2},
                {"from hive_db limit 2 offset 0", 2},
                {"from hive_db limit 2 offset 1", 2},
                {"from hive_db limit 3 offset 1", 2},
                {"hive_db", 3},
                {"hive_db where hive_db.name=\"Reporting\"", 1},
                {"hive_db where hive_db.name=\"Reporting\" or hive_db.name=\"Sales\" or hive_db.name=\"Logging\" limit 1 offset 1", 1},
                {"hive_db where hive_db.name=\"Reporting\" or hive_db.name=\"Sales\" or hive_db.name=\"Logging\" limit 1 offset 2", 1},
                {"hive_db where hive_db.name=\"Reporting\" or hive_db.name=\"Sales\" or hive_db.name=\"Logging\" limit 2 offset 1", 2},
                {"hive_db where hive_db.name=\"Reporting\" limit 10 ", 1},
                {"hive_db where hive_db.name=\"Reporting\" select name, owner", 1},
                {"hive_db has name", 3},
                {"hive_db has name limit 2 offset 0", 2},
                {"hive_db has name limit 2 offset 1", 2},
                {"hive_db has name limit 10 offset 1", 2},
                {"hive_db has name limit 10 offset 0", 3},

                {"from hive_table", 10},
                {"from hive_table limit 5", 5},
                {"from hive_table limit 5 offset 5", 5},

                {"hive_table", 10},
                {"hive_table limit 5", 5},
                {"hive_table limit 5 offset 5", 5},

                {"hive_table isa Dimension", 3},
                {"hive_table isa Dimension limit 2", 2},
                {"hive_table isa Dimension limit 2 offset 0", 2},
                {"hive_table isa Dimension limit 2 offset 1", 2},
                {"hive_table isa Dimension limit 3 offset 1", 2},
                  {"hive_table where db.name='Sales' and db.clusterName='cl1'", 4},

                {"hive_column where hive_column isa PII", 4},
                {"hive_column where hive_column isa PII limit 5", 4},
                {"hive_column where hive_column isa PII limit 5 offset 1", 3},
                {"hive_column where hive_column isa PII limit 5 offset 5", 0},

                {"hive_column select hive_column.qualifiedName", 17},
                {"hive_column select hive_column.qualifiedName limit 5", 5},
                {"hive_column select hive_column.qualifiedName limit 5 offset 36", 0},

                {"hive_column select qualifiedName", 17},
                {"hive_column select qualifiedName limit 5", 5},
                {"hive_column select qualifiedName limit 5 offset 36 ", 0},

                {"hive_column where hive_column.name=\"customer_id\"", 2},
                {"hive_column where hive_column.name=\"customer_id\" limit 2", 2},
                {"hive_column where hive_column.name=\"customer_id\" limit 2 offset 1", 1},
                {"hive_column where hive_column.name=\"customer_id\" limit 10 offset 3", 0},

                {"from hive_table select hive_table.name", 10},
                {"from hive_table select hive_table.name limit 5", 5},
                {"from hive_table select hive_table.name limit 5 offset 5", 5},

                {"hive_db where (name = \"Reporting\")", 1},
                {"hive_db where (name = \"Reporting\") limit 10", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1 limit 10", 1},
                {"hive_db where hive_db is JdbcAccess", 0}, //Not supposed to work
                {"hive_db where hive_db has name", 3},
                {"hive_db where hive_db has name limit 5", 3},
                {"hive_db where hive_db has name limit 2 offset 0", 2},
                {"hive_db where hive_db has name limit 2 offset 1", 2},

                {"hive_db as db1 hive_table where (db1.name = \"Reporting\")", 0},

                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 limit 10", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 limit 10 offset 0", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 limit 10 offset 5", 0},

                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 10 offset 0", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 10 offset 1", 0},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 10", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 0 offset 1", 0},

                {"hive_table where name='sales_fact', db where name='Sales'", 1},
                {"hive_table where name='sales_fact', db where name='Sales' limit 10", 1},
                {"hive_table where name='sales_fact', db where name='Sales' limit 10 offset 1", 0},
                {"hive_table where name='sales_fact', db where name='Reporting'", 0},
                {"hive_table where name='sales_fact', db where name='Reporting' limit 10", 0},
                {"hive_table where name='sales_fact', db where name='Reporting' limit 10 offset 1", 0},

                {"hive_db as d where owner = ['John ETL', 'Jane BI']", 2},
                {"hive_db as d where owner = ['John ETL', 'Jane BI'] limit 10", 2},
                {"hive_db as d where owner = ['John ETL', 'Jane BI'] limit 10 offset 1", 1},

        };
    }

    @Test(dataProvider = "syntaxVerifierProvider")
    public void syntax(String query, int expected) throws AtlasBaseException {
         AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertSearchResult(searchResult, expected);
    }

    @DataProvider(name = "orderByProvider")
    private Object[][] orderByQueries() {
        return new Object[][]{
                {"from hive_db as h orderby h.owner limit 3", 3, "owner", true},
                {"hive_column as c select c.qualifiedName orderby hive_column.qualifiedName ", 17, "c.qualifiedName", true},
                {"hive_column as c select c.qualifiedName orderby hive_column.qualifiedName limit 5", 5, "c.qualifiedName", true},
                {"hive_column as c select c.qualifiedName orderby hive_column.qualifiedName desc limit 5", 5, "c.qualifiedName", false},

                {"from hive_db orderby hive_db.owner limit 3", 3, "owner", true},
                {"hive_column select hive_column.qualifiedName orderby hive_column.qualifiedName ", 17, "hive_column.qualifiedName", true},
                {"hive_column select hive_column.qualifiedName orderby hive_column.qualifiedName limit 5", 5, "hive_column.qualifiedName", true},
                {"hive_column select hive_column.qualifiedName orderby hive_column.qualifiedName desc limit 5", 5, "hive_column.qualifiedName", false},

                {"from hive_db orderby owner limit 3", 3, "owner", true},
                {"hive_column select hive_column.qualifiedName orderby qualifiedName ", 17, "hive_column.qualifiedName", true},
                {"hive_column select hive_column.qualifiedName orderby qualifiedName limit 5", 5, "hive_column.qualifiedName", true},
                {"hive_column select hive_column.qualifiedName orderby qualifiedName desc limit 5", 5, "hive_column.qualifiedName", false},

                {"from hive_db orderby hive_db.owner limit 3", 3, "owner", true},
                {"hive_db where hive_db.name=\"Reporting\" orderby owner", 1, "owner", true},

                {"hive_db where hive_db.name=\"Reporting\" orderby hive_db.owner limit 10 ", 1, "owner", true},
                {"hive_db where hive_db.name=\"Reporting\" select name, owner orderby hive_db.name ", 1, "name", true},
                {"hive_db has name orderby hive_db.owner limit 10 offset 0", 3, "owner", true},

                {"from hive_table select hive_table.owner orderby hive_table.owner", 10, "hive_table.owner", true},
                {"from hive_table select hive_table.owner orderby hive_table.owner limit 8", 8, "hive_table.owner", true},

                {"hive_table orderby hive_table.name", 10, "name", true},

                {"hive_table orderby hive_table.owner", 10, "owner", true},
                {"hive_table orderby hive_table.owner limit 8", 8, "owner", true},
                {"hive_table orderby hive_table.owner limit 8 offset 0", 8, "owner", true},
                {"hive_table orderby hive_table.owner desc limit 8 offset 0", 8, "owner", false},

                {"hive_column select hive_column.qualifiedName orderby hive_column.qualifiedName ", 17, "hive_column.qualifiedName", true},
                {"hive_column select hive_column.qualifiedName orderby hive_column.qualifiedName limit 5", 5, "hive_column.qualifiedName", true},
                {"hive_column select hive_column.qualifiedName orderby hive_column.qualifiedName desc limit 5", 5, "hive_column.qualifiedName", false},

                {"hive_column select hive_column.qualifiedName orderby hive_column.qualifiedName limit 5 offset 2", 5, "hive_column.qualifiedName", true},

                {"hive_column select qualifiedName orderby hive_column.qualifiedName", 17, "qualifiedName", true},
                {"hive_column select qualifiedName orderby hive_column.qualifiedName limit 5", 5, "qualifiedName", true},
                {"hive_column select qualifiedName orderby hive_column.qualifiedName desc", 17, "qualifiedName", false},

                {"hive_column where hive_column.name=\"customer_id\" orderby hive_column.name", 2, "name", true},
                {"hive_column where hive_column.name=\"customer_id\" orderby hive_column.name limit 2", 2, "name", true},
                {"hive_column where hive_column.name=\"customer_id\" orderby hive_column.name limit 2 offset 1", 1, "name", true},

                {"from hive_table select owner orderby hive_table.owner",10, "owner", true},
                {"from hive_table select owner orderby hive_table.owner limit 5", 5, "owner", true},
                {"from hive_table select owner orderby hive_table.owner desc limit 5", 5, "owner", false},
                {"from hive_table select owner orderby hive_table.owner limit 5 offset 5", 5, "owner", true},

                {"hive_db where (name = \"Reporting\") orderby hive_db.name", 1, "name", true},
                {"hive_db where (name = \"Reporting\") orderby hive_db.name limit 10", 1, "name", true},
                {"hive_db where hive_db has name orderby hive_db.owner", 3, "owner", true},
                {"hive_db where hive_db has name orderby hive_db.owner limit 5", 3, "owner", true},
                {"hive_db where hive_db has name orderby hive_db.owner limit 2 offset 0", 2, "owner", true},
                {"hive_db where hive_db has name orderby hive_db.owner limit 2 offset 1", 2, "owner", true},

                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby createTime ", 1, "_col_1", true},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby createTime limit 10 ", 1, "_col_1", true},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby createTime limit 10 offset 0", 1, "_col_1", true},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby createTime limit 10 offset 5", 0, "_col_1", true},

                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby name ", 1, "_col_0", true},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby name limit 10 offset 0", 1, "_col_0", true},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby name limit 10 offset 1", 0, "_col_0", true},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby name limit 10", 1, "_col_0", true},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby name limit 0 offset 1", 0, "_col_0", true},
        };
    }

    @Test(dataProvider = "orderByProvider")
    public void orderBy(String query, int expected, String orderBy, boolean ascending) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertSearchResult(searchResult, expected);
    }

    @DataProvider(name = "likeQueriesProvider")
    private Object[][] likeQueries() {
        return new Object[][]{
                {"hive_table where name like \"sa?es*\"", 3},
                {"hive_db where name like \"R*\"", 1},
                {"hive_db where hive_db.name like \"R???rt?*\" or hive_db.name like \"S?l?s\" or hive_db.name like\"Log*\"", 3},
                {"hive_db where hive_db.name like \"R???rt?*\" and hive_db.name like \"S?l?s\" and hive_db.name like\"Log*\"", 0},
                {"hive_table where name like 'sales*', db where name like 'Sa?es'", 1},
                {"hive_table where name like 'sales*' and db.name like 'Sa?es'", 1},
                {"hive_table where db.name like \"Sa*\"", 4},
                {"hive_table where db.name like \"Sa*\" and name like \"*dim\"", 3},
        };
    }

    @DataProvider(name = "minMaxCountProvider")
    private Object[][] minMaxCountQueries() {
        return new Object[][]{
                {"from hive_db groupby (owner) select count() ",
                        new FieldValueValidator()
                                .withFieldNames("count()")
                                .withExpectedValues(1)
                                .withExpectedValues(1)
                                .withExpectedValues(1) },
                                     // FIXME
//                { "from hive_db groupby (owner, name) select Asset.owner, Asset.name, count()",
//                        new FieldValueValidator()
//                                .withFieldNames("Asset.owner", "Asset.name", "count()")
//                                .withExpectedValues("Jane BI", "Reporting", 1)
//                                .withExpectedValues("Tim ETL", "Logging", 1)
//                                .withExpectedValues("John ETL", "Sales", 1) },
                { "from hive_db groupby (owner) select count() ",
                        new FieldValueValidator()
                                .withFieldNames("count()").
                                withExpectedValues(1).
                                withExpectedValues(1).
                                withExpectedValues(1) },
                { "from hive_db groupby (owner) select Asset.owner, count() ",
                        new FieldValueValidator()
                                .withFieldNames("Asset.owner", "count()")
                                .withExpectedValues("Jane BI", 1)
                                .withExpectedValues("Tim ETL", 1)
                                .withExpectedValues("John ETL", 1) },
                { "from hive_db groupby (owner) select count() ",
                        new FieldValueValidator()
                                .withFieldNames("count()")
                                .withExpectedValues(1)
                                .withExpectedValues(1)
                                .withExpectedValues(1) },

                { "from hive_db groupby (owner) select Asset.owner, count() ",
                        new FieldValueValidator()
                                .withFieldNames("Asset.owner", "count()")
                                .withExpectedValues("Jane BI", 1)
                                .withExpectedValues("Tim ETL", 1)
                                .withExpectedValues("John ETL", 1) },

                { "from hive_db groupby (owner) select Asset.owner, max(Asset.name) ",
                        new FieldValueValidator()
                                .withFieldNames("Asset.owner", "max(Asset.name)")
                                .withExpectedValues("Tim ETL", "Logging")
                                .withExpectedValues("Jane BI", "Reporting")
                                .withExpectedValues("John ETL", "Sales") },

                { "from hive_db groupby (owner) select max(Asset.name) ",
                        new FieldValueValidator()
                                .withFieldNames("max(Asset.name)")
                                .withExpectedValues("Logging")
                                .withExpectedValues("Reporting")
                                .withExpectedValues("Sales") },

                { "from hive_db groupby (owner) select owner, Asset.name, min(Asset.name)  ",
                        new FieldValueValidator()
                                .withFieldNames("owner", "Asset.name", "min(Asset.name)")
                                .withExpectedValues("Tim ETL", "Logging", "Logging")
                                .withExpectedValues("Jane BI", "Reporting", "Reporting")
                                .withExpectedValues("John ETL", "Sales", "Sales") },

                { "from hive_db groupby (owner) select owner, min(Asset.name)  ",
                        new FieldValueValidator()
                                .withFieldNames("owner", "min(Asset.name)")
                                .withExpectedValues("Tim ETL", "Logging")
                                .withExpectedValues("Jane BI", "Reporting")
                                .withExpectedValues("John ETL", "Sales") },

                { "from hive_db groupby (owner) select min(name)  ",
                        new FieldValueValidator()
                                .withFieldNames("min(name)")
                                .withExpectedValues("Reporting")
                                .withExpectedValues("Logging")
                                .withExpectedValues("Sales") },
                { "from hive_db groupby (owner) select min('name') ",
                        new FieldValueValidator()
                                .withFieldNames("min('name')")
                                .withExpectedValues("name")
                                .withExpectedValues("name")
                                .withExpectedValues("name") },
                { "from hive_db select count() ",
                        new FieldValueValidator()
                                .withFieldNames("count()")
                                .withExpectedValues(3) },
                { "from Person select count() as 'count', max(Person.age) as 'max', min(Person.age) as 'min'",
                        new FieldValueValidator()
                                .withFieldNames("'count'", "'max'", "'min'")
                                .withExpectedValues(50, 0, 4) },
                { "from Person select count() as 'count', sum(Person.age) as 'sum'",
                        new FieldValueValidator()
                                .withFieldNames("'count'", "'sum'")
                                .withExpectedValues(4, 86) },
                // tests to ensure that group by works with order by and limit
                                     // FIXME:
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
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertSearchResult(searchResult, fv);
    }

    @Test(dataProvider = "likeQueriesProvider")
    public void likeQueries(String query, int expected) throws AtlasBaseException {
        AtlasSearchResult searchResult = discoveryService.searchUsingDslQuery(query, 25, 0);
        assertSearchResult(searchResult, expected);
    }

    @Test
    public void classification() {
        String expected = "g.V().has('__traitNames', within('PII')).limit(25).toList()";
        verify("PII", expected);
    }

    @Test
    public void dimension() {
        String expected = "g.V().has('__typeName', 'hive_table').has('__traitNames', within('Dimension')).limit(25).toList()";
        verify("hive_table isa Dimension", expected);
        verify("hive_table is Dimension", expected);
        verify("hive_table where hive_table is Dimension", expected);
        // Not supported since it requires two singleSrcQuery, one for isa clause other for where clause
//        verify("Table isa Dimension where name = 'sales'",
//                "g.V().has('__typeName', 'Table').has('__traitNames', within('Dimension')).has('Table.name', eq('sales')).limit(25).toList()");
    }

    @Test
    public void fromDB() {
        verify("from hive_db", "g.V().has('__typeName', 'hive_db').limit(25).toList()");
        verify("from hive_db limit 10", "g.V().has('__typeName', 'hive_db').limit(10).toList()");
        verify("hive_db limit 10", "g.V().has('__typeName', 'hive_db').limit(10).toList()");
    }

    @Test
    public void hasName() {
        String expected = "g.V().has('__typeName', within('DataSet','hive_column_lineage','Infrastructure','Asset','Process','hive_table','hive_column','hive_db','hive_process')).has('Asset.name').limit(25).toList()";
        verify("Asset has name", expected);
        verify("Asset where Asset has name", expected);
    }

    @Test
    public void simpleAlias() {
        verify("Asset as a", "g.V().has('__typeName', within('DataSet','hive_column_lineage','Infrastructure','Asset','Process','hive_table','hive_column','hive_db','hive_process')).as('a').limit(25).toList()");
    }

    @Test
    public void selectQueries() {
        String expected = "def f(r){ t=[['d.name','d.owner']];  r.each({t.add([it.value('Asset.name'),it.value('Asset.owner')])}); t.unique(); }; " +
                                  "f(g.V().has('__typeName', within('DataSet','hive_column_lineage','Infrastructure','Asset','Process','hive_table','hive_column','hive_db','hive_process')).as('d')";
        verify("Asset as d select d.name, d.owner", expected + ".limit(25).toList())");
        verify("Asset as d select d.name, d.owner limit 10", expected + ".limit(10).toList())");
    }

    @Test
    public void tableSelectColumns() {
        String exMain = "g.V().has('__typeName', 'hive_table').out('__hive_table.columns').limit(10).toList()";
        String exSel = "def f(r){ r };";
        String exSel1 = "def f(r){ t=[['db.name']];  r.each({t.add([it.value('Asset.name')])}); t.unique(); };";
        verify("hive_table select columns limit 10", getExpected(exSel, exMain));

        String exMain2 = "g.V().has('__typeName', 'hive_table').out('__hive_table.db').limit(25).toList()";
        verify("hive_table select db", getExpected(exSel, exMain2));

        String exMain3 = "g.V().has('__typeName', 'hive_table').out('__hive_table.db').limit(25).toList()";
        verify("hive_table select db.name", getExpected(exSel1, exMain3));

    }

    @Test(enabled = false)
    public void SelectLimit() {
        verify("from hive_db limit 5", "g.V().has('__typeName', 'hive_db').limit(5).toList()");
        verify("from hive_db limit 5 offset 2", "g.V().has('__typeName', 'hive_db').range(2, 7).toList()");
    }

    @Test
    public void orderBy() {
        String expected = "g.V().has('__typeName', 'hive_db').order().by('Asset.name').limit(25).toList()";
        verify("hive_db orderby name", expected);
        verify("from hive_db orderby name", expected);
        verify("from hive_db as d orderby d.owner limit 3", "g.V().has('__typeName', 'hive_db').as('d').order().by('Asset.owner').limit(3).toList()");
        verify("hive_db as d orderby d.owner limit 3", "g.V().has('__typeName', 'hive_db').as('d').order().by('Asset.owner').limit(3).toList()");


        String exSel = "def f(r){ t=[['d.name','d.owner']];  r.each({t.add([it.value('Asset.name'),it.value('Asset.owner')])}); t.unique(); };";
        String exMain = "g.V().has('__typeName', 'hive_db').as('d').order().by('Asset.owner').limit(25).toList()";
        verify("hive_db as d select d.name, d.owner orderby (d.owner) limit 25", getExpected(exSel, exMain));

        String exMain2 = "g.V().has('__typeName', 'hive_table').and(__.has('Asset.name', eq(\"sales_fact\")),__.has('hive_table.createTime', gt('1388563200000'))).order().by('hive_table.createTime').limit(25).toList()";
        String exSel2 = "def f(r){ t=[['_col_0','_col_1']];  r.each({t.add([it.value('Asset.name'),it.value('hive_table.createTime')])}); t.unique(); };";
        verify("hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby _col_1",
               getExpected(exSel2, exMain2));
    }

    @Test
    public void fromDBOrderByNameDesc() {
        verify("from hive_db orderby name DESC", "g.V().has('__typeName', 'hive_db').order().by('Asset.name', decr).limit(25).toList()");
    }

    @Test
    public void fromDBSelect() {
        String expected = "def f(r){ t=[['Asset.name','Asset.owner']];  r.each({t.add([it.value('Asset.name'),it.value('Asset.owner')])}); t.unique(); };" +
                                  " f(g.V().has('__typeName', 'hive_db').limit(25).toList())";
        verify("from hive_db select Asset.name, Asset.owner", expected);
        expected = "def f(r){ t=[['min(name)','max(owner)']]; " +
                           "def min=r.min({it.value('Asset.name')}).value('Asset.name'); " +
                           "def max=r.max({it.value('Asset.owner')}).value('Asset.owner'); " +
                           "t.add([min,max]); t;}; " +
                           "f(g.V().has('__typeName', 'hive_db').limit(25).toList())";
        verify("hive_db select min(name), max(owner)", expected);
        expected = "def f(r){ t=[['owner','min(name)','max(owner)']]; " +
                           "def min=r.min({it.value('Asset.name')}).value('Asset.name'); " +
                           "def max=r.max({it.value('Asset.owner')}).value('Asset.owner'); " +
                           "r.each({t.add([it.value('Asset.owner'),min,max])}); t.unique(); }; " +
                           "f(g.V().has('__typeName', 'hive_db').limit(25).toList())";
        verify("hive_db select owner, min(name), max(owner)", expected);
    }

    @Test
    public void fromDBGroupBy() {
        verify("from hive_db groupby (Asset.owner)", "g.V().has('__typeName', 'hive_db').group().by('Asset.owner').limit(25).toList()");
    }

    @Test
    public void whereClauseTextContains() {
        String exMain = "g.V().has('__typeName', 'hive_db').has('Asset.name', eq(\"Reporting\")).limit(25).toList()";
        String exSel = "def f(r){ t=[['name','owner']];  r.each({t.add([it.value('Asset.name'),it.value('Asset.owner')])}); t.unique(); };";
        verify("from hive_db where name = \"Reporting\" select name, owner", getExpected(exSel, exMain));
        verify("from hive_db where (name = \"Reporting\") select name, owner", getExpected(exSel, exMain));
        verify("hive_table where Asset.name like \"Tab*\"",
               "g.V().has('__typeName', 'hive_table').has('Asset.name', org.janusgraph.core.attribute.Text.textRegex(\"Tab.*\")).limit(25).toList()");
        verify("from hive_table where (db.name = \"Reporting\")",
               "g.V().has('__typeName', 'hive_table').out('__hive_table.db').has('Asset.name', eq(\"Reporting\")).dedup().in('__hive_table.db').limit(25).toList()");
    }

    @Test
    public void whereClauseWithAsTextContains() {
        String exSel = "def f(r){ t=[['t.name','t.owner']];  r.each({t.add([it.value('Asset.name'),it.value('Asset.owner')])}); t.unique(); };";
        String exMain = "g.V().has('__typeName', 'hive_table').as('t').has('Asset.name', eq(\"testtable_1\")).limit(25).toList()";
        verify("hive_table as t where t.name = \"testtable_1\" select t.name, t.owner)", getExpected(exSel, exMain));
    }

    @Test
    public void whereClauseWithDateCompare() {
        String exSel = "def f(r){ t=[['t.name','t.owner']];  r.each({t.add([it.value('Asset.name'),it.value('Asset.owner')])}); t.unique(); };";
        String exMain = "g.V().has('__typeName', 'hive_table').as('t').has('hive_table.createTime', eq('1513046158440')).limit(25).toList()";
        verify("hive_table as t where t.createTime = \"2017-12-12T02:35:58.440Z\" select t.name, t.owner)", getExpected(exSel, exMain));
    }

    @Test
    public void subType() {
        String exMain = "g.V().has('__typeName', within('DataSet','hive_column_lineage','Infrastructure','Asset','Process','hive_table','hive_column','hive_db','hive_process')).limit(25).toList()";
        String exSel = "def f(r){ t=[['name','owner']];  r.each({t.add([it.value('Asset.name'),it.value('Asset.owner')])}); t.unique(); };";

        verify("Asset select name, owner", getExpected(exSel, exMain));
    }

    @Test
    public void TraitWithSpace() {
        verify("`Log Data`", "g.V().has('__traitNames', within('Log Data')).limit(25).toList()");
    }

    @Test
    public void nestedQueries() {
        verify("hive_table where name=\"sales_fact\" or name=\"testtable_1\"",
               "g.V().has('__typeName', 'hive_table').or(__.has('Asset.name', eq(\"sales_fact\")),__.has('Asset.name', eq(\"testtable_1\"))).limit(25).toList()");
        verify("hive_table where name=\"sales_fact\" and name=\"testtable_1\"",
               "g.V().has('__typeName', 'hive_table').and(__.has('Asset.name', eq(\"sales_fact\")),__.has('Asset.name', eq(\"testtable_1\"))).limit(25).toList()");
        verify("hive_table where name=\"sales_fact\" or name=\"testtable_1\" or name=\"testtable_2\"",
               "g.V().has('__typeName', 'hive_table')" +
                       ".or(" +
                       "__.has('Asset.name', eq(\"sales_fact\"))," +
                       "__.has('Asset.name', eq(\"testtable_1\"))," +
                       "__.has('Asset.name', eq(\"testtable_2\"))" +
                       ").limit(25).toList()");
        verify("hive_table where name=\"sales_fact\" and name=\"testtable_1\" and name=\"testtable_2\"",
               "g.V().has('__typeName', 'hive_table')" +
                       ".and(" +
                       "__.has('Asset.name', eq(\"sales_fact\"))," +
                       "__.has('Asset.name', eq(\"testtable_1\"))," +
                       "__.has('Asset.name', eq(\"testtable_2\"))" +
                       ").limit(25).toList()");
        verify("hive_table where (name=\"sales_fact\" or name=\"testtable_1\") and name=\"testtable_2\"",
               "g.V().has('__typeName', 'hive_table')" +
                       ".and(" +
                       "__.or(" +
                       "__.has('Asset.name', eq(\"sales_fact\"))," +
                       "__.has('Asset.name', eq(\"testtable_1\"))" +
                       ")," +
                       "__.has('Asset.name', eq(\"testtable_2\")))" +
                       ".limit(25).toList()");
        verify("hive_table where name=\"sales_fact\" or (name=\"testtable_1\" and name=\"testtable_2\")",
               "g.V().has('__typeName', 'hive_table')" +
                       ".or(" +
                       "__.has('Asset.name', eq(\"sales_fact\"))," +
                       "__.and(" +
                       "__.has('Asset.name', eq(\"testtable_1\"))," +
                       "__.has('Asset.name', eq(\"testtable_2\")))" +
                       ")" +
                       ".limit(25).toList()");
        verify("hive_table where name=\"sales_fact\" or name=\"testtable_1\" and name=\"testtable_2\"",
               "g.V().has('__typeName', 'hive_table')" +
                       ".and(" +
                       "__.or(" +
                       "__.has('Asset.name', eq(\"sales_fact\"))," +
                       "__.has('Asset.name', eq(\"testtable_1\"))" +
                       ")," +
                       "__.has('Asset.name', eq(\"testtable_2\")))" +
                       ".limit(25).toList()");
        verify("hive_table where (name=\"sales_fact\" and owner=\"Joe\") OR (name=\"sales_fact_daily_mv\" and owner=\"Joe BI\")",
               "g.V().has('__typeName', 'hive_table')" +
                       ".or(" +
                       "__.and(" +
                       "__.has('Asset.name', eq(\"sales_fact\"))," +
                       "__.has('Asset.owner', eq(\"Joe\"))" +
                       ")," +
                       "__.and(" +
                       "__.has('Asset.name', eq(\"sales_fact_daily_mv\"))," +
                       "__.has('Asset.owner', eq(\"Joe BI\"))" +
                       "))" +
                       ".limit(25).toList()");
        verify("hive_table where owner=\"hdfs\" or ((name=\"testtable_1\" or name=\"testtable_2\") and createTime < \"2017-12-12T02:35:58.440Z\")",
               "g.V().has('__typeName', 'hive_table').or(__.has('Asset.owner', eq(\"hdfs\")),__.and(__.or(__.has('Asset.name', eq(\"testtable_1\")),__.has('Asset.name', eq(\"testtable_2\"))),__.has('hive_table.createTime', lt('1513046158440')))).limit(25).toList()");
        verify("hive_table where hive_table.name='Reporting' and hive_table.createTime < '2017-12-12T02:35:58.440Z'",
               "g.V().has('__typeName', 'hive_table').and(__.has('Asset.name', eq('Reporting')),__.has('hive_table.createTime', lt('1513046158440'))).limit(25).toList()");
        verify("hive_table where db.name='Sales' and db.clusterName='cl1'",
               "g.V().has('__typeName', 'hive_table').and(__.out('__hive_table.db').has('Asset.name', eq('Sales')).dedup().in('__hive_table.db'),__.out('__hive_table.db').has('hive_db.clusterName', eq('cl1')).dedup().in('__hive_table.db')).limit(25).toList()");
    }

    private void verify(String dsl, String expectedGremlin) {
        AtlasDSLParser.QueryContext queryContext = getParsedQuery(dsl);
        String actualGremlin = getGremlinQuery(queryContext);
        assertEquals(actualGremlin, expectedGremlin);
    }

    private String getExpected(String select, String main) {
        return String.format("%s f(%s)", select, main);
    }

    private AtlasDSLParser.QueryContext getParsedQuery(String query) {
        AtlasDSLParser.QueryContext queryContext = null;
        InputStream                 stream       = new ByteArrayInputStream(query.getBytes());
        AtlasDSLLexer               lexer        = null;

        try {
            lexer = new AtlasDSLLexer(CharStreams.fromStream(stream));
        } catch (IOException e) {
            assertTrue(false);
        }

        TokenStream    inputTokenStream = new CommonTokenStream(lexer);
        AtlasDSLParser parser           = new AtlasDSLParser(inputTokenStream);
        queryContext = parser.query();

        assertNotNull(queryContext);
        assertNull(queryContext.exception);

        return queryContext;
    }

    private String getGremlinQuery(AtlasDSLParser.QueryContext queryContext) {
        GremlinQueryComposer gremlinQueryComposer = new GremlinQueryComposer(typeRegistry, new AtlasDSL.QueryMetadata(queryContext));
        DSLVisitor           qv                   = new DSLVisitor(gremlinQueryComposer);
        qv.visit(queryContext);

        String s = gremlinQueryComposer.get();
        assertTrue(StringUtils.isNotEmpty(s));
        return s;
    }



    private void assertSearchResult(AtlasSearchResult searchResult, int expected) {
        assertNotNull(searchResult);
        if(expected == 0) {
            assertTrue(searchResult.getAttributes() == null || CollectionUtils.isEmpty(searchResult.getAttributes().getValues()));
            assertNull(searchResult.getEntities());
        } else if(searchResult.getEntities() != null) {
            assertEquals(searchResult.getEntities().size(), expected);
        } else {
            assertNotNull(searchResult.getAttributes());
            assertNotNull(searchResult.getAttributes().getValues());
            assertEquals(searchResult.getAttributes().getValues().size(), expected);
        }
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
