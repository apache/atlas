/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.impala.hook;

import org.apache.atlas.impala.model.ImpalaOperationType;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class ImpalaLineageHookTest {
    private static final Logger LOG = LoggerFactory.getLogger(ImpalaLineageHookTest.class);

    @Test(dataProvider = "queryDataProvider")
    public void testAllImpalaOperationTypes(String query, ImpalaOperationType expectedOperationType) {
        try {
            ImpalaOperationType operationType = ImpalaOperationParser.getImpalaOperationType(query);
            assertEquals(operationType, expectedOperationType);
        } catch (Exception e) {
            fail("Query processing failed for query: " + query + " due to exception: " + e.getMessage());
        }
    }

    @DataProvider(name = "queryDataProvider")
    public Object[][] provideTestData() {
        String table1 = "table_" + random();
        String table2 = "table_" + random();

        return new Object[][] {
                {"CREATE VIEW my_view AS SELECT id, name FROM " + table1, ImpalaOperationType.CREATEVIEW },
                {"CREATE TABLE " + table1 + " AS SELECT id, name FROM " + table1, ImpalaOperationType.CREATETABLE_AS_SELECT },
                {"ALTER VIEW my_view AS SELECT id, name FROM " + table1, ImpalaOperationType.ALTERVIEW_AS },
                {"INSERT INTO " + table1 + " SELECT id, name FROM " + table1, ImpalaOperationType.QUERY },
                {"WITH filtered_data AS (SELECT id, name, amount FROM " + table1 + " WHERE amount > 100) " +
                        "INSERT INTO " + table2 + " SELECT id, name, amount FROM filtered_data", ImpalaOperationType.QUERY_WITH_CLAUSE }
        };
    }

    private String random() {
        return RandomStringUtils.randomAlphanumeric(10);
    }
}
