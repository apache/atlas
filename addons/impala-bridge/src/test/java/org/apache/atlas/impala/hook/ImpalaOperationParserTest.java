/** Licensed to the Apache Software Foundation (ASF) under one
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
 **/
package org.apache.atlas.impala.hook;

import org.apache.atlas.impala.model.ImpalaOperationType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ImpalaOperationParserTest {

    @Test
    public void testGetImpalaOperationTypeCREATEVIEW() {
        String viewQuery = "create view test_view as select * from test_table";
        ImpalaOperationType operationType = ImpalaOperationParser.getImpalaOperationType(viewQuery);
        assertEquals(ImpalaOperationType.CREATEVIEW,operationType);
    }

    @Test
    public void testGetImpalaOperationTypeCREATETABLE_AS_SELECT() {
        String selectAsQuery = "create table test_table as select * from test_table_1";
        ImpalaOperationType operationType = ImpalaOperationParser.getImpalaOperationType(selectAsQuery);
        assertEquals(ImpalaOperationType.CREATETABLE_AS_SELECT,operationType);
    }

    @Test
    public void testGetImpalaOperationTypeALTERVIEW_AS() {
        String alterViewQuery = "ALTER VIEW test_view AS SELECT * FROM test_table_1;";
        ImpalaOperationType operationType = ImpalaOperationParser.getImpalaOperationType(alterViewQuery);
        assertEquals(ImpalaOperationType.ALTERVIEW_AS,operationType);
    }

    @Test
    public void testGetImpalaOperationTypeQUERY() {
        String query = "INSERT INTO test_table SELECT * FROM test_source_table;";
        ImpalaOperationType operationType = ImpalaOperationParser.getImpalaOperationType(query);
        assertEquals(ImpalaOperationType.QUERY,operationType);
    }


    @Test
    public void testGetImpalaOperationTypeQUERY_WITH_CLAUSE() {
        String queryWithClause = "WITH test_table_2 AS (SELECT id FROM test_table)INSERT INTO test_table_1 SELECT id FROM test_table_2";
        ImpalaOperationType operationType = ImpalaOperationParser.getImpalaOperationType(queryWithClause);
        assertEquals(ImpalaOperationType.QUERY_WITH_CLAUSE,operationType);
    }

    @Test
    public void testGetImpalaOperationTypeUNKNOWN() {
        String unknowQuery = "SELECT * from test_table";
        ImpalaOperationType operationType = ImpalaOperationParser.getImpalaOperationType(unknowQuery);
        assertEquals(ImpalaOperationType.UNKNOWN,operationType);
    }

    @Test
    public void testGetImpalaOperationSubTypeWithInvalidQuery() {
        String unknowQuery = "SELECT * from test_table";
        ImpalaOperationType operationType = ImpalaOperationParser.getImpalaOperationSubType(ImpalaOperationType.QUERY_WITH_CLAUSE,unknowQuery);
        assertEquals(ImpalaOperationType.UNKNOWN,operationType);
    }

    @Test
    public void testGetImpalaOperationSubTypeINSERT() {
        String query = "INSERT INTO test_table SELECT * FROM test_source_table;";
        ImpalaOperationType operationType = ImpalaOperationParser.getImpalaOperationSubType(ImpalaOperationType.QUERY,query);
        assertEquals(ImpalaOperationType.INSERT,operationType);
    }

    @Test
    public void testGetImpalaOperationSubTypeINSERT_OVERWRITE() {
        String query = "INSERT OVERWRITE TABLE test_table\n" +
                "SELECT region, SUM(amount) AS test_table_1\n" +
                "FROM test_table_2\n" +
                "GROUP BY region;\n";
        ImpalaOperationType operationType = ImpalaOperationParser.getImpalaOperationSubType(ImpalaOperationType.QUERY,query);
        assertEquals(ImpalaOperationType.INSERT_OVERWRITE,operationType);
    }
}
