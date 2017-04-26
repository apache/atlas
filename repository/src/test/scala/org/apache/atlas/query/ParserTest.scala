/*
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

package org.apache.atlas.query

import org.apache.atlas.DBSandboxer
import org.apache.atlas.repository.BaseTest
import org.testng.annotations.{BeforeMethod, Listeners, Test}


class ParserTest extends BaseTest {

    @BeforeMethod
    override def setup {
        super.setup
        QueryTestsUtils.setupTypes
    }

    @Test def testFrom: Unit = {
        println(QueryParser.apply("from DB").right.get.toString)
    }

    @Test def testFrom2: Unit = {
        println(QueryParser.apply("DB").right.get.toString)
    }

    @Test def testJoin1: Unit = {
        println(QueryParser.apply("DB, Table").right.get.toString)
    }

    @Test def testWhere1: Unit = {
        println(QueryParser.apply("DB as db1 Table where db1.name ").right.get.toString)
    }

    @Test def testWhere2: Unit = {
        println(QueryParser.apply("DB name = \"Reporting\"").right.get.toString)
    }

  @Test def testIsTrait: Unit = {
    println(QueryParser.apply("Table isa Dimension").right.get.toString)
    println(QueryParser.apply("Table is Dimension").right.get.toString)
  }

    @Test def test4: Unit = {
        println(QueryParser.apply("DB where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1").right.get.toString)
    }

    @Test def testJoin2: Unit = {
        println(QueryParser.apply("DB as db1 where (createTime + 1) > 0 and (db1.name = \"Reporting\") or DB has owner Table as tab " +
            " select db1.name as dbName, tab.name as tabName").right.get.toString)
    }

    @Test def testLoop: Unit = {
        println(QueryParser.apply("Table loop (LoadProcess outputTable)").right.get.toString)
    }

  @Test def testNegInvalidateType: Unit = {
    val x = QueryParser.apply("from blah")
    println(QueryParser.apply("from blah").left)
  }

    @Test def testPath1: Unit = {
      println(QueryParser.apply("Table loop (LoadProcess outputTable) withPath").right.get.toString)
    }

    @Test def testPath2: Unit = {
      println(QueryParser.apply(
        "Table as src loop (LoadProcess outputTable) as dest " +
          "select src.name as srcTable, dest.name as destTable withPath").right.get.toString
      )
    }

  @Test def testList: Unit = {
    println(QueryParser.apply(
      "Partition as p where values = ['2015-01-01']," +
        " table where name = 'tableoq8ty'," +
        " db where name = 'default' and clusterName = 'test'").right.get.toString
    )
  }

  @Test def testorder_by: Unit = {
    println(QueryParser.apply("from DB order by columnA").right.get.toString)
  }

}
