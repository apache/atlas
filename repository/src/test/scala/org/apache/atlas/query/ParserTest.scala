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

import org.apache.atlas.repository.BaseTest
import org.junit.{Before, Test}


class ParserTest extends BaseTest {

    @Before
    override def setup {
        super.setup
        QueryTestsUtils.setupTypes
    }

    @Test def testFrom: Unit = {
        val p = new QueryParser
        println(p("from DB").right.get.toString)
    }

    @Test def testFrom2: Unit = {
        val p = new QueryParser
        println(p("DB").right.get.toString)
    }

    @Test def testJoin1: Unit = {
        val p = new QueryParser
        println(p("DB, Table").right.get.toString)
    }

    @Test def testWhere1: Unit = {
        val p = new QueryParser
        println(p("DB as db1 Table where db1.name ").right.get.toString)
    }

    @Test def testWhere2: Unit = {
        val p = new QueryParser
        println(p("DB name = \"Reporting\"").right.get.toString)
    }

  @Test def testIsTrait: Unit = {
    val p = new QueryParser
    println(p("Table isa Dimension").right.get.toString)
    println(p("Table is Dimension").right.get.toString)
  }

    @Test def test4: Unit = {
        val p = new QueryParser
        println(p("DB where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1").right.get.toString)
    }

    @Test def testJoin2: Unit = {
        val p = new QueryParser
        println(p("DB as db1 where (createTime + 1) > 0 and (db1.name = \"Reporting\") or DB has owner Table as tab " +
            " select db1.name as dbName, tab.name as tabName").right.get.toString)
    }

    @Test def testLoop: Unit = {
        val p = new QueryParser
        println(p("Table loop (LoadProcess outputTable)").right.get.toString)
    }

  @Test def testNegInvalidateType: Unit = {
    val p = new QueryParser
    val x = p("from blah")
    println(p("from blah").left)
  }

    @Test def testPath1: Unit = {
      val p = new QueryParser
      println(p("Table loop (LoadProcess outputTable) withPath").right.get.toString)
    }

    @Test def testPath2: Unit = {
      val p = new QueryParser
      println(p(
        "Table as src loop (LoadProcess outputTable) as dest " +
          "select src.name as srcTable, dest.name as destTable withPath").right.get.toString
      )
    }

  @Test def testList: Unit = {
    val p = new QueryParser
    println(p(
      "Partition as p where values = ['2015-01-01']," +
        " table where name = 'tableoq8ty'," +
        " db where name = 'default' and clusterName = 'test'").right.get.toString
    )
  }

}
