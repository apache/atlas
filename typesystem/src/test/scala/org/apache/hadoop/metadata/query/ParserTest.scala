package org.apache.hadoop.metadata.query

import org.apache.hadoop.metadata.BaseTest
import org.apache.hadoop.metadata.query.Expressions._
import org.junit.{Test, Before}


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

  @Test def test4 : Unit = {
    val p = new QueryParser
    println(p("DB where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1").right.get.toString)
  }

  @Test def testJoin2 : Unit = {
    val p = new QueryParser
    println(p("DB as db1 where (createTime + 1) > 0 and (db1.name = \"Reporting\") or DB has owner Table as tab " +
      " select db1.name as dbName, tab.name as tabName").right.get.toString)
  }

  @Test def testLoop : Unit = {
    val p = new QueryParser
    println(p("Table loop (LoadProcess outputTable)").right.get.toString)
  }
}
