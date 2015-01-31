package org.apache.hadoop.metadata.query

import com.thinkaurelius.titan.core.TitanGraph
import org.apache.hadoop.metadata.query.Expressions._
import org.apache.hadoop.metadata.types.TypeSystem
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GremlinTest extends FunSuite with BeforeAndAfterAll {

  var g: TitanGraph = null

  override def beforeAll() {
    TypeSystem.getInstance().reset()
    QueryTestsUtils.setupTypes
    g = QueryTestsUtils.setupTestGraph
  }

  override def afterAll() {
    g.shutdown()
  }

  test("testClass") {
    val r = QueryProcessor.evaluate(_class("DB"), g)
    println(r)
  }

  test("testFilter") {
    val r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting"))), g)
    println(r)
  }

  test("testSelect") {
    val r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting"))).
      select(id("name"), id("owner")), g)
    println(r)
  }

  test("testIsTrait") {
    val r = QueryProcessor.evaluate(_class("Table").where(isTrait("Dimension")), g)
    println(r)
  }

  test("testhasField") {
    val r = QueryProcessor.evaluate(_class("DB").where(hasField("name")), g)
    println(r)
  }

  test("testFieldReference") {
    val r = QueryProcessor.evaluate(_class("DB").field("Table"), g)
    println(r)
  }

  test("testBackReference") {
    val r = QueryProcessor.evaluate(
      _class("DB").as("db").field("Table").where(id("db").field("name").`=`(string("Reporting"))), g)
    println(r)
  }

  test("testArith") {
    val r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting"))).
      select(id("name"), id("createTime") + int(1)), g)
    println(r)
  }

  test("testComparisonLogical") {
    val r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting")).
      and(id("createTime") > int(0))), g)
    println(r)
  }

  test("testJoinAndSelect1") {
    val r = QueryProcessor.evaluate(
      _class("DB").as("db1").where(id("name").`=`(string("Sales"))).field("Table").as("tab").
        where((isTrait("Dimension"))).
        select(id("db1").field("name").as("dbName"), id("tab").field("name").as("tabName")),g
    )
    println(r)
  }

  test("testJoinAndSelect2") {
    val r = QueryProcessor.evaluate(
      _class("DB").as("db1").where((id("db1").field("createTime") > int(0))
        .or(id("name").`=`(string("Reporting")))).field("Table").as("tab")
        .select(id("db1").field("name").as("dbName"), id("tab").field("name").as("tabName")), g
    )
    println(r)
  }

  test("testJoinAndSelect3") {
    val r = QueryProcessor.evaluate(
      _class("DB").as("db1").where((id("db1").field("createTime")  > int(0))
        .and(id("db1").field("name").`=`(string("Reporting")))
        .or(id("db1").hasField("owner"))).field("Table").as("tab")
        .select(id("db1").field("name").as("dbName"), id("tab").field("name").as("tabName")), g
    )
    println(r)
  }

}
