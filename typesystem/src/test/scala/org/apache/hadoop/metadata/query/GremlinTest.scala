package org.apache.hadoop.metadata.query

import com.thinkaurelius.titan.core.TitanGraph
import org.apache.hadoop.metadata.query.Expressions._
import org.scalatest._

class GremlinTest extends FunSuite with BeforeAndAfterAll {

  var g: TitanGraph = null

  override def beforeAll() {
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


}
