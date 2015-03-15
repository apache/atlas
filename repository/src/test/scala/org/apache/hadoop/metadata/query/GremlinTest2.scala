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

package org.apache.hadoop.metadata.query

import com.thinkaurelius.titan.core.TitanGraph
import org.apache.hadoop.metadata.query.Expressions._
import org.apache.hadoop.metadata.typesystem.types.TypeSystem
import org.junit.runner.RunWith
import org.scalatest._
import Matchers._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GremlinTest2 extends FunSuite with BeforeAndAfterAll {

  var g: TitanGraph = null

  override def beforeAll() {
    TypeSystem.getInstance().reset()
    QueryTestsUtils.setupTypes
    g = QueryTestsUtils.setupTestGraph
  }

  override def afterAll() {
    g.shutdown()
  }

  val STRUCT_NAME_REGEX = (TypeUtils.TEMP_STRUCT_NAME_PREFIX + "\\d+").r

  def validateJson(r: GremlinQueryResult, expected: String = null): Unit = {
    val rJ = r.toJson
    if (expected != null) {
      val a = STRUCT_NAME_REGEX.replaceAllIn(rJ, "")
      val b = STRUCT_NAME_REGEX.replaceAllIn(expected, "")
      Assertions.assert(a == b)
    } else {
      println(rJ)
    }
  }

  test("testTraitSelect") {
    val r = QueryProcessor.evaluate(_class("Table").as("t").join("Dimension").as("dim").select(id("t"), id("dim")), g)
    validateJson(r, "{\n  \"query\":\"Table as t.Dimension as dim select t as _col_0, dim as _col_1\",\n  \"dataType\":{\n    \"typeName\":\"\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"_col_0\",\n        \"dataTypeName\":\"Table\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"_col_1\",\n        \"dataTypeName\":\"Dimension\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"\",\n      \"_col_1\":{\n        \"$typeName$\":\"Dimension\"\n      },\n      \"_col_0\":{\n        \"id\":\"3328\",\n        \"$typeName$\":\"Table\",\n        \"version\":0\n      }\n    },\n    {\n      \"$typeName$\":\"\",\n      \"_col_1\":{\n        \"$typeName$\":\"Dimension\"\n      },\n      \"_col_0\":{\n        \"id\":\"4864\",\n        \"$typeName$\":\"Table\",\n        \"version\":0\n      }\n    },\n    {\n      \"$typeName$\":\"\",\n      \"_col_1\":{\n        \"$typeName$\":\"Dimension\"\n      },\n      \"_col_0\":{\n        \"id\":\"6656\",\n        \"$typeName$\":\"Table\",\n        \"version\":0\n      }\n    }\n  ]\n}")
  }

}