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
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Assertions, BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class LineageQueryTest extends FunSuite with BeforeAndAfterAll {

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
    val PREFIX_SPACES_REGEX = ("\\n\\s*").r

    def validateJson(r: GremlinQueryResult, expected: String = null): Unit = {
        val rJ = r.toJson
        if (expected != null) {
            var a = STRUCT_NAME_REGEX.replaceAllIn(rJ, "")
            a = PREFIX_SPACES_REGEX.replaceAllIn(a, "")
            var b = STRUCT_NAME_REGEX.replaceAllIn(expected, "")
            b = PREFIX_SPACES_REGEX.replaceAllIn(b, "")
            Assertions.assert(a == b)
        } else {
            println(rJ)
        }
    }

    test("testInputTables") {
        val r = QueryProcessor.evaluate(_class("LoadProcess").field("inputTables"), g)
        val x = r.toJson
        validateJson(r, """{
  "query":"LoadProcess inputTables",
  "dataType":{
    "superTypes":[

    ],
    "hierarchicalMetaTypeName":"org.apache.hadoop.metadata.typesystem.types.ClassType",
    "typeName":"Table",
    "attributeDefinitions":[
      {
        "name":"name",
        "dataTypeName":"string",
        "multiplicity":{
          "lower":0,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      },
      {
        "name":"db",
        "dataTypeName":"DB",
        "multiplicity":{
          "lower":1,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      },
      {
        "name":"sd",
        "dataTypeName":"StorageDesc",
        "multiplicity":{
          "lower":1,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      }
    ]
  },
  "rows":[
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"2048",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"512",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"256",
        "$typeName$":"DB",
        "version":0
      },
      "name":"sales_fact"
    },
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"4864",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"3840",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"256",
        "$typeName$":"DB",
        "version":0
      },
      "name":"time_dim",
      "$traits$":{
        "Dimension":{
          "$typeName$":"Dimension"
        }
      }
    },
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"8960",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"7424",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"7168",
        "$typeName$":"DB",
        "version":0
      },
      "name":"sales_fact_daily_mv"
    }
  ]
}""")
    }

    test("testLoadProcessOut") {
        val r = QueryProcessor.evaluate(_class("Table").field("LoadProcess").field("outputTable"), g)
        validateJson(r, null)
    }

    test("testLineageAll") {
        val r = QueryProcessor.evaluate(_class("Table").loop(id("LoadProcess").field("outputTable")), g)
        validateJson(r, """{
  "query":"Table as _loop0 loop (LoadProcess outputTable)",
  "dataType":{
    "superTypes":[

    ],
    "hierarchicalMetaTypeName":"org.apache.hadoop.metadata.typesystem.types.ClassType",
    "typeName":"Table",
    "attributeDefinitions":[
      {
        "name":"name",
        "dataTypeName":"string",
        "multiplicity":{
          "lower":0,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      },
      {
        "name":"db",
        "dataTypeName":"DB",
        "multiplicity":{
          "lower":1,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      },
      {
        "name":"sd",
        "dataTypeName":"StorageDesc",
        "multiplicity":{
          "lower":1,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      }
    ]
  },
  "rows":[
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"8960",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"7424",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"7168",
        "$typeName$":"DB",
        "version":0
      },
      "name":"sales_fact_daily_mv"
    },
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"12800",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"11264",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"7168",
        "$typeName$":"DB",
        "version":0
      },
      "name":"sales_fact_monthly_mv"
    },
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"8960",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"7424",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"7168",
        "$typeName$":"DB",
        "version":0
      },
      "name":"sales_fact_daily_mv"
    },
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"12800",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"11264",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"7168",
        "$typeName$":"DB",
        "version":0
      },
      "name":"sales_fact_monthly_mv"
    },
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"12800",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"11264",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"7168",
        "$typeName$":"DB",
        "version":0
      },
      "name":"sales_fact_monthly_mv"
    }
  ]
}""")
    }

    test("testLineageAllSelect") {
        val r = QueryProcessor.evaluate(_class("Table").as("src").loop(id("LoadProcess").field("outputTable")).as("dest").
            select(id("src").field("name").as("srcTable"), id("dest").field("name").as("destTable")), g)
        validateJson(r, """{
  "query":"Table as src loop (LoadProcess outputTable) as dest select src.name as srcTable, dest.name as destTable",
  "dataType":{
    "typeName":"__tempQueryResultStruct2",
    "attributeDefinitions":[
      {
        "name":"srcTable",
        "dataTypeName":"string",
        "multiplicity":{
          "lower":0,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      },
      {
        "name":"destTable",
        "dataTypeName":"string",
        "multiplicity":{
          "lower":0,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      }
    ]
  },
  "rows":[
    {
      "$typeName$":"__tempQueryResultStruct2",
      "srcTable":"sales_fact",
      "destTable":"sales_fact_daily_mv"
    },
    {
      "$typeName$":"__tempQueryResultStruct2",
      "srcTable":"sales_fact",
      "destTable":"sales_fact_monthly_mv"
    },
    {
      "$typeName$":"__tempQueryResultStruct2",
      "srcTable":"time_dim",
      "destTable":"sales_fact_daily_mv"
    },
    {
      "$typeName$":"__tempQueryResultStruct2",
      "srcTable":"time_dim",
      "destTable":"sales_fact_monthly_mv"
    },
    {
      "$typeName$":"__tempQueryResultStruct2",
      "srcTable":"sales_fact_daily_mv",
      "destTable":"sales_fact_monthly_mv"
    }
  ]
}""")
    }

    test("testLineageFixedDepth") {
        val r = QueryProcessor.evaluate(_class("Table").loop(id("LoadProcess").field("outputTable"), int(1)), g)
        validateJson(r, """{
  "query":"Table as _loop0 loop (LoadProcess outputTable) times 1",
  "dataType":{
    "superTypes":[

    ],
    "hierarchicalMetaTypeName":"org.apache.hadoop.metadata.typesystem.types.ClassType",
    "typeName":"Table",
    "attributeDefinitions":[
      {
        "name":"name",
        "dataTypeName":"string",
        "multiplicity":{
          "lower":0,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      },
      {
        "name":"db",
        "dataTypeName":"DB",
        "multiplicity":{
          "lower":1,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      },
      {
        "name":"sd",
        "dataTypeName":"StorageDesc",
        "multiplicity":{
          "lower":1,
          "upper":1,
          "isUnique":false
        },
        "isComposite":false,
        "isUnique":false,
        "isIndexable":true,
        "reverseAttributeName":null
      }
    ]
  },
  "rows":[
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"8960",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"7424",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"7168",
        "$typeName$":"DB",
        "version":0
      },
      "name":"sales_fact_daily_mv"
    },
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"8960",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"7424",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"7168",
        "$typeName$":"DB",
        "version":0
      },
      "name":"sales_fact_daily_mv"
    },
    {
      "$typeName$":"Table",
      "$id$":{
        "id":"12800",
        "$typeName$":"Table",
        "version":0
      },
      "sd":{
        "id":"11264",
        "$typeName$":"StorageDesc",
        "version":0
      },
      "db":{
        "id":"7168",
        "$typeName$":"DB",
        "version":0
      },
      "name":"sales_fact_monthly_mv"
    }
  ]
}""")
    }
}