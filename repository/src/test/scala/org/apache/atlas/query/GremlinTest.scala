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

import com.thinkaurelius.titan.core.TitanGraph
import org.apache.atlas.query.Expressions._
import org.apache.atlas.typesystem.types.TypeSystem
import org.junit.runner.RunWith
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GremlinTest extends FunSuite with BeforeAndAfterAll with BaseGremlinTest {

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
        validateJson(r, "{\n  \"query\":\"DB\",\n  \"dataType\":{\n    \"superTypes\":[\n      \n    ],\n    \"hierarchicalMetaTypeName\":\"org.apache.atlas.typesystem.types.ClassType\",\n    \"typeName\":\"DB\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"name\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"owner\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"createTime\",\n        \"dataTypeName\":\"int\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"DB\",\n      \"$id$\":{\n        \"id\":\"256\",\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"owner\":\"John ETL\",\n      \"name\":\"Sales\",\n      \"createTime\":1000\n    },\n    {\n      \"$typeName$\":\"DB\",\n      \"$id$\":{\n        \"id\":\"7424\",\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"owner\":\"Jane BI\",\n      \"name\":\"Reporting\",\n      \"createTime\":1500\n    }\n  ]\n}")
    }

    test("testName") {
        val r = QueryProcessor.evaluate(_class("DB").field("name"), g)
        validateJson(r, "{\n  \"query\":\"DB.name\",\n  \"dataType\":\"string\",\n  \"rows\":[\n    \"Sales\",\n    \"Reporting\"\n  ]\n}")
    }

    test("testFilter") {
        var r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting"))), g)
        validateJson(r, "{\n  \"query\":\"DB where (name = \\\"Reporting\\\")\",\n  \"dataType\":{\n    \"superTypes\":[\n      \n    ],\n    \"hierarchicalMetaTypeName\":\"org.apache.atlas.typesystem.types.ClassType\",\n    \"typeName\":\"DB\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"name\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"owner\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"createTime\",\n        \"dataTypeName\":\"int\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"DB\",\n      \"$id$\":{\n        \"id\":\"7424\",\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"owner\":\"Jane BI\",\n      \"name\":\"Reporting\",\n      \"createTime\":1500\n    }\n  ]\n}")
    }

    test("testFilter2") {
        var r = QueryProcessor.evaluate(_class("DB").where(id("DB").field("name").`=`(string("Reporting"))), g)
        validateJson(r, "{\n  \"query\":\"DB where (name = \\\"Reporting\\\")\",\n  \"dataType\":{\n    \"superTypes\":[\n      \n    ],\n    \"hierarchicalMetaTypeName\":\"org.apache.atlas.typesystem.types.ClassType\",\n    \"typeName\":\"DB\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"name\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"owner\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"createTime\",\n        \"dataTypeName\":\"int\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"DB\",\n      \"$id$\":{\n        \"id\":\"7424\",\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"owner\":\"Jane BI\",\n      \"name\":\"Reporting\",\n      \"createTime\":1500\n    }\n  ]\n}")
    }


    test("testSelect") {
        val r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting"))).
            select(id("name"), id("owner")), g)
        validateJson(r, "{\n  \"query\":\"DB where (name = \\\"Reporting\\\") as _src1 select _src1.name as _col_0, _src1.owner as _col_1\",\n  \"dataType\":{\n    \"typeName\":\"__tempQueryResultStruct1\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"_col_0\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"_col_1\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"__tempQueryResultStruct1\",\n      \"_col_1\":\"Jane BI\",\n      \"_col_0\":\"Reporting\"\n    }\n  ]\n}")
    }

    test("testIsTrait") {
        val r = QueryProcessor.evaluate(_class("Table").where(isTrait("Dimension")), g)
        validateJson(r, """{
                          |  "query":"Table where Table is Dimension",
                          |  "dataType":{
                          |    "superTypes":[
                          |
                          |    ],
                          |    "hierarchicalMetaTypeName":"org.apache.atlas.typesystem.types.ClassType",
                          |    "typeName":"Table",
                          |    "attributeDefinitions":[
                          |      {
                          |        "name":"name",
                          |        "dataTypeName":"string",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"db",
                          |        "dataTypeName":"DB",
                          |        "multiplicity":{
                          |          "lower":1,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"sd",
                          |        "dataTypeName":"StorageDesc",
                          |        "multiplicity":{
                          |          "lower":1,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"created",
                          |        "dataTypeName":"date",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      }
                          |    ]
                          |  },
                          |  "rows":[
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDesc",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"product_dim",
                          |      "$traits$":{
                          |        "Dimension":{
                          |          "$typeName$":"Dimension"
                          |        }
                          |      }
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDesc",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"time_dim",
                          |      "$traits$":{
                          |        "Dimension":{
                          |          "$typeName$":"Dimension"
                          |        }
                          |      }
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDesc",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"customer_dim",
                          |      "$traits$":{
                          |        "Dimension":{
                          |          "$typeName$":"Dimension"
                          |        }
                          |      }
                          |    }
                          |  ]
                          |}""".stripMargin)
    }

    test("testhasField") {
        val r = QueryProcessor.evaluate(_class("DB").where(hasField("name")), g)
        validateJson(r, """{
                          |  "query":"DB where DB has name",
                          |  "dataType":{
                          |    "superTypes":[
                          |
                          |    ],
                          |    "hierarchicalMetaTypeName":"org.apache.atlas.typesystem.types.ClassType",
                          |    "typeName":"DB",
                          |    "attributeDefinitions":[
                          |      {
                          |        "name":"name",
                          |        "dataTypeName":"string",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"owner",
                          |        "dataTypeName":"string",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"createTime",
                          |        "dataTypeName":"int",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      }
                          |    ]
                          |  },
                          |  "rows":[
                          |    {
                          |      "$typeName$":"DB",
                          |      "$id$":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "owner":"John ETL",
                          |      "name":"Sales",
                          |      "createTime":1000
                          |    },
                          |    {
                          |      "$typeName$":"DB",
                          |      "$id$":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "owner":"Jane BI",
                          |      "name":"Reporting",
                          |      "createTime":1500
                          |    }
                          |  ]
                          |}""".stripMargin)
    }

    test("testFieldReference") {
        val r = QueryProcessor.evaluate(_class("DB").field("Table"), g)
        validateJson(r, """{
                          |  "query":"DB Table",
                          |  "dataType":{
                          |    "superTypes":[      ],
                          |    "hierarchicalMetaTypeName":"org.apache.atlas.typesystem.types.ClassType",
                          |    "typeName":"Table",
                          |    "attributeDefinitions":[
                          |      {
                          |        "name":"name",
                          |        "dataTypeName":"string",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"db",
                          |        "dataTypeName":"DB",
                          |        "multiplicity":{
                          |          "lower":1,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"sd",
                          |        "dataTypeName":"StorageDesc",
                          |        "multiplicity":{
                          |          "lower":1,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      },
                          |      {
                          |        "name":"created",
                          |        "dataTypeName":"date",
                          |        "multiplicity":{
                          |          "lower":0,
                          |          "upper":1,
                          |          "isUnique":false
                          |        },
                          |        "isComposite":false,
                          |        "isUnique":false,
                          |        "isIndexable":true,
                          |        "reverseAttributeName":null
                          |      }
                          |    ]
                          |  },
                          |  "rows":[
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDesc",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact"
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDesc",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"product_dim",
                          |      "$traits$":{
                          |        "Dimension":{
                          |          "$typeName$":"Dimension"
                          |        }
                          |      }
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDesc",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"time_dim",
                          |      "$traits$":{
                          |        "Dimension":{
                          |          "$typeName$":"Dimension"
                          |        }
                          |      }
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDesc",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"customer_dim",
                          |      "$traits$":{
                          |        "Dimension":{
                          |          "$typeName$":"Dimension"
                          |        }
                          |      }
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDesc",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact_daily_mv"
                          |    },
                          |    {
                          |      "$typeName$":"Table",
                          |      "$id$":{
                          |        "$typeName$":"Table",
                          |        "version":0
                          |      },
                          |      "created":"2014-12-11T02:35:58.440Z",
                          |      "sd":{
                          |        "$typeName$":"StorageDesc",
                          |        "version":0
                          |      },
                          |      "db":{
                          |        "$typeName$":"DB",
                          |        "version":0
                          |      },
                          |      "name":"sales_fact_monthly_mv"
                          |    }
                          |  ]
                          |}""".stripMargin);
    }

    test("testBackReference") {
        val r = QueryProcessor.evaluate(
            _class("DB").as("db").field("Table").where(id("db").field("name").`=`(string("Reporting"))), g)
        validateJson(r, null)
    }

    test("testArith") {
        val r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting"))).
            select(id("name"), id("createTime") + int(1)), g)
        validateJson(r, "{\n  \"query\":\"DB where (name = \\\"Reporting\\\") as _src1 select _src1.name as _col_0, (_src1.createTime + 1) as _col_1\",\n  \"dataType\":{\n    \"typeName\":\"__tempQueryResultStruct3\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"_col_0\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"_col_1\",\n        \"dataTypeName\":\"int\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"__tempQueryResultStruct3\",\n      \"_col_1\":1501,\n      \"_col_0\":\"Reporting\"\n    }\n  ]\n}")
    }

    test("testComparisonLogical") {
        val r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting")).
            and(id("createTime") > int(0))), g)
        validateJson(r, "{\n  \"query\":\"DB where (name = \\\"Reporting\\\") and (createTime > 0)\",\n  \"dataType\":{\n    \"superTypes\":[\n      \n    ],\n    \"hierarchicalMetaTypeName\":\"org.apache.atlas.typesystem.types.ClassType\",\n    \"typeName\":\"DB\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"name\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"owner\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"createTime\",\n        \"dataTypeName\":\"int\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"DB\",\n      \"$id$\":{\n        \"id\":\"7424\",\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"owner\":\"Jane BI\",\n      \"name\":\"Reporting\",\n      \"createTime\":1500\n    }\n  ]\n}")
    }

    test("testJoinAndSelect1") {
        val r = QueryProcessor.evaluate(
            _class("DB").as("db1").where(id("name").`=`(string("Sales"))).field("Table").as("tab").
                where((isTrait("Dimension"))).
                select(id("db1").field("name").as("dbName"), id("tab").field("name").as("tabName")), g
        )
        validateJson(r, "{\n  \"query\":\"DB as db1 where (name = \\\"Sales\\\") Table as tab where DB as db1 where (name = \\\"Sales\\\") Table as tab is Dimension as _src1 select db1.name as dbName, tab.name as tabName\",\n  \"dataType\":{\n    \"typeName\":\"__tempQueryResultStruct5\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"dbName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"tabName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"__tempQueryResultStruct5\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"product_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct5\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"time_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct5\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"customer_dim\"\n    }\n  ]\n}")
    }

    test("testJoinAndSelect2") {
        val r = QueryProcessor.evaluate(
            _class("DB").as("db1").where((id("db1").field("createTime") > int(0))
                .or(id("name").`=`(string("Reporting")))).field("Table").as("tab")
                .select(id("db1").field("name").as("dbName"), id("tab").field("name").as("tabName")), g
        )
        validateJson(r, "{\n  \"query\":\"DB as db1 where (db1.createTime > 0) or (name = \\\"Reporting\\\") Table as tab select db1.name as dbName, tab.name as tabName\",\n  \"dataType\":{\n    \"typeName\":\"__tempQueryResultStruct6\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"dbName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"tabName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"sales_fact\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"product_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"time_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"customer_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Reporting\",\n      \"tabName\":\"sales_fact_daily_mv\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Reporting\",\n      \"tabName\":\"sales_fact_monthly_mv\"\n    }\n  ]\n}")
    }

    test("testJoinAndSelect3") {
        val r = QueryProcessor.evaluate(
            _class("DB").as("db1").where((id("db1").field("createTime") > int(0))
                .and(id("db1").field("name").`=`(string("Reporting")))
                .or(id("db1").hasField("owner"))).field("Table").as("tab")
                .select(id("db1").field("name").as("dbName"), id("tab").field("name").as("tabName")), g
        )
        validateJson(r, "{\n  \"query\":\"DB as db1 where (db1.createTime > 0) and (db1.name = \\\"Reporting\\\") or DB as db1 has owner Table as tab select db1.name as dbName, tab.name as tabName\",\n  \"dataType\":{\n    \"typeName\":\"__tempQueryResultStruct7\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"dbName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"tabName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"sales_fact\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"product_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"time_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"customer_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Reporting\",\n      \"tabName\":\"sales_fact_daily_mv\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Reporting\",\n      \"tabName\":\"sales_fact_monthly_mv\"\n    }\n  ]\n}")
    }

    test("testJoinAndSelect4") {
      val r = QueryProcessor.evaluate(
        _class("DB").as("db1").where(id("name").`=`(string("Sales"))).field("Table").as("tab").
          where((isTrait("Dimension"))).
          select(id("db1").as("dbO"), id("tab").field("name").as("tabName")), g
      )
      validateJson(r, "{\n  \"query\":\"DB as db1 where (name = \\\"Sales\\\") Table as tab where DB as db1 where (name = \\\"Sales\\\") Table as tab is Dimension as _src1 select db1 as dbO, tab.name as tabName\",\n  \"dataType\":{\n    \"typeName\":\"\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"dbO\",\n        \"dataTypeName\":\"DB\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"tabName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"\",\n      \"dbO\":{\n        \"id\":\"256\",\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"tabName\":\"product_dim\"\n    },\n    {\n      \"$typeName$\":\"\",\n      \"dbO\":{\n        \"id\":\"256\",\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"tabName\":\"time_dim\"\n    },\n    {\n      \"$typeName$\":\"\",\n      \"dbO\":{\n        \"id\":\"256\",\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"tabName\":\"customer_dim\"\n    }\n  ]\n}")
    }

    test("testNegativeInvalidType") {
      val p = new QueryParser
      val e = p("from blah").right.get
      an [ExpressionException] should be thrownBy QueryProcessor.evaluate(e, g)
    }

    test("Bug37860") {
        val p = new QueryParser
        val e = p("Table as t where name = 'sales_fact' db where name = 'Sales' and owner = 'John ETL' select t").right.get
        val r = QueryProcessor.evaluate(e, g)
        validateJson(r)
    }
}
