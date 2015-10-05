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
import com.thinkaurelius.titan.core.util.TitanCleanup
import org.apache.atlas.discovery.graph.DefaultGraphPersistenceStrategy
import org.apache.atlas.query.Expressions._
import org.apache.atlas.repository.graph.{TitanGraphProvider, GraphBackedMetadataRepository}
import org.apache.atlas.typesystem.types.TypeSystem
import org.junit.Test
import org.junit.runner.RunWith
import org.scalatest.Matchers._
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GremlinTest extends FunSuite with BeforeAndAfterAll with BaseGremlinTest {

  var g: TitanGraph = null
  var gp: GraphPersistenceStrategies = null;
  var gProvider: TitanGraphProvider = null;

  override def beforeAll() {
    TypeSystem.getInstance().reset()
    QueryTestsUtils.setupTypes
    gProvider = new TitanGraphProvider();
    gp = new DefaultGraphPersistenceStrategy(new GraphBackedMetadataRepository(gProvider))
    g = QueryTestsUtils.setupTestGraph(gProvider)

  }

  override def afterAll() {
    g.shutdown()
    try {
      TitanCleanup.clear(g);
    } catch {
      case ex: Exception =>
        print("Could not clear the graph ", ex);
    }
  }

  test("testClass") {
    val r = QueryProcessor.evaluate(_class("DB"), g, gp)
    validateJson(r, """{
                      |    "query": "DB",
                      |    "dataType": {
                      |        "superTypes": [
                      |
                      |        ],
                      |        "hierarchicalMetaTypeName": "org.apache.atlas.typesystem.types.ClassType",
                      |        "typeName": "DB",
                      |        "attributeDefinitions": [
                      |            {
                      |                "name": "name",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "owner",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "createTime",
                      |                "dataTypeName": "int",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                 },
                      |                "isComposite": false,
                      |               "isUnique": false,
                      |               "isIndexable": true,
                      |               "reverseAttributeName": null
                      |
                      |            },
                      |            {
                      |                "name": "clusterName",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |               "isComposite": false,
                      |               "isUnique": false,
                      |               "isIndexable": true,
                      |               "reverseAttributeName": null
                      |            }
                      |            ]
                      |        },
                      |        "rows": [
                      |            {
                      |                "$typeName$": "DB",
                      |                "$id$": {
                      |                    "$typeName$": "DB",
                      |                    "version": 0
                      |                },
                      |                "owner": "John ETL",
                      |                "name": "Sales",
                      |                "createTime": 1000,
                      |                "clusterName": "test"
                      |            },
                      |            {
                      |                "$typeName$": "DB",
                      |                "$id$": {
                      |                    "$typeName$": "DB",
                      |                    "version": 0
                      |                },
                      |                "owner": "Jane BI",
                      |                "name": "Reporting",
                      |                "createTime": 1500,
                      |                "clusterName": "test"
                      |            }
                      |        ]
                      |    }""".stripMargin)
  }

  test("testName") {
    val r = QueryProcessor.evaluate(_class("DB").field("name"), g, gp)
    validateJson(r, "{\n  \"query\":\"DB.name\",\n  \"dataType\":\"string\",\n  \"rows\":[\n    \"Sales\",\n    \"Reporting\"\n  ]\n}")
  }

  test("testFilter") {
    var r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting"))), g, gp)
    validateJson(r, """{
                      |    "query": "DB where (name = \"Reporting\")",
                      |    "dataType": {
                      |        "superTypes": [],
                      |        "hierarchicalMetaTypeName": "org.apache.atlas.typesystem.types.ClassType",
                      |        "typeName": "DB",
                      |        "attributeDefinitions": [
                      |            {
                      |                "name": "name",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "owner",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "createTime",
                      |                "dataTypeName": "int",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "clusterName",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |               "isComposite": false,
                      |               "isUnique": false,
                      |               "isIndexable": true,
                      |               "reverseAttributeName": null
                      |            }
                      |        ]
                      |    },
                      |    "rows": [
                      |        {
                      |            "$typeName$": "DB",
                      |            "$id$": {
                      |                "$typeName$": "DB",
                      |                "version": 0
                      |            },
                      |            "owner": "Jane BI",
                      |            "name": "Reporting",
                      |            "createTime": 1500,
                      |            "clusterName": "test"
                      |        }
                      |    ]
                      |}""".stripMargin);
  }

  test("testFilter2") {
    var r = QueryProcessor.evaluate(_class("DB").where(id("DB").field("name").`=`(string("Reporting"))), g, gp)
    validateJson(r, """{
                      |    "query": "DB where (name = \"Reporting\")",
                      |    "dataType": {
                      |        "superTypes": [],
                      |        "hierarchicalMetaTypeName": "org.apache.atlas.typesystem.types.ClassType",
                      |        "typeName": "DB",
                      |        "attributeDefinitions": [
                      |            {
                      |                "name": "name",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "owner",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "createTime",
                      |                "dataTypeName": "int",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "clusterName",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |               "isComposite": false,
                      |               "isUnique": false,
                      |               "isIndexable": true,
                      |               "reverseAttributeName": null
                      |            }
                      |        ]
                      |    },
                      |    "rows": [
                      |        {
                      |            "$typeName$": "DB",
                      |            "$id$": {
                      |                "$typeName$": "DB",
                      |                "version": 0
                      |            },
                      |            "owner": "Jane BI",
                      |            "name": "Reporting",
                      |            "createTime": 1500,
                      |            "clusterName": "test"
                      |        }
                      |    ]
                      |}""".stripMargin);
  }

  test("testSelect") {
    val r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting"))).
      select(id("name"), id("owner")), g, gp)
    validateJson(r, """{
                      |    "query": "DB where (name = \"Reporting\") as _src1 select _src1.name as _col_0, _src1.owner as _col_1",
                      |    "dataType": {
                      |        "typeName": "__tempQueryResultStruct1",
                      |        "attributeDefinitions": [
                      |            {
                      |                "name": "_col_0",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "_col_1",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            }
                      |        ]
                      |    },
                      |    "rows": [
                      |        {
                      |            "$typeName$": "__tempQueryResultStruct1",
                      |            "_col_1": "Jane BI",
                      |            "_col_0": "Reporting"
                      |        }
                      |    ]
                      |}""".stripMargin);
  }

  test("testIsTrait") {
    val r = QueryProcessor.evaluate(_class("Table").where(isTrait("Dimension")), g, gp)
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
                      |        "dataTypeName":"StorageDescriptor",
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
                      |        "$typeName$":"StorageDescriptor",
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
                      |        "$typeName$":"StorageDescriptor",
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
                      |        "$typeName$":"StorageDescriptor",
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
    val r = QueryProcessor.evaluate(_class("DB").where(hasField("name")), g, gp)
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
                      |      },
                      |      {
                      |        "name":"clusterName",
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
                      |      "createTime":1000,
                      |      "clusterName":"test"
                      |    },
                      |    {
                      |      "$typeName$":"DB",
                      |      "$id$":{
                      |        "$typeName$":"DB",
                      |        "version":0
                      |      },
                      |      "owner":"Jane BI",
                      |      "name":"Reporting",
                      |      "createTime":1500,
                      |      "clusterName":"test"
                      |    }
                      |  ]
                      |}""".stripMargin)
  }

  test("testFieldReference") {
    val r = QueryProcessor.evaluate(_class("DB").field("Table"), g, gp)
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
                      |        "dataTypeName":"StorageDescriptor",
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
                      |        "$typeName$":"StorageDescriptor",
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
                      |        "$typeName$":"StorageDescriptor",
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
                      |        "$typeName$":"StorageDescriptor",
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
                      |        "$typeName$":"StorageDescriptor",
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
                      |        "$typeName$":"StorageDescriptor",
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
                      |        "$typeName$":"StorageDescriptor",
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
      _class("DB").as("db").field("Table").where(id("db").field("name").`=`(string("Reporting"))), g, gp)
    validateJson(r, null)
  }

  test("testArith") {
    val r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting"))).
      select(id("name"), id("createTime") + int(1)), g, gp)
    validateJson(r, "{\n  \"query\":\"DB where (name = \\\"Reporting\\\") as _src1 select _src1.name as _col_0, (_src1.createTime + 1) as _col_1\",\n  \"dataType\":{\n    \"typeName\":\"__tempQueryResultStruct3\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"_col_0\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"_col_1\",\n        \"dataTypeName\":\"int\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"__tempQueryResultStruct3\",\n      \"_col_1\":1501,\n      \"_col_0\":\"Reporting\"\n    }\n  ]\n}")
  }

  test("testComparisonLogical") {
    val r = QueryProcessor.evaluate(_class("DB").where(id("name").`=`(string("Reporting")).
      and(id("createTime") > int(0))), g, gp)
    validateJson(r, """{
                      |    "query": "DB where (name = \"Reporting\") and (createTime > 0)",
                      |    "dataType": {
                      |        "superTypes": [
                      |
                      |        ],
                      |        "hierarchicalMetaTypeName": "org.apache.atlas.typesystem.types.ClassType",
                      |        "typeName": "DB",
                      |        "attributeDefinitions": [
                      |            {
                      |                "name": "name",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "owner",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "createTime",
                      |                "dataTypeName": "int",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |                "isComposite": false,
                      |                "isUnique": false,
                      |                "isIndexable": true,
                      |                "reverseAttributeName": null
                      |            },
                      |            {
                      |                "name": "clusterName",
                      |                "dataTypeName": "string",
                      |                "multiplicity": {
                      |                    "lower": 0,
                      |                    "upper": 1,
                      |                    "isUnique": false
                      |                },
                      |               "isComposite": false,
                      |               "isUnique": false,
                      |               "isIndexable": true,
                      |               "reverseAttributeName": null
                      |            }
                      |        ]
                      |    },
                      |    "rows": [
                      |        {
                      |            "$typeName$": "DB",
                      |            "$id$": {
                      |                "$typeName$": "DB",
                      |                "version": 0
                      |            },
                      |            "owner": "Jane BI",
                      |            "name": "Reporting",
                      |            "createTime": 1500,
                      |            "clusterName": "test"
                      |        }
                      |    ]
                      |}""".stripMargin);
  }

  test("testJoinAndSelect1") {
    val r = QueryProcessor.evaluate(
      _class("DB").as("db1").where(id("name").`=`(string("Sales"))).field("Table").as("tab").
        where((isTrait("Dimension"))).
        select(id("db1").field("name").as("dbName"), id("tab").field("name").as("tabName")), g, gp
    )
    validateJson(r, "{\n  \"query\":\"DB as db1 where (name = \\\"Sales\\\") Table as tab where DB as db1 where (name = \\\"Sales\\\") Table as tab is Dimension as _src1 select db1.name as dbName, tab.name as tabName\",\n  \"dataType\":{\n    \"typeName\":\"__tempQueryResultStruct5\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"dbName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"tabName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"__tempQueryResultStruct5\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"product_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct5\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"time_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct5\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"customer_dim\"\n    }\n  ]\n}")
  }

  test("testJoinAndSelect2") {
    val r = QueryProcessor.evaluate(
      _class("DB").as("db1").where((id("db1").field("createTime") > int(0))
        .or(id("name").`=`(string("Reporting")))).field("Table").as("tab")
        .select(id("db1").field("name").as("dbName"), id("tab").field("name").as("tabName")), g, gp
    )
    validateJson(r, "{\n  \"query\":\"DB as db1 where (db1.createTime > 0) or (name = \\\"Reporting\\\") Table as tab select db1.name as dbName, tab.name as tabName\",\n  \"dataType\":{\n    \"typeName\":\"__tempQueryResultStruct6\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"dbName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"tabName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"sales_fact\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"product_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"time_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"customer_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Reporting\",\n      \"tabName\":\"sales_fact_daily_mv\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct6\",\n      \"dbName\":\"Reporting\",\n      \"tabName\":\"sales_fact_monthly_mv\"\n    }\n  ]\n}")
  }

  test("testJoinAndSelect3") {
    val r = QueryProcessor.evaluate(
      _class("DB").as("db1").where((id("db1").field("createTime") > int(0))
        .and(id("db1").field("name").`=`(string("Reporting")))
        .or(id("db1").hasField("owner"))).field("Table").as("tab")
        .select(id("db1").field("name").as("dbName"), id("tab").field("name").as("tabName")), g, gp
    )
    validateJson(r, "{\n  \"query\":\"DB as db1 where (db1.createTime > 0) and (db1.name = \\\"Reporting\\\") or db1 has owner Table as tab select db1.name as dbName, tab.name as tabName\",\n  \"dataType\":{\n    \"typeName\":\"__tempQueryResultStruct7\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"dbName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"tabName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"sales_fact\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"product_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"time_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Sales\",\n      \"tabName\":\"customer_dim\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Reporting\",\n      \"tabName\":\"sales_fact_daily_mv\"\n    },\n    {\n      \"$typeName$\":\"__tempQueryResultStruct7\",\n      \"dbName\":\"Reporting\",\n      \"tabName\":\"sales_fact_monthly_mv\"\n    }\n  ]\n}")
  }

  test("testJoinAndSelect4") {
    val r = QueryProcessor.evaluate(
      _class("DB").as("db1").where(id("name").`=`(string("Sales"))).field("Table").as("tab").
        where((isTrait("Dimension"))).
        select(id("db1").as("dbO"), id("tab").field("name").as("tabName")), g, gp
    )
    validateJson(r, "{\n  \"query\":\"DB as db1 where (name = \\\"Sales\\\") Table as tab where DB as db1 where (name = \\\"Sales\\\") Table as tab is Dimension as _src1 select db1 as dbO, tab.name as tabName\",\n  \"dataType\":{\n    \"typeName\":\"\",\n    \"attributeDefinitions\":[\n      {\n        \"name\":\"dbO\",\n        \"dataTypeName\":\"DB\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      },\n      {\n        \"name\":\"tabName\",\n        \"dataTypeName\":\"string\",\n        \"multiplicity\":{\n          \"lower\":0,\n          \"upper\":1,\n          \"isUnique\":false\n        },\n        \"isComposite\":false,\n        \"isUnique\":false,\n        \"isIndexable\":true,\n        \"reverseAttributeName\":null\n      }\n    ]\n  },\n  \"rows\":[\n    {\n      \"$typeName$\":\"\",\n      \"dbO\":{\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"tabName\":\"product_dim\"\n    },\n    {\n      \"$typeName$\":\"\",\n      \"dbO\":{\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"tabName\":\"time_dim\"\n    },\n    {\n      \"$typeName$\":\"\",\n      \"dbO\":{\n        \"$typeName$\":\"DB\",\n        \"version\":0\n      },\n      \"tabName\":\"customer_dim\"\n    }\n  ]\n}")
  }

  test("testArrayComparision") {
    val p = new QueryParser
    val e = p("Partition as p where values = ['2015-01-01']," +
      " table where name = 'sales_fact_daily_mv'," +
      " db where name = 'Reporting' and clusterName = 'test' select p").right.get
    val r = QueryProcessor.evaluate(e, g, gp)
    validateJson(r, """{
                      |  "query":"Partition as p where (values = [\"2015-01-01\"]) table where (name = \"sales_fact_daily_mv\") db where (name = \"Reporting\") and (clusterName = \"test\") as _src1 select p as _col_0",
                      |  "dataType":{
                      |    "typeName":"__tempQueryResultStruct2",
                      |    "attributeDefinitions":[
                      |      {
                      |        "name":"_col_0",
                      |        "dataTypeName":"Partition",
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
                      |      "$typeName$":"__tempQueryResultStruct2",
                      |      "_col_0":{
                      |        "$typeName$":"Partition",
                      |        "version":0
                      |      }
                      |    }
                      |  ]
                      |}""".stripMargin)
  }

  test("testArrayComparisionWithSelectOnArray") {
    val p = new QueryParser
    val e = p("Partition as p where values = ['2015-01-01']," +
      " table where name = 'sales_fact_daily_mv'," +
      " db where name = 'Reporting' and clusterName = 'test' select p.values").right.get
    val r = QueryProcessor.evaluate(e, g, gp)
    validateJson(r,
      """{
        |  "query":"Partition as p where (values = [\"2015-01-01\"]) table where (name = \"sales_fact_daily_mv\") db where (name = \"Reporting\") and (clusterName = \"test\") as _src1 select p.values as _col_0",
        |  "dataType":{
        |    "typeName":"__tempQueryResultStruct2",
        |    "attributeDefinitions":[
        |  {
        |    "name":"_col_0",
        |    "dataTypeName":"array<string>",
        |    "multiplicity":{
        |    "lower":0,
        |    "upper":1,
        |    "isUnique":false
        |  },
        |    "isComposite":false,
        |    "isUnique":false,
        |    "isIndexable":true,
        |    "reverseAttributeName":null
        |  }
        |    ]
        |  },
        |  "rows":[
        |  {
        |    "$typeName$":"__tempQueryResultStruct2",
        |    "_col_0":[
        |    "2015-01-01"
        |    ]
        |  }
        |  ]
        |}
      """.stripMargin)
  }

  test("testArrayInWhereClause") {
    val p = new QueryParser
    val e = p("Partition as p where values = ['2015-01-01']").right.get
    val r = QueryProcessor.evaluate(e, g, gp)
    validateJson(r, """{
                      |  "query":"Partition as p where (values = [\"2015-01-01\"])",
                      |  "dataType":{
                      |    "superTypes":[
                      |
                      |    ],
                      |    "hierarchicalMetaTypeName":"org.apache.atlas.typesystem.types.ClassType",
                      |    "typeName":"Partition",
                      |    "attributeDefinitions":[
                      |      {
                      |        "name":"values",
                      |        "dataTypeName":"array<string>",
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
                      |        "name":"table",
                      |        "dataTypeName":"Table",
                      |        "multiplicity":{
                      |          "lower":1,
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
                      |      "$typeName$":"Partition",
                      |      "$id$":{
                      |        "$typeName$":"Partition",
                      |        "version":0
                      |      },
                      |      "values":[
                      |        "2015-01-01"
                      |      ],
                      |      "table":{
                      |        "$typeName$":"Table",
                      |        "version":0
                      |      }
                      |    }
                      |  ]
                      |}""".stripMargin)
  }

  test("testArrayWithStruct") {
//    val p = new QueryParser
//    val e = p("from LoadProcess select inputTables").right.get
//    val r = QueryProcessor.evaluate(e, g)
    val r = QueryProcessor.evaluate(_class("LoadProcess").field("inputTables"), g, gp)
    validateJson(r)
  }


  test("testNegativeInvalidType") {
    val p = new QueryParser
    val e = p("from blah").right.get
    an[ExpressionException] should be thrownBy QueryProcessor.evaluate(e, g, gp)
  }

  test("testJoinAndSelect5") {
    val p = new QueryParser
    val e = p("Table as t where name = 'sales_fact' db where name = 'Sales' and owner = 'John ETL' select t").right.get
    val r = QueryProcessor.evaluate(e, g, gp)
    validateJson(r)
  }
}
