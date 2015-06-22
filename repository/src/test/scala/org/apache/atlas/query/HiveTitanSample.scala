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

import java.io.File
import java.util.{Date, UUID}
import java.util.concurrent.atomic.AtomicInteger
import javax.script.{Bindings, ScriptEngine, ScriptEngineManager}

import com.thinkaurelius.titan.core.TitanGraph
import com.typesafe.config.ConfigFactory
import org.apache.atlas.repository.BaseTest
import org.apache.atlas.typesystem.types.TypeSystem
import org.apache.commons.io.FileUtils

import scala.collection.mutable.ArrayBuffer

object HiveTitanSample {

    private var nextVertexId: AtomicInteger = new AtomicInteger(0)
    private var nextEdgeId: AtomicInteger = new AtomicInteger(1000)

    trait Vertex {
        val _id: String

        def id = _id
        val version = 0
        val guid = s"""${UUID.randomUUID()}""".stripMargin

        def addEdge(to: Vertex, label: String, edges: ArrayBuffer[String]): Unit = {
            edges +=
                s"""{"_id" : "${nextEdgeId.incrementAndGet()}", "_type" : "edge", "_inV" : "${to.id}", "_outV" : "$id", "_label" : "$label"}"""
        }

        def toGSon(vertices: ArrayBuffer[String],
                   edges: ArrayBuffer[String]): Unit = {

            val sb = new StringBuilder
            sb.append( s"""{"typeName" : "${this.getClass.getSimpleName}", "_type" : "vertex"""")

            this.getClass.getDeclaredFields filter (_.getName != "traits") foreach { f =>
                f.setAccessible(true)
                val fV = f.get(this)
                val convertedVal = fV match {
                    case _: String => s""""$fV""""
                    case d: Date => d.getTime
                    case _ => fV
                }

                convertedVal match {
                    case x: Vertex => addEdge(x, s"${this.getClass.getSimpleName}.${f.getName}", edges)
                    case l: List[_] => l.foreach(x => addEdge(x.asInstanceOf[Vertex],
                        s"${this.getClass.getSimpleName}.${f.getName}", edges))
                    case _ => sb.append( s""", "${f.getName}" : $convertedVal""")
                        sb.append( s""", "${this.getClass.getSimpleName}.${f.getName}" : $convertedVal""")
                }
            }

            this.getClass.getDeclaredFields filter (_.getName == "traits") foreach { f =>
                f.setAccessible(true)
                var traits = f.get(this).asInstanceOf[Option[List[Trait]]]

                if (traits.isDefined) {
                    val fV = traits.get.map(_.getClass.getSimpleName).mkString(",")
                    sb.append( s""", "traitNames" : "$fV"""")
                }
            }

            sb.append("}")
            vertices += sb.toString()
        }
    }

    trait Trait extends Vertex

    trait Struct extends Vertex

    trait Instance extends Vertex {
        val traits: Option[List[Trait]]

        override def toGSon(vertices: ArrayBuffer[String],
                            edges: ArrayBuffer[String]): Unit = {
            super.toGSon(vertices, edges)

            if (traits.isDefined) {
                traits.get foreach { t =>
                    t.toGSon(vertices, edges)
                    addEdge(t, s"${this.getClass.getSimpleName}.${t.getClass.getSimpleName}", edges)
                }
            }
        }

    }

    case class JdbcAccess(_id: String = "" + nextVertexId.incrementAndGet()) extends Trait

    case class PII(_id: String = "" + nextVertexId.incrementAndGet()) extends Trait

    case class Dimension(_id: String = "" + nextVertexId.incrementAndGet()) extends Trait

    case class Metric(_id: String = "" + nextVertexId.incrementAndGet()) extends Trait

    case class ETL(_id: String = "" + nextVertexId.incrementAndGet()) extends Trait


    case class DB(name: String, owner: String, createTime: Int, traits: Option[List[Trait]] = None,
                  _id: String = "" + nextVertexId.incrementAndGet()) extends Instance

    case class StorageDescriptor(inputFormat: String, outputFormat: String,
                                 _id: String = "" + nextVertexId.incrementAndGet()) extends Struct

    case class Column(name: String, dataType: String, sd: StorageDescriptor,
                      traits: Option[List[Trait]] = None,
                      _id: String = "" + nextVertexId.incrementAndGet()) extends Instance

    case class Table(name: String, db: DB, sd: StorageDescriptor,
                     created: Date,
                     traits: Option[List[Trait]] = None,
                     _id: String = "" + nextVertexId.incrementAndGet()) extends Instance

    case class TableDef(name: String, db: DB, inputFormat: String, outputFormat: String,
                        columns: List[(String, String, Option[List[Trait]])],
                        traits: Option[List[Trait]] = None,
                        created: Option[Date] = None) {
        val createdDate : Date = created match {
            case Some(x) => x
            case None => new Date(BaseTest.TEST_DATE_IN_LONG)
        }
        val sd = StorageDescriptor(inputFormat, outputFormat)
        val colDefs = columns map { c =>
            Column(c._1, c._2, sd, c._3)
        }
        val tablDef = Table(name, db, sd, createdDate, traits)

        def toGSon(vertices: ArrayBuffer[String],
                   edges: ArrayBuffer[String]): Unit = {
            sd.toGSon(vertices, edges)
            colDefs foreach {
                _.toGSon(vertices, edges)
            }
            tablDef.toGSon(vertices, edges)
        }
    }

    case class LoadProcess(name: String, inputTables: List[Vertex],
                           outputTable: Vertex,
                           traits: Option[List[Trait]] = None,
                           _id: String = "" + nextVertexId.incrementAndGet()) extends Instance

    case class View(name: String, db: DB, inputTables: List[Vertex],
                    traits: Option[List[Trait]] = None,
                    _id: String = "" + nextVertexId.incrementAndGet()) extends Instance

    val salesDB = DB("Sales", "John ETL", 1000)
    val salesFact = TableDef("sales_fact",
        salesDB,
        "TextInputFormat",
        "TextOutputFormat",
        List(
            ("time_id", "int", None),
            ("product_id", "int", None),
            ("customer_id", "int", None),
            ("created", "date", None),
            ("sales", "double", Some(List(Metric())))
        ))
    val productDim = TableDef("product_dim",
        salesDB,
        "TextInputFormat",
        "TextOutputFormat",
        List(
            ("product_id", "int", None),
            ("product_name", "string", None),
            ("brand_name", "string", None)
        ),
        Some(List(Dimension())))
    val timeDim = TableDef("time_dim",
        salesDB,
        "TextInputFormat",
        "TextOutputFormat",
        List(
            ("time_id", "int", None),
            ("dayOfYear", "int", None),
            ("weekDay", "string", None)
        ),
        Some(List(Dimension())))
    val customerDim = TableDef("customer_dim",
        salesDB,
        "TextInputFormat",
        "TextOutputFormat",
        List(
            ("customer_id", "int", None),
            ("name", "int", None),
            ("address", "string", Some(List(PII())))
        ),
        Some(List(Dimension())))

    val reportingDB = DB("Reporting", "Jane BI", 1500)
    val salesFactDaily = TableDef("sales_fact_daily_mv",
        reportingDB,
        "TextInputFormat",
        "TextOutputFormat",
        List(
            ("time_id", "int", None),
            ("product_id", "int", None),
            ("customer_id", "int", None),
            ("sales", "double", Some(List(Metric())))
        ))
    val loadSalesFactDaily = LoadProcess("loadSalesDaily",
        List(salesFact.tablDef, timeDim.tablDef), salesFactDaily.tablDef,
        Some(List(ETL())))


    val productDimView = View("product_dim_view", reportingDB,
        List(productDim.tablDef),
        Some(List(Dimension(), JdbcAccess())))

    val customerDimView = View("customer_dim_view", reportingDB,
        List(customerDim.tablDef),
        Some(List(Dimension(), JdbcAccess())))

    val salesFactMonthly = TableDef("sales_fact_monthly_mv",
        reportingDB,
        "TextInputFormat",
        "TextOutputFormat",
        List(
            ("time_id", "int", None),
            ("product_id", "int", None),
            ("customer_id", "int", None),
            ("sales", "double", Some(List(Metric())))
        ))
    val loadSalesFactMonthly = LoadProcess("loadSalesMonthly",
        List(salesFactDaily.tablDef), salesFactMonthly.tablDef,
        Some(List(ETL())))


    val vertices: ArrayBuffer[String] = new ArrayBuffer[String]()
    val edges: ArrayBuffer[String] = new ArrayBuffer[String]()

    salesDB.toGSon(vertices, edges)
    salesFact.toGSon(vertices, edges)
    productDim.toGSon(vertices, edges)
    timeDim.toGSon(vertices, edges)
    customerDim.toGSon(vertices, edges)

    reportingDB.toGSon(vertices, edges)
    salesFactDaily.toGSon(vertices, edges)
    loadSalesFactDaily.toGSon(vertices, edges)
    productDimView.toGSon(vertices, edges)
    customerDimView.toGSon(vertices, edges)
    salesFactMonthly.toGSon(vertices, edges)
    loadSalesFactMonthly.toGSon(vertices, edges)

    def toGSon(): String = {
        s"""{
        "mode":"NORMAL",
        "vertices": ${vertices.mkString("[\n\t", ",\n\t", "\n]")},
        "edges": ${edges.mkString("[\n\t", ",\n\t", "\n]")}
        }
        """.stripMargin
    }

    def writeGson(fileName: String): Unit = {
        FileUtils.writeStringToFile(new File(fileName), toGSon())
    }


    val GremlinQueries = List(
        // 1. List all DBs
        """g.V.has("typeName", "DB")""",

        // 2. List all DB nmes
        """g.V.has("typeName", "DB").name""",

        // 3. List all Tables in Reporting DB
        """g.V.has("typeName", "DB").has("name", "Reporting").inE("Table.db").outV""",
        """g.V.has("typeName", "DB").as("db").inE("Table.db").outV.and(_().back("db").has("name", "Reporting"))""",

        // 4. List all Tables in Reporting DB, list as D.name, Tbl.name
        """
    g.V.has("typeName", "DB").has("name", "Reporting").as("db").inE("Table.db").outV.as("tbl").select{it.name}{it.name}
        """.stripMargin,

        // 5. List all tables that are Dimensions and have the TextInputFormat
        """
    g.V.as("v").and(_().outE("Table.Dimension"), _().out("Table.sd").has("inputFormat", "TextInputFormat")).name
        """.stripMargin,

        // 6. List all tables that are Dimensions or have the TextInputFormat
        """
    g.V.as("v").or(_().outE("Table.Dimension"), _().out("Table.sd").has("inputFormat", "TextInputFormat")).name
        """.stripMargin,

        // 7. List tables that have at least 1 PII column
        """
    g.V.has("typeName", "Table").as("tab").out("Table.sd").in("Column.sd").as("column"). \
      out("Column.PII").select.groupBy{it.getColumn("tab")}{it.getColumn("column")}{[ "c" : it.size]}.cap.scatter.filter{it.value.c > 0}. \
      transform{it.key}.name  """.stripMargin

        // 7.a from Table as tab -> g.V.has("typeName", "Table").as("tab")
        // 7.b sd.Column as column -> out("Table.sd").in("Column.sd").as("column")
        // 7.c is PII -> out("Column.PII")
        // 7.d select tab, column -> select{it}{it}
        // 7.e groupBy tab compute count(column) as c
        // 7.f where c > 0

        // 7.a Alias(Type("Table"), "tab")
        // 7b. Field("sd", Alias(Type("Table"), "tab"))
        //     Alias(Field("Column", Field("sd", Alias(Type("Table"), "tab"))), "column")
        // 7.c Filter(is("PII"), Alias(Field("Column", Field("sd", Alias(Type("Table"), "tab"))), "column"))
        // 7.d
    )
}

object TestApp extends App with GraphUtils {

    var conf = ConfigFactory.load()
    conf = conf.getConfig("graphRepo")
    val g: TitanGraph = titanGraph(conf)
    val manager: ScriptEngineManager = new ScriptEngineManager
    val engine: ScriptEngine = manager.getEngineByName("gremlin-groovy")
    val bindings: Bindings = engine.createBindings
    bindings.put("g", g)

    val hiveGraphFile = FileUtils.getTempDirectory().getPath + File.separator + System.nanoTime() + ".gson"
    HiveTitanSample.writeGson(hiveGraphFile)
    bindings.put("hiveGraphFile", hiveGraphFile)

    try {
        engine.eval("g.loadGraphSON(hiveGraphFile)", bindings)

        println(engine.eval("g.V.typeName.toList()", bindings))

        HiveTitanSample.GremlinQueries.foreach { q =>
            println(q)
            println("Result: " + engine.eval(q + ".toList()", bindings))
        }
    } finally {
        g.shutdown()
    }
}