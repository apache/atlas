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

import scala.collection.JavaConversions._


import org.apache.atlas.typesystem.ITypedReferenceableInstance
import org.apache.atlas.typesystem.json.TypedReferenceableInstanceSerializer
import org.apache.atlas.utils.HiveModel.Column
import org.apache.atlas.utils.HiveModel.DB
import org.apache.atlas.utils.HiveModel.HiveOrder
import org.apache.atlas.utils.HiveModel.LoadProcess
import org.apache.atlas.utils.HiveModel.Partition
import org.apache.atlas.utils.HiveModel.StorageDescriptor
import org.apache.atlas.utils.HiveModel.Table
import org.apache.atlas.utils.HiveModel.View
import scala.collection.mutable.Buffer



object HiveTitanSample {
       
    val MetricTrait = "Metric"
    val DimensionTrait = "Dimension"
    val ETLTrait = "ETL"
    val JdbcAccessTrait = "JdbcAccess"
    
    val salesDB = new DB("Sales", "John ETL", 1000, "test")

    
    
    val salesFact = new Table("sales_fact",
        salesDB,
        new StorageDescriptor("TextInputFormat",
            "TextOutputFormat", List(new HiveOrder("customer_id", 0))),
            List(
                new Column("time_id", "int"),
                new Column("product_id", "int"),
                new Column("customer_id", "int"),
                new Column("created", "date"),
                new Column("sales", "double").withTrait(MetricTrait)
                )
        );
    
    
    val productDim = new Table("product_dim",
        salesDB,
        new StorageDescriptor("TextInputFormat",
            "TextOutputFormat", List(new HiveOrder("product_id", 0))),
        List(
            new Column("product_id", "int"),
            new Column("product_name", "string"),
            new Column("brand_name", "string")
        )
    ).withTrait(DimensionTrait)

    val timeDim = new Table("time_dim",
        salesDB,
        new StorageDescriptor("TextInputFormat",
            "TextOutputFormat", List(new HiveOrder("time_id", 0))),
        List(
             new Column("time_id", "int"),
             new Column("dayOfYear", "int"),
             new Column("weekDay", "string")
        )
    ).withTrait(DimensionTrait)
        
    val customerDim = new Table("customer_dim",
        salesDB,
        new StorageDescriptor("TextInputFormat",
            "TextOutputFormat", List(new HiveOrder("customer_id", 0))),
        List(
             new Column("customer_id", "int"),
             new Column("name", "int"),
             new Column("address", "string").withTrait("PII")
        )
    ).withTrait(DimensionTrait)


    val reportingDB = new DB("Reporting", "Jane BI", 1500, "test")
    val salesFactDaily = new Table("sales_fact_daily_mv",
        reportingDB,
        new StorageDescriptor("TextInputFormat",
            "TextOutputFormat", List(new HiveOrder("customer_id", 0))),
        List(
             new Column("time_id", "int"),
             new Column("product_id", "int"),
             new Column("customer_id", "int"),
             new Column("sales", "double").withTrait(MetricTrait)
        )
    )
    
    val loadSalesFactDaily = new LoadProcess(
            "loadSalesDaily",
            List(salesFact, timeDim), 
            salesFactDaily
    ).withTrait(ETLTrait)
        


    val productDimView = new View(
        "product_dim_view", 
        reportingDB,
        List(productDim)
    ).withTraits(List(DimensionTrait, JdbcAccessTrait))

    val customerDimView = new View(
        "customer_dim_view", 
        reportingDB,
        List(customerDim)
        
    ).withTraits(List(DimensionTrait, JdbcAccessTrait))

    val salesFactMonthly = new Table("sales_fact_monthly_mv",
        reportingDB,
        new StorageDescriptor(
                "TextInputFormat",
                "TextOutputFormat", 
                List(new HiveOrder("customer_id", 0))
        ),
        List(
             new Column("time_id", "int"),
             new Column("product_id", "int"),
             new Column("customer_id", "int"),
             new Column("sales", "double").withTrait(MetricTrait)
        )
    )
    val loadSalesFactMonthly = new LoadProcess("loadSalesMonthly",
        List(salesFactDaily), salesFactMonthly).withTraits(List(ETLTrait))

    val salesDailyPartition = new Partition(List("2015-01-01"), salesFactDaily)
   
    import scala.collection.JavaConversions._
   
   def getEntitiesToCreate() : Buffer[ITypedReferenceableInstance] = {
       var list = salesDB.getTypedReferencebles() ++
          salesFact.getTypedReferencebles() ++
          productDim.getTypedReferencebles() ++
          timeDim.getTypedReferencebles() ++
          customerDim.getTypedReferencebles() ++
          reportingDB.getTypedReferencebles() ++
          salesFactDaily.getTypedReferencebles() ++
          loadSalesFactDaily.getTypedReferencebles() ++
          productDimView.getTypedReferencebles() ++
          customerDimView.getTypedReferencebles() ++
          salesFactMonthly.getTypedReferencebles() ++
          loadSalesFactMonthly.getTypedReferencebles() ++
          salesDailyPartition.getTypedReferencebles();
       return list;
       
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

//object TestApp extends App with GraphUtils {
//
//    val g: TitanGraph = TitanGraphProvider.getGraphInstance
//    val manager: ScriptEngineManager = new ScriptEngineManager
//    val engine: ScriptEngine = manager.getEngineByName("gremlin-groovy")
//    val bindings: Bindings = engine.createBindings
//    bindings.put("g", g)
//
//    val hiveGraphFile = FileUtils.getTempDirectory().getPath + File.separator + System.nanoTime() + ".gson"
//    HiveTitanSample.writeGson(hiveGraphFile)
//    bindings.put("hiveGraphFile", hiveGraphFile)
//
//    try {
//        engine.eval("g.loadGraphSON(hiveGraphFile)", bindings)
//
//        println(engine.eval("g.V.typeName.toList()", bindings))
//
//        HiveTitanSample.GremlinQueries.foreach { q =>
//            println(q)
//            println("Result: " + engine.eval(q + ".toList()", bindings))
//        }
//    } finally {
//        g.shutdown()
//    }
//}