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

package org.apache.atlas.typesystem.builders

import org.apache.atlas.typesystem.types.{ClassType, Multiplicity, TypeSystem}
import org.testng.annotations.Test

class InstanceBuilderTest extends BuilderTest {

  @Test def test1 {
    TypeSystem.getInstance().defineTypes(tDef)

    val b = new InstanceBuilder
    import b._

    val instances = b create {

      val salesDB = instance("DB") {  // use instance to create Referenceables. use closure to
                                         // set attributes of instance
        'name ~ "Sales"                  // use '~' to set attributes. Use a Symbol (names starting with ') for
                                         // attribute names.
        'owner ~ "John ETL"
        'createTime ~ 1000
      }

      val salesFact = instance("Table") {
        'name ~ "sales_fact"
        'db ~ salesDB
        val sd = instance("StorageDesc") {    // any valid scala allowed in closure.
          'inputFormat ~ "TextIputFormat"
          'outputFormat ~ "TextOutputFormat"
        }
        'sd ~ sd                              // use ~ to set references, collections and maps.
        val columns = Seq(
          instance("Column") {
            'name ~ "time_id"
            'dataType ~ "int"
            'sd ~ sd
          },
          instance("Column") {
            'name ~ "product_id"
            'dataType ~ "int"
            'sd ~ sd
          },
          instance("Column") {
            'name ~ "customer_id"
            'dataType ~ "int"
            'sd ~ sd
          },
          instance("Column", "Metric") {
            'name ~ "sales"
            'dataType ~ "int"
            'sd ~ sd
            'Metric("x") ~ 1                // use 'TraitName("attrName") to set values on traits.
          }
        )

        'columns ~ columns

      }

      salesFact.sd.inputFormat ~ "TextInputFormat"   // use dot navigation to alter attributes in the object graph.
                                                     // here I am fixing the typo in "TextInputFormat"
      // dot navigation also works for arrays.
      // here I am fixing column(3). Metric trait has no attributes.
      val c = salesFact.columns
      c(3) = instance("Column", "Metric") {
        'name ~ "sales"
        'dataType ~ "int"
        'sd ~ salesFact.sd
      }

    }

    val ts = TypeSystem.getInstance()

    import scala.collection.JavaConversions._
    val typedInstances = instances.map { i =>
      val iTyp = ts.getDataType(classOf[ClassType], i.getTypeName)
      iTyp.convert(i, Multiplicity.REQUIRED)
    }

    typedInstances.foreach { i =>
      println(i)
    }

  }

}
