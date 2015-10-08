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

import org.apache.atlas.AtlasException
import org.apache.atlas.typesystem.types.{ClassType, Multiplicity, TypeSystem}
import org.testng.annotations.{BeforeMethod,Test}

class MultiplicityTest {

  @BeforeMethod
  def beforeAll {
    TypeSystem.getInstance().reset()

    val b = new TypesBuilder
    import b._

    val tDef = types {

      _trait("Dimension") {}
      _trait("PII") {}
      _trait("Metric") {}
      _trait("ETL") {}
      _trait("JdbcAccess") {}

      _class("DB") {
        "name" ~ (string, required, indexed, unique)
        "owner" ~ (string)
        "createTime" ~ (int)
      }

      _class("StorageDesc") {
        "inputFormat" ~ (string, required)
        "outputFormat" ~ (string, required)
      }

      _class("Column") {
        "name" ~ (string, required)
        "dataType" ~ (string, required)
        "sd" ~ ("StorageDesc", required)
      }

      _class("Table", List()) {
        "name" ~ (string,  required,  indexed)
        "db" ~ ("DB", required)
        "sd" ~ ("StorageDesc", required)
      }

      _class("LoadProcess") {
        "name" ~ (string, required)
        "inputTables" ~ (array("Table"), collection)
        "outputTable" ~ ("Table", required)

      }

      _class("View") {
        "name" ~ (string, required)
        "inputTables" ~ (array("Table"), collection)
      }

      _class("AT") {
        "name" ~ (string, required)
        "stringSet" ~ (array("string"), multiplicty(0, Int.MaxValue, true))
      }
    }

    TypeSystem.getInstance().defineTypes(tDef)
  }

  @Test
  def test1 {

    val b = new InstanceBuilder
    import b._

    val instances = b create {
      val a = instance("AT") {  // use instance to create Referenceables. use closure to
        // set attributes of instance
        'name ~ "A1"                  // use '~' to set attributes. Use a Symbol (names starting with ') for
        'stringSet ~ Seq("a", "a")
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

  @Test(expectedExceptions =  Array(classOf[AtlasException]) , expectedExceptionsMessageRegExp = "A multiplicty of more than one requires a collection type for attribute 'stringSet'")
  def WrongMultiplicity {
    val b = new TypesBuilder
    import b._
    val tDef = types {
      _class("Wrong") {
        "name" ~ (string, required)
        "stringSet" ~ (string, multiplicty(0, Int.MaxValue, true))
      }
    }
    TypeSystem.getInstance().defineTypes(tDef)
  }
}
