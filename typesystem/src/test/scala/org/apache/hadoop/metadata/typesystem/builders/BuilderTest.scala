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

package org.apache.hadoop.metadata.typesystem.builders

import org.apache.hadoop.metadata.typesystem.TypesDef
import org.apache.hadoop.metadata.typesystem.types.TypeSystem
import org.scalatest.{BeforeAndAfter, FunSuite}

abstract class BuilderTest extends FunSuite with BeforeAndAfter {

  var tDef : TypesDef = null

  before {
    TypeSystem.getInstance().reset()

    val b = new TypesBuilder
    import b._

    tDef = types {

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
    }
  }

}
