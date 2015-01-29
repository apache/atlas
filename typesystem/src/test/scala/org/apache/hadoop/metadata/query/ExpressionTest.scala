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

import com.google.common.collect.ImmutableList
import org.apache.hadoop.metadata.BaseTest
import org.apache.hadoop.metadata.types._
import org.junit.{Before, Test}
import Expressions._

class ExpressionTest extends BaseTest {

  @Before
  override def setup {
    super.setup

    def attrDef(name : String, dT : IDataType[_],
                m : Multiplicity = Multiplicity.OPTIONAL,
                isComposite: Boolean = false,
                reverseAttributeName: String = null) = {
      require(name != null)
      require(dT != null)
      new AttributeDefinition(name, dT.getName, m, isComposite, reverseAttributeName)
    }

    def dbClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "DB", null,
      Array(
        attrDef("name", DataTypes.STRING_TYPE),
        attrDef("owner", DataTypes.STRING_TYPE)
      ))

    def storageDescClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "StorageDesc", null,
      Array(
        attrDef("inputFormat", DataTypes.STRING_TYPE),
        attrDef("outputFormat", DataTypes.STRING_TYPE)
      ))

    def columnClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "Column", null,
      Array(
        attrDef("name", DataTypes.STRING_TYPE),
        attrDef("dataType", DataTypes.STRING_TYPE),
        new AttributeDefinition("sd", "StorageDesc", Multiplicity.REQUIRED, false, null)
      ))

    def tblClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "Table", null,
      Array(
        attrDef("name", DataTypes.STRING_TYPE),
        new AttributeDefinition("db", "DB", Multiplicity.REQUIRED, false, null),
        new AttributeDefinition("sd", "StorageDesc", Multiplicity.REQUIRED, false, null)
      ))

    def loadProcessClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "LoadProcess", null,
      Array(
        attrDef("name", DataTypes.STRING_TYPE),
        new AttributeDefinition("inputTables", "Table", Multiplicity.COLLECTION, false, null),
        new AttributeDefinition("outputTable", "Table", Multiplicity.REQUIRED, false, null)
      ))

    def viewClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "View", null,
      Array(
        attrDef("name", DataTypes.STRING_TYPE),
        new AttributeDefinition("inputTables", "Table", Multiplicity.COLLECTION, false, null)
      ))

    def dimTraitDef = new HierarchicalTypeDefinition[TraitType](classOf[TraitType], "Dimension", null,
      Array[AttributeDefinition]())
    def piiTraitDef = new HierarchicalTypeDefinition[TraitType](classOf[TraitType], "PII", null,
      Array[AttributeDefinition]())
    def metricTraitDef = new HierarchicalTypeDefinition[TraitType](classOf[TraitType], "Metric", null,
      Array[AttributeDefinition]())
    def etlTraitDef = new HierarchicalTypeDefinition[TraitType](classOf[TraitType], "ETL", null,
      Array[AttributeDefinition]())
    def jdbcTraitDef = new HierarchicalTypeDefinition[TraitType](classOf[TraitType], "Jdbc", null,
      Array[AttributeDefinition]())

    getTypeSystem.defineTypes(ImmutableList.of[StructTypeDefinition],
      ImmutableList.of[HierarchicalTypeDefinition[TraitType]](dimTraitDef, piiTraitDef,
        metricTraitDef, etlTraitDef, jdbcTraitDef),
      ImmutableList.of[HierarchicalTypeDefinition[ClassType]](dbClsDef, storageDescClsDef, columnClsDef, tblClsDef,
        loadProcessClsDef, viewClsDef))

  }

  @Test def testClass: Unit = {
    val e = QueryProcessor.validate(_class("DB"))
    println(e)
  }

  @Test def testFilter: Unit = {
    val e = QueryProcessor.validate(_class("DB").where(id("name").`=`(string("Reporting"))))
    println(e)
  }

  @Test def testSelect: Unit = {
    val e = QueryProcessor.validate(_class("DB").where(id("name").`=`(string("Reporting"))).
    select(id("name"), id("owner")))
    println(e)
  }

  @Test def testNegTypeTest: Unit = {
    try {
      val e = QueryProcessor.validate(_class("DB").where(id("name")))
      println(e)
    } catch {
      case e : ExpressionException if e.getMessage.endsWith("expression: DB where name") => ()
    }
  }

  @Test def testIsTrait: Unit = {
    val e = QueryProcessor.validate(_class("DB").where(isTrait("Jdbc")))
    println(e)
  }

  @Test def testIsTraitNegative: Unit = {
    try {
    val e = QueryProcessor.validate(_class("DB").where(isTrait("Jdb")))
    println(e)
    } catch {
      case e : ExpressionException if e.getMessage.endsWith("not a TraitType, expression:  is Jdb") => ()
    }
  }

  @Test def testhasField: Unit = {
    val e = QueryProcessor.validate(_class("DB").where(hasField("name")))
    println(e)
  }

  @Test def testHasFieldNegative: Unit = {
    try {
      val e = QueryProcessor.validate(_class("DB").where(hasField("nam")))
      println(e)
    } catch {
      case e : ExpressionException if e.getMessage.endsWith("not a TraitType, expression:  is Jdb") => ()
    }
  }

  @Test def testFieldReference: Unit = {
    val e = QueryProcessor.validate(_class("DB").field("Table"))
    println(e)
  }

  @Test def testBackReference: Unit = {
    val e = QueryProcessor.validate(
      _class("DB").as("db").field("Table")).where(id("db").field("name").`=`(string("Reporting")))
    println(e)
  }
}
