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

import java.io.File
import javax.script.{Bindings, ScriptEngine, ScriptEngineManager}

import com.google.common.collect.ImmutableList
import com.thinkaurelius.titan.core.{TitanFactory, TitanGraph}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.configuration.{ConfigurationException, MapConfiguration, Configuration}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.metadata.types._

trait GraphUtils {
  import scala.collection.JavaConversions._

  def getConfiguration(config : Config) : Configuration = {
    val keys = config.entrySet().map { _.getKey}
    val gConfig : java.util.Map[String, String] = new java.util.HashMap[String, String]()
    keys.foreach { k =>
      gConfig.put(k, config.getString(k))
    }
    return new MapConfiguration(gConfig)
  }


  def titanGraph(conf : Config) = {
    try {
      TitanFactory.open(getConfiguration(conf))
    } catch  {
      case e : ConfigurationException =>  throw new RuntimeException(e)
    }
  }
}

object QueryTestsUtils extends GraphUtils {

  def setupTypes : Unit = {
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
        attrDef("owner", DataTypes.STRING_TYPE),
        attrDef("createTime", DataTypes.LONG_TYPE)
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

    TypeSystem.getInstance().defineTypes(ImmutableList.of[StructTypeDefinition],
      ImmutableList.of[HierarchicalTypeDefinition[TraitType]](dimTraitDef, piiTraitDef,
        metricTraitDef, etlTraitDef, jdbcTraitDef),
      ImmutableList.of[HierarchicalTypeDefinition[ClassType]](dbClsDef, storageDescClsDef, columnClsDef, tblClsDef,
        loadProcessClsDef, viewClsDef))

    ()
  }

  def setupTestGraph : TitanGraph = {
    var conf = ConfigFactory.load()
    conf = conf.getConfig("graphRepo")
    val g = titanGraph(conf)
    val manager: ScriptEngineManager = new ScriptEngineManager
    val engine: ScriptEngine = manager.getEngineByName("gremlin-groovy")
    val bindings: Bindings = engine.createBindings
    bindings.put("g", g)

    val hiveGraphFile = FileUtils.getTempDirectory().getPath.toString + File.separator + System.nanoTime() + ".gson"
    HiveTitanSample.writeGson(hiveGraphFile)
    bindings.put("hiveGraphFile", hiveGraphFile)

    engine.eval("g.loadGraphSON(hiveGraphFile)", bindings)
    g
  }
}
