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
import javax.script.{Bindings, ScriptEngine, ScriptEngineManager}

import com.google.common.collect.ImmutableList
import org.apache.atlas.repository.graphdb.AtlasVertex
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.atlas.typesystem.types._
import org.apache.commons.configuration.{Configuration, ConfigurationException, MapConfiguration}
import org.apache.commons.io.FileUtils
import org.apache.commons.lang.RandomStringUtils
import org.json.JSONObject
import org.skyscreamer.jsonassert.JSONAssert

import scala.util.Random
import org.apache.atlas.repository.MetadataRepository
import org.apache.atlas.repository.graphdb.AtlasGraph
import org.apache.atlas.repository.graph.AtlasGraphProvider
import java.net.URL

import org.apache.atlas.`type`.AtlasTypeRegistry
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer
import org.apache.atlas.typesystem.TypesDef
import org.apache.atlas.typesystem.ITypedReferenceableInstance


trait GraphUtils {

    import scala.collection.JavaConversions._

    def getConfiguration(config: Config): Configuration = {
        val keys = config.entrySet().map {
            _.getKey
        }
        val gConfig: java.util.Map[String, String] = new java.util.HashMap[String, String]()
        keys.foreach { k =>
            gConfig.put(k, config.getString(k))
        }
        return new MapConfiguration(gConfig)
    }


    def graph(conf: Configuration) = {
        try {
            val g = AtlasGraphProvider.getGraphInstance
            val mgmt = g.getManagementSystem
            val typname = mgmt.makePropertyKey("typeName", classOf[String], null);
            mgmt.createExactMatchIndex("byTypeName", false, List(typname));
            mgmt.commit()
            g
        } catch {
            case e: ConfigurationException => throw new RuntimeException(e)
        }
    }
}

object QueryTestsUtils extends GraphUtils {

     def setupTypesAndIndices() : Unit = {
        val indexer = new GraphBackedSearchIndexer(new AtlasTypeRegistry());
        val typesDef : TypesDef = defineTypes;
        val newTypes = TypeSystem.getInstance.defineTypes(typesDef);
        indexer.onAdd(newTypes.values());        
    }
     
     def setupTypes: Unit = {
        
        val types : TypesDef = defineTypes;
        TypeSystem.getInstance.defineTypes(types);
    }
    
     
    def defineTypes: TypesDef = {
        def attrDef(name: String, dT: IDataType[_],
                    m: Multiplicity = Multiplicity.OPTIONAL,
                    isComposite: Boolean = false,
                    reverseAttributeName: String = null) = {
            require(name != null)
            require(dT != null)
            new AttributeDefinition(name, dT.getName, m, isComposite, reverseAttributeName)
        }

        def dbClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "DB", null, null,
            Array(
                attrDef("name", DataTypes.STRING_TYPE),
                attrDef("owner", DataTypes.STRING_TYPE),
                attrDef("createTime", DataTypes.INT_TYPE),
                attrDef("clusterName", DataTypes.STRING_TYPE)
            ))

        def hiveOrderDef = new StructTypeDefinition("HiveOrder",
            Array(
                attrDef("col", DataTypes.STRING_TYPE),
                attrDef("order", DataTypes.INT_TYPE)
            ))

        def storageDescClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "StorageDescriptor", null, null,
            Array(
                attrDef("inputFormat", DataTypes.STRING_TYPE),
                attrDef("outputFormat", DataTypes.STRING_TYPE),
                new AttributeDefinition("sortCols", DataTypes.arrayTypeName("HiveOrder"), Multiplicity.REQUIRED, false, null)
            ))

        def columnClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "Column", null, null,
            Array(
                attrDef("name", DataTypes.STRING_TYPE),
                attrDef("dataType", DataTypes.STRING_TYPE),
                new AttributeDefinition("sd", "StorageDescriptor", Multiplicity.REQUIRED, false, null)
            ))

        def tblClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "Table", null, null,
            Array(
                attrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("db", "DB", Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("sd", "StorageDescriptor", Multiplicity.REQUIRED, false, null),
                attrDef("created", DataTypes.DATE_TYPE)
            ))

        def partitionClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "Partition", null, null,
            Array(
                new AttributeDefinition("values", DataTypes.arrayTypeName(DataTypes.STRING_TYPE.getName), Multiplicity.REQUIRED, false, null),
                new AttributeDefinition("table", "Table", Multiplicity.REQUIRED, false, null)
            ))

        def loadProcessClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "LoadProcess", null, null,
            Array(
                attrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("inputTables", DataTypes.arrayTypeName("Table"), Multiplicity.COLLECTION, false, null),
                new AttributeDefinition("outputTable", "Table", Multiplicity.REQUIRED, false, null)
            ))

        def viewClsDef = new HierarchicalTypeDefinition[ClassType](classOf[ClassType], "View", null, null,
            Array(
                attrDef("name", DataTypes.STRING_TYPE),
                new AttributeDefinition("inputTables", DataTypes.arrayTypeName("Table"), Multiplicity.COLLECTION, false, null)
            ))

        def dimTraitDef = new HierarchicalTypeDefinition[TraitType](classOf[TraitType], "Dimension", null, null,
            Array[AttributeDefinition]())
        def piiTraitDef = new HierarchicalTypeDefinition[TraitType](classOf[TraitType], "PII", null, null,
            Array[AttributeDefinition]())
        def metricTraitDef = new HierarchicalTypeDefinition[TraitType](classOf[TraitType], "Metric", null, null,
            Array[AttributeDefinition]())
        def etlTraitDef = new HierarchicalTypeDefinition[TraitType](classOf[TraitType], "ETL", null, null,
            Array[AttributeDefinition]())
        def jdbcTraitDef = new HierarchicalTypeDefinition[TraitType](classOf[TraitType], "JdbcAccess", null, null,
            Array[AttributeDefinition]())

        TypesDef(Seq[EnumTypeDefinition](),
           Seq[StructTypeDefinition](hiveOrderDef),
            Seq[HierarchicalTypeDefinition[TraitType]](dimTraitDef, piiTraitDef,
                metricTraitDef, etlTraitDef, jdbcTraitDef),
            Seq[HierarchicalTypeDefinition[ClassType]](dbClsDef, storageDescClsDef, columnClsDef, tblClsDef,
                partitionClsDef, loadProcessClsDef, viewClsDef))
    }

    def setupTestGraph(repo : MetadataRepository): AtlasGraph[_,_] = {

        val g = AtlasGraphProvider.getGraphInstance();
        val entities = HiveTitanSample.getEntitiesToCreate();
        repo.createEntities(entities:_*)   
        g.commit();
        g
    }
    
   

}



trait BaseGremlinTest {
  val STRUCT_NAME_REGEX = (TypeUtils.TEMP_STRUCT_NAME_PREFIX + "\\d+").r
  def validateJson(r: GremlinQueryResult, expected: String = null): Unit = {
    val rJ = r.toJson
    if (expected != null) {
      val a = STRUCT_NAME_REGEX.replaceAllIn(rJ, "")
      val b = STRUCT_NAME_REGEX.replaceAllIn(expected, "")
      val actualjsonObj = new JSONObject(a)
      val expectedjsonObj = new JSONObject(b)
      JSONAssert.assertEquals(expectedjsonObj, actualjsonObj, false)
    } else {
      println(rJ)
    }
  }

}
