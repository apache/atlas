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

package org.apache.atlas.typesystem.json

import scala.util.Random

import org.apache.atlas.typesystem.Referenceable
import org.apache.atlas.typesystem.persistence.Id
import org.apache.atlas.typesystem.types.DataTypes
import org.apache.atlas.typesystem.types.TypeSystem
import org.apache.atlas.typesystem.types.utils.TypesUtil
import org.testng.Assert.assertEquals
import org.testng.Assert.assertNotNull
import org.testng.Assert.assertTrue
import org.testng.annotations.BeforeClass
import org.testng.annotations.Test

import com.google.common.collect.ImmutableSet

class InstanceSerializationTest {
  private var typeName: String = null

  @BeforeClass def setup {
    typeName = "Random_" + Math.abs(Random.nextInt())
    val clsType = TypesUtil.createClassTypeDef(typeName, "Random-description", ImmutableSet.of[String](),
      TypesUtil.createRequiredAttrDef("name", DataTypes.STRING_TYPE))
    TypeSystem.getInstance().defineClassType(clsType)
  }

  @Test def testIdentity {
    val entity: Referenceable = new Referenceable(typeName)
    val json: String = InstanceSerialization.toJson(entity, true)
    val entity2: Referenceable = InstanceSerialization.fromJsonReferenceable(json, true)
    assertNotNull(entity2)
    assertEquals(entity2.getId, entity.getId, "Simple conversion failed")
    assertEquals(entity2.getTraits, entity.getTraits, "Traits mismatch")
  }

  @Test def testReferenceArrayWithNoState {
      
      val staticJson = """{ 
        "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Reference",
        "id": {
            "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
            "version": 0,
            "typeName": "LoadProcess"
        },
        "typeName": "LoadProcess",
        "values": {
            "inputTables": [{
                    "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
                    "id": "bacfa996-e88e-4d7e-9630-68c9829b10b4",
                    "version": 0,
                    "typeName": "Table"
                }, {
                    "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
                    "id": "6da06805-3f56-446f-8831-672a65ac2199",
                    "version": 0,
                    "typeName": "Table"
                }

            ],
            "outputTable": {
                "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Id",
                "id": "d5c3d6d0-aa10-44c1-b05d-ed9400d2a5ac",
                "version": 0,
                "typeName": "Table"
            },
            "name": "loadSalesDaily"
        },
        "traitNames": [
            "ETL"
        ],
        "traits": {
            "ETL": {
                "jsonClass": "org.apache.atlas.typesystem.json.InstanceSerialization$_Struct",
                "typeName": "ETL",
                "values": {

                }
            }
        }
    }
    """;
      
      val entity: Referenceable = InstanceSerialization.fromJsonReferenceable(staticJson, true)
      val outputTable = entity.getValuesMap.get("outputTable")
      val inputTables : java.util.List[_] = entity.getValuesMap().get("inputTables").asInstanceOf[java.util.List[_]]
     
      assertTrue(outputTable.isInstanceOf[Id]);
      import scala.collection.JavaConversions._
      for(inputTable <- inputTables) {
          assertTrue(inputTable.isInstanceOf[Id]);
      }
  }
  
  @Test def testMissingStateInId: Unit = {
    val entity: Referenceable = new Referenceable(typeName)
    val json: String = InstanceSerialization.toJson(entity, true)
    val staticJson: String = "{\n" +
      "  \"jsonClass\":\"org.apache.atlas.typesystem.json.InstanceSerialization$_Reference\",\n" +
      "  \"id\":{\n" +
      "    \"jsonClass\":\"org.apache.atlas.typesystem.json.InstanceSerialization$_Id\",\n" +
      "    \"id\":\"" + entity.getId.id + "\",\n" +
      "    \"version\":0,\n" +
      "    \"typeName\":\"" + entity.getTypeName + "\",\n" +
      "  },\n" +
      "  \"typeName\":\"" + entity.getTypeName + "\",\n" +
      "  \"values\":{}\n" +
      "  \"traitNames\":[]\n" +
      "  \"traits\":{}\n" +
      "}"
    val entity2: Referenceable = InstanceSerialization.fromJsonReferenceable(staticJson, true)
    assertNotNull(entity2)
    assertNotNull(entity2.getId)
    assertNotNull(entity2.getId.id) // This creates a new id so the values will not match.
    assertEquals(entity2.getId.typeName, entity.getId.typeName)
    assertEquals(entity2.getId.version, entity.getId.version)
    assertEquals(entity2.getId.state, entity.getId.state)
    assertEquals(entity2.getTypeName, entity.getTypeName, "Type name mismatch")
    assertEquals(entity2.getValuesMap, entity.getValuesMap, "Values mismatch")
    assertEquals(entity2.getTraits, entity.getTraits, "Traits mismatch")
  }

  @Test def testMissingId: Unit = {
    val entity: Referenceable = new Referenceable(typeName)
    val json: String = InstanceSerialization.toJson(entity, true)
    val staticJson: String = "{\n" +
      "  \"jsonClass\":\"org.apache.atlas.typesystem.json.InstanceSerialization$_Reference\",\n" +
      "  \"typeName\":\"" + entity.getTypeName + "\",\n" +
      "  \"values\":{}\n" +
      "  \"traitNames\":[],\n" +
      "  \"traits\":{}\n" +
      "}"
    val entity2: Referenceable = InstanceSerialization.fromJsonReferenceable(staticJson, true)
    assertNotNull(entity2)
    assertNotNull(entity2.getId)
    assertNotNull(entity2.getId.id) // This creates a new id so the values will not match.
    assertEquals(entity2.getId.typeName, entity.getId.typeName)
    assertEquals(entity2.getId.version, entity.getId.version)
    assertEquals(entity2.getId.state, entity.getId.state)
    assertEquals(entity2.getTypeName, entity.getTypeName, "Type name mismatch")
    assertEquals(entity2.getValuesMap, entity.getValuesMap, "Values mismatch")
    assertEquals(entity2.getTraits, entity.getTraits, "Traits mismatch")
  }
}
