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

import com.google.common.collect.ImmutableSet
import org.apache.atlas.typesystem.Referenceable
import org.apache.atlas.typesystem.types.{DataTypes, TypeSystem}
import org.apache.atlas.typesystem.types.utils.TypesUtil
import org.testng.Assert._
import org.testng.annotations.{BeforeClass, Test}

import scala.util.Random

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
