/**
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

import com.google.common.collect.ImmutableList
import org.apache.atlas.typesystem.types._
import org.testng.Assert
import org.testng.annotations.Test

class TypesSerializationTest extends BaseTest with TypeHelpers {

    @Test def test1: Unit = {

        val ts = getTypeSystem

        val sDef = structDef("ts1", requiredAttr("a", DataTypes.INT_TYPE),
            optionalAttr("b", DataTypes.BOOLEAN_TYPE),
            optionalAttr("c", DataTypes.BYTE_TYPE),
            optionalAttr("d", DataTypes.SHORT_TYPE),
            optionalAttr("e", DataTypes.INT_TYPE),
            optionalAttr("f", DataTypes.INT_TYPE),
            optionalAttr("g", DataTypes.LONG_TYPE),
            optionalAttr("h", DataTypes.FLOAT_TYPE),
            optionalAttr("i", DataTypes.DOUBLE_TYPE),
            optionalAttr("j", DataTypes.BIGINTEGER_TYPE),
            optionalAttr("k", DataTypes.BIGDECIMAL_TYPE),
            optionalAttr("l", DataTypes.DATE_TYPE),
            optionalAttr("m", DataTypes.arrayTypeName(DataTypes.INT_TYPE)),
            optionalAttr("n", DataTypes.arrayTypeName(DataTypes.BIGDECIMAL_TYPE)),
            optionalAttr("o", DataTypes.mapTypeName(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE)))


        ts.defineTypes(ImmutableList.of[EnumTypeDefinition], ImmutableList.of[StructTypeDefinition](sDef),
            ImmutableList.of[HierarchicalTypeDefinition[TraitType]],
            ImmutableList.of[HierarchicalTypeDefinition[ClassType]]
        )

        val A: HierarchicalTypeDefinition[TraitType] = createTraitTypeDef("A", List(),
            requiredAttr("a", DataTypes.INT_TYPE),
            optionalAttr("b", DataTypes.BOOLEAN_TYPE),
            optionalAttr("c", DataTypes.BYTE_TYPE),
            optionalAttr("d", DataTypes.SHORT_TYPE))
        val B: HierarchicalTypeDefinition[TraitType] =
            createTraitTypeDef("B", Seq("A"), optionalAttr("b", DataTypes.BOOLEAN_TYPE))
        val C: HierarchicalTypeDefinition[TraitType] =
            createTraitTypeDef("C", Seq("A"), optionalAttr("c", DataTypes.BYTE_TYPE))
        val D: HierarchicalTypeDefinition[TraitType] =
            createTraitTypeDef("D", Seq("B", "C"), optionalAttr("d", DataTypes.SHORT_TYPE))

        defineTraits(ts, A, B, C, D)

        ts.defineEnumType("HiveObjectType",
            new EnumValue("GLOBAL", 1),
            new EnumValue("DATABASE", 2),
            new EnumValue("TABLE", 3),
            new EnumValue("PARTITION", 4),
            new EnumValue("COLUMN", 5))

        ts.defineEnumType("PrincipalType",
            new EnumValue("USER", 1),
            new EnumValue("ROLE", 2),
            new EnumValue("GROUP", 3))

        ts.defineEnumType("TxnState",
            new EnumValue("COMMITTED", 1),
            new EnumValue("ABORTED", 2),
            new EnumValue("OPEN", 3))

        ts.defineEnumType("LockLevel",
            new EnumValue("DB", 1),
            new EnumValue("TABLE", 2),
            new EnumValue("PARTITION", 3))

        defineClassType(ts, createClassTypeDef("t4", List(),
            requiredAttr("a", DataTypes.INT_TYPE),
            optionalAttr("b", DataTypes.BOOLEAN_TYPE),
            optionalAttr("c", DataTypes.BYTE_TYPE),
            optionalAttr("d", DataTypes.SHORT_TYPE),
            optionalAttr("enum1", ts.getDataType(classOf[EnumType], "HiveObjectType")),
            optionalAttr("e", DataTypes.INT_TYPE),
            optionalAttr("f", DataTypes.INT_TYPE),
            optionalAttr("g", DataTypes.LONG_TYPE),
            optionalAttr("enum2", ts.getDataType(classOf[EnumType], "PrincipalType")),
            optionalAttr("h", DataTypes.FLOAT_TYPE),
            optionalAttr("i", DataTypes.DOUBLE_TYPE),
            optionalAttr("j", DataTypes.BIGINTEGER_TYPE),
            optionalAttr("k", DataTypes.BIGDECIMAL_TYPE),
            optionalAttr("enum3", ts.getDataType(classOf[EnumType], "TxnState")),
            optionalAttr("l", DataTypes.DATE_TYPE),
            optionalAttr("m", ts.defineArrayType(DataTypes.INT_TYPE)),
            optionalAttr("n", ts.defineArrayType(DataTypes.BIGDECIMAL_TYPE)),
            optionalAttr("o", ts.defineMapType(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE)),
            optionalAttr("enum4", ts.getDataType(classOf[EnumType], "LockLevel"))))

        val deptTypeDef: HierarchicalTypeDefinition[ClassType] = createClassTypeDef("Department", List(),
            requiredAttr("name", DataTypes.STRING_TYPE),
            new AttributeDefinition("employees", String.format("array<%s>", "Person"),
                Multiplicity.COLLECTION, true, "department"))
        val personTypeDef: HierarchicalTypeDefinition[ClassType] = createClassTypeDef("Person", List(),
            requiredAttr("name", DataTypes.STRING_TYPE),
            new AttributeDefinition("department", "Department", Multiplicity.REQUIRED, false, "employees"),
            new AttributeDefinition("manager", "Manager", Multiplicity.OPTIONAL, false, "subordinates")
        )
        val managerTypeDef: HierarchicalTypeDefinition[ClassType] = createClassTypeDef("Manager", List("Person"),
            new AttributeDefinition("subordinates", String.format("array<%s>", "Person"),
                Multiplicity.COLLECTION, false, "manager")
        )
        val securityClearanceTypeDef: HierarchicalTypeDefinition[TraitType] = createTraitTypeDef("SecurityClearance", List(),
            requiredAttr("level", DataTypes.INT_TYPE)
        )
        ts.defineTypes(ImmutableList.of[EnumTypeDefinition], ImmutableList.of[StructTypeDefinition],
            ImmutableList.of[HierarchicalTypeDefinition[TraitType]](securityClearanceTypeDef),
            ImmutableList.of[HierarchicalTypeDefinition[ClassType]](deptTypeDef, personTypeDef, managerTypeDef))

        val ser = TypesSerialization.toJson(ts, _ => true)

        val typesDef1 = TypesSerialization.fromJson(ser)

        val ts1 = TypeSystem.getInstance()
        ts1.reset()

        typesDef1.enumTypes.foreach(ts1.defineEnumType(_))

        ts1.defineTypes(ImmutableList.of[EnumTypeDefinition], ImmutableList.copyOf(typesDef1.structTypes.toArray),
            ImmutableList.copyOf(typesDef1.traitTypes.toArray),
            ImmutableList.copyOf(typesDef1.classTypes.toArray)
        )
        val ser2 = TypesSerialization.toJson(ts1, _ => true)
        val typesDef2 = TypesSerialization.fromJson(ser2)

        Assert.assertEquals(typesDef1, typesDef2)
    }

  @Test def test2: Unit = {

    val sDef = structDef("ts1", requiredAttr("a", DataTypes.INT_TYPE),
      optionalAttr("b", DataTypes.BOOLEAN_TYPE),
      optionalAttr("c", DataTypes.BYTE_TYPE),
      optionalAttr("d", DataTypes.SHORT_TYPE),
      optionalAttr("e", DataTypes.INT_TYPE),
      optionalAttr("f", DataTypes.INT_TYPE),
      optionalAttr("g", DataTypes.LONG_TYPE),
      optionalAttr("h", DataTypes.FLOAT_TYPE),
      optionalAttr("i", DataTypes.DOUBLE_TYPE),
      optionalAttr("j", DataTypes.BIGINTEGER_TYPE),
      optionalAttr("k", DataTypes.BIGDECIMAL_TYPE),
      optionalAttr("l", DataTypes.DATE_TYPE),
      optionalAttr("m", DataTypes.arrayTypeName(DataTypes.INT_TYPE)),
      optionalAttr("n", DataTypes.arrayTypeName(DataTypes.BIGDECIMAL_TYPE)),
      optionalAttr("o", DataTypes.mapTypeName(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE)))



    val ser2 = TypesSerialization.toJson(sDef)
    val typesDef2 = TypesSerialization.fromJson(ser2)

    Assert.assertEquals(sDef, typesDef2.structTypes(0))
  }

  @Test def test3: Unit = {

    val sDef = structDef("ts1", requiredAttr("a", DataTypes.INT_TYPE),
      optionalAttr("b", DataTypes.BOOLEAN_TYPE),
      optionalAttr("c", DataTypes.BYTE_TYPE),
      optionalAttr("d", DataTypes.SHORT_TYPE),
      optionalAttr("e", DataTypes.INT_TYPE),
      optionalAttr("f", DataTypes.INT_TYPE),
      optionalAttr("g", DataTypes.LONG_TYPE),
      optionalAttr("h", DataTypes.FLOAT_TYPE),
      optionalAttr("i", DataTypes.DOUBLE_TYPE),
      optionalAttr("j", DataTypes.BIGINTEGER_TYPE),
      optionalAttr("k", DataTypes.BIGDECIMAL_TYPE),
      optionalAttr("l", DataTypes.DATE_TYPE),
      optionalAttr("m", DataTypes.arrayTypeName(DataTypes.INT_TYPE)),
      optionalAttr("n", DataTypes.arrayTypeName(DataTypes.BIGDECIMAL_TYPE)),
      optionalAttr("o", DataTypes.mapTypeName(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE)))



    val ser2 = TypesSerialization.toJson(sDef)
    val typesDef2 = TypesSerialization.fromJson(ser2)

    Assert.assertEquals(sDef, typesDef2.structTypes(0))
  }

  @Test def test4 : Unit = {

    val A: HierarchicalTypeDefinition[TraitType] = createTraitTypeDef("A", List(),
      requiredAttr("a", DataTypes.INT_TYPE),
      optionalAttr("b", DataTypes.BOOLEAN_TYPE),
      optionalAttr("c", DataTypes.BYTE_TYPE),
      optionalAttr("d", DataTypes.SHORT_TYPE))
    val B: HierarchicalTypeDefinition[TraitType] =
      createTraitTypeDef("B", Seq("A"), optionalAttr("b", DataTypes.BOOLEAN_TYPE))
    val C: HierarchicalTypeDefinition[TraitType] =
      createTraitTypeDef("C", Seq("A"), optionalAttr("c", DataTypes.BYTE_TYPE))
    val D: HierarchicalTypeDefinition[TraitType] =
      createTraitTypeDef("D", Seq("B", "C"), optionalAttr("d", DataTypes.SHORT_TYPE))

    val typDefs = Seq(A,B,C,D)
    typDefs.foreach { tDef =>
      val ser2 = TypesSerialization.toJson(tDef, true)
      val typesDef2 = TypesSerialization.fromJson(ser2)
      Assert.assertEquals(tDef, typesDef2.traitTypes(0))

    }
  }

  @Test def test5 : Unit = {
    val e1 = new EnumTypeDefinition("HiveObjectType",
      new EnumValue("GLOBAL", 1),
      new EnumValue("DATABASE", 2),
      new EnumValue("TABLE", 3),
      new EnumValue("PARTITION", 4),
      new EnumValue("COLUMN", 5))

    val e2 = new EnumTypeDefinition("PrincipalType",
      new EnumValue("USER", 1),
      new EnumValue("ROLE", 2),
      new EnumValue("GROUP", 3))

    val e3 = new EnumTypeDefinition("TxnState",
      new EnumValue("COMMITTED", 1),
      new EnumValue("ABORTED", 2),
      new EnumValue("OPEN", 3))

    val e4 = new EnumTypeDefinition("LockLevel",
      new EnumValue("DB", 1),
      new EnumValue("TABLE", 2),
      new EnumValue("PARTITION", 3))

    val typDefs = Seq(e1,e2,e3,e4)
    typDefs.foreach { tDef =>
      val ser2 = TypesSerialization.toJson(tDef)
      val typesDef2 = TypesSerialization.fromJson(ser2)
      Assert.assertEquals(tDef, typesDef2.enumTypes(0))

    }
  }

  @Test def test6 : Unit = {
    val typDef = createClassTypeDef("t4", List(),
      requiredAttr("a", DataTypes.INT_TYPE),
      optionalAttr("b", DataTypes.BOOLEAN_TYPE),
      optionalAttr("c", DataTypes.BYTE_TYPE),
      optionalAttr("d", DataTypes.SHORT_TYPE),
      optionalAttr("enum1", "HiveObjectType"),
      optionalAttr("e", DataTypes.INT_TYPE),
      optionalAttr("f", DataTypes.INT_TYPE),
      optionalAttr("g", DataTypes.LONG_TYPE),
      optionalAttr("enum2", "PrincipalType"),
      optionalAttr("h", DataTypes.FLOAT_TYPE),
      optionalAttr("i", DataTypes.DOUBLE_TYPE),
      optionalAttr("j", DataTypes.BIGINTEGER_TYPE),
      optionalAttr("k", DataTypes.BIGDECIMAL_TYPE),
      optionalAttr("enum3", "TxnState"),
      optionalAttr("l", DataTypes.DATE_TYPE),
      optionalAttr("m", DataTypes.INT_TYPE),
      optionalAttr("n", DataTypes.BIGDECIMAL_TYPE),
      optionalAttr("o", DataTypes.mapTypeName(DataTypes.STRING_TYPE, DataTypes.DOUBLE_TYPE)),
      optionalAttr("enum4", "LockLevel"))

    val deptTypeDef: HierarchicalTypeDefinition[ClassType] = createClassTypeDef("Department", List(),
      requiredAttr("name", DataTypes.STRING_TYPE),
      new AttributeDefinition("employees", String.format("array<%s>", "Person"),
        Multiplicity.COLLECTION, true, "department"))
    val personTypeDef: HierarchicalTypeDefinition[ClassType] = createClassTypeDef("Person", List(),
      requiredAttr("name", DataTypes.STRING_TYPE),
      new AttributeDefinition("department", "Department", Multiplicity.REQUIRED, false, "employees"),
      new AttributeDefinition("manager", "Manager", Multiplicity.OPTIONAL, false, "subordinates")
    )
    val managerTypeDef: HierarchicalTypeDefinition[ClassType] = createClassTypeDef("Manager", List("Person"),
      new AttributeDefinition("subordinates", String.format("array<%s>", "Person"),
        Multiplicity.COLLECTION, false, "manager")
    )

    val typDefs = Seq(typDef, deptTypeDef, personTypeDef, managerTypeDef)
    typDefs.foreach { tDef =>
      val ser2 = TypesSerialization.toJson(tDef, false)
      val typesDef2 = TypesSerialization.fromJson(ser2)
      Assert.assertEquals(tDef, typesDef2.classTypes(0))

    }
  }
}
