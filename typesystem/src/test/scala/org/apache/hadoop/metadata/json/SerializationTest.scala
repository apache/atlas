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

package org.apache.hadoop.metadata.json

import com.google.common.collect.ImmutableList
import org.apache.hadoop.metadata._
import org.apache.hadoop.metadata.storage.{ReferenceableInstance, StructInstance}
import org.apache.hadoop.metadata.types._

import org.json4s.NoTypeHints
import org.junit.Before
import org.junit.Test
import org.junit.Assert

import org.json4s._
import org.json4s.native.Serialization.{write => swrite, _}
import org.json4s.native.JsonMethods._

class SerializationTest extends BaseTest {

  private[metadata] var structType: StructType = null
  private[metadata] var recursiveStructType: StructType = null

  @Before
  override def setup {
    super.setup
    structType = ms.getTypeSystem.getDataType(classOf[StructType], BaseTest.STRUCT_TYPE_1).asInstanceOf[StructType]
    recursiveStructType = ms.getTypeSystem.getDataType(classOf[StructType], BaseTest.STRUCT_TYPE_2).asInstanceOf[StructType]
  }

  @Test def test1 {
    val s: Struct = BaseTest.createStruct(ms)
    val ts: ITypedStruct = structType.convert(s, Multiplicity.REQUIRED)

    Assert.assertEquals(ts.toString, "{\n\ta : \t1\n\tb : \ttrue\n\tc : \t1\n\td : \t2\n\te : \t1\n\tf : \t1\n\tg : \t1\n\th : \t1.0\n\ti : \t1.0\n\tj : \t1\n\tk : \t1\n\tl : \t2014-12-10\n\tm : \t[1, 1]\n\tn : \t[1.1, 1.1]\n\to : \t{b=2.0, a=1.0}\n}")

    implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
      new BigDecimalSerializer + new BigIntegerSerializer

    //Json representation
    val ser = swrite(ts)
    Assert.assertEquals(ser, "{\"$typeName$\":\"t1\",\"e\":1,\"n\":[1.1,1.1],\"h\":1.0,\"b\":true,\"k\":1,\"j\":1,\"d\":2,\"m\":[1,1],\"g\":1,\"a\":1,\"i\":1.0,\"c\":1,\"l\":\"2014-12-11T02:35:58.440Z\",\"f\":1,\"o\":{\"b\":2.0,\"a\":1.0}}")

    // Typed Struct read back
    val ts1 = read[StructInstance](ser)
    Assert.assertEquals(ts1.toString, "{\n\ta : \t1\n\tb : \ttrue\n\tc : \t1\n\td : \t2\n\te : \t1\n\tf : \t1\n\tg : \t1\n\th : \t1.0\n\ti : \t1.0\n\tj : \t1\n\tk : \t1\n\tl : \t2014-12-11\n\tm : \t[1, 1]\n\tn : \t[1.100000000000000088817841970012523233890533447265625, 1.100000000000000088817841970012523233890533447265625]\n\to : \t{b=2.0, a=1.0}\n}")
  }

  @Test def test2 {
    val s: Struct = BaseTest.createStruct(ms)
    val ts: ITypedStruct = structType.convert(s, Multiplicity.REQUIRED)

    implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
      new BigDecimalSerializer + new BigIntegerSerializer

    val ts1 = read[StructInstance](
      """
        {"$typeName$":"t1","e":1,"n":[1.1,1.1],"h":1.0,"b":true,"k":1,"j":1,"d":2,"m":[1,1],"g":1,"a":1,"i":1.0,
        "c":1,"l":"2014-12-03T19:38:55.053Z","f":1,"o":{"b":2.0,"a":1.0}}""")
    // Typed Struct read from string
    Assert.assertEquals(ts1.toString, "{\n\ta : \t1\n\tb : \ttrue\n\tc : \t1\n\td : \t2\n\te : \t1\n\tf : \t1\n\tg : \t1\n\th : \t1.0\n\ti : \t1.0\n\tj : \t1\n\tk : \t1\n\tl : \t2014-12-03\n\tm : \t[1, 1]\n\tn : \t[1.100000000000000088817841970012523233890533447265625, 1.100000000000000088817841970012523233890533447265625]\n\to : \t{b=2.0, a=1.0}\n}")
  }

  @Test def testTrait {
    val A: HierarchicalTypeDefinition[TraitType] = createTraitTypeDef("A", null,
      BaseTest.createRequiredAttrDef("a", DataTypes.INT_TYPE),
      BaseTest.createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE),
      BaseTest.createOptionalAttrDef("c", DataTypes.BYTE_TYPE),
      BaseTest.createOptionalAttrDef("d", DataTypes.SHORT_TYPE))
    val B: HierarchicalTypeDefinition[TraitType] = createTraitTypeDef("B", ImmutableList.of[String]("A"),
      BaseTest.createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE))
    val C: HierarchicalTypeDefinition[TraitType] = createTraitTypeDef("C", ImmutableList.of[String]("A"),
      BaseTest.createOptionalAttrDef("c", DataTypes.BYTE_TYPE))
    val D: HierarchicalTypeDefinition[TraitType] = createTraitTypeDef("D", ImmutableList.of[String]("B", "C"),
      BaseTest.createOptionalAttrDef("d", DataTypes.SHORT_TYPE))

    defineTraits(A, B, C, D)

    val DType: TraitType = ms.getTypeSystem.getDataType(classOf[TraitType], "D").asInstanceOf[TraitType]
    val s1: Struct = new Struct("D")
    s1.set("d", 1)
    s1.set("c", 1)
    s1.set("b", true)
    s1.set("a", 1)
    s1.set("A.B.D.b", true)
    s1.set("A.B.D.c", 2)
    s1.set("A.B.D.d", 2)
    s1.set("A.C.D.a", 3)
    s1.set("A.C.D.b", false)
    s1.set("A.C.D.c", 3)
    s1.set("A.C.D.d", 3)

    val s: Struct = BaseTest.createStruct(ms)
    val ts: ITypedStruct = DType.convert(s1, Multiplicity.REQUIRED)

    implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
      new BigDecimalSerializer + new BigIntegerSerializer

    // Typed Struct :
    Assert.assertEquals(ts.toString, "{\n\td : \t1\n\tb : \ttrue\n\tc : \t1\n\ta : \t1\n\tA.B.D.b : \ttrue\n\tA.B.D.c : \t2\n\tA.B.D.d : \t2\n\tA.C.D.a : \t3\n\tA.C.D.b : \tfalse\n\tA.C.D.c : \t3\n\tA.C.D.d : \t3\n}")

    // Json representation :
    val ser = swrite(ts)
    Assert.assertEquals(ser, "{\"$typeName$\":\"D\",\"A.C.D.d\":3,\"A.B.D.c\":2,\"b\":true,\"A.C.D.c\":3,\"d\":1,\"A.B.D.b\":true,\"a\":1,\"A.C.D.b\":false,\"A.B.D.d\":2,\"c\":1,\"A.C.D.a\":3}")

    val ts1 = read[StructInstance](
      """
        {"$typeName$":"D","A.C.D.d":3,"A.B.D.c":2,"b":true,"A.C.D.c":3,"d":1,
        "A.B.D.b":true,"a":1,"A.C.D.b":false,"A.B.D.d":2,"c":1,"A.C.D.a":3}""")
    // Typed Struct read from string:
    Assert.assertEquals(ts1.toString, "{\n\td : \t1\n\tb : \ttrue\n\tc : \t1\n\ta : \t1\n\tA.B.D.b : \ttrue\n\tA.B.D.c : \t2\n\tA.B.D.d : \t2\n\tA.C.D.a : \t3\n\tA.C.D.b : \tfalse\n\tA.C.D.c : \t3\n\tA.C.D.d : \t3\n}")
  }

  @Test def testClass {

    val ts: TypeSystem = ms.getTypeSystem

    val deptTypeDef: HierarchicalTypeDefinition[ClassType] = createClassTypeDef("Department",
      ImmutableList.of[String],
      BaseTest.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
      new AttributeDefinition("employees", String.format("array<%s>", "Person"),
        Multiplicity.COLLECTION, true, "department"))
    val personTypeDef: HierarchicalTypeDefinition[ClassType] = createClassTypeDef("Person",
      ImmutableList.of[String],
      BaseTest.createRequiredAttrDef("name", DataTypes.STRING_TYPE),
      new AttributeDefinition("department", "Department", Multiplicity.REQUIRED, false, "employees"),
      new AttributeDefinition("manager", "Manager", Multiplicity.OPTIONAL, false, "subordinates"))
    val managerTypeDef: HierarchicalTypeDefinition[ClassType] = createClassTypeDef("Manager",
      ImmutableList.of[String]("Person"),
      new AttributeDefinition("subordinates", String.format("array<%s>", "Person"),
        Multiplicity.COLLECTION, false, "manager"))
    val securityClearanceTypeDef: HierarchicalTypeDefinition[TraitType] = createTraitTypeDef("SecurityClearance",
      ImmutableList.of[String],
      BaseTest.createRequiredAttrDef("level", DataTypes.INT_TYPE))

    ts.defineTypes(ImmutableList.of[StructTypeDefinition],
      ImmutableList.of[HierarchicalTypeDefinition[TraitType]](securityClearanceTypeDef),
      ImmutableList.of[HierarchicalTypeDefinition[ClassType]](deptTypeDef, personTypeDef, managerTypeDef)
    )

    val hrDept: Referenceable = new Referenceable("Department")
    val john: Referenceable = new Referenceable("Person")
    val jane: Referenceable = new Referenceable("Manager", "SecurityClearance")
    hrDept.set("name", "hr")
    john.set("name", "John")
    john.set("department", hrDept)
    jane.set("name", "Jane")
    jane.set("department", hrDept)
    john.set("manager", jane)
    hrDept.set("employees", ImmutableList.of[Referenceable](john, jane))
    jane.set("subordinates", ImmutableList.of[Referenceable](john))
    jane.getTrait("SecurityClearance").set("level", 1)

    val deptType: ClassType = ts.getDataType(classOf[ClassType], "Department")
    val hrDept2: ITypedReferenceableInstance = deptType.convert(hrDept, Multiplicity.REQUIRED)

    println(s"HR Dept Object Graph:\n${hrDept2}\n")

    implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
      new TypedReferenceableInstanceSerializer +  new BigDecimalSerializer + new BigIntegerSerializer

    val ser = swrite(hrDept2)
    println(s"HR Dept JSON:\n${pretty(render(parse(ser)))}\n"  )

    println(s"HR Dept Object Graph read from JSON:${read[ReferenceableInstance](ser)}\n")
  }

}