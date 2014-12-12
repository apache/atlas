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

package org.apache.metadata.json

import com.google.common.collect.ImmutableList
import org.apache.metadata.{ITypedStruct, Struct, BaseTest}
import org.apache.metadata.storage.StructInstance
import org.apache.metadata.storage.StructInstance
import org.apache.metadata.types._
import org.json4s.NoTypeHints
import org.junit.Before
import org.junit.Test
import org.junit.Assert

import org.json4s._
import org.json4s.native.Serialization.{read, write => swrite}
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

    Assert.assertEquals(ts.toString, "{\n\ta : 1\n\tb : true\n\tc : 1\n\td : 2\n\te : 1\n\tf : 1\n\tg : 1\n\th : 1.0\n\ti : 1.0\n\tj : 1\n\tk : 1\n\tl : Wed Dec 10 18:35:58 PST 2014\n\tm : [1, 1]\n\tn : [1.1, 1.1]\n\to : {b=2.0, a=1.0}\n\n}\n")

    implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
        new BigDecimalSerializer + new BigIntegerSerializer

    //Json representation
    val ser = swrite(ts)
    Assert.assertEquals(ser, "{\"$typeName$\":\"t1\",\"e\":1,\"n\":[1.1,1.1],\"h\":1.0,\"b\":true,\"k\":1,\"j\":1,\"d\":2,\"m\":[1,1],\"g\":1,\"a\":1,\"i\":1.0,\"c\":1,\"l\":\"2014-12-11T02:35:58.440Z\",\"f\":1,\"o\":{\"b\":2.0,\"a\":1.0}}")

    // Typed Struct read back
    val ts1 = read[StructInstance](ser)
    Assert.assertEquals(ts1.toString, "{\n\ta : 1\n\tb : true\n\tc : 1\n\td : 2\n\te : 1\n\tf : 1\n\tg : 1\n\th : 1.0\n\ti : 1.0\n\tj : 1\n\tk : 1\n\tl : Thu Dec 11 00:00:00 PST 2014\n\tm : [1, 1]\n\tn : [1.100000000000000088817841970012523233890533447265625, 1.100000000000000088817841970012523233890533447265625]\n\to : {b=2.0, a=1.0}\n\n}\n")
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
    Assert.assertEquals(ts1.toString, "{\n\ta : 1\n\tb : true\n\tc : 1\n\td : 2\n\te : 1\n\tf : 1\n\tg : 1\n\th : 1.0\n\ti : 1.0\n\tj : 1\n\tk : 1\n\tl : Wed Dec 03 00:00:00 PST 2014\n\tm : [1, 1]\n\tn : [1.100000000000000088817841970012523233890533447265625, 1.100000000000000088817841970012523233890533447265625]\n\to : {b=2.0, a=1.0}\n\n}\n")
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
    Assert.assertEquals(ts.toString, "{\n\td : 1\n\tb : true\n\tc : 1\n\ta : 1\n\tA.B.D.b : true\n\tA.B.D.c : 2\n\tA.B.D.d : 2\n\tA.C.D.a : 3\n\tA.C.D.b : false\n\tA.C.D.c : 3\n\tA.C.D.d : 3\n\n}\n")

    // Json representation :
    val ser = swrite(ts)
    Assert.assertEquals(ser, "{\"$typeName$\":\"D\",\"A.C.D.d\":3,\"A.B.D.c\":2,\"b\":true,\"A.C.D.c\":3,\"d\":1,\"A.B.D.b\":true,\"a\":1,\"A.C.D.b\":false,\"A.B.D.d\":2,\"c\":1,\"A.C.D.a\":3}")

    val ts1 = read[StructInstance](
      """
        {"$typeName$":"D","A.C.D.d":3,"A.B.D.c":2,"b":true,"A.C.D.c":3,"d":1,
        "A.B.D.b":true,"a":1,"A.C.D.b":false,"A.B.D.d":2,"c":1,"A.C.D.a":3}""")
    // Typed Struct read from string:
    Assert.assertEquals(ts1.toString, "{\n\td : 1\n\tb : true\n\tc : 1\n\ta : 1\n\tA.B.D.b : true\n\tA.B.D.c : 2\n\tA.B.D.d : 2\n\tA.C.D.a : 3\n\tA.C.D.b : false\n\tA.C.D.c : 3\n\tA.C.D.d : 3\n\n}\n")
  }
}
