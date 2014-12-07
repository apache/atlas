package org.apache.metadata.json

import org.apache.metadata.Struct
import org.apache.metadata.storage.TypedStruct
import org.apache.metadata.storage.TypedStruct
import org.apache.metadata.types.Multiplicity
import org.apache.metadata.types.StructType
import org.apache.metadata.{Struct, BaseTest}
import org.apache.metadata.types.{Multiplicity, StructType}
import org.json4s.NoTypeHints
import org.junit.Before
import org.junit.Test

import org.json4s._
import org.json4s.native.Serialization.{read, write => swrite}
import org.json4s.native.JsonMethods._

class SerializationTest extends BaseTest {

  private[metadata] var structType: StructType = null
  private[metadata] var recursiveStructType: StructType = null

  @Before
  override def setup {
    super.setup
    structType = ms.getTypeSystem.getDataType(BaseTest.STRUCT_TYPE_1).asInstanceOf[StructType]
    recursiveStructType = ms.getTypeSystem.getDataType(BaseTest.STRUCT_TYPE_2).asInstanceOf[StructType]
  }

  @Test def test1 {
    val s: Struct = BaseTest.createStruct(ms)
    val ts: TypedStruct = structType.convert(s, Multiplicity.REQUIRED)

    println("Typed Struct :")
    println(ts)

    implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
        new BigDecimalSerializer + new BigIntegerSerializer

    val ser = swrite(ts)
    println("Json representation :")
    println(ser)

    val ts1 = read[TypedStruct](ser)
    println("Typed Struct read back:")
    println(ts1)
  }

  @Test def test2 {
    val s: Struct = BaseTest.createStruct(ms)
    val ts: TypedStruct = structType.convert(s, Multiplicity.REQUIRED)

    implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
      new BigDecimalSerializer + new BigIntegerSerializer

    val ts1 = read[TypedStruct](
      """
        {"$typeName$":"t1","e":1,"n":[1.1,1.1],"h":1.0,"b":true,"k":1,"j":1,"d":2,"m":[1,1],"g":1,"a":1,"i":1.0,
        "c":1,"l":"2014-12-03T19:38:55.053Z","f":1,"o":{"b":2.0,"a":1.0}}""")
    println("Typed Struct read from string:")
    println(ts1)
  }
}
