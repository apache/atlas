package org.apache.metadata.dsl

import org.apache.metadata.hive.HiveMockMetadataService
import org.apache.metadata.json.{BigIntegerSerializer, BigDecimalSerializer, TypedStructSerializer}
import org.apache.metadata.storage.TypedStruct
import org.apache.metadata.{Struct, BaseTest}
import org.apache.metadata.types.{IDataType, Multiplicity, StructType}
import org.json4s.NoTypeHints
import org.json4s.native.Serialization._
import org.junit.{Test, Before}
import org.apache.metadata.dsl._
import org.json4s.native.JsonMethods._

class DSLTest extends BaseTest {

  @Before
  override def setup {
    super.setup
  }

  @Test def test1 {

    // 1. Existing Types in System
    println(s"Existing Types:\n\t ${listTypes}\n")

    defineStructType("mytype",
      attrDef("a", INT_TYPE, ATTR_REQUIRED),
      attrDef("b", BOOLEAN_TYPE),
      attrDef("c", BYTE_TYPE),
      attrDef("d", SHORT_TYPE),
      attrDef("e", INT_TYPE),
      attrDef("f", INT_TYPE),
      attrDef("g", LONG_TYPE),
      attrDef("h", FLOAT_TYPE),
      attrDef("i", DOUBLE_TYPE),
      attrDef("j", BIGINT_TYPE),
      attrDef("k", BIGDECIMAL_TYPE),
      attrDef("l", DATE_TYPE),
      attrDef("m", arrayType(INT_TYPE)),
      attrDef("n", arrayType(BIGDECIMAL_TYPE)),
      attrDef("o", mapType(STRING_TYPE, DOUBLE_TYPE)))

    // 2. 'mytype' available as a a Type
    println(s"Added Type:\n\t ${listTypes}\n")

    // 3. Create a 'mytype' instance from Json
    val i = createInstance("mytype", """
        {
                               "$typeName$":"mytype",
                               "e":1,
                               "n":[1,1.1],
                               "h":1.0,
                               "b":true,
                               "k":1,
                               "j":1,
                               "d":2,
                               "m":[1,1],
                               "g":1,
                               "a":1,
                               "i":1.0,
                               "c":1,
                               "l":"2014-12-03T08:00:00.000Z",
                               "f":1,
                               "o":{
                                 "b":2.0,
                                 "a":1.0
                               }
                             }
                                       """)

    // 4. Navigate mytype instance in code
    println("Examples of Navigate mytype instance in code:\n")
    println(s"i.a -> ${i.a}")
    println(s"i.o -> ${i.o}")
    println(s"i.o.keys -> ${i.o.asInstanceOf[java.util.Map[_,_]].keySet}")

    // 5. Serialize mytype instance to Json
    println(s"\nJSON:\n ${pretty(render(i))}")
  }

  @Test def test2 {

    // 1. Existing Types in System
    println(s"Existing Types:\n\t ${listTypes}\n")

    val addrType = defineStructType("addressType",
      attrDef("houseNum", INT_TYPE, ATTR_REQUIRED),
      attrDef("street", STRING_TYPE, ATTR_REQUIRED),
      attrDef("city", STRING_TYPE, ATTR_REQUIRED),
      attrDef("state", STRING_TYPE, ATTR_REQUIRED),
      attrDef("zip", INT_TYPE, ATTR_REQUIRED),
      attrDef("country", STRING_TYPE, ATTR_REQUIRED)
      )

    val personType =  defineStructType("personType",
      attrDef("first_name", STRING_TYPE, ATTR_REQUIRED),
      attrDef("last_name", STRING_TYPE, ATTR_REQUIRED),
      attrDef("address", addrType)
    )

    // 2. updated Types in System
    println(s"Updated Types:\n\t ${listTypes}")

    // 3. Construct a Person in Code
    val person = createInstance("personType")
    val address = createInstance("addressType")

    person.first_name = "Meta"
    person.last_name = "Hadoop"

    address.houseNum = 3460
    address.street = "W Bayshore Rd"
    address.city = "Palo Alto"
    address.state = "CA"
    address.zip = 94303
    address.country = "USA"

    person.address = address

    // 4. Convert to Json
    println(s"\nJSON:\n ${pretty(render(person))}")

    val p2 = createInstance("personType", """{
                                              "first_name":"Meta",
                                              "address":{
                                                "houseNum":3460,
                                                "city":"Palo Alto",
                                                "country":"USA",
                                                "state":"CA",
                                                "zip":94303,
                                                "street":"W Bayshore Rd"
                                              },
                                              "last_name":"Hadoop"
                                            }""")

  }

  @Test def testHive(): Unit = {
    val hiveTable = HiveMockMetadataService.getTable("tpcds", "date_dim")
    println(hiveTable)

    //name : String, typeName : String, comment : String
    val fieldType = defineStructType("FieldSchema",
      attrDef("name", STRING_TYPE, ATTR_REQUIRED),
      attrDef("typeName", STRING_TYPE, ATTR_REQUIRED),
      attrDef("comment", STRING_TYPE)
    )
    /*
    SerDe(name : String, serializationLib : String, parameters : Map[String, String])
     */
    defineStructType("SerDe",
      attrDef("name", STRING_TYPE, ATTR_REQUIRED),
      attrDef("serializationLib", STRING_TYPE, ATTR_REQUIRED),
      attrDef("parameters", mapType(STRING_TYPE, STRING_TYPE))
    )

    /*
    StorageDescriptor(fields : List[FieldSchema],
                               location : String, inputFormat : String,
                                outputFormat : String, compressed : Boolean,
                                numBuckets : Int, bucketColumns : List[String],
                                sortColumns : List[String],
                                parameters : Map[String, String],
                                storedAsSubDirs : Boolean
                                )
     */
    val sdType = defineStructType("StorageDescriptor",
      attrDef("location", STRING_TYPE, ATTR_REQUIRED),
      attrDef("inputFormat", STRING_TYPE, ATTR_REQUIRED),
      attrDef("outputFormat", STRING_TYPE, ATTR_REQUIRED),
      attrDef("compressed", BOOLEAN_TYPE),
      attrDef("numBuckets", INT_TYPE),
      attrDef("bucketColumns", arrayType(STRING_TYPE)),
      attrDef("sortColumns", arrayType(STRING_TYPE)),
      attrDef("parameters", mapType(STRING_TYPE, STRING_TYPE)),
      attrDef("storedAsSubDirs", BOOLEAN_TYPE)
    )

    /*
    case class Table(dbName : String, tableName : String, storageDesc : StorageDescriptor,
                   parameters : Map[String, String],
                    tableType : String)
     */
    defineStructType("Table",
      attrDef("dbName", STRING_TYPE, ATTR_REQUIRED),
      attrDef("tableName", STRING_TYPE, ATTR_REQUIRED),
      attrDef("storageDesc", sdType, ATTR_REQUIRED),
      attrDef("compressed", BOOLEAN_TYPE),
      attrDef("numBuckets", INT_TYPE),
      attrDef("bucketColumns", arrayType(STRING_TYPE)),
      attrDef("sortColumns", arrayType(STRING_TYPE)),
      attrDef("parameters", mapType(STRING_TYPE, STRING_TYPE)),
      attrDef("storedAsSubDirs", BOOLEAN_TYPE)
    )
  }
}
