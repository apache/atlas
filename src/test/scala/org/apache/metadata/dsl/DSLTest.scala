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

package org.apache.metadata.dsl

import org.apache.metadata.hive.HiveMockMetadataService
import org.apache.metadata.json.{BigIntegerSerializer, BigDecimalSerializer, TypedStructSerializer}
import org.apache.metadata.storage.StructInstance
import org.apache.metadata.{Struct, BaseTest}
import org.apache.metadata.types.{IDataType, Multiplicity, StructType}
import org.json4s.NoTypeHints
import org.json4s.native.Serialization._
import org.junit.{Test, Before}
import org.apache.metadata.dsl._
import org.json4s.native.JsonMethods._
import org.junit.Assert

class DSLTest extends BaseTest {

  @Before
  override def setup {
    super.setup
  }

  @Test def test1 {

    // 1. Existing Types in System
    Assert.assertEquals(s"${listTypes}", "[t2, t1, int, array<bigdecimal>, long, double, date, float, short, biginteger, byte, string, boolean, bigdecimal, map<string,double>, array<int>]")

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
    Assert.assertEquals(s"${listTypes}", "[t2, t1, int, mytype, array<bigdecimal>, long, double, date, float, short, biginteger, byte, string, boolean, bigdecimal, map<string,double>, array<int>]")

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
    // Examples of Navigate mytype instance in code
    Assert.assertEquals(s"${i.a}", "1")
    Assert.assertEquals(s"${i.o}", "{b=2.0, a=1.0}")
    Assert.assertEquals(s"${i.o.asInstanceOf[java.util.Map[_,_]].keySet}", "[b, a]")

    // 5. Serialize mytype instance to Json
    Assert.assertEquals(s"${pretty(render(i))}", "{\n  \"$typeName$\":\"mytype\",\n  \"e\":1,\n  \"n\":[1,1.100000000000000088817841970012523233890533447265625],\n  \"h\":1.0,\n  \"b\":true,\n  \"k\":1,\n  \"j\":1,\n  \"d\":2,\n  \"m\":[1,1],\n  \"g\":1,\n  \"a\":1,\n  \"i\":1.0,\n  \"c\":1,\n  \"l\":\"2014-12-03T08:00:00.000Z\",\n  \"f\":1,\n  \"o\":{\n    \"b\":2.0,\n    \"a\":1.0\n  }\n}")
  }

  @Test def test2 {

    // 1. Existing Types in System
    Assert.assertEquals(s"${listTypes}", "[t2, t1, int, array<bigdecimal>, long, double, date, float, short, biginteger, byte, string, boolean, bigdecimal, map<string,double>, array<int>]")

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
    Assert.assertEquals(s"${listTypes}", "[t2, t1, int, addressType, array<bigdecimal>, long, double, date, float, short, biginteger, byte, string, boolean, bigdecimal, personType, map<string,double>, array<int>]")


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
    Assert.assertEquals(s"${pretty(render(person))}", "{\n  \"$typeName$\":\"personType\",\n  \"first_name\":\"Meta\",\n  \"address\":{\n    \"$typeName$\":\"addressType\",\n    \"houseNum\":3460,\n    \"city\":\"Palo Alto\",\n    \"country\":\"USA\",\n    \"state\":\"CA\",\n    \"zip\":94303,\n    \"street\":\"W Bayshore Rd\"\n  },\n  \"last_name\":\"Hadoop\"\n}");

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
    //println(hiveTable)

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
