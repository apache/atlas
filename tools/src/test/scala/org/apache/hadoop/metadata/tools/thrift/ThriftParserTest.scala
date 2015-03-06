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

package org.apache.hadoop.metadata.tools.thrift

import com.google.gson.JsonParser
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.{write => swrite}
import org.json4s.{NoTypeHints, _}
import org.junit.{Assert, Test}

import scala.io.Source
import scala.reflect.ClassTag

/**
 * Copied from
 * [[https://github.com/json4s/json4s/blob/master/ext/src/main/scala/org/json4s/ext/EnumSerializer.scala json4s github]]
 * to avoid dependency on json4s-ext.
 */
class EnumNameSerializer[E <: Enumeration: ClassTag](enum: E) extends Serializer[E#Value] {
    import JsonDSL._

    val EnumerationClass = classOf[E#Value]

    def deserialize(implicit format: Formats): PartialFunction[(TypeInfo, JValue), E#Value] = {
        case (t@TypeInfo(EnumerationClass, _), json) if (isValid(json)) => {
            json match {
                case JString(value) => enum.withName(value)
                case value => throw new MappingException("Can't convert " +
                    value + " to " + EnumerationClass)
            }
        }
    }

    private[this] def isValid(json: JValue) = json match {
        case JString(value) if (enum.values.exists(_.toString == value)) => true
        case _ => false
    }

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
        case i: E#Value => i.toString
    }
}

class ThriftParserTest {

    @Test def testSimple {
        var p = new ThriftParser
        val parser = new JsonParser

        var td: Option[ThriftDef] = p( """include "share/fb303/if/fb303.thrift"

                             namespace java org.apache.hadoop.hive.metastore.api
                             namespace php metastore
                             namespace cpp Apache.Hadoop.Hive
                                       """)

        val parsed = parser.parse(toJson(td.get))
        val sample = parser.parse( """{
  "includes":[{
    "value":"share/fb303/if/fb303.thrift"
  }],
  "cppIncludes":[],
  "namespaces":[{
    "lang":"",
    "name":"Apache.Hadoop.Hive",
    "otherLang":"cpp"
  },{
    "lang":"",
    "name":"metastore",
    "otherLang":"php"
  },{
    "lang":"",
    "name":"org.apache.hadoop.hive.metastore.api",
    "otherLang":"java"
  }],
  "constants":[],
  "typedefs":[],
  "enums":[],
  "senums":[],
  "structs":[],
  "unions":[],
  "xceptions":[],
  "services":[]
}""")

        Assert.assertEquals(parsed.toString, sample.toString)
    }

    @Test def testStruct {
        val p = new ThriftParser
        val parser = new JsonParser

        var td: Option[ThriftDef] = p( """struct PartitionSpecWithSharedSD {
           1: list<PartitionWithoutSD> partitions,
           2: StorageDescriptor sd
         }""")

        val parsed = parser.parse(toJson(td.get))

        val sample = parser.parse( """{
  "includes":[],
  "cppIncludes":[],
  "namespaces":[],
  "constants":[],
  "typedefs":[],
  "enums":[],
  "senums":[],
  "structs":[{
    "name":"PartitionSpecWithSharedSD",
    "xsdAll":false,
    "fields":[{
      "id":{
        "value":1
      },
      "requiredNess":false,
      "fieldType":{
        "elemType":{
          "name":"PartitionWithoutSD"
        }
      },
      "name":"partitions",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":2
      },
      "requiredNess":false,
      "fieldType":{
        "name":"StorageDescriptor"
      },
      "name":"sd",
      "xsdOptional":false,
      "xsdNillable":false
    }]
  }],
  "unions":[],
  "xceptions":[],
  "services":[]
}""")

        Assert.assertEquals(parsed.toString, sample.toString)
    }

    def toJson(td: ThriftDef) = {
        implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new EnumNameSerializer(BASE_TYPES) +
            new EnumNameSerializer(THRIFT_LANG)
        val ser = swrite(td)
        pretty(render(parse(ser)))
    }

    @Test def testTableStruct {
        val p = new ThriftParser
        val parser = new JsonParser

        var td: Option[ThriftDef] = p( """// table information
         struct Table {
           1: string tableName,                // name of the table
           2: string dbName,                   // database name ('default')
           3: string owner,                    // owner of this table
           4: i32    createTime,               // creation time of the table
           5: i32    lastAccessTime,           // last access time (usually this will be filled from HDFS and shouldn't be relied on)
           6: i32    retention,                // retention time
           7: StorageDescriptor sd,            // storage descriptor of the table
           8: list<FieldSchema> partitionKeys, // partition keys of the table. only primitive types are supported
           9: map<string, string> parameters,   // to store comments or any other user level parameters
           10: string viewOriginalText,         // original view text, null for non-view
           11: string viewExpandedText,         // expanded view text, null for non-view
           12: string tableType,                 // table type enum, e.g. EXTERNAL_TABLE
           13: optional PrincipalPrivilegeSet privileges,
           14: optional bool temporary=false
         }""")

        val parsed = parser.parse(toJson(td.get))
        val sample = parser.parse( """{
  "includes":[],
  "cppIncludes":[],
  "namespaces":[],
  "constants":[],
  "typedefs":[],
  "enums":[],
  "senums":[],
  "structs":[{
    "name":"Table",
    "xsdAll":false,
    "fields":[{
      "id":{
        "value":1
      },
      "requiredNess":false,
      "fieldType":{
        "typ":"string"
      },
      "name":"tableName",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":2
      },
      "requiredNess":false,
      "fieldType":{
        "typ":"string"
      },
      "name":"dbName",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":3
      },
      "requiredNess":false,
      "fieldType":{
        "typ":"string"
      },
      "name":"owner",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":4
      },
      "requiredNess":false,
      "fieldType":{
        "typ":"i32"
      },
      "name":"createTime",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":5
      },
      "requiredNess":false,
      "fieldType":{
        "typ":"i32"
      },
      "name":"lastAccessTime",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":6
      },
      "requiredNess":false,
      "fieldType":{
        "typ":"i32"
      },
      "name":"retention",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":7
      },
      "requiredNess":false,
      "fieldType":{
        "name":"StorageDescriptor"
      },
      "name":"sd",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":8
      },
      "requiredNess":false,
      "fieldType":{
        "elemType":{
          "name":"FieldSchema"
        }
      },
      "name":"partitionKeys",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":9
      },
      "requiredNess":false,
      "fieldType":{
        "keyType":{
          "typ":"string"
        },
        "valueType":{
          "typ":"string"
        }
      },
      "name":"parameters",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":10
      },
      "requiredNess":false,
      "fieldType":{
        "typ":"string"
      },
      "name":"viewOriginalText",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":11
      },
      "requiredNess":false,
      "fieldType":{
        "typ":"string"
      },
      "name":"viewExpandedText",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":12
      },
      "requiredNess":false,
      "fieldType":{
        "typ":"string"
      },
      "name":"tableType",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":13
      },
      "requiredNess":false,
      "fieldType":{
        "name":"PrincipalPrivilegeSet"
      },
      "name":"privileges",
      "xsdOptional":false,
      "xsdNillable":false
    },{
      "id":{
        "value":14
      },
      "requiredNess":false,
      "fieldType":{
        "typ":"bool"
      },
      "name":"temporary",
      "fieldValue":{
        "value":"false"
      },
      "xsdOptional":false,
      "xsdNillable":false
    }]
  }],
  "unions":[],
  "xceptions":[],
  "services":[]
}""")

        Assert.assertEquals(parsed.toString, sample.toString)
    }

    @Test def testHiveThrift {
        val p = new ThriftParser
        val is = getClass().getResourceAsStream("/test.thrift")
        val src: Source = Source.fromInputStream(is)
        val t: String = src.getLines().mkString("\n")
        var td: Option[ThriftDef] = p(t)
        Assert.assertTrue(td.isDefined)
        //println(toJson(td.get))
    }

    @Test def testService {
        val p = new ThriftParser
        val parser = new JsonParser

        var td: Option[ThriftDef] = p( """/**
             * This interface is live.
             */
             service ThriftHiveMetastore extends fb303.FacebookService
             {
               string getMetaConf(1:string key) throws(1:MetaException o1)
               void setMetaConf(1:string key, 2:string value) throws(1:MetaException o1)

               void create_database(1:Database database) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
               Database get_database(1:string name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
               void drop_database(1:string name, 2:bool deleteData, 3:bool cascade) throws(1:NoSuchObjectException o1, 2:InvalidOperationException o2, 3:MetaException o3)
               list<string> get_databases(1:string pattern) throws(1:MetaException o1)
               list<string> get_all_databases() throws(1:MetaException o1)
               void alter_database(1:string dbname, 2:Database db) throws(1:MetaException o1, 2:NoSuchObjectException o2)

             }""")

        val parsed = parser.parse(toJson(td.get))
        val sample = parser.parse( """{
  "includes":[],
  "cppIncludes":[],
  "namespaces":[],
  "constants":[],
  "typedefs":[],
  "enums":[],
  "senums":[],
  "structs":[],
  "unions":[],
  "xceptions":[],
  "services":[{
    "name":"ThriftHiveMetastore",
    "superName":"fb303.FacebookService",
    "functions":[{
      "oneway":false,
      "returnType":{
        "typ":"string"
      },
      "name":"getMetaConf",
      "parameters":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "typ":"string"
        },
        "name":"key",
        "xsdOptional":false,
        "xsdNillable":false
      }],
      "throwFields":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "name":"MetaException"
        },
        "name":"o1",
        "xsdOptional":false,
        "xsdNillable":false
      }]
    },{
      "oneway":false,
      "returnType":{

      },
      "name":"setMetaConf",
      "parameters":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "typ":"string"
        },
        "name":"key",
        "xsdOptional":false,
        "xsdNillable":false
      },{
        "id":{
          "value":2
        },
        "requiredNess":false,
        "fieldType":{
          "typ":"string"
        },
        "name":"value",
        "xsdOptional":false,
        "xsdNillable":false
      }],
      "throwFields":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "name":"MetaException"
        },
        "name":"o1",
        "xsdOptional":false,
        "xsdNillable":false
      }]
    },{
      "oneway":false,
      "returnType":{

      },
      "name":"create_database",
      "parameters":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "name":"Database"
        },
        "name":"database",
        "xsdOptional":false,
        "xsdNillable":false
      }],
      "throwFields":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "name":"AlreadyExistsException"
        },
        "name":"o1",
        "xsdOptional":false,
        "xsdNillable":false
      },{
        "id":{
          "value":2
        },
        "requiredNess":false,
        "fieldType":{
          "name":"InvalidObjectException"
        },
        "name":"o2",
        "xsdOptional":false,
        "xsdNillable":false
      },{
        "id":{
          "value":3
        },
        "requiredNess":false,
        "fieldType":{
          "name":"MetaException"
        },
        "name":"o3",
        "xsdOptional":false,
        "xsdNillable":false
      }]
    },{
      "oneway":false,
      "returnType":{
        "name":"Database"
      },
      "name":"get_database",
      "parameters":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "typ":"string"
        },
        "name":"name",
        "xsdOptional":false,
        "xsdNillable":false
      }],
      "throwFields":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "name":"NoSuchObjectException"
        },
        "name":"o1",
        "xsdOptional":false,
        "xsdNillable":false
      },{
        "id":{
          "value":2
        },
        "requiredNess":false,
        "fieldType":{
          "name":"MetaException"
        },
        "name":"o2",
        "xsdOptional":false,
        "xsdNillable":false
      }]
    },{
      "oneway":false,
      "returnType":{

      },
      "name":"drop_database",
      "parameters":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "typ":"string"
        },
        "name":"name",
        "xsdOptional":false,
        "xsdNillable":false
      },{
        "id":{
          "value":2
        },
        "requiredNess":false,
        "fieldType":{
          "typ":"bool"
        },
        "name":"deleteData",
        "xsdOptional":false,
        "xsdNillable":false
      },{
        "id":{
          "value":3
        },
        "requiredNess":false,
        "fieldType":{
          "typ":"bool"
        },
        "name":"cascade",
        "xsdOptional":false,
        "xsdNillable":false
      }],
      "throwFields":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "name":"NoSuchObjectException"
        },
        "name":"o1",
        "xsdOptional":false,
        "xsdNillable":false
      },{
        "id":{
          "value":2
        },
        "requiredNess":false,
        "fieldType":{
          "name":"InvalidOperationException"
        },
        "name":"o2",
        "xsdOptional":false,
        "xsdNillable":false
      },{
        "id":{
          "value":3
        },
        "requiredNess":false,
        "fieldType":{
          "name":"MetaException"
        },
        "name":"o3",
        "xsdOptional":false,
        "xsdNillable":false
      }]
    },{
      "oneway":false,
      "returnType":{
        "elemType":{
          "typ":"string"
        }
      },
      "name":"get_databases",
      "parameters":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "typ":"string"
        },
        "name":"pattern",
        "xsdOptional":false,
        "xsdNillable":false
      }],
      "throwFields":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "name":"MetaException"
        },
        "name":"o1",
        "xsdOptional":false,
        "xsdNillable":false
      }]
    },{
      "oneway":false,
      "returnType":{
        "elemType":{
          "typ":"string"
        }
      },
      "name":"get_all_databases",
      "parameters":[],
      "throwFields":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "name":"MetaException"
        },
        "name":"o1",
        "xsdOptional":false,
        "xsdNillable":false
      }]
    },{
      "oneway":false,
      "returnType":{

      },
      "name":"alter_database",
      "parameters":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "typ":"string"
        },
        "name":"dbname",
        "xsdOptional":false,
        "xsdNillable":false
      },{
        "id":{
          "value":2
        },
        "requiredNess":false,
        "fieldType":{
          "name":"Database"
        },
        "name":"db",
        "xsdOptional":false,
        "xsdNillable":false
      }],
      "throwFields":[{
        "id":{
          "value":1
        },
        "requiredNess":false,
        "fieldType":{
          "name":"MetaException"
        },
        "name":"o1",
        "xsdOptional":false,
        "xsdNillable":false
      },{
        "id":{
          "value":2
        },
        "requiredNess":false,
        "fieldType":{
          "name":"NoSuchObjectException"
        },
        "name":"o2",
        "xsdOptional":false,
        "xsdNillable":false
      }]
    }]
  }]
}""")

        Assert.assertEquals(parsed.toString, sample.toString)
    }

}
