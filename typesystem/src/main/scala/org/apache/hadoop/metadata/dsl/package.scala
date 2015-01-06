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

package org.apache.hadoop.metadata

import java.text.SimpleDateFormat

import org.apache.hadoop.metadata.json.{BigIntegerSerializer, BigDecimalSerializer, TypedStructSerializer, Serialization}
import org.apache.hadoop.metadata.storage.StructInstance
import org.apache.hadoop.metadata.types._

import scala.collection.JavaConverters._
import org.json4s._
import org.json4s.native.Serialization.{read, write => swrite}
import org.json4s.native.JsonMethods._

import scala.language.implicitConversions

package object dsl {

  val defFormat = new DefaultFormats {
    override protected def dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    override val typeHints = NoTypeHints
  }

  implicit val formats = defFormat + new TypedStructSerializer +
    new BigDecimalSerializer + new BigIntegerSerializer

  def service = MetadataService.getCurrentService
  def ts = MetadataService.getCurrentTypeSystem
  def repo = MetadataService.getCurrentRepository

  val BOOLEAN_TYPE = DataTypes.BOOLEAN_TYPE
  val BYTE_TYPE = DataTypes.BYTE_TYPE
  val SHORT_TYPE = DataTypes.SHORT_TYPE
  val INT_TYPE = DataTypes.INT_TYPE
  val LONG_TYPE = DataTypes.LONG_TYPE
  val FLOAT_TYPE = DataTypes.FLOAT_TYPE
  val DOUBLE_TYPE = DataTypes.DOUBLE_TYPE
  val BIGINT_TYPE = DataTypes.BIGINTEGER_TYPE
  val BIGDECIMAL_TYPE = DataTypes.BIGDECIMAL_TYPE
  val DATE_TYPE = DataTypes.DATE_TYPE
  val STRING_TYPE = DataTypes.STRING_TYPE

  val ATTR_OPTIONAL = Multiplicity.OPTIONAL
  val ATTR_REQUIRED = Multiplicity.REQUIRED

  def arrayType(dT : IDataType[_]) = ts.defineArrayType(dT)
  def mapType(kT : IDataType[_], vT : IDataType[_]) = ts.defineMapType(kT, vT)

  def attrDef(name : String, dT : IDataType[_],
              m : Multiplicity = Multiplicity.OPTIONAL,
              isComposite: Boolean = false,
              reverseAttributeName: String = null) = {
    require(name != null)
    require(dT != null)
    new AttributeDefinition(name, dT.getName, m, isComposite, reverseAttributeName)
  }

  def listTypes = ts.getTypeNames

  def defineStructType(name : String, attrDef : AttributeDefinition*) = {
    require(name != null)
    ts.defineStructType(name, false, attrDef:_*)
  }

  def createInstance(typeName : String, jsonStr : String)(implicit formats: Formats) = {
    val j = parse(jsonStr)
    assert(j.isInstanceOf[JObject])
    var j1 = j.asInstanceOf[JObject]
    j1 = JObject(JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(typeName)) :: j1.obj)
    new DynamicTypedStruct(Extraction.extract[StructInstance](j1))
  }

  def createInstance(typeName : String) = {
    new DynamicTypedStruct(
      ts.getDataType(classOf[StructType],typeName).asInstanceOf[IConstructableType[IStruct, ITypedStruct]].createInstance())
  }

  implicit def dynTypedStructToTypedStruct(s : DynamicTypedStruct) = s.ts
  implicit def dynTypedStructToJson(s : DynamicTypedStruct)(implicit formats: Formats) = {
    Extraction.decompose(s.ts)(formats)
  }
}
