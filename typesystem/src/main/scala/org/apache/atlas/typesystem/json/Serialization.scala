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

import org.apache.atlas.typesystem._
import org.apache.atlas.typesystem.persistence.{Id, ReferenceableInstance, StructInstance}
import org.apache.atlas.typesystem.types.DataTypes.{ArrayType, MapType, TypeCategory}
import org.apache.atlas.typesystem.types._
import org.json4s.JsonAST.JInt
import org.json4s._
import org.json4s.native.Serialization._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class BigDecimalSerializer extends CustomSerializer[java.math.BigDecimal](format => (
    {
        case JDecimal(e) => e.bigDecimal
    },
    {
        case e: java.math.BigDecimal => JDecimal(new BigDecimal(e))
    }
    ))

class BigIntegerSerializer extends CustomSerializer[java.math.BigInteger](format => (
    {
        case JInt(e) => e.bigInteger
    },
    {
        case e: java.math.BigInteger => JInt(new BigInt(e))
    }
    ))

class IdSerializer extends CustomSerializer[Id](format => ( {
    case JObject(JField("id", JInt(id)) ::
        JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(className)) ::
        JField("version", JInt(version)) :: Nil) => new Id(id.toLong, version.toInt, className)
    case JObject(JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(className)) ::
        JField("id", JInt(id)) ::
        JField("version", JInt(version)) :: Nil) => new Id(id.toLong, version.toInt, className)
    case JObject(JField("id", JString(id)) ::
        JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(className)) ::
        JField("version", JString(version)) :: Nil) => new Id(id, version.toInt, className)
    case JObject(JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(className)) ::
        JField("id", JString(id)) ::
        JField("version", JString(version)) :: Nil) => new Id(id, version.toInt, className)
}, {
    case id: Id => JObject(JField("id", JString(id.id)),
        JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(id.className)),
        JField("version", JInt(id.version)))
}
    ))

class TypedStructSerializer() extends Serializer[ITypedStruct] {

    def deserialize(implicit format: Formats) = {
        case (TypeInfo(clazz, ptype), json) if classOf[ITypedStruct].isAssignableFrom(clazz) => json match {
            case JObject(fs) =>
                val (typ, fields) = fs.partition(f => f._1 == Serialization.STRUCT_TYPE_FIELD_NAME)
                val typName = typ(0)._2.asInstanceOf[JString].s
                val sT = typSystem.getDataType(
                    classOf[IConstructableType[IStruct, ITypedStruct]], typName).asInstanceOf[IConstructableType[IStruct, ITypedStruct]]
                val s = sT.createInstance()
                Serialization.deserializeFields(typSystem, sT, s, fields)
                s
            case x => throw new MappingException("Can't convert " + x + " to TypedStruct")
        }

    }

    def typSystem = TypeSystem.getInstance()

    /**
     * Implicit conversion from `java.math.BigInteger` to `scala.BigInt`.
     * match the builtin conversion for BigDecimal.
     * See https://groups.google.com/forum/#!topic/scala-language/AFUamvxu68Q
     */
    //implicit def javaBigInteger2bigInt(x: java.math.BigInteger): BigInt = new BigInt(x)

    def serialize(implicit format: Formats) = {
        case e: ITypedStruct =>
            val fields = Serialization.serializeFields(e)
            JObject(JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(e.getTypeName)) :: fields)
    }
}

class TypedReferenceableInstanceSerializer()
    extends Serializer[ITypedReferenceableInstance] {

    def deserialize(implicit format: Formats) = {
        case (TypeInfo(clazz, ptype), json) if classOf[ITypedReferenceableInstance].isAssignableFrom(clazz) => json match {
            case JObject(JField("id", JInt(id)) ::
                JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(className)) ::
                JField("version", JInt(version)) :: Nil) => new Id(id.toLong, version.toInt, className)
            case JObject(JField("id", JString(id)) ::
                JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(className)) ::
                JField("version", JInt(version)) :: Nil) => new Id(id, version.toInt, className)
            case JObject(fs) =>
                var typField: Option[JField] = None
                var idField: Option[JField] = None
                var traitsField: Option[JField] = None
                var fields: List[JField] = Nil

                fs.foreach { f: JField => f._1 match {
                    case Serialization.STRUCT_TYPE_FIELD_NAME => typField = Some(f)
                    case Serialization.ID_TYPE_FIELD_NAME => idField = Some(f)
                    case Serialization.TRAIT_TYPE_FIELD_NAME => traitsField = Some(f)
                    case _ => fields = fields :+ f
                }
                }

                var traitNames: List[String] = Nil

                traitsField.map { t =>
                    val tObj: JObject = t._2.asInstanceOf[JObject]
                    tObj.obj.foreach { oTrait =>
                        val tName: String = oTrait._1
                        traitNames = traitNames :+ tName
                    }
                }

                val typName = typField.get._2.asInstanceOf[JString].s
                val sT = typSystem.getDataType(
                    classOf[ClassType], typName).asInstanceOf[ClassType]
                val id = Serialization.deserializeId(idField.get._2)
                val s = sT.createInstance(id, traitNames: _*)
                Serialization.deserializeFields(typSystem, sT, s, fields)

                traitsField.map { t =>
                    val tObj: JObject = t._2.asInstanceOf[JObject]
                    tObj.obj.foreach { oTrait =>
                        val tName: String = oTrait._1
                        val traitJObj: JObject = oTrait._2.asInstanceOf[JObject]
                        val traitObj = s.getTrait(tName).asInstanceOf[ITypedStruct]
                        val tT = typSystem.getDataType(
                            classOf[TraitType], traitObj.getTypeName).asInstanceOf[TraitType]
                        val (tTyp, tFields) = traitJObj.obj.partition(f => f._1 == Serialization.STRUCT_TYPE_FIELD_NAME)
                        Serialization.deserializeFields(typSystem, tT, traitObj, tFields)
                    }
                }

                s
            case x => throw new MappingException("Can't convert " + x + " to TypedStruct")
        }

    }

    def typSystem = TypeSystem.getInstance()

    def serialize(implicit format: Formats) = {
        case id: Id => Serialization.serializeId(id)
        case e: ITypedReferenceableInstance =>
            val idJ = JField(Serialization.ID_TYPE_FIELD_NAME, Serialization.serializeId(e.getId))
            var fields = Serialization.serializeFields(e)
            val traitsJ: List[JField] = e.getTraits.map(tName => JField(tName, Extraction.decompose(e.getTrait(tName)))).toList

            fields = idJ :: fields
            if (traitsJ.size > 0) {
                fields = fields :+ JField(Serialization.TRAIT_TYPE_FIELD_NAME, JObject(traitsJ: _*))
            }

            JObject(JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(e.getTypeName)) :: fields)
    }
}


object Serialization {
    val STRUCT_TYPE_FIELD_NAME = "$typeName$"
    val ID_TYPE_FIELD_NAME = "$id$"
    val TRAIT_TYPE_FIELD_NAME = "$traits$"

    def extractList(lT: ArrayType, value: JArray)(implicit format: Formats): Any = {
        val dT = lT.getElemType
        value.arr.map(extract(dT, _)).asJava
    }

    def extractMap(mT: MapType, value: JObject)(implicit format: Formats): Any = {
        val kT = mT.getKeyType
        val vT = mT.getValueType
        value.obj.map { f: JField => f._1 -> extract(vT, f._2)}.toMap.asJava
    }

    def extract(dT: IDataType[_], value: JValue)(implicit format: Formats): Any = value match {
        case value: JBool => Extraction.extract[Boolean](value)
        case value: JInt => Extraction.extract[Int](value)
        case value: JDouble => Extraction.extract[Double](value)
        case value: JDecimal => Extraction.extract[BigDecimal](value)
        case value: JString => Extraction.extract[String](value)
        case JNull => null
        case value: JArray => extractList(dT.asInstanceOf[ArrayType], value.asInstanceOf[JArray])
        case value: JObject if dT.getTypeCategory eq TypeCategory.MAP =>
            extractMap(dT.asInstanceOf[MapType], value.asInstanceOf[JObject])
        case value: JObject if ((dT.getTypeCategory eq TypeCategory.STRUCT) || (dT.getTypeCategory eq TypeCategory.TRAIT)) =>
            Extraction.extract[ITypedStruct](value)
        case value: JObject =>
            Extraction.extract[ITypedReferenceableInstance](value)
    }

    def serializeId(id: Id) = JObject(JField("id", JString(id.id)),
        JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(id.className)),
        JField("version", JInt(id.version)))

    def serializeFields(e: ITypedInstance)(implicit format: Formats) = e.fieldMapping.fields.map {
        case (fName, info) => {
            var v = e.get(fName)
            if (v != null && (info.dataType().getTypeCategory eq TypeCategory.MAP)) {
                v = v.asInstanceOf[java.util.Map[_, _]].toMap
            }

            if (v != null && (info.dataType().getTypeCategory eq TypeCategory.CLASS) && !info.isComposite) {
                v = v.asInstanceOf[IReferenceableInstance].getId
            }

            if (v != null && (info.dataType().getTypeCategory eq TypeCategory.ENUM)) {
                v = v.asInstanceOf[EnumValue].value
            }

            JField(fName, Extraction.decompose(v))
        }
    }.toList.map(_.asInstanceOf[JField])

    def deserializeFields[T <: ITypedInstance](typeSystem: TypeSystem,
                                               sT: IConstructableType[_, T],
                                               s: T, fields: List[JField])(implicit format: Formats)
    = {
        //MetadataService.setCurrentService(currentMdSvc)
        fields.foreach { f =>
            val fName = f._1
            val fInfo = sT.fieldMapping.fields(fName)
            if (fInfo != null) {
                //println(fName)
                var v = f._2
                if (fInfo.dataType().getTypeCategory == TypeCategory.TRAIT ||
                    fInfo.dataType().getTypeCategory == TypeCategory.STRUCT) {
                    v = v match {
                        case JObject(sFields) =>
                            JObject(JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(fInfo.dataType.getName)) :: sFields)
                        case x => x
                    }
                }
                s.set(fName, Serialization.extract(fInfo.dataType(), v))
            }
        }
    }

    def deserializeId(value: JValue)(implicit format: Formats) = value match {
        case JObject(JField("id", JInt(id)) ::
            JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(className)) ::
            JField("version", JInt(version)) :: Nil) => new Id(id.toLong, version.toInt, className)
        case JObject(JField("id", JString(id)) ::
            JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(className)) ::
            JField("version", JInt(version)) :: Nil) => new Id(id, version.toInt, className)
    }

    def toJson(value: ITypedReferenceableInstance): String = {
        implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
            new TypedReferenceableInstanceSerializer + new BigDecimalSerializer + new BigIntegerSerializer

        write(value)
    }

    def toJson(value: ITypedInstance): String = {
      implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
        new TypedReferenceableInstanceSerializer + new BigDecimalSerializer + new BigIntegerSerializer

      write(value)
    }

    def toJsonPretty(value: ITypedReferenceableInstance): String = {
        implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
            new TypedReferenceableInstanceSerializer + new BigDecimalSerializer + new BigIntegerSerializer

        writePretty(value)
    }

    def fromJson(jsonStr: String): ITypedReferenceableInstance = {
        implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
            new TypedReferenceableInstanceSerializer + new BigDecimalSerializer + new BigIntegerSerializer

        read[ReferenceableInstance](jsonStr)
    }

    def traitFromJson(jsonStr: String): ITypedInstance = {
      implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
        new TypedReferenceableInstanceSerializer + new BigDecimalSerializer + new BigIntegerSerializer

        read[StructInstance](jsonStr)
    }

    def arrayFromJson(jsonStr: String): ITypedInstance = {
        implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
          new TypedReferenceableInstanceSerializer + new BigDecimalSerializer + new BigIntegerSerializer

        read[StructInstance](jsonStr)
    }
}


