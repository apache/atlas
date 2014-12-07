package org.apache.metadata.json

import org.apache.metadata.types.DataTypes.{MapType, TypeCategory, ArrayType}
import org.apache.metadata.{MetadataException, MetadataService}
import org.apache.metadata.types._
import org.json4s.JsonAST.JInt
import org.json4s._
import org.json4s.native.Serialization.{read, write => swrite}
import org.json4s.reflect.{ScalaType, Reflector}
import java.util.regex.Pattern
import java.util.Date
import org.apache.metadata.storage.TypedStruct
import collection.JavaConversions._
import scala.collection.JavaConverters._

class BigDecimalSerializer extends CustomSerializer[java.math.BigDecimal](format => ( {
    case JDecimal(e) => e.bigDecimal
}, {
  case e: java.math.BigDecimal => JDecimal(new BigDecimal(e))
}
  ))

class BigIntegerSerializer extends CustomSerializer[java.math.BigInteger](format => ( {
  case JInt(e) => e.bigInteger
}, {
  case e: java.math.BigInteger => JInt(new BigInt(e))
}
  ))

class TypedStructSerializer extends Serializer[TypedStruct] {

  def extractList(lT : ArrayType, value : JArray)(implicit format: Formats) : Any = {
    val dT = lT.getElemType
    value.arr.map(extract(dT, _)).asJava
  }

  def extractMap(mT : MapType, value : JObject)(implicit format: Formats) : Any = {
    val kT = mT.getKeyType
    val vT = mT.getValueType
    value.obj.map{f : JField => f._1 -> extract(vT, f._2) }.toMap.asJava
  }

  def extract(dT : IDataType[_], value : JValue)(implicit format: Formats) : Any = value match {
    case value : JBool => Extraction.extract[Boolean](value)
    case value : JInt => Extraction.extract[Int](value)
    case value : JDouble => Extraction.extract[Double](value)
    case value : JDecimal => Extraction.extract[BigDecimal](value)
    case value : JString => Extraction.extract[String](value)
    case JNull => null
    case value : JArray => extractList(dT.asInstanceOf[ArrayType], value.asInstanceOf[JArray])
    case value : JObject if dT.getTypeCategory eq TypeCategory.MAP =>
      extractMap(dT.asInstanceOf[MapType], value.asInstanceOf[JObject])
    case value : JObject  =>
      Extraction.extract[TypedStruct](value)
  }

  def deserialize(implicit format: Formats) = {
    case (TypeInfo(clazz, ptype), json) if classOf[TypedStruct].isAssignableFrom(clazz) => json match {
      case JObject(fs) =>
        val(typ, fields) = fs.partition(f => f._1 == Serialization.STRUCT_TYPE_FIELD_NAME)
        val typName = typ(0)._2.asInstanceOf[JString].s
        val sT = MetadataService.getCurrentTypeSystem().getDataType(typName).asInstanceOf[StructType]
        val s = sT.createInstance()
        fields.foreach { f =>
          val fName = f._1
          val fInfo = sT.fields(fName)
          if ( fInfo != null ) {
            //println(fName)
            var v = f._2
            if ( fInfo.dataType().isInstanceOf[StructType] ) {
              v = v match {
                case JObject(sFields) =>
                  JObject(JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(fInfo.dataType.getName)) :: sFields)
                case x => x
              }
            }
            s.set(fName, extract(fInfo.dataType(), v))
          }
        }
        s
      case x => throw new MappingException("Can't convert " + x + " to TypedStruct")
    }

  }

  /**
   * Implicit conversion from `java.math.BigInteger` to `scala.BigInt`.
   * match the builtin conversion for BigDecimal.
   * See https://groups.google.com/forum/#!topic/scala-language/AFUamvxu68Q
   */
  //implicit def javaBigInteger2bigInt(x: java.math.BigInteger): BigInt = new BigInt(x)

  def serialize(implicit format: Formats) = {
    case e: TypedStruct =>
      val fields  = e.dataType.fields.map {
        case (fName, info) => {
          var v = e.get(fName)
          if ( v != null && (info.dataType().getTypeCategory eq TypeCategory.MAP) ) {
            v = v.asInstanceOf[java.util.Map[_,_]].toMap
          }
          JField(fName, Extraction.decompose(v))
        }
      }.toList.map(_.asInstanceOf[JField])
      JObject(JField(Serialization.STRUCT_TYPE_FIELD_NAME, JString(e.dataType.getName)) :: fields)
  }
}

object Serialization {
  val STRUCT_TYPE_FIELD_NAME = "$typeName$"
}
