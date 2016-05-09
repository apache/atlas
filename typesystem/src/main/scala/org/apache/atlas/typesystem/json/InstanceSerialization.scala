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

import java.text.SimpleDateFormat

import org.apache.atlas.typesystem._
import org.apache.atlas.typesystem.persistence.Id
import org.apache.atlas.typesystem.types._
import org.json4s._
import org.json4s.native.Serialization._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object InstanceSerialization {

  case class _Id(id : String, version : Int, typeName : String, state : Option[String])
  case class _Struct(typeName : String, values : Map[String, AnyRef])
  case class _Reference(id : Option[_Id],
                        typeName : String,
                        values : Map[String, AnyRef],
                        traitNames : List[String],
                        traits : Map[String, _Struct])

  def Try[B](x : => B) : Option[B] = {
    try { Some(x) } catch { case _ : Throwable => None }
  }

  /**
   * Convert a Map into
   * - a Reference or
   * - a Struct or
   * - a Id or
   * - a Java Map whose values are recursively converted.
   * @param jsonMap
   * @param format
   */
  class InstanceJavaConversion(jsonMap : Map[String, _], format : Formats) {

    /**
     * For Id, Map must contain the [[_Id]] 'typeHint'
     * @return
     */
    def idClass: Option[String] = {
      jsonMap.get(format.typeHintFieldName).flatMap(x => Try(x.asInstanceOf[String])).
        filter(s => s == classOf[_Id].getName)
    }

    /**
     * validate and extract 'id' attribute from Map
     * @return
     */
    def id: Option[String] = {
      jsonMap.get("id").filter(_.isInstanceOf[String]).flatMap(v => Some(v.asInstanceOf[String]))
    }

    /**
     * validate and extract 'state' attribute from Map
     * @return
     */
    def state: Option[String] = {
      jsonMap.get("state").filter(_.isInstanceOf[String]).flatMap(v => Some(v.asInstanceOf[String]))
    }

    /**
     * validate and extract 'version' attribute from Map
     * @return
     */
    def version: Option[Int] = {
      jsonMap.get("version").flatMap{
        case i : Int => Some(i)
        case bI : BigInt => Some(bI.toInt)
        case _ => None
      }
    }

    /**
     * A Map is an Id if:
     * - it has the correct [[format.typeHintFieldName]]
     * - it has a 'typeName'
     * - it has an 'id'
     * - it has a 'version'
     * @return
     */
    def convertId : Option[_Id] = {
      for {
        refClass <- idClass
        typNm <- typeName
        i <- id
        s <- Some(state)
        v <- version
      } yield _Id(i, v, typNm, s)
    }

    /**
     * validate and extract 'typeName' attribute from Map
     * @return
     */
    def typeName: Option[String] = {
      jsonMap.get("typeName").flatMap(x => Try(x.asInstanceOf[String]))
    }

    /**
     * For Reference, Map must contain the [[_Reference]] 'typeHint'
     * @return
     */
    def referenceClass: Option[String] = {
      jsonMap.get(format.typeHintFieldName).flatMap(x => Try(x.asInstanceOf[String])).
        filter(s => s == classOf[_Reference].getName)
    }

    /**
     * For Reference, Map must contain the [[_Struct]] 'typeHint'
     * @return
     */
    def structureClass: Option[String] = {
      jsonMap.get(format.typeHintFieldName).flatMap(x => Try(x.asInstanceOf[String])).
        filter(s => s == classOf[_Struct].getName)
    }

    /**
     * validate and extract 'values' attribute from Map
     * @return
     */
    def valuesMap: Option[Map[String, AnyRef]] = {
      jsonMap.get("values").flatMap(x => Try(x.asInstanceOf[Map[String, AnyRef]]))
    }

    /**
     * validate and extract 'traitNames' attribute from Map
     * @return
     */
    def traitNames: Option[Seq[String]] = {
      jsonMap.get("traitNames").flatMap(x => Try(x.asInstanceOf[Seq[String]]))
    }

    /**
     * A Map is an Struct if:
     * - it has the correct [[format.typeHintFieldName]]
     * - it has a 'typeName'
     * - it has a 'values' attribute
     * @return
     */
    def struct: Option[_Struct] = {
      for {
        refClass <- structureClass
        typNm <- typeName
        values <- valuesMap
      } yield _Struct(typNm, values)
    }

    def sequence[A](a : List[(String,Option[A])]) : Option[List[(String,A)]] = a match {
      case Nil => Some(Nil)
      case h :: t => {
        h._2 flatMap {hh => sequence(t) map { (h._1,hh) :: _}}
      }
    }

    /**
     * Extract and convert the traits in this Map.
     *
     * @return
     */
    def traits: Option[Map[String, _Struct]] = {

      /**
       * 1. validate and extract 'traitss' attribute from Map
       * Must be a Map[String, _]
       */
      val tEntry : Option[Map[String, _]] = jsonMap.get("traits").flatMap(x => Try(x.asInstanceOf[Map[String, _]]))


      /**
       * Try to convert each entry in traits Map into a _Struct
       * - each entry itself must be of type Map[String, _]
       * - recursively call InstanceJavaConversion on this Map to convert to a struct
       */
      val x: Option[List[(String, Option[_Struct])]] = tEntry.map { tMap: Map[String, _] =>
        val y: Map[String, Option[_Struct]] = tMap.map { t =>
          val tObj: Option[_Struct] = Some(t._2).flatMap(x => Try(x.asInstanceOf[Map[String, _]])).
            flatMap { traitObj: Map[String, _] =>
            new InstanceJavaConversion(traitObj, format).struct
          }
          (t._1, tObj)
        }
        y.toList
      }

      /**
       * Convert a List of Optional successes into an Option of List
       */
      x flatMap (sequence(_)) map (_.toMap)

    }

    def idObject : Option[_Id] = {
      val idM = jsonMap.get("id").flatMap(x => Try(x.asInstanceOf[Map[String, _]]))
      idM flatMap  (m => new InstanceJavaConversion(m, format).convertId)
    }

    /**
     * A Map is an Reference if:
     * - it has the correct [[format.typeHintFieldName]]
     * - it has a 'typeName'
     * - it has a 'values' attribute
     * - it has 'traitNames' attribute
     * - it has 'traits' attribute
     * @return
     */
    def reference : Option[_Reference] = {
      for {
        refClass <- referenceClass
        typNm <- typeName
        i <- Some(idObject)
        values <- valuesMap
        traitNms <- traitNames
        ts <- traits
      } yield _Reference(i, typNm, values, traitNms.toList, ts)
    }

    /**
     * A Map converted to Java:
     * - if Map can be materialized as a _Reference, materialize and then recursively call asJava on it.
     * - if Map can be materialized as a _Struct, materialize and then recursively call asJava on it.
     * - if Map can be materialized as a _Id, materialize and then recursively call asJava on it.
     * - otherwise convert each value with asJava and construct as new JavaMap.
     * @return
     */
    def convert : Any = {
      reference.map(asJava(_)(format)).getOrElse {
        struct.map(asJava(_)(format)).getOrElse {
          convertId.map(asJava(_)(format)).getOrElse {
            jsonMap.map { t =>
              (t._1 -> asJava(t._2)(format))
            }.toMap.asJava
          }
        }
      }
    }
  }

  def asJava(v : Any)(implicit format: Formats) : Any = v match {
    case i : _Id => new Id(i.id, i.version, i.typeName, i.state.orNull)
    case s : _Struct => new Struct(s.typeName, asJava(s.values).asInstanceOf[java.util.Map[String, Object]])
    case r : _Reference => {
      val id = r.id match {
        case Some(i) => new Id(i.id, i.version, i.typeName, i.state.orNull)
        case None => new Id(r.typeName)
      }
      new Referenceable(id,
        r.typeName,
        asJava(r.values).asInstanceOf[java.util.Map[String, Object]],
        asJava(r.traitNames).asInstanceOf[java.util.List[String]],
        asJava(r.traits).asInstanceOf[java.util.Map[String, IStruct]])
    }
    case l : List[_] => l.map(e => asJava(e)).toList.asJava
    case m : Map[_, _] if Try{m.asInstanceOf[Map[String,_]]}.isDefined => {
      if (m.keys.size == 2 && m.keys.contains("value") && m.keys.contains("ordinal")) {
        new EnumValue(m.get("value").toString, m.get("ordinal").asInstanceOf[BigInt].intValue())
      } else {
        new InstanceJavaConversion(m.asInstanceOf[Map[String,_]], format).convert
      }
    }

    case _ => v
  }

  def asScala(v : Any) : Any = v match {
    case i : Id => _Id(i._getId(), i.getVersion, i.getClassName, Some(i.getStateAsString))
    case r : IReferenceableInstance => {
      val traits = r.getTraits.map { tName =>
        val t = r.getTrait(tName).asInstanceOf[IStruct]
        (tName -> _Struct(t.getTypeName, asScala(t.getValuesMap).asInstanceOf[Map[String, AnyRef]]))
      }.toMap
      _Reference(Some(asScala(r.getId).asInstanceOf[_Id]),
        r.getTypeName, asScala(r.getValuesMap).asInstanceOf[Map[String, AnyRef]],
        asScala(r.getTraits).asInstanceOf[List[String]],
        traits.asInstanceOf[Map[String, _Struct]])
    }
    case s : IStruct => _Struct(s.getTypeName, asScala(s.getValuesMap).asInstanceOf[Map[String, AnyRef]])
    case l : java.util.List[_] => l.asScala.map(e => asScala(e)).toList
    case m : java.util.Map[_, _] => m.asScala.map(t => (asScala(t._1), asScala(t._2))).toMap
    case _ => v
  }

  val _formats = new DefaultFormats {
    override val dateFormatter = TypeSystem.getInstance().getDateFormat.asInstanceOf[SimpleDateFormat]
    override val typeHints = FullTypeHints(List(classOf[_Id], classOf[_Struct], classOf[_Reference]))
  }

  def buildFormat(withBigDecimals : Boolean) = {
    if (withBigDecimals)
      _formats + new BigDecimalSerializer + new BigIntegerSerializer
    else
      _formats
  }

  def _toJson(value: AnyRef, withBigDecimals : Boolean = false): String = {
    implicit val formats = buildFormat(withBigDecimals)

    val _s : AnyRef = asScala(value).asInstanceOf[AnyRef]
    writePretty(_s)
  }

  def toJson(value: IStruct, withBigDecimals : Boolean = false): String = {
    _toJson(value, withBigDecimals)
  }

  def fromJsonStruct(jsonStr: String, withBigDecimals : Boolean = false): Struct = {
    implicit val formats = buildFormat(withBigDecimals)
    val _s = read[_Struct](jsonStr)
    asJava(_s).asInstanceOf[Struct]
  }

  //def toJsonReferenceable(value: Referenceable, withBigDecimals : Boolean = false): String = _toJson(value, withBigDecimals)
  def fromJsonReferenceable(jsonStr: String, withBigDecimals : Boolean = false): Referenceable = {
    implicit val formats = buildFormat(withBigDecimals)
    val _s = read[_Reference](jsonStr)
    asJava(_s).asInstanceOf[Referenceable]
  }
}
