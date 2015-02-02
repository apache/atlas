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

package org.apache.hadoop.metadata.query

import com.thinkaurelius.titan.core.TitanVertex
import com.tinkerpop.blueprints.Direction
import org.apache.hadoop.metadata.types.DataTypes._
import org.apache.hadoop.metadata.{ITypedInstance, ITypedReferenceableInstance}
import org.apache.hadoop.metadata.query.Expressions.{ExpressionException, ComparisonExpression}
import org.apache.hadoop.metadata.query.TypeUtils.FieldInfo
import org.apache.hadoop.metadata.storage.Id
import org.apache.hadoop.metadata.types._


/**
 * Represents the Bridge between the QueryProcessor and the Graph Persistence scheme used.
 * Some of the behaviors captured are:
 * - how is type and id information stored in the Vertex that represents an [[ITypedReferenceableInstance]]
 * - how are edges representing trait and attribute relationships labelled.
 * - how are attribute names mapped to Property Keys in Vertices.
 *
 * This is a work in progress.
 */
trait GraphPersistenceStrategies {
  /**
   * Name of attribute used to store typeName in vertex
   */
  def typeAttributeName : String

  /**
   * Given a dataType and a reference attribute, how is edge labeled
   */
  def edgeLabel(iDataType: IDataType[_], aInfo : AttributeInfo) : String

  def traitLabel(cls : IDataType[_], traitName : String) : String

  /**
   * The propertyKey used to store the attribute in a Graph Vertex.
   * @param dataType
   * @param aInfo
   * @return
   */
  def fieldNameInVertex(dataType : IDataType[_], aInfo : AttributeInfo) : String

  /**
   * from a vertex for an [[ITypedReferenceableInstance]] get the traits that it has.
   * @param v
   * @return
   */
  def traitNames(v : TitanVertex) : Seq[String]

  def edgeLabel(fInfo : FieldInfo) : String = fInfo match {
    case FieldInfo(dataType, aInfo, null) => edgeLabel(dataType, aInfo)
    case FieldInfo(dataType, aInfo, reverseDataType) => edgeLabel(reverseDataType, aInfo)
  }

  def fieldPrefixInSelect : String

  /**
   * extract the Id from a Vertex.
   * @param dataTypeNm the dataType of the instance that the given vertex represents
   * @param v
   * @return
   */
  def getIdFromVertex(dataTypeNm : String, v : TitanVertex) : Id

  /**
   * construct a  [[ITypedReferenceableInstance]] from its vertex
   *
   * @param dataType
   * @param v
   * @return
   */
  def constructClassInstance(dataType : ClassType, v : TitanVertex) : ITypedReferenceableInstance

  def gremlinCompOp(op : ComparisonExpression) = op.symbol match {
    case "=" => "T.eq"
    case "!=" => "T.neq"
    case ">" => "T.gt"
    case ">=" => "T.gte"
    case "<" => "T.lt"
    case "<=" => "T.lte"
    case _ => throw new ExpressionException(op, "Comparison operator not supported in Gremlin")
  }
}

object GraphPersistenceStrategy1 extends GraphPersistenceStrategies {
  val typeAttributeName = "typeName"
  def edgeLabel(dataType: IDataType[_], aInfo : AttributeInfo) = s"${dataType.getName}.${aInfo.name}"
  val fieldPrefixInSelect = "it"
  def traitLabel(cls : IDataType[_], traitName : String) = s"${cls.getName}.$traitName"

  def fieldNameInVertex(dataType : IDataType[_], aInfo : AttributeInfo) = aInfo.name

  def getIdFromVertex(dataTypeNm : String, v : TitanVertex) : Id =
    new Id(v.getId.toString, 0, dataTypeNm)

  def traitNames(v : TitanVertex) : Seq[String] = {
    val s = v.getProperty[String]("traitNames")
    if ( s != null ) {
      Seq[String](s.split(","):_*)
    } else {
      Seq()
    }
  }

  def loadStructInstance(dataType : IConstructableType[_,_ <: ITypedInstance],
                         typInstance : ITypedInstance, v : TitanVertex) : Unit = {
    import scala.collection.JavaConversions._
    dataType.fieldMapping().fields.foreach { t =>
      val fName = t._1
      val aInfo = t._2
      loadAttribute(dataType, aInfo, typInstance, v)
    }
  }

  def constructClassInstance(dataType : ClassType, v : TitanVertex) : ITypedReferenceableInstance = {
    val id = getIdFromVertex(dataType.name, v)
    val tNms = traitNames(v)
    val cInstance = dataType.createInstance(id, tNms:_*)
    // load traits
    tNms.foreach { tNm =>
      val tLabel = traitLabel(dataType, tNm)
      val edges = v.getEdges(Direction.OUT, tLabel)
      val tVertex = edges.iterator().next().getVertex(Direction.IN).asInstanceOf[TitanVertex]
      val tType = TypeSystem.getInstance().getDataType[TraitType](classOf[TraitType], tNm)
      val tInstance = cInstance.getTrait(tNm).asInstanceOf[ITypedInstance]
      loadStructInstance(tType, tInstance, tVertex)
    }
    loadStructInstance(dataType, cInstance, v)
    cInstance
  }

  def loadAttribute(dataType : IDataType[_], aInfo : AttributeInfo, i : ITypedInstance, v : TitanVertex) : Unit = {
    aInfo.dataType.getTypeCategory match {
      case DataTypes.TypeCategory.PRIMITIVE => loadPrimitiveAttribute(dataType, aInfo, i, v)
      case DataTypes.TypeCategory.ENUM => loadEnumAttribute(dataType, aInfo, i, v)
      case DataTypes.TypeCategory.ARRAY =>
        throw new UnsupportedOperationException(s"load for ${aInfo.dataType()} not supported")
      case DataTypes.TypeCategory.MAP =>
        throw new UnsupportedOperationException(s"load for ${aInfo.dataType()} not supported")
      case DataTypes.TypeCategory.STRUCT => loadStructAttribute(dataType, aInfo, i, v)
      case DataTypes.TypeCategory.TRAIT =>
        throw new UnsupportedOperationException(s"load for ${aInfo.dataType()} not supported")
      case DataTypes.TypeCategory.CLASS => loadStructAttribute(dataType, aInfo, i, v)
    }
  }

  private def loadEnumAttribute(dataType : IDataType[_], aInfo : AttributeInfo, i : ITypedInstance, v : TitanVertex)
  : Unit = {
    val fName = fieldNameInVertex(dataType, aInfo)
    i.setInt(aInfo.name, v.getProperty[java.lang.Integer](fName))
  }

  private def loadPrimitiveAttribute(dataType : IDataType[_], aInfo : AttributeInfo,
                                     i : ITypedInstance, v : TitanVertex) : Unit = {
    val fName = fieldNameInVertex(dataType, aInfo)
    aInfo.dataType() match {
      case x : BooleanType => i.setBoolean(aInfo.name, v.getProperty[java.lang.Boolean](fName))
      case x : ByteType => i.setByte(aInfo.name, v.getProperty[java.lang.Byte](fName))
      case x : ShortType => i.setShort(aInfo.name, v.getProperty[java.lang.Short](fName))
      case x : IntType => i.setInt(aInfo.name, v.getProperty[java.lang.Integer](fName))
      case x : LongType => i.setLong(aInfo.name, v.getProperty[java.lang.Long](fName))
      case x : FloatType => i.setFloat(aInfo.name, v.getProperty[java.lang.Float](fName))
      case x : DoubleType => i.setDouble(aInfo.name, v.getProperty[java.lang.Double](fName))
      case x : StringType => i.setString(aInfo.name, v.getProperty[java.lang.String](fName))
      case _ => throw new UnsupportedOperationException(s"load for ${aInfo.dataType()} not supported")
    }
  }

  private def loadStructAttribute(dataType : IDataType[_], aInfo : AttributeInfo,
                                  i : ITypedInstance, v : TitanVertex) : Unit = {
    val eLabel = edgeLabel(FieldInfo(dataType, aInfo, null))
    val edges = v.getEdges(Direction.OUT, eLabel)
    val sVertex = edges.iterator().next().getVertex(Direction.IN).asInstanceOf[TitanVertex]
    if ( aInfo.dataType().getTypeCategory == DataTypes.TypeCategory.STRUCT ) {
      val sType = aInfo.dataType().asInstanceOf[StructType]
      val sInstance = sType.createInstance()
      loadStructInstance(sType, sInstance, sVertex)
      i.set(aInfo.name, sInstance)
    } else {
      val cInstance = constructClassInstance(aInfo.dataType().asInstanceOf[ClassType], sVertex)
      i.set(aInfo.name, cInstance)
    }

  }
}

