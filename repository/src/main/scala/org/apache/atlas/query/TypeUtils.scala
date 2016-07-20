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

package org.apache.atlas.query

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.apache.atlas.AtlasException
import org.apache.atlas.query.Expressions.{LimitExpression, PathExpression, SelectExpression}
import org.apache.atlas.repository.Constants
import org.apache.atlas.typesystem.types.DataTypes.{ArrayType, PrimitiveType, TypeCategory}
import org.apache.atlas.typesystem.types._

object TypeUtils {
    val typSystem = TypeSystem.getInstance()

    def numericTypes : Seq[PrimitiveType[_]] = Seq(DataTypes.BYTE_TYPE,
        DataTypes.SHORT_TYPE,
        DataTypes.INT_TYPE,
        DataTypes.FLOAT_TYPE,
        DataTypes.LONG_TYPE,
        DataTypes.DOUBLE_TYPE,
        DataTypes.BIGINTEGER_TYPE,
        DataTypes.BIGDECIMAL_TYPE)

    def combinedType(typ1 : IDataType[_], typ2 : IDataType[_]) : PrimitiveType[_] = {
        val typ1Idx =  if (numericTypes.contains(typ1))  Some(numericTypes.indexOf(typ1)) else None
        val typ2Idx =  if (numericTypes.contains(typ2))  Some(numericTypes.indexOf(typ2)) else None

        if ( typ1Idx.isDefined && typ2Idx.isDefined ) {
            val rIdx = math.max(typ1Idx.get, typ2Idx.get)

            if ( (typ1 == DataTypes.FLOAT_TYPE && typ2 == DataTypes.LONG_TYPE) ||
                (typ1 == DataTypes.LONG_TYPE && typ2 == DataTypes.FLOAT_TYPE) ) {
                return DataTypes.DOUBLE_TYPE
            }
            return numericTypes(rIdx)
        }

        throw new AtlasException(s"Cannot combine types: ${typ1.getName} and ${typ2.getName}")
    }

    var tempStructCounter : AtomicInteger = new AtomicInteger(0)
    val TEMP_STRUCT_NAME_PREFIX = "__tempQueryResultStruct"
    def createStructType(selectExprs : List[Expressions.AliasExpression]) : StructType = {
        val aDefs = new Array[AttributeDefinition](selectExprs.size)
        selectExprs.zipWithIndex.foreach { t =>
            val (e,i) = t
            aDefs(i) = new AttributeDefinition(e.alias,e.dataType.getName, Multiplicity.OPTIONAL, false, null)
        }
        return typSystem.defineQueryResultType(s"${TEMP_STRUCT_NAME_PREFIX}${tempStructCounter.getAndIncrement}",
            null,
            aDefs:_*);
    }

    object ResultWithPathStruct {
      val pathAttrName = "path"
      val resultAttrName = "result"
      val pathAttrType = DataTypes.arrayTypeName(typSystem.getIdType.getStructType)

      val pathAttr = new AttributeDefinition(pathAttrName, pathAttrType, Multiplicity.COLLECTION, false, null)

      def createType(pE : PathExpression, resultType : IDataType[_]) : StructType = {
        val resultAttr = new AttributeDefinition(resultAttrName, resultType.getName, Multiplicity.REQUIRED, false, null)
        val typName = s"${TEMP_STRUCT_NAME_PREFIX}${tempStructCounter.getAndIncrement}"
        val m : java.util.HashMap[String, IDataType[_]] = new util.HashMap[String, IDataType[_]]()
        if (pE.child.isInstanceOf[SelectExpression] || pE.child.isInstanceOf[LimitExpression]) {
          m.put(pE.child.dataType.getName, pE.child.dataType)
        }
        typSystem.defineQueryResultType(typName, m, pathAttr, resultAttr);
      }
    }

  /**
   * Structure representing the Closure Graph.
   * Returns:
   * 1. A map of vertexId -> vertex Info(these are the attributes requested in the query)
   * 2. A edges map: each entry is a mapping from an vertexId to the List of adjacent vertexIds.
   *
   * '''The Vertex Map doesn't contain all the vertices in the Graph. Only the ones for which Attributes are
   * available.''' These are the vertices that represent the EntityType whose Closure was requested. For e.g. for
   * Table Lineage the ''vertex map'' will contain information  about Tables, but not about ''Load Process'' vertices
   * that connect Tables.
   */
  object GraphResultStruct {
    val SRC_PREFIX = "src"
    val DEST_PREFIX = "dest"

    val verticesAttrName = "vertices"
    val edgesAttrName = "edges"
    val vertexIdAttrName = "vertexId"

    lazy val edgesAttrType = typSystem.defineMapType(DataTypes.STRING_TYPE,
      typSystem.defineArrayType(DataTypes.STRING_TYPE))

    def createType(resultWithPathType: StructType): StructType = {
      val resultType = resultWithPathType.fieldMapping().fields.get(ResultWithPathStruct.resultAttrName).dataType()

      val verticesAttrType = typSystem.defineMapType(DataTypes.STRING_TYPE,
        vertexType(resultType.asInstanceOf[StructType]))
      val typName = s"${TEMP_STRUCT_NAME_PREFIX}${tempStructCounter.getAndIncrement}"
      val verticesAttr = new AttributeDefinition(verticesAttrName, verticesAttrType.getName,
        Multiplicity.REQUIRED, false, null)
      val edgesAttr = new AttributeDefinition(edgesAttrName, edgesAttrType.getName, Multiplicity.REQUIRED, false, null)

      val m: java.util.HashMap[String, IDataType[_]] = new util.HashMap[String, IDataType[_]]()
      m.put(resultWithPathType.getName, resultWithPathType)
      m.put(resultType.getName, resultType)
      m.put(edgesAttrType.getName, edgesAttrType)
      m.put(verticesAttrType.getName, verticesAttrType)
      typSystem.defineQueryResultType(typName, m, verticesAttr, edgesAttr)
    }

    private def vertexType(resultType: StructType): StructType = {

      import scala.collection.JavaConverters._

      var attrs: List[AttributeDefinition] =
        resultType.fieldMapping.fields.asScala.filter(_._1.startsWith(s"${SRC_PREFIX}_")).mapValues { aInfo =>

        new AttributeDefinition(aInfo.name.substring(s"${SRC_PREFIX}_".length), aInfo.dataType.getName,
          aInfo.multiplicity, aInfo.isComposite, aInfo.reverseAttributeName)
      }.values.toList

      attrs = new AttributeDefinition(vertexIdAttrName, typSystem.getIdType.getStructType.name,
        Multiplicity.REQUIRED, false, null) :: attrs

      return typSystem.defineQueryResultType(s"${TEMP_STRUCT_NAME_PREFIX}${tempStructCounter.getAndIncrement}",
        null,
        attrs: _*)
    }
  }

    def fieldMapping(iDataType: IDataType[_]) : Option[FieldMapping] = iDataType match {
        case c : ClassType => Some(c.fieldMapping())
        case t : TraitType => Some(t.fieldMapping())
        case s : StructType => Some(s.fieldMapping())
        case _ => None
    }

    def hasFields(iDataType: IDataType[_]) : Boolean = {
        fieldMapping(iDataType).isDefined
    }

    import scala.language.existentials
    case class FieldInfo(dataType : IDataType[_],
                         attrInfo : AttributeInfo,
                         reverseDataType : IDataType[_] = null,
                          traitName : String = null) {
        def isReverse = reverseDataType != null
        override  def toString : String = {
            if ( traitName != null ) {
              s"""FieldInfo("${dataType.getName}", "$traitName")"""
            }
            else if ( reverseDataType == null ) {
                s"""FieldInfo("${dataType.getName}", "${attrInfo.name}")"""
            } else {
                s"""FieldInfo("${dataType.getName}", "${attrInfo.name}", "${reverseDataType.getName}")"""
            }
        }
    }

    val FIELD_QUALIFIER = "(.*?)(->.*)?".r

    /**
     * Given a ComposedType `t` and a name resolve using the following rules:
     * - if `id` is a field in `t` resolve to the field
     * - if `id` is the name of a  Struct|Class|Trait Type and it has a field that is of type `t` then return that type
     *
     * For e.g.
     * 1. if we have types Table(name : String, cols : List[Column]), Column(name : String) then
     * `resolveReference(Table, "cols")` resolves to type Column. So a query can be "Table.cols"
     * 2. But if we have Table(name : String), Column(name : String, tbl : Table) then "Table.Column" will resolve
     * to type Column
     *
     * This way the language will support navigation even if the relationship is one-sided.
     *
     * @param typ
     * @param id
     * @return
     */
    def resolveReference(typ : IDataType[_], id : String) : Option[FieldInfo] = {

        val fMap = fieldMapping(typ)
        if ( fMap.isDefined) {

            if (fMap.get.fields.containsKey(id)) {
                return Some(FieldInfo(typ,fMap.get.fields.get(id)))
            }

            val systemField = Constants.getAttributeInfoForSystemAttributes(id)
            if (systemField != null) {
              return Some(FieldInfo(systemField.dataType(), systemField))
            }

            try {
              val FIELD_QUALIFIER(clsNm, rest) = id
                val idTyp = typSystem.getDataType(classOf[IDataType[_]], clsNm)
                val idTypFMap = fieldMapping(idTyp)

                if (rest != null ) {
                  val attrNm = rest.substring(2)

                  if (idTypFMap.get.fields.containsKey(attrNm)) {
                    return Some(FieldInfo(typ,idTypFMap.get.fields.get(attrNm), idTyp))
                  }
                }

                if (idTypFMap.isDefined) {
                    import scala.collection.JavaConversions._
                    val fields: Seq[AttributeInfo] = idTypFMap.get.fields.values().filter { aInfo =>
                        aInfo.dataType() == typ ||
                            ( aInfo.dataType().getTypeCategory == TypeCategory.ARRAY &&
                                aInfo.dataType().asInstanceOf[ArrayType].getElemType == typ
                                )
                    }.toSeq
                    if (fields.size == 1) {
                        return Some(FieldInfo(typ, fields(0), idTyp))
                    }
                    /*
                     * is there only 1 array field of this type?
                     * If yes resolve to it.
                     * @todo: allow user to specify the relationship to follow by further qualifying the type. for e.g.
                     *   field("LoadProcess.inputTables")
                     */
                    val aFields = fields.filter { aInfo => aInfo.dataType().getTypeCategory == TypeCategory.ARRAY}
                    if (aFields.size == 1) {
                        return Some(FieldInfo(typ, aFields(0), idTyp))
                    }
                }
            } catch {
                case _ : AtlasException => None
            }
        }
        None
    }

    def resolveAsClassType(id : String) : Option[ClassType] = {
        try {
            Some(typSystem.getDataType(classOf[ClassType], id))
        } catch {
            case _ : AtlasException => None
        }
    }

    def resolveAsTraitType(id : String) : Option[TraitType] = {
        try {
            Some(typSystem.getDataType(classOf[TraitType], id))
        } catch {
            case _ : AtlasException => None
        }
    }
}
