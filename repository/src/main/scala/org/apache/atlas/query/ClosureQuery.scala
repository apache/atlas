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

import com.thinkaurelius.titan.core.TitanGraph
import org.apache.atlas.query.Expressions._
import org.apache.atlas.typesystem.ITypedStruct
import org.apache.atlas.typesystem.json.{InstanceSerialization, Serialization}
import org.apache.atlas.typesystem.persistence.StructInstance
import org.apache.atlas.typesystem.types.DataTypes.{MapType, PrimitiveType}
import org.apache.atlas.typesystem.types.{DataTypes, StructType, TypeSystem}

/**
 * Represents a Query to compute the closure based on a relationship between entities of a particular type.
 * For e.g. Database Tables are related to each other to capture the '''Lineage''' of data in a Table based
 * on other Tables.
 *
 * A Closure Query is specified by the following information:
 *  - The Type whose instances are in a closure relationship. For e.g. 'Table'
 *  - The Closure relation. This is specified as an ''Attribute path''. For e.g. if we have the following model:
 * {{{
 *   class Table {
 *    name : String,
 *    ...
 *   }
 *
 *   class LoadTableProcess {
 *    name : String,
 *    inputTables : List[Table],
 *    outputTable : Table,
 *    ...
 *   }
 * }}}
 * ''LoadTable'' instance captures the relationship between the data in an output Table and a set of input Tables.
 * In order to compute the '''Lineage''' of a Table, the ''Attribute path'' that relates 2 Tables is
 * '''[(LoadTableProcess,outputTable), inputTables]'''. This list is saying that for any Table I want to connect to other
 * tables via the LoadProcess.outputTable attribute, and then via the inputTables attribute. So each entry in the
 * Attribute Path represents an attribute in an object. For reverse relations the Type and attribute must be specified,
 * as in 'LoadTableProcess,outputTable)', whereas for forward relations the attribute name is sufficient.
 *  - The depth of the traversal. Certain times you are not interested in the complete closure, but to only
 * discover related instances up to a certain depth. Specify the depth as number of hops, or you can ask for the
 * complete closure.
 *  - You can ask for certain attributes to be returned. For e.g. you may only want the Table name, owner and
 * creationDate. By default only the Ids of the related instances is returned.
 *  - For pair of related instances, you optionally ask for the Path of the relation to be returned. This is
 * returned as a list of ''Id''s.
 *
 * Given these 5 things the ClosureQuery can be executed, it returns a GremlinQueryResult of the Closure Query.
 */
trait ClosureQuery {

  val SRC_PREFIX = TypeUtils.GraphResultStruct.SRC_PREFIX
  val DEST_PREFIX = TypeUtils.GraphResultStruct.DEST_PREFIX

  sealed trait PathAttribute {

    def toExpr : Expression = this match {
      case r : Relation => id(r.attributeName)
      case rr : ReverseRelation => id(s"${rr.typeName}->${rr.attributeName}")
    }

    def toFieldName : String = this match {
      case r : Relation => r.attributeName
      case rr : ReverseRelation => rr.typeName
    }
  }
  case class ReverseRelation(typeName : String, attributeName : String) extends PathAttribute
  case class Relation(attributeName : String) extends PathAttribute

  /**
   * Type on whose instances the closure needs to be computed
   * @return
   */
  def closureType : String

  /**
   * specify how instances are related.
   */
  def closureRelation  : List[PathAttribute]

  /**
   * The maximum hops between related instances. A [[None]] implies there is maximum.
   * @return
   */
  def depth : Option[Int]

  /**
   * The attributes to return for the instances. These will be prefixed by 'src_' and 'dest_' in the
   * output rows.
   * @return
   */
  def selectAttributes : Option[List[String]]

  /**
   * specify if the Path should be returned.
   * @return
   */
  def withPath : Boolean

  def persistenceStrategy: GraphPersistenceStrategies
  def g: TitanGraph

  def pathExpr : Expressions.Expression = {
    closureRelation.tail.foldLeft(closureRelation.head.toExpr)((b,a) => b.field(a.toFieldName))
  }

  def selectExpr(alias : String) : List[Expression] = {
    selectAttributes.map { _.map { a =>
      id(alias).field(a).as(s"${alias}_$a")
    }
    }.getOrElse(List(id(alias)))
  }

  /**
   * hook to allow a filter to be added for the closureType
   * @param expr
   * @return
   */
  def srcCondition(expr : Expression) : Expression = expr

  def expr : Expressions.Expression = {
    val e = srcCondition(Expressions._class(closureType)).as(SRC_PREFIX).loop(pathExpr).as(DEST_PREFIX).
      select((selectExpr(SRC_PREFIX) ++ selectExpr(DEST_PREFIX)):_*)
    if (withPath) e.path else e
  }

  def evaluate(): GremlinQueryResult = {
    var e = expr
    QueryProcessor.evaluate(e, g, persistenceStrategy)
  }

  def graph(res: GremlinQueryResult) : GraphResult = {

    if (!withPath) {
      throw new ExpressionException(expr, "Graph requested for non Path Query")
    }

    import scala.collection.JavaConverters._

    val graphResType = TypeUtils.GraphResultStruct.createType(res.resultDataType.asInstanceOf[StructType])
    val vertexPayloadType = {
      val mT = graphResType.fieldMapping.fields.get(TypeUtils.GraphResultStruct.verticesAttrName).
        dataType().asInstanceOf[MapType]
      mT.getValueType.asInstanceOf[StructType]
    }

    def id(idObj : StructInstance) : String = idObj.getString(TypeSystem.ID_STRUCT_ID_ATTRNAME)

    def vertexStruct(idObj : StructInstance, resRow : ITypedStruct, attrPrefix : String) : StructInstance = {
      val vP = vertexPayloadType.createInstance()
      vP.set(TypeUtils.GraphResultStruct.vertexIdAttrName, idObj)
      vertexPayloadType.fieldMapping.fields.asScala.keys.
        filter(_ != TypeUtils.GraphResultStruct.vertexIdAttrName).foreach{a =>
        vP.set(a, resRow.get(s"${attrPrefix}$a"))
      }
      vP.asInstanceOf[StructInstance]
    }

    val instance = graphResType.createInstance()
    val vertices = new util.HashMap[String, AnyRef]()
    val edges = new util.HashMap[String,java.util.List[String]]()

    /**
     * foreach resultRow
     *   for each Path entry
     *     add an entry in the edges Map
     *   add an entry for the Src Vertex to the vertex Map
     *   add an entry for the Dest Vertex to the vertex Map
     */
    res.rows.asScala.map(_.asInstanceOf[StructInstance]).foreach { r =>

      val path = r.get(TypeUtils.ResultWithPathStruct.pathAttrName).asInstanceOf[java.util.List[_]].asScala
      val srcVertex = path.head.asInstanceOf[StructInstance]

      var currVertex = srcVertex
      path.tail.foreach { n =>
        val nextVertex = n.asInstanceOf[StructInstance]
        val iList = if (!edges.containsKey(id(currVertex))) {
          val l = new util.ArrayList[String]()
          edges.put(id(currVertex), l)
          l
        } else {
          edges.get(id(currVertex))
        }
        if ( !iList.contains(id(nextVertex))) {
          iList.add(id(nextVertex))
        }
        currVertex = nextVertex
      }
      val vertex = r.get(TypeUtils.ResultWithPathStruct.resultAttrName)
      vertices.put(id(srcVertex), vertexStruct(srcVertex,
        r.get(TypeUtils.ResultWithPathStruct.resultAttrName).asInstanceOf[ITypedStruct],
        s"${SRC_PREFIX}_"))
      vertices.put(id(currVertex), vertexStruct(currVertex,
        r.get(TypeUtils.ResultWithPathStruct.resultAttrName).asInstanceOf[ITypedStruct],
        s"${DEST_PREFIX}_"))
    }

    instance.set(TypeUtils.GraphResultStruct.verticesAttrName, vertices)
    instance.set(TypeUtils.GraphResultStruct.edgesAttrName, edges)
    GraphResult(res.query, instance)
  }
}

/**
 * Closure for a single instance. Instance is specified by an ''attributeToSelectInstance'' and the value
 * for the attribute.
 *
 * @tparam T
 */
trait SingleInstanceClosureQuery[T] extends ClosureQuery {

  def attributeToSelectInstance : String

  def attributeTyp : PrimitiveType[T]
  def instanceValue : T

  override  def srcCondition(expr : Expression) : Expression = {
    expr.where(
      Expressions.id(attributeToSelectInstance).`=`(Expressions.literal(attributeTyp, instanceValue))
    )
  }
}

/**
 * A ClosureQuery to compute '''Lineage''' for Hive tables. Assumes the Lineage relation is captured in a ''CTAS''
 * type, and the table relations are captured as attributes from a CTAS instance to Table instances.
 *
 * @param tableTypeName The name of the Table Type.
 * @param ctasTypeName The name of the Create Table As Select(CTAS) Type.
 * @param ctasInputTableAttribute The attribute in CTAS Type that associates it to the ''Input'' tables.
 * @param ctasOutputTableAttribute The attribute in CTAS Type that associates it to the ''Output'' tables.
 * @param depth depth as needed by the closure Query.
 * @param selectAttributes as needed by the closure Query.
 * @param withPath as needed by the closure Query.
 * @param persistenceStrategy as needed to evaluate the Closure Query.
 * @param g as needed to evaluate the Closure Query.
 */
case class InputLineageClosureQuery(tableTypeName : String,
                                    attributeToSelectInstance : String,
                                    tableName : String,
                                    ctasTypeName : String,
                                    ctasInputTableAttribute : String,
                                    ctasOutputTableAttribute : String,
                                    depth : Option[Int],
                                    selectAttributes : Option[List[String]],
                                    withPath : Boolean,
                                    persistenceStrategy: GraphPersistenceStrategies,
                                    g: TitanGraph
                        ) extends SingleInstanceClosureQuery[String] {

  val closureType : String = tableTypeName

  val attributeTyp = DataTypes.STRING_TYPE

  val instanceValue = tableName

  lazy val closureRelation = List(
    ReverseRelation(ctasTypeName, ctasOutputTableAttribute),
    Relation(ctasInputTableAttribute)
  )
}

/**
 * A ClosureQuery to compute where a table is used based on the '''Lineage''' for Hive tables.
 * Assumes the Lineage relation is captured in a ''CTAS''
 * type, and the table relations are captured as attributes from a CTAS instance to Table instances.
 *
 * @param tableTypeName The name of the Table Type.
 * @param ctasTypeName The name of the Create Table As Select(CTAS) Type.
 * @param ctasInputTableAttribute The attribute in CTAS Type that associates it to the ''Input'' tables.
 * @param ctasOutputTableAttribute The attribute in CTAS Type that associates it to the ''Output'' tables.
 * @param depth depth as needed by the closure Query.
 * @param selectAttributes as needed by the closure Query.
 * @param withPath as needed by the closure Query.
 * @param persistenceStrategy as needed to evaluate the Closure Query.
 * @param g as needed to evaluate the Closure Query.
 */
case class OutputLineageClosureQuery(tableTypeName : String,
                                     attributeToSelectInstance : String,
                                     tableName : String,
                                     ctasTypeName : String,
                                     ctasInputTableAttribute : String,
                                     ctasOutputTableAttribute : String,
                                     depth : Option[Int],
                                     selectAttributes : Option[List[String]],
                                     withPath : Boolean,
                                     persistenceStrategy: GraphPersistenceStrategies,
                                     g: TitanGraph
                             ) extends SingleInstanceClosureQuery[String] {

  val closureType : String = tableTypeName

  val attributeTyp = DataTypes.STRING_TYPE

  val instanceValue = tableName

  lazy val closureRelation = List(
    ReverseRelation(ctasTypeName, ctasInputTableAttribute),
    Relation(ctasOutputTableAttribute)
  )
}

case class GraphResult(query: String, result : ITypedStruct) {

  def toTypedJson = Serialization.toJson(result)

  def toInstanceJson = InstanceSerialization.toJson(result)
}