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

import Expressions._
import org.apache.hadoop.metadata.types.{AttributeInfo, IDataType}


case class GremlinQuery(queryStr : String) {

}

trait GraphPersistenceStrategies {
  /**
   * Name of attribute used to store typeName in vertex
   */
  def typeAttributeName : String

  /**
   * Given a dataType and a reference attribute, how is edge labeled
   */
  def edgeLabel(iDataType: IDataType[_], aInfo : AttributeInfo) : String
}

object GraphPersistenceStrategy1 extends GraphPersistenceStrategies {
  val typeAttributeName = "typeName"
  def edgeLabel(dataType: IDataType[_], aInfo : AttributeInfo) = s"${dataType}.${aInfo.name}"
}

class GremlinTranslator(expr : Expression,
                        gPersistenceBehavior : GraphPersistenceStrategies = GraphPersistenceStrategy1) {

  val wrapAndRule : PartialFunction[Expression, Expression] = {
    case f : FilterExpression if !f.condExpr.isInstanceOf[LogicalExpression] =>
      FilterExpression(f.child, new LogicalExpression("and", List(f.condExpr)))
  }

  val validateComparisonForm : PartialFunction[Expression, Unit]= {
    case c@ComparisonExpression(_, left, right) =>
      if (!left.isInstanceOf[FieldExpression]) {
        throw new ExpressionException(c, s"Gremlin transaltion not supported: lhs of comparison is not a field")
      }
      if (!right.isInstanceOf[Literal[_]]) {
        throw new ExpressionException(c,
          s"Gremlin transaltion for $c not supported: rhs of comparison is not a literal")
      }
      ()
  }

  private def genQuery(expr : Expression) : String = expr match {
    case ClassExpression(clsName) => s"""has("${gPersistenceBehavior.typeAttributeName}","$clsName")"""
    case TraitExpression(clsName) => s"""has("${gPersistenceBehavior.typeAttributeName}","$clsName")"""
    case x => "" //throw new ExpressionException(x, "Gremlin translation not supported")
  }

  def translate() : GremlinQuery = {
    val e1 = expr.transformUp(wrapAndRule)

    e1.traverseUp(validateComparisonForm)
    GremlinQuery("g.V." + genQuery(e1) + ".toList()")
  }

}