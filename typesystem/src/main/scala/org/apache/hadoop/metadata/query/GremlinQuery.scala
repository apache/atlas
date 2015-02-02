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

import org.apache.hadoop.metadata.query.Expressions._
import org.apache.hadoop.metadata.types.DataTypes.TypeCategory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


case class GremlinQuery(expr: Expression, queryStr: String) {

}

trait SelectExpressionHandling {

  /**
   * To aide in gremlinQuery generation add an alias to the input of SelectExpressions
   */
  class AddAliasToSelectInput extends PartialFunction[Expression, Expression] {

    private var idx = 0

    def isDefinedAt(e: Expression) = true

    class DecorateFieldWithAlias(aliasE: AliasExpression)
      extends PartialFunction[Expression, Expression] {
      def isDefinedAt(e: Expression) = true

      def apply(e: Expression) = e match {
        case fe@FieldExpression(fieldName, fInfo, None) =>
          FieldExpression(fieldName, fInfo, Some(BackReference(aliasE.alias, aliasE.child, None)))
        case _ => e
      }
    }

    def apply(e: Expression) = e match {
      case SelectExpression(_: AliasExpression, _) => e
      case SelectExpression(child, selList) => {
        idx = idx + 1
        val aliasE = AliasExpression(child, s"_src$idx")
        SelectExpression(aliasE, selList.map(_.transformUp(new DecorateFieldWithAlias(aliasE))))
      }
      case _ => e
    }
  }

  def getSelectExpressionSrc(e: Expression): List[String] = {
    val l = ArrayBuffer[String]()
    e.traverseUp {
      case BackReference(alias, _, _) => l += alias
    }
    l.toSet.toList
  }

  def validateSelectExprHaveOneSrc: PartialFunction[Expression, Unit] = {
    case SelectExpression(_, selList) => {
      selList.foreach { se =>
        val srcs = getSelectExpressionSrc(se)
        if (srcs.size > 1) {
          throw new GremlinTranslationException(se, "Only one src allowed in a Select Expression")
        }
      }
    }
  }

  def groupSelectExpressionsBySrc(sel: SelectExpression): mutable.LinkedHashMap[String, List[Expression]] = {
    val m = mutable.LinkedHashMap[String, List[Expression]]()
    sel.selectListWithAlias.foreach { se =>
      val l = getSelectExpressionSrc(se.child)
      if (!m.contains(l(0))) {
        m(l(0)) = List()
      }
      m(l(0)) = m(l(0)) :+ se.child
    }
    m
  }

}

class GremlinTranslationException(expr: Expression, reason: String) extends
ExpressionException(expr, s"Unsupported Gremlin translation: $reason")

class GremlinTranslator(expr: Expression,
                        gPersistenceBehavior: GraphPersistenceStrategies)
  extends SelectExpressionHandling {

  val wrapAndRule: PartialFunction[Expression, Expression] = {
    case f: FilterExpression if !f.condExpr.isInstanceOf[LogicalExpression] =>
      FilterExpression(f.child, new LogicalExpression("and", List(f.condExpr)))
  }

  val validateComparisonForm: PartialFunction[Expression, Unit] = {
    case c@ComparisonExpression(_, left, right) =>
      if (!left.isInstanceOf[FieldExpression]) {
        throw new GremlinTranslationException(c, s"lhs of comparison is not a field")
      }
      if (!right.isInstanceOf[Literal[_]]) {
        throw new GremlinTranslationException(c,
          s"rhs of comparison is not a literal")
      }
      ()
  }

  private def genQuery(expr: Expression, inSelect: Boolean): String = expr match {
    case ClassExpression(clsName) => s"""has("${gPersistenceBehavior.typeAttributeName}","$clsName")"""
    case TraitExpression(clsName) => s"""has("${gPersistenceBehavior.typeAttributeName}","$clsName")"""
    case fe@FieldExpression(fieldName, fInfo, child) if fe.dataType.getTypeCategory == TypeCategory.PRIMITIVE => {
      child match {
        case Some(e) => s"${genQuery(e, inSelect)}.$fieldName"
        case None => fieldName
      }
    }
    case fe@FieldExpression(fieldName, fInfo, child)
      if fe.dataType.getTypeCategory == TypeCategory.CLASS || fe.dataType.getTypeCategory == TypeCategory.STRUCT => {
      val direction = if (fInfo.isReverse) "in" else "out"
      val edgeLbl = gPersistenceBehavior.edgeLabel(fInfo)
      val step = s"""$direction("$edgeLbl")"""
      child match {
        case Some(e) => s"${genQuery(e, inSelect)}.$step"
        case None => step
      }
    }
    case c@ComparisonExpression(symb, f@FieldExpression(fieldName, fInfo, ch), l) => {
      ch match {
        case Some(child) =>
          s"""${genQuery(child, inSelect)}.has("$fieldName", ${gPersistenceBehavior.gremlinCompOp(c)}, $l)"""
        case None => s"""has("$fieldName", ${gPersistenceBehavior.gremlinCompOp(c)}, $l)"""
      }
    }
    case fil@FilterExpression(child, condExpr) => {
      s"${genQuery(child, inSelect)}.${genQuery(condExpr, inSelect)}"
    }
    case l@LogicalExpression(symb, children) => {
      s"""$symb${children.map("_()." + genQuery(_, inSelect)).mkString("(", ",", ")")}"""
    }
    case sel@SelectExpression(child, selList) => {
      val m = groupSelectExpressionsBySrc(sel)
      var srcNamesList: List[String] = List()
      var srcExprsList: List[List[String]] = List()
      val it = m.iterator
      while (it.hasNext) {
        val (src, selExprs) = it.next
        srcNamesList = srcNamesList :+ s""""$src""""
        srcExprsList = srcExprsList :+ selExprs.map { selExpr =>
          genQuery(selExpr, true)
        }
      }
      val srcNamesString = srcNamesList.mkString("[", ",", "]")
      val srcExprsStringList = srcExprsList.map {
        _.mkString("[", ",", "]")
      }
      val srcExprsString = srcExprsStringList.foldLeft("")(_ + "{" + _ + "}")
      s"${genQuery(child, inSelect)}.select($srcNamesString)$srcExprsString"
    }
    case BackReference(alias, _, _) =>
      if (inSelect) gPersistenceBehavior.fieldPrefixInSelect else s"""back("$alias")"""
    case AliasExpression(child, alias) => s"""${genQuery(child, inSelect)}.as("$alias")"""
    case isTraitLeafExpression(traitName, Some(clsExp)) =>
      s"""out("${gPersistenceBehavior.traitLabel(clsExp.dataType, traitName)}")"""
    case isTraitUnaryExpression(traitName, child) =>
      s"""out("${gPersistenceBehavior.traitLabel(child.dataType, traitName)}")"""
    case hasFieldLeafExpression(fieldName, Some(clsExp)) =>
      s"""has("$fieldName")"""
    case hasFieldUnaryExpression(fieldName, child) =>
      s"""${genQuery(child, inSelect)}.has("$fieldName")"""
    case ArithmeticExpression(symb, left, right) => s"${genQuery(left, inSelect)} $symb ${genQuery(right, inSelect)}"
    case l: Literal[_] => l.toString
    case x => throw new GremlinTranslationException(x, "expression not yet supported")
  }

  def translate(): GremlinQuery = {
    var e1 = expr.transformUp(wrapAndRule)

    e1.traverseUp(validateComparisonForm)

    e1 = e1.transformUp(new AddAliasToSelectInput)
    e1.traverseUp(validateSelectExprHaveOneSrc)

    e1 match {
      case e1: SelectExpression => GremlinQuery(e1, s"g.V.${genQuery(e1, false)}.toList()")
      case e1 => GremlinQuery(e1, s"g.V.${genQuery(e1, false)}.toList()")
    }

  }

  /*
   * Translation Issues:
   * 1. back references in filters. For e.g. testBackreference: 'DB as db Table where (db.name = "Reporting")'
   *    this is translated to:
   * g.V.has("typeName","DB").as("db").in("Table.db").and(_().back("db").has("name", T.eq, "Reporting")).map().toList()
   * But the '_().back("db") within the and is ignored, the has condition is applied on the current element.
   * The solution is to to do predicate pushdown and apply the filter immediately on top of the referred Expression.
   */

}