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

import java.lang.Boolean
import java.lang.Byte
import java.lang.Double
import java.lang.Float
import java.lang.Integer
import java.lang.Long
import java.lang.Short
import java.util.ArrayList

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


import org.apache.atlas.gremlin.GremlinExpressionFactory
import org.apache.atlas.gremlin.optimizer.GremlinQueryOptimizer
import org.apache.atlas.groovy.CastExpression
import org.apache.atlas.groovy.ClosureExpression
import org.apache.atlas.groovy.LabeledExpression
import org.apache.atlas.groovy.FunctionCallExpression
import org.apache.atlas.groovy.GroovyExpression
import org.apache.atlas.groovy.GroovyGenerationContext
import org.apache.atlas.groovy.IdentifierExpression
import org.apache.atlas.groovy.ListExpression
import org.apache.atlas.groovy.LiteralExpression
import org.apache.atlas.groovy.TraversalStepType
import org.apache.atlas.query.Expressions.AliasExpression
import org.apache.atlas.query.Expressions.ArithmeticExpression
import org.apache.atlas.query.Expressions.BackReference
import org.apache.atlas.query.Expressions.ClassExpression
import org.apache.atlas.query.Expressions.ComparisonExpression
import org.apache.atlas.query.Expressions.Expression
import org.apache.atlas.query.Expressions.ExpressionException
import org.apache.atlas.query.Expressions.FieldExpression
import org.apache.atlas.query.Expressions.FilterExpression
import org.apache.atlas.query.Expressions.InstanceExpression
import org.apache.atlas.query.Expressions.LimitExpression
import org.apache.atlas.query.Expressions.ListLiteral
import org.apache.atlas.query.Expressions.Literal
import org.apache.atlas.query.Expressions.LogicalExpression
import org.apache.atlas.query.Expressions.LoopExpression
import org.apache.atlas.query.Expressions.OrderExpression
import org.apache.atlas.query.Expressions.PathExpression
import org.apache.atlas.query.Expressions.SelectExpression
import org.apache.atlas.query.Expressions.TraitExpression
import org.apache.atlas.query.Expressions.TraitInstanceExpression
import org.apache.atlas.query.Expressions.hasFieldLeafExpression
import org.apache.atlas.query.Expressions.hasFieldUnaryExpression
import org.apache.atlas.query.Expressions.id
import org.apache.atlas.query.Expressions.isTraitLeafExpression
import org.apache.atlas.query.Expressions.isTraitUnaryExpression
import org.apache.atlas.repository.RepositoryException
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection
import org.apache.atlas.typesystem.types.DataTypes
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory
import org.apache.atlas.typesystem.types.IDataType
import org.apache.atlas.typesystem.types.TypeSystem
import org.joda.time.format.ISODateTimeFormat
import org.apache.atlas.query.Expressions.GroupByExpression
import org.apache.atlas.query.Expressions.MaxExpression
import org.apache.atlas.query.Expressions.MinExpression
import org.apache.atlas.query.Expressions.SumExpression
import org.apache.atlas.query.Expressions.CountExpression

import org.apache.atlas.util.AtlasRepositoryConfiguration
import java.util.HashSet

trait IntSequence {
    def next: Int
}

case class GremlinQuery(expr: Expression, queryStr: String, resultMaping: Map[String, (String, Int)]) {

    def hasSelectList = resultMaping != null

    def isPathExpression = expr.isInstanceOf[PathExpression]

    def isGroupBy = expr.isInstanceOf[GroupByExpression]
}


trait SelectExpressionHandling {

  class AliasFinder extends PartialFunction[Expression,Unit] {
        val aliases = new HashSet[String]()

         def isDefinedAt(e: Expression) = true

         def apply(e: Expression) = e match {
            case e@AliasExpression(_, alias) => {
                aliases.add(alias)
            }
            case x => Unit
        }
    }

    class ReplaceAliasWithBackReference(aliases: HashSet[String]) extends PartialFunction[Expression, Expression] {

        def isDefinedAt(e: Expression) = true

        def apply(e: Expression) = e match {
            case e@AliasExpression(child,alias) if aliases.contains(alias) => {
                new BackReference(alias, child, None)
            }
            case x => x
        }
    }

    // Removes back references in comparison expressions that are
    // right after an alias expression.
    //
    //For example:
    // .as('x').and(select('x').has(y),...) is changed to
    // .as('x').and(has(y),...)
    //
    //This allows the "has" to be extracted out of the and/or by
    //the GremlinQueryOptimizer so the index can be used to evaluate
    //the predicate.

     val RemoveUnneededBackReferences : PartialFunction[Expression, Expression] = {

         case filterExpr@FilterExpression(aliasExpr@AliasExpression(_,aliasName), filterChild) => {
             val updatedChild = removeUnneededBackReferences(filterChild, aliasName)
             val changed  = !(updatedChild eq filterChild)
             if(changed) {
                 FilterExpression(aliasExpr, updatedChild)
             }
             else {
                 filterExpr
             }

         }
         case x => x
     }
     def removeUnneededBackReferences(expr: Expression, outerAlias: String) : Expression = expr match {
         case logicalExpr@LogicalExpression(logicalOp,children) => {
            var changed : Boolean = false;
            val updatedChildren : List[Expression] = children.map { child =>
                val updatedChild = removeUnneededBackReferences(child, outerAlias);
                changed |= ! (updatedChild eq child);
                updatedChild
            }
           if(changed) {
               LogicalExpression(logicalOp,updatedChildren)
           }
           else {
              logicalExpr
           }
        }
        case comparisonExpr@ComparisonExpression(_,_,_) => {
            var changed = false
             val updatedLeft = removeUnneededBackReferences(comparisonExpr.left, outerAlias);
             changed |= !( updatedLeft eq comparisonExpr.left);

             val updatedRight = removeUnneededBackReferences(comparisonExpr.right, outerAlias);
             changed |= !(updatedRight eq comparisonExpr.right);

             if (changed) {
                 ComparisonExpression(comparisonExpr.symbol, updatedLeft, updatedRight)
             } else {
                 comparisonExpr
             }
        }
        case FieldExpression(fieldName, fieldInfo, Some(br @ BackReference(brAlias, _, _))) if outerAlias.equals(brAlias) => {
           //Remove the back reference, since the thing it references is right in front
           //of the comparison expression we're in
           FieldExpression(fieldName, fieldInfo, None)
       }
       case x => x
    }

    //in groupby, convert alias expressions defined in the group by child to BackReferences
    //in the groupby list and selectList.
    val AddBackReferencesToGroupBy : PartialFunction[Expression, Expression] = {
        case GroupByExpression(child, groupBy, selExpr) => {

            val aliases = ArrayBuffer[AliasExpression]()
            val finder = new AliasFinder();
            child.traverseUp(finder);

            val replacer = new ReplaceAliasWithBackReference(finder.aliases)

            val newGroupBy = new SelectExpression(
                groupBy.child.transformUp(replacer),
                groupBy.selectList.map {
                    expr => expr.transformUp(replacer)

                },
                groupBy.forGroupBy);

            val newSelExpr = new SelectExpression(
                selExpr.child.transformUp(replacer),
                selExpr.selectList.map {
                    expr => expr.transformUp(replacer)
                },
                selExpr.forGroupBy);

            new GroupByExpression(child, newGroupBy, newSelExpr)
        }
        case x => x
    }
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
            case SelectExpression(aliasE@AliasExpression(_, _), selList, forGroupBy) => {
                idx = idx + 1
                SelectExpression(aliasE, selList.map(_.transformUp(new DecorateFieldWithAlias(aliasE))), forGroupBy)
            }
            case SelectExpression(child, selList, forGroupBy) => {
                idx = idx + 1
                val aliasE = AliasExpression(child, s"_src$idx")
                SelectExpression(aliasE, selList.map(_.transformUp(new DecorateFieldWithAlias(aliasE))), forGroupBy)
            }
            case OrderExpression(aliasE@AliasExpression(_, _), order, asc) => {
                OrderExpression(aliasE, order.transformUp(new DecorateFieldWithAlias(aliasE)),asc)
            }
            case OrderExpression(child, order, asc) => {
                idx = idx + 1
                val aliasE = AliasExpression(child, s"_src$idx")
                OrderExpression(aliasE, order.transformUp(new DecorateFieldWithAlias(aliasE)),asc)
            }
            case _ => e
        }
    }

    def getSelectExpressionSrc(e: Expression): List[String] = {
        val l = ArrayBuffer[String]()
        e.traverseUp {
            case BackReference(alias, _, _) => l += alias
            case ClassExpression(clsName) => l += clsName
        }
        l.toSet.toList
    }

    def validateSelectExprHaveOneSrc: PartialFunction[Expression, Unit] = {
        case SelectExpression(_, selList, forGroupBy) => {
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

    /**
     * For each Output Column in the SelectExpression compute the ArrayList(Src) this maps to and the position within
     * this list.
     *
     * @param sel
     * @return
     */
    def buildResultMapping(sel: SelectExpression): Map[String, (String, Int)] = {
        val srcToExprs = groupSelectExpressionsBySrc(sel)
        val m = new mutable.HashMap[String, (String, Int)]
        sel.selectListWithAlias.foreach { se =>
            val src = getSelectExpressionSrc(se.child)(0)
            val srcExprs = srcToExprs(src)
            var idx = srcExprs.indexOf(se.child)
            m(se.alias) = (src, idx)
        }
        m.toMap
    }
}

class GremlinTranslationException(expr: Expression, reason: String) extends
ExpressionException(expr, s"Unsupported Gremlin translation: $reason")

class GremlinTranslator(expr: Expression,
                        gPersistenceBehavior: GraphPersistenceStrategies)
    extends SelectExpressionHandling {

    val preStatements = ArrayBuffer[GroovyExpression]()
    val postStatements = ArrayBuffer[GroovyExpression]()

    val wrapAndRule: PartialFunction[Expression, Expression] = {
        case f: FilterExpression if ((!f.condExpr.isInstanceOf[LogicalExpression]) &&
                                      (f.condExpr.isInstanceOf[isTraitLeafExpression] || !f.namedExpressions.isEmpty)) =>
          FilterExpression(f.child, new LogicalExpression("and", List(f.condExpr)))
    }

    val validateComparisonForm: PartialFunction[Expression, Unit] = {
        case c@ComparisonExpression(op, left, right) =>
            if (!left.isInstanceOf[FieldExpression]) {
                throw new GremlinTranslationException(c, s"lhs of comparison is not a field")
            }
            if (!right.isInstanceOf[Literal[_]] && !right.isInstanceOf[ListLiteral[_]]) {
                throw new GremlinTranslationException(c,
                    s"rhs of comparison is not a literal")
            }

            if(right.isInstanceOf[ListLiteral[_]] && (!op.equals("=") && !op.equals("!="))) {
                throw new GremlinTranslationException(c,
                    s"operation not supported with list literal")
            }
            ()
    }

    val counter =  new IntSequence {
        var i: Int = -1;

        def next: Int = {
            i += 1; i
        }
    }

    def addAliasToLoopInput(c: IntSequence = counter): PartialFunction[Expression, Expression] = {
        case l@LoopExpression(aliasE@AliasExpression(_, _), _, _) => l
        case l@LoopExpression(inputExpr, loopExpr, t) => {
            val aliasE = AliasExpression(inputExpr, s"_loop${c.next}")
            LoopExpression(aliasE, loopExpr, t)
        }
    }

    def instanceClauseToTop(topE : Expression) : PartialFunction[Expression, Expression] = {
      case le : LogicalExpression if (le fastEquals topE) =>  {
        le.instance()
      }
      case ce : ComparisonExpression if (ce fastEquals topE) =>  {
        ce.instance()
      }
      case he : hasFieldUnaryExpression if (he fastEquals topE) =>  {
        he.instance()
      }
    }

    def traitClauseWithInstanceForTop(topE : Expression) : PartialFunction[Expression, Expression] = {
//          This topE check prevented the comparison of trait expression when it is a child. Like trait as t limit 2
        case te : TraitExpression =>  {
        val theTrait = te.as("theTrait")
        val theInstance = theTrait.traitInstance().as("theInstance")
        val outE =
          theInstance.select(id("theTrait").as("traitDetails"),
            id("theInstance").as("instanceInfo"))
        QueryProcessor.validate(outE)
      }
    }

    def typeTestExpression(parent: GroovyExpression, typeName : String) : GroovyExpression = {
        val stats = GremlinExpressionFactory.INSTANCE.generateTypeTestExpression(gPersistenceBehavior, parent, typeName, counter)

        preStatements ++= stats.init
        stats.last

    }

    val QUOTE = "\"";

    private def cleanStringLiteral(l : Literal[_]) : String = {
        return l.toString.stripPrefix(QUOTE).stripSuffix(QUOTE);
    }


    private def genQuery(parent: GroovyExpression, expr: Expression, inClosure: Boolean): GroovyExpression = expr match {
        case ClassExpression(clsName) => typeTestExpression(parent, clsName)
        case TraitExpression(clsName) => typeTestExpression(parent, clsName)
        case fe@FieldExpression(fieldName, fInfo, child)
            if fe.dataType.getTypeCategory == TypeCategory.PRIMITIVE || fe.dataType.getTypeCategory == TypeCategory.ARRAY => {
                val fN = gPersistenceBehavior.fieldNameInVertex(fInfo.dataType, fInfo.attrInfo)
                val childExpr = translateOptChild(parent, child, inClosure);
                return GremlinExpressionFactory.INSTANCE.generateFieldExpression(childExpr, fInfo, fN, inClosure);
            }
        case fe@FieldExpression(fieldName, fInfo, child)
            if fe.dataType.getTypeCategory == TypeCategory.CLASS || fe.dataType.getTypeCategory == TypeCategory.STRUCT => {
            val childExpr = translateOptChild(parent, child, inClosure);
            val direction = if (fInfo.isReverse) AtlasEdgeDirection.IN else AtlasEdgeDirection.OUT
            val edgeLbl = gPersistenceBehavior.edgeLabel(fInfo)
            return GremlinExpressionFactory.INSTANCE.generateAdjacentVerticesExpression(childExpr, direction, edgeLbl)

            }
        case fe@FieldExpression(fieldName, fInfo, child) if fInfo.traitName != null => {
            val childExpr = translateOptChild(parent, child, inClosure);
            val direction = gPersistenceBehavior.instanceToTraitEdgeDirection
            val edgeLbl = gPersistenceBehavior.edgeLabel(fInfo)
            return GremlinExpressionFactory.INSTANCE.generateAdjacentVerticesExpression(childExpr, direction, edgeLbl)

        }
        case c@ComparisonExpression(symb, f@FieldExpression(fieldName, fInfo, ch), l) => {
            val qualifiedPropertyName = s"${gPersistenceBehavior.fieldNameInVertex(fInfo.dataType, fInfo.attrInfo)}"

            val childExpr = translateOptChild(parent, ch, inClosure)
            val persistentExprValue : GroovyExpression = if(l.isInstanceOf[Literal[_]]) {
                translateLiteralValue(fInfo.attrInfo.dataType, l.asInstanceOf[Literal[_]]);
            }
            else {
                genQuery(null, l, inClosure);
            }

            if (symb == "like") {
              return GremlinExpressionFactory.INSTANCE.generateLikeExpressionUsingFilter(childExpr, qualifiedPropertyName, persistentExprValue);
            }

           return GremlinExpressionFactory.INSTANCE.generateHasExpression(gPersistenceBehavior, childExpr, qualifiedPropertyName, c.symbol, persistentExprValue, fInfo);
        }
        case fil@FilterExpression(child, condExpr) => {
            var newParent = genQuery(parent, child, inClosure);
            val alias = "a" + counter.next;
            newParent = GremlinExpressionFactory.INSTANCE.generateAliasExpression(newParent, alias);
            val translated =  genQuery(newParent, condExpr, inClosure);
            //we want the query to return instances of the class whose instances we are filtering out
            //The act of filtering may traverse edges and have other side effects.
            GremlinExpressionFactory.INSTANCE.generateBackReferenceExpression(translated, false, alias);
        }
        case l@LogicalExpression(symb, children) => {
            val translatedChildren : java.util.List[GroovyExpression] = translateList(children, false);
            return GremlinExpressionFactory.INSTANCE.generateLogicalExpression(parent, symb, translatedChildren);
        }
        case sel@SelectExpression(child, selList, forGroupBy) => {
              val m = groupSelectExpressionsBySrc(sel)
              var srcNamesList: java.util.List[LiteralExpression] = new ArrayList()
              var srcExprsList: List[java.util.List[GroovyExpression]] = List()
              val it = m.iterator

              while (it.hasNext) {
                  val (src, selExprs) = it.next
                  srcNamesList.add(new LiteralExpression(src));
                  val translatedSelExprs : java.util.List[GroovyExpression] = translateList(selExprs, true);
                  srcExprsList = srcExprsList :+ translatedSelExprs
               }
               val srcExprsStringList : java.util.List[GroovyExpression] = new ArrayList();
               srcExprsList.foreach { it =>
                    srcExprsStringList.add(new ListExpression(it));
               }

               val childExpr = genQuery(parent, child, inClosure)
               return GremlinExpressionFactory.INSTANCE.generateSelectExpression(childExpr, srcNamesList, srcExprsStringList);

        }
        case loop@LoopExpression(input, loopExpr, t) => {

            val times : Integer = if(t.isDefined) {
                t.get.rawValue.asInstanceOf[Integer]
            }
            else {
                null.asInstanceOf[Integer]
            }
            val alias = input.asInstanceOf[AliasExpression].alias;
            val inputQry = genQuery(parent, input, inClosure)
            val translatedLoopExpr = genQuery(GremlinExpressionFactory.INSTANCE.getLoopExpressionParent(inputQry), loopExpr, inClosure);
            return GremlinExpressionFactory.INSTANCE.generateLoopExpression(inputQry, gPersistenceBehavior, input.dataType, translatedLoopExpr, alias, times);
        }
        case BackReference(alias, _, _) => {

            return GremlinExpressionFactory.INSTANCE.generateBackReferenceExpression(parent, inClosure, alias);
        }
        case AliasExpression(child, alias) => {
            var childExpr = genQuery(parent, child, inClosure);
            return GremlinExpressionFactory.INSTANCE.generateAliasExpression(childExpr, alias);
        }
        case isTraitLeafExpression(traitName, Some(clsExp)) => {
            val label = gPersistenceBehavior.traitLabel(clsExp.dataType, traitName);
            return GremlinExpressionFactory.INSTANCE.generateAdjacentVerticesExpression(parent, AtlasEdgeDirection.OUT, label);
        }
        case isTraitUnaryExpression(traitName, child) => {
            val label = gPersistenceBehavior.traitLabel(child.dataType, traitName);
            return GremlinExpressionFactory.INSTANCE.generateAdjacentVerticesExpression(parent, AtlasEdgeDirection.OUT, label);
        }
        case hasFieldLeafExpression(fieldName, clsExp) => clsExp match {
            case None => GremlinExpressionFactory.INSTANCE.generateUnaryHasExpression(parent, fieldName)
            case Some(x) => {
                val fi = TypeUtils.resolveReference(clsExp.get.dataType, fieldName);
                if(! fi.isDefined) {
                    return GremlinExpressionFactory.INSTANCE.generateUnaryHasExpression(parent, fieldName);
                }
                else {
                    val fName = gPersistenceBehavior.fieldNameInVertex(fi.get.dataType, fi.get.attrInfo)
                    return GremlinExpressionFactory.INSTANCE.generateUnaryHasExpression(parent, fName);
                }
            }
        }
        case hasFieldUnaryExpression(fieldName, child) =>
            val childExpr = genQuery(parent, child, inClosure);
            return GremlinExpressionFactory.INSTANCE.generateUnaryHasExpression(childExpr, fieldName);
        case ArithmeticExpression(symb, left, right) => {
            val leftExpr = genQuery(parent, left, inClosure);
            val rightExpr = genQuery(parent, right, inClosure);
            return GremlinExpressionFactory.INSTANCE.generateArithmeticExpression(leftExpr, symb, rightExpr);
        }
        case l: Literal[_] =>  {

            if(parent != null) {
                return new org.apache.atlas.groovy.FieldExpression(parent, cleanStringLiteral(l));
            }
            return translateLiteralValue(l.dataType, l);
        }
        case list: ListLiteral[_] =>  {
            //Here, we are creating a Groovy list literal expression ([value1, value2, value3]).  Because
            //of this, any gremlin query expressions within the list must start with an anonymous traversal.
            //We set 'inClosure' to true in this case to make that happen.
            val  values : java.util.List[GroovyExpression] = translateList(list.rawValue, true);
            return new ListExpression(values);
        }
        case in@TraitInstanceExpression(child) => {
          val childExpr = genQuery(parent, child, inClosure);
          val direction = gPersistenceBehavior.traitToInstanceEdgeDirection;
          return GremlinExpressionFactory.INSTANCE.generateAdjacentVerticesExpression(childExpr, direction);
        }
        case in@InstanceExpression(child) => {
            return genQuery(parent, child, inClosure);
        }
        case pe@PathExpression(child) => {
            val childExpr = genQuery(parent, child, inClosure)
            return GremlinExpressionFactory.INSTANCE.generatePathExpression(childExpr);
        }
        case order@OrderExpression(child, odr, asc) => {
          var orderExpression = odr
          if(odr.isInstanceOf[BackReference]) {
              orderExpression = odr.asInstanceOf[BackReference].reference
          }
          else if (odr.isInstanceOf[AliasExpression]) {
              orderExpression = odr.asInstanceOf[AliasExpression].child
          }

          val childExpr = genQuery(parent, child, inClosure);
          var orderByParents : java.util.List[GroovyExpression] = GremlinExpressionFactory.INSTANCE.getOrderFieldParents();

          val translatedParents : java.util.List[GroovyExpression] = new ArrayList[GroovyExpression]();
          var translatedOrderParents = orderByParents.foreach { it =>
                 translatedParents.add(genQuery(it, orderExpression, true));
          }
          return GremlinExpressionFactory.INSTANCE.generateOrderByExpression(childExpr, translatedParents,asc);

        }
        case limitOffset@LimitExpression(child, limit, offset) => {
            val childExpr = genQuery(parent, child, inClosure);
            val totalResultRows = limit.value + offset.value;
            return GremlinExpressionFactory.INSTANCE.generateRangeExpression(childExpr, offset.value, totalResultRows);
        }
        case count@CountExpression() => {
          val listExpr = GremlinExpressionFactory.INSTANCE.getClosureArgumentValue();
          GremlinExpressionFactory.INSTANCE.generateCountExpression(listExpr);
        }
        case max@MaxExpression(child) => {
          //use "it" as the parent since the child will become
          //part of a closure.  Its value will be whatever vertex
          //we are looking at in the collection.
          val childExprParent = null;
          val childExpr = genQuery(childExprParent, child, true);
          val listExpr = GremlinExpressionFactory.INSTANCE.getClosureArgumentValue();
          GremlinExpressionFactory.INSTANCE.generateMaxExpression(listExpr, childExpr);
        }
        case min@MinExpression(child) => {
          //use "it" as the parent since the child will become
          //part of a closure.  Its value will be whatever vertex
          //we are looking at in the collection.
          val childExprParent = null;
          val childExpr = genQuery(childExprParent, child, true);
          val listExpr = GremlinExpressionFactory.INSTANCE.getClosureArgumentValue();
          GremlinExpressionFactory.INSTANCE.generateMinExpression(listExpr, childExpr);
        }
        case sum@SumExpression(child) => {
           //use "it" as the parent since the child will become
          //part of a closure.  Its value will be whatever vertex
          //we are looking at in the collection.
          val childExprParent = null;
          val childExpr = genQuery(childExprParent, child, true);
          val listExpr = GremlinExpressionFactory.INSTANCE.getClosureArgumentValue();
          GremlinExpressionFactory.INSTANCE.generateSumExpression(listExpr, childExpr);
        }
        case groupByExpr@GroupByExpression(child, groupBy, selExpr) => {
          //remove aliases
          val groupByExprListToTranslate = (groupBy.asInstanceOf[SelectExpression]).selectListWithAlias.map {
            x => x.child;
          }
          val grpByExprsList = translateList(groupByExprListToTranslate, true);
          val groupByValue = new ListExpression(grpByExprsList);

          //reduction only aggregate methods are supported here as of now.(Max, Min, Count)
          //remove aliases
          val srcExprListToTranslate = selExpr.selectListWithAlias.map {
            x => x.child;
          }
          val srcExprsList = translateList(srcExprListToTranslate, true, true);
          val srcExprsStringList = new ListExpression(srcExprsList)

          val childExpr = genQuery(parent, child, inClosure);
          return GremlinExpressionFactory.INSTANCE.generateGroupByExpression(childExpr, groupByValue, srcExprsStringList);
         }
        case x => throw new GremlinTranslationException(x, "expression not yet supported")
    }

     def translateList(exprs : List[Expressions.Expression], inClosure : Boolean, inGroupBy: Boolean = false) : java.util.List[GroovyExpression] = {
        var parent = if (inGroupBy) {
          GremlinExpressionFactory.INSTANCE.getGroupBySelectFieldParent();
        }

        else if(inClosure) {
          null;
        }
        else {
          GremlinExpressionFactory.INSTANCE.getAnonymousTraversalExpression()
        }
        var result : java.util.List[GroovyExpression] = new java.util.ArrayList(exprs.size);
        exprs.foreach { it =>
            result.add(genQuery(parent, it, inClosure));
        }
        return result;
    }

    def translateOptChild(parent : GroovyExpression, child : Option[Expressions.Expression] , inClosure: Boolean) : GroovyExpression = child match {

        case Some(x) => genQuery(parent, x, inClosure)
        case None => parent
    }

    def translateLiteralValue(dataType: IDataType[_], l: Literal[_]): GroovyExpression = {


      if (dataType == DataTypes.DATE_TYPE) {
           try {
               //Accepts both date, datetime formats
               val dateStr = cleanStringLiteral(l)
               val dateVal = ISODateTimeFormat.dateOptionalTimeParser().parseDateTime(dateStr).getMillis
               return new LiteralExpression(dateVal)
            } catch {
                  case pe: java.text.ParseException =>
                       throw new GremlinTranslationException(l,
                                   "Date format " + l + " not supported. Should be of the format " +
                                   TypeSystem.getInstance().getDateFormat.toPattern);
            }
       }
       else if(dataType == DataTypes.BYTE_TYPE) {
           //cast needed, otherwise get class cast exception when trying to compare, since the
           //persist value is assumed to be an Integer
           return new CastExpression(new LiteralExpression(Byte.valueOf(s"""${l}"""), true),"byte");
       }
       else if(dataType == DataTypes.INT_TYPE) {
           return new LiteralExpression(Integer.valueOf(s"""${l}"""));
       }
       else if(dataType == DataTypes.BOOLEAN_TYPE) {
           return new LiteralExpression(Boolean.valueOf(s"""${l}"""));
       }
       else if(dataType == DataTypes.SHORT_TYPE) {
           return new CastExpression(new LiteralExpression(Short.valueOf(s"""${l}"""), true),"short");
       }
       else if(dataType == DataTypes.LONG_TYPE) {
           return new LiteralExpression(Long.valueOf(s"""${l}"""), true);
       }
       else if(dataType == DataTypes.FLOAT_TYPE) {
           return new LiteralExpression(Float.valueOf(s"""${l}"""), true);
       }
       else if(dataType == DataTypes.DOUBLE_TYPE) {
           return new LiteralExpression(Double.valueOf(s"""${l}"""), true);
       }
       else if(dataType == DataTypes.STRING_TYPE) {
           return new LiteralExpression(cleanStringLiteral(l));
       }
       else {
           return new LiteralExpression(l.rawValue);
       }
    }

    def genFullQuery(expr: Expression, hasSelect: Boolean): String = {

        var q : GroovyExpression = new FunctionCallExpression(TraversalStepType.START, new IdentifierExpression(TraversalStepType.SOURCE, "g"),"V");

        val debug:Boolean = false
        if(gPersistenceBehavior.addGraphVertexPrefix(preStatements)) {
            q = gPersistenceBehavior.addInitialQueryCondition(q);
        }

        q = genQuery(q, expr, false)

        q = GremlinExpressionFactory.INSTANCE.generateToListExpression(q);
        q = gPersistenceBehavior.getGraph().addOutputTransformationPredicate(q, hasSelect, expr.isInstanceOf[PathExpression]);


        if(AtlasRepositoryConfiguration.isGremlinOptimizerEnabled()) {
            q = GremlinQueryOptimizer.getInstance().optimize(q);
        }

	    val closureExpression = new ClosureExpression();

        closureExpression.addStatements(preStatements);
        closureExpression.addStatement(q)
        closureExpression.addStatements(postStatements);

	    val overallExpression = new LabeledExpression("L", closureExpression);

        val qryStr = generateGremlin(overallExpression);

        if(debug) {
          println(" query " + qryStr)
        }

        qryStr;

    }

    def generateGremlin(expr: GroovyExpression) : String = {
         val ctx : GroovyGenerationContext = new GroovyGenerationContext();
         ctx.setParametersAllowed(false);
         expr.generateGroovy(ctx);
         return ctx.getQuery;
    }

    def translate(): GremlinQuery = {
        var e1 = expr.transformUp(wrapAndRule)

        e1.traverseUp(validateComparisonForm)
        e1 = e1.transformUp(AddBackReferencesToGroupBy)
        e1 = e1.transformUp(new AddAliasToSelectInput)
        e1.traverseUp(validateSelectExprHaveOneSrc)
        e1 = e1.transformUp(addAliasToLoopInput())
        e1 = e1.transformUp(instanceClauseToTop(e1))
        e1 = e1.transformUp(traitClauseWithInstanceForTop(e1))
        e1 = e1.transformUp(RemoveUnneededBackReferences)

        //Following code extracts the select expressions from expression tree.

        val se = SelectExpressionHelper.extractSelectExpression(e1)
        if (se.isDefined) {
          val rMap = buildResultMapping(se.get)
          GremlinQuery(e1, genFullQuery(e1, true), rMap)
        } else {
            GremlinQuery(e1, genFullQuery(e1, false), null)
        }

    }
}
    object SelectExpressionHelper {
       /**
     * This method extracts the child select expression from parent expression
     */
     def extractSelectExpression(child: Expression): Option[SelectExpression] = {
      child match {
            case se@SelectExpression(child, selectList, false) =>{
              Some(se)
            }
            case limit@LimitExpression(child, lmt, offset) => {
              extractSelectExpression(child)
            }
            case order@OrderExpression(child, odr, odrBy) => {
              extractSelectExpression(child)
            }
           case path@PathExpression(child) => {
              extractSelectExpression(child)
           }
           case _ => {
             None
           }

      }
    }
    }
    /*
     * TODO
     * Translation Issues:
     * 1. back references in filters. For e.g. testBackreference: 'DB as db Table where (db.name = "Reporting")'
     *    this is translated to:
     * g.V.has("typeName","DB").as("db").in("Table.db").and(_().back("db").has("name", T.eq, "Reporting")).map().toList()
     * But the '_().back("db") within the and is ignored, the has condition is applied on the current element.
     * The solution is to to do predicate pushdown and apply the filter immediately on top of the referred Expression.
     */

