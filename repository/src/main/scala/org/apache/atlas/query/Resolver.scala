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

import org.apache.atlas.query.Expressions._
import org.apache.atlas.typesystem.types.IDataType

class Resolver(srcExpr: Option[Expression] = None, aliases: Map[String, Expression] = Map(),
               connectClassExprToSrc: Boolean = false)
    extends PartialFunction[Expression, Expression] {

    import org.apache.atlas.query.TypeUtils._

    def isDefinedAt(x: Expression) = true

    def apply(e: Expression): Expression = e match {
        case idE@IdExpression(name) => {
            val backExpr = aliases.get(name)
            if (backExpr.isDefined) {
                return new BackReference(name, backExpr.get, None)
            }
            if (srcExpr.isDefined) {
                val fInfo = resolveReference(srcExpr.get.dataType, name)
                if (fInfo.isDefined) {
                    return new FieldExpression(name, fInfo.get, None)
                }
            }
            val cType = resolveAsClassType(name)
            if (cType.isDefined) {
                return new ClassExpression(name)
            }
            val tType = resolveAsTraitType(name)
            if (tType.isDefined) {
                return new TraitExpression(name)
            }
            idE
        }
        case ce@ClassExpression(clsName) if connectClassExprToSrc && srcExpr.isDefined => {
            val fInfo = resolveReference(srcExpr.get.dataType, clsName)
            if (fInfo.isDefined) {
                return new FieldExpression(clsName, fInfo.get, None)
            }
            ce
        }
        case f@UnresolvedFieldExpression(child, fieldName) if child.resolved => {
            var fInfo: Option[FieldInfo] = None

            fInfo = resolveReference(child.dataType, fieldName)
            if (fInfo.isDefined) {
                return new FieldExpression(fieldName, fInfo.get, Some(child))
            }
            val tType = resolveAsTraitType(fieldName)
            if (tType.isDefined) {
              return new FieldExpression(fieldName, FieldInfo(child.dataType, null, null, fieldName), Some(child))
            }
            f
        }
        case isTraitLeafExpression(traitName, classExpression)
            if srcExpr.isDefined && !classExpression.isDefined =>
            isTraitLeafExpression(traitName, srcExpr)
        case hasFieldLeafExpression(traitName, classExpression)
            if srcExpr.isDefined && !classExpression.isDefined =>
            hasFieldLeafExpression(traitName, srcExpr)
        case f@FilterExpression(inputExpr, condExpr) if inputExpr.resolved => {
            val r = new Resolver(Some(inputExpr), inputExpr.namedExpressions)
            return new FilterExpression(inputExpr, condExpr.transformUp(r))
        }
        case SelectExpression(child, selectList) if child.resolved => {
            val r = new Resolver(Some(child), child.namedExpressions)
            return new SelectExpression(child, selectList.map {
                _.transformUp(r)
            })
        }
        case l@LoopExpression(inputExpr, loopExpr, t) if inputExpr.resolved => {
            val r = new Resolver(Some(inputExpr), inputExpr.namedExpressions, true)
            return new LoopExpression(inputExpr, loopExpr.transformUp(r), t)
            }
        case lmt@LimitExpression(child, limit, offset) => {
            val r = new Resolver(Some(child), child.namedExpressions)
            return new LimitExpression(child.transformUp(r), limit, offset)
        }
        case order@OrderExpression(child, odr, asc) => {
            val r = new Resolver(Some(child), child.namedExpressions)
            return new OrderExpression(child.transformUp(r), odr, asc)
        }
        case x => x
    }
}

/**
 * - any FieldReferences that explicitly reference the input, can be converted to implicit references
 * - any FieldReferences that explicitly reference a
 */
object FieldValidator extends PartialFunction[Expression, Expression] {

    def isDefinedAt(x: Expression) = true

    def isSrc(e: Expression) = e.isInstanceOf[ClassExpression] || e.isInstanceOf[TraitExpression]

    def validateQualifiedField(srcDataType: IDataType[_]): PartialFunction[Expression, Expression] = {
        case FieldExpression(fNm, fInfo, Some(child))
            if (child.children == Nil && !child.isInstanceOf[BackReference] && child.dataType == srcDataType) =>
            FieldExpression(fNm, fInfo, None)
        case fe@FieldExpression(fNm, fInfo, Some(child)) if isSrc(child) =>
            throw new ExpressionException(fe, s"srcType of field doesn't match input type")
        case hasFieldUnaryExpression(fNm, child) if child.dataType == srcDataType =>
            hasFieldLeafExpression(fNm, Some(child))
        case hF@hasFieldUnaryExpression(fNm, child) if isSrc(child) =>
            throw new ExpressionException(hF, s"srcType of field doesn't match input type")
        case isTraitUnaryExpression(fNm, child) if child.dataType == srcDataType =>
            isTraitLeafExpression(fNm)
        case iT@isTraitUnaryExpression(fNm, child) if isSrc(child) =>
            throw new ExpressionException(iT, s"srcType of field doesn't match input type")
    }

    def validateOnlyFieldReferencesInLoopExpressions(loopExpr: LoopExpression)
    : PartialFunction[Expression, Unit] = {
        case f: FieldExpression => ()
        case x => throw new ExpressionException(loopExpr,
            s"Loop Expression can only contain field references; '${x.toString}' not supported.")
    }

    def apply(e: Expression): Expression = e match {
        case f@FilterExpression(inputExpr, condExpr) => {
            val validatedCE = condExpr.transformUp(validateQualifiedField(inputExpr.dataType))
            if (validatedCE.fastEquals(condExpr)) {
                f
            } else {
                new FilterExpression(inputExpr, validatedCE)
            }
        }
        case SelectExpression(child, selectList) if child.resolved => {
            val v = validateQualifiedField(child.dataType)
            return new SelectExpression(child, selectList.map {
                _.transformUp(v)
            })
        }
        case l@LoopExpression(inputExpr, loopExpr, t) => {
            val validatedLE = loopExpr.transformUp(validateQualifiedField(inputExpr.dataType))
            val l1 = {
                if (validatedLE.fastEquals(loopExpr)) l
                else new LoopExpression(inputExpr, validatedLE, t)
            }
            l1.loopingExpression.traverseUp(validateOnlyFieldReferencesInLoopExpressions(l1))
            l1
        }
        case x => x
    }
}