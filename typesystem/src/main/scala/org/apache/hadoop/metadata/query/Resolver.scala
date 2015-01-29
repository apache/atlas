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

class Resolver(srcExpr : Option[Expression] = None, aliases : Map[String, Expression] = Map())
extends PartialFunction[Expression, Expression] {

  import TypeUtils._

  def isDefinedAt(x: Expression) = true

  def apply(e : Expression) : Expression = e match {
    case idE@IdExpression(name) => {
      val cType = resolveAsClassType(name)
      if (cType.isDefined) {
        return new ClassExpression(name)
      }
      val tType = resolveAsTraitType(name)
      if (tType.isDefined) {
        return new TraitExpression(name)
      }
      if (srcExpr.isDefined ) {
        val fInfo = resolveReference(srcExpr.get.dataType, name)
        if ( fInfo.isDefined) {
          return new FieldExpression(name, fInfo.get, None)
        }
      }
      val backExpr = aliases.get(name)
      if ( backExpr.isDefined) {
        return new BackReference(name, backExpr.get, None)
      }
      idE
    }
    case f@UnresolvedFieldExpression(child, fieldName) if child.resolved => {
      var fInfo : Option[FieldInfo] = None

      fInfo = resolveReference(child.dataType, fieldName)
      if ( fInfo.isDefined) {
        return new FieldExpression(fieldName, fInfo.get, Some(child))
      }
      f
    }
    case isTraitLeafExpression(traitName, classExpression)
      if srcExpr.isDefined  && !classExpression.isDefined =>
      isTraitLeafExpression(traitName, srcExpr)
    case hasFieldLeafExpression(traitName, classExpression)
      if srcExpr.isDefined  && !classExpression.isDefined =>
      hasFieldLeafExpression(traitName, srcExpr)
    case f@FilterExpression(inputExpr, condExpr) if inputExpr.resolved => {
      val r = new Resolver(Some(inputExpr), inputExpr.namedExpressions)
      return new FilterExpression(inputExpr, condExpr.transformUp(r))
    }
    case SelectExpression(child, selectList) if child.resolved => {
      val r = new Resolver(Some(child), child.namedExpressions)
      return new SelectExpression(child, selectList.map{_.transformUp(r)})
    }
    case x => x
  }
}
