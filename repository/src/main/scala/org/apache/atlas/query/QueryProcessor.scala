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

import org.apache.atlas.repository.graphdb.AtlasGraph
import org.apache.atlas.query.Expressions._
import org.slf4j.{Logger, LoggerFactory}

object QueryProcessor {
    val LOG : Logger = LoggerFactory.getLogger("org.apache.atlas.query.QueryProcessor")

    def evaluate(e: Expression, g: AtlasGraph[_,_], gP : GraphPersistenceStrategies = null):
    GremlinQueryResult = {

        var strategy = gP;
        if(strategy == null) {
            strategy = GraphPersistenceStrategy1(g);
        }

        val e1 = validate(e)
        val q = new GremlinTranslator(e1, strategy).translate()
        LOG.debug("Query: " + e1)
        LOG.debug("Expression Tree:\n" + e1.treeString)
        LOG.debug("Gremlin Query: " + q.queryStr)
        new GremlinEvaluator(q, strategy, g).evaluate()
    }

    def validate(e: Expression): Expression = {

        val e1 = e.transformUp(refineIdExpressionType);
        val e2 = e1.transformUp(new Resolver(None,e1.namedExpressions))

        e2.traverseUp {
            case x: Expression if !x.resolved =>
                throw new ExpressionException(x, s"Failed to resolved expression $x")
        }

        /*
         * trigger computation of dataType of expression tree
         */
        e2.dataType

        /*
         * ensure fieldReferences match the input expression's dataType
         */
        val e3 = e2.transformUp(FieldValidator)
        val e4 = e3.transformUp(new Resolver(None,e3.namedExpressions))

        e4.dataType

        e4
    }

    val convertToFieldIdExpression : PartialFunction[Expression,Expression] = {
        case IdExpression(name, IdExpressionType.Unresolved) => IdExpression(name, IdExpressionType.NonType);
    }


    //this function is called in a depth first manner on the expression tree to set the exprType in IdExpressions
    //when we know them.  Since Expression classes are immutable, in order to do this we need to create new instances
    //of the case.  The logic here enumerates the cases that have been identified where the given IdExpression
    //cannot resolve to a class or trait.  This is the case in any places where a field value must be used.
    //For example, you cannot add two classes together or compare traits.  Any IdExpressions in those contexts
    //refer to unqualified attribute names.  On a similar note, select clauses need to product an actual value.
    //For example, in 'from DB select name' or 'from DB select name as n', name must be an attribute.
     val refineIdExpressionType : PartialFunction[Expression,Expression] = {

         //spit out the individual cases to minimize the object churn.  Specifically, for ComparsionExpressions where neither
         //child is an IdExpression, there is no need to create a new ComparsionExpression object since neither child will
         //change.  This applies to ArithmeticExpression as well.
         case c@ComparisonExpression(symbol, l@IdExpression(_,IdExpressionType.Unresolved) , r@IdExpression(_,IdExpressionType.Unresolved)) => {
             ComparisonExpression(symbol, convertToFieldIdExpression(l), convertToFieldIdExpression(r))
         }
         case c@ComparisonExpression(symbol, l@IdExpression(_,IdExpressionType.Unresolved) , r) => ComparisonExpression(symbol, convertToFieldIdExpression(l), r)
         case c@ComparisonExpression(symbol, l, r@IdExpression(_,IdExpressionType.Unresolved)) => ComparisonExpression(symbol, l, convertToFieldIdExpression(r))

         case e@ArithmeticExpression(symbol, l@IdExpression(_,IdExpressionType.Unresolved) , r@IdExpression(_,IdExpressionType.Unresolved)) => {
             ArithmeticExpression(symbol, convertToFieldIdExpression(l), convertToFieldIdExpression(r))
         }
         case e@ArithmeticExpression(symbol, l@IdExpression(_,IdExpressionType.Unresolved) , r) => ArithmeticExpression(symbol, convertToFieldIdExpression(l), r)
         case e@ArithmeticExpression(symbol, l, r@IdExpression(_,IdExpressionType.Unresolved)) => ArithmeticExpression(symbol, l, convertToFieldIdExpression(r))

         case s@SelectExpression(child, selectList, forGroupBy) => {
             var changed = false
             val newSelectList = selectList.map {

                 expr => expr match {
                     case e@IdExpression(_,IdExpressionType.Unresolved) => { changed=true; convertToFieldIdExpression(e) }
                     case AliasExpression(child@IdExpression(_,IdExpressionType.Unresolved), alias) => {changed=true; AliasExpression(convertToFieldIdExpression(child), alias)}
                     case x => x
                  }
             }
             if(changed) {
                 SelectExpression(child, newSelectList, forGroupBy)
             }
             else {
                 s
             }
         }

    }
}
