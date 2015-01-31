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
import com.thinkaurelius.titan.core.TitanGraph

object QueryProcessor {

  def evaluate(e : Expression, g : TitanGraph) : AnyRef = {
    val e1 = validate(e)
    val q = new GremlinTranslator(e1).translate()
    println(q.queryStr)
    new GremlinEvaluator(q, g).evaluate()
  }

  def validate(e : Expression) : Expression = {
    val e1 = e.transformUp(new Resolver())

    e1.traverseUp {
      case x : Expression if !x.resolved =>
        throw new ExpressionException(x, s"Failed to resolved expression $x")
    }

    /*
     * trigger computation of dataType of expression tree
     */
    e1.dataType

    e1
  }

}
