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

import javax.script.{Bindings, ScriptEngine, ScriptEngineManager}

import com.thinkaurelius.titan.core.{TitanVertex, TitanGraph}
import org.apache.hadoop.metadata.ITypedInstance
import org.apache.hadoop.metadata.types.{ClassType, IConstructableType}
import scala.language.existentials

case class GremlinQueryResult(qry : GremlinQuery,
                              resultDataType : IConstructableType[_, _ <: ITypedInstance],
                               rows : List[ITypedInstance])

class GremlinEvaluator(qry : GremlinQuery, persistenceStrategy : GraphPersistenceStrategies, g: TitanGraph) {

  val manager: ScriptEngineManager = new ScriptEngineManager
  val engine: ScriptEngine = manager.getEngineByName("gremlin-groovy")
  val bindings: Bindings = engine.createBindings
  bindings.put("g", g)

  def evaluate() : GremlinQueryResult = {
    import scala.collection.JavaConversions._
    val rType = qry.expr.dataType
    val rawRes = engine.eval(qry.queryStr, bindings)

    if ( rType.isInstanceOf[ClassType]) {
      val dType = rType.asInstanceOf[ClassType]
      val rows = rawRes.asInstanceOf[java.util.List[TitanVertex]].map { v =>
        persistenceStrategy.constructClassInstance(dType, v)
      }
      GremlinQueryResult(qry, dType, rows.toList)
    } else {
      null
    }

  }
}
