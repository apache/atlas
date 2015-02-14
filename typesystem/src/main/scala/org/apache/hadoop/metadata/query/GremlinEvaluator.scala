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

import com.thinkaurelius.titan.core.TitanGraph
import com.tinkerpop.pipes.util.structures.Row
import org.apache.hadoop.metadata.json._
import org.apache.hadoop.metadata.types._
import org.json4s._
import org.json4s.native.Serialization._

import scala.language.existentials

case class GremlinQueryResult(query : String,
                              resultDataType : IDataType[_],
                               rows : List[_]) {
  def toJson = JsonHelper.toJson(this)
}

class GremlinEvaluator(qry : GremlinQuery, persistenceStrategy : GraphPersistenceStrategies, g: TitanGraph) {

  val manager: ScriptEngineManager = new ScriptEngineManager
  val engine: ScriptEngine = manager.getEngineByName("gremlin-groovy")
  val bindings: Bindings = engine.createBindings
  bindings.put("g", g)

  def evaluate() : GremlinQueryResult = {
    import scala.collection.JavaConversions._
    val rType = qry.expr.dataType
    val rawRes = engine.eval(qry.queryStr, bindings)

    if ( !qry.hasSelectList ) {
      val rows = rawRes.asInstanceOf[java.util.List[AnyRef]].map { v =>
        persistenceStrategy.constructInstance(rType, v)
      }
      GremlinQueryResult(qry.expr.toString, rType, rows.toList)
    } else {
      val sType = rType.asInstanceOf[StructType]
      val rows = rawRes.asInstanceOf[java.util.List[Row[java.util.List[_]]]].map { r =>
        val sInstance = sType.createInstance()
        val selExpr = qry.expr.asInstanceOf[Expressions.SelectExpression]
        selExpr.selectListWithAlias.foreach { aE =>
          val cName = aE.alias
          val (src, idx) = qry.resultMaping(cName)
          val v = r.getColumn(src).get(idx)
          sInstance.set(cName, aE.dataType.convert(v, Multiplicity.OPTIONAL))
        }
        sInstance
      }
      GremlinQueryResult(qry.expr.toString, sType, rows.toList)
    }

  }
}

object JsonHelper {

  class GremlinQueryResultSerializer()
    extends Serializer[GremlinQueryResult] {
    def deserialize(implicit format: Formats) = {
      throw new UnsupportedOperationException("Deserialization of GremlinQueryResult not supported")
    }

    def serialize(implicit f: Formats) = {
      case GremlinQueryResult(query, rT, rows) =>
        JObject(JField("query", JString(query)),
          JField("dataType", TypesSerialization.toJsonValue(rT)(f)),
          JField("rows", Extraction.decompose(rows)(f))
        )
    }
  }

  implicit val formats = org.json4s.native.Serialization.formats(NoTypeHints) + new TypedStructSerializer +
    new TypedReferenceableInstanceSerializer +  new BigDecimalSerializer + new BigIntegerSerializer +
    new GremlinQueryResultSerializer

  def toJson(r : GremlinQueryResult ) : String = {
    writePretty(r)
  }
}