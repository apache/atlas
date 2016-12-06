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
import org.apache.atlas.repository.graphdb.AtlasGraph
import org.apache.atlas.query.TypeUtils.ResultWithPathStruct
import org.apache.atlas.typesystem.json._
import org.apache.atlas.typesystem.types._
import org.json4s._
import org.json4s.native.Serialization._
import scala.language.existentials
import org.apache.atlas.query.Expressions._

case class GremlinQueryResult(query: String,
                              resultDataType: IDataType[_],
                              rows: List[_]) {
    def this(query: String,resultDataType: IDataType[_]) {
      this(query,resultDataType,List.empty)
    }
  
    def toJson = JsonHelper.toJson(this)
}

class GremlinEvaluator(qry: GremlinQuery, persistenceStrategy: GraphPersistenceStrategies, g: AtlasGraph[_,_]) {

   /**
     *
     * @param gResultObj is the object returned from gremlin. This must be a List
     * @param qryResultObj is the object constructed for the output w/o the Path.
     * @return a ResultWithPathStruct
     */
    def addPathStruct(gResultObj: AnyRef, qryResultObj: Any): Any = {
        if (!qry.isPathExpression) {
            qryResultObj
        } else {
            import scala.collection.JavaConversions._
            import scala.collection.JavaConverters._
            
            val iPaths = gResultObj.asInstanceOf[java.util.List[AnyRef]].init

            val oPaths = iPaths.map { value =>
                persistenceStrategy.constructInstance(TypeSystem.getInstance().getIdType.getStructType, value)
            }.toList.asJava
            val sType = qry.expr.dataType.asInstanceOf[StructType]
            val sInstance = sType.createInstance()
            sInstance.set(ResultWithPathStruct.pathAttrName, oPaths)
            sInstance.set(ResultWithPathStruct.resultAttrName, qryResultObj)
            sInstance
        }
    }

    def instanceObject(v: AnyRef): AnyRef = {
        if (qry.isPathExpression) {
            import scala.collection.JavaConversions._
            v.asInstanceOf[java.util.List[AnyRef]].last
        } else {
            v
        }
    }

    def evaluate(): GremlinQueryResult = {
        import scala.collection.JavaConversions._
         val debug:Boolean = false
        val rType = qry.expr.dataType
        val oType = if (qry.isPathExpression) {
            qry.expr.children(0).dataType
        }
        else {
            rType
        }
        val rawRes = g.executeGremlinScript(qry.queryStr, qry.isPathExpression);
        if(debug) {
            println(" rawRes " +rawRes)
        }
        if (!qry.hasSelectList && ! qry.isGroupBy) {
            val rows = rawRes.asInstanceOf[java.util.List[AnyRef]].map { v =>
                val instObj = instanceObject(v)
                val o = persistenceStrategy.constructInstance(oType, instObj)
                addPathStruct(v, o)
            }
            GremlinQueryResult(qry.expr.toString, rType, rows.toList)
        } else {
            val sType = oType.asInstanceOf[StructType]
            val rows = rawRes.asInstanceOf[java.util.List[AnyRef]].map { r =>
                val rV = instanceObject(r)
                val sInstance = sType.createInstance()
                val selObj = SelectExpressionHelper.extractSelectExpression(qry.expr)
                if (selObj.isDefined) {
                    val selExpr = selObj.get.asInstanceOf[Expressions.SelectExpression]
                    selExpr.selectListWithAlias.foreach { aE =>
                        val cName = aE.alias
                        val (src, idx) = qry.resultMaping(cName)
                        val v = getColumnValue(rV, src, idx)
                        sInstance.set(cName, persistenceStrategy.constructInstance(aE.dataType, v))
                    }
                }
                else if(qry.isGroupBy) {
                    //the order in the result will always match the order in the select list
                    val selExpr = qry.expr.asInstanceOf[GroupByExpression].selExpr
                    var idx = 0;
                    val row : java.util.List[Object] = rV.asInstanceOf[java.util.List[Object]]
                    selExpr.selectListWithAlias.foreach { aE =>
                        val cName = aE.alias
                        val cValue = row.get(idx);

                        sInstance.set(cName, persistenceStrategy.constructInstance(aE.dataType, cValue))
                        idx += 1;
                    }
                }
                addPathStruct(r, sInstance)
            }
            GremlinQueryResult(qry.expr.toString, rType, rows.toList)
        }

    }
    
    private def getColumnValue(rowValue: AnyRef, colName: String, idx: Integer) : AnyRef  = {

        var rawColumnValue: AnyRef = null;
        if(rowValue.isInstanceOf[java.util.Map[_,_]]) {
            val columnsMap = rowValue.asInstanceOf[java.util.Map[String,AnyRef]];
            rawColumnValue = columnsMap.get(colName);
        }
        else {
            //when there is only one column, result does not come back as a map
            rawColumnValue = rowValue;
        }

        var value : AnyRef = null;
        if(rawColumnValue.isInstanceOf[java.util.List[_]] && idx >= 0) {
            val arr = rawColumnValue.asInstanceOf[java.util.List[AnyRef]];
            value = arr.get(idx);
        }
        else {
            value = rawColumnValue;
        }

        return value;
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
        new TypedReferenceableInstanceSerializer + new BigDecimalSerializer + new BigIntegerSerializer +
        new GremlinQueryResultSerializer

    def toJson(r: GremlinQueryResult): String = {
        writePretty(r)
    }
}
