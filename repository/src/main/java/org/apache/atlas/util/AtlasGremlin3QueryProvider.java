/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.util;

public class AtlasGremlin3QueryProvider extends AtlasGremlin2QueryProvider {
    @Override
    public String getQuery(final AtlasGremlinQuery gremlinQuery) {
        // In case any overrides are necessary, a specific switch case can be added here to
        // return Gremlin 3 specific query otherwise delegate to super.getQuery
        switch (gremlinQuery) {
            case TYPE_UNUSED_COUNT_METRIC:
                return "g.V().has('__type', 'typeSystem').filter({ !it.getProperty('__type.category').name().matches('TRAIT') && it.inE().count() == 0}).count()";
            case ENTITY_COUNT_METRIC:
                return "g.V().has('__superTypeNames', within(['Referenceable'])).count()";
            case EXPORT_TYPE_STARTS_WITH:
                return "g.V().has('__typeName',typeName).filter({it.get().value(attrName).startsWith(attrValue)}).has('__guid').values('__guid').toList()";
            case EXPORT_TYPE_ENDS_WITH:
                return "g.V().has('__typeName',typeName).filter({it.get().value(attrName).endsWith(attrValue)}).has('__guid').values('__guid').toList()";
            case EXPORT_TYPE_CONTAINS:
                return "g.V().has('__typeName',typeName).filter({it.get().value(attrName).contains(attrValue)}).has('__guid').values('__guid').toList()";
            case EXPORT_TYPE_MATCHES:
                return "g.V().has('__typeName',typeName).filter({it.get().value(attrName).matches(attrValue)}).has('__guid').values('__guid').toList()";
            case EXPORT_TYPE_DEFAULT:
                return "g.V().has('__typeName',typeName).has(attrName, attrValue).has('__guid').values('__guid').toList()";
            case EXPORT_BY_GUID_FULL:
                return "g.V().has('__guid', startGuid).bothE().bothV().has('__guid').transform{[__guid:it.__guid,isProcess:(it.__superTypeNames != null) ? it.__superTypeNames.contains('Process') : false ]}.dedup().toList()";
            case EXPORT_BY_GUID_CONNECTED_IN_EDGE:
                return "g.V().has('__guid', startGuid).inE().outV().has('__guid').project('__guid', 'isProcess').by('__guid').by(map {it.get().values('__superTypeNames').toSet().contains('Process')}).dedup().toList()";
            case EXPORT_BY_GUID_CONNECTED_OUT_EDGE:
                return "g.V().has('__guid', startGuid).outE().inV().has('__guid').project('__guid', 'isProcess').by('__guid').by(map {it.get().values('__superTypeNames').toSet().contains('Process')}).dedup().toList()";
            case FULL_LINEAGE:
                return "g.V().has('__guid', '%s').repeat(__.in('%s').out('%s'))." +
                        "emit(has('__superTypeNames').and().properties('__superTypeNames').hasValue('DataSet'))." +
                        "path().toList()";
            case PARTIAL_LINEAGE:
                return "g.V().has('__guid', '%s').repeat(__.in('%s').out('%s')).times(%s)." +
                        "emit(has('__superTypeNames').and().properties('__superTypeNames').hasValue('DataSet'))." +
                        "path().toList()";
            case TO_RANGE_LIST:
                return ".range(startIdx, endIdx).toList()";
            case RELATIONSHIP_SEARCH:
                return "g.V().has('__guid', guid).both(relation).has('__state', within(states))";
            case RELATIONSHIP_SEARCH_ASCENDING_SORT:
                return ".order().by(sortAttributeName, incr)";
            case RELATIONSHIP_SEARCH_DESCENDING_SORT:
                return ".order().by(sortAttributeName, decr)";
            case GREMLIN_SEARCH_RETURNS_VERTEX_ID:
                return "g.V().range(0,1).toList()";
            case GREMLIN_SEARCH_RETURNS_EDGE_ID:
                return "g.E().range(0,1).toList()";
        }
        return super.getQuery(gremlinQuery);
    }
}
