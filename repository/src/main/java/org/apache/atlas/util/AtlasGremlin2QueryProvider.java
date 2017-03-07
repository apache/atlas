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

public class AtlasGremlin2QueryProvider extends AtlasGremlinQueryProvider {
    @Override
    public String getQuery(final AtlasGremlinQuery gremlinQuery) {
        switch (gremlinQuery) {
            case TYPE_COUNT_METRIC:
                return "g.V().has('__type', 'typeSystem').filter({!it.'__type.category'.name().matches('TRAIT')}).count()";
            case TYPE_UNUSED_COUNT_METRIC:
                return "g.V('__type', 'typeSystem').filter({ !it.getProperty('__type.category').name().matches('TRAIT') && it.inE().count() == 0}).count()";
            case ENTITY_COUNT_METRIC:
                return "g.V().has('__superTypeNames', T.in, ['Referenceable']).count()";
            case TAG_COUNT_METRIC:
                return "g.V().has('__type', 'typeSystem').filter({it.getProperty('__type.category').name().matches('TRAIT')}).count()";
            case ENTITY_DELETED_METRIC:
                return "g.V().has('__typeName', T.in, g.V().has('__type', 'typeSystem').filter{it.getProperty('__type.category').name().matches('CLASS')}.'__type.name'.toSet()).has('__status', 'DELETED').count()";
            case ENTITIES_PER_TYPE_METRIC:
                return "g.V().has('__typeName', T.in, g.V().has('__type', 'typeSystem').filter{it.getProperty('__type.category').name() == 'CLASS'}.'__type.name'.toSet()).groupCount{it.getProperty('__typeName')}.cap.toList()";
            case TAGGED_ENTITIES_METRIC:
                return "g.V().has('__traitNames', T.in, g.V().has('__type', 'typeSystem').filter{it.getProperty('__type.category').name() == 'TRAIT'}.'__type.name'.toSet()).count()";
            case ENTITIES_FOR_TAG_METRIC:
                return "g.V().has('__typeName', T.in, g.V().has('__type', 'typeSystem').filter{it.getProperty('__type.category').name() == 'TRAIT'}.'__type.name'.toSet()).groupCount{it.getProperty('__typeName')}.cap.toList()";
            case EXPORT_BY_GUID_FULL:
                return "g.V('__guid', startGuid).bothE().bothV().has('__guid').__guid.dedup().toList()";
            case EXPORT_BY_GUID_CONNECTED_IN_EDGE:
                return "g.V('__guid', startGuid).inE().outV().has('__guid').__guid.dedup().toList()";
            case EXPORT_BY_GUID_CONNECTED_OUT_EDGE:
                return "g.V('__guid', startGuid).outE().inV().has('__guid').__guid.dedup().toList()";
            case EXPORT_TYPE_STARTS_WITH:
                return "g.V().has('__typeName',typeName).filter({it.getProperty(attrName).startsWith(attrValue)}).has('__guid').__guid.toList()";
            case EXPORT_TYPE_ENDS_WITH:
                return "g.V().has('__typeName',typeName).filter({it.getProperty(attrName).endsWith(attrValue)}).has('__guid').__guid.toList()";
            case EXPORT_TYPE_CONTAINS:
                return "g.V().has('__typeName',typeName).filter({it.getProperty(attrName).contains(attrValue)}).has('__guid').__guid.toList()";
            case EXPORT_TYPE_MATCHES:
                return "g.V().has('__typeName',typeName).filter({it.getProperty(attrName).matches(attrValue)}).has('__guid').__guid.toList()";
            case EXPORT_TYPE_DEFAULT:
                return "g.V().has('__typeName',typeName).has(attrName, attrValue).has('__guid').__guid.toList()";
            case FULL_LINEAGE:
                return "g.V('__guid', '%s').as('src').in('%s').out('%s')." +
                        "loop('src', {((it.path.contains(it.object)) ? false : true)}, " +
                        "{((it.object.'__superTypeNames') ? " +
                        "(it.object.'__superTypeNames'.contains('DataSet')) : false)})." +
                        "path().toList()";
            case PARTIAL_LINEAGE:
                return "g.V('__guid', '%s').as('src').in('%s').out('%s')." +
                        "loop('src', {it.loops <= %s}, {((it.object.'__superTypeNames') ? " +
                        "(it.object.'__superTypeNames'.contains('DataSet')) : false)})." +
                        "path().toList()";

            case BASIC_SEARCH_TYPE_FILTER:
                return ".has('__typeName', T.in, typeNames)";
            case BASIC_SEARCH_CLASSIFICATION_FILTER:
                return ".has('__traitNames', T.in, traitNames)";
            case TO_RANGE_LIST:
                return " [startIdx..<endIdx].toList()";
        }
        // Should never reach this point
        return null;
    }

}
