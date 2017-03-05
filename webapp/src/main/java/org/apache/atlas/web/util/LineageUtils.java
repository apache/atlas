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
package org.apache.atlas.web.util;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.types.TypeSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_SUFFIX;

public final class LineageUtils {
    private LineageUtils() {}

    private static final String VERTICES_ATTR_NAME    = "vertices";
    private static final String EDGES_ATTR_NAME       = "edges";
    private static final String VERTEX_ID_ATTR_NAME   = "vertexId";
    private static final String TEMP_STRUCT_ID_RESULT = "__IdType";

    private static final AtomicInteger COUNTER = new AtomicInteger();

    public static String toLineageStruct(AtlasLineageInfo lineageInfo, AtlasTypeRegistry registry) throws AtlasBaseException {
        String ret = null;

        if (lineageInfo != null) {
            Map<String, AtlasEntityHeader>        entities    = lineageInfo.getGuidEntityMap();
            Set<AtlasLineageInfo.LineageRelation> relations   = lineageInfo.getRelations();
            AtlasLineageInfo.LineageDirection     direction   = lineageInfo.getLineageDirection();
            Map<String, Struct>                   verticesMap = new HashMap<>();

            // Lineage Entities mapping -> verticesMap (vertices)
            for (String guid : entities.keySet()) {
                AtlasEntityHeader entityHeader = entities.get(guid);

                if (isDataSet(entityHeader.getTypeName(), registry)) {
                    Map<String, Object> vertexIdMap = new HashMap<>();
                    TypeSystem.IdType   idType      = TypeSystem.getInstance().getIdType();

                    vertexIdMap.put(idType.idAttrName(), guid);
                    vertexIdMap.put(idType.stateAttrName(), (entityHeader.getStatus() == AtlasEntity.Status.ACTIVE) ? "ACTIVE" : "DELETED");
                    vertexIdMap.put(idType.typeNameAttrName(), entityHeader.getTypeName());

                    Object qualifiedName = entityHeader.getAttribute(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME);
                    if (qualifiedName == null) {
                        qualifiedName = entityHeader.getDisplayText();
                    }

                    Map<String, Object> values = new HashMap<>();
                    values.put(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, qualifiedName);
                    values.put(VERTEX_ID_ATTR_NAME, constructResultStruct(vertexIdMap, true));
                    values.put(AtlasClient.NAME, entityHeader.getDisplayText());
                    verticesMap.put(guid, constructResultStruct(values, false));
                }
            }

            // Lineage Relations mapping -> edgesMap (edges)
            Map<String, List<String>> edgesMap = new HashMap<>();

            for (AtlasLineageInfo.LineageRelation relation : relations) {
                String fromEntityId = relation.getFromEntityId();
                String toEntityId   = relation.getToEntityId();

                if (direction == AtlasLineageInfo.LineageDirection.INPUT) {
                    if (!edgesMap.containsKey(toEntityId)) {
                        edgesMap.put(toEntityId, new ArrayList<String>());
                    }
                    edgesMap.get(toEntityId).add(fromEntityId);

                } else if (direction == AtlasLineageInfo.LineageDirection.OUTPUT) {
                    if (!edgesMap.containsKey(fromEntityId)) {
                        edgesMap.put(fromEntityId, new ArrayList<String>());
                    }
                    edgesMap.get(fromEntityId).add(toEntityId);
                }
            }

            Map<String, Object> map = new HashMap<>();
            map.put(VERTICES_ATTR_NAME, verticesMap);
            map.put(EDGES_ATTR_NAME, edgesMap);

            ret = InstanceSerialization.toJson(constructResultStruct(map, false), false);
        }

        return ret;
    }

    private static Struct constructResultStruct(Map<String, Object> values, boolean idType) {
        if (idType) {
            return new Struct(TEMP_STRUCT_ID_RESULT, values);
        }

        return new Struct(org.apache.atlas.query.TypeUtils.TEMP_STRUCT_NAME_PREFIX() + COUNTER.getAndIncrement(), values);
    }

    private static boolean isDataSet(String typeName, AtlasTypeRegistry registry) throws AtlasBaseException {
        boolean   ret  = false;
        AtlasType type = registry.getType(typeName);

        if (type instanceof AtlasEntityType) {
            AtlasEntityType entityType = (AtlasEntityType) type;
            ret = entityType.getAllSuperTypes().contains(AtlasBaseTypeDef.ATLAS_TYPE_DATASET);
        }

        return ret;
    }

}
