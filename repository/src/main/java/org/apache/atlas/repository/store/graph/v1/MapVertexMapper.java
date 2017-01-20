/**
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
package org.apache.atlas.repository.store.graph.v1;


import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.repository.graph.GraphHelper.string;

public class MapVertexMapper implements InstanceGraphMapper<Map> {

    private DeleteHandlerV1 deleteHandler;

    private static final Logger LOG = LoggerFactory.getLogger(MapVertexMapper.class);

    private StructVertexMapper structVertexMapper;

    @Inject
    public MapVertexMapper(DeleteHandlerV1 deleteHandler) {
        this.deleteHandler = deleteHandler;
    }

    void init(StructVertexMapper structVertexMapper) {
        this.structVertexMapper = structVertexMapper;
    }

    @Override
    public Map<String, Object> toGraph(GraphMutationContext ctx) throws AtlasBaseException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping instance to vertex {} for map type {}", string(ctx.getReferringVertex()), ctx.getAttrType().getTypeName());
        }

        @SuppressWarnings("unchecked") Map<Object, Object> newVal =
            (Map<Object, Object>) ctx.getValue();

        boolean newAttributeEmpty = MapUtils.isEmpty(newVal);

        Map<String, Object> currentMap = new HashMap<>();
        Map<String, Object> newMap = new HashMap<>();

        AtlasMapType mapType = (AtlasMapType) ctx.getAttrType();

        try {
            List<String> currentKeys = GraphHelper.getListProperty(ctx.getReferringVertex(), ctx.getVertexPropertyKey());
            if (currentKeys != null && !currentKeys.isEmpty()) {
                for (String key : currentKeys) {
                    String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(ctx.getVertexPropertyKey(), key);
                    Object propertyValueForKey = getMapValueProperty(mapType.getValueType(), ctx.getReferringVertex(), propertyNameForKey);
                    currentMap.put(key, propertyValueForKey);
                }
            }

            if (!newAttributeEmpty) {
                for (Map.Entry<Object, Object> entry : newVal.entrySet()) {
                    String keyStr = entry.getKey().toString();
                    String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(ctx.getVertexPropertyKey(), keyStr);
                    Optional<AtlasEdge> existingEdge = getEdgeIfExists(mapType, currentMap, keyStr);

                    GraphMutationContext mapCtx =  new GraphMutationContext.Builder(ctx.getAttribute(), mapType.getValueType(), entry.getValue())
                        .referringVertex(ctx.getReferringVertex())
                        .edge(existingEdge)
                        .vertexProperty(propertyNameForKey).build();


                    Object newEntry = structVertexMapper.mapCollectionElementsToVertex(mapCtx);
                    newMap.put(keyStr, newEntry);
                }
            }

            Map<String, Object> finalMap =
                removeUnusedMapEntries(ctx.getParentType(), mapType, ctx.getAttributeDef(), ctx.getReferringVertex(), ctx.getVertexPropertyKey(), currentMap, newMap);

            Set<String> newKeys = new HashSet<>(newMap.keySet());
            newKeys.addAll(finalMap.keySet());

            // for dereference on way out
            GraphHelper.setListProperty(ctx.getReferringVertex(), ctx.getVertexPropertyKey(), new ArrayList<>(newKeys));
        } catch (AtlasException e) {
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Map values set in vertex {} {}", mapType.getTypeName(), newMap);
        }

        return newMap;
    }

    @Override
    public void cleanUp() throws AtlasBaseException {
    }


    public static Object getMapValueProperty(AtlasType elementType, AtlasVertex instanceVertex, String propertyName) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            return instanceVertex.getProperty(actualPropertyName, AtlasEdge.class);
        }
        else {
            return instanceVertex.getProperty(actualPropertyName, String.class).toString();
        }
    }

    public static void setMapValueProperty(AtlasType elementType, AtlasVertex instanceVertex, String propertyName, Object value) {
        String actualPropertyName = GraphHelper.encodePropertyKey(propertyName);
        if (AtlasGraphUtilsV1.isReference(elementType)) {
            instanceVertex.setPropertyFromElementId(actualPropertyName, (AtlasEdge)value);
        }
        else {
            instanceVertex.setProperty(actualPropertyName, value);
        }
    }

    //Remove unused entries from map
    private Map<String, Object> removeUnusedMapEntries(
        AtlasStructType entityType,
        AtlasMapType mapType, AtlasStructDef.AtlasAttributeDef attributeDef,
        AtlasVertex instanceVertex, String propertyName,
        Map<String, Object> currentMap,
        Map<String, Object> newMap)
        throws AtlasException, AtlasBaseException {

        Map<String, Object> additionalMap = new HashMap<>();
        for (String currentKey : currentMap.keySet()) {

            boolean shouldDeleteKey = !newMap.containsKey(currentKey);
            if (AtlasGraphUtilsV1.isReference(mapType.getValueType())) {

                //Delete the edge reference if its not part of new edges created/updated
                AtlasEdge currentEdge = (AtlasEdge)currentMap.get(currentKey);

                if (!newMap.values().contains(currentEdge)) {
                    boolean deleteChildReferences = StructVertexMapper.shouldManageChildReferences(entityType, attributeDef.getName());
                    boolean deleted =
                        deleteHandler.deleteEdgeReference(currentEdge, mapType.getValueType().getTypeCategory(), deleteChildReferences, true);
                    if (!deleted) {
                        additionalMap.put(currentKey, currentEdge);
                        shouldDeleteKey = false;
                    }
                }
            }

            if (shouldDeleteKey) {
                String propertyNameForKey = GraphHelper.getQualifiedNameForMapKey(propertyName, currentKey);
                GraphHelper.setProperty(instanceVertex, propertyNameForKey, null);
            }
        }
        return additionalMap;
    }
    
    private Optional<AtlasEdge> getEdgeIfExists(AtlasMapType mapType, Map<String, Object> currentMap, String keyStr) {
        Optional<AtlasEdge> existingEdge = Optional.absent();
        if ( AtlasGraphUtilsV1.isReference(mapType.getValueType()) ) {
            existingEdge = Optional.of((AtlasEdge) currentMap.get(keyStr));
        }
        
        return existingEdge;
    }
}
