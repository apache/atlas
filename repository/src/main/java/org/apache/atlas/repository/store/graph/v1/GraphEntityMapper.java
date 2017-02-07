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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityExtInfo;
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntityWithExtInfo;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasStructType.AtlasAttribute;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BIGDECIMAL;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BIGINTEGER;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BOOLEAN;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_BYTE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_DATE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_DOUBLE;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_FLOAT;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_INT;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_LONG;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_SHORT;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_STRING;
import static org.apache.atlas.repository.graph.GraphHelper.EDGE_LABEL_PREFIX;

@Singleton
public final class GraphEntityMapper {
    private static final Logger LOG = LoggerFactory.getLogger(GraphEntityMapper.class);
    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    @Inject
    protected AtlasTypeRegistry typeRegistry;

    public AtlasEntityWithExtInfo toAtlasEntity(String guid, boolean includeReferences) throws AtlasBaseException {
        AtlasEntityExtInfo     entityExtInfo = includeReferences ? new AtlasEntityExtInfo() : null;
        AtlasEntity            entity        = mapVertexToAtlasEntity(guid, entityExtInfo);
        AtlasEntityWithExtInfo ret           = new AtlasEntityWithExtInfo(entity, entityExtInfo);

        ret.compact();

        return ret;
    }

    private AtlasEntity mapVertexToAtlasEntity(String guid, AtlasEntityExtInfo entityExtInfo) throws AtlasBaseException {
        AtlasEntity entity = entityExtInfo != null ? entityExtInfo.getEntity(guid) : null;

        if (entity != null) {
            return entity;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping graph vertex to atlas entity for guid {}", guid);
        }

        try {
            AtlasVertex entityVertex = graphHelper.getVertexForGUID(guid);
            entity = new AtlasEntity();

            if (entityExtInfo != null) {
                entityExtInfo.addReferredEntity(guid, entity);
            }

            mapSystemAttributes(entityVertex, entity);

            mapAttributes(entityVertex, entity, entityExtInfo);

            mapClassifications(entityVertex, entity, entityExtInfo);

        } catch (AtlasException e) {
            throw new AtlasBaseException(AtlasErrorCode.INSTANCE_GUID_NOT_FOUND, guid);
        }

        return entity;
    }

    private AtlasEntity mapSystemAttributes(AtlasVertex entityVertex, AtlasEntity entity) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping system attributes for type {}", entity.getTypeName());
        }

        entity.setGuid(GraphHelper.getGuid(entityVertex));
        entity.setTypeName(GraphHelper.getSingleValuedProperty(entityVertex, Constants.ENTITY_TYPE_PROPERTY_KEY, String.class));
        entity.setStatus(GraphHelper.getStatus(entityVertex));
        entity.setVersion(GraphHelper.getVersion(entityVertex).longValue());

        entity.setCreatedBy(GraphHelper.getCreatedByAsString(entityVertex));
        entity.setUpdatedBy(GraphHelper.getModifiedByAsString(entityVertex));

        entity.setCreateTime(new Date(GraphHelper.getCreatedTime(entityVertex)));
        entity.setUpdateTime(new Date(GraphHelper.getModifiedTime(entityVertex)));

        return entity;
    }

    private void mapAttributes(AtlasVertex entityVertex, AtlasStruct struct, AtlasEntityExtInfo entityExtInfo) throws AtlasBaseException {
        AtlasType objType  = typeRegistry.getType(struct.getTypeName());

        if (!(objType instanceof AtlasStructType)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, struct.getTypeName());
        }

        AtlasStructType            structType = (AtlasStructType) objType;
        Collection<AtlasAttribute> attributes = structType.getAllAttributes().values();

        if (CollectionUtils.isNotEmpty(attributes)) {
            for (AtlasAttribute attribute : attributes) {
                Object attrValue = mapVertexToAttribute(entityVertex, attribute, entityExtInfo);

                struct.setAttribute(attribute.getName(), attrValue);
            }
        }
    }

    private void mapClassifications(AtlasVertex entityVertex, AtlasEntity entity, AtlasEntityExtInfo entityExtInfo) throws AtlasBaseException {
        List<String> classficationNames = GraphHelper.getTraitNames(entityVertex);

        if (CollectionUtils.isNotEmpty(classficationNames)) {
            List<AtlasClassification> classifications = new ArrayList<>();

            for (String classficationName : classficationNames) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("mapping classification {} to atlas entity", classficationName);
                }

                Iterable<AtlasEdge> edges = entityVertex.getEdges(AtlasEdgeDirection.OUT, classficationName);

                AtlasEdge edge = (edges != null && edges.iterator().hasNext()) ? edges.iterator().next() : null;

                if (edge != null) {
                    AtlasClassification classification = new AtlasClassification(classficationName);

                    mapAttributes(edge.getInVertex(), classification, entityExtInfo);

                    classifications.add(classification);
                }
            }

            entity.setClassifications(classifications);
        }
    }

    private Object mapVertexToAttribute(AtlasVertex entityVertex, AtlasAttribute attribute, AtlasEntityExtInfo entityExtInfo) throws AtlasBaseException {
        Object    ret                = null;
        AtlasType attrType           = attribute.getAttributeType();
        String    vertexPropertyName = attribute.getQualifiedName();
        String    edgeLabel          = EDGE_LABEL_PREFIX + vertexPropertyName;
        boolean   isOwnedAttribute   = attribute.isOwnedRef();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping vertex {} to atlas entity {}.{}", entityVertex, attribute.getDefinedInDef().getName(), attribute.getName());
        }

        switch (attrType.getTypeCategory()) {
            case PRIMITIVE:
                ret = mapVertexToPrimitive(entityVertex, vertexPropertyName, attribute.getAttributeDef());
                break;
            case ENUM:
                ret = GraphHelper.getProperty(entityVertex, vertexPropertyName);
                break;
            case STRUCT:
                ret = mapVertexToStruct(entityVertex, edgeLabel, null, entityExtInfo);
                break;
            case ENTITY:
                ret = mapVertexToObjectId(entityVertex, edgeLabel, null, entityExtInfo, isOwnedAttribute);
                break;
            case ARRAY:
                ret = mapVertexToArray(entityVertex, (AtlasArrayType) attrType, vertexPropertyName, entityExtInfo, isOwnedAttribute);
                break;
            case MAP:
                ret = mapVertexToMap(entityVertex, (AtlasMapType) attrType, vertexPropertyName, entityExtInfo, isOwnedAttribute);
                break;
            case CLASSIFICATION:
                // do nothing
                break;
        }

        return ret;
    }

    private Map<String, Object> mapVertexToMap(AtlasVertex entityVertex, AtlasMapType atlasMapType, final String propertyName,
                                               AtlasEntityExtInfo entityExtInfo, boolean isOwnedAttribute) throws AtlasBaseException {
        Map<String, Object> ret       = new HashMap<>();
        List<String>        mapKeys   = GraphHelper.getListProperty(entityVertex, propertyName);
        AtlasType           mapValueType = atlasMapType.getValueType();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping map attribute {} for vertex {}", atlasMapType.getTypeName(), entityVertex);
        }

        if (CollectionUtils.isEmpty(mapKeys)) {
            return null;
        }

        for (String mapKey : mapKeys) {
            final String keyPropertyName = propertyName + "." + mapKey;
            final String edgeLabel       = EDGE_LABEL_PREFIX + keyPropertyName;
            final Object keyValue        = GraphHelper.getMapValueProperty(mapValueType, entityVertex, keyPropertyName);

            Object mapValue = mapVertexToCollectionEntry(entityVertex, mapValueType, keyValue, edgeLabel, entityExtInfo, isOwnedAttribute);
            if (mapValue != null) {
                ret.put(mapKey, mapValue);
            }
        }

        return ret;
    }

    private List<Object> mapVertexToArray(AtlasVertex entityVertex, AtlasArrayType arrayType, String propertyName,
                                          AtlasEntityExtInfo entityExtInfo, boolean isOwnedAttribute) throws AtlasBaseException {
        AtlasType    arrayElementType = arrayType.getElementType();
        List<Object> arrayElements    = GraphHelper.getArrayElementsProperty(arrayElementType, entityVertex, propertyName);
        List         arrValues        = new ArrayList();
        String       edgeLabel        = EDGE_LABEL_PREFIX + propertyName;

        if (LOG.isDebugEnabled()) {
            LOG.debug("Mapping array attribute {} for vertex {}", arrayElementType.getTypeName(), entityVertex);
        }

        if (CollectionUtils.isEmpty(arrayElements)) {
            return null;
        }

        for (Object element : arrayElements) {
            Object arrValue = mapVertexToCollectionEntry(entityVertex, arrayElementType, element,
                                                         edgeLabel, entityExtInfo, isOwnedAttribute);

            arrValues.add(arrValue);
        }

        return arrValues;
    }

    private Object mapVertexToCollectionEntry(AtlasVertex entityVertex, AtlasType arrayElement, Object value, String edgeLabel,
                                              AtlasEntityExtInfo entityExtInfo, boolean isOwnedAttribute) throws AtlasBaseException {
        Object ret = null;

        switch (arrayElement.getTypeCategory()) {
            case PRIMITIVE:
            case ENUM:
                ret = value;
                break;

            case ARRAY:
            case MAP:
            case CLASSIFICATION:
                break;

            case STRUCT:
                ret = mapVertexToStruct(entityVertex, edgeLabel, (AtlasEdge) value, entityExtInfo);
                break;

            case ENTITY:
                ret = mapVertexToObjectId(entityVertex, edgeLabel, (AtlasEdge) value, entityExtInfo, isOwnedAttribute);
                break;

            default:
                break;
        }

        return ret;
    }

    private Object mapVertexToPrimitive(AtlasVertex entityVertex, final String vertexPropertyName, AtlasAttributeDef attrDef) {
        Object ret = null;

        if (GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, Object.class) == null) {
            return null;
        }

        switch (attrDef.getTypeName().toLowerCase()) {
            case ATLAS_TYPE_STRING:
                ret = GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, String.class);
                break;
            case ATLAS_TYPE_SHORT:
                ret = GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, Short.class);
                break;
            case ATLAS_TYPE_INT:
                ret = GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, Integer.class);
                break;
            case ATLAS_TYPE_BIGINTEGER:
                ret = GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, BigInteger.class);
                break;
            case ATLAS_TYPE_BOOLEAN:
                ret = GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, Boolean.class);
                break;
            case ATLAS_TYPE_BYTE:
                ret = GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, Byte.class);
                break;
            case ATLAS_TYPE_LONG:
                ret = GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, Long.class);
                break;
            case ATLAS_TYPE_FLOAT:
                ret = GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, Float.class);
                break;
            case ATLAS_TYPE_DOUBLE:
                ret = GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, Double.class);
                break;
            case ATLAS_TYPE_BIGDECIMAL:
                ret = GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, BigDecimal.class);
                break;
            case ATLAS_TYPE_DATE:
                ret = new Date(GraphHelper.getSingleValuedProperty(entityVertex, vertexPropertyName, Long.class));
                break;
            default:
                break;
        }

        return ret;
    }

    private AtlasObjectId mapVertexToObjectId(AtlasVertex entityVertex, final String edgeLabel, final AtlasEdge optionalEdge,
                                              AtlasEntityExtInfo entityExtInfo, boolean isOwnedAttribute) throws AtlasBaseException {
        AtlasObjectId ret = new AtlasObjectId();
        AtlasEdge     edge;

        if (optionalEdge == null) {
            edge = graphHelper.getEdgeForLabel(entityVertex, edgeLabel);
        } else {
            edge = optionalEdge;
        }

        if (GraphHelper.elementExists(edge)) {
            final AtlasVertex referenceVertex = edge.getInVertex();
            final String      guid            = GraphHelper.getSingleValuedProperty(referenceVertex, Constants.GUID_PROPERTY_KEY, String.class);
            final String      typeName        = GraphHelper.getTypeName(referenceVertex);

            if (entityExtInfo != null && isOwnedAttribute) {
                mapVertexToAtlasEntity(guid, entityExtInfo);
            }

            ret = new AtlasObjectId(typeName, guid);
        }

        return ret;
    }

    private AtlasStruct mapVertexToStruct(AtlasVertex entityVertex, final String edgeLabel, final AtlasEdge optionalEdge,
                                          AtlasEntityExtInfo entityExtInfo) throws AtlasBaseException {
        AtlasStruct ret = null;
        AtlasEdge   edge;

        if (optionalEdge == null) {
            edge = graphHelper.getEdgeForLabel(entityVertex, edgeLabel);
        } else {
            edge = optionalEdge;
        }

        if (GraphHelper.elementExists(edge)) {
            final AtlasVertex referenceVertex = edge.getInVertex();
            ret = new AtlasStruct(GraphHelper.getSingleValuedProperty(referenceVertex, Constants.ENTITY_TYPE_PROPERTY_KEY, String.class));

            mapAttributes(referenceVertex, ret, entityExtInfo);
        }

        return ret;
    }
}