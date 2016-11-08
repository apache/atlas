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

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasStructDefs;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasStructDefStore;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_ON_DELETE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_VAL_CASCADE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_FOREIGN_KEY;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_MAPPED_FROM_REF;

/**
 * StructDef store in v1 format.
 */
public class AtlasStructDefStoreV1 extends AtlasAbstractDefStoreV1 implements AtlasStructDefStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasStructDefStoreV1.class);

    public AtlasStructDefStoreV1(AtlasTypeDefGraphStoreV1 typeDefStore, AtlasTypeRegistry typeRegistry) {
        super(typeDefStore, typeRegistry);
    }

    @Override
    public AtlasVertex preCreate(AtlasStructDef structDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.preCreate({})", structDef);
        }

        AtlasType type = typeRegistry.getType(structDef.getName());

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.STRUCT) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, structDef.getName(), TypeCategory.STRUCT.name());
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByName(structDef.getName());

        if (ret != null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_ALREADY_EXISTS, structDef.getName());
        }

        ret = typeDefStore.createTypeVertex(structDef);

        AtlasStructDefStoreV1.updateVertexPreCreate(structDef, (AtlasStructType)type, ret, typeDefStore);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.preCreate({}): {}", structDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasStructDef create(AtlasStructDef structDef, Object preCreateResult) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.create({}, {})", structDef, preCreateResult);
        }

        AtlasVertex vertex;

        if (preCreateResult == null || !(preCreateResult instanceof AtlasVertex)) {
            vertex = preCreate(structDef);
        } else {
            vertex = (AtlasVertex)preCreateResult;
        }

        AtlasStructDefStoreV1.updateVertexAddReferences(structDef, vertex, typeDefStore);

        AtlasStructDef ret = toStructDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.create({}, {}): {}", structDef, preCreateResult, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasStructDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.getAll()");
        }

        List<AtlasStructDef> ret = new ArrayList<>();

        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(TypeCategory.STRUCT);
        while (vertices.hasNext()) {
            ret.add(toStructDef(vertices.next()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.getAll(): count={}", ret.size());
        }
        return ret;
    }

    @Override
    public AtlasStructDef getByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.getByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.STRUCT);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, String.class);

        AtlasStructDef ret = toStructDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.getByName({}): {}", name, ret);
        }

        return ret;
    }

    @Override
    public AtlasStructDef getByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.getByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.STRUCT);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        AtlasStructDef ret = toStructDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.getByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasStructDef update(AtlasStructDef structDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.update({})", structDef);
        }

        AtlasStructDef ret = StringUtils.isNotBlank(structDef.getGuid()) ? updateByGuid(structDef.getGuid(), structDef)
                                                                         : updateByName(structDef.getName(), structDef);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.update({}): {}", structDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasStructDef updateByName(String name, AtlasStructDef structDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.updateByName({}, {})", name, structDef);
        }

        AtlasType type = typeRegistry.getType(structDef.getName());

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.STRUCT) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, structDef.getName(), TypeCategory.STRUCT.name());
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.STRUCT);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        AtlasStructDefStoreV1.updateVertexPreUpdate(structDef, (AtlasStructType)type, vertex, typeDefStore);
        AtlasStructDefStoreV1.updateVertexAddReferences(structDef, vertex, typeDefStore);

        AtlasStructDef ret = toStructDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.updateByName({}, {}): {}", name, structDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasStructDef updateByGuid(String guid, AtlasStructDef structDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.updateByGuid({})", guid);
        }

        AtlasType type = typeRegistry.getTypeByGuid(guid);

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.STRUCT) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, structDef.getName(), TypeCategory.STRUCT.name());
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.STRUCT);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        AtlasStructDefStoreV1.updateVertexPreUpdate(structDef, (AtlasStructType)type, vertex, typeDefStore);
        AtlasStructDefStoreV1.updateVertexAddReferences(structDef, vertex, typeDefStore);

        AtlasStructDef ret = toStructDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.updateByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasVertex preDeleteByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.preDeleteByName({})", name);
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.STRUCT);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        typeDefStore.deleteTypeVertexOutEdges(ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.preDeleteByName({}): {}", name, ret);
        }

        return ret;
    }

    @Override
    public void deleteByName(String name, Object preDeleteResult) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.deleteByName({}, {})", name, preDeleteResult);
        }

        AtlasVertex vertex;

        if (preDeleteResult == null || !(preDeleteResult instanceof AtlasVertex)) {
            vertex = preDeleteByName(name);
        } else {
            vertex = (AtlasVertex)preDeleteResult;
        }

        typeDefStore.deleteTypeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.deleteByName({}, {})", name, preDeleteResult);
        }
    }

    @Override
    public AtlasVertex preDeleteByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.preDeleteByGuid({})", guid);
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.STRUCT);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        typeDefStore.deleteTypeVertexOutEdges(ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.preDeleteByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public void deleteByGuid(String guid, Object preDeleteResult) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.deleteByGuid({}, {})", guid, preDeleteResult);
        }

        AtlasVertex vertex;

        if (preDeleteResult == null || !(preDeleteResult instanceof AtlasVertex)) {
            vertex = preDeleteByGuid(guid);
        } else {
            vertex = (AtlasVertex)preDeleteResult;
        }

        typeDefStore.deleteTypeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.deleteByGuid({}, {})", guid, preDeleteResult);
        }
    }

    @Override
    public AtlasStructDefs search(SearchFilter filter) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.search({})", filter);
        }

        List<AtlasStructDef>  structDefs = new ArrayList<>();
        Iterator<AtlasVertex> vertices   = typeDefStore.findTypeVerticesByCategory(TypeCategory.STRUCT);

        while (vertices.hasNext()) {
            AtlasVertex    vertex    = vertices.next();
            AtlasStructDef structDef = toStructDef(vertex);

            if (structDef != null) {
                structDefs.add(structDef);
            }
        }

        CollectionUtils.filter(structDefs, FilterUtil.getPredicateFromSearchFilter(filter));

        AtlasStructDefs ret = new AtlasStructDefs(structDefs);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.search({}): {}", filter, ret);
        }

        return ret;
    }

    private AtlasStructDef toStructDef(AtlasVertex vertex) throws AtlasBaseException {
        AtlasStructDef ret = null;

        if (vertex != null && typeDefStore.isTypeVertex(vertex, TypeCategory.STRUCT)) {
            ret = toStructDef(vertex, new AtlasStructDef(), typeDefStore);
        }

        return ret;
    }

    public static void updateVertexPreCreate(AtlasStructDef structDef, AtlasStructType structType,
                                             AtlasVertex vertex, AtlasTypeDefGraphStoreV1 typeDefStore) {
        List<String> attrNames = new ArrayList<>(structDef.getAttributeDefs().size());

        for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
            String propertyKey = AtlasGraphUtilsV1.getPropertyKey(structDef, attributeDef.getName());

            AtlasGraphUtilsV1.setProperty(vertex, propertyKey, toJsonFromAttributeDef(attributeDef, structType));

            attrNames.add(attributeDef.getName());
        }
        AtlasGraphUtilsV1.setProperty(vertex, AtlasGraphUtilsV1.getPropertyKey(structDef), attrNames);
    }

    public static void updateVertexPreUpdate(AtlasStructDef structDef, AtlasStructType structType,
                                             AtlasVertex vertex, AtlasTypeDefGraphStoreV1 typeDefStore)
        throws AtlasBaseException {

        List<String> attrNames = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(structDef.getAttributeDefs())) {
            for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                attrNames.add(attributeDef.getName());
            }
        }

        List<String> currAttrNames = vertex.getProperty(AtlasGraphUtilsV1.getPropertyKey(structDef), List.class);

        // delete attributes that are not present in updated structDef
        if (CollectionUtils.isNotEmpty(currAttrNames)) {
            for (String currAttrName : currAttrNames) {
                if (!attrNames.contains(currAttrName)) {
                    throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_DELETION_NOT_SUPPORTED,
                            structDef.getName(), currAttrName);
                }
            }
        }

        typeDefStore.updateTypeVertex(structDef, vertex);

        // add/update attributes that are present in updated structDef
        if (CollectionUtils.isNotEmpty(structDef.getAttributeDefs())) {
            for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
                if (CollectionUtils.isEmpty(currAttrNames) || !currAttrNames.contains(attributeDef.getName())) {
                    // new attribute - only allow if optional
                    if (!attributeDef.getIsOptional()) {
                        throw new AtlasBaseException(AtlasErrorCode.CANNOT_ADD_MANDATORY_ATTRIBUTE, structDef.getName(), attributeDef.getName());
                    }
                }

                String propertyKey = AtlasGraphUtilsV1.getPropertyKey(structDef, attributeDef.getName());

                AtlasGraphUtilsV1.setProperty(vertex, propertyKey, toJsonFromAttributeDef(attributeDef, structType));
            }
        }

        AtlasGraphUtilsV1.setProperty(vertex, AtlasGraphUtilsV1.getPropertyKey(structDef), attrNames);
    }

    public static void updateVertexAddReferences(AtlasStructDef structDef, AtlasVertex vertex,
                                                 AtlasTypeDefGraphStoreV1 typeDefStore) throws AtlasBaseException {
        for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
            addReferencesForAttribute(vertex, attributeDef, typeDefStore);
        }
    }

    public static AtlasStructDef toStructDef(AtlasVertex vertex, AtlasStructDef structDef,
                                             AtlasTypeDefGraphStoreV1 typeDefStore) throws AtlasBaseException {
        AtlasStructDef ret = (structDef != null) ? structDef :new AtlasStructDef();

        typeDefStore.vertexToTypeDef(vertex, ret);

        List<AtlasAttributeDef> attributeDefs = new ArrayList<>();
        List<String> attrNames = vertex.getProperty(AtlasGraphUtilsV1.getPropertyKey(ret), List.class);

        if (CollectionUtils.isNotEmpty(attrNames)) {
            for (String attrName : attrNames) {
                String propertyKey = AtlasGraphUtilsV1.getPropertyKey(ret, attrName);
                String attribJson  = vertex.getProperty(propertyKey, String.class);

                attributeDefs.add(toAttributeDefFromJson(structDef, AtlasType.fromJson(attribJson, Map.class),
                                  typeDefStore));
            }
        }
        ret.setAttributeDefs(attributeDefs);

        return ret;
    }

    private static void addReferencesForAttribute(AtlasVertex vertex, AtlasAttributeDef attributeDef,
                                                  AtlasTypeDefGraphStoreV1 typeDefStore) throws AtlasBaseException {
        Set<String> referencedTypeNames = AtlasTypeUtil.getReferencedTypeNames(attributeDef.getTypeName());

        String typeName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);

        for (String referencedTypeName : referencedTypeNames) {
            if (!AtlasTypeUtil.isBuiltInType(referencedTypeName)) {
                AtlasVertex referencedTypeVertex = typeDefStore.findTypeVertexByName(referencedTypeName);

                if (referencedTypeVertex == null) {
                    throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPE, referencedTypeName, typeName, attributeDef.getName());
                }

                String label = AtlasGraphUtilsV1.getEdgeLabel(typeName, attributeDef.getName());

                typeDefStore.getOrCreateEdge(vertex, referencedTypeVertex, label);
            }
        }
    }

    private static String toJsonFromAttributeDef(AtlasAttributeDef attributeDef, AtlasStructType structType) {
        boolean isForeignKey      = structType.isForeignKeyAttribute(attributeDef.getName());
        boolean isMappedFromRef   = structType.isMappedFromRefAttribute(attributeDef.getName());
        String  reverseAttribName = null;

        if (isForeignKey) { // check if the referenced entity has foreignKeyRef to this attribute
            AtlasType attribType = structType.getAttributeType(attributeDef.getName());

            if (attribType.getTypeCategory() == org.apache.atlas.model.TypeCategory.ARRAY) {
                attribType = ((AtlasArrayType)attribType).getElementType();
            }

            if (attribType.getTypeCategory() == org.apache.atlas.model.TypeCategory.ENTITY) {
                reverseAttribName = ((AtlasStructType)attribType).getMappedFromRefAttribute(structType.getTypeName(),
                                                                                            attributeDef.getName());
            }
        }

        boolean isComposite = isMappedFromRef || (isForeignKey && StringUtils.isBlank(reverseAttribName));

        Map<String, Object> attribInfo = new HashMap<>();

        attribInfo.put("name", attributeDef.getName());
        attribInfo.put("dataType", attributeDef.getTypeName());
        attribInfo.put("isUnique", attributeDef.getIsUnique());
        attribInfo.put("isIndexable", attributeDef.getIsIndexable());
        attribInfo.put("isComposite", isComposite);
        attribInfo.put("reverseAttributeName", reverseAttribName);

        final int lower;
        final int upper;

        if (attributeDef.getCardinality() == AtlasAttributeDef.Cardinality.SINGLE) {
            lower = attributeDef.getIsOptional() ? 0 : 1;
            upper = 1;
        } else {
            if(attributeDef.getIsOptional()) {
                lower = 0;
            } else {
                lower = attributeDef.getValuesMinCount() < 1 ? 1 : attributeDef.getValuesMinCount();
            }

            upper = attributeDef.getValuesMaxCount() < 2 ? Integer.MAX_VALUE : attributeDef.getValuesMaxCount();
        }

        Map<String, Object> multiplicity = new HashMap<>();
        multiplicity.put("lower", lower);
        multiplicity.put("upper", upper);
        multiplicity.put("isUnique", AtlasAttributeDef.Cardinality.SET.equals(attributeDef.getCardinality()));

        attribInfo.put("multiplicity", AtlasType.toJson(multiplicity));

        return AtlasType.toJson(attribInfo);
    }

    private static AtlasAttributeDef toAttributeDefFromJson(AtlasStructDef           structDef,
                                                            Map                      attribInfo,
                                                            AtlasTypeDefGraphStoreV1 typeDefStore)
        throws AtlasBaseException {
        AtlasAttributeDef ret = new AtlasAttributeDef();

        ret.setName((String) attribInfo.get("name"));
        ret.setTypeName((String) attribInfo.get("dataType"));
        ret.setIsUnique((Boolean) attribInfo.get("isUnique"));
        ret.setIsIndexable((Boolean) attribInfo.get("isIndexable"));

        String attrTypeName = ret.getTypeName();

        if (AtlasTypeUtil.isArrayType(attrTypeName)) {
            Set<String> typeNames = AtlasTypeUtil.getReferencedTypeNames(ret.getTypeName());

            if (typeNames.size() > 0) {
                attrTypeName = typeNames.iterator().next();
            }
        }

        if (!AtlasTypeUtil.isBuiltInType(attrTypeName)) {
            AtlasVertex attributeType = typeDefStore.findTypeVertexByName(attrTypeName);

            // check for isComposite/reverseAttributeName for entity types
            if (attributeType != null && typeDefStore.isTypeVertex(attributeType, TypeCategory.CLASS)) {
                String  reverseAttribName = (String) attribInfo.get("reverseAttributeName");
                Boolean isComposite       = (Boolean) attribInfo.get("isComposite");

                if (StringUtils.isNotBlank(reverseAttribName) || isComposite) {
                    if (AtlasTypeUtil.isMapType(attrTypeName)) {
                        throw new AtlasBaseException(AtlasErrorCode.CONSTRAINT_NOT_SUPPORTED_ON_MAP_TYPE,
                                structDef.getName(), ret.getName(), attrTypeName);
                    }

                    String       refAttributeName = null;
                    List<String> attrNames        = attributeType.getProperty(
                                                            AtlasGraphUtilsV1.getPropertyKey(attrTypeName), List.class);

                    if (CollectionUtils.isNotEmpty(attrNames)) {
                        for (String attrName : attrNames) {
                            String attribJson = attributeType.getProperty(
                                                AtlasGraphUtilsV1.getPropertyKey(attrTypeName, attrName), String.class);

                            Map    refAttrInfo            = AtlasType.fromJson(attribJson, Map.class);
                            String refAttribType          = (String) refAttrInfo.get("dataType");
                            String refAttribRevAttribName = (String) refAttrInfo.get("reverseAttributeName");

                            if (StringUtils.equals(refAttribType, structDef.getName()) &&
                                StringUtils.equals(refAttribRevAttribName, ret.getName())) {
                                refAttributeName = (String) refAttrInfo.get("name");

                                break;
                            }
                        }
                    }

                    if (isComposite) {
                        if (StringUtils.isNotBlank(refAttributeName)) { // ex: hive_table.columns, hive_column.table
                            Map<String, Object> params = new HashMap<>();
                            params.put(AtlasConstraintDef.CONSTRAINT_PARAM_REF_ATTRIBUTE, refAttributeName);

                            ret.addConstraint(new AtlasConstraintDef(CONSTRAINT_TYPE_MAPPED_FROM_REF, params));
                        } else { // ex: hive_table.partitionKeys, with no reverseAttribute-reference
                            ret.addConstraint(new AtlasConstraintDef(CONSTRAINT_TYPE_FOREIGN_KEY));
                        }
                    }

                    if (StringUtils.isNotBlank(reverseAttribName)) { // ex: hive_column.table
                        Map<String, Object> params = new HashMap<>();
                        params.put(CONSTRAINT_PARAM_ON_DELETE, CONSTRAINT_PARAM_VAL_CASCADE);

                        ret.addConstraint(new AtlasConstraintDef(CONSTRAINT_TYPE_FOREIGN_KEY, params));
                    }
                }
            }
        }

        Map     multiplicity = AtlasType.fromJson((String) attribInfo.get("multiplicity"), Map.class);
        Number  minCount     = (Number) multiplicity.get("lower");
        Number  maxCount     = (Number) multiplicity.get("upper");
        Boolean isUnique     = (Boolean) multiplicity.get("isUnique");

        if (minCount == null || minCount.intValue() == 0) {
            ret.setIsOptional(true);
            ret.setValuesMinCount(0);
        } else {
            ret.setIsOptional(false);
            ret.setValuesMinCount(minCount.intValue());
        }

        if (maxCount == null || maxCount.intValue() < 2) {
            ret.setCardinality(AtlasAttributeDef.Cardinality.SINGLE);
            ret.setValuesMaxCount(1);
        } else {
            if (isUnique == null || isUnique == Boolean.FALSE) {
                ret.setCardinality(AtlasAttributeDef.Cardinality.LIST);
            } else {
                ret.setCardinality(AtlasAttributeDef.Cardinality.SET);
            }

            ret.setValuesMaxCount(maxCount.intValue());
        }

        return ret;
    }
}
