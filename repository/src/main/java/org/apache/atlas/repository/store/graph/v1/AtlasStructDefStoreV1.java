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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef;

import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_ON_DELETE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_PARAM_VAL_CASCADE;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_FOREIGN_KEY;
import static org.apache.atlas.model.typedef.AtlasStructDef.AtlasConstraintDef.CONSTRAINT_TYPE_MAPPED_FROM_REF;

import org.apache.atlas.model.typedef.AtlasStructDef.AtlasStructDefs;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasStructDefStore;
import org.apache.atlas.repository.util.FilterUtil;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasEntityType;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * StructDef store in v1 format.
 */
public class AtlasStructDefStoreV1 implements AtlasStructDefStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasStructDefStoreV1.class);

    private final AtlasTypeDefGraphStoreV1 typeDefStore;

    public AtlasStructDefStoreV1(AtlasTypeDefGraphStoreV1 typeDefStore) {
        super();

        this.typeDefStore = typeDefStore;
    }

    @Override
    public AtlasStructDef create(AtlasStructDef structDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.create({})", structDef);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByName(structDef.getName());

        if (vertex != null) {
            throw new AtlasBaseException(structDef.getName() + ": type already exists");
        }

        vertex = typeDefStore.createTypeVertex(structDef);

        toVertex(structDef, vertex, typeDefStore);

        AtlasStructDef ret = toStructDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.create({}): {}", structDef, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasStructDef> create(List<AtlasStructDef> structDefs) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.create({})", structDefs);
        }
        List<AtlasStructDef> structDefList = new LinkedList<>();
        for (AtlasStructDef structDef : structDefs) {
            try {
                AtlasStructDef atlasStructDef = create(structDef);
                structDefList.add(atlasStructDef);
            } catch (AtlasBaseException baseException) {
                LOG.error("Failed to create {}", structDef);
                LOG.error("Exception: {}", baseException);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.create({}, {})", structDefs, structDefList);
        }
        return structDefList;
    }

    @Override
    public List<AtlasStructDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.getAll()");
        }

        List<AtlasStructDef> structDefs = new LinkedList<>();
        Iterator<AtlasVertex> verticesByCategory = typeDefStore.findTypeVerticesByCategory(TypeCategory.STRUCT);
        while (verticesByCategory.hasNext()) {
            AtlasStructDef atlasStructDef = toStructDef(verticesByCategory.next());
            structDefs.add(atlasStructDef);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.getAll()");
        }
        return structDefs;
    }

    @Override
    public AtlasStructDef getByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.getByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.STRUCT);

        if (vertex == null) {
            throw new AtlasBaseException("no structDef exists with name " + name);
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
            throw new AtlasBaseException("no structDef exists with guid " + guid);
        }

        AtlasStructDef ret = toStructDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.getByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasStructDef updateByName(String name, AtlasStructDef structDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.updateByName({}, {})", name, structDef);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.STRUCT);

        if (vertex == null) {
            throw new AtlasBaseException("no structDef exists with name " + name);
        }

        toVertex(structDef, vertex);

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

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.STRUCT);

        if (vertex == null) {
            throw new AtlasBaseException("no structDef exists with guid " + guid);
        }

        toVertex(structDef, vertex);

        AtlasStructDef ret = toStructDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.updateByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasStructDef> update(List<AtlasStructDef> structDefs) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.update({})", structDefs);
        }

        List<AtlasStructDef> updatedDefs = new ArrayList<>();

        for (AtlasStructDef structDef : structDefs) {
            try {
                AtlasStructDef updatedDef = updateByName(structDef.getName(), structDef);
                updatedDefs.add(updatedDef);
            } catch (AtlasBaseException ex) {}
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.update({}): {}", structDefs, updatedDefs);
        }

        return updatedDefs;
    }

    @Override
    public void deleteByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.deleteByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.STRUCT);

        if (vertex == null) {
            throw new AtlasBaseException("no structDef exists with name " + name);
        }

        typeDefStore.deleteTypeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.deleteByName({})", name);
        }
    }

    @Override
    public void deleteByNames(List<String> names) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.deleteByNames({})", names);
        }

        List<AtlasStructDef> updatedDefs = new ArrayList<>();

        for (String name : names) {
            try {
                deleteByName(name);
            } catch (AtlasBaseException ex) {}
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.deleteByNames({})", names);
        }
    }

    @Override
    public void deleteByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.deleteByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.STRUCT);

        if (vertex == null) {
            throw new AtlasBaseException("no structDef exists with guid " + guid);
        }

        typeDefStore.deleteTypeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.deleteByGuid({})", guid);
        }
    }

    @Override
    public void deleteByGuids(List<String> guids) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.deleteByGuids({})", guids);
        }

        List<AtlasStructDef> updatedDefs = new ArrayList<>();

        for (String guid : guids) {
            try {
                deleteByGuid(guid);
            } catch (AtlasBaseException ex) {}
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.deleteByGuids({})", guids);
        }
    }

    @Override
    public AtlasStructDefs search(SearchFilter filter) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasStructDefStoreV1.search({})", filter);
        }

        List<AtlasStructDef> structDefs = new ArrayList<AtlasStructDef>();

        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(TypeCategory.STRUCT);

        while(vertices.hasNext()) {
            AtlasVertex       AtlasVertex  = vertices.next();
            AtlasStructDef structDef = toStructDef(AtlasVertex);

            if (structDef != null) {
                structDefs.add(structDef);
            }
        }

        if (CollectionUtils.isNotEmpty(structDefs)) {
            CollectionUtils.filter(structDefs, FilterUtil.getPredicateFromSearchFilter(filter));
        }

        AtlasStructDefs ret = new AtlasStructDefs(structDefs);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasStructDefStoreV1.search({}): {}", filter, ret);
        }

        return ret;
    }

    private void toVertex(AtlasStructDef structDef, AtlasVertex vertex) {
        toVertex(structDef, vertex, typeDefStore);
    }

    private AtlasStructDef toStructDef(AtlasVertex vertex) throws AtlasBaseException {
        AtlasStructDef ret = null;

        if (vertex != null && typeDefStore.isTypeVertex(vertex, TypeCategory.STRUCT)) {
            ret = toStructDef(vertex, new AtlasStructDef(), typeDefStore);
        }

        return ret;
    }

    public static void toVertex(AtlasStructDef structDef, AtlasVertex vertex, AtlasTypeDefGraphStoreV1 typeDefStore) {
        AtlasTypeRegistry typeRegistry = typeDefStore.getTypeRegistry();
        AtlasStructType   structType   = (AtlasStructType)typeRegistry.getType(structDef.getName());
        List<String>      attrNames    = new ArrayList<>(structDef.getAttributeDefs().size());

        for (AtlasAttributeDef attributeDef : structDef.getAttributeDefs()) {
            String propertyKey = AtlasGraphUtilsV1.getPropertyKey(structDef, attributeDef.getName());

            AtlasGraphUtilsV1.setProperty(vertex, propertyKey, toJsonFromAttributeDef(attributeDef, structType, typeRegistry));

            attrNames.add(attributeDef.getName());
            addReferencesForAttribute(vertex, attributeDef, typeDefStore);
        }
        AtlasGraphUtilsV1.setProperty(vertex, AtlasGraphUtilsV1.getPropertyKey(structDef), attrNames);
    }

    public static AtlasStructDef toStructDef(AtlasVertex              vertex,
                                             AtlasStructDef           structDef,
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

    private static void addReferencesForAttribute(AtlasVertex              vertex,
                                                  AtlasAttributeDef        attributeDef,
                                                  AtlasTypeDefGraphStoreV1 typeDefStore) {
        Set<String> referencedTypeNames = AtlasTypeUtil.getReferencedTypeNames(attributeDef.getTypeName());

        String typeName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);

        for (String referencedTypeName : referencedTypeNames) {
            if (!AtlasTypeUtil.isBuiltInType(referencedTypeName)) {
                AtlasVertex referencedTypeAtlasVertex = typeDefStore.findTypeVertexByName(referencedTypeName);

                if (referencedTypeAtlasVertex == null) {
                    // create atlasVertex?
                }

                if (referencedTypeAtlasVertex != null) {
                    String label = AtlasGraphUtilsV1.getEdgeLabel(typeName, attributeDef.getName());

                    typeDefStore.getOrCreateEdge(vertex, referencedTypeAtlasVertex, label);
                }
            }
        }
    }

    private static String toJsonFromAttributeDef(AtlasAttributeDef attributeDef,
                                                 AtlasStructType   structType,
                                                 AtlasTypeRegistry typeRegistry) {
        boolean isForeignKey      = structType.isForeignKeyAttribute(attributeDef.getName());
        boolean isMappedFromRef   = structType.isMappedFromRefAttribute(attributeDef.getName());
        String  reverseAttribName = null;

        if (isForeignKey) { // check if the referenced entity has foreignKeyRef to this attribute
            AtlasType attribType = typeRegistry.getType(attributeDef.getTypeName());

            if (attribType instanceof AtlasArrayType) {
                attribType = ((AtlasArrayType)attribType).getElementType();
            }

            if (attribType instanceof AtlasEntityType) {
                reverseAttribName = ((AtlasStructType)attribType).getMappedFromRefAttribute(structType.getTypeName(),
                                                                                            attributeDef.getName());
            }
        }

        boolean isComposite = isMappedFromRef || (isForeignKey && StringUtils.isBlank(reverseAttribName));

        Map<String, Object> attribInfo = new HashMap<String, Object>();

        attribInfo.put("name", attributeDef.getName());
        attribInfo.put("dataType", attributeDef.getTypeName());
        attribInfo.put("isUnique", attributeDef.isUnique());
        attribInfo.put("isIndexable", attributeDef.isIndexable());
        attribInfo.put("isComposite", isComposite);
        attribInfo.put("reverseAttributeName", reverseAttribName);
        Map<String, Object> multiplicity = new HashMap<String, Object>();
        multiplicity.put("lower", attributeDef.getValuesMinCount());
        multiplicity.put("upper", attributeDef.getValuesMaxCount());
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
        ret.setUnique((Boolean) attribInfo.get("isUnique"));
        ret.setIndexable((Boolean) attribInfo.get("isIndexable"));

        String attrTypeName = ret.getTypeName();

        if (AtlasTypeUtil.isArrayType(attrTypeName)) {
            Set<String> typeNames = AtlasTypeUtil.getReferencedTypeNames(ret.getTypeName());

            if (typeNames.size() > 0) {
                attrTypeName = typeNames.iterator().next();
            }
        }

        if (! AtlasTypeUtil.isBuiltInType(attrTypeName)) {
            AtlasVertex attributeType = typeDefStore.findTypeVertexByName(attrTypeName);

            // check for isComposite/reverseAttributeName for entity types
            if (attributeType != null && typeDefStore.isTypeVertex(attributeType, TypeCategory.CLASS)) {
                String reverseAttribName = (String) attribInfo.get("reverseAttributeName");
                Boolean isComposite      = (Boolean) attribInfo.get("isComposite");

                // find the attribute that refers to this attribute
                if (StringUtils.isNotBlank(reverseAttribName) || isComposite) {
                    if (AtlasTypeUtil.isMapType(attrTypeName)) {
                        throw new AtlasBaseException(structDef.getName() + "." + ret.getName()
                                                     + ": constraints not supported on map type " + attrTypeName);
                    }

                    if (attributeType != null) {
                        String refAttributeName = null;

                        List<String> attrNames = attributeType.getProperty(
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
                                Map<String, Object> params = new HashMap<String, Object>();
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
        }


        Map     multiplicity      = AtlasType.fromJson((String) attribInfo.get("multiplicity"), Map.class);
        Number  minCount          = (Number) multiplicity.get("lower");
        Number  maxCount          = (Number) multiplicity.get("upper");
        Boolean isUnique          = (Boolean) multiplicity.get("isUnique");


        if (minCount == null || minCount.intValue() == 0) {
            ret.setOptional(true);
            ret.setValuesMinCount(0);
        } else {
            ret.setOptional(false);
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
