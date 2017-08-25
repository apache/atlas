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
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEnumDef.AtlasEnumElementDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * EnumDef store in v1 format.
 */
class AtlasEnumDefStoreV1 extends AtlasAbstractDefStoreV1<AtlasEnumDef> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasEnumDefStoreV1.class);

    public AtlasEnumDefStoreV1(AtlasTypeDefGraphStoreV1 typeDefStore, AtlasTypeRegistry typeRegistry) {
        super(typeDefStore, typeRegistry);
    }

    @Override
    public AtlasVertex preCreate(AtlasEnumDef enumDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
          LOG.debug("==> AtlasEnumDefStoreV1.preCreate({})", enumDef);
        }

        validateType(enumDef);

        AtlasVertex vertex = typeDefStore.findTypeVertexByName(enumDef.getName());

        if (vertex != null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_ALREADY_EXISTS, enumDef.getName());
        }

        vertex = typeDefStore.createTypeVertex(enumDef);

        toVertex(enumDef, vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEnumDefStoreV1.preCreate({}): {}", enumDef, vertex);
        }

        return vertex;
    }

    @Override
    public AtlasEnumDef create(AtlasEnumDef enumDef, AtlasVertex preCreateResult) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
          LOG.debug("==> AtlasEnumDefStoreV1.create({}, {})", enumDef, preCreateResult);
        }

        AtlasVertex vertex = (preCreateResult == null) ? preCreate(enumDef) : preCreateResult;

        AtlasEnumDef ret = toEnumDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEntityDefStoreV1.create({}, {}): {}", enumDef, preCreateResult, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasEnumDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEnumDefStoreV1.getAll()");
        }

        List<AtlasEnumDef> ret = new ArrayList<>();

        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(TypeCategory.ENUM);
        while (vertices.hasNext()) {
            ret.add(toEnumDef(vertices.next()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEnumDefStoreV1.getAll(): count={}", ret.size());
        }

        return ret;
    }

    @Override
    public AtlasEnumDef getByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEnumDefStoreV1.getByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.ENUM);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, TypeCategory.class);

        AtlasEnumDef ret = toEnumDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEnumDefStoreV1.getByName({}): {}", name, ret);
        }

        return ret;
    }

    @Override
    public AtlasEnumDef getByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEnumDefStoreV1.getByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.ENUM);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        AtlasEnumDef ret = toEnumDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEnumDefStoreV1.getByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasEnumDef update(AtlasEnumDef enumDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEnumDefStoreV1.update({})", enumDef);
        }

        validateType(enumDef);

        AtlasEnumDef ret = StringUtils.isNotBlank(enumDef.getGuid()) ? updateByGuid(enumDef.getGuid(), enumDef)
                                                                     : updateByName(enumDef.getName(), enumDef);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEnumDefStoreV1.update({}): {}", enumDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasEnumDef updateByName(String name, AtlasEnumDef enumDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEnumDefStoreV1.updateByName({}, {})", name, enumDef);
        }

        validateType(enumDef);

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.ENUM);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        typeDefStore.updateTypeVertex(enumDef, vertex);

        toVertex(enumDef, vertex);

        AtlasEnumDef ret = toEnumDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEnumDefStoreV1.updateByName({}, {}): {}", name, enumDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasEnumDef updateByGuid(String guid, AtlasEnumDef enumDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasEnumDefStoreV1.updateByGuid({})", guid);
        }

        validateType(enumDef);

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.ENUM);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        typeDefStore.updateTypeVertex(enumDef, vertex);

        toVertex(enumDef, vertex);

        AtlasEnumDef ret = toEnumDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasEnumDefStoreV1.updateByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasVertex preDeleteByName(String name) throws AtlasBaseException {
        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.ENUM);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        return vertex;
    }

    @Override
    public AtlasVertex preDeleteByGuid(String guid) throws AtlasBaseException {
        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.ENUM);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        return vertex;
    }

    private void toVertex(AtlasEnumDef enumDef, AtlasVertex vertex) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(enumDef.getElementDefs())) {
            throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE, enumDef.getName(), "values");
        }

        List<String> values = new ArrayList<>(enumDef.getElementDefs().size());

        for (AtlasEnumElementDef element : enumDef.getElementDefs()) {
            // Validate the enum element
            if (StringUtils.isEmpty(element.getValue()) || null == element.getOrdinal()) {
                throw new AtlasBaseException(AtlasErrorCode.MISSING_MANDATORY_ATTRIBUTE, enumDef.getName(), "elementValue");
            }

            String elemKey = AtlasGraphUtilsV1.getTypeDefPropertyKey(enumDef, element.getValue());

            AtlasGraphUtilsV1.setProperty(vertex, elemKey, element.getOrdinal());

            if (StringUtils.isNotBlank(element.getDescription())) {
                String descKey = AtlasGraphUtilsV1.getTypeDefPropertyKey(elemKey, "description");

                AtlasGraphUtilsV1.setProperty(vertex, descKey, element.getDescription());
            }

            values.add(element.getValue());
        }
        AtlasGraphUtilsV1.setProperty(vertex, AtlasGraphUtilsV1.getTypeDefPropertyKey(enumDef), values);
    }

    private AtlasEnumDef toEnumDef(AtlasVertex vertex) {
        AtlasEnumDef ret = null;

        if (vertex != null && typeDefStore.isTypeVertex(vertex, TypeCategory.ENUM)) {
            ret = toEnumDef(vertex, new AtlasEnumDef(), typeDefStore);
        }

        return ret;
    }

    private static AtlasEnumDef toEnumDef(AtlasVertex vertex, AtlasEnumDef enumDef, AtlasTypeDefGraphStoreV1 typeDefStore) {
        AtlasEnumDef ret = enumDef != null ? enumDef : new AtlasEnumDef();

        typeDefStore.vertexToTypeDef(vertex, ret);

        List<AtlasEnumElementDef> elements = new ArrayList<>();
        List<String> elemValues = vertex.getProperty(AtlasGraphUtilsV1.getTypeDefPropertyKey(ret), List.class);
        for (String elemValue : elemValues) {
            String elemKey = AtlasGraphUtilsV1.getTypeDefPropertyKey(ret, elemValue);
            String descKey = AtlasGraphUtilsV1.getTypeDefPropertyKey(elemKey, "description");

            Integer ordinal = AtlasGraphUtilsV1.getProperty(vertex, elemKey, Integer.class);
            String  desc    = AtlasGraphUtilsV1.getProperty(vertex, descKey, String.class);

            elements.add(new AtlasEnumElementDef(elemValue, desc, ordinal));
        }
        ret.setElementDefs(elements);

        return ret;
    }
}
