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
import org.apache.atlas.model.typedef.AtlasRelationshipDef;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.RelationshipCategory;
import org.apache.atlas.model.typedef.AtlasRelationshipDef.PropagateTags;
import org.apache.atlas.model.typedef.AtlasRelationshipEndPointDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasRelationshipDefStore;
import org.apache.atlas.type.AtlasRelationshipType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * RelationshipDef store in v1 format.
 */
public class AtlasRelationshipDefStoreV1 extends AtlasAbstractDefStoreV1 implements AtlasRelationshipDefStore {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasRelationshipDefStoreV1.class);

    public AtlasRelationshipDefStoreV1(AtlasTypeDefGraphStoreV1 typeDefStore, AtlasTypeRegistry typeRegistry) {
        super(typeDefStore, typeRegistry);
    }

    @Override
    public AtlasVertex preCreate(AtlasRelationshipDef relationshipDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.preCreate({})", relationshipDef);
        }

        validateType(relationshipDef);

        AtlasType type = typeRegistry.getType(relationshipDef.getName());

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.RELATIONSHIP) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, relationshipDef.getName(), TypeCategory.RELATIONSHIP.name());
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByName(relationshipDef.getName());

        if (ret != null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_ALREADY_EXISTS, relationshipDef.getName());
        }

        ret = typeDefStore.createTypeVertex(relationshipDef);

        updateVertexPreCreate(relationshipDef, (AtlasRelationshipType) type, ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.preCreate({}): {}", relationshipDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasRelationshipDef create(AtlasRelationshipDef relationshipDef, Object preCreateResult)
            throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.create({}, {})", relationshipDef, preCreateResult);
        }

        AtlasVertex vertex;

        if (preCreateResult == null || !(preCreateResult instanceof AtlasVertex)) {
            vertex = preCreate(relationshipDef);
        } else {
            vertex = (AtlasVertex) preCreateResult;
        }

        AtlasRelationshipDef ret = toRelationshipDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.create({}, {}): {}", relationshipDef, preCreateResult, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasRelationshipDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.getAll()");
        }

        List<AtlasRelationshipDef> ret = new ArrayList<>();
        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(TypeCategory.RELATIONSHIP);

        while (vertices.hasNext()) {
            ret.add(toRelationshipDef(vertices.next()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.getAll(): count={}", ret.size());
        }

        return ret;
    }

    @Override
    public AtlasRelationshipDef getByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.getByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.RELATIONSHIP);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, TypeCategory.class);

        AtlasRelationshipDef ret = toRelationshipDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.getByName({}): {}", name, ret);
        }

        return ret;
    }

    @Override
    public AtlasRelationshipDef getByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.getByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.RELATIONSHIP);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        AtlasRelationshipDef ret = toRelationshipDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.getByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasRelationshipDef update(AtlasRelationshipDef relationshipDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.update({})", relationshipDef);
        }

        validateType(relationshipDef);

        AtlasRelationshipDef ret = StringUtils.isNotBlank(relationshipDef.getGuid())
                ? updateByGuid(relationshipDef.getGuid(), relationshipDef)
                : updateByName(relationshipDef.getName(), relationshipDef);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.update({}): {}", relationshipDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasRelationshipDef updateByName(String name, AtlasRelationshipDef relationshipDef)
            throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.updateByName({}, {})", name, relationshipDef);
        }

        validateType(relationshipDef);

        AtlasType type = typeRegistry.getType(relationshipDef.getName());

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.RELATIONSHIP) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, relationshipDef.getName(), TypeCategory.RELATIONSHIP.name());
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.RELATIONSHIP);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        updateVertexPreUpdate(relationshipDef, (AtlasRelationshipType) type, vertex);

        AtlasRelationshipDef ret = toRelationshipDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.updateByName({}, {}): {}", name, relationshipDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasRelationshipDef updateByGuid(String guid, AtlasRelationshipDef relationshipDef)
            throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.updateByGuid({})", guid);
        }

        validateType(relationshipDef);

        AtlasType type = typeRegistry.getTypeByGuid(guid);

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.RELATIONSHIP) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, relationshipDef.getName(), TypeCategory.RELATIONSHIP.name());
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.RELATIONSHIP);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        updateVertexPreUpdate(relationshipDef, (AtlasRelationshipType) type, vertex);
        // TODO delete / create edges to entitytypes
        AtlasRelationshipDef ret = toRelationshipDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.updateByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasVertex preDeleteByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.preDeleteByName({})", name);
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByNameAndCategory(name, TypeCategory.RELATIONSHIP);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        if (AtlasGraphUtilsV1.typeHasInstanceVertex(name)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_HAS_REFERENCES, name);
        }

        // TODO delete the edges to the other types

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.preDeleteByName({}): {}", name, ret);
        }

        return ret;
    }

    @Override
    public void deleteByName(String name, Object preDeleteResult) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.deleteByName({}, {})", name, preDeleteResult);
        }

        AtlasVertex vertex;

        if (preDeleteResult == null || !(preDeleteResult instanceof AtlasVertex)) {
            vertex = preDeleteByName(name);
        } else {
            vertex = (AtlasVertex) preDeleteResult;
        }

        typeDefStore.deleteTypeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.deleteByName({}, {})", name, preDeleteResult);
        }
    }

    @Override
    public AtlasVertex preDeleteByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.preDeleteByGuid({})", guid);
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByGuidAndCategory(guid, TypeCategory.RELATIONSHIP);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        String typeName = AtlasGraphUtilsV1.getProperty(ret, Constants.TYPENAME_PROPERTY_KEY, String.class);

        if (AtlasGraphUtilsV1.typeHasInstanceVertex(typeName)) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_HAS_REFERENCES, typeName);
        }

        // TODO delete the edges to the other types

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.preDeleteByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public void deleteByGuid(String guid, Object preDeleteResult) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasRelationshipDefStoreV1.deleteByGuid({}, {})", guid, preDeleteResult);
        }

        AtlasVertex vertex;

        if (preDeleteResult == null || !(preDeleteResult instanceof AtlasVertex)) {
            vertex = preDeleteByGuid(guid);
        } else {
            vertex = (AtlasVertex) preDeleteResult;
        }

        typeDefStore.deleteTypeVertex(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasRelationshipDefStoreV1.deleteByGuid({}, {})", guid, preDeleteResult);
        }
    }

    private void updateVertexPreCreate(AtlasRelationshipDef relationshipDef, AtlasRelationshipType relationshipType,
                                       AtlasVertex vertex) throws AtlasBaseException {
        AtlasStructDefStoreV1.updateVertexPreCreate(relationshipDef, relationshipType, vertex, typeDefStore);
        // Update endpoints
        vertex.setProperty(Constants.RELATIONSHIPTYPE_ENDPOINT1_KEY, AtlasType.toJson(relationshipDef.getEndPointDef1()));
        vertex.setProperty(Constants.RELATIONSHIPTYPE_ENDPOINT2_KEY, AtlasType.toJson(relationshipDef.getEndPointDef2()));
        // Update RelationshipCategory
        vertex.setProperty(Constants.RELATIONSHIPTYPE_CATEGORY_KEY, relationshipDef.getRelationshipCategory().name());
        vertex.setProperty(Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, relationshipDef.getPropagateTags().name());
    }

    private void updateVertexPreUpdate(AtlasRelationshipDef relationshipDef, AtlasRelationshipType relationshipType,
                                       AtlasVertex vertex) throws AtlasBaseException {
        AtlasStructDefStoreV1.updateVertexPreUpdate(relationshipDef, relationshipType, vertex, typeDefStore);
        vertex.setProperty(Constants.RELATIONSHIPTYPE_ENDPOINT1_KEY, AtlasType.toJson(relationshipDef.getEndPointDef1()));
        vertex.setProperty(Constants.RELATIONSHIPTYPE_ENDPOINT2_KEY, AtlasType.toJson(relationshipDef.getEndPointDef2()));
        // Update RelationshipCategory
        vertex.setProperty(Constants.RELATIONSHIPTYPE_CATEGORY_KEY, relationshipDef.getRelationshipCategory().name());
        vertex.setProperty(Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, relationshipDef.getPropagateTags().name());
    }

    private AtlasRelationshipDef toRelationshipDef(AtlasVertex vertex) throws AtlasBaseException {
        AtlasRelationshipDef ret = null;

        if (vertex != null && typeDefStore.isTypeVertex(vertex, TypeCategory.RELATIONSHIP)) {
            String name         = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);
            String description  = vertex.getProperty(Constants.TYPEDESCRIPTION_PROPERTY_KEY, String.class);
            String version      = vertex.getProperty(Constants.TYPEVERSION_PROPERTY_KEY, String.class);
            String endPoint1Str = vertex.getProperty(Constants.RELATIONSHIPTYPE_ENDPOINT1_KEY, String.class);
            String endPoint2Str = vertex.getProperty(Constants.RELATIONSHIPTYPE_ENDPOINT2_KEY, String.class);
            String relationStr  = vertex.getProperty(Constants.RELATIONSHIPTYPE_CATEGORY_KEY, String.class);
            String propagateStr = vertex.getProperty(Constants.RELATIONSHIPTYPE_TAG_PROPAGATION_KEY, String.class);

            // set the endpoints
            AtlasRelationshipEndPointDef endPointDef1 = AtlasType.fromJson(endPoint1Str, AtlasRelationshipEndPointDef.class);
            AtlasRelationshipEndPointDef endPointDef2 = AtlasType.fromJson(endPoint2Str, AtlasRelationshipEndPointDef.class);

            // set the relationship Category
            RelationshipCategory relationshipCategory = null;
            for (RelationshipCategory value : RelationshipCategory.values()) {
                if (value.name().equals(relationStr)) {
                    relationshipCategory = value;
                }
            }

            // set the propagateTags
            PropagateTags propagateTags = null;
            for (PropagateTags value : PropagateTags.values()) {
                if (value.name().equals(propagateStr)) {
                    propagateTags = value;
                }
            }

            ret = new AtlasRelationshipDef(name, description, version, relationshipCategory,  propagateTags, endPointDef1, endPointDef2);

            // add in the attributes
            AtlasStructDefStoreV1.toStructDef(vertex, ret, typeDefStore);
        }

        return ret;
    }

}
