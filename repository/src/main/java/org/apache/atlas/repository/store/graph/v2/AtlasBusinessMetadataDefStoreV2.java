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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasBusinessMetadataType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.atlas.model.typedef.AtlasBusinessMetadataDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES;

public class AtlasBusinessMetadataDefStoreV2 extends AtlasAbstractDefStoreV2<AtlasBusinessMetadataDef> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasBusinessMetadataDefStoreV2.class);

    @Inject
    public AtlasBusinessMetadataDefStoreV2(AtlasTypeDefGraphStoreV2 typeDefStore, AtlasTypeRegistry typeRegistry) {
        super(typeDefStore, typeRegistry);
    }

    @Override
    public AtlasVertex preCreate(AtlasBusinessMetadataDef businessMetadataDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.preCreate({})", businessMetadataDef);
        }

        validateType(businessMetadataDef);

        AtlasType type = typeRegistry.getType(businessMetadataDef.getName());

        if (type.getTypeCategory() != TypeCategory.BUSINESS_METADATA) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, businessMetadataDef.getName(),
                    DataTypes.TypeCategory.BUSINESS_METADATA.name());
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByName(businessMetadataDef.getName());

        if (ret != null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_ALREADY_EXISTS, businessMetadataDef.getName());
        }

        ret = typeDefStore.createTypeVertex(businessMetadataDef);

        updateVertexPreCreate(businessMetadataDef, (AtlasBusinessMetadataType) type, ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.preCreate({}): {}", businessMetadataDef, ret);
        }

        return ret;
    }

    @Override
    public void validateType(AtlasBaseTypeDef typeDef) throws AtlasBaseException {
        super.validateType(typeDef);
        AtlasBusinessMetadataDef businessMetadataDef = (AtlasBusinessMetadataDef) typeDef;
        if (CollectionUtils.isNotEmpty(businessMetadataDef.getAttributeDefs())) {
            for (AtlasStructDef.AtlasAttributeDef attributeDef : businessMetadataDef.getAttributeDefs()) {
                if (!isValidName(attributeDef.getName())) {
                    throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_NAME_INVALID_CHARS, attributeDef.getName());
                }
            }
        }
    }

    @Override
    public AtlasBusinessMetadataDef create(AtlasBusinessMetadataDef businessMetadataDef, AtlasVertex preCreateResult) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.create({}, {})", businessMetadataDef, preCreateResult);
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_CREATE, businessMetadataDef), "create businessMetadata-def ", businessMetadataDef.getName());

        AtlasVertex vertex = (preCreateResult == null) ? preCreate(businessMetadataDef) : preCreateResult;

        AtlasBusinessMetadataDef ret = toBusinessMetadataDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.create({}, {}): {}", businessMetadataDef, preCreateResult, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasBusinessMetadataDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDef.getAll()");
        }

        List<AtlasBusinessMetadataDef> ret = new ArrayList<>();

        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(DataTypes.TypeCategory.BUSINESS_METADATA);
        while (vertices.hasNext()) {
            ret.add(toBusinessMetadataDef(vertices.next()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.getAll(): count={}", ret.size());
        }
        return ret;
    }

    @Override
    public AtlasBusinessMetadataDef getByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.getByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, DataTypes.TypeCategory.BUSINESS_METADATA);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, String.class);

        AtlasBusinessMetadataDef ret = toBusinessMetadataDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.getByName({}): {}", name, ret);
        }

        return ret;
    }

    @Override
    public AtlasBusinessMetadataDef getByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.getByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, DataTypes.TypeCategory.BUSINESS_METADATA);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        AtlasBusinessMetadataDef ret = toBusinessMetadataDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.getByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasBusinessMetadataDef update(AtlasBusinessMetadataDef typeDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.update({})", typeDef);
        }

        validateType(typeDef);

        AtlasBusinessMetadataDef ret = StringUtils.isNotBlank(typeDef.getGuid()) ? updateByGuid(typeDef.getGuid(), typeDef)
                : updateByName(typeDef.getName(), typeDef);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.update({}): {}", typeDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasBusinessMetadataDef updateByName(String name, AtlasBusinessMetadataDef typeDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.updateByName({}, {})", name, typeDef);
        }

        AtlasBusinessMetadataDef existingDef = typeRegistry.getBusinessMetadataDefByName(name);

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_UPDATE, existingDef), "update businessMetadata-def ", name);

        validateType(typeDef);

        AtlasType type = typeRegistry.getType(typeDef.getName());

        if (type.getTypeCategory() != TypeCategory.BUSINESS_METADATA) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, typeDef.getName(), DataTypes.TypeCategory.BUSINESS_METADATA.name());
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, DataTypes.TypeCategory.BUSINESS_METADATA);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }


        updateVertexPreUpdate(typeDef, (AtlasBusinessMetadataType)type, vertex);

        AtlasBusinessMetadataDef ret = toBusinessMetadataDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.updateByName({}, {}): {}", name, typeDef, ret);
        }

        return ret;
    }

    public AtlasBusinessMetadataDef updateByGuid(String guid, AtlasBusinessMetadataDef typeDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.updateByGuid({})", guid);
        }

        AtlasBusinessMetadataDef existingDef   = typeRegistry.getBusinessMetadataDefByGuid(guid);

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_UPDATE, existingDef), "update businessMetadata-def ", (existingDef != null ? existingDef.getName() : guid));

        validateType(typeDef);

        AtlasType type = typeRegistry.getTypeByGuid(guid);

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.BUSINESS_METADATA) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, typeDef.getName(), DataTypes.TypeCategory.BUSINESS_METADATA.name());
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, DataTypes.TypeCategory.BUSINESS_METADATA);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        updateVertexPreUpdate(typeDef, (AtlasBusinessMetadataType)type, vertex);

        AtlasBusinessMetadataDef ret = toBusinessMetadataDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.updateByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    public AtlasVertex preDeleteByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.preDeleteByName({})", name);
        }

        AtlasBusinessMetadataDef existingDef = typeRegistry.getBusinessMetadataDefByName(name);

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_DELETE, existingDef), "delete businessMetadata-def ", name);

        AtlasVertex ret = typeDefStore.findTypeVertexByNameAndCategory(name, DataTypes.TypeCategory.BUSINESS_METADATA);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.preDeleteByName({}): {}", name, ret);
        }

        return ret;
    }

    public AtlasVertex preDeleteByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.preDeleteByGuid({})", guid);
        }

        AtlasBusinessMetadataDef existingDef = typeRegistry.getBusinessMetadataDefByGuid(guid);

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_DELETE, existingDef), "delete businessMetadata-def ", (existingDef != null ? existingDef.getName() : guid));

        AtlasVertex ret = typeDefStore.findTypeVertexByGuidAndCategory(guid, DataTypes.TypeCategory.BUSINESS_METADATA);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.preDeleteByGuid({}): ret={}", guid, ret);
        }

        return ret;
    }

    private void updateVertexPreCreate(AtlasBusinessMetadataDef businessMetadataDef, AtlasBusinessMetadataType businessMetadataType,
                                       AtlasVertex vertex) throws AtlasBaseException {
        AtlasStructDefStoreV2.updateVertexPreCreate(businessMetadataDef, businessMetadataType, vertex, typeDefStore);
    }

    private void updateVertexPreUpdate(AtlasBusinessMetadataDef businessMetadataDef, AtlasBusinessMetadataType businessMetadataType,
                                       AtlasVertex vertex) throws AtlasBaseException {
        // Load up current struct definition for matching attributes
        AtlasBusinessMetadataDef currentBusinessMetadataDef = toBusinessMetadataDef(vertex);

        // Check to verify that in an update call we only allow addition of new entity types, not deletion of existing
        // entity types
        if (CollectionUtils.isNotEmpty(businessMetadataDef.getAttributeDefs())) {
            for (AtlasStructDef.AtlasAttributeDef attributeDef : businessMetadataDef.getAttributeDefs()) {
                String updatedApplicableEntityTypesString = attributeDef.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES);
                Set<String> updatedApplicableEntityTypes = StringUtils.isBlank(updatedApplicableEntityTypesString) ? null : AtlasType.fromJson(updatedApplicableEntityTypesString, Set.class);

                AtlasStructDef.AtlasAttributeDef existingAttribute = currentBusinessMetadataDef.getAttribute(attributeDef.getName());
                if (existingAttribute != null) {
                    String existingApplicableEntityTypesString = existingAttribute.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES);
                    Set<String> existingApplicableEntityTypes = StringUtils.isBlank(existingApplicableEntityTypesString) ? null : AtlasType.fromJson(existingApplicableEntityTypesString, Set.class);

                    if (existingApplicableEntityTypes != null) {
                        if (!updatedApplicableEntityTypes.containsAll(existingApplicableEntityTypes)) {
                            throw new AtlasBaseException(AtlasErrorCode.APPLICABLE_ENTITY_TYPES_DELETION_NOT_SUPPORTED,
                                    attributeDef.getName(), businessMetadataDef.getName());
                        }
                    }
                }
            }
        }

        AtlasStructDefStoreV2.updateVertexPreUpdate(businessMetadataDef, businessMetadataType, vertex, typeDefStore);
    }

    private AtlasBusinessMetadataDef toBusinessMetadataDef(AtlasVertex vertex) throws AtlasBaseException {
        AtlasBusinessMetadataDef ret = null;

        if (vertex != null && typeDefStore.isTypeVertex(vertex, DataTypes.TypeCategory.BUSINESS_METADATA)) {
            ret = new AtlasBusinessMetadataDef();

            AtlasStructDefStoreV2.toStructDef(vertex, ret, typeDefStore);
        }

        return ret;
    }
}
