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
import org.apache.atlas.model.typedef.AtlasNamespaceDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasNamespaceType;
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

import static org.apache.atlas.model.typedef.AtlasNamespaceDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES;

public class AtlasNamespaceDefStoreV2 extends AtlasAbstractDefStoreV2<AtlasNamespaceDef> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasNamespaceDefStoreV2.class);

    @Inject
    public AtlasNamespaceDefStoreV2(AtlasTypeDefGraphStoreV2 typeDefStore, AtlasTypeRegistry typeRegistry) {
        super(typeDefStore, typeRegistry);
    }

    @Override
    public AtlasVertex preCreate(AtlasNamespaceDef namespaceDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasNamespaceDefStoreV2.preCreate({})", namespaceDef);
        }

        validateType(namespaceDef);

        AtlasType type = typeRegistry.getType(namespaceDef.getName());

        if (type.getTypeCategory() != TypeCategory.NAMESPACE) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, namespaceDef.getName(),
                    DataTypes.TypeCategory.NAMESPACE.name());
        }

        AtlasVertex ret = typeDefStore.findTypeVertexByName(namespaceDef.getName());

        if (ret != null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_ALREADY_EXISTS, namespaceDef.getName());
        }

        ret = typeDefStore.createTypeVertex(namespaceDef);

        updateVertexPreCreate(namespaceDef, (AtlasNamespaceType) type, ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasNamespaceDefStoreV2.preCreate({}): {}", namespaceDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasNamespaceDef create(AtlasNamespaceDef namespaceDef, AtlasVertex preCreateResult) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasNamespaceDefStoreV2.create({}, {})", namespaceDef, preCreateResult);
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_CREATE, namespaceDef), "create namespace-def ", namespaceDef.getName());

        AtlasVertex vertex = (preCreateResult == null) ? preCreate(namespaceDef) : preCreateResult;

        AtlasNamespaceDef ret = toNamespaceDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasNamespaceDefStoreV2.create({}, {}): {}", namespaceDef, preCreateResult, ret);
        }

        return ret;
    }

    @Override
    public List<AtlasNamespaceDef> getAll() throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasNamespaceDef.getAll()");
        }

        List<AtlasNamespaceDef> ret = new ArrayList<>();

        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(DataTypes.TypeCategory.NAMESPACE);
        while (vertices.hasNext()) {
            ret.add(toNamespaceDef(vertices.next()));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasNamespaceDefStoreV2.getAll(): count={}", ret.size());
        }
        return ret;
    }

    @Override
    public AtlasNamespaceDef getByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasNamespaceDefStoreV2.getByName({})", name);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, DataTypes.TypeCategory.NAMESPACE);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, String.class);

        AtlasNamespaceDef ret = toNamespaceDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasNamespaceDefStoreV2.getByName({}): {}", name, ret);
        }

        return ret;
    }

    @Override
    public AtlasNamespaceDef getByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasNamespaceDefStoreV2.getByGuid({})", guid);
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, DataTypes.TypeCategory.NAMESPACE);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        AtlasNamespaceDef ret = toNamespaceDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasNamespaceDefStoreV2.getByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    @Override
    public AtlasNamespaceDef update(AtlasNamespaceDef typeDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasNamespaceDefStoreV2.update({})", typeDef);
        }

        validateType(typeDef);

        AtlasNamespaceDef ret = StringUtils.isNotBlank(typeDef.getGuid()) ? updateByGuid(typeDef.getGuid(), typeDef)
                : updateByName(typeDef.getName(), typeDef);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasNamespaceDefStoreV2.update({}): {}", typeDef, ret);
        }

        return ret;
    }

    @Override
    public AtlasNamespaceDef updateByName(String name, AtlasNamespaceDef typeDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasNamespaceDefStoreV2.updateByName({}, {})", name, typeDef);
        }

        AtlasNamespaceDef existingDef = typeRegistry.getNamespaceDefByName(name);

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_UPDATE, existingDef), "update namespace-def ", name);

        validateType(typeDef);

        AtlasType type = typeRegistry.getType(typeDef.getName());

        if (type.getTypeCategory() != TypeCategory.NAMESPACE) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, typeDef.getName(), DataTypes.TypeCategory.NAMESPACE.name());
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, DataTypes.TypeCategory.NAMESPACE);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }


        updateVertexPreUpdate(typeDef, (AtlasNamespaceType)type, vertex);

        AtlasNamespaceDef ret = toNamespaceDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasNamespaceDefStoreV2.updateByName({}, {}): {}", name, typeDef, ret);
        }

        return ret;
    }

    public AtlasNamespaceDef updateByGuid(String guid, AtlasNamespaceDef typeDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasNamespaceDefStoreV2.updateByGuid({})", guid);
        }

        AtlasNamespaceDef existingDef   = typeRegistry.getNamespaceDefByGuid(guid);

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_UPDATE, existingDef), "update namespace-def ", (existingDef != null ? existingDef.getName() : guid));

        validateType(typeDef);

        AtlasType type = typeRegistry.getTypeByGuid(guid);

        if (type.getTypeCategory() != org.apache.atlas.model.TypeCategory.NAMESPACE) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, typeDef.getName(), DataTypes.TypeCategory.NAMESPACE.name());
        }

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, DataTypes.TypeCategory.NAMESPACE);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        updateVertexPreUpdate(typeDef, (AtlasNamespaceType)type, vertex);

        AtlasNamespaceDef ret = toNamespaceDef(vertex);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasNamespaceDefStoreV2.updateByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    public AtlasVertex preDeleteByName(String name) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasNamespaceDefStoreV2.preDeleteByName({})", name);
        }

        AtlasNamespaceDef existingDef = typeRegistry.getNamespaceDefByName(name);

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_DELETE, existingDef), "delete namespace-def ", name);

        AtlasVertex ret = typeDefStore.findTypeVertexByNameAndCategory(name, DataTypes.TypeCategory.NAMESPACE);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasNamespaceDefStoreV2.preDeleteByName({}): {}", name, ret);
        }

        return ret;
    }

    public AtlasVertex preDeleteByGuid(String guid) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasNamespaceDefStoreV2.preDeleteByGuid({})", guid);
        }

        AtlasNamespaceDef existingDef = typeRegistry.getNamespaceDefByGuid(guid);

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_DELETE, existingDef), "delete namespace-def ", (existingDef != null ? existingDef.getName() : guid));

        AtlasVertex ret = typeDefStore.findTypeVertexByGuidAndCategory(guid, DataTypes.TypeCategory.NAMESPACE);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasNamespaceDefStoreV2.preDeleteByGuid({}): ret={}", guid, ret);
        }

        return ret;
    }

    private void updateVertexPreCreate(AtlasNamespaceDef namespaceDef, AtlasNamespaceType namespaceType,
                                      AtlasVertex vertex) throws AtlasBaseException {
        AtlasStructDefStoreV2.updateVertexPreCreate(namespaceDef, namespaceType, vertex, typeDefStore);
    }

    private void updateVertexPreUpdate(AtlasNamespaceDef namespaceDef, AtlasNamespaceType namespaceType,
                                       AtlasVertex vertex) throws AtlasBaseException {
        // Load up current struct definition for matching attributes
        AtlasNamespaceDef currentNamespaceDef = toNamespaceDef(vertex);

        // Check to verify that in an update call we only allow addition of new entity types, not deletion of existing
        // entity types
        if (CollectionUtils.isNotEmpty(namespaceDef.getAttributeDefs())) {
            for (AtlasStructDef.AtlasAttributeDef attributeDef : namespaceDef.getAttributeDefs()) {
                String updatedApplicableEntityTypesString = attributeDef.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES);
                Set<String> updatedApplicableEntityTypes = StringUtils.isBlank(updatedApplicableEntityTypesString) ? null : AtlasType.fromJson(updatedApplicableEntityTypesString, Set.class);

                AtlasStructDef.AtlasAttributeDef existingAttribute = currentNamespaceDef.getAttribute(attributeDef.getName());
                if (existingAttribute != null) {
                    String existingApplicableEntityTypesString = existingAttribute.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES);
                    Set<String> existingApplicableEntityTypes = StringUtils.isBlank(existingApplicableEntityTypesString) ? null : AtlasType.fromJson(existingApplicableEntityTypesString, Set.class);

                    if (existingApplicableEntityTypes != null) {
                        if (!updatedApplicableEntityTypes.containsAll(existingApplicableEntityTypes)) {
                            throw new AtlasBaseException(AtlasErrorCode.APPLICABLE_ENTITY_TYPES_DELETION_NOT_SUPPORTED,
                                    attributeDef.getName(), namespaceDef.getName());
                        }
                    }
                }
            }
        }

        AtlasStructDefStoreV2.updateVertexPreUpdate(namespaceDef, namespaceType, vertex, typeDefStore);
    }

    private AtlasNamespaceDef toNamespaceDef(AtlasVertex vertex) throws AtlasBaseException {
        AtlasNamespaceDef ret = null;

        if (vertex != null && typeDefStore.isTypeVertex(vertex, DataTypes.TypeCategory.NAMESPACE)) {
            ret = new AtlasNamespaceDef();

            AtlasStructDefStoreV2.toStructDef(vertex, ret, typeDefStore);
        }

        return ret;
    }
}
