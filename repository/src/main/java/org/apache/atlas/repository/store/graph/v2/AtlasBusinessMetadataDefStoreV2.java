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
package org.apache.atlas.repository.store.graph.v2;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.discovery.SearchContext;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasBusinessMetadataType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import static org.apache.atlas.model.typedef.AtlasBusinessMetadataDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES;

public class AtlasBusinessMetadataDefStoreV2 extends AtlasAbstractDefStoreV2<AtlasBusinessMetadataDef> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasBusinessMetadataDefStoreV2.class);

    private final EntityDiscoveryService entityDiscoveryService;

    private final AtlasGraph graph;

    @Inject
    public AtlasBusinessMetadataDefStoreV2(AtlasTypeDefGraphStoreV2 typeDefStore, AtlasTypeRegistry typeRegistry, EntityDiscoveryService entityDiscoveryService, AtlasGraph graph) {
        super(typeDefStore, typeRegistry);

        this.entityDiscoveryService = entityDiscoveryService;
        this.graph = graph;
    }

    @Override
    public AtlasVertex preCreate(AtlasBusinessMetadataDef businessMetadataDef) throws AtlasBaseException {
        LOG.debug("==> AtlasBusinessMetadataDefStoreV2.preCreate({})", businessMetadataDef);

        validateType(businessMetadataDef);

        AtlasType type = typeRegistry.getType(businessMetadataDef.getName());

        if (type.getTypeCategory() != TypeCategory.BUSINESS_METADATA) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_MATCH_FAILED, businessMetadataDef.getName(), DataTypes.TypeCategory.BUSINESS_METADATA.name());
        }

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_CREATE, businessMetadataDef), "create businessMetadata-def ", businessMetadataDef.getName());

        AtlasVertex ret = typeDefStore.findTypeVertexByName(businessMetadataDef.getName());

        if (ret != null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_ALREADY_EXISTS, businessMetadataDef.getName());
        }

        ret = typeDefStore.createTypeVertex(businessMetadataDef);

        updateVertexPreCreate(businessMetadataDef, (AtlasBusinessMetadataType) type, ret);

        LOG.debug("<== AtlasBusinessMetadataDefStoreV2.preCreate({}): {}", businessMetadataDef, ret);

        return ret;
    }

    @Override
    public AtlasBusinessMetadataDef create(AtlasBusinessMetadataDef businessMetadataDef, AtlasVertex preCreateResult) throws AtlasBaseException {
        LOG.debug("==> AtlasBusinessMetadataDefStoreV2.create({}, {})", businessMetadataDef, preCreateResult);

        verifyAttributeTypeReadAccess(businessMetadataDef.getAttributeDefs());

        if (CollectionUtils.isNotEmpty(businessMetadataDef.getAttributeDefs())) {
            AtlasBusinessMetadataType businessMetadataType = typeRegistry.getBusinessMetadataTypeByName(businessMetadataDef.getName());

            for (AtlasStructType.AtlasAttribute attribute : businessMetadataType.getAllAttributes().values()) {
                AtlasBusinessMetadataType.AtlasBusinessAttribute bmAttribute = (AtlasBusinessMetadataType.AtlasBusinessAttribute) attribute;

                verifyTypesReadAccess(bmAttribute.getApplicableEntityTypes());
            }
        }

        AtlasVertex vertex = (preCreateResult == null) ? preCreate(businessMetadataDef) : preCreateResult;

        AtlasBusinessMetadataDef ret = toBusinessMetadataDef(vertex);

        LOG.debug("<== AtlasBusinessMetadataDefStoreV2.create({}, {}): {}", businessMetadataDef, preCreateResult, ret);

        return ret;
    }

    @Override
    public List<AtlasBusinessMetadataDef> getAll() throws AtlasBaseException {
        LOG.debug("==> AtlasBusinessMetadataDef.getAll()");

        List<AtlasBusinessMetadataDef> ret = new ArrayList<>();

        Iterator<AtlasVertex> vertices = typeDefStore.findTypeVerticesByCategory(DataTypes.TypeCategory.BUSINESS_METADATA);

        while (vertices.hasNext()) {
            ret.add(toBusinessMetadataDef(vertices.next()));
        }

        LOG.debug("<== AtlasBusinessMetadataDefStoreV2.getAll(): count={}", ret.size());

        return ret;
    }

    @Override
    public AtlasBusinessMetadataDef getByName(String name) throws AtlasBaseException {
        LOG.debug("==> AtlasBusinessMetadataDefStoreV2.getByName({})", name);

        AtlasVertex vertex = typeDefStore.findTypeVertexByNameAndCategory(name, DataTypes.TypeCategory.BUSINESS_METADATA);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, String.class);

        AtlasBusinessMetadataDef ret = toBusinessMetadataDef(vertex);

        LOG.debug("<== AtlasBusinessMetadataDefStoreV2.getByName({}): {}", name, ret);

        return ret;
    }

    @Override
    public AtlasBusinessMetadataDef getByGuid(String guid) throws AtlasBaseException {
        LOG.debug("==> AtlasBusinessMetadataDefStoreV2.getByGuid({})", guid);

        AtlasVertex vertex = typeDefStore.findTypeVertexByGuidAndCategory(guid, DataTypes.TypeCategory.BUSINESS_METADATA);

        if (vertex == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        AtlasBusinessMetadataDef ret = toBusinessMetadataDef(vertex);

        LOG.debug("<== AtlasBusinessMetadataDefStoreV2.getByGuid({}): {}", guid, ret);

        return ret;
    }

    @Override
    public AtlasBusinessMetadataDef update(AtlasBusinessMetadataDef typeDef) throws AtlasBaseException {
        LOG.debug("==> AtlasBusinessMetadataDefStoreV2.update({})", typeDef);

        verifyAttributeTypeReadAccess(typeDef.getAttributeDefs());

        if (CollectionUtils.isNotEmpty(typeDef.getAttributeDefs())) {
            AtlasBusinessMetadataType businessMetadataType = typeRegistry.getBusinessMetadataTypeByName(typeDef.getName());

            for (AtlasStructType.AtlasAttribute attribute : businessMetadataType.getAllAttributes().values()) {
                AtlasBusinessMetadataType.AtlasBusinessAttribute bmAttribute = (AtlasBusinessMetadataType.AtlasBusinessAttribute) attribute;

                verifyTypesReadAccess(bmAttribute.getApplicableEntityTypes());
            }
        }

        validateType(typeDef);

        AtlasBusinessMetadataDef ret = StringUtils.isNotBlank(typeDef.getGuid()) ? updateByGuid(typeDef.getGuid(), typeDef) : updateByName(typeDef.getName(), typeDef);

        LOG.debug("<== AtlasBusinessMetadataDefStoreV2.update({}): {}", typeDef, ret);

        return ret;
    }

    @Override
    public AtlasBusinessMetadataDef updateByName(String name, AtlasBusinessMetadataDef typeDef) throws AtlasBaseException {
        LOG.debug("==> AtlasBusinessMetadataDefStoreV2.updateByName({}, {})", name, typeDef);

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

        updateVertexPreUpdate(typeDef, (AtlasBusinessMetadataType) type, vertex);

        AtlasBusinessMetadataDef ret = toBusinessMetadataDef(vertex);

        LOG.debug("<== AtlasBusinessMetadataDefStoreV2.updateByName({}, {}): {}", name, typeDef, ret);

        return ret;
    }

    public AtlasBusinessMetadataDef updateByGuid(String guid, AtlasBusinessMetadataDef typeDef) throws AtlasBaseException {
        LOG.debug("==> AtlasBusinessMetadataDefStoreV2.updateByGuid({})", guid);

        AtlasBusinessMetadataDef existingDef = typeRegistry.getBusinessMetadataDefByGuid(guid);

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

        updateVertexPreUpdate(typeDef, (AtlasBusinessMetadataType) type, vertex);

        AtlasBusinessMetadataDef ret = toBusinessMetadataDef(vertex);

        LOG.debug("<== AtlasBusinessMetadataDefStoreV2.updateByGuid({}): {}", guid, ret);

        return ret;
    }

    @Override
    public AtlasVertex preDeleteByName(String name) throws AtlasBaseException {
        return preDeleteByName(name, false);
    }

    @Override
    public AtlasVertex preDeleteByName(String name, boolean forceDelete) throws AtlasBaseException {
        LOG.debug("==> AtlasBusinessMetadataDefStoreV2.preDeleteByName({}, {})", name, forceDelete);

        AtlasBusinessMetadataDef existingDef = typeRegistry.getBusinessMetadataDefByName(name);

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_DELETE, existingDef), "delete businessMetadata-def ", name);

        AtlasVertex ret = typeDefStore.findTypeVertexByNameAndCategory(name, DataTypes.TypeCategory.BUSINESS_METADATA);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_NOT_FOUND, name);
        }

        if (!forceDelete) {
            boolean hasIndexableAttribute = hasIndexableAttribute(existingDef);

            if (!hasIndexableAttribute) {
                LOG.warn("Deletion blocked for non-indexable Business Metadata '{}' without force-delete flag", name);
                throw new AtlasBaseException(AtlasErrorCode.NON_INDEXABLE_BM_DELETE_NOT_ALLOWED, name);
            }
            checkBusinessMetadataRef(existingDef.getName());
        }

        LOG.debug("<== AtlasBusinessMetadataDefStoreV2.preDeleteByName({}, {}): {}", name, forceDelete, ret);

        return ret;
    }

    @Override
    public AtlasVertex preDeleteByGuid(String guid) throws AtlasBaseException {
        return preDeleteByGuid(guid, false);
    }

    @Override
    public AtlasVertex preDeleteByGuid(String guid, boolean forceDelete) throws AtlasBaseException {
        LOG.debug("==> AtlasBusinessMetadataDefStoreV2.preDeleteByGuid({}, {})", guid, forceDelete);

        AtlasBusinessMetadataDef existingDef = typeRegistry.getBusinessMetadataDefByGuid(guid);

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_DELETE, existingDef), "delete businessMetadata-def ", (existingDef != null ? existingDef.getName() : guid));

        AtlasVertex ret = typeDefStore.findTypeVertexByGuidAndCategory(guid, DataTypes.TypeCategory.BUSINESS_METADATA);

        if (ret == null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_GUID_NOT_FOUND, guid);
        }

        if (existingDef != null && !forceDelete) {
            boolean hasIndexableAttribute = hasIndexableAttribute(existingDef);

            if (!hasIndexableAttribute) {
                LOG.warn("Deletion blocked for non-indexable Business Metadata '{}' without force-delete flag", existingDef.getName());
                throw new AtlasBaseException(AtlasErrorCode.NON_INDEXABLE_BM_DELETE_NOT_ALLOWED, existingDef.getName());
            }
            checkBusinessMetadataRef(existingDef.getName());
        }

        LOG.debug("<== AtlasBusinessMetadataDefStoreV2.preDeleteByGuid({}, {}): ret={}", guid, forceDelete, ret);

        return ret;
    }

    private boolean hasIndexableAttribute(AtlasBusinessMetadataDef bmDef) {
        if (bmDef == null || CollectionUtils.isEmpty(bmDef.getAttributeDefs())) {
            return false;
        }

        for (AtlasStructDef.AtlasAttributeDef attributeDef : bmDef.getAttributeDefs()) {
            if (attributeDef.getIsIndexable()) {
                return true;
            }
        }

        return false;
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

    private void updateVertexPreCreate(AtlasBusinessMetadataDef businessMetadataDef, AtlasBusinessMetadataType businessMetadataType, AtlasVertex vertex) throws AtlasBaseException {
        AtlasStructDefStoreV2.updateVertexPreCreate(businessMetadataDef, businessMetadataType, vertex, typeDefStore);
    }

    private void updateVertexPreUpdate(AtlasBusinessMetadataDef businessMetadataDef, AtlasBusinessMetadataType businessMetadataType, AtlasVertex vertex) throws AtlasBaseException {
        // Load up current struct definition for matching attributes
        AtlasBusinessMetadataDef currentBusinessMetadataDef = toBusinessMetadataDef(vertex);

        // Check to verify that in an update call we only allow addition of new entity types, not deletion of existing
        // entity types
        if (CollectionUtils.isNotEmpty(businessMetadataDef.getAttributeDefs())) {
            for (AtlasStructDef.AtlasAttributeDef attributeDef : businessMetadataDef.getAttributeDefs()) {
                String      updatedApplicableEntityTypesString = attributeDef.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES);
                Set<String> updatedApplicableEntityTypes       = StringUtils.isBlank(updatedApplicableEntityTypesString) ? null : AtlasType.fromJson(updatedApplicableEntityTypesString, Set.class);

                AtlasStructDef.AtlasAttributeDef existingAttribute = currentBusinessMetadataDef.getAttribute(attributeDef.getName());

                if (existingAttribute != null) {
                    String      existingApplicableEntityTypesString = existingAttribute.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES);
                    Set<String> existingApplicableEntityTypes       = StringUtils.isBlank(existingApplicableEntityTypesString) ? null : AtlasType.fromJson(existingApplicableEntityTypesString, Set.class);

                    if (existingApplicableEntityTypes != null && updatedApplicableEntityTypes != null) {
                        if (!updatedApplicableEntityTypes.containsAll(existingApplicableEntityTypes)) {
                            throw new AtlasBaseException(AtlasErrorCode.APPLICABLE_ENTITY_TYPES_DELETION_NOT_SUPPORTED, attributeDef.getName(), businessMetadataDef.getName());
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

    private void checkBusinessMetadataRef(String typeName) throws AtlasBaseException {
        AtlasBusinessMetadataDef businessMetadataDef = typeRegistry.getBusinessMetadataDefByName(typeName);

        if (businessMetadataDef == null || CollectionUtils.isEmpty(businessMetadataDef.getAttributeDefs())) {
            return;
        }

        for (AtlasStructDef.AtlasAttributeDef attributeDef : businessMetadataDef.getAttributeDefs()) {
            validateAttributeReferences(businessMetadataDef, attributeDef);
        }
    }

    private void validateAttributeReferences(AtlasBusinessMetadataDef bmDef, AtlasStructDef.AtlasAttributeDef attributeDef) throws AtlasBaseException {
        String applicableTypesStr = attributeDef.getOption(ATTR_OPTION_APPLICABLE_ENTITY_TYPES);
        Set<String> applicableTypes = StringUtils.isBlank(applicableTypesStr) ? null : AtlasJson.fromJson(applicableTypesStr, Set.class);

        if (CollectionUtils.isEmpty(applicableTypes)) {
            return;
        }

        Set<String> allApplicableTypes = getApplicableTypesWithSubTypes(applicableTypes);
        String qualifiedName      = AtlasStructType.AtlasAttribute.getQualifiedAttributeName(bmDef, attributeDef.getName());
        String vertexPropertyName = AtlasStructType.AtlasAttribute.generateVertexPropertyName(bmDef, attributeDef, qualifiedName);

        long startTime = System.currentTimeMillis();

        boolean isPresent = isBusinessAttributePresentInGraph(vertexPropertyName, allApplicableTypes);

        if (LOG.isDebugEnabled()) {
            LOG.info("Reference check for attribute {} took {} ms. Found: {}",
                    attributeDef.getName(), (System.currentTimeMillis() - startTime), isPresent);
        }

        if (isPresent) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_HAS_REFERENCES, bmDef.getName());
        }
    }


    private boolean isBusinessAttributePresentInGraph(String vertexPropertyName, Set<String> allApplicableTypes) {
        if (graph == null || CollectionUtils.isEmpty(allApplicableTypes)) {
            return false;
        }

        try {
            List<String> typesList = new ArrayList<>(allApplicableTypes);

            // 1. To Check if the BM property exists on a vertex where the direct type matches
            Iterable<AtlasVertex> verticesDirect = graph.query()
                    .has(vertexPropertyName, AtlasGraphQuery.ComparisionOperator.NOT_EQUAL, (Object) null)
                    .in(Constants.ENTITY_TYPE_PROPERTY_KEY, typesList)
                    .vertices();

            if (verticesDirect != null && verticesDirect.iterator().hasNext()) {
                return true;
            }

            // 2. To Check if the BM property exists on a vertex where it inherits from one of parent Types
            // This is crucial for Case 6 (Parent -> Child)
            Iterable<AtlasVertex> verticesInherited = graph.query()
                    .has(vertexPropertyName, AtlasGraphQuery.ComparisionOperator.NOT_EQUAL, (Object) null)
                    .in(Constants.SUPER_TYPES_PROPERTY_KEY, typesList)
                    .vertices();

            if (verticesInherited != null && verticesInherited.iterator().hasNext()) {
                return true;
            }
        } catch (Exception e) {
            LOG.error("Error occurred while querying graph for references of property: {}", vertexPropertyName, e);
            return true;
        }
        return false;
    }

    private Set<String> getApplicableTypesWithSubTypes(Set<String> applicableTypes) throws AtlasBaseException {
        Set<String> allTypes = new HashSet<>();

        for (String typeName : applicableTypes) {
            AtlasType type = typeRegistry.getType(typeName);

            if (type instanceof AtlasEntityType) {
                AtlasEntityType entityType = (AtlasEntityType) type;
                allTypes.add(entityType.getTypeName());
                allTypes.addAll(entityType.getAllSubTypes());
            } else {
                allTypes.add(typeName);
            }
        }
        return allTypes;
    }
}