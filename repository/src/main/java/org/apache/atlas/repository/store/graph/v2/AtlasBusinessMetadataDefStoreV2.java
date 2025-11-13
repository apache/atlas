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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.authorizer.AtlasAuthorizationUtils;
import org.apache.atlas.discovery.EntityDiscoveryService;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.type.AtlasBusinessMetadataType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.utils.AtlasJson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.*;

import static org.apache.atlas.model.typedef.AtlasBusinessMetadataDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES;

public class AtlasBusinessMetadataDefStoreV2 extends AtlasAbstractDefStoreV2<AtlasBusinessMetadataDef> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasBusinessMetadataDefStoreV2.class);

    private final EntityDiscoveryService entityDiscoveryService;

    private static final int DEFAULT_RICH_TEXT_ATTRIBUTE_LIMIT = 50;
    private static final String RICH_TEXT_ATTRIBUTE_LIMIT_PROPERTY = "atlas.business.metadata.richtext.limit";

    @Inject
    public AtlasBusinessMetadataDefStoreV2(AtlasTypeDefGraphStoreV2 typeDefStore, AtlasTypeRegistry typeRegistry, EntityDiscoveryService entityDiscoveryService) {
        super(typeDefStore, typeRegistry);
        this.entityDiscoveryService = entityDiscoveryService;
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

        AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_CREATE, businessMetadataDef), "create businessMetadata-def ", businessMetadataDef.getName());

        AtlasVertex ret = typeDefStore.findTypeVertexByName(businessMetadataDef.getName());

        if (ret != null) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_ALREADY_EXISTS, businessMetadataDef.getName());
        }

        //validate uniqueness of display name for BM
        if (type.getTypeCategory() == TypeCategory.BUSINESS_METADATA) {
            ret = typeDefStore.findTypeVertexByDisplayName(
                    businessMetadataDef.getDisplayName(), DataTypes.TypeCategory.BUSINESS_METADATA);
            if (ret != null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_WITH_DISPLAY_NAME_ALREADY_EXISTS, businessMetadataDef.getDisplayName());
            }
        }

        ret = typeDefStore.createTypeVertex(businessMetadataDef);

        updateVertexPreCreate(businessMetadataDef, (AtlasBusinessMetadataType) type, ret);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.preCreate({}): {}", businessMetadataDef, ret);
        }

        return ret;
    }

    private int getRichTextAttributeLimit() {
        try {
            return ApplicationProperties.get().getInt(RICH_TEXT_ATTRIBUTE_LIMIT_PROPERTY, DEFAULT_RICH_TEXT_ATTRIBUTE_LIMIT);
        } catch (Exception e) {
            LOG.warn("Failed to read rich text attribute limit configuration, using default: {}", DEFAULT_RICH_TEXT_ATTRIBUTE_LIMIT);
            return DEFAULT_RICH_TEXT_ATTRIBUTE_LIMIT;
        }
    }

    @Override
    public void validateType(AtlasBaseTypeDef typeDef) throws AtlasBaseException {
        super.validateType(typeDef);
        AtlasBusinessMetadataDef businessMetadataDef = (AtlasBusinessMetadataDef) typeDef;

        //validate uniqueness of display name for BM
        AtlasVertex ret = typeDefStore.findTypeVertexByDisplayName(
                businessMetadataDef.getDisplayName(), DataTypes.TypeCategory.BUSINESS_METADATA);
        if (ret != null && (
                businessMetadataDef.getGuid() == null || !businessMetadataDef.getGuid().equals(ret.getProperty(Constants.GUID_PROPERTY_KEY, String.class)))) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_WITH_DISPLAY_NAME_ALREADY_EXISTS, businessMetadataDef.getDisplayName());
        }

        if (CollectionUtils.isNotEmpty(businessMetadataDef.getAttributeDefs())) {
            for (AtlasStructDef.AtlasAttributeDef attributeDef : businessMetadataDef.getAttributeDefs()) {
                if (!isValidName(attributeDef.getName())) {
                    throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_NAME_INVALID_CHARS, attributeDef.getName());
                }
            }
        }

        validateRichTextAttributeLimit(businessMetadataDef);
    }

    @Override
    public AtlasBusinessMetadataDef create(AtlasBusinessMetadataDef businessMetadataDef, AtlasVertex preCreateResult) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.create({}, {})", businessMetadataDef, preCreateResult);
        }

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

        verifyAttributeTypeReadAccess(typeDef.getAttributeDefs());

        if (CollectionUtils.isNotEmpty(typeDef.getAttributeDefs())) {
            AtlasBusinessMetadataType businessMetadataType = typeRegistry.getBusinessMetadataTypeByName(typeDef.getName());
            for (AtlasStructType.AtlasAttribute attribute : businessMetadataType.getAllAttributes().values()) {
                AtlasBusinessMetadataType.AtlasBusinessAttribute bmAttribute = (AtlasBusinessMetadataType.AtlasBusinessAttribute) attribute;
                verifyTypesReadAccess(bmAttribute.getApplicableEntityTypes());
            }
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

        checkBusinessMetadataRef(existingDef.getName());

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

        checkBusinessMetadataRef(existingDef.getName());

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

        // Check if assets are attached with the attributes that are being archived
        checkAttributesAttachedToAssets(businessMetadataDef, currentBusinessMetadataDef);

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

    private void checkBusinessMetadataRef(String typeName) throws AtlasBaseException {
        AtlasBusinessMetadataDef businessMetadataDef = typeRegistry.getBusinessMetadataDefByName(typeName);
        if (businessMetadataDef != null) {
            List<AtlasStructDef.AtlasAttributeDef> attributeDefs = businessMetadataDef.getAttributeDefs();

            for (AtlasStructDef.AtlasAttributeDef attributeDef : attributeDefs) {
                String      vertexPropertyName  = AtlasStructType.AtlasAttribute.generateVertexPropertyName(attributeDef);
                Set<String> applicableTypes     = AtlasJson.fromJson(attributeDef.getOption(AtlasBusinessMetadataDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES), Set.class);

                if (isBusinessAttributePresent(vertexPropertyName, applicableTypes)) {
                    throw new AtlasBaseException(AtlasErrorCode.TYPE_HAS_REFERENCES, typeName);
                }
            }
        }
    }

    private void checkAttributesAttachedToAssets(AtlasBusinessMetadataDef updatedDef, AtlasBusinessMetadataDef currentDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasBusinessMetadataDefStoreV2.checkAttributesAttachedToAssets({})", updatedDef.getName());
        }

        if (CollectionUtils.isEmpty(currentDef.getAttributeDefs())) {
            return;
        }

        Map<String, AtlasStructDef.AtlasAttributeDef> updatedAttributeMap = new HashMap<>();

        if (CollectionUtils.isNotEmpty(updatedDef.getAttributeDefs())) {
            for (AtlasStructDef.AtlasAttributeDef attributeDef : updatedDef.getAttributeDefs()) {
                updatedAttributeMap.put(attributeDef.getName(), attributeDef);
            }
        }

        for (AtlasStructDef.AtlasAttributeDef currentAttribute : currentDef.getAttributeDefs()) {
            AtlasStructDef.AtlasAttributeDef updatedAttr = updatedAttributeMap.get(currentAttribute.getName());
            if (updatedAttr == null) {
                continue;
            }

            //  Diff Calculation: Check if attribute is being archived now but was not archived earlier
            boolean previouslyArchived = isArchivedAttribute(currentAttribute);
            boolean currentlyArchived = isArchivedAttribute(updatedAttr);
            boolean isBeingArchived = !previouslyArchived && currentlyArchived;
            if (!isBeingArchived) {
                continue;
            }

            String vertexPropertyName = AtlasStructType.AtlasAttribute.generateVertexPropertyName(currentAttribute);
            Set<String> applicableTypes = AtlasJson.fromJson(
                    currentAttribute.getOption(AtlasBusinessMetadataDef.ATTR_OPTION_APPLICABLE_ENTITY_TYPES),
                    Set.class
            );

            if (isBusinessAttributePresent(vertexPropertyName, applicableTypes)) {
                LOG.warn("Business metadata attribute '{}' cannot be archived as it is currently attached to one or more assets", currentAttribute.getName());
                throw new AtlasBaseException(
                    AtlasErrorCode.TYPE_HAS_REFERENCES,
                        currentAttribute.getName()
                );
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasBusinessMetadataDefStoreV2.checkAttributesAttachedToAssets({})", updatedDef.getName());
        }
    }

    private Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);

        return map;
    }

    private boolean isBusinessAttributePresent(String attrName, Set<String> applicableTypes) throws AtlasBaseException {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = getMap("size", "1");
        dsl.put("query", getMap("exists", getMap("field", attrName)));

        indexSearchParams.setDsl(dsl);

        AtlasSearchResult atlasSearchResult = entityDiscoveryService.directIndexSearch(indexSearchParams);

        return CollectionUtils.isNotEmpty(atlasSearchResult.getEntities());
    }

    private void validateRichTextAttributeLimit(AtlasBusinessMetadataDef businessMetadataDef) throws AtlasBaseException {
        if (CollectionUtils.isEmpty(businessMetadataDef.getAttributeDefs())) {
            return;
        }

        int newRichTextCount = 0;
        for (AtlasStructDef.AtlasAttributeDef attributeDef : businessMetadataDef.getAttributeDefs()) {
            if (isRichTextAttribute(attributeDef) && !isArchivedAttribute(attributeDef)) {
                newRichTextCount++;
            }
        }

        if (newRichTextCount == 0) {
            return;
        }

        int existingRichTextCount = countExistingRichTextAttributes(businessMetadataDef.getGuid());
        int totalRichTextCount = existingRichTextCount + newRichTextCount;

        int limit = getRichTextAttributeLimit();

        if (totalRichTextCount > limit) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PARAMETERS,
                    String.format("Cannot create business metadata attributes. Total rich text attributes would exceed limit of %d. " +
                                    "Current: %d, Attempting to add: %d, Limit: %d",
                            limit, existingRichTextCount, newRichTextCount, limit));
        }
    }

    private boolean isRichTextAttribute(AtlasStructDef.AtlasAttributeDef attributeDef) {
        if (attributeDef.getOptions() == null) {
            return false;
        }

        String isRichText = attributeDef.getOptions().get("isRichText");
        return "true".equalsIgnoreCase(isRichText);
    }

    private boolean isArchivedAttribute(AtlasStructDef.AtlasAttributeDef attributeDef) {
        if (attributeDef.getOptions() == null) {
            return false;
        }

        String isArchived = attributeDef.getOptions().get("isArchived");
        return "true".equalsIgnoreCase(isArchived);
    }

    private int countExistingRichTextAttributes(String excludeBusinessMetadataGuid) throws AtlasBaseException {
        int count = 0;

        try {
            List<AtlasBusinessMetadataDef> allBusinessMetadataDefs = getAll();

            for (AtlasBusinessMetadataDef bmDef : allBusinessMetadataDefs) {
                if (excludeBusinessMetadataGuid != null && excludeBusinessMetadataGuid.equals(bmDef.getGuid())) {
                    continue;
                }

                if (CollectionUtils.isNotEmpty(bmDef.getAttributeDefs())) {
                    for (AtlasStructDef.AtlasAttributeDef attributeDef : bmDef.getAttributeDefs()) {
                        if (isRichTextAttribute(attributeDef) && !isArchivedAttribute(attributeDef)) {
                            count++;
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to count existing rich text attributes, at business metadata : {}", excludeBusinessMetadataGuid, e);
            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Failed to validate rich text attribute limit");
        }

        return count;
    }
}
