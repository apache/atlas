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

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.AtlasException;
import org.apache.atlas.authorize.AtlasAuthorizationUtils;
import org.apache.atlas.authorize.AtlasPrivilege;
import org.apache.atlas.authorize.AtlasTypeAccessRequest;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.query.AtlasDSL;
import org.apache.atlas.repository.graphdb.AtlasCardinality;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.AtlasDefStore;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Abstract typedef-store for v1 format.
 */
abstract class AtlasAbstractDefStoreV2<T extends AtlasBaseTypeDef> implements AtlasDefStore<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasAbstractDefStoreV2.class);

    public static final String ALLOW_RESERVED_KEYWORDS = "atlas.types.allowReservedKeywords";

    private static final String      NAME_REGEX                 = "[a-zA-Z][a-zA-Z0-9_ ]*";
    private static final String      INTERNAL_NAME_REGEX        = "__" + NAME_REGEX;
    private static final Pattern     INTERNAL_NAME_PATTERN      = Pattern.compile(INTERNAL_NAME_REGEX);
    private static final Pattern     NAME_PATTERN               = Pattern.compile(NAME_REGEX);
    private static final Set<String> INVALID_TYPEDEF_NAMES_LIST = new HashSet<>(Arrays.asList("description", "version", "options", "name", "servicetype"));

    protected final AtlasTypeDefGraphStoreV2 typeDefStore;
    protected final AtlasTypeRegistry        typeRegistry;

    public AtlasAbstractDefStoreV2(AtlasTypeDefGraphStoreV2 typeDefStore, AtlasTypeRegistry typeRegistry) {
        this.typeDefStore = typeDefStore;
        this.typeRegistry = typeRegistry;
    }

    public void verifyTypesReadAccess(Collection<? extends AtlasType> types) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(types)) {
            for (AtlasType type : types) {
                AtlasBaseTypeDef def = typeRegistry.getTypeDefByName(type.getTypeName());

                if (def != null) {
                    AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_READ, def), "read type-def of category ", def.getCategory(), " ", def.getName());
                }
            }
        }
    }

    public void verifyTypeReadAccess(Collection<String> types) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(types)) {
            for (String type : types) {
                AtlasBaseTypeDef def = typeRegistry.getTypeDefByName(type);

                if (def != null) {
                    AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_READ, def), "read type-def of category ", def.getCategory(), " ", def.getName());
                }
            }
        }
    }

    public void verifyTypeReadAccess(String type) throws AtlasBaseException {
        if (StringUtils.isNotEmpty(type)) {
            AtlasBaseTypeDef def = typeRegistry.getTypeDefByName(type);

            if (def != null) {
                AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_READ, def), "read type-def of category ", def.getCategory(), " ", def.getName());
            }
        }
    }

    public void verifyAttributeTypeReadAccess(Collection<AtlasStructDef.AtlasAttributeDef> types) throws AtlasBaseException {
        if (CollectionUtils.isNotEmpty(types)) {
            for (AtlasStructDef.AtlasAttributeDef attributeDef : types) {
                AtlasBaseTypeDef def = typeRegistry.getTypeDefByName(attributeDef.getTypeName());

                if (def != null) {
                    AtlasAuthorizationUtils.verifyAccess(new AtlasTypeAccessRequest(AtlasPrivilege.TYPE_READ, def), "read type-def of category ", def.getCategory(), " ", def.getName());
                }
            }
        }
    }

    public void validateType(AtlasBaseTypeDef typeDef) throws AtlasBaseException {
        if (!isValidName(typeDef.getName())) {
            throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID_FORMAT, typeDef.getName(), typeDef.getCategory().name());
        }

        // To validate unsupported typeDef name
        if (isInvalidTypeDefName(typeDef.getName())) {
            throw new AtlasBaseException(AtlasErrorCode.UNSUPPORTED_TYPE_NAME, typeDef.getCategory().name());
        }

        try {
            final boolean allowReservedKeywords = ApplicationProperties.get().getBoolean(ALLOW_RESERVED_KEYWORDS, true);

            if (!allowReservedKeywords && typeDef instanceof AtlasStructDef) {
                final List<AtlasStructDef.AtlasAttributeDef> attributeDefs = ((AtlasStructDef) typeDef).getAttributeDefs();

                for (AtlasStructDef.AtlasAttributeDef attrDef : attributeDefs) {
                    if (AtlasDSL.Parser.isKeyword(attrDef.getName())) {
                        throw new AtlasBaseException(AtlasErrorCode.ATTRIBUTE_NAME_INVALID, attrDef.getName(), typeDef.getCategory().name());
                    }
                }
            }
        } catch (AtlasException e) {
            LOG.error("Exception while loading configuration ", e);

            throw new AtlasBaseException(AtlasErrorCode.INTERNAL_ERROR, "Could not load configuration");
        }
    }

    public boolean isValidName(String typeName) {
        return NAME_PATTERN.matcher(typeName).matches() || INTERNAL_NAME_PATTERN.matcher(typeName).matches();
    }

    @Override
    public void deleteByName(String name, AtlasVertex preDeleteResult) throws AtlasBaseException {
        LOG.debug("==> AtlasAbstractDefStoreV1.deleteByName({}, {})", name, preDeleteResult);

        AtlasVertex vertex = (preDeleteResult == null) ? preDeleteByName(name) : preDeleteResult;

        typeDefStore.deleteTypeVertex(vertex);

        LOG.debug("<== AtlasAbstractDefStoreV1.deleteByName({}, {})", name, preDeleteResult);
    }

    @Override
    public void deleteByGuid(String guid, AtlasVertex preDeleteResult) throws AtlasBaseException {
        LOG.debug("==> AtlasAbstractDefStoreV1.deleteByGuid({}, {})", guid, preDeleteResult);

        AtlasVertex vertex = (preDeleteResult == null) ? preDeleteByGuid(guid) : preDeleteResult;

        typeDefStore.deleteTypeVertex(vertex);

        LOG.debug("<== AtlasAbstractDefStoreV1.deleteByGuid({}, {})", guid, preDeleteResult);
    }

    public boolean isInvalidTypeDefName(String typeName) {
        return INVALID_TYPEDEF_NAMES_LIST.contains(typeName);
    }

    protected static void createPropertyKey(String propertyKey, Class<?> clz, AtlasCardinality cardinality, AtlasGraphManagement management) {
        if (!management.containsPropertyKey(propertyKey)) {
            management.makePropertyKey(propertyKey, clz, cardinality);
        }
    }
}
