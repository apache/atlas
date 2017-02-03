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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Provider;


public class AtlasEntityGraphDiscoveryV1 implements EntityGraphDiscovery {

    private final AtlasTypeRegistry           typeRegistry;
    private final EntityGraphDiscoveryContext discoveredEntities;
    private final Set<String>                 processedIds    = new HashSet<>();
    private final Collection<EntityResolver>  entityResolvers = new LinkedHashSet<>();

    @Inject
    public AtlasEntityGraphDiscoveryV1(AtlasTypeRegistry typeRegistry, Collection<Provider<EntityResolver>> entityResolverProviders) {
        this.typeRegistry       = typeRegistry;
        this.discoveredEntities = new EntityGraphDiscoveryContext(typeRegistry);

        for (Provider<EntityResolver> entityResolverProvider : entityResolverProviders) {
             entityResolvers.add(entityResolverProvider.get());
        }
    }

    @VisibleForTesting
    public AtlasEntityGraphDiscoveryV1(AtlasTypeRegistry typeRegistry, List<EntityResolver> entityResolvers) {
        this.typeRegistry       = typeRegistry;
        this.discoveredEntities = new EntityGraphDiscoveryContext(typeRegistry);

        for (EntityResolver entityResolver : entityResolvers) {
            this.entityResolvers.add(entityResolver);
        }
    }

    @Override
    public void init() throws AtlasBaseException {
        //Nothing to do
    }

    @Override
    public EntityGraphDiscoveryContext discoverEntities(final List<AtlasEntity> entities) throws AtlasBaseException {

        //walk the graph and discover entity references
        discover(entities);

        //resolve root and referred entities
        resolveReferences();

        return discoveredEntities;
    }

    @Override
    public void cleanUp() throws AtlasBaseException {
        processedIds.clear();
        discoveredEntities.cleanUp();

        for (EntityResolver resolver : entityResolvers) {
            resolver.cleanUp();
        }
    }


    protected void discover(List<AtlasEntity> entities) throws AtlasBaseException {
        for (AtlasEntity entity : entities) {
            AtlasEntityType type = typeRegistry.getEntityTypeByName(entity.getTypeName());

            if (type == null) {
                throw new AtlasBaseException(AtlasErrorCode.TYPE_NAME_INVALID, TypeCategory.ENTITY.name(), entity.getTypeName());
            }

            discoveredEntities.addRootEntity(entity);

            walkEntityGraph(type, entity);
        }
    }

    protected void resolveReferences() throws AtlasBaseException {
        for (EntityResolver resolver : entityResolvers) {
            resolver.init(discoveredEntities);

            resolver.resolveEntityReferences();
        }

        if (discoveredEntities.hasUnresolvedReferences()) {
            throw new AtlasBaseException(AtlasErrorCode.UNRESOLVED_REFERENCES_FOUND,
                                                         discoveredEntities.getUnresolvedIds().toString(),
                                                         discoveredEntities.getUnresolvedIdsByUniqAttribs().toString());
        }
    }

    private void visitReference(AtlasEntityType type, Object entity) throws AtlasBaseException {
        if (entity != null) {
            if (entity instanceof AtlasObjectId) {
                AtlasObjectId objId = (AtlasObjectId)entity;

                if (!objId.isValid()) {
                    throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, "Invalid object id " + objId);
                }

                if (!StringUtils.isEmpty(objId.getGuid()) && (objId.isAssignedGuid() || objId.isUnAssignedGuid())) {
                    discoveredEntities.addUnResolvedId(objId);
                } else {
                    discoveredEntities.addUnresolvedIdByUniqAttribs(objId);
                }
            } else if (entity instanceof AtlasEntity) {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, "Use AtlasObjectId to refer to another instance instead of AtlasEntity " + type.getTypeName());
            } else {
                throw new AtlasBaseException(AtlasErrorCode.INSTANCE_CRUD_INVALID_PARAMS, "Invalid object type " + entity.getClass());
            }
        }
    }

    void visitAttribute(AtlasStructType parentType, AtlasType attrType, AtlasAttributeDef attrDef, Object val) throws AtlasBaseException {
        if (val != null) {
            if ( isPrimitive(attrType.getTypeCategory()) ) {
                return;
            }
            if (attrType.getTypeCategory() == TypeCategory.ARRAY) {
                AtlasArrayType arrayType = (AtlasArrayType) attrType;
                AtlasType      elemType  = arrayType.getElementType();

                visitCollectionReferences(parentType, attrType, attrDef, elemType, val);
            } else if (attrType.getTypeCategory() == TypeCategory.MAP) {
                AtlasType keyType   = ((AtlasMapType) attrType).getKeyType();
                AtlasType valueType = ((AtlasMapType) attrType).getValueType();

                visitMapReferences(parentType, attrType, attrDef, keyType, valueType, val);
            } else if (attrType.getTypeCategory() == TypeCategory.STRUCT) {
                visitStruct((AtlasStructType)attrType, val);
            } else if (attrType.getTypeCategory() == TypeCategory.ENTITY) {
                visitReference((AtlasEntityType) attrType,  val);
            }
        }
    }

    void visitMapReferences(AtlasStructType parentType, final AtlasType attrType, AtlasAttributeDef attrDef, AtlasType keyType, AtlasType valueType, Object val) throws AtlasBaseException {
        if (isPrimitive(keyType.getTypeCategory()) && isPrimitive(valueType.getTypeCategory())) {
            return;
        }

        if (val != null) {
            if (Map.class.isAssignableFrom(val.getClass())) {
                Iterator<Map.Entry> it = ((Map) val).entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry e = it.next();
                    visitAttribute(parentType, keyType, attrDef, e.getKey());
                    visitAttribute(parentType, valueType, attrDef, e.getValue());
                }
            }
        }
    }

    void visitCollectionReferences(final AtlasStructType parentType, final AtlasType attrType, final AtlasAttributeDef attrDef, AtlasType elemType, Object val) throws AtlasBaseException {
        if (isPrimitive(elemType.getTypeCategory())) {
            return;
        }

        if (val != null) {
            Iterator it = null;
            if (val instanceof Collection) {
                it = ((Collection) val).iterator();
            } else if (val instanceof Iterable) {
                it = ((Iterable) val).iterator();
            } else if (val instanceof Iterator) {
                it = (Iterator) val;
            }
            if (it != null) {
                while (it.hasNext()) {
                    Object elem = it.next();
                    visitAttribute(parentType, elemType, attrDef, elem);
                }
            }
        }
    }

    void visitStruct(AtlasStructType structType, Object val) throws AtlasBaseException {
        if (structType == null) {
            return;
        }

        for (AtlasStructType.AtlasAttribute attribute : structType.getAllAttributes().values()) {
            AtlasType attrType = attribute.getAttributeType();
            Object    attrVal  = ((AtlasStruct) val).getAttribute(attribute.getName());

            visitAttribute(structType, attrType, attribute.getAttributeDef(), attrVal);
        }
    }


    void walkEntityGraph(AtlasEntityType entityType, AtlasEntity entity) throws AtlasBaseException {
        visitStruct(entityType, entity);
    }


    boolean isPrimitive(TypeCategory typeCategory) {
        return typeCategory == TypeCategory.PRIMITIVE || typeCategory == TypeCategory.ENUM;
    }
}
