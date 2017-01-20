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

import atlas.shaded.hbase.guava.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Provider;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.TypeCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.repository.store.graph.EntityGraphDiscoveryContext;
import org.apache.atlas.repository.store.graph.EntityGraphDiscovery;
import org.apache.atlas.repository.store.graph.EntityResolver;
import org.apache.atlas.type.AtlasArrayType;
import org.apache.atlas.type.AtlasEntityType;
import org.apache.atlas.type.AtlasMapType;
import org.apache.atlas.type.AtlasStructType;
import org.apache.atlas.type.AtlasType;
import org.apache.atlas.type.AtlasTypeRegistry;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class AtlasEntityGraphDiscoveryV1 implements EntityGraphDiscovery {

    private AtlasTypeRegistry typeRegistry;

    private Set<String> processedIds = new HashSet<>();

    private EntityGraphDiscoveryContext discoveredEntities = new EntityGraphDiscoveryContext();

    private final Collection<EntityResolver> entityResolvers = new LinkedHashSet<>();

    @Inject
    public AtlasEntityGraphDiscoveryV1(AtlasTypeRegistry typeRegistry, final Collection<Provider<EntityResolver>> entityResolverProviders) {
        this.typeRegistry = typeRegistry;

        for (Provider<EntityResolver> entityResolverProvider : entityResolverProviders) {
             entityResolvers.add(entityResolverProvider.get());
        }
    }

    @VisibleForTesting
    public AtlasEntityGraphDiscoveryV1(AtlasTypeRegistry typeRegistry, final List<EntityResolver> entityResolvers) {
        this.typeRegistry = typeRegistry;

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
        final Collection<EntityResolver> entityResolvers = this.entityResolvers;
        for (EntityResolver resolver : entityResolvers) {
            resolver.cleanUp();
        }
    }


    protected void resolveReferences() throws AtlasBaseException {
        for (EntityResolver resolver : entityResolvers ) {
            resolver.init(discoveredEntities);
            resolver.resolveEntityReferences();
        }
    }


    protected void discover(final List<AtlasEntity> entities) throws AtlasBaseException {
        for (AtlasEntity entity : entities) {
            AtlasType type = typeRegistry.getType(entity.getTypeName());

            discoveredEntities.addRootEntity(entity);
            walkEntityGraph(type, entity);
        }
    }

    private void visitReference(AtlasEntityType type, Object entity, boolean isManagedEntity) throws AtlasBaseException {
        if ( entity != null) {
            if ( entity instanceof String ) {
                String guid = (String) entity;
                discoveredEntities.addUnResolvedIdReference(type, guid);
            } else if ( entity instanceof AtlasObjectId ) {
                final String guid = ((AtlasObjectId) entity).getGuid();
                discoveredEntities.addUnResolvedIdReference(type, guid);
            } else if ( entity instanceof  AtlasEntity ) {
                AtlasEntity entityObj = ( AtlasEntity ) entity;
                if (!processedIds.contains(entityObj.getGuid())) {
                    processedIds.add(entityObj.getGuid());

                    if ( isManagedEntity ) {
                        discoveredEntities.addRootEntity(entityObj);
                        visitStruct(type, entityObj);
                    } else if ( entity instanceof AtlasObjectId) {
                        discoveredEntities.addUnResolvedIdReference(type, ((AtlasObjectId) entity).getGuid());
                    } else {
                        discoveredEntities.addUnResolvedEntityReference(entityObj);
                    }
                }
            }
        }
    }

    void visitAttribute(AtlasStructType parentType, AtlasType attrType, AtlasStructDef.AtlasAttributeDef attrDef, Object val) throws AtlasBaseException {
        if (val != null) {
            if ( isPrimitive(attrType.getTypeCategory()) ) {
                return;
            }
            if (attrType.getTypeCategory() == TypeCategory.ARRAY) {
                AtlasArrayType arrayType = (AtlasArrayType) attrType;
                AtlasType elemType = arrayType.getElementType();
                visitCollectionReferences(parentType, attrType, attrDef, elemType, val);
            } else if (attrType.getTypeCategory() == TypeCategory.MAP) {
                AtlasType keyType = ((AtlasMapType) attrType).getKeyType();
                AtlasType valueType = ((AtlasMapType) attrType).getValueType();
                visitMapReferences(parentType, attrType, attrDef, keyType, valueType, val);
            } else if (attrType.getTypeCategory() == TypeCategory.STRUCT) {
                visitStruct(attrType, val);
            } else if (attrType.getTypeCategory() == TypeCategory.ENTITY) {
                if ( val instanceof AtlasObjectId || val instanceof String) {
                    visitReference((AtlasEntityType) attrType,  val, false);
                } else if ( val instanceof AtlasEntity ) {
                    //TODO - Change this to foreign key checks after changes in the model
                   if ( parentType.isMappedFromRefAttribute(attrDef.getName())) {
                       visitReference((AtlasEntityType) attrType,  val, true);
                   }
                }
            }
        }
    }

    void visitMapReferences(AtlasStructType parentType, final AtlasType attrType, AtlasStructDef.AtlasAttributeDef attrDef, AtlasType keyType, AtlasType valueType, Object val) throws AtlasBaseException {
        if (isPrimitive(keyType.getTypeCategory()) && isPrimitive(valueType.getTypeCategory())) {
            return;
        }

        if (val != null) {
            Iterator<Map.Entry> it = null;
            if (Map.class.isAssignableFrom(val.getClass())) {
                it = ((Map) val).entrySet().iterator();
                ImmutableMap.Builder b = ImmutableMap.builder();
                while (it.hasNext()) {
                    Map.Entry e = it.next();
                    visitAttribute(parentType, keyType, attrDef, e.getKey());
                    visitAttribute(parentType, valueType, attrDef, e.getValue());
                }
            }
        }
    }

    void visitCollectionReferences(final AtlasStructType parentType, final AtlasType attrType, final AtlasStructDef.AtlasAttributeDef attrDef, AtlasType elemType, Object val) throws AtlasBaseException {

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

    void visitStruct(AtlasType type, Object val) throws AtlasBaseException {

        if (val == null || !(val instanceof AtlasStruct)) {
            return;
        }

        AtlasStructType structType = (AtlasStructType) type;

        for (AtlasStructDef.AtlasAttributeDef attributeDef : structType.getStructDef().getAttributeDefs()) {
            String attrName = attributeDef.getName();
            AtlasType attrType = structType.getAttributeType(attrName);
            Object attrVal = ((AtlasStruct) val).getAttribute(attrName);
            visitAttribute(structType, attrType, attributeDef, attrVal);
        }
    }


    void walkEntityGraph(AtlasType type, AtlasEntity entity) throws AtlasBaseException {
        visitStruct(type, entity);
    }


    boolean isPrimitive(TypeCategory typeCategory) {
        return typeCategory == TypeCategory.PRIMITIVE || typeCategory == TypeCategory.ENUM;
    }
}
