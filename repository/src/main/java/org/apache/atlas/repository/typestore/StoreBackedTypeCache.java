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
package org.apache.atlas.repository.typestore;

import com.google.common.collect.ImmutableList;
import org.apache.atlas.AtlasException;
import org.apache.atlas.annotation.ConditionalOnAtlasProperty;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeSystem.TransientTypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.typesystem.types.cache.DefaultTypeCache;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * An extension of {@link DefaultTypeCache} which loads
 * the requested type from the type store if it is not found in the cache,
 * and adds it to the cache if it's found in the store.
 * Any attribute and super types that are required by the requested type
 * are also loaded from the store if they are not already in the cache.
 */
@Singleton
@Component
@Deprecated
@ConditionalOnAtlasProperty(property = "atlas.TypeCache.impl")
public class StoreBackedTypeCache extends DefaultTypeCache {

    private ITypeStore typeStore;

    private ImmutableList<String> coreTypes;
    private TypeSystem typeSystem;

    @Inject
    public StoreBackedTypeCache(final ITypeStore typeStore) {
        this.typeStore = typeStore;
        typeSystem = TypeSystem.getInstance();
        coreTypes = typeSystem.getCoreTypes();
    }

    private static class Context {
        ImmutableList.Builder<EnumTypeDefinition> enums = ImmutableList.builder();
        ImmutableList.Builder<StructTypeDefinition> structs = ImmutableList.builder();
        ImmutableList.Builder<HierarchicalTypeDefinition<ClassType>> classTypes = ImmutableList.builder();
        ImmutableList.Builder<HierarchicalTypeDefinition<TraitType>> traits = ImmutableList.builder();
        Set<String> loadedFromStore = new HashSet<>();

        public void addTypesDefToLists(TypesDef typesDef) {

            List<EnumTypeDefinition> enumTypesAsJavaList = typesDef.enumTypesAsJavaList();
            enums.addAll(enumTypesAsJavaList);
            for (EnumTypeDefinition etd : enumTypesAsJavaList) {
                loadedFromStore.add(etd.name);
            }
            List<StructTypeDefinition> structTypesAsJavaList = typesDef.structTypesAsJavaList();
            structs.addAll(structTypesAsJavaList);
            for (StructTypeDefinition std : structTypesAsJavaList) {
                loadedFromStore.add(std.typeName);
            }
            List<HierarchicalTypeDefinition<ClassType>> classTypesAsJavaList = typesDef.classTypesAsJavaList();
            classTypes.addAll(classTypesAsJavaList);
            for (HierarchicalTypeDefinition<ClassType> classTypeDef : classTypesAsJavaList) {
                loadedFromStore.add(classTypeDef.typeName);
            }
            List<HierarchicalTypeDefinition<TraitType>> traitTypesAsJavaList = typesDef.traitTypesAsJavaList();
            traits.addAll(traitTypesAsJavaList);
            for (HierarchicalTypeDefinition<TraitType> traitTypeDef : traitTypesAsJavaList) {
                loadedFromStore.add(traitTypeDef.typeName);
            }
        }

        public boolean isLoadedFromStore(String typeName) {
            return loadedFromStore.contains(typeName);
        }

        public TypesDef getTypesDef() {
            return TypesUtil.getTypesDef(enums.build(), structs.build(), traits.build(), classTypes.build());
        }
    }

    /**
     * Checks whether the specified type is cached in memory and does *not*
     * access the type store.  Used for testing.
     *
     * @param typeName
     * @return
     */
    public boolean isCachedInMemory(String typeName) throws AtlasException {
        return super.has(typeName);
    }

    /**
     * Check the type store for the requested type. 
     * If found in the type store, the type and any required super and attribute types
     * are loaded from the type store, and added to the cache.
     */
    @Override
    public IDataType onTypeFault(String typeName) throws AtlasException {

        // Type is not cached - check the type store.
        // Any super and attribute types needed by the requested type
        // which are not cached will also be loaded from the store.
        Context context = new Context();
        TypesDef typesDef = getTypeFromStore(typeName, context);
        if (typesDef.isEmpty()) {
            // Type not found in the type store.
            return null;
        }

        // Add all types that were loaded from the store to the cache.
        TransientTypeSystem transientTypeSystem = typeSystem.createTransientTypeSystem(context.getTypesDef(), false);
        Map<String, IDataType> typesAdded = transientTypeSystem.getTypesAdded();
        putAll(typesAdded.values());
        return typesAdded.get(typeName);
    }

    private void getTypeFromCacheOrStore(String typeName, Context context)
            throws AtlasException {

        if (coreTypes.contains(typeName) || super.has(typeName)) {
            return;
        }

        if (context.isLoadedFromStore(typeName)) {
            return;
        }

        // Type not cached and hasn't been loaded during this operation, so check the store.
        TypesDef typesDef = getTypeFromStore(typeName, context);
        if (typesDef.isEmpty()) {
            // Attribute type not found in cache or store.
            throw new AtlasException(typeName + " not found in type store");
        }
    }

    private TypesDef getTypeFromStore(String typeName, Context context)
            throws AtlasException {

        TypesDef typesDef = typeStore.restoreType(typeName);
        if (!typesDef.isEmpty()) {
            // Type found in store, add it to lists.
            context.addTypesDefToLists(typesDef);

            // Check the attribute and super types that are
            // used by the requested type, and restore them
            // as needed.
            checkAttributeAndSuperTypes(typesDef, context);
        }
        return typesDef;
    }

    private void checkAttributeAndSuperTypes(TypesDef typesDef, Context context)
            throws AtlasException {

        // Check the cache and store for attribute types and super types.
        for (HierarchicalTypeDefinition<ClassType> classTypeDef : typesDef.classTypesAsJavaList()) {
            checkAttributeTypes(classTypeDef.attributeDefinitions, context);
            for (String superTypeName : classTypeDef.superTypes) {
                getTypeFromCacheOrStore(superTypeName, context);
            }
        }
        for (HierarchicalTypeDefinition<TraitType> traitTypeDef : typesDef.traitTypesAsJavaList()) {
            checkAttributeTypes(traitTypeDef.attributeDefinitions, context);
            for (String superTypeName : traitTypeDef.superTypes) {
                getTypeFromCacheOrStore(superTypeName, context);
            }
        }
        for (StructTypeDefinition structTypeDef : typesDef.structTypesAsJavaList()) {
            checkAttributeTypes(structTypeDef.attributeDefinitions, context);
        }
    }

    private void checkAttributeTypes(AttributeDefinition[] attributeDefinitions,
        Context context) throws AtlasException {

        for (AttributeDefinition attrDef : attributeDefinitions) {
            checkAttributeType(attrDef, context);
        }
    }

    private void checkAttributeType(AttributeDefinition attrDef, Context context) throws AtlasException {

        List<String> typeNamesToLookup = new ArrayList<>(2);

        // Get the attribute type(s).
        String elementTypeName = TypeUtils.parseAsArrayType(attrDef.dataTypeName);
        if (elementTypeName != null) {
            // Array attribute, lookup the element type.
            typeNamesToLookup.add(elementTypeName);
        }
        else {
            String[] mapTypeNames = TypeUtils.parseAsMapType(attrDef.dataTypeName);
            if (mapTypeNames != null) {
                // Map attribute, lookup the key and value types.
                typeNamesToLookup.addAll(Arrays.asList(mapTypeNames));
            }
            else {
                // Not an array or map, lookup the attribute type.
                typeNamesToLookup.add(attrDef.dataTypeName);
            }
        }

        for (String typeName : typeNamesToLookup) {
            getTypeFromCacheOrStore(typeName, context);
        }
    }
}