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
package org.apache.atlas.type;

import com.sun.jersey.spi.resource.Singleton;

import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_SUFFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_KEY_VAL_SEP;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_SUFFIX;

/**
 * registry for all types defined in Atlas.
 */
@Singleton
public class AtlasTypeRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasStructType.class);

    protected RegistryData registryData;

    public AtlasTypeRegistry() {
        registryData = new RegistryData();
    }

    protected AtlasTypeRegistry(AtlasTypeRegistry other) {
        registryData = new RegistryData(other.registryData);
    }

    public Collection<String> getAllTypeNames() { return registryData.allTypes.getAllTypeNames(); }

    public boolean isRegisteredType(String typeName) {
        return registryData.allTypes.isKnownType(typeName);
    }

    public AtlasType getType(String typeName) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.getType({})", typeName);
        }

        AtlasType ret = registryData.allTypes.getTypeByName(typeName);

        if (ret == null) {
            if (typeName.startsWith(ATLAS_TYPE_ARRAY_PREFIX) && typeName.endsWith(ATLAS_TYPE_ARRAY_SUFFIX)) {
                int    startIdx        = ATLAS_TYPE_ARRAY_PREFIX.length();
                int    endIdx          = typeName.length() - ATLAS_TYPE_ARRAY_SUFFIX.length();
                String elementTypeName = typeName.substring(startIdx, endIdx);

                ret = new AtlasArrayType(elementTypeName, this);
            } else if (typeName.startsWith(ATLAS_TYPE_MAP_PREFIX) && typeName.endsWith(ATLAS_TYPE_MAP_SUFFIX)) {
                int      startIdx      = ATLAS_TYPE_MAP_PREFIX.length();
                int      endIdx        = typeName.length() - ATLAS_TYPE_MAP_SUFFIX.length();
                String[] keyValueTypes = typeName.substring(startIdx, endIdx).split(ATLAS_TYPE_MAP_KEY_VAL_SEP, 2);
                String   keyTypeName   = keyValueTypes.length > 0 ? keyValueTypes[0] : null;
                String   valueTypeName = keyValueTypes.length > 1 ? keyValueTypes[1] : null;

                ret = new AtlasMapType(keyTypeName, valueTypeName, this);
            } else {
                throw new AtlasBaseException(AtlasErrorCode.UNKNOWN_TYPENAME, typeName);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.getType({}): {}", typeName, ret);
        }

        return ret;
    }

    public AtlasType getTypeByGuid(String guid) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.getTypeByGuid({})", guid);
        }

        AtlasType ret = registryData.allTypes.getTypeByGuid(guid);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.getTypeByGuid({}): {}", guid, ret);
        }

        return ret;
    }

    public AtlasBaseTypeDef getTypeDefByName(String name) { return registryData.getTypeDefByName(name); }

    public AtlasBaseTypeDef getTypeDefByGuid(String guid) { return registryData.getTypeDefByGuid(guid); }

    public Collection<AtlasEnumDef> getAllEnumDefs() { return registryData.enumDefs.getAll(); }

    public AtlasEnumDef getEnumDefByGuid(String guid) {
        return registryData.enumDefs.getTypeDefByGuid(guid);
    }

    public AtlasEnumDef getEnumDefByName(String name) {
        return registryData.enumDefs.getTypeDefByName(name);
    }


    public Collection<AtlasStructDef> getAllStructDefs() { return registryData.structDefs.getAll(); }

    public AtlasStructDef getStructDefByGuid(String guid) {
        return registryData.structDefs.getTypeDefByGuid(guid);
    }

    public AtlasStructDef getStructDefByName(String name) { return registryData.structDefs.getTypeDefByName(name); }


    public Collection<AtlasClassificationDef> getAllClassificationDefs() {
        return registryData.classificationDefs.getAll();
    }

    public AtlasClassificationDef getClassificationDefByGuid(String guid) {
        return registryData.classificationDefs.getTypeDefByGuid(guid);
    }

    public AtlasClassificationDef getClassificationDefByName(String name) {
        return registryData.classificationDefs.getTypeDefByName(name);
    }


    public Collection<AtlasEntityDef> getAllEntityDefs() { return registryData.entityDefs.getAll(); }

    public AtlasEntityDef getEntityDefByGuid(String guid) {
        return registryData.entityDefs.getTypeDefByGuid(guid);
    }

    public AtlasEntityDef getEntityDefByName(String name) {
        return registryData.entityDefs.getTypeDefByName(name);
    }

    public AtlasTransientTypeRegistry createTransientTypeRegistry() {
        return new AtlasTransientTypeRegistry(this);
    }

    public void commitTransientTypeRegistry(AtlasTransientTypeRegistry transientTypeRegistry) {
        this.registryData = transientTypeRegistry.registryData;
    }

    static class RegistryData {
        final TypeCache                            allTypes;
        final TypeDefCache<AtlasEnumDef>           enumDefs;
        final TypeDefCache<AtlasStructDef>         structDefs;
        final TypeDefCache<AtlasClassificationDef> classificationDefs;
        final TypeDefCache<AtlasEntityDef>         entityDefs;
        final TypeDefCache<? extends AtlasBaseTypeDef>[] allDefCaches;

        RegistryData() {
            allTypes           = new TypeCache();
            enumDefs           = new TypeDefCache<>(allTypes);
            structDefs         = new TypeDefCache<>(allTypes);
            classificationDefs = new TypeDefCache<>(allTypes);
            entityDefs         = new TypeDefCache<>(allTypes);
            allDefCaches       = new TypeDefCache[] { enumDefs, structDefs, classificationDefs, entityDefs };

            allTypes.addType(new AtlasBuiltInTypes.AtlasBooleanType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasByteType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasShortType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasIntType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasLongType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasFloatType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasDoubleType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasBigIntegerType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasBigDecimalType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasDateType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasStringType());
            allTypes.addType(new AtlasBuiltInTypes.AtlasObjectIdType());
        }

        RegistryData(RegistryData other) {
            allTypes           = new TypeCache(other.allTypes);
            enumDefs           = new TypeDefCache<>(other.enumDefs, allTypes);
            structDefs         = new TypeDefCache<>(other.structDefs, allTypes);
            classificationDefs = new TypeDefCache<>(other.classificationDefs, allTypes);
            entityDefs         = new TypeDefCache<>(other.entityDefs, allTypes);
            allDefCaches       = new TypeDefCache[] { enumDefs, structDefs, classificationDefs, entityDefs };
        }

        AtlasBaseTypeDef getTypeDefByName(String name) {
            AtlasBaseTypeDef ret = null;

            if (name != null) {
                for (TypeDefCache typeDefCache : allDefCaches) {
                    ret = typeDefCache.getTypeDefByName(name);

                    if (ret != null) {
                        break;
                    }
                }
            }

            return ret;
        }

        AtlasBaseTypeDef getTypeDefByGuid(String guid) {
            AtlasBaseTypeDef ret = null;

            if (guid != null) {
                for (TypeDefCache typeDefCache : allDefCaches) {
                    ret = typeDefCache.getTypeDefByGuid(guid);

                    if (ret != null) {
                        break;
                    }
                }
            }

            return ret;
        }

        void updateGuid(String typeName, String guid) {
            if (typeName != null) {
                enumDefs.updateGuid(typeName, guid);
                structDefs.updateGuid(typeName, guid);
                classificationDefs.updateGuid(typeName, guid);
                entityDefs.updateGuid(typeName, guid);
            }
        }

        void removeByGuid(String guid) {
            if (guid != null) {
                enumDefs.removeTypeDefByGuid(guid);
                structDefs.removeTypeDefByGuid(guid);
                classificationDefs.removeTypeDefByGuid(guid);
                entityDefs.removeTypeDefByGuid(guid);
            }
        }

        void removeByName(String typeName) {
            if (typeName != null) {
                enumDefs.removeTypeDefByName(typeName);
                structDefs.removeTypeDefByName(typeName);
                classificationDefs.removeTypeDefByName(typeName);
                entityDefs.removeTypeDefByName(typeName);
            }
        }
    }

    public static class AtlasTransientTypeRegistry extends AtlasTypeRegistry {
        private List<AtlasBaseTypeDef> addedTypes   = new ArrayList<>();
        private List<AtlasBaseTypeDef> updatedTypes = new ArrayList<>();
        private List<AtlasBaseTypeDef> deletedTypes = new ArrayList<>();


        private AtlasTransientTypeRegistry(AtlasTypeRegistry parent) {
            super(parent);
        }

        private void resolveReferences() throws AtlasBaseException {
            for (AtlasType type : registryData.allTypes.getAllTypes()) {
                type.resolveReferences(this);
            }
        }

        public void addType(AtlasBaseTypeDef typeDef) throws AtlasBaseException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.addType({})", typeDef);
            }

            if (typeDef != null) {
                addTypeWithNoRefResolve(typeDef);

                resolveReferences();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.addType({})", typeDef);
            }
        }

        public void updateGuid(String typeName, String guid) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.updateGuid({}, {})", typeName, guid);
            }

            registryData.updateGuid(typeName, guid);

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.updateGuid({}, {})", typeName, guid);
            }
        }

        public void addTypes(Collection<? extends AtlasBaseTypeDef> typeDefs) throws AtlasBaseException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.addTypes(length={})", (typeDefs == null ? 0 : typeDefs.size()));
            }

            if (CollectionUtils.isNotEmpty(typeDefs)) {
                addTypesWithNoRefResolve(typeDefs);

                resolveReferences();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.addTypes(length={})", (typeDefs == null ? 0 : typeDefs.size()));
            }
        }

        public void addTypes(AtlasTypesDef typesDef) throws AtlasBaseException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.addTypes({})", typesDef);
            }

            if (typesDef != null) {
                addTypesWithNoRefResolve(typesDef.getEnumDefs());
                addTypesWithNoRefResolve(typesDef.getStructDefs());
                addTypesWithNoRefResolve(typesDef.getClassificationDefs());
                addTypesWithNoRefResolve(typesDef.getEntityDefs());

                resolveReferences();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.addTypes({})", typesDef);
            }
        }

        public void updateType(AtlasBaseTypeDef typeDef) throws AtlasBaseException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.updateType({})", typeDef);
            }

            if (typeDef != null) {
                updateTypeWithNoRefResolve(typeDef);

                resolveReferences();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.updateType({})", typeDef);
            }
        }

        public void updateTypeByGuid(String guid, AtlasBaseTypeDef typeDef) throws AtlasBaseException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.updateTypeByGuid({})", guid);
            }

            if (guid != null && typeDef != null) {
                updateTypeByGuidWithNoRefResolve(guid, typeDef);

                resolveReferences();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.updateTypeByGuid({})", guid);
            }
        }

        public void updateTypeByName(String name, AtlasBaseTypeDef typeDef) throws AtlasBaseException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.updateEnumDefByName({})", name);
            }

            if (name != null && typeDef != null) {
                updateTypeByNameWithNoRefResolve(name, typeDef);

                resolveReferences();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.updateEnumDefByName({})", name);
            }
        }

        public void updateTypes(Collection<? extends AtlasBaseTypeDef> typeDefs) throws AtlasBaseException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.updateTypes(length={})", (typeDefs == null ? 0 : typeDefs.size()));
            }

            if (CollectionUtils.isNotEmpty(typeDefs)) {
                updateTypesWithNoRefResolve(typeDefs);

                resolveReferences();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.updateTypes(length={})", (typeDefs == null ? 0 : typeDefs.size()));
            }
        }

        public void updateTypes(AtlasTypesDef typesDef) throws AtlasBaseException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.updateTypes({})", typesDef);
            }

            if (typesDef != null) {
                updateTypesWithNoRefResolve(typesDef.getEnumDefs());
                updateTypesWithNoRefResolve(typesDef.getStructDefs());
                updateTypesWithNoRefResolve(typesDef.getClassificationDefs());
                updateTypesWithNoRefResolve(typesDef.getEntityDefs());

                resolveReferences();
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.updateTypes({})", typesDef);
            }
        }

        public void removeTypeByGuid(String guid) throws AtlasBaseException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.removeTypeByGuid({})", guid);
            }

            if (guid != null) {
                AtlasBaseTypeDef typeDef = getTypeDefByGuid(guid);

                registryData.removeByGuid(guid);

                resolveReferences();

                if (typeDef != null) {
                    deletedTypes.add(typeDef);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.removeTypeByGuid({})", guid);
            }
        }

        public void removeTypeByName(String name) throws AtlasBaseException {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.removeTypeByName({})", name);
            }

            if (name != null) {
                AtlasBaseTypeDef typeDef = getTypeDefByName(name);

                registryData.removeByName(name);

                resolveReferences();

                if (typeDef != null) {
                    deletedTypes.add(typeDef);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.removeEnumDefByName({})", name);
            }
        }

        public List<AtlasBaseTypeDef> getAddedTypes() { return addedTypes; }

        public List<AtlasBaseTypeDef> getUpdatedTypes() { return updatedTypes; }

        public List<AtlasBaseTypeDef> getDeleteedTypes() { return deletedTypes; }


        private void addTypeWithNoRefResolve(AtlasBaseTypeDef typeDef) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.addTypeWithNoRefResolve({})", typeDef);
            }

            if (typeDef != null) {
                if (typeDef.getClass().equals(AtlasEnumDef.class)) {
                    AtlasEnumDef enumDef = (AtlasEnumDef) typeDef;

                    registryData.enumDefs.addType(enumDef, new AtlasEnumType(enumDef));
                } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
                    AtlasStructDef structDef = (AtlasStructDef) typeDef;

                    registryData.structDefs.addType(structDef, new AtlasStructType(structDef));
                } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
                    AtlasClassificationDef classificationDef = (AtlasClassificationDef) typeDef;

                    registryData.classificationDefs.addType(classificationDef,
                                                            new AtlasClassificationType(classificationDef));
                } else if (typeDef.getClass().equals(AtlasEntityDef.class)) {
                    AtlasEntityDef entityDef = (AtlasEntityDef) typeDef;

                    registryData.entityDefs.addType(entityDef, new AtlasEntityType(entityDef));
                }

                addedTypes.add(typeDef);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.addTypeWithNoRefResolve({})", typeDef);
            }
        }

        private void addTypesWithNoRefResolve(Collection<? extends AtlasBaseTypeDef> typeDefs) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.addTypesWithNoRefResolve(length={})",
                          (typeDefs == null ? 0 : typeDefs.size()));
            }

            if (CollectionUtils.isNotEmpty(typeDefs)) {
                for (AtlasBaseTypeDef typeDef : typeDefs) {
                    addTypeWithNoRefResolve(typeDef);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.addTypesWithNoRefResolve(length={})",
                          (typeDefs == null ? 0 : typeDefs.size()));
            }
        }

        private void updateTypeWithNoRefResolve(AtlasBaseTypeDef typeDef) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.updateType({})", typeDef);
            }

            if (typeDef != null) {
                if (StringUtils.isNotBlank(typeDef.getGuid())) {
                    updateTypeByGuidWithNoRefResolve(typeDef.getGuid(), typeDef);
                } else if (StringUtils.isNotBlank(typeDef.getName())) {
                    updateTypeByNameWithNoRefResolve(typeDef.getName(), typeDef);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.updateType({})", typeDef);
            }
        }

        private void updateTypeByGuidWithNoRefResolve(String guid, AtlasBaseTypeDef typeDef) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.updateTypeByGuidWithNoRefResolve({})", guid);
            }

            if (guid != null && typeDef != null) {
                // ignore
                if (typeDef.getClass().equals(AtlasEnumDef.class)) {
                    AtlasEnumDef enumDef = (AtlasEnumDef) typeDef;

                    registryData.enumDefs.removeTypeDefByGuid(guid);
                    registryData.enumDefs.addType(enumDef, new AtlasEnumType(enumDef));
                } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
                    AtlasStructDef structDef = (AtlasStructDef) typeDef;

                    registryData.structDefs.removeTypeDefByGuid(guid);
                    registryData.structDefs.addType(structDef, new AtlasStructType(structDef));
                } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
                    AtlasClassificationDef classificationDef = (AtlasClassificationDef) typeDef;

                    registryData.classificationDefs.removeTypeDefByGuid(guid);
                    registryData.classificationDefs.addType(classificationDef,
                                                            new AtlasClassificationType(classificationDef));
                } else if (typeDef.getClass().equals(AtlasEntityDef.class)) {
                    AtlasEntityDef entityDef = (AtlasEntityDef) typeDef;

                    registryData.entityDefs.removeTypeDefByGuid(guid);
                    registryData.entityDefs.addType(entityDef, new AtlasEntityType(entityDef));
                }

                updatedTypes.add(typeDef);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.updateTypeByGuidWithNoRefResolve({})", guid);
            }
        }

        private void updateTypeByNameWithNoRefResolve(String name, AtlasBaseTypeDef typeDef) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.updateTypeByNameWithNoRefResolve({})", name);
            }

            if (name != null && typeDef != null) {
                if (typeDef.getClass().equals(AtlasEnumDef.class)) {
                    AtlasEnumDef enumDef = (AtlasEnumDef) typeDef;

                    registryData.enumDefs.removeTypeDefByName(name);
                    registryData.enumDefs.addType(enumDef, new AtlasEnumType(enumDef));
                } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
                    AtlasStructDef structDef = (AtlasStructDef) typeDef;

                    registryData.structDefs.removeTypeDefByName(name);
                    registryData.structDefs.addType(structDef, new AtlasStructType(structDef));
                } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
                    AtlasClassificationDef classificationDef = (AtlasClassificationDef) typeDef;

                    registryData.classificationDefs.removeTypeDefByName(name);
                    registryData.classificationDefs.addType(classificationDef,
                                                            new AtlasClassificationType(classificationDef));
                } else if (typeDef.getClass().equals(AtlasEntityDef.class)) {
                    AtlasEntityDef entityDef = (AtlasEntityDef) typeDef;

                    registryData.entityDefs.removeTypeDefByName(name);
                    registryData.entityDefs.addType(entityDef, new AtlasEntityType(entityDef));
                }

                updatedTypes.add(typeDef);
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.updateTypeByNameWithNoRefResolve({})", name);
            }
        }

        private void updateTypesWithNoRefResolve(Collection<? extends AtlasBaseTypeDef> typeDefs) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("==> AtlasTypeRegistry.updateTypesWithNoRefResolve(length={})",
                                                                             (typeDefs == null ? 0 : typeDefs.size()));
            }

            if (CollectionUtils.isNotEmpty(typeDefs)) {
                for (AtlasBaseTypeDef typeDef : typeDefs) {
                    updateTypeWithNoRefResolve(typeDef);
                }
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("<== AtlasTypeRegistry.updateTypesWithNoRefResolve(length={})",
                                                                              (typeDefs == null ? 0 : typeDefs.size()));
            }
        }
    }
}

class TypeCache {
    private final Map<String, AtlasType> typeGuidMap;
    private final Map<String, AtlasType> typeNameMap;

    public TypeCache() {
        typeGuidMap = new ConcurrentHashMap<>();
        typeNameMap = new ConcurrentHashMap<>();
    }

    public TypeCache(TypeCache other) {
        typeGuidMap = new ConcurrentHashMap<>(other.typeGuidMap);
        typeNameMap = new ConcurrentHashMap<>(other.typeNameMap);
    }

    public void addType(AtlasType type) {
        if (type != null) {
            if (StringUtils.isNotEmpty(type.getTypeName())) {
                typeNameMap.put(type.getTypeName(), type);
            }
        }
    }

    public void addType(AtlasBaseTypeDef typeDef, AtlasType type) {
        if (typeDef != null && type != null) {
            if (StringUtils.isNotEmpty(typeDef.getGuid())) {
                typeGuidMap.put(typeDef.getGuid(), type);
            }

            if (StringUtils.isNotEmpty(typeDef.getName())) {
                typeNameMap.put(typeDef.getName(), type);
            }
        }
    }

    public boolean isKnownType(String typeName) {
        return typeNameMap.containsKey(typeName);
    }

    public Collection<String> getAllTypeNames() {
        return Collections.unmodifiableCollection(typeNameMap.keySet());
    }

    public Collection<AtlasType> getAllTypes() {
        return Collections.unmodifiableCollection(typeNameMap.values());
    }

    public AtlasType getTypeByGuid(String guid) {

        return guid != null ? typeGuidMap.get(guid) : null;
    }

    public AtlasType getTypeByName(String name) {

        return name != null ? typeNameMap.get(name) : null;
    }

    public void updateGuid(String typeName, String currGuid, String newGuid) {
        if (currGuid != null) {
            typeGuidMap.remove(currGuid);
        }

        if (typeName != null && newGuid != null) {
            AtlasType type = typeNameMap.get(typeName);

            if (type != null) {
                typeGuidMap.put(newGuid, type);
            }
        }
    }

    public void removeTypeByGuid(String guid) {
        if (guid != null) {
            typeGuidMap.remove(guid);
        }
    }

    public void removeTypeByName(String name) {
        if (name != null) {
            typeNameMap.get(name);
        }
    }
}

class TypeDefCache<T extends AtlasBaseTypeDef> {
    private final TypeCache      typeCache;
    private final Map<String, T> typeDefGuidMap;
    private final Map<String, T> typeDefNameMap;

    public TypeDefCache(TypeCache typeCache) {
        this.typeCache      = typeCache;
        this.typeDefGuidMap = new ConcurrentHashMap<>();
        this.typeDefNameMap = new ConcurrentHashMap<>();
    }

    public TypeDefCache(TypeDefCache other, TypeCache typeCache) {
        this.typeCache      = typeCache;
        this.typeDefGuidMap = new ConcurrentHashMap<>(other.typeDefGuidMap);
        this.typeDefNameMap = new ConcurrentHashMap<>(other.typeDefNameMap);
    }

    public void addType(T typeDef, AtlasType type) {
        if (typeDef != null && type != null) {
            if (StringUtils.isNotEmpty(typeDef.getGuid())) {
                typeDefGuidMap.put(typeDef.getGuid(), typeDef);
            }

            if (StringUtils.isNotEmpty(typeDef.getName())) {
                typeDefNameMap.put(typeDef.getName(), typeDef);
            }

            typeCache.addType(typeDef, type);
        }
    }

    public Collection<T> getAll() {
        return Collections.unmodifiableCollection(typeDefNameMap.values());
    }

    public T getTypeDefByGuid(String guid) {

        return guid != null ? typeDefGuidMap.get(guid) : null;
    }

    public T getTypeDefByName(String name) {

        return name != null ? typeDefNameMap.get(name) : null;
    }

    public void updateGuid(String typeName, String newGuid) {
        if (typeName != null) {
            T typeDef = typeDefNameMap.get(typeName);

            if (typeDef != null) {
                String currGuid = typeDef.getGuid();

                if (!StringUtils.equals(currGuid, newGuid)) {
                    if (currGuid != null) {
                        typeDefGuidMap.remove(currGuid);
                    }

                    typeDef.setGuid(newGuid);

                    if (newGuid != null) {
                        typeDefGuidMap.put(newGuid, typeDef);
                    }

                    typeCache.updateGuid(typeName, currGuid, newGuid);
                }
            }
        }
    }

    public void removeTypeDefByGuid(String guid) {
        if (guid != null) {
            T typeDef = typeDefGuidMap.remove(guid);

            typeCache.removeTypeByGuid(guid);

            String name = typeDef != null ? typeDef.getName() : null;

            if (name != null) {
                typeDefNameMap.remove(name);
                typeCache.removeTypeByName(name);
            }

        }
    }

    public void removeTypeDefByName(String name) {
        if (name != null) {
            T typeDef = typeDefNameMap.remove(name);

            typeCache.removeTypeByName(name);

            String guid = typeDef != null ? typeDef.getGuid() : null;

            if (guid != null) {
                typeDefGuidMap.remove(guid);
                typeCache.removeTypeByGuid(guid);
            }
        }
    }
}
