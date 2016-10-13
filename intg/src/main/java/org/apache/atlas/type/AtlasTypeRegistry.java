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

import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.typedef.AtlasBaseTypeDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_SUFFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_SUFFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_KEY_VAL_SEP;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * registry for all types defined in Atlas.
 */
public class AtlasTypeRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasStructType.class);

    private final Map<String, AtlasType>               allTypes;
    private final TypeDefCache<AtlasEnumDef>           enumDefs;
    private final TypeDefCache<AtlasStructDef>         structDefs;
    private final TypeDefCache<AtlasClassificationDef> classificationDefs;
    private final TypeDefCache<AtlasEntityDef>         entityDefs;

    public AtlasTypeRegistry() {
        allTypes           = new ConcurrentHashMap<>();
        enumDefs           = new TypeDefCache<>(this);
        structDefs         = new TypeDefCache<>(this);
        classificationDefs = new TypeDefCache<>(this);
        entityDefs         = new TypeDefCache<>(this);

        registerType(new AtlasBuiltInTypes.AtlasBooleanType());
        registerType(new AtlasBuiltInTypes.AtlasByteType());
        registerType(new AtlasBuiltInTypes.AtlasShortType());
        registerType(new AtlasBuiltInTypes.AtlasIntType());
        registerType(new AtlasBuiltInTypes.AtlasLongType());
        registerType(new AtlasBuiltInTypes.AtlasFloatType());
        registerType(new AtlasBuiltInTypes.AtlasDoubleType());
        registerType(new AtlasBuiltInTypes.AtlasBigIntegerType());
        registerType(new AtlasBuiltInTypes.AtlasBigDecimalType());
        registerType(new AtlasBuiltInTypes.AtlasDateType());
        registerType(new AtlasBuiltInTypes.AtlasStringType());
        registerType(new AtlasBuiltInTypes.AtlasObjectIdType());
    }

    public Collection<String> getAllTypeNames() { return Collections.unmodifiableSet(allTypes.keySet()); }

    public AtlasType getType(String typeName) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.getType({})", typeName);
        }

        AtlasType ret = allTypes.get(typeName);

        if (ret == null) {
            try {
                if (typeName.startsWith(ATLAS_TYPE_ARRAY_PREFIX) && typeName.endsWith(ATLAS_TYPE_ARRAY_SUFFIX)) {
                    int startIdx = ATLAS_TYPE_ARRAY_PREFIX.length();
                    int endIdx = typeName.length() - ATLAS_TYPE_ARRAY_SUFFIX.length();
                    String elementTypeName = typeName.substring(startIdx, endIdx);

                    ret = new AtlasArrayType(elementTypeName, this);
                } else if (typeName.startsWith(ATLAS_TYPE_MAP_PREFIX) && typeName.endsWith(ATLAS_TYPE_MAP_SUFFIX)) {
                    int startIdx = ATLAS_TYPE_MAP_PREFIX.length();
                    int endIdx = typeName.length() - ATLAS_TYPE_MAP_SUFFIX.length();
                    String[] keyValueTypes = typeName.substring(startIdx, endIdx).split(ATLAS_TYPE_MAP_KEY_VAL_SEP, 2);
                    String keyTypeName = keyValueTypes.length > 0 ? keyValueTypes[0] : null;
                    String valueTypeName = keyValueTypes.length > 1 ? keyValueTypes[1] : null;

                    ret = new AtlasMapType(keyTypeName, valueTypeName, this);
                }
            } catch(AtlasBaseException excp) {
                LOG.warn("failed to instantiate type for " + typeName, excp);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.getType({})", typeName);
        }

        return ret;
    }


    public void addType(AtlasBaseTypeDef typeDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addType({})", typeDef);
        }

        if (typeDef == null) {
            // ignore
        } else if (typeDef.getClass().equals(AtlasEnumDef.class)) {
            AtlasEnumDef enumDef = (AtlasEnumDef)typeDef;

            enumDefs.addType(enumDef, new AtlasEnumType(enumDef));
        } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
            AtlasStructDef structDef = (AtlasStructDef)typeDef;

            structDefs.addType(structDef, new AtlasStructType(structDef, this));
        } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
            AtlasClassificationDef classificationDef = (AtlasClassificationDef)typeDef;

            classificationDefs.addType(classificationDef, new AtlasClassificationType(classificationDef, this));
        } else if (typeDef.getClass().equals(AtlasEntityDef.class)) {
            AtlasEntityDef entityDef = (AtlasEntityDef)typeDef;

            entityDefs.addType(entityDef, new AtlasEntityType(entityDef, this));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addType({})", typeDef);
        }
    }

    public void addTypeWithNoRefResolve(AtlasBaseTypeDef typeDef) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addTypeWithNoRefResolve({})", typeDef);
        }

        if (typeDef == null) {
            // ignore
        } else if (typeDef.getClass().equals(AtlasEnumDef.class)) {
            AtlasEnumDef enumDef = (AtlasEnumDef)typeDef;

            enumDefs.addType(enumDef, new AtlasEnumType(enumDef));
        } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
            AtlasStructDef structDef = (AtlasStructDef)typeDef;

            structDefs.addType(structDef, new AtlasStructType(structDef));
        } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
            AtlasClassificationDef classificationDef = (AtlasClassificationDef)typeDef;

            classificationDefs.addType(classificationDef, new AtlasClassificationType(classificationDef));
        } else if (typeDef.getClass().equals(AtlasEntityDef.class)) {
            AtlasEntityDef entityDef = (AtlasEntityDef)typeDef;

            entityDefs.addType(entityDef, new AtlasEntityType(entityDef));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addTypeWithNoRefResolve({})", typeDef);
        }
    }

    public void updateType(AtlasBaseTypeDef typeDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.updateType({})", typeDef);
        }

        if (typeDef == null) {
            // ignore
        } else if (StringUtils.isNotBlank(typeDef.getGuid())) {
            updateTypeByGuid(typeDef.getGuid(), typeDef);
        } else if (StringUtils.isNotBlank(typeDef.getName())) {
            updateTypeByName(typeDef.getName(), typeDef);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.updateType({})", typeDef);
        }
    }

    public void updateTypeWithNoRefResolve(AtlasBaseTypeDef typeDef) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.updateType({})", typeDef);
        }

        if (typeDef == null) {
            // ignore
        } else if (StringUtils.isNotBlank(typeDef.getGuid())) {
            updateTypeByGuidWithNoRefResolve(typeDef.getGuid(), typeDef);
        } else if (StringUtils.isNotBlank(typeDef.getName())) {
            updateTypeByNameWithNoRefResolve(typeDef.getName(), typeDef);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.updateType({})", typeDef);
        }
    }

    public void updateTypeByGuid(String guid, AtlasBaseTypeDef typeDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.updateTypeByGuid({})", guid);
        }

        if (guid == null || typeDef == null) {
            // ignore
        } else if (typeDef.getClass().equals(AtlasEnumDef.class)) {
            AtlasEnumDef enumDef = (AtlasEnumDef)typeDef;

            enumDefs.removeTypeDefByGuid(guid);
            enumDefs.addType(enumDef, new AtlasEnumType(enumDef));
        } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
            AtlasStructDef structDef = (AtlasStructDef)typeDef;

            structDefs.removeTypeDefByGuid(guid);
            structDefs.addType(structDef, new AtlasStructType(structDef, this));
        } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
            AtlasClassificationDef classificationDef = (AtlasClassificationDef)typeDef;

            classificationDefs.removeTypeDefByGuid(guid);
            classificationDefs.addType(classificationDef, new AtlasClassificationType(classificationDef, this));
        } else if (typeDef.getClass().equals(AtlasEntityDef.class)) {
            AtlasEntityDef entityDef = (AtlasEntityDef)typeDef;

            entityDefs.removeTypeDefByGuid(guid);
            entityDefs.addType(entityDef, new AtlasEntityType(entityDef, this));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.updateTypeByGuid({})", guid);
        }
    }

    public void updateTypeByName(String name, AtlasBaseTypeDef typeDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.updateEnumDefByName({})", name);
        }

        if (name == null || typeDef == null) {
            // ignore
        } else if (typeDef.getClass().equals(AtlasEnumDef.class)) {
            AtlasEnumDef enumDef = (AtlasEnumDef)typeDef;

            enumDefs.removeTypeDefByName(name);
            enumDefs.addType(enumDef, new AtlasEnumType(enumDef));
        } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
            AtlasStructDef structDef = (AtlasStructDef)typeDef;

            structDefs.removeTypeDefByName(name);
            structDefs.addType(structDef, new AtlasStructType(structDef, this));
        } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
            AtlasClassificationDef classificationDef = (AtlasClassificationDef)typeDef;

            classificationDefs.removeTypeDefByName(name);
            classificationDefs.addType(classificationDef, new AtlasClassificationType(classificationDef, this));
        } else if (typeDef.getClass().equals(AtlasEntityDef.class)) {
            AtlasEntityDef entityDef = (AtlasEntityDef)typeDef;

            entityDefs.removeTypeDefByName(name);
            entityDefs.addType(entityDef, new AtlasEntityType(entityDef, this));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.updateEnumDefByName({})", name);
        }
    }

    public void updateTypeByGuidWithNoRefResolve(String guid, AtlasBaseTypeDef typeDef) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.updateTypeByGuidWithNoRefResolve({})", guid);
        }

        if (guid == null || typeDef == null) {
            // ignore
        } else if (typeDef.getClass().equals(AtlasEnumDef.class)) {
            AtlasEnumDef enumDef = (AtlasEnumDef)typeDef;

            enumDefs.removeTypeDefByGuid(guid);
            enumDefs.addType(enumDef, new AtlasEnumType(enumDef));
        } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
            AtlasStructDef structDef = (AtlasStructDef)typeDef;

            structDefs.removeTypeDefByGuid(guid);
            structDefs.addType(structDef, new AtlasStructType(structDef));
        } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
            AtlasClassificationDef classificationDef = (AtlasClassificationDef)typeDef;

            classificationDefs.removeTypeDefByGuid(guid);
            classificationDefs.addType(classificationDef, new AtlasClassificationType(classificationDef));
        } else if (typeDef.getClass().equals(AtlasEntityDef.class)) {
            AtlasEntityDef entityDef = (AtlasEntityDef)typeDef;

            entityDefs.removeTypeDefByGuid(guid);
            entityDefs.addType(entityDef, new AtlasEntityType(entityDef));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.updateTypeByGuidWithNoRefResolve({})", guid);
        }
    }

    public void updateTypeByNameWithNoRefResolve(String name, AtlasBaseTypeDef typeDef) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.updateTypeByNameWithNoRefResolve({})", name);
        }

        if (name == null || typeDef == null) {
            // ignore
        } else if (typeDef.getClass().equals(AtlasEnumDef.class)) {
            AtlasEnumDef enumDef = (AtlasEnumDef)typeDef;

            enumDefs.removeTypeDefByName(name);
            enumDefs.addType(enumDef, new AtlasEnumType(enumDef));
        } else if (typeDef.getClass().equals(AtlasStructDef.class)) {
            AtlasStructDef structDef = (AtlasStructDef)typeDef;

            structDefs.removeTypeDefByName(name);
            structDefs.addType(structDef, new AtlasStructType(structDef));
        } else if (typeDef.getClass().equals(AtlasClassificationDef.class)) {
            AtlasClassificationDef classificationDef = (AtlasClassificationDef)typeDef;

            classificationDefs.removeTypeDefByName(name);
            classificationDefs.addType(classificationDef, new AtlasClassificationType(classificationDef));
        } else if (typeDef.getClass().equals(AtlasEntityDef.class)) {
            AtlasEntityDef entityDef = (AtlasEntityDef)typeDef;

            entityDefs.removeTypeDefByName(name);
            entityDefs.addType(entityDef, new AtlasEntityType(entityDef));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.updateTypeByNameWithNoRefResolve({})", name);
        }
    }

    public void removeTypeByGuid(String guid) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.removeTypeByGuid({})", guid);
        }

        if (guid != null) {
            enumDefs.removeTypeDefByGuid(guid);
            structDefs.removeTypeDefByGuid(guid);
            classificationDefs.removeTypeDefByGuid(guid);
            entityDefs.removeTypeDefByGuid(guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.removeTypeByGuid({})", guid);
        }
    }

    public void removeTypeByName(String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.removeTypeByName({})", name);
        }

        if (name != null) {
            enumDefs.removeTypeDefByName(name);
            structDefs.removeTypeDefByName(name);
            classificationDefs.removeTypeDefByName(name);
            entityDefs.removeTypeDefByName(name);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.removeEnumDefByName({})", name);
        }
    }

    public void addTypes(Collection<? extends AtlasBaseTypeDef> typeDefs) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addTypes(length={})", (typeDefs == null ? 0 : typeDefs.size()));
        }

        if (CollectionUtils.isNotEmpty(typeDefs)) {
            for (AtlasBaseTypeDef typeDef : typeDefs) {
                addType(typeDef);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addTypes(length={})", (typeDefs == null ? 0 : typeDefs.size()));
        }
    }

    public void addTypesWithNoRefResolve(Collection<? extends AtlasBaseTypeDef> typeDefs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addTypes(length={})", (typeDefs == null ? 0 : typeDefs.size()));
        }

        if (CollectionUtils.isNotEmpty(typeDefs)) {
            for (AtlasBaseTypeDef typeDef : typeDefs) {
                addTypeWithNoRefResolve(typeDef);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addTypes(length={})", (typeDefs == null ? 0 : typeDefs.size()));
        }
    }

    public void updateTypes(Collection<? extends AtlasBaseTypeDef> typeDefs) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.updateTypes(length={})", (typeDefs == null ? 0 : typeDefs.size()));
        }

        if (CollectionUtils.isNotEmpty(typeDefs)) {
            for (AtlasBaseTypeDef typeDef : typeDefs) {
                updateType(typeDef);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.updateTypes(length={})", (typeDefs == null ? 0 : typeDefs.size()));
        }
    }

    public void updateTypesWithNoRefResolve(Collection<? extends AtlasBaseTypeDef> typeDefs) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.updateTypesWithNoRefResolve(length={})", (typeDefs == null ? 0 : typeDefs.size()));
        }

        if (CollectionUtils.isNotEmpty(typeDefs)) {
            for (AtlasBaseTypeDef typeDef : typeDefs) {
                updateTypeWithNoRefResolve(typeDef);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.updateTypesWithNoRefResolve(length={})", (typeDefs == null ? 0 : typeDefs.size()));
        }
    }


    public Collection<AtlasEnumDef> getAllEnumDefs() { return enumDefs.getAll(); }

    public AtlasEnumDef getEnumDefByGuid(String guid) {
        return enumDefs.getTypeDefByGuid(guid);
    }

    public AtlasEnumDef getEnumDefByName(String name) {
        return enumDefs.getTypeDefByName(name);
    }


    public Collection<AtlasStructDef> getAllStructDefs() { return structDefs.getAll(); }

    public AtlasStructDef getStructDefByGuid(String guid) {
        return structDefs.getTypeDefByGuid(guid);
    }

    public AtlasStructDef getStructDefByName(String name) { return structDefs.getTypeDefByName(name); }


    public Collection<AtlasClassificationDef> getAllClassificationDefs() { return classificationDefs.getAll(); }

    public AtlasClassificationDef getClassificationDefByGuid(String guid) {
        return classificationDefs.getTypeDefByGuid(guid);
    }

    public AtlasClassificationDef getClassificationDefByName(String name) {
        return classificationDefs.getTypeDefByName(name);
    }


    public Collection<AtlasEntityDef> getAllEntityDefs() { return entityDefs.getAll(); }

    public AtlasEntityDef getEntityDefByGuid(String guid) {
        return entityDefs.getTypeDefByGuid(guid);
    }

    public AtlasEntityDef getEntityDefByName(String name) {
        return entityDefs.getTypeDefByName(name);
    }


    public void resolveReferences() throws AtlasBaseException {
        for (Map.Entry<String, AtlasType> e : allTypes.entrySet()) {
            e.getValue().resolveReferences(this);
        }
    }


    private void registerType(AtlasType dataType) {
        allTypes.put(dataType.getTypeName(), dataType);
    }

    private void unregisterType(AtlasType dataType) {
        allTypes.remove(dataType.getTypeName());
    }

    private void unregisterTypeByName(String typeName) {
        allTypes.remove(typeName);
    }


    class TypeDefCache<T extends AtlasBaseTypeDef> {
        private final AtlasTypeRegistry typeRegistry;
        private final Map<String, T>    typeDefGuidMap = new ConcurrentHashMap<String, T>();
        private final Map<String, T>    typeDefNameMap = new ConcurrentHashMap<String, T>();

        public TypeDefCache(AtlasTypeRegistry typeRegistry) {
            this.typeRegistry = typeRegistry;
        }

        public void addType(T typeDef, AtlasType type) {
            if (type != null) {
                if (StringUtils.isNotEmpty(typeDef.getGuid())) {
                    typeDefGuidMap.put(typeDef.getGuid(), typeDef);
                }

                if (StringUtils.isNotEmpty(typeDef.getName())) {
                    typeDefNameMap.put(typeDef.getName(), typeDef);
                }

                typeRegistry.registerType(type);
            }
        }

        public Collection<T> getAll() {
            return Collections.unmodifiableCollection(typeDefNameMap.values());
        }

        public T getTypeDefByGuid(String guid) {
            T ret = guid != null ? typeDefGuidMap.get(guid) : null;

            return ret;
        }

        public T getTypeDefByName(String name) {
            T ret = name != null ? typeDefNameMap.get(name) : null;

            return ret;
        }

        public void removeTypeDefByGuid(String guid) {
            T      typeDef = guid != null ? typeDefGuidMap.remove(guid) : null;
            String name    = typeDef != null ? typeDef.getName() : null;

            if (name != null) {
                typeDefNameMap.remove(name);
                typeRegistry.unregisterTypeByName(name);
            }
        }

        public void removeTypeDefByName(String name) {
            T      typeDef = name != null ? typeDefNameMap.get(name) : null;
            String guid    = typeDef != null ? typeDef.getGuid() : null;

            if (guid != null) {
                typeDefGuidMap.remove(guid);
            }

            if (name != null) {
                typeRegistry.unregisterTypeByName(name);
            }
        }
    }
}
