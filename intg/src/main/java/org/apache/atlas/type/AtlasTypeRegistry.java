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
import org.apache.atlas.model.typedef.*;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_ARRAY_SUFFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_PREFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_SUFFIX;
import static org.apache.atlas.model.typedef.AtlasBaseTypeDef.ATLAS_TYPE_MAP_KEY_VAL_SEP;
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

    public void resolveReferences() throws AtlasBaseException {
        for (Map.Entry<String, AtlasType> e : allTypes.entrySet()) {
            e.getValue().resolveReferences(this);
        }
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


    public void addEnumDef(AtlasEnumDef enumDef) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addEnumDef({})", enumDef);
        }

        enumDefs.addType(enumDef, new AtlasEnumType(enumDef));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addEnumDef({})", enumDef);
        }
    }

    public Collection<AtlasEnumDef> getAllEnumDefs() { return enumDefs.getAll(); }

    public AtlasEnumDef getEnumDefByGuid(String guid) {
        return enumDefs.getTypeDefByGuid(guid);
    }

    public AtlasEnumDef getEnumDefByName(String name) {
        return enumDefs.getTypeDefByName(name);
    }

    public void removeEnumDefByGuid(String guid) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.removeEnumDefByGuid({})", guid);
        }

        AtlasEnumDef enumDef = enumDefs.getTypeDefByGuid(guid);

        if (enumDef != null) {
            enumDefs.removeTypeDefByGuid(guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.removeEnumDefByGuid({})", guid);
        }
    }

    public void removeEnumDefByName(String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.removeEnumDefByName({})", name);
        }

        AtlasEnumDef enumDef = enumDefs.getTypeDefByName(name);

        if (enumDef != null) {
            enumDefs.removeTypeDefByName(name);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.removeEnumDefByName({})", name);
        }
    }


    public void addStructDefWithNoRefResolve(AtlasStructDef structDef) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addStructDefWithNoRefResolve({})", structDef);
        }

        structDefs.addType(structDef, new AtlasStructType(structDef));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addStructDefWithNoRefResolve({})", structDef);
        }
    }

    public void addStructDef(AtlasStructDef structDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addStructDef({})", structDef);
        }

        structDefs.addType(structDef, new AtlasStructType(structDef, this));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addStructDef({})", structDef);
        }
    }

    public Collection<AtlasStructDef> getAllStructDefs() { return structDefs.getAll(); }

    public AtlasStructDef getStructDefByGuid(String guid) {
        return structDefs.getTypeDefByGuid(guid);
    }

    public AtlasStructDef getStructDefByName(String name) { return structDefs.getTypeDefByName(name); }

    public void removeStructDefByGuid(String guid) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.removeStructDefByGuid({})", guid);
        }

        AtlasStructDef structDef = structDefs.getTypeDefByGuid(guid);

        if (structDef != null) {
            structDefs.removeTypeDefByGuid(guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.removeStructDefByGuid({})", guid);
        }
    }

    public void removeStructDefByName(String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.removeStructDefByName({})", name);
        }

        AtlasStructDef structDef = structDefs.getTypeDefByName(name);

        if (structDef != null) {
            structDefs.removeTypeDefByName(name);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.removeStructDefByName({})", name);
        }
    }


    public void addClassificationDefWithNoRefResolve(AtlasClassificationDef classificationDef) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addClassificationDefWithNoRefResolve({})", classificationDef);
        }

        classificationDefs.addType(classificationDef, new AtlasClassificationType(classificationDef));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addClassificationDefWithNoRefResolve({})", classificationDef);
        }
    }

    public void addClassificationDef(AtlasClassificationDef classificationDef)
        throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addClassificationDef({})", classificationDef);
        }

        classificationDefs.addType(classificationDef, new AtlasClassificationType(classificationDef, this));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addClassificationDef({})", classificationDef);
        }
    }

    public Collection<AtlasClassificationDef> getAllClassificationDefs() { return classificationDefs.getAll(); }

    public AtlasClassificationDef getClassificationDefByGuid(String guid) {
        return classificationDefs.getTypeDefByGuid(guid);
    }

    public AtlasClassificationDef getClassificationDefByName(String name) {
        return classificationDefs.getTypeDefByName(name);
    }

    public void removeClassificationDefByGuid(String guid) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.removeClassificationDefByGuid({})", guid);
        }

        AtlasClassificationDef classificationDef = classificationDefs.getTypeDefByGuid(guid);

        if (classificationDef != null) {
            classificationDefs.removeTypeDefByGuid(guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.removeClassificationDefByGuid({})", guid);
        }
    }

    public void removeClassificationDefByName(String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.removeClassificationDefByName({})", name);
        }

        AtlasClassificationDef classificationDef = classificationDefs.getTypeDefByName(name);

        if (classificationDef != null) {
            classificationDefs.removeTypeDefByName(name);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.removeClassificationDefByName({})", name);
        }
    }


    public void addEntityDefWithNoRefResolve(AtlasEntityDef entityDef) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addEntityDefWithNoRefResolve({})", entityDef);
        }

        entityDefs.addType(entityDef, new AtlasEntityType(entityDef));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addEntityDefWithNoRefResolve({})", entityDef);
        }
    }

    public void addEntityDef(AtlasEntityDef entityDef) throws AtlasBaseException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.addEntityDef({})", entityDef);
        }

        entityDefs.addType(entityDef, new AtlasEntityType(entityDef, this));

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.addEntityDef({})", entityDef);
        }
    }

    public Collection<AtlasEntityDef> getAllEntityDefs() { return entityDefs.getAll(); }

    public AtlasEntityDef getEntityDefByGuid(String guid) {
        return entityDefs.getTypeDefByGuid(guid);
    }

    public AtlasEntityDef getEntityDefByName(String name) {
        return entityDefs.getTypeDefByName(name);
    }

    public void removeEntityDefByGuid(String guid) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.removeEntityDefByGuid({})", guid);
        }

        AtlasEntityDef entityDef = entityDefs.getTypeDefByGuid(guid);

        if (entityDef != null) {
            entityDefs.removeTypeDefByGuid(guid);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.removeEntityDefByGuid({})", guid);
        }
    }

    public void removeEntityDefByName(String name) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> AtlasTypeRegistry.removeEntityDefByName({})", name);
        }

        AtlasEntityDef entityDef = entityDefs.getTypeDefByName(name);

        if (entityDef != null) {
            entityDefs.removeTypeDefByName(name);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== AtlasTypeRegistry.removeEntityDefByName({})", name);
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
            T typeDef = guid != null ? typeDefGuidMap.remove(guid) : null;

            if (typeDef != null) {
                if (StringUtils.isNotEmpty(typeDef.getName())) {
                    typeDefNameMap.remove(typeDef.getName());
                    typeRegistry.unregisterTypeByName(typeDef.getName());
                }
            }
        }

        public void removeTypeDefByName(String name) {
            T typeDef = name != null ? typeDefNameMap.get(name) : null;

            if (typeDef != null) {
                if (StringUtils.isNotEmpty(typeDef.getGuid())) {
                    typeDefGuidMap.remove(typeDef.getGuid());
                    typeRegistry.unregisterTypeByName(typeDef.getName());
                }
            }
        }
    }
}
