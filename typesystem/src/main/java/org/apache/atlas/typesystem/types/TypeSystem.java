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

package org.apache.atlas.typesystem.types;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.atlas.AtlasException;
import org.apache.atlas.classification.InterfaceAudience;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.exception.TypeExistsException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.types.cache.DefaultTypeCache;
import org.apache.atlas.typesystem.types.cache.TypeCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import javax.inject.Singleton;

@Singleton
@InterfaceAudience.Private
public class TypeSystem {
    private static final Logger LOG = LoggerFactory.getLogger(TypeSystem.class);

    private static final TypeSystem INSTANCE = new TypeSystem();
    private static ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal<SimpleDateFormat>() {
        @Override
        public SimpleDateFormat initialValue() {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return dateFormat;
        }
    };

    private TypeCache typeCache  = new DefaultTypeCache();
    private IdType idType;
    private Map<String, IDataType> coreTypes;

    public TypeSystem() {
        initialize();
    }

    public static TypeSystem getInstance() {
        return INSTANCE;
    }

    /**
     * This is only used for testing purposes. Not intended for public use.
     */
    @InterfaceAudience.Private
    public TypeSystem reset() {

        typeCache.clear(); // clear all entries in cache
        initialize();

        return this;
    }

    public void setTypeCache(TypeCache typeCache) {
        this.typeCache = typeCache;
    }

    private void initialize() {

        coreTypes = new ConcurrentHashMap<>();

        registerPrimitiveTypes();
        registerCoreTypes();
    }

    public ImmutableList<String> getCoreTypes() {
        return ImmutableList.copyOf(coreTypes.keySet());
    }

    public ImmutableList<String> getTypeNames() throws AtlasException {
        List<String> typeNames = new ArrayList<>(typeCache.getAllTypeNames());
        return ImmutableList.copyOf(typeNames);
    }

    public ImmutableList<String> getTypeNamesByCategory(final DataTypes.TypeCategory typeCategory) throws AtlasException {
        return getTypeNames(new HashMap<TypeCache.TYPE_FILTER, String>() {{
            put(TypeCache.TYPE_FILTER.CATEGORY, typeCategory.name());
        }});
    }

    public ImmutableList<String> getTypeNames(Map<TypeCache.TYPE_FILTER, String> filterMap) throws AtlasException {
        return ImmutableList.copyOf(typeCache.getTypeNames(filterMap));
    }

    private void registerPrimitiveTypes() {
        coreTypes.put(DataTypes.BOOLEAN_TYPE.getName(), DataTypes.BOOLEAN_TYPE);
        coreTypes.put(DataTypes.BYTE_TYPE.getName(), DataTypes.BYTE_TYPE);
        coreTypes.put(DataTypes.SHORT_TYPE.getName(), DataTypes.SHORT_TYPE);
        coreTypes.put(DataTypes.INT_TYPE.getName(), DataTypes.INT_TYPE);
        coreTypes.put(DataTypes.LONG_TYPE.getName(), DataTypes.LONG_TYPE);
        coreTypes.put(DataTypes.FLOAT_TYPE.getName(), DataTypes.FLOAT_TYPE);
        coreTypes.put(DataTypes.DOUBLE_TYPE.getName(), DataTypes.DOUBLE_TYPE);
        coreTypes.put(DataTypes.BIGINTEGER_TYPE.getName(), DataTypes.BIGINTEGER_TYPE);
        coreTypes.put(DataTypes.BIGDECIMAL_TYPE.getName(), DataTypes.BIGDECIMAL_TYPE);
        coreTypes.put(DataTypes.DATE_TYPE.getName(), DataTypes.DATE_TYPE);
        coreTypes.put(DataTypes.STRING_TYPE.getName(), DataTypes.STRING_TYPE);
    }

    /*
     * The only core OOB type we will define is the Struct to represent the Identity of an Instance.
     */
    private void registerCoreTypes() {

        idType = new IdType();
        coreTypes.put(idType.getStructType().getName(), idType.getStructType());
    }

    public IdType getIdType() {
        return idType;
    }

    public boolean isRegistered(String typeName) throws AtlasException {
        return isCoreType(typeName) || typeCache.has(typeName);
    }

    protected boolean isCoreType(String typeName) {

        return coreTypes.containsKey(typeName);
    }

    public IDataType getDataType(String name) throws AtlasException {
        if (isCoreType(name)) {
            return coreTypes.get(name);
        }

        if (typeCache.has(name)) {
            return typeCache.get(name);
        }

        /*
         * is this an Array Type?
         */
        String arrElemType = TypeUtils.parseAsArrayType(name);
        if (arrElemType != null) {
            IDataType dT = defineArrayType(getDataType(arrElemType));
            return dT;
        }

        /*
         * is this a Map Type?
         */
        String[] mapType = TypeUtils.parseAsMapType(name);
        if (mapType != null) {
            IDataType dT =
                    defineMapType(getDataType(mapType[0]), getDataType(mapType[1]));
            return dT;
        }

        /*
         * Invoke cache callback to possibly obtain type from other storage.
         */
        IDataType dT = typeCache.onTypeFault(name);
        if (dT != null) {
            return dT;
        }

        throw new TypeNotFoundException(String.format("Unknown datatype: %s", name));
    }

    public <T extends IDataType> T getDataType(Class<T> cls, String name) throws AtlasException {
        try {
            IDataType dt = getDataType(name);
            return cls.cast(dt);
        } catch (ClassCastException cce) {
            throw new AtlasException(cce);
        }

    }

    public StructType defineStructType(String name, boolean errorIfExists, AttributeDefinition... attrDefs)
    throws AtlasException {
        return defineStructType(name, null, errorIfExists, attrDefs);
    }

    public StructType defineStructType(String name, String description, boolean errorIfExists, AttributeDefinition... attrDefs)
    throws AtlasException {
        StructTypeDefinition structDef = new StructTypeDefinition(name, description, attrDefs);
        defineTypes(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.of(structDef),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());
        return getDataType(StructType.class, structDef.typeName);
    }

    /**
     * construct a temporary StructType for a Query Result. This is not registered in the
     * typeSystem.
     * The attributes in the typeDefinition can only reference permanent types.
     * @param name     struct type name
     * @param attrDefs struct type definition
     * @return temporary struct type
     * @throws AtlasException
     */
    public StructType defineQueryResultType(String name, Map<String, IDataType> tempTypes,
            AttributeDefinition... attrDefs) throws AtlasException {

        AttributeInfo[] infos = new AttributeInfo[attrDefs.length];
        for (int i = 0; i < attrDefs.length; i++) {
            infos[i] = new AttributeInfo(this, attrDefs[i], tempTypes);
        }

        return new StructType(this, name, null, infos);
    }

    public TraitType defineTraitType(HierarchicalTypeDefinition<TraitType> traitDef) throws AtlasException {
        defineTypes(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(traitDef), ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());
        return getDataType(TraitType.class, traitDef.typeName);
    }

    public ClassType defineClassType(HierarchicalTypeDefinition<ClassType> classDef) throws AtlasException {
        defineTypes(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(), ImmutableList.of(classDef));
        return getDataType(ClassType.class, classDef.typeName);
    }

    public Map<String, IDataType> defineTraitTypes(HierarchicalTypeDefinition<TraitType>... traitDefs)
    throws AtlasException {
        TransientTypeSystem transientTypes =
                new TransientTypeSystem(ImmutableList.<EnumTypeDefinition>of(),
                        ImmutableList.<StructTypeDefinition>of(), ImmutableList.copyOf(traitDefs),
                        ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());
        return transientTypes.defineTypes(false);
    }

    public Map<String, IDataType> defineClassTypes(HierarchicalTypeDefinition<ClassType>... classDefs)
    throws AtlasException {
        TransientTypeSystem transientTypes = new TransientTypeSystem(ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.copyOf(classDefs));
        return transientTypes.defineTypes(false);
    }

    public Map<String, IDataType> updateTypes(TypesDef typesDef) throws AtlasException {
        ImmutableList<EnumTypeDefinition> enumDefs = ImmutableList.copyOf(typesDef.enumTypesAsJavaList());
        ImmutableList<StructTypeDefinition> structDefs = ImmutableList.copyOf(typesDef.structTypesAsJavaList());
        ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs =
                ImmutableList.copyOf(typesDef.traitTypesAsJavaList());
        ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs =
                ImmutableList.copyOf(typesDef.classTypesAsJavaList());

        TransientTypeSystem transientTypes = new TransientTypeSystem(enumDefs, structDefs, traitDefs, classDefs);
        return transientTypes.defineTypes(true);
    }

    public Map<String, IDataType> defineTypes(TypesDef typesDef) throws AtlasException {
        ImmutableList<EnumTypeDefinition> enumDefs = ImmutableList.copyOf(typesDef.enumTypesAsJavaList());
        ImmutableList<StructTypeDefinition> structDefs = ImmutableList.copyOf(typesDef.structTypesAsJavaList());
        ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs =
                ImmutableList.copyOf(typesDef.traitTypesAsJavaList());
        ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs =
                ImmutableList.copyOf(typesDef.classTypesAsJavaList());

        return defineTypes(enumDefs, structDefs, traitDefs, classDefs);
    }

    public Map<String, IDataType> defineTypes(ImmutableList<EnumTypeDefinition> enumDefs,
            ImmutableList<StructTypeDefinition> structDefs,
            ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs,
            ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs) throws AtlasException {
        TransientTypeSystem transientTypes = new TransientTypeSystem(enumDefs, structDefs, traitDefs, classDefs);
        return transientTypes.defineTypes(false);
    }

    public DataTypes.ArrayType defineArrayType(IDataType elemType) throws AtlasException {
        assert elemType != null;
        DataTypes.ArrayType dT = new DataTypes.ArrayType(elemType);
        return dT;
    }

    public DataTypes.MapType defineMapType(IDataType keyType, IDataType valueType) throws AtlasException {
        assert keyType != null;
        assert valueType != null;
        DataTypes.MapType dT = new DataTypes.MapType(keyType, valueType);
        return dT;
    }

    public EnumType defineEnumType(String name, EnumValue... values) throws AtlasException {
        return defineEnumType(new EnumTypeDefinition(name, values));
    }

    public EnumType defineEnumType(String name, String description, EnumValue... values) throws AtlasException {
        return defineEnumType(new EnumTypeDefinition(name, description, values));
    }

    public EnumType defineEnumType(EnumTypeDefinition eDef) throws AtlasException {
        assert eDef.name != null;
        if (isRegistered(eDef.name)) {
            throw new AtlasException(String.format("Redefinition of type %s not supported", eDef.name));
        }

        EnumType eT = new EnumType(this, eDef.name, eDef.description, eDef.version, eDef.enumValues);
        typeCache.put(eT);
        return eT;
    }

    public SimpleDateFormat getDateFormat() {
        return dateFormat.get();
    }

    public boolean allowNullsInCollections() {
        return false;
    }

    /**
     * Create an instance of {@link TransientTypeSystem} with the types defined in the {@link TypesDef}.
     *
     * As part of this, a set of verifications are run on the types defined.
     * @param typesDef The new list of types to be created or updated.
     * @param isUpdate True, if types are updated, false otherwise.
     * @return {@link TransientTypeSystem} that holds the newly added types.
     * @throws AtlasException
     */
    public TransientTypeSystem createTransientTypeSystem(TypesDef typesDef, boolean isUpdate) throws AtlasException {
        ImmutableList<EnumTypeDefinition> enumDefs = ImmutableList.copyOf(typesDef.enumTypesAsJavaList());
        ImmutableList<StructTypeDefinition> structDefs = ImmutableList.copyOf(typesDef.structTypesAsJavaList());
        ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs =
                ImmutableList.copyOf(typesDef.traitTypesAsJavaList());
        ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs =
                ImmutableList.copyOf(typesDef.classTypesAsJavaList());
        TransientTypeSystem transientTypeSystem = new TransientTypeSystem(enumDefs, structDefs, traitDefs, classDefs);
        transientTypeSystem.verifyTypes(isUpdate);
        return transientTypeSystem;
    }

    /**
     * Commit the given types to this {@link TypeSystem} instance.
     *
     * This step should be called only after the types have been committed to the backend stores successfully.
     * @param typesAdded newly added types.
     * @throws AtlasException
     */
    public void commitTypes(Map<String, IDataType> typesAdded) throws AtlasException {
        for (Map.Entry<String, IDataType> typeEntry : typesAdded.entrySet()) {
            IDataType type = typeEntry.getValue();
            //Add/replace the new type in the typesystem
            typeCache.put(type);
        }
    }

    public class TransientTypeSystem extends TypeSystem {

        final ImmutableList<StructTypeDefinition> structDefs;
        final ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs;
        final ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs;
        private final ImmutableList<EnumTypeDefinition> enumDefs;

        Map<String, StructTypeDefinition> structNameToDefMap = new HashMap<>();
        Map<String, HierarchicalTypeDefinition<TraitType>> traitNameToDefMap = new HashMap<>();
        Map<String, HierarchicalTypeDefinition<ClassType>> classNameToDefMap = new HashMap<>();

        Map<String, IDataType> transientTypes = null;

        List<AttributeInfo> recursiveRefs = new ArrayList<>();
        List<DataTypes.ArrayType> recursiveArrayTypes = new ArrayList<>();
        List<DataTypes.MapType> recursiveMapTypes = new ArrayList<>();


        TransientTypeSystem(ImmutableList<EnumTypeDefinition> enumDefs, ImmutableList<StructTypeDefinition> structDefs,
                            ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs,
                            ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs) {
            this.enumDefs = enumDefs;
            this.structDefs = structDefs;
            this.traitDefs = traitDefs;
            this.classDefs = classDefs;
            transientTypes = new HashMap<>();
        }

        private IDataType dataType(String name) throws AtlasException {
            if (transientTypes.containsKey(name)) {
                return transientTypes.get(name);
            }

            return TypeSystem.this.getDataType(IDataType.class, name);
        }

        /*
         * Step 1:
         * - validate cannot redefine types
         * - setup shallow Type instances to facilitate recursive type graphs
         */
        private void validateAndSetupShallowTypes(boolean update) throws AtlasException {
            for (EnumTypeDefinition eDef : enumDefs) {
                assert eDef.name != null;
                if (!update) {
                    if (TypeSystem.this.isRegistered(eDef.name)) {
                        throw new TypeExistsException(String.format("Redefinition of type %s is not supported", eDef.name));
                    } else if (transientTypes.containsKey(eDef.name)) {
                        LOG.warn("Found duplicate definition of type {}. Ignoring..", eDef.name);
                        continue;
                    }
                }

                EnumType eT = new EnumType(this, eDef.name, eDef.description, eDef.version, eDef.enumValues);
                transientTypes.put(eDef.name, eT);
            }

            for (StructTypeDefinition sDef : structDefs) {
                assert sDef.typeName != null;
                if (!update) {
                    if (TypeSystem.this.isRegistered(sDef.typeName)) {
                        throw new TypeExistsException(String.format("Redefinition of type %s is not supported", sDef.typeName));
                    } else if (transientTypes.containsKey(sDef.typeName)) {
                        LOG.warn("Found duplicate definition of type {}. Ignoring..", sDef.typeName);
                        continue;
                    }
                }

                StructType sT = new StructType(this, sDef.typeName, sDef.typeDescription, sDef.typeVersion, sDef.attributeDefinitions.length);
                structNameToDefMap.put(sDef.typeName, sDef);
                transientTypes.put(sDef.typeName, sT);
            }

            for (HierarchicalTypeDefinition<TraitType> traitDef : traitDefs) {
                assert traitDef.typeName != null;
                if (!update) {
                    if (TypeSystem.this.isRegistered(traitDef.typeName)) {
                        throw new TypeExistsException(String.format("Redefinition of type %s is not supported", traitDef.typeName));
                    } else if (transientTypes.containsKey(traitDef.typeName)) {
                        LOG.warn("Found duplicate definition of type {}. Ignoring..", traitDef.typeName);
                        continue;
                    }
                }

                TraitType tT = new TraitType(this, traitDef.typeName, traitDef.typeDescription, traitDef.typeVersion, traitDef.superTypes,
                        traitDef.attributeDefinitions.length);
                traitNameToDefMap.put(traitDef.typeName, traitDef);
                transientTypes.put(traitDef.typeName, tT);
            }

            for (HierarchicalTypeDefinition<ClassType> classDef : classDefs) {
                assert classDef.typeName != null;
                if (!update) {
                    if (TypeSystem.this.isRegistered(classDef.typeName)) {
                        throw new TypeExistsException(String.format("Redefinition of type %s is not supported", classDef.typeName));
                    } else if (transientTypes.containsKey(classDef.typeName)) {
                        LOG.warn("Found duplicate definition of type {}. Ignoring..", classDef.typeName);
                        continue;
                    }
                }

                ClassType cT = new ClassType(this, classDef.typeName, classDef.typeDescription, classDef.typeVersion, classDef.superTypes,
                        classDef.attributeDefinitions.length);
                classNameToDefMap.put(classDef.typeName, classDef);
                transientTypes.put(classDef.typeName, cT);
            }
        }

        @Override
        public boolean isRegistered(String typeName) throws AtlasException {
            return transientTypes.containsKey(typeName) || TypeSystem.this.isRegistered(typeName);
        }

        private <U extends HierarchicalType> void validateSuperTypes(Class<U> cls, HierarchicalTypeDefinition<U> def)
        throws AtlasException {
            for (String superTypeName : def.superTypes) {

                IDataType dT = dataType(superTypeName);

                if (dT == null) {
                    throw new AtlasException(
                            String.format("Unknown superType %s in definition of type %s", superTypeName,
                                    def.typeName));
                }

                if (!cls.isAssignableFrom(dT.getClass())) {
                    throw new AtlasException(
                            String.format("SuperType %s must be a %s, in definition of type %s", superTypeName,
                                    cls.getName(), def.typeName));
                }
            }
        }

        /*
         * Step 2:
         * - for Hierarchical Types, validate SuperTypes.
         * - for each Hierarchical Type setup their SuperTypes Graph
         */
        private void validateAndSetupSuperTypes() throws AtlasException {
            for (HierarchicalTypeDefinition<TraitType> traitDef : traitDefs) {
                validateSuperTypes(TraitType.class, traitDef);
                TraitType traitType = getDataType(TraitType.class, traitDef.typeName);
                traitType.setupSuperTypesGraph();
            }

            for (HierarchicalTypeDefinition<ClassType> classDef : classDefs) {
                validateSuperTypes(ClassType.class, classDef);
                ClassType classType = getDataType(ClassType.class, classDef.typeName);
                classType.setupSuperTypesGraph();
            }
        }

        private AttributeInfo constructAttributeInfo(AttributeDefinition attrDef) throws AtlasException {
            AttributeInfo info = new AttributeInfo(this, attrDef, null);
            if (transientTypes.keySet().contains(attrDef.dataTypeName)) {
                recursiveRefs.add(info);
            }
            if (info.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
                DataTypes.ArrayType arrType = (DataTypes.ArrayType) info.dataType();
                if (transientTypes.keySet().contains(arrType.getElemType().getName())) {
                    recursiveArrayTypes.add(arrType);
                }
            }
            if (info.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP) {
                DataTypes.MapType mapType = (DataTypes.MapType) info.dataType();
                if (transientTypes.keySet().contains(mapType.getKeyType().getName())) {
                    recursiveMapTypes.add(mapType);
                } else if (transientTypes.keySet().contains(mapType.getValueType().getName())) {
                    recursiveMapTypes.add(mapType);
                }
            }

            if (info.multiplicity.upper > 1 && !(info.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP
                    || info.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY)) {
                throw new AtlasException(
                        String.format("A multiplicty of more than one requires a collection type for attribute '%s'",
                                info.name));
            }

            return info;
        }

        private StructType constructStructureType(StructTypeDefinition def) throws AtlasException {
            AttributeInfo[] infos = new AttributeInfo[def.attributeDefinitions.length];
            for (int i = 0; i < def.attributeDefinitions.length; i++) {
                infos[i] = constructAttributeInfo(def.attributeDefinitions[i]);
            }

            StructType type = new StructType(this, def.typeName, def.typeDescription, def.typeVersion, infos);
            transientTypes.put(def.typeName, type);
            return type;
        }

        private <U extends HierarchicalType> U constructHierarchicalType(Class<U> cls,
                HierarchicalTypeDefinition<U> def) throws AtlasException {
            AttributeInfo[] infos = new AttributeInfo[def.attributeDefinitions.length];
            for (int i = 0; i < def.attributeDefinitions.length; i++) {
                infos[i] = constructAttributeInfo(def.attributeDefinitions[i]);
            }

            try {
                Constructor<U> cons = cls.getDeclaredConstructor(TypeSystem.class, String.class, String.class, String.class, ImmutableSet.class,
                        AttributeInfo[].class);
                U type = cons.newInstance(this, def.typeName, def.typeDescription, def.typeVersion, def.superTypes, infos);
                transientTypes.put(def.typeName, type);
                return type;
            } catch (Exception e) {
                e.printStackTrace();
                throw new AtlasException(String.format("Cannot construct Type of MetaType %s - %s", cls.getName(), def.typeName), e);
            }
        }

        /*
         * Step 3:
         * - Order Hierarchical Types in order of SuperType before SubType.
         * - Construct all the Types
         */
        private void orderAndConstructTypes() throws AtlasException {

            List<TraitType> traitTypes = new ArrayList<>();
            for (String traitTypeName : traitNameToDefMap.keySet()) {
                traitTypes.add(getDataType(TraitType.class, traitTypeName));
            }
            traitTypes = HierarchicalTypeDependencySorter.sortTypes(traitTypes);

            List<ClassType> classTypes = new ArrayList<>();
            for (String classTypeName : classNameToDefMap.keySet()) {
                classTypes.add(getDataType(ClassType.class, classTypeName));
            }
            classTypes = HierarchicalTypeDependencySorter.sortTypes(classTypes);

            for (StructTypeDefinition structDef : structDefs) {
                constructStructureType(structDef);
            }

            for (TraitType traitType : traitTypes) {
                constructHierarchicalType(TraitType.class, traitNameToDefMap.get(traitType.getName()));
            }

            for (ClassType classType : classTypes) {
                constructHierarchicalType(ClassType.class, classNameToDefMap.get(classType.getName()));
            }
        }

        /*
         * Step 4:
         * - fix up references in recursive AttrInfo and recursive Collection Types.
         */
        private void setupRecursiveTypes() throws AtlasException {
            for (AttributeInfo info : recursiveRefs) {
                info.setDataType(dataType(info.dataType().getName()));
            }
            for (DataTypes.ArrayType arrType : recursiveArrayTypes) {
                arrType.setElemType(dataType(arrType.getElemType().getName()));
            }
            for (DataTypes.MapType mapType : recursiveMapTypes) {
                mapType.setKeyType(dataType(mapType.getKeyType().getName()));
                mapType.setValueType(dataType(mapType.getValueType().getName()));
            }
        }

        /**
         * Step 5:
         * - Validate that the update can be done
         */
        private void validateUpdateIsPossible() throws TypeUpdateException, AtlasException {
            //If the type is modified, validate that update can be done
            for (IDataType newType : transientTypes.values()) {
                IDataType oldType = null;
                try {
                    oldType = TypeSystem.this.getDataType(IDataType.class, newType.getName());
                } catch (TypeNotFoundException e) {
                    LOG.debug(String.format("No existing type %s found - update OK", newType.getName()));
                }
                if (oldType != null) {
                    oldType.validateUpdate(newType);
                }
            }
        }

        Map<String, IDataType> defineTypes(boolean update) throws AtlasException {
            verifyTypes(update);
            Map<String, IDataType> typesAdded = getTypesAdded();
            commitTypes(typesAdded);
            return typesAdded;
        }

        @Override
        public ImmutableList<String> getTypeNames() throws AtlasException {
            Set<String> typeNames = transientTypes.keySet();
            typeNames.addAll(TypeSystem.this.getTypeNames());
            return ImmutableList.copyOf(typeNames);
        }

        //get from transient types. Else, from main type system
        @Override
        public IDataType getDataType(String name) throws AtlasException {

            if (transientTypes != null) {
                if (transientTypes.containsKey(name)) {
                    return transientTypes.get(name);
                }

            /*
             * is this an Array Type?
             */
                String arrElemType = TypeUtils.parseAsArrayType(name);
                if (arrElemType != null) {
                    IDataType dT = defineArrayType(getDataType(IDataType.class, arrElemType));
                    return dT;
                }

            /*
             * is this a Map Type?
             */
                String[] mapType = TypeUtils.parseAsMapType(name);
                if (mapType != null) {
                    IDataType dT =
                            defineMapType(getDataType(IDataType.class, mapType[0]), getDataType(IDataType.class, mapType[1]));
                    return dT;
                }
            }

            return TypeSystem.this.getDataType(name);
        }

        @Override
        public StructType defineStructType(String name, boolean errorIfExists, AttributeDefinition... attrDefs)
        throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TransientTypeSystem");
        }

        @Override
        public TraitType defineTraitType(HierarchicalTypeDefinition traitDef) throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TransientTypeSystem");
        }

        @Override
        public ClassType defineClassType(HierarchicalTypeDefinition<ClassType> classDef) throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TransientTypeSystem");
        }

        @Override
        public Map<String, IDataType> defineTypes(ImmutableList<EnumTypeDefinition> enumDefs,
                ImmutableList<StructTypeDefinition> structDefs,
                ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs,
                ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs) throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TransientTypeSystem");
        }

        @Override
        public DataTypes.ArrayType defineArrayType(IDataType elemType) throws AtlasException {
            return super.defineArrayType(elemType);
        }

        @Override
        public DataTypes.MapType defineMapType(IDataType keyType, IDataType valueType) throws AtlasException {
            return super.defineMapType(keyType, valueType);
        }

        void verifyTypes(boolean isUpdate) throws AtlasException {
            validateAndSetupShallowTypes(isUpdate);
            validateAndSetupSuperTypes();
            orderAndConstructTypes();
            setupRecursiveTypes();
            if (isUpdate) {
                validateUpdateIsPossible();
            }
        }

        @Override
        public void commitTypes(Map<String, IDataType> typesAdded) throws AtlasException {
            TypeSystem.this.commitTypes(typesAdded);
        }

        public Map<String, IDataType> getTypesAdded() {
            return new HashMap<>(transientTypes);
        }

        /**
         * The core types do not change and they are registered
         * once in the main type system.
         */
        @Override
        public ImmutableList<String> getCoreTypes() {
            return TypeSystem.this.getCoreTypes();
        }
    }

    public class IdType {
        private static final String ID_ATTRNAME = "guid";
        private static final String TYPENAME_ATTRNAME = "typeName";
        private static final String STATE_ATTRNAME = "state";
        private static final String VERSION_ATTRNAME = "version";
        private static final String TYP_NAME = "__IdType";

        private StructType type;

        private IdType() {
            AttributeDefinition idAttr =
                    new AttributeDefinition(ID_ATTRNAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                            null);
            AttributeDefinition typNmAttr =
                    new AttributeDefinition(TYPENAME_ATTRNAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED,
                            false, null);
            AttributeDefinition stateAttr =
                    new AttributeDefinition(STATE_ATTRNAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED,
                            false, null);
            AttributeDefinition versionAttr =
                    new AttributeDefinition(VERSION_ATTRNAME, DataTypes.INT_TYPE.getName(), Multiplicity.REQUIRED,
                            false, null);
            try {
                AttributeInfo[] infos = new AttributeInfo[4];
                infos[0] = new AttributeInfo(TypeSystem.this, idAttr, null);
                infos[1] = new AttributeInfo(TypeSystem.this, typNmAttr, null);
                infos[2] = new AttributeInfo(TypeSystem.this, stateAttr, null);
                infos[3] = new AttributeInfo(TypeSystem.this, versionAttr, null);

                type = new StructType(TypeSystem.this, TYP_NAME, null, infos);
            } catch (AtlasException me) {
                throw new RuntimeException(me);
            }
        }

        public StructType getStructType() {
            return type;
        }

        public String getName() {
            return TYP_NAME;
        }

        public String idAttrName() {
            return ID_ATTRNAME;
        }

        public String typeNameAttrName() {
            return TYPENAME_ATTRNAME;
        }

        public String stateAttrName() {
            return STATE_ATTRNAME;
        }

        public String versionAttrName() {
            return VERSION_ATTRNAME;
        }
    }

    public static final String ID_STRUCT_ID_ATTRNAME = IdType.ID_ATTRNAME;
    public static final String ID_STRUCT_TYP_NAME = IdType.TYP_NAME;
}
