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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.atlas.AtlasException;
import org.apache.atlas.classification.InterfaceAudience;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.exception.TypeExistsException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;

import javax.inject.Singleton;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

@Singleton
@InterfaceAudience.Private
public class TypeSystem {
    private static final TypeSystem INSTANCE = new TypeSystem();
    private static ThreadLocal<SimpleDateFormat> dateFormat = new ThreadLocal() {
        @Override
        public SimpleDateFormat initialValue() {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            return dateFormat;
        }
    };

    private Map<String, IDataType> types;
    private IdType idType;

    /**
     * An in-memory copy of type categories vs types for convenience.
     */
    private Multimap<DataTypes.TypeCategory, String> typeCategoriesToTypeNamesMap;

    private ImmutableList<String> coreTypes;

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
    public void reset() {
        initialize();
    }

    private void initialize() {
        types = new ConcurrentHashMap<>();
        typeCategoriesToTypeNamesMap = ArrayListMultimap.create(DataTypes.TypeCategory.values().length, 10);

        registerPrimitiveTypes();
        registerCoreTypes();
        coreTypes = ImmutableList.copyOf(types.keySet());
    }

    public ImmutableList<String> getCoreTypes() {
        return coreTypes;
    }

    public ImmutableList<String> getTypeNames() {
        List<String> typeNames = new ArrayList<>(types.keySet());
        typeNames.removeAll(getCoreTypes());
        return ImmutableList.copyOf(typeNames);
    }

    public ImmutableList<String> getTypeNamesByCategory(DataTypes.TypeCategory typeCategory) {
        return ImmutableList.copyOf(typeCategoriesToTypeNamesMap.get(typeCategory));
    }

    private void registerPrimitiveTypes() {
        types.put(DataTypes.BOOLEAN_TYPE.getName(), DataTypes.BOOLEAN_TYPE);
        types.put(DataTypes.BYTE_TYPE.getName(), DataTypes.BYTE_TYPE);
        types.put(DataTypes.SHORT_TYPE.getName(), DataTypes.SHORT_TYPE);
        types.put(DataTypes.INT_TYPE.getName(), DataTypes.INT_TYPE);
        types.put(DataTypes.LONG_TYPE.getName(), DataTypes.LONG_TYPE);
        types.put(DataTypes.FLOAT_TYPE.getName(), DataTypes.FLOAT_TYPE);
        types.put(DataTypes.DOUBLE_TYPE.getName(), DataTypes.DOUBLE_TYPE);
        types.put(DataTypes.BIGINTEGER_TYPE.getName(), DataTypes.BIGINTEGER_TYPE);
        types.put(DataTypes.BIGDECIMAL_TYPE.getName(), DataTypes.BIGDECIMAL_TYPE);
        types.put(DataTypes.DATE_TYPE.getName(), DataTypes.DATE_TYPE);
        types.put(DataTypes.STRING_TYPE.getName(), DataTypes.STRING_TYPE);

        typeCategoriesToTypeNamesMap.putAll(DataTypes.TypeCategory.PRIMITIVE, types.keySet());
    }


    /*
     * The only core OOB type we will define is the Struct to represent the Identity of an Instance.
     */
    private void registerCoreTypes() {
        idType = new IdType();
    }

    public IdType getIdType() {
        return idType;
    }

    public boolean isRegistered(String typeName) {
        return types.containsKey(typeName);
    }

    public <T> T getDataType(Class<T> cls, String name) throws AtlasException {
        if (types.containsKey(name)) {
            try {
                return cls.cast(types.get(name));
            } catch (ClassCastException cce) {
                throw new AtlasException(cce);
            }
        }

        /*
         * is this an Array Type?
         */
        String arrElemType = TypeUtils.parseAsArrayType(name);
        if (arrElemType != null) {
            IDataType dT = defineArrayType(getDataType(IDataType.class, arrElemType));
            return cls.cast(dT);
        }

        /*
         * is this a Map Type?
         */
        String[] mapType = TypeUtils.parseAsMapType(name);
        if (mapType != null) {
            IDataType dT =
                    defineMapType(getDataType(IDataType.class, mapType[0]), getDataType(IDataType.class, mapType[1]));
            return cls.cast(dT);
        }

        throw new TypeNotFoundException(String.format("Unknown datatype: %s", name));
    }

    public StructType defineStructType(String name, boolean errorIfExists, AttributeDefinition... attrDefs)
    throws AtlasException {
        StructTypeDefinition structDef = new StructTypeDefinition(name, attrDefs);
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

        return new StructType(this, name, infos);
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
        return transientTypes.defineTypes();
    }

    public Map<String, IDataType> defineClassTypes(HierarchicalTypeDefinition<ClassType>... classDefs)
    throws AtlasException {
        TransientTypeSystem transientTypes = new TransientTypeSystem(ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.copyOf(classDefs));
        return transientTypes.defineTypes();
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
        return transientTypes.defineTypes();
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

    public EnumType defineEnumType(EnumTypeDefinition eDef) throws AtlasException {
        assert eDef.name != null;
        if (types.containsKey(eDef.name)) {
            throw new AtlasException(String.format("Redefinition of type %s not supported", eDef.name));
        }

        EnumType eT = new EnumType(this, eDef.name, eDef.enumValues);
        types.put(eDef.name, eT);
        typeCategoriesToTypeNamesMap.put(DataTypes.TypeCategory.ENUM, eDef.name);
        return eT;
    }

    public SimpleDateFormat getDateFormat() {
        return dateFormat.get();
    }

    public boolean allowNullsInCollections() {
        return false;
    }

    public void removeTypes(Collection<String> typeNames) {
        for (String typeName : typeNames) {
            IDataType dataType = types.get(typeName);
            final DataTypes.TypeCategory typeCategory = dataType.getTypeCategory();
            typeCategoriesToTypeNamesMap.get(typeCategory).remove(typeName);
            types.remove(typeName);
        }
    }

    class TransientTypeSystem extends TypeSystem {

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
            return TypeSystem.this.types.get(name);
        }

        /*
         * Step 1:
         * - validate cannot redefine types
         * - setup shallow Type instances to facilitate recursive type graphs
         */
        private void step1(boolean update) throws AtlasException {
            for (EnumTypeDefinition eDef : enumDefs) {
                assert eDef.name != null;
                if (!update && (transientTypes.containsKey(eDef.name) || types.containsKey(eDef.name))) {
                    throw new AtlasException(String.format("Redefinition of type %s not supported", eDef.name));
                }

                EnumType eT = new EnumType(this, eDef.name, eDef.enumValues);
                transientTypes.put(eDef.name, eT);
            }

            for (StructTypeDefinition sDef : structDefs) {
                assert sDef.typeName != null;
                if (!update && (transientTypes.containsKey(sDef.typeName) || types.containsKey(sDef.typeName))) {
                    throw new TypeExistsException(String.format("Cannot redefine type %s", sDef.typeName));
                }
                StructType sT = new StructType(this, sDef.typeName, sDef.attributeDefinitions.length);
                structNameToDefMap.put(sDef.typeName, sDef);
                transientTypes.put(sDef.typeName, sT);
            }

            for (HierarchicalTypeDefinition<TraitType> traitDef : traitDefs) {
                assert traitDef.typeName != null;
                if (!update &&
                        (transientTypes.containsKey(traitDef.typeName) || types.containsKey(traitDef.typeName))) {
                    throw new TypeExistsException(String.format("Cannot redefine type %s", traitDef.typeName));
                }
                TraitType tT = new TraitType(this, traitDef.typeName, traitDef.superTypes,
                        traitDef.attributeDefinitions.length);
                traitNameToDefMap.put(traitDef.typeName, traitDef);
                transientTypes.put(traitDef.typeName, tT);
            }

            for (HierarchicalTypeDefinition<ClassType> classDef : classDefs) {
                assert classDef.typeName != null;
                if (!update &&
                        (transientTypes.containsKey(classDef.typeName) || types.containsKey(classDef.typeName))) {
                    throw new TypeExistsException(String.format("Cannot redefine type %s", classDef.typeName));
                }

                ClassType cT = new ClassType(this, classDef.typeName, classDef.superTypes,
                        classDef.attributeDefinitions.length);
                classNameToDefMap.put(classDef.typeName, classDef);
                transientTypes.put(classDef.typeName, cT);
            }
        }

        private <U extends HierarchicalType> void validateSuperTypes(Class<U> cls, HierarchicalTypeDefinition<U> def)
        throws AtlasException {
            Set<String> s = new HashSet<>();
            ImmutableList<String> superTypes = def.superTypes;
            for (String superTypeName : superTypes) {

                if (s.contains(superTypeName)) {
                    throw new AtlasException(
                            String.format("Type %s extends superType %s multiple times", def.typeName, superTypeName));
                }

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
                s.add(superTypeName);
            }
        }

        /*
         * Step 2:
         * - for Hierarchical Types, validate SuperTypes.
         * - for each Hierarchical Type setup their SuperTypes Graph
         */
        private void step2() throws AtlasException {
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

            StructType type = new StructType(this, def.typeName, infos);
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
                Constructor<U> cons = cls.getDeclaredConstructor(TypeSystem.class, String.class, ImmutableList.class,
                        AttributeInfo[].class);
                U type = cons.newInstance(this, def.typeName, def.superTypes, infos);
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
        private void step3() throws AtlasException {

            List<TraitType> traitTypes = new ArrayList<>();
            for (String traitTypeName : traitNameToDefMap.keySet()) {
                traitTypes.add(getDataType(TraitType.class, traitTypeName));
            }
            Collections.sort(traitTypes);

            List<ClassType> classTypes = new ArrayList<>();
            for (String classTypeName : classNameToDefMap.keySet()) {
                classTypes.add(getDataType(ClassType.class, classTypeName));
            }
            Collections.sort(classTypes);

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
        private void step4() throws AtlasException {
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
        private void step5() throws TypeUpdateException {
            //If the type is modified, validate that update can be done
            for (IDataType newType : transientTypes.values()) {
                if (TypeSystem.this.types.containsKey(newType.getName())) {
                    IDataType oldType = TypeSystem.this.types.get(newType.getName());
                    oldType.validateUpdate(newType);
                }
            }
        }

        Map<String, IDataType> defineTypes() throws AtlasException {
            return defineTypes(false);
        }

        Map<String, IDataType> defineTypes(boolean update) throws AtlasException {
            step1(update);
            step2();

            step3();
            step4();

            if (update) {
                step5();
            }

            Map<String, IDataType> newTypes = new HashMap<>();

            for (Map.Entry<String, IDataType> typeEntry : transientTypes.entrySet()) {
                String typeName = typeEntry.getKey();
                IDataType type = typeEntry.getValue();

                //Add/replace the new type in the typesystem
                TypeSystem.this.types.put(typeName, type);
                typeCategoriesToTypeNamesMap.put(type.getTypeCategory(), typeName);

                newTypes.put(typeName, type);
            }
            return newTypes;
        }

        @Override
        public ImmutableList<String> getTypeNames() {
            Set<String> typeNames = transientTypes.keySet();
            typeNames.addAll(TypeSystem.this.getTypeNames());
            return ImmutableList.copyOf(typeNames);
        }

        //get from transient types. Else, from main type system
        @Override
        public <T> T getDataType(Class<T> cls, String name) throws AtlasException {
            if (transientTypes != null) {
                if (transientTypes.containsKey(name)) {
                    try {
                        return cls.cast(transientTypes.get(name));
                    } catch (ClassCastException cce) {
                        throw new AtlasException(cce);
                    }
                }

            /*
             * is this an Array Type?
             */
                String arrElemType = TypeUtils.parseAsArrayType(name);
                if (arrElemType != null) {
                    IDataType dT = defineArrayType(getDataType(IDataType.class, arrElemType));
                    return cls.cast(dT);
                }

            /*
             * is this a Map Type?
             */
                String[] mapType = TypeUtils.parseAsMapType(name);
                if (mapType != null) {
                    IDataType dT =
                            defineMapType(getDataType(IDataType.class, mapType[0]), getDataType(IDataType.class, mapType[1]));
                    return cls.cast(dT);
                }
            }

            return TypeSystem.this.getDataType(cls, name);
        }

        @Override
        public StructType defineStructType(String name, boolean errorIfExists, AttributeDefinition... attrDefs)
        throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public TraitType defineTraitType(HierarchicalTypeDefinition traitDef) throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public ClassType defineClassType(HierarchicalTypeDefinition<ClassType> classDef) throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public Map<String, IDataType> defineTypes(ImmutableList<EnumTypeDefinition> enumDefs,
                ImmutableList<StructTypeDefinition> structDefs,
                ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs,
                ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs) throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public DataTypes.ArrayType defineArrayType(IDataType elemType) throws AtlasException {
            return super.defineArrayType(elemType);
        }

        @Override
        public DataTypes.MapType defineMapType(IDataType keyType, IDataType valueType) throws AtlasException {
            return super.defineMapType(keyType, valueType);
        }
    }

    public class IdType {
        private static final String ID_ATTRNAME = "guid";
        private static final String TYPENAME_ATTRNAME = "typeName";
        private static final String TYP_NAME = "__IdType";

        private IdType() {
            AttributeDefinition idAttr =
                    new AttributeDefinition(ID_ATTRNAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false,
                            null);
            AttributeDefinition typNmAttr =
                    new AttributeDefinition(TYPENAME_ATTRNAME, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED,
                            false, null);
            try {
                AttributeInfo[] infos = new AttributeInfo[2];
                infos[0] = new AttributeInfo(TypeSystem.this, idAttr, null);
                infos[1] = new AttributeInfo(TypeSystem.this, typNmAttr, null);

                StructType type = new StructType(TypeSystem.this, TYP_NAME, infos);
                TypeSystem.this.types.put(TYP_NAME, type);

            } catch (AtlasException me) {
                throw new RuntimeException(me);
            }
        }

        public StructType getStructType() throws AtlasException {
            return getDataType(StructType.class, TYP_NAME);
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
    }

    public static final String ID_STRUCT_ID_ATTRNAME = IdType.ID_ATTRNAME;
}
