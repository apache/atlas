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
import org.apache.atlas.TypeNotFoundException;
import org.apache.atlas.classification.InterfaceAudience;
import org.apache.atlas.typesystem.TypesDef;

import javax.inject.Singleton;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
        defineTypes(ImmutableList.of(structDef), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
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

        defineTypes(ImmutableList.<StructTypeDefinition>of(), ImmutableList.of(traitDef),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());

        return getDataType(TraitType.class, traitDef.typeName);
    }

    public ClassType defineClassType(HierarchicalTypeDefinition<ClassType> classDef) throws AtlasException {

        defineTypes(ImmutableList.<StructTypeDefinition>of(), ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(classDef));

        return getDataType(ClassType.class, classDef.typeName);
    }

    public Map<String, IDataType> defineTraitTypes(HierarchicalTypeDefinition<TraitType>... traitDefs)
    throws AtlasException {
        TransientTypeSystem transientTypes =
                new TransientTypeSystem(ImmutableList.<StructTypeDefinition>of(), ImmutableList.copyOf(traitDefs),
                        ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());
        return transientTypes.defineTypes();
    }

    public Map<String, IDataType> defineClassTypes(HierarchicalTypeDefinition<ClassType>... classDefs)
    throws AtlasException {
        TransientTypeSystem transientTypes = new TransientTypeSystem(ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(), ImmutableList.copyOf(classDefs));
        return transientTypes.defineTypes();
    }

    public Map<String, IDataType> defineTypes(TypesDef typesDef) throws AtlasException {
        Map<String, IDataType> typesAdded = new HashMap<>();
        for (EnumTypeDefinition enumDef : typesDef.enumTypesAsJavaList()) {
            typesAdded.put(enumDef.name, defineEnumType(enumDef));
        }

        ImmutableList<StructTypeDefinition> structDefs = ImmutableList.copyOf(typesDef.structTypesAsJavaList());
        ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs =
                ImmutableList.copyOf(typesDef.traitTypesAsJavaList());
        ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs =
                ImmutableList.copyOf(typesDef.classTypesAsJavaList());

        typesAdded.putAll(defineTypes(structDefs, traitDefs, classDefs));
        return typesAdded;
    }

    public Map<String, IDataType> defineTypes(ImmutableList<StructTypeDefinition> structDefs,
            ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs,
            ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs) throws AtlasException {
        TransientTypeSystem transientTypes = new TransientTypeSystem(structDefs, traitDefs, classDefs);
        Map<String, IDataType> definedTypes = transientTypes.defineTypes();
        //        LOG.debug("Defined new types " + Arrays.toString(definedTypes.keySet().toArray(new
        // String[definedTypes.size()])));
        return definedTypes;
    }

    public DataTypes.ArrayType defineArrayType(IDataType elemType) throws AtlasException {
        assert elemType != null;
        DataTypes.ArrayType dT = new DataTypes.ArrayType(elemType);
        //        types.put(dT.getName(), dT);
        //        typeCategoriesToTypeNamesMap.put(DataTypes.TypeCategory.ARRAY, dT.getName());
        return dT;
    }

    public DataTypes.MapType defineMapType(IDataType keyType, IDataType valueType) throws AtlasException {
        assert keyType != null;
        assert valueType != null;
        DataTypes.MapType dT = new DataTypes.MapType(keyType, valueType);
        //        types.put(dT.getName(), dT);
        //        typeCategoriesToTypeNamesMap.put(DataTypes.TypeCategory.MAP, dT.getName());
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
        Map<String, StructTypeDefinition> structNameToDefMap = new HashMap<>();
        Map<String, HierarchicalTypeDefinition<TraitType>> traitNameToDefMap = new HashMap<>();
        Map<String, HierarchicalTypeDefinition<ClassType>> classNameToDefMap = new HashMap<>();

        Set<String> transientTypes;

        List<AttributeInfo> recursiveRefs;
        List<DataTypes.ArrayType> recursiveArrayTypes;
        List<DataTypes.MapType> recursiveMapTypes;


        TransientTypeSystem(ImmutableList<StructTypeDefinition> structDefs,
                ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs,
                ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs) {

            this.structDefs = structDefs;
            this.traitDefs = traitDefs;
            this.classDefs = classDefs;
            structNameToDefMap = new HashMap<>();
            traitNameToDefMap = new HashMap<>();
            classNameToDefMap = new HashMap<>();

            recursiveRefs = new ArrayList<>();
            recursiveArrayTypes = new ArrayList<>();
            recursiveMapTypes = new ArrayList<>();
            transientTypes = new LinkedHashSet<>();
        }

        private IDataType dataType(String name) {
            return TypeSystem.this.types.get(name);
        }

        /*
         * Step 1:
         * - validate cannot redefine types
         * - setup shallow Type instances to facilitate recursive type graphs
         */
        private void step1() throws AtlasException {
            for (StructTypeDefinition sDef : structDefs) {
                assert sDef.typeName != null;
                if (dataType(sDef.typeName) != null) {
                    throw new AtlasException(String.format("Cannot redefine type %s", sDef.typeName));
                }
                TypeSystem.this.types
                        .put(sDef.typeName, new StructType(this, sDef.typeName, sDef.attributeDefinitions.length));
                structNameToDefMap.put(sDef.typeName, sDef);
                transientTypes.add(sDef.typeName);
            }

            for (HierarchicalTypeDefinition<TraitType> traitDef : traitDefs) {
                assert traitDef.typeName != null;
                if (types.containsKey(traitDef.typeName)) {
                    throw new AtlasException(String.format("Cannot redefine type %s", traitDef.typeName));
                }

                TypeSystem.this.types.put(traitDef.typeName, new TraitType(this, traitDef.typeName, traitDef.superTypes,
                                traitDef.attributeDefinitions.length));
                traitNameToDefMap.put(traitDef.typeName, traitDef);
                transientTypes.add(traitDef.typeName);
            }

            for (HierarchicalTypeDefinition<ClassType> classDef : classDefs) {
                assert classDef.typeName != null;
                if (types.containsKey(classDef.typeName)) {
                    throw new AtlasException(String.format("Cannot redefine type %s", classDef.typeName));
                }

                TypeSystem.this.types.put(classDef.typeName, new ClassType(this, classDef.typeName, classDef.superTypes,
                                classDef.attributeDefinitions.length));
                classNameToDefMap.put(classDef.typeName, classDef);
                transientTypes.add(classDef.typeName);
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
            if (transientTypes.contains(attrDef.dataTypeName)) {
                recursiveRefs.add(info);
            }
            if (info.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
                DataTypes.ArrayType arrType = (DataTypes.ArrayType) info.dataType();
                if (transientTypes.contains(arrType.getElemType().getName())) {
                    recursiveArrayTypes.add(arrType);
                }
            }
            if (info.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP) {
                DataTypes.MapType mapType = (DataTypes.MapType) info.dataType();
                if (transientTypes.contains(mapType.getKeyType().getName())) {
                    recursiveMapTypes.add(mapType);
                } else if (transientTypes.contains(mapType.getValueType().getName())) {
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

            StructType type = new StructType(TypeSystem.this, def.typeName, null, infos);
            TypeSystem.this.types.put(def.typeName, type);
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
                U type = cons.newInstance(TypeSystem.this, def.typeName, def.superTypes, infos);
                TypeSystem.this.types.put(def.typeName, type);
                return type;
            } catch (Exception e) {
                throw new AtlasException(String.format("Cannot construct Type of MetaType %s", cls.getName()), e);
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
                typeCategoriesToTypeNamesMap.put(DataTypes.TypeCategory.CLASS, structDef.typeName);
            }

            for (TraitType traitType : traitTypes) {
                constructHierarchicalType(TraitType.class, traitNameToDefMap.get(traitType.getName()));
                typeCategoriesToTypeNamesMap.put(DataTypes.TypeCategory.TRAIT, traitType.getName());
            }

            for (ClassType classType : classTypes) {
                constructHierarchicalType(ClassType.class, classNameToDefMap.get(classType.getName()));
                typeCategoriesToTypeNamesMap.put(DataTypes.TypeCategory.CLASS, classType.getName());
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

        Map<String, IDataType> defineTypes() throws AtlasException {
            try {
                step1();
                step2();

                step3();
                step4();
            } catch (AtlasException me) {
                for (String sT : transientTypes) {
                    types.remove(sT);
                }
                throw me;
            }

            Map<String, IDataType> newTypes = new HashMap<>();

            for (String tName : transientTypes) {
                newTypes.put(tName, dataType(tName));
            }
            return newTypes;
        }

        @Override
        public ImmutableList<String> getTypeNames() {
            return TypeSystem.this.getTypeNames();
        }

        @Override
        public <T> T getDataType(Class<T> cls, String name) throws AtlasException {
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
        public Map<String, IDataType> defineTypes(ImmutableList<StructTypeDefinition> structDefs,
                ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs,
                ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs) throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public DataTypes.ArrayType defineArrayType(IDataType elemType) throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public DataTypes.MapType defineMapType(IDataType keyType, IDataType valueType) throws AtlasException {
            throw new AtlasException("Internal Error: define type called on TrasientTypeSystem");
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

                StructType type = new StructType(TypeSystem.this, TYP_NAME, null, infos);
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
