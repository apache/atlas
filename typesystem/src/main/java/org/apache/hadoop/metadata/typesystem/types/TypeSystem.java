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

package org.apache.hadoop.metadata.typesystem.types;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.typesystem.TypesDef;

import javax.inject.Singleton;
import java.lang.reflect.Constructor;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Singleton
public class TypeSystem {

    private static final TypeSystem INSTANCE = new TypeSystem();
    public static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private Map<String, IDataType> types;

    private TypeSystem() {
        initialize();
    }

    public static TypeSystem getInstance() {
        return INSTANCE;
    }

    /**
     * This is only used for testing prurposes.
     * @nonpublic
     */
    public void reset() {
        initialize();
    }

    private void initialize() {
        types = new HashMap<>();
        registerPrimitiveTypes();
    }

    public ImmutableList<String> getTypeNames() {
        return ImmutableList.copyOf(types.keySet());
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
    }

    public <T> T getDataType(Class<T> cls, String name) throws MetadataException {
        if (types.containsKey(name)) {
            return cls.cast(types.get(name));
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
            IDataType dT = defineMapType(getDataType(IDataType.class, mapType[0]),
                    getDataType(IDataType.class, mapType[1]));
            return cls.cast(dT);
        }

        throw new MetadataException(String.format("Unknown datatype: %s", name));
    }

    public StructType defineStructType(String name,
                                       boolean errorIfExists,
                                       AttributeDefinition... attrDefs) throws MetadataException {
        StructTypeDefinition structDef = new StructTypeDefinition(name, attrDefs);
        defineTypes(ImmutableList.of(structDef),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());

        return getDataType(StructType.class, structDef.typeName);
    }

    /**
     * construct a temporary StructType for a Query Result. This is not registered in the
     * typeSystem.
     * The attributes in the typeDefinition can only reference permanent types.
     * @param name
     * @param attrDefs
     * @return
     * @throws MetadataException
     */
    public StructType defineQueryResultType(String name,
                                            AttributeDefinition... attrDefs)
            throws MetadataException {

        AttributeInfo[] infos = new AttributeInfo[attrDefs.length];
        for (int i = 0; i < attrDefs.length; i++) {
            infos[i] = new AttributeInfo(this, attrDefs[i]);
        }
        StructType type = new StructType(TypeSystem.this, name, null, infos);
        return type;
    }

    public TraitType defineTraitType(HierarchicalTypeDefinition<TraitType> traitDef)
    throws MetadataException {

        defineTypes(ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(traitDef),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());

        return getDataType(TraitType.class, traitDef.typeName);
    }

    public ClassType defineClassType(HierarchicalTypeDefinition<ClassType> classDef)
    throws MetadataException {

        defineTypes(ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(classDef));

        return getDataType(ClassType.class, classDef.typeName);
    }

    public Map<String, IDataType> defineTraitTypes(
            HierarchicalTypeDefinition<TraitType>... traitDefs)
    throws MetadataException {
        TransientTypeSystem transientTypes = new TransientTypeSystem(
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.copyOf(traitDefs),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());
        return transientTypes.defineTypes();
    }

    public Map<String, IDataType> defineTypes(TypesDef typesDef)
    throws MetadataException {

        for (EnumTypeDefinition enumDef : typesDef.enumTypesAsJavaList()) {
            defineEnumType(enumDef);
        }

        ImmutableList<StructTypeDefinition> structDefs = ImmutableList
                .copyOf(typesDef.structTypesAsJavaList());
        ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs =
                ImmutableList.copyOf(typesDef.traitTypesAsJavaList());
        ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs =
                ImmutableList.copyOf(typesDef.classTypesAsJavaList());

        return defineTypes(structDefs, traitDefs, classDefs);
    }

    public Map<String, IDataType> defineTypes(ImmutableList<StructTypeDefinition> structDefs,
                                              ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs,
                                              ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs)
    throws MetadataException {
        TransientTypeSystem transientTypes = new TransientTypeSystem(structDefs,
                traitDefs,
                classDefs);
        return transientTypes.defineTypes();
    }

    public DataTypes.ArrayType defineArrayType(IDataType elemType) throws MetadataException {
        assert elemType != null;
        DataTypes.ArrayType dT = new DataTypes.ArrayType(elemType);
        types.put(dT.getName(), dT);
        return dT;
    }

    public DataTypes.MapType defineMapType(IDataType keyType, IDataType valueType)
    throws MetadataException {
        assert keyType != null;
        assert valueType != null;
        DataTypes.MapType dT = new DataTypes.MapType(keyType, valueType);
        types.put(dT.getName(), dT);
        return dT;
    }

    public EnumType defineEnumType(String name, EnumValue... values) throws MetadataException {
        return defineEnumType(new EnumTypeDefinition(name, values));
    }

    public EnumType defineEnumType(EnumTypeDefinition eDef) throws MetadataException {
        assert eDef.name != null;
        if (types.containsKey(eDef.name)) {
            throw new MetadataException(
                    String.format("Redefinition of type %s not supported", eDef.name));
        }
        EnumType eT = new EnumType(this, eDef.name, eDef.enumValues);
        types.put(eDef.name, eT);
        return eT;
    }

    public DateFormat getDateFormat() {
        return dateFormat;
    }

    public boolean allowNullsInCollections() {
        return false;
    }

    class TransientTypeSystem extends TypeSystem {

        final ImmutableList<StructTypeDefinition> structDefs;
        final ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs;
        final ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs;
        Map<String, StructTypeDefinition> structNameToDefMap = new HashMap<>();
        Map<String, HierarchicalTypeDefinition<TraitType>> traitNameToDefMap =
                new HashMap<>();
        Map<String, HierarchicalTypeDefinition<ClassType>> classNameToDefMap =
                new HashMap<>();

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
        private void step1() throws MetadataException {
            for (StructTypeDefinition sDef : structDefs) {
                assert sDef.typeName != null;
                TypeUtils.validateName(sDef.typeName);
                if (dataType(sDef.typeName) != null) {
                    throw new MetadataException(
                            String.format("Cannot redefine type %s", sDef.typeName));
                }
                TypeSystem.this.types.put(sDef.typeName,
                        new StructType(this, sDef.typeName, sDef.attributeDefinitions.length));
                structNameToDefMap.put(sDef.typeName, sDef);
                transientTypes.add(sDef.typeName);
            }

            for (HierarchicalTypeDefinition<TraitType> traitDef : traitDefs) {
                assert traitDef.typeName != null;
                TypeUtils.validateName(traitDef.typeName);
                if (types.containsKey(traitDef.typeName)) {
                    throw new MetadataException(
                            String.format("Cannot redefine type %s", traitDef.typeName));
                }

                TypeSystem.this.types.put(traitDef.typeName,
                        new TraitType(this, traitDef.typeName, traitDef.superTypes,
                                traitDef.attributeDefinitions.length));
                traitNameToDefMap.put(traitDef.typeName, traitDef);
                transientTypes.add(traitDef.typeName);
            }

            for (HierarchicalTypeDefinition<ClassType> classDef : classDefs) {
                assert classDef.typeName != null;
                TypeUtils.validateName(classDef.typeName);
                if (types.containsKey(classDef.typeName)) {
                    throw new MetadataException(
                            String.format("Cannot redefine type %s", classDef.typeName));
                }

                TypeSystem.this.types.put(classDef.typeName,
                        new ClassType(this, classDef.typeName, classDef.superTypes,
                                classDef.attributeDefinitions.length));
                classNameToDefMap.put(classDef.typeName, classDef);
                transientTypes.add(classDef.typeName);
            }
        }

        private <U extends HierarchicalType> void validateSuperTypes(Class<U> cls,
                                                                     HierarchicalTypeDefinition<U> def)
        throws MetadataException {
            Set<String> s = new HashSet<>();
            ImmutableList<String> superTypes = def.superTypes;
            for (String superTypeName : superTypes) {

                if (s.contains(superTypeName)) {
                    throw new MetadataException(
                            String.format("Type %s extends superType %s multiple times",
                                    def.typeName, superTypeName));
                }

                IDataType dT = dataType(superTypeName);

                if (dT == null) {
                    throw new MetadataException(
                            String.format("Unknown superType %s in definition of type %s",
                                    superTypeName, def.typeName));
                }

                if (!cls.isAssignableFrom(dT.getClass())) {
                    throw new MetadataException(
                            String.format("SuperType %s must be a %s, in definition of type %s",
                                    superTypeName, cls.getName(), def.typeName));
                }
                s.add(superTypeName);
            }
        }

        /*
         * Step 2:
         * - for Hierarchical Types, validate SuperTypes.
         * - for each Hierarchical Type setup their SuperTypes Graph
         */
        private void step2() throws MetadataException {
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

        private AttributeInfo constructAttributeInfo(AttributeDefinition attrDef)
        throws MetadataException {
            AttributeInfo info = new AttributeInfo(this, attrDef);
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

            return info;
        }

        private StructType constructStructureType(StructTypeDefinition def)
        throws MetadataException {
            AttributeInfo[] infos = new AttributeInfo[def.attributeDefinitions.length];
            for (int i = 0; i < def.attributeDefinitions.length; i++) {
                infos[i] = constructAttributeInfo(def.attributeDefinitions[i]);
            }

            StructType type = new StructType(TypeSystem.this, def.typeName, null, infos);
            TypeSystem.this.types.put(def.typeName, type);
            return type;
        }

        private <U extends HierarchicalType> U constructHierarchicalType(Class<U> cls,
                                                                         HierarchicalTypeDefinition<U> def)
        throws MetadataException {
            AttributeInfo[] infos = new AttributeInfo[def.attributeDefinitions.length];
            for (int i = 0; i < def.attributeDefinitions.length; i++) {
                infos[i] = constructAttributeInfo(def.attributeDefinitions[i]);
            }

            try {
                Constructor<U> cons = cls.getDeclaredConstructor(new Class[]{
                        TypeSystem.class,
                        String.class,
                        ImmutableList.class,
                        AttributeInfo[].class});
                U type = cons.newInstance(TypeSystem.this, def.typeName, def.superTypes, infos);
                TypeSystem.this.types.put(def.typeName, type);
                return type;
            } catch (Exception e) {
                throw new MetadataException(
                        String.format("Cannot construct Type of MetaType %s", cls.getName()), e);
            }
        }

        /*
         * Step 3:
         * - Order Hierarchical Types in order of SuperType before SubType.
         * - Construct all the Types
         */
        private void step3() throws MetadataException {

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
                constructHierarchicalType(TraitType.class,
                        traitNameToDefMap.get(traitType.getName()));
            }

            for (ClassType classType : classTypes) {
                constructHierarchicalType(ClassType.class,
                        classNameToDefMap.get(classType.getName()));
            }

        }

        /*
         * Step 4:
         * - fix up references in recursive AttrInfo and recursive Collection Types.
         */
        private void step4() throws MetadataException {
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

        Map<String, IDataType> defineTypes() throws MetadataException {
            step1();
            step2();
            try {
                step3();
                step4();
            } catch (MetadataException me) {
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
        public <T> T getDataType(Class<T> cls, String name) throws MetadataException {
            return TypeSystem.this.getDataType(cls, name);
        }

        @Override
        public StructType defineStructType(String name, boolean errorIfExists,
                                           AttributeDefinition... attrDefs)
        throws MetadataException {
            throw new MetadataException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public TraitType defineTraitType(HierarchicalTypeDefinition traitDef)
        throws MetadataException {
            throw new MetadataException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public ClassType defineClassType(HierarchicalTypeDefinition<ClassType> classDef
        ) throws MetadataException {
            throw new MetadataException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public Map<String, IDataType> defineTypes(ImmutableList<StructTypeDefinition> structDefs,
                                                  ImmutableList<HierarchicalTypeDefinition<TraitType>> traitDefs,
                                                  ImmutableList<HierarchicalTypeDefinition<ClassType>> classDefs)
        throws MetadataException {
            throw new MetadataException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public DataTypes.ArrayType defineArrayType(IDataType elemType) throws MetadataException {
            throw new MetadataException("Internal Error: define type called on TrasientTypeSystem");
        }

        @Override
        public DataTypes.MapType defineMapType(IDataType keyType, IDataType valueType)
        throws MetadataException {
            throw new MetadataException("Internal Error: define type called on TrasientTypeSystem");
        }
    }
}
