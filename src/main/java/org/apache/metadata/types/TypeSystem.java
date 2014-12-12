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

package org.apache.metadata.types;

import com.google.common.collect.ImmutableList;
import org.apache.metadata.MetadataException;

import java.util.*;

public class TypeSystem {

    private Map<String, IDataType> types;

    public TypeSystem() throws MetadataException {
        types = new HashMap<String, IDataType>();
        registerPrimitiveTypes();
    }

    private TypeSystem(TypeSystem ts) {}

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

    public IDataType dataType(String name) {
        return types.get(name);
    }

    public <T> T getDataType(Class<T> cls, String name) throws MetadataException {
        if ( types.containsKey(name) ) {
            return cls.cast(types.get(name));
        }

        /*
         * is this an Array Type?
         */
        String arrElemType = TypeUtils.parseAsArrayType(name);
        if ( arrElemType != null ) {
            IDataType dT = defineArrayType(getDataType(IDataType.class, arrElemType));
            return cls.cast(dT);
        }

        /*
         * is this a Map Type?
         */
        String[] mapType = TypeUtils.parseAsMapType(name);
        if ( mapType != null ) {
            IDataType dT = defineMapType(getDataType(IDataType.class, mapType[0]),
                    getDataType(IDataType.class, mapType[1]));
            return cls.cast(dT);
        }

        throw new MetadataException(String.format("Unknown datatype: %s", name));
    }

    public StructType defineStructType(String name,
                                       boolean errorIfExists,
                                       AttributeDefinition... attrDefs) throws MetadataException {
         if ( types.containsKey(name) ) {
            throw new MetadataException(String.format("Cannot redefine type %s", name));
        }
        assert name != null;
        AttributeInfo[] infos = new AttributeInfo[attrDefs.length];
        Map<Integer, AttributeDefinition> recursiveRefs = new HashMap<Integer, AttributeDefinition>();
        try {
            types.put(name, new StructType(this, name, attrDefs.length));
            for (int i = 0; i < attrDefs.length; i++) {
                infos[i] = new AttributeInfo(this, attrDefs[i]);
                if ( attrDefs[i].dataTypeName == name ) {
                    recursiveRefs.put(i, attrDefs[i]);
                }
            }
        } catch(MetadataException me) {
            types.remove(name);
            throw me;
        } catch(RuntimeException re) {
            types.remove(name);
            throw re;
        }
        StructType sT = new StructType(this, name, null, infos);
        types.put(name, sT);
        for(Map.Entry<Integer, AttributeDefinition> e : recursiveRefs.entrySet()) {
            infos[e.getKey()].setDataType(sT);
        }
        return sT;
    }

    public TraitType defineTraitType(boolean errorIfExists,
                                     TraitTypeDefinition traitDef
                                       ) throws MetadataException {
        Map<String, TraitType> m = defineTraitTypes(errorIfExists, traitDef);
        return m.values().iterator().next();
    }

    public Map<String, TraitType> defineTraitTypes(boolean errorIfExists,
                                                   TraitTypeDefinition... traitDefs
                                      ) throws MetadataException {
        TransientTypeSystem transientTypes = new TransientTypeSystem();
        Map<String,TraitTypeDefinition> traitDefMap = new HashMap<String, TraitTypeDefinition>();


        /*
         * Step 1:
         * - validate cannot redefine types
         * - setup an empty TraitType to allow for recursive type graphs.
         */
        for(TraitTypeDefinition traitDef : traitDefs) {
            assert traitDef.typeName != null;
            if ( types.containsKey(traitDef.typeName) ) {
                throw new MetadataException(String.format("Cannot redefine type %s", traitDef.typeName));
            }

            transientTypes.traitTypes.put(traitDef.typeName,
                    new TraitType(transientTypes, traitDef.typeName, traitDef.superTraits,
                    traitDef.attributeDefinitions.length));
            traitDefMap.put(traitDef.typeName, traitDef);
        }

        /*
         * Step 2:
         * - validate SuperTypes.
         */
        for(TraitTypeDefinition traitDef : traitDefs) {
            Set<String> s = new HashSet<String>();
            for(String superTraitName : traitDef.superTraits ) {

                if (s.contains(superTraitName) ) {
                    throw new MetadataException(String.format("Trait %s extends superTrait %s multiple times",
                            traitDef.typeName, superTraitName));
                }

                IDataType dT = types.get(superTraitName);
                dT = dT == null ? transientTypes.traitTypes.get(superTraitName) : dT;

                if ( dT == null ) {
                    throw new MetadataException(String.format("Unknown superType %s in definition of type %s",
                            superTraitName, traitDef.typeName));
                }

                if ( dT.getTypeCategory() != DataTypes.TypeCategory.TRAIT ) {
                    throw new MetadataException(String.format("SuperType %s must be a Trait, in definition of type %s",
                            superTraitName, traitDef.typeName));
                }
                s.add(superTraitName);
            }
        }

        /*
         * Step 3:
         * - Construct TraitTypes in order of SuperType before SubType.
         */
        List<TraitType> l = new ArrayList<TraitType>(transientTypes.traitTypes.values());
        Collections.sort(l);
        List<AttributeInfo> recursiveRefs = new ArrayList<AttributeInfo>();
        List<DataTypes.ArrayType> recursiveArrayTypes = new ArrayList<DataTypes.ArrayType>();
        List<DataTypes.MapType> recursiveMapTypes = new ArrayList<DataTypes.MapType>();


        try {
            for (TraitType ttO : l) {
                TraitTypeDefinition traitDef = traitDefMap.get(ttO.getName());
                AttributeInfo[] infos = new AttributeInfo[traitDef.attributeDefinitions.length];
                for (int i = 0; i < traitDef.attributeDefinitions.length; i++) {
                    infos[i] = new AttributeInfo(this, traitDef.attributeDefinitions[i]);
                    if (transientTypes.traitTypes.containsKey(traitDef.attributeDefinitions[i].dataTypeName)) {
                        recursiveRefs.add(infos[i]);
                    }
                    if ( infos[i].dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY ) {
                        DataTypes.ArrayType arrType = (DataTypes.ArrayType) infos[i].dataType();
                        if (transientTypes.traitTypes.containsKey(arrType.getElemType().getName())) {
                            recursiveArrayTypes.add(arrType);
                        }
                    }
                    if ( infos[i].dataType().getTypeCategory() == DataTypes.TypeCategory.MAP ) {
                        DataTypes.MapType mapType = (DataTypes.MapType) infos[i].dataType();
                        if (transientTypes.traitTypes.containsKey(mapType.getKeyType().getName())) {
                            recursiveMapTypes.add(mapType);
                        } else if (transientTypes.traitTypes.containsKey(mapType.getValueType().getName())) {
                            recursiveMapTypes.add(mapType);
                        }
                    }
                }

                TraitType tt = new TraitType(this, traitDef.typeName, traitDef.superTraits, infos);
                types.put(tt.getName(), tt);
            }

            /*
             * Step 4:
             * - fix up references in recursive AttrInfo and recursive Collection Types.
             */
            for (AttributeInfo info : recursiveRefs) {
                info.setDataType(dataType(info.dataType().getName()));
            }
            for(DataTypes.ArrayType arrType : recursiveArrayTypes ) {
                arrType.setElemType(dataType(arrType.getElemType().getName()));
            }
            for(DataTypes.MapType mapType : recursiveMapTypes ) {
                mapType.setKeyType(dataType(mapType.getKeyType().getName()));
                mapType.setValueType(dataType(mapType.getValueType().getName()));
            }
        } catch(MetadataException me) {
            for(String sT : transientTypes.traitTypes.keySet()) {
                types.remove(sT);
            }
            throw me;
        }

        return transientTypes.traitTypes;
    }



    public DataTypes.ArrayType defineArrayType(IDataType elemType) throws MetadataException {
        assert elemType != null;
        DataTypes.ArrayType dT = new DataTypes.ArrayType(elemType);
        types.put(dT.getName(), dT);
        return dT;
    }

    public DataTypes.MapType defineMapType(IDataType keyType, IDataType valueType) throws MetadataException {
        assert keyType != null;
        assert valueType != null;
        DataTypes.MapType dT =  new DataTypes.MapType(keyType, valueType);
        types.put(dT.getName(), dT);
        return dT;
    }

    class TransientTypeSystem extends TypeSystem {
        Map<String, TraitType> traitTypes = new HashMap<String, TraitType>();

        TransientTypeSystem() {
            super(TypeSystem.this);
        }

        public IDataType dataType(String name) {
            IDataType dT = TypeSystem.this.dataType(name);
            dT = dT == null ? traitTypes.get(name) : dT;
            return dT;
        }

        @Override
        public ImmutableList<String> getTypeNames() {
            return TypeSystem.this.getTypeNames();
        }

        @Override
        public <T> T getDataType(Class<T> cls, String name) throws MetadataException  {
            return TypeSystem.this.getDataType(cls, name);
        }

        @Override
        public StructType defineStructType(String name, boolean errorIfExists, AttributeDefinition... attrDefs)
                throws MetadataException {
            return TypeSystem.this.defineStructType(name, errorIfExists, attrDefs);
        }

        @Override
        public TraitType defineTraitType(boolean errorIfExists, TraitTypeDefinition traitDef) throws MetadataException {
            return TypeSystem.this.defineTraitType(errorIfExists, traitDef);
        }

        @Override
        public Map<String, TraitType> defineTraitTypes(boolean errorIfExists, TraitTypeDefinition... traitDefs)
                throws MetadataException {
            return TypeSystem.this.defineTraitTypes(errorIfExists, traitDefs);
        }

        @Override
        public DataTypes.ArrayType defineArrayType(IDataType elemType) throws MetadataException {
            return TypeSystem.this.defineArrayType(elemType);
        }

        @Override
        public DataTypes.MapType defineMapType(IDataType keyType, IDataType valueType) throws MetadataException {
            return TypeSystem.this.defineMapType(keyType, valueType);
        }
    }
}
