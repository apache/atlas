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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.metadata.*;
import org.apache.metadata.storage.Id;
import org.apache.metadata.storage.ReferenceableInstance;
import org.apache.metadata.storage.StructInstance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public class ClassType extends HierarchicalType<ClassType, IReferenceableInstance>
        implements IConstructableType<IReferenceableInstance, ITypedReferenceableInstance> {

    public static final String TRAIT_NAME_SEP = "::";

    /**
     * Used when creating a ClassType, to support recursive Structs.
     */
    ClassType(TypeSystem typeSystem, String name, ImmutableList<String> superTypes, int numFields) {
        super(typeSystem, name, superTypes, numFields);
    }

    ClassType(TypeSystem typeSystem, String name, ImmutableList<String> superTraits, AttributeInfo... fields)
            throws MetadataException {
        super(typeSystem, name, superTraits, fields);
    }

    @Override
    public DataTypes.TypeCategory getTypeCategory() {
        return DataTypes.TypeCategory.CLASS;
    }

    public void validateId(Id id) throws MetadataException {
        if ( id != null ) {
            ClassType cType = typeSystem.getDataType(ClassType.class, id.className);
            if ( isSubType(cType.getName()) ) {
                return;
            }
            throw new MetadataException(String.format("Id %s is not valid for class %s", id, getName()));
        }
    }

    @Override
    public ITypedReferenceableInstance convert(Object val, Multiplicity m) throws MetadataException {

        if ( val != null ) {
            if ( val instanceof Struct) {
                Struct s = (Struct) val;
                Referenceable r = null;

                if ( s.typeName != getName() ) {
                    throw new ValueConversionException(this, val);
                }

                if ( val instanceof Referenceable ) {
                     r = (Referenceable)val;
                }

                ITypedReferenceableInstance tr = r != null ?
                        createInstanceWithTraits(r, r.getTraits().toArray(new String[0])) : createInstance();

                for(Map.Entry<String,AttributeInfo> e : fieldMapping.fields.entrySet() ) {
                    String attrKey = e.getKey();
                    AttributeInfo i = e.getValue();
                    Object aVal = s.get(attrKey);
                    try {
                        tr.set(attrKey, aVal);
                    } catch(ValueConversionException ve) {
                        throw new ValueConversionException(this, val, ve);
                    }
                }

                return tr;
            } else if ( val instanceof ReferenceableInstance ) {
                validateId(((ReferenceableInstance)val).getId());
                return (ReferenceableInstance) val;
            } else {
                throw new ValueConversionException(this, val);
            }
        }
        if (!m.nullAllowed() ) {
            throw new ValueConversionException.NullConversionException(m);
        }
        return null;
    }

    @Override
    public ITypedReferenceableInstance createInstance() throws MetadataException {
        return createInstanceWithTraits(null);
    }

    public ITypedReferenceableInstance createInstanceWithTraits(Referenceable r, String... traitNames)
    throws MetadataException {

        ImmutableMap.Builder<String, ITypedStruct> b = new ImmutableBiMap.Builder<String, ITypedStruct>();
        for(String t : traitNames) {
            TraitType tType = typeSystem.getDataType(TraitType.class, t);
            IStruct iTraitObject = r == null ? null : r.getTrait(t);
            ITypedStruct trait = iTraitObject == null ? tType.createInstance() :
                    tType.convert(iTraitObject, Multiplicity.REQUIRED);
            b.put(t, trait);
        }

        return new ReferenceableInstance(new Id(getName()),
                getName(),
                fieldMapping,
                new boolean[fieldMapping.fields.size()],
                fieldMapping.numBools == 0 ? null : new boolean[fieldMapping.numBools],
                fieldMapping.numBytes == 0 ? null : new byte[fieldMapping.numBytes],
                fieldMapping.numShorts == 0 ? null : new short[fieldMapping.numShorts],
                fieldMapping.numInts == 0 ? null : new int[fieldMapping.numInts],
                fieldMapping.numLongs == 0 ? null : new long[fieldMapping.numLongs],
                fieldMapping.numFloats == 0 ? null : new float[fieldMapping.numFloats],
                fieldMapping.numDoubles == 0 ? null : new double[fieldMapping.numDoubles],
                fieldMapping.numBigDecimals == 0 ? null : new BigDecimal[fieldMapping.numBigDecimals],
                fieldMapping.numBigInts == 0 ? null : new BigInteger[fieldMapping.numBigInts],
                fieldMapping.numDates == 0 ? null : new Date[fieldMapping.numDates],
                fieldMapping.numStrings == 0 ? null : new String[fieldMapping.numStrings],
                fieldMapping.numArrays == 0 ? null : new ImmutableList[fieldMapping.numArrays],
                fieldMapping.numMaps == 0 ? null : new ImmutableMap[fieldMapping.numMaps],
                fieldMapping.numStructs == 0 ? null : new StructInstance[fieldMapping.numStructs],
                fieldMapping.numReferenceables == 0 ? null : new ReferenceableInstance[fieldMapping.numReferenceables],
                fieldMapping.numReferenceables == 0 ? null : new Id[fieldMapping.numReferenceables],
                b.build());
    }

    @Override
    public void output(IReferenceableInstance s, Appendable buf, String prefix) throws MetadataException {
        TypeUtils.outputVal("{", buf, prefix);
        if ( s == null ) {
            TypeUtils.outputVal("<null>\n", buf, "");
            return;
        }
        TypeUtils.outputVal("\n", buf, "");
        String fieldPrefix = prefix + "\t";

        TypeUtils.outputVal("id : ", buf, fieldPrefix);
        TypeUtils.outputVal(s.getId().toString(), buf, "");
        TypeUtils.outputVal("\n", buf, "");

        for(AttributeInfo i : fieldMapping.fields.values()) {
            Object aVal = s.get(i.name);
            TypeUtils.outputVal(i.name + " : ", buf, fieldPrefix);
            i.dataType().output(aVal, buf, "");
            TypeUtils.outputVal("\n", buf, "");
        }

        for(String sT : s.getTraits() ) {
            TraitType tt = typeSystem.getDataType(TraitType.class, sT);
            TypeUtils.outputVal(sT + " : ", buf, fieldPrefix);
            tt.output(s.getTrait(sT), buf, fieldPrefix);
        }

        TypeUtils.outputVal("}", buf, fieldPrefix);
    }

}