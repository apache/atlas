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
import com.google.common.collect.ImmutableMap;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.persistence.StructInstance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TypedStructHandler {

    private final IConstructableType<IStruct, ITypedStruct> structType;
    private final FieldMapping fieldMapping;

    public TypedStructHandler(IConstructableType<IStruct, ITypedStruct> structType) {
        this.structType = structType;
        fieldMapping = structType.fieldMapping();
    }

    public ITypedStruct convert(Object val, Multiplicity m) throws AtlasException {
        if (val != null) {
            if (val instanceof ITypedStruct) {
                ITypedStruct ts = (ITypedStruct) val;
                if (!Objects.equals(ts.getTypeName(), structType.getName())) {
                    throw new ValueConversionException(structType, val);
                }
                return ts;
            } else if (val instanceof Struct) {
                Struct s = (Struct) val;
                if (!s.typeName.equals(structType.getName())) {
                    throw new ValueConversionException(structType, val);
                }
                ITypedStruct ts = createInstance();
                for (Map.Entry<String, AttributeInfo> e : fieldMapping.fields.entrySet()) {
                    String attrKey = e.getKey();
                    AttributeInfo i = e.getValue();
                    Object aVal = s.get(attrKey);
                    try {
                        ts.set(attrKey, aVal);
                    } catch (ValueConversionException ve) {
                        throw new ValueConversionException(structType, val, ve);
                    }
                }
                return ts;
            } else if (val instanceof StructInstance && Objects.equals(((StructInstance) val).getTypeName(), structType.getName())) {
                return (StructInstance) val;
            } else if (val instanceof Map) {
                Map s = (Map) val;

                ITypedStruct ts = createInstance();
                for (Map.Entry<String, AttributeInfo> e : fieldMapping.fields.entrySet()) {
                    String attrKey = e.getKey();
                    AttributeInfo i = e.getValue();
                    Object aVal = s.get(attrKey);
                    try {
                        ts.set(attrKey, aVal);
                    } catch (ValueConversionException ve) {
                        throw new ValueConversionException(structType, val, ve);
                    }
                }
                return ts;
            } else {
                throw new ValueConversionException(structType, val);
            }
        }
        if (!m.nullAllowed()) {
            throw new ValueConversionException.NullConversionException(m);
        }
        return null;
    }

    public DataTypes.TypeCategory getTypeCategory() {
        return DataTypes.TypeCategory.STRUCT;
    }

    public ITypedStruct createInstance() {
        return new StructInstance(structType.getName(), fieldMapping, new boolean[fieldMapping.fields.size()],
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
                fieldMapping.numReferenceables == 0 ? null : new Id[fieldMapping.numReferenceables]);
    }

    public void output(IStruct s, Appendable buf, String prefix, Set<IStruct> inProcess) throws AtlasException {
        fieldMapping.output(s, buf, prefix, inProcess);
    }

}
