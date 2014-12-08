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
import com.google.common.collect.ImmutableMap;
import org.apache.metadata.IStruct;
import org.apache.metadata.MetadataException;
import org.apache.metadata.Struct;
import org.apache.metadata.storage.StructInstance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class StructType  extends AbstractDataType<IStruct> {

    public final String name;
    public final FieldMapping fieldMapping;

    /**
     * Used when creating a StructType, to support recursive Structs.
     */
    StructType(String name) {
        this.name = name;
        this.fieldMapping = null;
    }

    StructType(String name, AttributeInfo... fields) throws MetadataException {
        this.name = name;
        this.fieldMapping = constructFieldMapping(fields);
    }

    @Override
    public String getName() {
        return name;
    }

    protected FieldMapping constructFieldMapping(AttributeInfo... fields)
            throws MetadataException {

        Map<String,AttributeInfo> fieldsMap = new LinkedHashMap<String, AttributeInfo>();
        Map<String, Integer> fieldPos = new HashMap<String, Integer>();
        Map<String, Integer> fieldNullPos = new HashMap<String, Integer>();
        int numBools = 0;
        int numBytes = 0;
        int numShorts = 0;
        int numInts = 0;
        int numLongs = 0;
        int numFloats = 0;
        int numDoubles = 0;
        int numBigInts = 0;
        int numBigDecimals = 0;
        int numDates = 0;
        int numStrings = 0;
        int numArrays = 0;
        int numMaps = 0;
        int numStructs = 0;

        for(AttributeInfo i : fields) {
            if ( fieldsMap.containsKey(i.name) ) {
                throw new MetadataException(
                        String.format("Struct defintion cannot contain multiple fields with the same name %s", i.name));
            }
            fieldsMap.put(i.name, i);
            fieldNullPos.put(i.name, fieldNullPos.size());
            if ( i.dataType() == DataTypes.BOOLEAN_TYPE ) {
                fieldPos.put(i.name, numBools);
                numBools++;
            } else if ( i.dataType() == DataTypes.BYTE_TYPE ) {
                fieldPos.put(i.name, numBytes);
                numBytes++;
            } else if ( i.dataType() == DataTypes.SHORT_TYPE ) {
                fieldPos.put(i.name, numShorts);
                numShorts++;
            } else if ( i.dataType() == DataTypes.INT_TYPE ) {
                fieldPos.put(i.name, numInts);
                numInts++;
            } else if ( i.dataType() == DataTypes.LONG_TYPE ) {
                fieldPos.put(i.name, numLongs);
                numLongs++;
            } else if ( i.dataType() == DataTypes.FLOAT_TYPE ) {
                fieldPos.put(i.name, numFloats);
                numFloats++;
            } else if ( i.dataType() == DataTypes.DOUBLE_TYPE ) {
                fieldPos.put(i.name, numDoubles);
                numDoubles++;
            } else if ( i.dataType() == DataTypes.BIGINTEGER_TYPE ) {
                fieldPos.put(i.name, numBigInts);
                numBigInts++;
            } else if ( i.dataType() == DataTypes.BIGDECIMAL_TYPE ) {
                fieldPos.put(i.name, numBigDecimals);
                numBigDecimals++;
            } else if ( i.dataType() == DataTypes.DATE_TYPE ) {
                fieldPos.put(i.name, numDates);
                numDates++;
            } else if ( i.dataType() == DataTypes.STRING_TYPE ) {
                fieldPos.put(i.name, numStrings);
                numStrings++;
            } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY ) {
                fieldPos.put(i.name, numArrays);
                numArrays++;
            } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP ) {
                fieldPos.put(i.name, numMaps);
                numMaps++;
            } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT ) {
                fieldPos.put(i.name, numStructs);
                numStructs++;
            } else {
                throw new MetadataException(String.format("Unknown datatype %s", i.dataType()));
            }
        }

        return new FieldMapping(fieldsMap,
                fieldPos,
                fieldNullPos,
                numBools,
                numBytes,
                numShorts,
                numInts,
                numLongs,
                numFloats,
                numDoubles,
                numBigInts,
                numBigDecimals,
                numDates,
                numStrings,
                numArrays,
                numMaps,
                numStructs);
    }


    @Override
    public StructInstance convert(Object val, Multiplicity m) throws MetadataException {
        if ( val != null ) {
            if ( val instanceof Struct ) {
                Struct s = (Struct) val;
                if ( s.typeName != name ) {
                    throw new ValueConversionException(this, val);
                }
                StructInstance ts = createInstance();
                for(AttributeInfo i : fieldMapping.fields.values()) {
                    Object aVal = s.get(i.name);
                    try {
                        ts.set(i.name, aVal);
                    } catch(ValueConversionException ve) {
                        throw new ValueConversionException(this, val, ve);
                    }
                }
                return ts;
            } else if ( val instanceof StructInstance && ((StructInstance)val).getTypeName() == getName() ) {
                return (StructInstance) val;
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
    public DataTypes.TypeCategory getTypeCategory() {
        return DataTypes.TypeCategory.STRUCT;
    }

    public StructInstance createInstance() {
        return new StructInstance(getName(),
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
                fieldMapping.numStructs == 0 ? null : new StructInstance[fieldMapping.numStructs]);
    }

    @Override
    public void output(IStruct s, Appendable buf, String prefix) throws MetadataException {
        TypeUtils.outputVal("{", buf, prefix);
        if ( s == null ) {
            TypeUtils.outputVal("<null>\n", buf, "");
            return;
        }
        TypeUtils.outputVal("\n", buf, "");
        String fieldPrefix = prefix + "\t";
        for(AttributeInfo i : fieldMapping.fields.values()) {
            Object aVal = s.get(i.name);
            TypeUtils.outputVal(i.name + " : ", buf, fieldPrefix);
            i.dataType().output(aVal, buf, "");
            TypeUtils.outputVal("\n", buf, "");
        }
        TypeUtils.outputVal("\n}\n", buf, "");
    }
}
