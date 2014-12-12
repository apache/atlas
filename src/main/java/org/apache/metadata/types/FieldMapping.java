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
import org.apache.metadata.MetadataException;
import org.apache.metadata.MetadataService;
import org.apache.metadata.storage.Id;
import org.apache.metadata.storage.ReferenceableInstance;
import org.apache.metadata.storage.StructInstance;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class FieldMapping {

    public final Map<String,AttributeInfo> fields;
    private final Map<String, Integer> fieldPos;
    private final Map<String, Integer> fieldNullPos;
    public final int numBools;
    public final int numBytes;
    public final int numShorts;
    public final int numInts;
    public final int numLongs;
    public final int numFloats;
    public final int numDoubles;
    public final int numBigInts;
    public final int numBigDecimals;
    public final int numDates;
    public final int numStrings;
    public final int numArrays;
    public final int numMaps;
    public final int numStructs;
    public final int numReferenceables;

    public FieldMapping(Map<String, AttributeInfo> fields, Map<String, Integer> fieldPos,
                        Map<String, Integer> fieldNullPos, int numBools, int numBytes, int numShorts,
                        int numInts, int numLongs, int numFloats, int numDoubles, int numBigInts, int numBigDecimals,
                        int numDates, int numStrings, int numArrays, int numMaps, int numStructs,
                        int numReferenceables) {
        this.fields = fields;
        this.fieldPos = fieldPos;
        this.fieldNullPos = fieldNullPos;
        this.numBools = numBools;
        this.numBytes = numBytes;
        this.numShorts = numShorts;
        this.numInts = numInts;
        this.numLongs = numLongs;
        this.numFloats = numFloats;
        this.numDoubles = numDoubles;
        this.numBigInts = numBigInts;
        this.numBigDecimals = numBigDecimals;
        this.numDates = numDates;
        this.numStrings = numStrings;
        this.numArrays = numArrays;
        this.numMaps = numMaps;
        this.numStructs = numStructs;
        this.numReferenceables = numReferenceables;
    }

    public void set(StructInstance s, String attrName, Object val) throws MetadataException {
        AttributeInfo i = fields.get(attrName);
        if ( i == null ) {
            throw new ValueConversionException(s.getTypeName(), val, "Unknown field " + attrName);
        }
        int pos = fieldPos.get(attrName);
        int nullPos = fieldNullPos.get(attrName);
        Object cVal = null;

        if (val != null && val instanceof Id) {
            ClassType clsType =
                    MetadataService.getCurrentTypeSystem().getDataType(ClassType.class, i.dataType().getName());
            clsType.validateId((Id)cVal);
            cVal = val;
        } else {
            cVal = i.dataType().convert(val, i.multiplicity);
        }
        if ( cVal == null ) {
            s.nullFlags[nullPos] = true;
            return;
        }
        s.nullFlags[nullPos] = false;
        if ( i.dataType() == DataTypes.BOOLEAN_TYPE ) {
            s.bools[pos] = ((Boolean)cVal).booleanValue();
        } else if ( i.dataType() == DataTypes.BYTE_TYPE ) {
            s.bytes[pos] = ((Byte)cVal).byteValue();
        } else if ( i.dataType() == DataTypes.SHORT_TYPE ) {
            s.shorts[pos] = ((Short)cVal).shortValue();
        } else if ( i.dataType() == DataTypes.INT_TYPE ) {
            s.ints[pos] = ((Integer)cVal).intValue();
        } else if ( i.dataType() == DataTypes.LONG_TYPE ) {
            s.longs[pos] = ((Long)cVal).longValue();
        } else if ( i.dataType() == DataTypes.FLOAT_TYPE ) {
            s.floats[pos] = ((Float)cVal).floatValue();
        } else if ( i.dataType() == DataTypes.DOUBLE_TYPE ) {
            s.doubles[pos] = ((Double)cVal).doubleValue();
        } else if ( i.dataType() == DataTypes.BIGINTEGER_TYPE ) {
            s.bigIntegers[pos] = (BigInteger) cVal;
        } else if ( i.dataType() == DataTypes.BIGDECIMAL_TYPE ) {
            s.bigDecimals[pos] = (BigDecimal) cVal;
        } else if ( i.dataType() == DataTypes.DATE_TYPE ) {
            s.dates[pos] = (Date) cVal;
        } else if ( i.dataType() == DataTypes.STRING_TYPE ) {
            s.strings[pos] = (String) cVal;
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY ) {
            s.arrays[pos] = (ImmutableList) cVal;
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP ) {
            s.maps[pos] = (ImmutableMap) cVal;
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT ||
                i.dataType().getTypeCategory() == DataTypes.TypeCategory.TRAIT ) {
            s.structs[pos] = (StructInstance) cVal;
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS ) {
            if ( cVal instanceof  Id ) {
                s.ids[pos] = (Id) cVal;
            } else {
                s.referenceables[pos] = (ReferenceableInstance) cVal;
            }
        } else {
            throw new MetadataException(String.format("Unknown datatype %s", i.dataType()));
        }
    }

    public Object get(StructInstance s, String attrName) throws MetadataException {
        AttributeInfo i = fields.get(attrName);
        if ( i == null ) {
            throw new MetadataException(String.format("Unknown field %s for Struct %s", attrName, s.getTypeName()));
        }
        int pos = fieldPos.get(attrName);
        int nullPos = fieldNullPos.get(attrName);

        if ( s.nullFlags[nullPos]) {
            return null;
        }

        if ( i.dataType() == DataTypes.BOOLEAN_TYPE ) {
            return s.bools[pos];
        } else if ( i.dataType() == DataTypes.BYTE_TYPE ) {
            return s.bytes[pos];
        } else if ( i.dataType() == DataTypes.SHORT_TYPE ) {
            return s.shorts[pos];
        } else if ( i.dataType() == DataTypes.INT_TYPE ) {
            return s.ints[pos];
        } else if ( i.dataType() == DataTypes.LONG_TYPE ) {
            return s.longs[pos];
        } else if ( i.dataType() == DataTypes.FLOAT_TYPE ) {
            return s.floats[pos];
        } else if ( i.dataType() == DataTypes.DOUBLE_TYPE ) {
            return s.doubles[pos];
        } else if ( i.dataType() == DataTypes.BIGINTEGER_TYPE ) {
            return s.bigIntegers[pos];
        } else if ( i.dataType() == DataTypes.BIGDECIMAL_TYPE ) {
            return s.bigDecimals[pos];
        } else if ( i.dataType() == DataTypes.DATE_TYPE ) {
            return s.dates[pos];
        } else if ( i.dataType() == DataTypes.STRING_TYPE ) {
            return s.strings[pos];
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY ) {
            return s.arrays[pos];
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP ) {
            return s.maps[pos];
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT ) {
            return s.structs[pos];
        } else {
            throw new MetadataException(String.format("Unknown datatype %s", i.dataType()));
        }
    }

}
