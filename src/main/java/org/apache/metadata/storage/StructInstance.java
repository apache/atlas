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
package org.apache.metadata.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.metadata.IStruct;
import org.apache.metadata.ITypedStruct;
import org.apache.metadata.MetadataException;
import org.apache.metadata.MetadataService;
import org.apache.metadata.types.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

public class StructInstance implements ITypedStruct {
    public final String dataTypeName;
    public final FieldMapping fieldMapping;
    public final boolean nullFlags[];
    public final boolean[] bools;
    public final byte[] bytes;
    public final short[] shorts;
    public final int[] ints;
    public final long[] longs;
    public final float[] floats;
    public final double[] doubles;
    public final BigDecimal[] bigDecimals;
    public final BigInteger[] bigIntegers;
    public final Date[] dates;
    public final String[] strings;
    public final ImmutableList<Object>[] arrays;
    public final ImmutableMap<Object,Object>[] maps;
    public final StructInstance[] structs;
    public final ReferenceableInstance[] referenceables;
    public final Id[] ids;

    public StructInstance(String dataTypeName, FieldMapping fieldMapping,
                          boolean[] nullFlags, boolean[] bools, byte[] bytes, short[] shorts, int[] ints,
                          long[] longs, float[] floats, double[] doubles,
                          BigDecimal[] bigDecimals, BigInteger[] bigIntegers, Date[] dates, String[] strings,
                          ImmutableList<Object>[] arrays, ImmutableMap<Object, Object>[] maps,
                          StructInstance[] structs, ReferenceableInstance[] referenceables, Id[] ids) {
        assert dataTypeName != null;
        this.dataTypeName = dataTypeName;
        this.fieldMapping = fieldMapping;
        this.nullFlags = nullFlags;
        this.bools = bools;
        this.bytes = bytes;
        this.shorts = shorts;
        this.ints = ints;
        this.longs = longs;
        this.floats = floats;
        this.doubles = doubles;
        this.bigDecimals = bigDecimals;
        this.bigIntegers = bigIntegers;
        this.dates = dates;
        this.strings = strings;
        this.arrays = arrays;
        this.maps = maps;
        this.structs = structs;
        this.referenceables = referenceables;
        this.ids = ids;

        for(int i=0; i<nullFlags.length; i++) {
            nullFlags[i] = true;
        }
    }

    @Override
    public String getTypeName() {
        return dataTypeName;
    }

    @Override
    public FieldMapping fieldMapping() {
        return fieldMapping;
    }

    public void set(String attrName, Object val) throws MetadataException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if ( i == null ) {
            throw new ValueConversionException(getTypeName(), val, "Unknown field " + attrName);
        }
        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);
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
            nullFlags[nullPos] = true;
            return;
        }
        nullFlags[nullPos] = false;
        if ( i.dataType() == DataTypes.BOOLEAN_TYPE ) {
            bools[pos] = ((Boolean)cVal).booleanValue();
        } else if ( i.dataType() == DataTypes.BYTE_TYPE ) {
            bytes[pos] = ((Byte)cVal).byteValue();
        } else if ( i.dataType() == DataTypes.SHORT_TYPE ) {
            shorts[pos] = ((Short)cVal).shortValue();
        } else if ( i.dataType() == DataTypes.INT_TYPE ) {
            ints[pos] = ((Integer)cVal).intValue();
        } else if ( i.dataType() == DataTypes.LONG_TYPE ) {
            longs[pos] = ((Long)cVal).longValue();
        } else if ( i.dataType() == DataTypes.FLOAT_TYPE ) {
            floats[pos] = ((Float)cVal).floatValue();
        } else if ( i.dataType() == DataTypes.DOUBLE_TYPE ) {
            doubles[pos] = ((Double)cVal).doubleValue();
        } else if ( i.dataType() == DataTypes.BIGINTEGER_TYPE ) {
            bigIntegers[pos] = (BigInteger) cVal;
        } else if ( i.dataType() == DataTypes.BIGDECIMAL_TYPE ) {
            bigDecimals[pos] = (BigDecimal) cVal;
        } else if ( i.dataType() == DataTypes.DATE_TYPE ) {
            dates[pos] = (Date) cVal;
        } else if ( i.dataType() == DataTypes.STRING_TYPE ) {
            strings[pos] = (String) cVal;
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY ) {
            arrays[pos] = (ImmutableList) cVal;
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP ) {
            maps[pos] = (ImmutableMap) cVal;
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT ||
                i.dataType().getTypeCategory() == DataTypes.TypeCategory.TRAIT ) {
            structs[pos] = (StructInstance) cVal;
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS ) {
            if ( cVal instanceof  Id ) {
                ids[pos] = (Id) cVal;
            } else {
                referenceables[pos] = (ReferenceableInstance) cVal;
            }
        } else {
            throw new MetadataException(String.format("Unknown datatype %s", i.dataType()));
        }
    }

    public Object get(String attrName) throws MetadataException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if ( i == null ) {
            throw new MetadataException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }
        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if ( nullFlags[nullPos]) {
            return null;
        }

        if ( i.dataType() == DataTypes.BOOLEAN_TYPE ) {
            return bools[pos];
        } else if ( i.dataType() == DataTypes.BYTE_TYPE ) {
            return bytes[pos];
        } else if ( i.dataType() == DataTypes.SHORT_TYPE ) {
            return shorts[pos];
        } else if ( i.dataType() == DataTypes.INT_TYPE ) {
            return ints[pos];
        } else if ( i.dataType() == DataTypes.LONG_TYPE ) {
            return longs[pos];
        } else if ( i.dataType() == DataTypes.FLOAT_TYPE ) {
            return floats[pos];
        } else if ( i.dataType() == DataTypes.DOUBLE_TYPE ) {
            return doubles[pos];
        } else if ( i.dataType() == DataTypes.BIGINTEGER_TYPE ) {
            return bigIntegers[pos];
        } else if ( i.dataType() == DataTypes.BIGDECIMAL_TYPE ) {
            return bigDecimals[pos];
        } else if ( i.dataType() == DataTypes.DATE_TYPE ) {
            return dates[pos];
        } else if ( i.dataType() == DataTypes.STRING_TYPE ) {
            return strings[pos];
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY ) {
            return arrays[pos];
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP ) {
            return maps[pos];
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT  ||
                i.dataType().getTypeCategory() == DataTypes.TypeCategory.TRAIT ) {
            return structs[pos];
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS ) {
            if ( ids[pos] != null ) {
                return ids[pos];
            } else {
                return referenceables[pos];
            }
        } else {
            throw new MetadataException(String.format("Unknown datatype %s", i.dataType()));
        }
    }


    public void output(IStruct s, Appendable buf, String prefix) throws MetadataException {
        TypeUtils.outputVal("{", buf, prefix);
        if ( s == null ) {
            TypeUtils.outputVal("<null>\n", buf, "");
            return;
        }
        TypeUtils.outputVal("\n", buf, "");
        String fieldPrefix = prefix + "\t";
        for(Map.Entry<String,AttributeInfo> e : fieldMapping.fields.entrySet()) {
            String attrName = e.getKey();
            AttributeInfo i = e.getValue();
            Object aVal = s.get(attrName);
            TypeUtils.outputVal(attrName + " : ", buf, fieldPrefix);
            i.dataType().output(aVal, buf, "");
            TypeUtils.outputVal("\n", buf, "");
        }
        TypeUtils.outputVal("\n}\n", buf, "");
    }

    @Override
    public String toString()  {
        try {
            StringBuilder buf = new StringBuilder();
            String prefix = "";

            fieldMapping.output(this, buf, prefix);
            return buf.toString();

        } catch(MetadataException me) {
            throw new RuntimeException(me);
        }
    }
}
