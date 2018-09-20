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

package org.apache.atlas.typesystem.persistence;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.atlas.utils.SHA256Utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class StructInstance implements ITypedStruct {
    public final String dataTypeName;
    public final FieldMapping fieldMapping;
    public final boolean nullFlags[];
    public final boolean explicitSets[];
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
    public final ImmutableMap<Object, Object>[] maps;
    public final StructInstance[] structs;
    public final ReferenceableInstance[] referenceables;
    public final Id[] ids;

    public StructInstance(String dataTypeName, FieldMapping fieldMapping, boolean[] nullFlags, boolean[] explicitSets, boolean[] bools,
            byte[] bytes, short[] shorts, int[] ints, long[] longs, float[] floats, double[] doubles,
            BigDecimal[] bigDecimals, BigInteger[] bigIntegers, Date[] dates, String[] strings,
            ImmutableList<Object>[] arrays, ImmutableMap<Object, Object>[] maps, StructInstance[] structs,
            ReferenceableInstance[] referenceables, Id[] ids) {
        assert dataTypeName != null;
        this.dataTypeName = dataTypeName;
        this.fieldMapping = fieldMapping;
        this.nullFlags = nullFlags;
        this.explicitSets = explicitSets;
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

        for (int i = 0; i < nullFlags.length; i++) {
            nullFlags[i] = true;
        }

        for (int i = 0; i < explicitSets.length; i++) {
            explicitSets[i] = false;
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

    public void set(String attrName, Object val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new ValueConversionException(getTypeName(), val, "Unknown field " + attrName);
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);
        Object cVal = null;

        explicitSets[nullPos] = true;

        if (val != null && val instanceof Id) {
            ClassType clsType = TypeSystem.getInstance().getDataType(ClassType.class, i.dataType().getName());
            clsType.validateId((Id) val);
            cVal = val;
        } else {
            try {
                cVal = i.dataType().convert(val, i.multiplicity);
            } catch(ValueConversionException.NullConversionException e) {
                throw new ValueConversionException.NullConversionException("For field '" + attrName + "'", e);
            }
        }
        if (cVal == null) {
            nullFlags[nullPos] = true;
            return;
        }
        nullFlags[nullPos] = false;
        if (i.dataType() == DataTypes.BOOLEAN_TYPE) {
            bools[pos] = (Boolean) cVal;
        } else if (i.dataType() == DataTypes.BYTE_TYPE) {
            bytes[pos] = (Byte) cVal;
        } else if (i.dataType() == DataTypes.SHORT_TYPE) {
            shorts[pos] = (Short) cVal;
        } else if (i.dataType() == DataTypes.INT_TYPE) {
            ints[pos] = (Integer) cVal;
        } else if (i.dataType() == DataTypes.LONG_TYPE) {
            longs[pos] = (Long) cVal;
        } else if (i.dataType() == DataTypes.FLOAT_TYPE) {
            floats[pos] = (Float) cVal;
        } else if (i.dataType() == DataTypes.DOUBLE_TYPE) {
            doubles[pos] = (Double) cVal;
        } else if (i.dataType() == DataTypes.BIGINTEGER_TYPE) {
            bigIntegers[pos] = (BigInteger) cVal;
        } else if (i.dataType() == DataTypes.BIGDECIMAL_TYPE) {
            bigDecimals[pos] = (BigDecimal) cVal;
        } else if (i.dataType() == DataTypes.DATE_TYPE) {
            dates[pos] = (Date) cVal;
        } else if (i.dataType() == DataTypes.STRING_TYPE) {
            strings[pos] = (String) cVal;
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.ENUM) {
            ints[pos] = ((EnumValue) cVal).ordinal;
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
            arrays[pos] = (ImmutableList) cVal;
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP) {
            maps[pos] = (ImmutableMap) cVal;
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT
                || i.dataType().getTypeCategory() == DataTypes.TypeCategory.TRAIT) {
            structs[pos] = (StructInstance) cVal;
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
            if (cVal instanceof Id) {
                ids[pos] = (Id) cVal;
            } else {
                referenceables[pos] = (ReferenceableInstance) cVal;
            }
        } else {
            throw new AtlasException(String.format("Unknown datatype %s", i.dataType()));
        }
    }

    public Object get(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }
        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.PRIMITIVE) {
                return ((DataTypes.PrimitiveType) i.dataType()).nullValue();
            } else {
                return null;
            }
        }

        if (i.dataType() == DataTypes.BOOLEAN_TYPE) {
            return bools[pos];
        } else if (i.dataType() == DataTypes.BYTE_TYPE) {
            return bytes[pos];
        } else if (i.dataType() == DataTypes.SHORT_TYPE) {
            return shorts[pos];
        } else if (i.dataType() == DataTypes.INT_TYPE) {
            return ints[pos];
        } else if (i.dataType() == DataTypes.LONG_TYPE) {
            return longs[pos];
        } else if (i.dataType() == DataTypes.FLOAT_TYPE) {
            return floats[pos];
        } else if (i.dataType() == DataTypes.DOUBLE_TYPE) {
            return doubles[pos];
        } else if (i.dataType() == DataTypes.BIGINTEGER_TYPE) {
            return bigIntegers[pos];
        } else if (i.dataType() == DataTypes.BIGDECIMAL_TYPE) {
            return bigDecimals[pos];
        } else if (i.dataType() == DataTypes.DATE_TYPE) {
            return dates[pos];
        } else if (i.dataType() == DataTypes.STRING_TYPE) {
            return strings[pos];
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.ENUM) {
            return ((EnumType) i.dataType()).fromOrdinal(ints[pos]);
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
            return arrays[pos];
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP) {
            return maps[pos];
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT
                || i.dataType().getTypeCategory() == DataTypes.TypeCategory.TRAIT) {
            return structs[pos];
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
            if (ids[pos] != null) {
                return ids[pos];
            } else {
                return referenceables[pos];
            }
        } else {
            throw new AtlasException(String.format("Unknown datatype %s", i.dataType()));
        }
    }

    public void setNull(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }
        int nullPos = fieldMapping.fieldNullPos.get(attrName);
        nullFlags[nullPos] = true;
        explicitSets[nullPos] = true;

        int pos = fieldMapping.fieldPos.get(attrName);

        if (i.dataType() == DataTypes.BIGINTEGER_TYPE) {
            bigIntegers[pos] = null;
        } else if (i.dataType() == DataTypes.BIGDECIMAL_TYPE) {
            bigDecimals[pos] = null;
        } else if (i.dataType() == DataTypes.DATE_TYPE) {
            dates[pos] = null;
        } else if (i.dataType() == DataTypes.INT_TYPE) {
            ints[pos] = 0;
        } else if (i.dataType() == DataTypes.BOOLEAN_TYPE) {
            bools[pos] = false;
        } else if (i.dataType() == DataTypes.BYTE_TYPE) {
            bytes[pos] = 0;
        } else if (i.dataType() == DataTypes.SHORT_TYPE) {
            shorts[pos] = 0;
        } else if (i.dataType() == DataTypes.LONG_TYPE) {
            longs[pos] = 0;
        } else if (i.dataType() == DataTypes.FLOAT_TYPE) {
            floats[pos] = 0;
        } else if (i.dataType() == DataTypes.DOUBLE_TYPE) {
            doubles[pos] = 0;
        } else if (i.dataType() == DataTypes.STRING_TYPE) {
            strings[pos] = null;
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.ENUM) {
            ints[pos] = 0;
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
            arrays[pos] = null;
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.MAP) {
            maps[pos] = null;
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT
            || i.dataType().getTypeCategory() == DataTypes.TypeCategory.TRAIT) {
            structs[pos] = null;
        } else if (i.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
            ids[pos] = null;
            referenceables[pos] = null;
        } else {
            throw new AtlasException(String.format("Unknown datatype %s", i.dataType()));
        }
    }

    /*
     * Use only for json serialization
     * @nonpublic
     */
    @Override
    public Map<String, Object> getValuesMap() throws AtlasException {
        Map<String, Object> m = new HashMap<>();
        for (String attr : fieldMapping.fields.keySet()) {
//            int pos = fieldMapping.fieldNullPos.get(attr);
//            if (!nullFlags[pos]) {
                m.put(attr, get(attr));
//            }
        }
        return m;
    }

    public boolean getBoolean(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.BOOLEAN_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.BOOLEAN_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.BOOLEAN_TYPE.nullValue();
        }

        return bools[pos];
    }

    public byte getByte(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.BYTE_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.BYTE_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.BYTE_TYPE.nullValue();
        }

        return bytes[pos];
    }

    public short getShort(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.SHORT_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.SHORT_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.SHORT_TYPE.nullValue();
        }

        return shorts[pos];
    }

    public int getInt(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }


        if (i.dataType() != DataTypes.INT_TYPE && !(i.dataType() instanceof EnumType)) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.INT_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.INT_TYPE.nullValue();
        }

        return ints[pos];
    }

    public long getLong(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.LONG_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.LONG_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.LONG_TYPE.nullValue();
        }

        return longs[pos];
    }

    public float getFloat(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.FLOAT_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.FLOAT_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.FLOAT_TYPE.nullValue();
        }

        return floats[pos];
    }

    public double getDouble(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.DOUBLE_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.DOUBLE_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.DOUBLE_TYPE.nullValue();
        }

        return doubles[pos];
    }

    public BigInteger getBigInt(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.BIGINTEGER_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.BIGINTEGER_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.BIGINTEGER_TYPE.nullValue();
        }

        return bigIntegers[pos];
    }

    public BigDecimal getBigDecimal(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.BIGDECIMAL_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.BIGDECIMAL_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.BIGDECIMAL_TYPE.nullValue();
        }

        return bigDecimals[pos];
    }

    public Date getDate(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.DATE_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.DATE_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.DATE_TYPE.nullValue();
        }

        return dates[pos];
    }

    public String getString(String attrName) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.STRING_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic get method", attrName,
                            getTypeName(), DataTypes.STRING_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        if (nullFlags[nullPos]) {
            return DataTypes.STRING_TYPE.nullValue();
        }

        return strings[pos];
    }

    public void setBoolean(String attrName, boolean val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.BOOLEAN_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.BOOLEAN_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = false;
        bools[pos] = val;
        explicitSets[nullPos] = true;
    }

    public void setByte(String attrName, byte val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.BYTE_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.BYTE_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = false;
        bytes[pos] = val;
        explicitSets[nullPos] = true;
    }

    public void setShort(String attrName, short val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.SHORT_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.SHORT_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = false;
        shorts[pos] = val;
        explicitSets[nullPos] = true;
    }

    public void setInt(String attrName, int val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.INT_TYPE && !(i.dataType() instanceof EnumType)) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.INT_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = false;
        ints[pos] = val;
        explicitSets[nullPos] = true;
    }

    public void setLong(String attrName, long val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.LONG_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.LONG_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = false;
        longs[pos] = val;
        explicitSets[nullPos] = true;
    }

    public void setFloat(String attrName, float val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.FLOAT_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.FLOAT_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = false;
        floats[pos] = val;
        explicitSets[nullPos] = true;
    }

    public void setDouble(String attrName, double val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.DOUBLE_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.DOUBLE_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = false;
        doubles[pos] = val;
        explicitSets[nullPos] = true;
    }

    public void setBigInt(String attrName, BigInteger val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.BIGINTEGER_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.BIGINTEGER_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = val == null;
        bigIntegers[pos] = val;
        explicitSets[nullPos] = true;
    }

    public void setBigDecimal(String attrName, BigDecimal val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.BIGDECIMAL_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.BIGDECIMAL_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = val == null;
        bigDecimals[pos] = val;
        explicitSets[nullPos] = true;
    }

    public void setDate(String attrName, Date val) throws AtlasException {
        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.DATE_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.DATE_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = val == null;
        dates[pos] = val;
        explicitSets[nullPos] = true;
    }

    public void setString(String attrName, String val) throws AtlasException {

        AttributeInfo i = fieldMapping.fields.get(attrName);
        if (i == null) {
            throw new AtlasException(String.format("Unknown field %s for Struct %s", attrName, getTypeName()));
        }

        if (i.dataType() != DataTypes.STRING_TYPE) {
            throw new AtlasException(
                    String.format("Field %s for Struct %s is not a %s, call generic set method", attrName,
                            getTypeName(), DataTypes.STRING_TYPE.getName()));
        }

        int pos = fieldMapping.fieldPos.get(attrName);
        int nullPos = fieldMapping.fieldNullPos.get(attrName);

        nullFlags[nullPos] = val == null;
        strings[pos] = val;
        explicitSets[nullPos] = true;
    }

    @Override
    public String toString() {
        try {
            StringBuilder buf = new StringBuilder();
            String prefix = "";

            fieldMapping.output(this, buf, prefix, null);
            return buf.toString();

        } catch (AtlasException me) {
            throw new RuntimeException(me);
        }
    }

    @Override
    public String getSignatureHash(MessageDigest digester) throws AtlasException {
        StructType structType = TypeSystem.getInstance().getDataType(StructType.class, getTypeName());
        structType.updateSignatureHash(digester, this);
        byte[] digest = digester.digest();
        return SHA256Utils.toString(digest);
    }

    @Override
    public boolean isValueSet(final String attrName) throws AtlasException {
        int nullPos = fieldMapping.fieldNullPos.get(attrName);
        return explicitSets[nullPos];
    }

    @Override
    public String toShortString() {
        return String.format("struct[type=%s]", dataTypeName);
    }
}
