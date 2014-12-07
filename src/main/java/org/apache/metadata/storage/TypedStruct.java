package org.apache.metadata.storage;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.metadata.IStruct;
import org.apache.metadata.MetadataException;
import org.apache.metadata.types.StructType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class TypedStruct implements IStruct {
    public final StructType dataType;
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
    public final TypedStruct[] structs;

    public TypedStruct(StructType dataType, boolean[] nullFlags, boolean[] bools, byte[] bytes, short[] shorts, int[] ints,
                       long[] longs, float[] floats, double[] doubles,
                       BigDecimal[] bigDecimals, BigInteger[] bigIntegers, Date[] dates, String[] strings,
                       ImmutableList<Object>[] arrays, ImmutableMap<Object, Object>[] maps, TypedStruct[] structs) {
        assert dataType != null;
        this.dataType = dataType;
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

        for(int i=0; i<nullFlags.length; i++) {
            nullFlags[i] = true;
        }
    }

    @Override
    public String getTypeName() {
        return dataType.getName();
    }

    @Override
    public Object get(String attrName) throws MetadataException {
        return dataType.get(this, attrName);
    }

    @Override
    public void set(String attrName, Object val) throws MetadataException {
        dataType.set(this, attrName, val);
    }

    @Override
    public String toString()  {
        try {
            StringBuilder b = new StringBuilder();
            dataType.output(this, b, "");
            return b.toString();
        } catch(MetadataException me) {
            throw new RuntimeException(me);
        }
    }
}
