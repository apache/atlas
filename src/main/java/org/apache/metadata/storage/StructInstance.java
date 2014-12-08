package org.apache.metadata.storage;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.metadata.IStruct;
import org.apache.metadata.MetadataException;
import org.apache.metadata.types.AttributeInfo;
import org.apache.metadata.types.FieldMapping;
import org.apache.metadata.types.TypeUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public class StructInstance implements IStruct {
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

    public StructInstance(String dataTypeName, FieldMapping fieldMapping,
                          boolean[] nullFlags, boolean[] bools, byte[] bytes, short[] shorts, int[] ints,
                          long[] longs, float[] floats, double[] doubles,
                          BigDecimal[] bigDecimals, BigInteger[] bigIntegers, Date[] dates, String[] strings,
                          ImmutableList<Object>[] arrays, ImmutableMap<Object, Object>[] maps, StructInstance[] structs) {
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

        for(int i=0; i<nullFlags.length; i++) {
            nullFlags[i] = true;
        }
    }

    @Override
    public String getTypeName() {
        return dataTypeName;
    }

    @Override
    public Object get(String attrName) throws MetadataException {
        return fieldMapping.get(this, attrName);
    }

    @Override
    public void set(String attrName, Object val) throws MetadataException {
        fieldMapping.set(this, attrName, val);
    }

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

    @Override
    public String toString()  {
        try {
            StringBuilder buf = new StringBuilder();
            String prefix = "";

            TypeUtils.outputVal("{", buf, prefix);
            TypeUtils.outputVal("\n", buf, "");
            String fieldPrefix = prefix + "\t";
            for(AttributeInfo i : fieldMapping.fields.values()) {
                Object aVal = get(i.name);
                TypeUtils.outputVal(i.name + " : ", buf, fieldPrefix);
                i.dataType().output(aVal, buf, "");
                TypeUtils.outputVal("\n", buf, "");
            }
            TypeUtils.outputVal("\n}\n", buf, "");
            return buf.toString();

        } catch(MetadataException me) {
            throw new RuntimeException(me);
        }
    }
}
