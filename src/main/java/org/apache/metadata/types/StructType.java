package org.apache.metadata.types;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.metadata.IStruct;
import org.apache.metadata.storage.IRepository;
import org.apache.metadata.MetadataException;
import org.apache.metadata.MetadataService;
import org.apache.metadata.Struct;
import org.apache.metadata.storage.TypedStruct;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

public class StructType  extends AbstractDataType<IStruct> {

    public final String name;
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

    /**
     * Used when creating a StructType, to support recursive Structs.
     */
    StructType(String name) {
        this.name = name;
        this.fields = new LinkedHashMap<String, AttributeInfo>();
        this.fieldPos = new HashMap<String, Integer>();
        fieldNullPos = new HashMap<String, Integer>();
        numBools = 0;
        numBytes = 0;
        numShorts = 0;
        numInts = 0;
        numLongs = 0;
        numFloats = 0;
        numDoubles = 0;
        numBigInts = 0;
        numBigDecimals = 0;
        numDates = 0;
        numStrings = 0;
        numArrays = 0;
        numMaps = 0;
        numStructs = 0;
    }

    StructType(String name, AttributeInfo... fields) throws MetadataException {
        this.name = name;
        this.fields = new LinkedHashMap<String, AttributeInfo>();
        this.fieldPos = new HashMap<String, Integer>();
        fieldNullPos = new HashMap<String, Integer>();
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
            if ( this.fields.containsKey(i.name) ) {
                throw new MetadataException(
                        String.format("Struct defintion cannot contain multiple fields with the same name %s", i.name));
            }
            this.fields.put(i.name, i);
            this.fieldNullPos.put(i.name, fieldNullPos.size());
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
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TypedStruct convert(Object val, Multiplicity m) throws MetadataException {
        if ( val != null ) {
            if ( val instanceof Struct ) {
                Struct s = (Struct) val;
                if ( s.typeName != name ) {
                    throw new ValueConversionException(this, val);
                }
                TypedStruct ts = createInstance();
                for(AttributeInfo i : fields.values()) {
                    Object aVal = s.get(i.name);
                    try {
                        set(ts, i.name, aVal);
                    } catch(ValueConversionException ve) {
                        throw new ValueConversionException(this, val, ve);
                    }
                }
                return ts;
            } else if ( val instanceof TypedStruct && ((TypedStruct)val).dataType == this ) {
                return (TypedStruct) val;
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

    public TypedStruct createInstance() {
        return new TypedStruct(this,
                new boolean[fields.size()],
        numBools == 0 ? null : new boolean[numBools],
                numBytes == 0 ? null : new byte[numBytes],
        numShorts == 0 ? null : new short[numShorts],
        numInts == 0 ? null : new int[numInts],
        numLongs == 0 ? null : new long[numLongs],
        numFloats == 0 ? null : new float[numFloats],
        numDoubles == 0 ? null : new double[numDoubles],
        numBigDecimals == 0 ? null : new BigDecimal[numBigDecimals],
        numBigInts == 0 ? null : new BigInteger[numBigInts],
        numDates == 0 ? null : new Date[numDates],
        numStrings == 0 ? null : new String[numStrings],
        numArrays == 0 ? null : new ImmutableList[numArrays],
        numMaps == 0 ? null : new ImmutableMap[numMaps],
        numStructs == 0 ? null : new TypedStruct[numStructs]);
    }

    public void set(TypedStruct s, String attrName, Object val) throws MetadataException {
        AttributeInfo i = fields.get(attrName);
        if ( i == null ) {
            throw new ValueConversionException(this, val, "Unknown field " + attrName);
        }
        int pos = fieldPos.get(attrName);
        int nullPos = fieldNullPos.get(attrName);
        Object cVal = i.dataType().convert(val, i.multiplicity);
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
        } else if ( i.dataType().getTypeCategory() == DataTypes.TypeCategory.STRUCT ) {
            s.structs[pos] = (TypedStruct) cVal;
        } else {
            throw new MetadataException(String.format("Unknown datatype %s", i.dataType()));
        }
    }

    public Object get(TypedStruct s, String attrName) throws MetadataException {
        AttributeInfo i = fields.get(attrName);
        if ( i == null ) {
            throw new MetadataException(String.format("Unknown field %s for Struct %s", attrName, this.getName()));
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

    @Override
    public void output(IStruct s, Appendable buf, String prefix) throws MetadataException {
        outputVal("{", buf, prefix);
        if ( s == null ) {
            outputVal("<null>\n", buf, "");
            return;
        }
        outputVal("\n", buf, "");
        String fieldPrefix = prefix + "\t";
        for(AttributeInfo i : fields.values()) {
            Object aVal = s.get(i.name);
            outputVal(i.name + " : ", buf, fieldPrefix);
            i.dataType().output(aVal, buf, "");
            outputVal("\n", buf, "");
        }
        outputVal("\n}\n", buf, "");
    }
}
