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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataTypes {

    public static BooleanType BOOLEAN_TYPE = new BooleanType();
    public static ByteType BYTE_TYPE = new ByteType();
    public static ShortType SHORT_TYPE = new ShortType();
    public static IntType INT_TYPE = new IntType();
    public static LongType LONG_TYPE = new LongType();
    public static FloatType FLOAT_TYPE = new FloatType();
    public static DoubleType DOUBLE_TYPE = new DoubleType();
    public static BigIntegerType BIGINTEGER_TYPE = new BigIntegerType();
    public static BigDecimalType BIGDECIMAL_TYPE = new BigDecimalType();
    public static DateType DATE_TYPE = new DateType();
    public static StringType STRING_TYPE = new StringType();
    public static String ARRAY_TYPE_PREFIX = "array<";
    static String ARRAY_TYPE_SUFFIX = ">";
    public static String MAP_TYPE_PREFIX = "map<";
    static String MAP_TYPE_SUFFIX = ">";

    public static String arrayTypeName(String elemTypeName) {
        assert elemTypeName != null;
        return String.format("%s%s%s", ARRAY_TYPE_PREFIX, elemTypeName, ARRAY_TYPE_SUFFIX);
    }

    public static String arrayTypeName(IDataType elemType) {
        return arrayTypeName(elemType.getName());
    }

    public static String mapTypeName(String keyTypeName, String valueTypeName) {
        return String.format("%s%s,%s%s", MAP_TYPE_PREFIX, keyTypeName, valueTypeName, MAP_TYPE_SUFFIX);
    }

    public static String mapTypeName(IDataType keyType, IDataType valueType) {
        assert keyType != null;
        assert valueType != null;
        return mapTypeName(keyType.getName(), valueType.getName());
    }

    public static enum TypeCategory {
        PRIMITIVE,
        ENUM,
        ARRAY,
        MAP,
        STRUCT,
        TRAIT,
        CLASS
    }

    public static abstract class PrimitiveType<T> extends AbstractDataType<T> {
        public PrimitiveType(String name, String description) {
            super(name, description);
        }

        @Override
        public TypeCategory getTypeCategory() {
            return TypeCategory.PRIMITIVE;
        }

        public abstract T nullValue();

        @Override
        protected T convertNull(Multiplicity m) throws AtlasException {
            if (!m.nullAllowed()) {
                throw new ValueConversionException.NullConversionException(m);
            }

            return nullValue();
        }

        @Override
        public void updateSignatureHash(MessageDigest digester, Object val) throws AtlasException {
            if ( val != null ) {
                digester.update(val.toString().getBytes(Charset.forName("UTF-8")));
            }
        }

    }

    public static class BooleanType extends PrimitiveType<Boolean> {

        private static final String name = "boolean".intern();

        private BooleanType() {
            super(name, null);
        }

        @Override
        public Boolean convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                if (val instanceof Boolean) {
                    return (Boolean) val;
                } else if (val instanceof String) {
                    return Boolean.parseBoolean((String) val);
                } else if (val instanceof Number) {
                    return ((Number) val).intValue() != 0;
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            return convertNull(m);
        }

        public Boolean nullValue() {
            return Boolean.FALSE;
        }
    }

    public static class ByteType extends PrimitiveType<Byte> {

        private static final String name = "byte".intern();

        private ByteType() {
            super(name, null);
        }

        @Override
        public Byte convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                if (val instanceof Byte) {
                    return (Byte) val;
                } else if (val instanceof String) {
                    return Byte.parseByte((String) val);
                } else if (val instanceof Number) {
                    return ((Number) val).byteValue();
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            return convertNull(m);
        }

        public Byte nullValue() {
            return 0;
        }

        @Override
        public void updateSignatureHash(MessageDigest digester, Object val) throws AtlasException {
            if ( val != null ) {
                digester.update(((Byte) val).byteValue());
            }
        }
    }

    public static class ShortType extends PrimitiveType<Short> {

        private static final String name = "short".intern();

        private ShortType() {
            super(name, null);
        }

        @Override
        public Short convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                if (val instanceof Short) {
                    return (Short) val;
                } else if (val instanceof String) {
                    return Short.parseShort((String) val);
                } else if (val instanceof Number) {
                    return ((Number) val).shortValue();
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            return convertNull(m);
        }

        public Short nullValue() {
            return 0;
        }
    }

    public static class IntType extends PrimitiveType<Integer> {

        private static final String name = "int".intern();

        private IntType() {
            super(name, null);
        }

        @Override
        public Integer convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                if (val instanceof Integer) {
                    return (Integer) val;
                } else if (val instanceof String) {
                    return Integer.parseInt((String) val);
                } else if (val instanceof Number) {
                    return ((Number) val).intValue();
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            return convertNull(m);
        }

        public Integer nullValue() {
            return 0;
        }
    }

    public static class LongType extends PrimitiveType<Long> {

        private static final String name = "long".intern();

        private LongType() {
            super(name, null);
        }

        @Override
        public Long convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                if (val instanceof Long) {
                    return (Long) val;
                } else if (val instanceof String) {
                    return Long.parseLong((String) val);
                } else if (val instanceof Number) {
                    return ((Number) val).longValue();
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            return convertNull(m);
        }

        public Long nullValue() {
            return 0l;
        }
    }

    public static class FloatType extends PrimitiveType<Float> {

        private static final String name = "float".intern();

        private FloatType() {
            super(name, null);
        }

        @Override
        public Float convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                if (val instanceof Float) {
                    return (Float) val;
                } else if (val instanceof String) {
                    return Float.parseFloat((String) val);
                } else if (val instanceof Number) {
                    return ((Number) val).floatValue();
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            return convertNull(m);
        }

        public Float nullValue() {
            return 0.0f;
        }
    }

    public static class DoubleType extends PrimitiveType<Double> {

        private static final String name = "double".intern();

        private DoubleType() {
            super(name, null);
        }

        @Override
        public Double convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                if (val instanceof Double) {
                    return (Double) val;
                } else if (val instanceof String) {
                    return Double.parseDouble((String) val);
                } else if (val instanceof Number) {
                    return ((Number) val).doubleValue();
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            return convertNull(m);
        }

        public Double nullValue() {
            return 0.0;
        }
    }

    public static class BigIntegerType extends PrimitiveType<BigInteger> {

        private static final String name = "biginteger".intern();

        private BigIntegerType() {
            super(name, null);
        }

        @Override
        public BigInteger convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                if (val instanceof BigInteger) {
                    return (BigInteger) val;
                } else if (val instanceof String) {
                    try {
                        return new BigInteger((String) val);
                    } catch (NumberFormatException ne) {
                        throw new ValueConversionException(this, val, ne);
                    }
                } else if (val instanceof Number) {
                    return BigInteger.valueOf(((Number) val).longValue());
                } else if (val instanceof BigDecimal) {
                    return ((BigDecimal) val).toBigInteger();
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            return convertNull(m);
        }

        public BigInteger nullValue() {
            return null;
        }
    }

    public static class BigDecimalType extends PrimitiveType<BigDecimal> {

        private static final String name = "bigdecimal".intern();

        private BigDecimalType() {
            super(name, null);
        }

        @Override
        public BigDecimal convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                if (val instanceof BigDecimal) {
                    return (BigDecimal) val;
                } else if (val instanceof String) {
                    try {
                        return new BigDecimal((String) val);
                    } catch (NumberFormatException ne) {
                        throw new ValueConversionException(this, val, ne);
                    }
                } else if (val instanceof Number) {
                    return new BigDecimal(((Number) val).doubleValue());
                } else if (val instanceof BigInteger) {
                    return new BigDecimal((BigInteger) val);
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            return convertNull(m);
        }

        public BigDecimal nullValue() {
            return null;
        }
    }

    public static class DateType extends PrimitiveType<Date> {

        private static final String name = "date".intern();

        private DateType() {
            super(name, null);
        }

        @Override
        public Date convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                if (val instanceof Date) {
                    return (Date) val;
                } else if (val instanceof String) {
                    try {
                        return TypeSystem.getInstance().getDateFormat().parse((String) val);
                    } catch (ParseException ne) {
                        throw new ValueConversionException(this, val, ne);
                    }
                } else if (val instanceof Number) {
                    return new Date(((Number) val).longValue());
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            return convertNull(m);
        }

        @Override
        public void output(Date val, Appendable buf, String prefix, Set<Date> inProcess) throws AtlasException {
            TypeUtils.outputVal(val == null ? "<null>" : TypeSystem.getInstance().getDateFormat().format(val), buf,
                    prefix);
        }

        public Date nullValue() {
            return null;
        }
    }

    public static class StringType extends PrimitiveType<String> {

        private static final String name = "string".intern();

        private StringType() {
            super(name, null);
        }

        @Override
        public String convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null && (!(val instanceof String) || StringUtils.isNotEmpty((CharSequence) val))) {
                return val.toString();
            }

            if (m.nullAllowed() && val != null){
                return val.toString();
            }
            return convertNull(m);
        }

        public String nullValue() {
            return null;
        }
    }

    public static class ArrayType extends AbstractDataType<ImmutableCollection<?>> {
        private IDataType elemType;

        public ArrayType(IDataType elemType) {
            super(arrayTypeName(elemType), null);
            this.elemType = elemType;
        }

        public IDataType getElemType() {
            return elemType;
        }

        protected void setElemType(IDataType elemType) {
            this.elemType = elemType;
        }

        @Override
        public ImmutableCollection<?> convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                Iterator it = null;
                if (val instanceof Collection) {
                    it = ((Collection) val).iterator();
                } else if (val instanceof Iterable) {
                    it = ((Iterable) val).iterator();
                } else if (val instanceof Iterator) {
                    it = (Iterator) val;
                }

                if (it != null) {
                    ImmutableCollection.Builder b = m.isUnique ? ImmutableSet.builder() : ImmutableList.builder();
                    while (it.hasNext()) {
                        b.add(elemType.convert(it.next(),
                                TypeSystem.getInstance().allowNullsInCollections() ? Multiplicity.OPTIONAL :
                                        Multiplicity.REQUIRED));
                    }
                    return m.isUnique ? b.build().asList() : b.build();
                } else {
                    try {
                        return ImmutableList.of(elemType.convert(val,
                                TypeSystem.getInstance().allowNullsInCollections() ? Multiplicity.OPTIONAL :
                                        Multiplicity.REQUIRED));
                    } catch (Exception e) {
                        throw new ValueConversionException(this, val, e);
                    }
                }
            }
            if (!m.nullAllowed()) {
                throw new ValueConversionException.NullConversionException(m);
            }
            return null;
        }

        public ImmutableCollection<?> mapIds(ImmutableCollection<?> val, Multiplicity m, Map<Id, Id> transientToNewIds)
        throws AtlasException {

            if (val == null || elemType.getTypeCategory() != TypeCategory.CLASS) {
                return val;
            }
            ImmutableCollection.Builder b = m.isUnique ? ImmutableSet.builder() : ImmutableList.builder();
            Iterator it = val.iterator();
            while (it.hasNext()) {
                Object elem = it.next();
                if (elem instanceof IReferenceableInstance) {
                    Id oldId = ((IReferenceableInstance) elem).getId();
                    Id newId = transientToNewIds.get(oldId);
                    b.add(newId == null ? oldId : newId);
                } else {
                    b.add(elem);
                }
            }
            return b.build();
        }

        @Override
        public TypeCategory getTypeCategory() {
            return TypeCategory.ARRAY;
        }

        @Override
        public void updateSignatureHash(MessageDigest digester, Object val) throws AtlasException {
            IDataType elemType = getElemType();
            List vals = (List) val;
            for (Object listElem : vals) {
                elemType.updateSignatureHash(digester, listElem);
            }
        }
    }

    public static class MapType extends AbstractDataType<ImmutableMap<?, ?>> {

        private IDataType keyType;
        private IDataType valueType;

        public MapType(IDataType keyType, IDataType valueType) {
            super(mapTypeName(keyType, valueType), null);
            this.keyType = keyType;
            this.valueType = valueType;
        }

        public IDataType getKeyType() {
            return keyType;
        }

        protected void setKeyType(IDataType keyType) {
            this.keyType = keyType;
        }

        public IDataType getValueType() {
            return valueType;
        }

        protected void setValueType(IDataType valueType) {
            this.valueType = valueType;
        }

        @Override
        public ImmutableMap<?, ?> convert(Object val, Multiplicity m) throws AtlasException {
            if (val != null) {
                Iterator<Map.Entry> it = null;
                if (Map.class.isAssignableFrom(val.getClass())) {
                    it = ((Map) val).entrySet().iterator();
                    ImmutableMap.Builder b = ImmutableMap.builder();
                    while (it.hasNext()) {
                        Map.Entry e = it.next();
                        b.put(keyType.convert(e.getKey(),
                                        TypeSystem.getInstance().allowNullsInCollections() ? Multiplicity.OPTIONAL :
                                                Multiplicity.REQUIRED),
                                        valueType.convert(e.getValue(), Multiplicity.OPTIONAL));
                    }
                    return b.build();
                } else {
                    throw new ValueConversionException(this, val);
                }
            }
            if (!m.nullAllowed()) {
                throw new ValueConversionException.NullConversionException(m);
            }
            return null;
        }

        public ImmutableMap<?, ?> mapIds(ImmutableMap val, Multiplicity m, Map<Id, Id> transientToNewIds)
        throws AtlasException {

            if (val == null || (keyType.getTypeCategory() != TypeCategory.CLASS
                    && valueType.getTypeCategory() != TypeCategory.CLASS)) {
                return val;
            }
            ImmutableMap.Builder b = ImmutableMap.builder();
            Iterator<Map.Entry> it = val.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry elem = it.next();
                Object oldKey = elem.getKey();
                Object oldValue = elem.getValue();
                Object newKey = oldKey;
                Object newValue = oldValue;

                if (oldKey instanceof IReferenceableInstance) {
                    Id oldId = ((IReferenceableInstance) oldKey).getId();
                    Id newId = transientToNewIds.get(oldId);
                    newKey = newId == null ? oldId : newId;
                }

                if (oldValue instanceof IReferenceableInstance) {
                    Id oldId = ((IReferenceableInstance) oldValue).getId();
                    Id newId = transientToNewIds.get(oldId);
                    newValue = newId == null ? oldId : newId;
                }

                b.put(newKey, newValue);
            }
            return b.build();
        }

        @Override
        public TypeCategory getTypeCategory() {
            return TypeCategory.MAP;
        }

        @Override
        public void updateSignatureHash(MessageDigest digester, Object val) throws AtlasException {
            IDataType keyType = getKeyType();
            IDataType valueType = getValueType();
            Map vals = (Map) val;
            for (Object key : vals.keySet()) {
                keyType.updateSignatureHash(digester, key);
                valueType.updateSignatureHash(digester, vals.get(key));
            }
        }
    }

}
