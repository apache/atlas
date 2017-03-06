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

package org.apache.atlas.repository.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.StructInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IConstructableType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Deprecated
public class AttributeStores {

    private static final Object NULL_VAL = new Object();

    static IAttributeStore createStore(AttributeInfo i) throws RepositoryException {
        switch (i.dataType().getTypeCategory()) {
        case PRIMITIVE:
            if (i.dataType() == DataTypes.BOOLEAN_TYPE) {
                return new BooleanAttributeStore(i);
            } else if (i.dataType() == DataTypes.BYTE_TYPE) {
                return new ByteAttributeStore(i);
            } else if (i.dataType() == DataTypes.SHORT_TYPE) {
                return new ShortAttributeStore(i);
            } else if (i.dataType() == DataTypes.INT_TYPE) {
                return new IntAttributeStore(i);
            } else if (i.dataType() == DataTypes.LONG_TYPE) {
                return new LongAttributeStore(i);
            } else if (i.dataType() == DataTypes.FLOAT_TYPE) {
                return new FloatAttributeStore(i);
            } else if (i.dataType() == DataTypes.DOUBLE_TYPE) {
                return new DoubleAttributeStore(i);
            } else if (i.dataType() == DataTypes.BIGINTEGER_TYPE) {
                return new BigIntStore(i);
            } else if (i.dataType() == DataTypes.BIGDECIMAL_TYPE) {
                return new BigDecimalStore(i);
            } else if (i.dataType() == DataTypes.DATE_TYPE) {
                return new DateStore(i);
            } else if (i.dataType() == DataTypes.STRING_TYPE) {
                return new StringStore(i);
            } else if (i.dataType() == DataTypes.STRING_TYPE) {
                return new StringStore(i);
            } else {
                throw new RepositoryException(String.format("Unknown datatype %s", i.dataType()));
            }
        case ENUM:
            return new IntAttributeStore(i);
        case ARRAY:
            return new ImmutableListStore(i);
        case MAP:
            return new ImmutableMapStore(i);
        case STRUCT:
            return new StructStore(i);
        case CLASS:
            return new IdStore(i);
        default:
            throw new RepositoryException(String.format("Unknown Category for datatype %s", i.dataType()));
        }
    }

    static abstract class AbstractAttributeStore implements IAttributeStore {
        final BooleanArrayList nullList;
        final Map<Integer, Map<String, Object>> hiddenVals;
        AttributeInfo attrInfo;

        AbstractAttributeStore(AttributeInfo attrInfo) {
            this.attrInfo = attrInfo;
            this.nullList = new BooleanArrayList();
            hiddenVals = new HashMap<>();
        }

        final void setNull(int pos, boolean flag) {
            nullList.set(pos, flag);
        }

        final boolean getNull(int pos) {
            return nullList.get(pos);
        }

        void storeHiddenVals(int pos, IConstructableType type, StructInstance instance) throws RepositoryException {
            List<String> attrNames = type.getNames(attrInfo);
            Map<String, Object> m = hiddenVals.get(pos);
            if (m == null) {
                m = new HashMap<>();
                hiddenVals.put(pos, m);
            }
            for (int i = 2; i < attrNames.size(); i++) {
                String attrName = attrNames.get(i);
                int nullPos = instance.fieldMapping().fieldNullPos.get(attrName);
                int colPos = instance.fieldMapping().fieldPos.get(attrName);
                if (instance.nullFlags[nullPos]) {
                    m.put(attrName, NULL_VAL);
                } else {
                    //m.put(attrName, instance.bools[colPos]);
                    store(instance, colPos, attrName, m);
                }
            }
        }

        void loadHiddenVals(int pos, IConstructableType type, StructInstance instance) throws RepositoryException {
            List<String> attrNames = type.getNames(attrInfo);
            Map<String, Object> m = hiddenVals.get(pos);
            for (int i = 2; i < attrNames.size(); i++) {
                String attrName = attrNames.get(i);
                int nullPos = instance.fieldMapping().fieldNullPos.get(attrName);
                int colPos = instance.fieldMapping().fieldPos.get(attrName);
                Object val = m == null ? NULL_VAL : m.get(attrName);
                if (val == NULL_VAL) {
                    instance.nullFlags[nullPos] = true;
                } else {
                    instance.nullFlags[nullPos] = false;
                    load(instance, colPos, val);
                }
            }
        }

        @Override
        public void store(int pos, IConstructableType type, StructInstance instance) throws RepositoryException {
            List<String> attrNames = type.getNames(attrInfo);
            String attrName = attrNames.get(0);
            int nullPos = instance.fieldMapping().fieldNullPos.get(attrName);
            int colPos = instance.fieldMapping().fieldPos.get(attrName);
            nullList.set(pos, instance.nullFlags[nullPos]);

            if (pos == nullList.size()) {
                nullList.add(instance.nullFlags[nullPos]);
            } else {
                nullList.set(pos, instance.nullFlags[nullPos]);
            }
            //list.set(pos, instance.bools[colPos]);
            store(instance, colPos, pos);

            if (attrNames.size() > 1) {
                storeHiddenVals(pos, type, instance);
            }
        }

        @Override
        public void load(int pos, IConstructableType type, StructInstance instance) throws RepositoryException {
            List<String> attrNames = type.getNames(attrInfo);
            String attrName = attrNames.get(0);
            int nullPos = instance.fieldMapping().fieldNullPos.get(attrName);
            int colPos = instance.fieldMapping().fieldPos.get(attrName);

            if (nullList.get(pos)) {
                instance.nullFlags[nullPos] = true;
            } else {
                instance.nullFlags[nullPos] = false;
                load(instance, colPos, pos);
            }

            if (attrNames.size() > 1) {
                loadHiddenVals(pos, type, instance);
            }
        }

        /*
         * store the value from colPos in instance into the list.
         */
        protected abstract void store(StructInstance instance, int colPos, int pos) throws RepositoryException;

        /*
         * load the value from pos in list into colPos in instance.
         */
        protected abstract void load(StructInstance instance, int colPos, int pos) throws RepositoryException;

        /*
         * store the value from colPos in map as attrName
         */
        protected abstract void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m);

        /*
         * load the val into colPos in instance.
         */
        protected abstract void load(StructInstance instance, int colPos, Object val);

    }

    static abstract class PrimitiveAttributeStore extends AbstractAttributeStore implements IAttributeStore {


        public PrimitiveAttributeStore(AttributeInfo attrInfo) {
            super(attrInfo);
        }

    }

    static class BooleanAttributeStore extends PrimitiveAttributeStore {

        final BooleanArrayList list;

        BooleanAttributeStore(AttributeInfo attrInfo) {
            super(attrInfo);
            this.list = new BooleanArrayList();
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.bools[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.bools[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.bools[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.bools[colPos] = (Boolean) val;
        }

        @Override
        public void ensureCapacity(int pos) throws RepositoryException {
            list.size(pos + 1);
            nullList.size(pos + 1);
        }
    }

    static class ByteAttributeStore extends PrimitiveAttributeStore {

        final ByteArrayList list;

        ByteAttributeStore(AttributeInfo attrInfo) {
            super(attrInfo);
            this.list = new ByteArrayList();
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.bytes[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.bytes[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.bytes[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.bytes[colPos] = (Byte) val;
        }

        @Override
        public void ensureCapacity(int pos) throws RepositoryException {
            list.size(pos + 1);
            nullList.size(pos + 1);
        }
    }

    static class ShortAttributeStore extends PrimitiveAttributeStore {

        final ShortArrayList list;

        ShortAttributeStore(AttributeInfo attrInfo) {
            super(attrInfo);
            this.list = new ShortArrayList();
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.shorts[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.shorts[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.shorts[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.shorts[colPos] = (Short) val;
        }

        @Override
        public void ensureCapacity(int pos) throws RepositoryException {
            list.size(pos + 1);
            nullList.size(pos + 1);
        }
    }

    static class IntAttributeStore extends PrimitiveAttributeStore {

        final IntArrayList list;

        IntAttributeStore(AttributeInfo attrInfo) {
            super(attrInfo);
            this.list = new IntArrayList();
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.ints[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.ints[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.ints[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.ints[colPos] = (Integer) val;
        }

        @Override
        public void ensureCapacity(int pos) throws RepositoryException {
            list.size(pos + 1);
            nullList.size(pos + 1);
        }
    }

    static class LongAttributeStore extends PrimitiveAttributeStore {

        final LongArrayList list;

        LongAttributeStore(AttributeInfo attrInfo) {
            super(attrInfo);
            this.list = new LongArrayList();
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.longs[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.longs[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.longs[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.longs[colPos] = (Long) val;
        }

        @Override
        public void ensureCapacity(int pos) throws RepositoryException {
            list.size(pos + 1);
            nullList.size(pos + 1);
        }
    }

    static class FloatAttributeStore extends PrimitiveAttributeStore {

        final FloatArrayList list;

        FloatAttributeStore(AttributeInfo attrInfo) {
            super(attrInfo);
            this.list = new FloatArrayList();
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.floats[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.floats[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.floats[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.floats[colPos] = (Float) val;
        }

        @Override
        public void ensureCapacity(int pos) throws RepositoryException {
            list.size(pos + 1);
            nullList.size(pos + 1);
        }
    }

    static class DoubleAttributeStore extends PrimitiveAttributeStore {

        final DoubleArrayList list;

        DoubleAttributeStore(AttributeInfo attrInfo) {
            super(attrInfo);
            this.list = new DoubleArrayList();
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.doubles[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.doubles[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.doubles[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.doubles[colPos] = (Double) val;
        }

        @Override
        public void ensureCapacity(int pos) throws RepositoryException {
            list.size(pos + 1);
            nullList.size(pos + 1);
        }
    }

    static abstract class ObjectAttributeStore<T> extends AbstractAttributeStore {

        final ArrayList<T> list;

        ObjectAttributeStore(Class<T> cls, AttributeInfo attrInfo) {
            super(attrInfo);
            this.list = Lists.newArrayList((T) null);
        }

        @Override
        public void ensureCapacity(int pos) throws RepositoryException {
            while (list.size() < pos + 1) {
                list.add(null);
            }
            nullList.size(pos + 1);
        }
    }

    static class BigIntStore extends ObjectAttributeStore<BigInteger> {

        public BigIntStore(AttributeInfo attrInfo) {
            super(BigInteger.class, attrInfo);
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.bigIntegers[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.bigIntegers[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.bigIntegers[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.bigIntegers[colPos] = (BigInteger) val;
        }

    }

    static class BigDecimalStore extends ObjectAttributeStore<BigDecimal> {

        public BigDecimalStore(AttributeInfo attrInfo) {
            super(BigDecimal.class, attrInfo);
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.bigDecimals[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.bigDecimals[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.bigDecimals[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.bigDecimals[colPos] = (BigDecimal) val;
        }

    }

    static class DateStore extends ObjectAttributeStore<Date> {

        public DateStore(AttributeInfo attrInfo) {
            super(Date.class, attrInfo);
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.dates[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.dates[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.dates[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.dates[colPos] = (Date) val;
        }

    }

    static class StringStore extends ObjectAttributeStore<String> {

        public StringStore(AttributeInfo attrInfo) {
            super(String.class, attrInfo);
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.strings[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.strings[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.strings[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.strings[colPos] = (String) val;
        }

    }

    static class IdStore extends ObjectAttributeStore<Id> {

        public IdStore(AttributeInfo attrInfo) {
            super(Id.class, attrInfo);
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.ids[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.ids[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.ids[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.ids[colPos] = (Id) val;
        }

    }

    static class ImmutableListStore extends ObjectAttributeStore<ImmutableList> {

        public ImmutableListStore(AttributeInfo attrInfo) {
            super(ImmutableList.class, attrInfo);
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.arrays[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.arrays[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.arrays[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.arrays[colPos] = (ImmutableList) val;
        }

    }

    static class ImmutableMapStore extends ObjectAttributeStore<ImmutableMap> {

        public ImmutableMapStore(AttributeInfo attrInfo) {
            super(ImmutableMap.class, attrInfo);
        }

        protected void store(StructInstance instance, int colPos, int pos) {
            list.set(pos, instance.maps[colPos]);
        }

        protected void load(StructInstance instance, int colPos, int pos) {
            instance.maps[colPos] = list.get(pos);
        }

        protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
            m.put(attrName, instance.maps[colPos]);
        }

        protected void load(StructInstance instance, int colPos, Object val) {
            instance.maps[colPos] = (ImmutableMap) val;
        }

    }
}
