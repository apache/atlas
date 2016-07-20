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
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.persistence.Id;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class ObjectGraphTraversal implements Iterator<ObjectGraphTraversal.InstanceTuple> {

    final Queue<InstanceTuple> queue;
    final TypeSystem typeSystem;
    Set<Id> processedIds;

    public ObjectGraphTraversal(TypeSystem typeSystem, IReferenceableInstance start) throws AtlasException {
        this.typeSystem = typeSystem;
        queue = new LinkedList<InstanceTuple>();
        processedIds = new HashSet<Id>();
        processReferenceableInstance(start);
    }

    void processValue(IDataType dT, Object val) throws AtlasException {
        if (val != null) {
            if (dT.getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
                IDataType elemType = ((DataTypes.ArrayType) dT).getElemType();
                processCollection(elemType, val);
            } else if (dT.getTypeCategory() == DataTypes.TypeCategory.MAP) {
                IDataType keyType = ((DataTypes.MapType) dT).getKeyType();
                IDataType valueType = ((DataTypes.MapType) dT).getValueType();
                processMap(keyType, valueType, val);
            } else if (dT.getTypeCategory() == DataTypes.TypeCategory.STRUCT
                    || dT.getTypeCategory() == DataTypes.TypeCategory.TRAIT) {
                processStruct(val);
            } else if (dT.getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                processReferenceableInstance(val);
            }
        }
    }

    void processMap(IDataType keyType, IDataType valueType, Object val) throws AtlasException {
        if (keyType.getTypeCategory() == DataTypes.TypeCategory.PRIMITIVE
                && valueType.getTypeCategory() == DataTypes.TypeCategory.PRIMITIVE) {
            return;
        }

        if (val != null) {
            Iterator<Map.Entry> it = null;
            if (Map.class.isAssignableFrom(val.getClass())) {
                it = ((Map) val).entrySet().iterator();
                ImmutableMap.Builder b = ImmutableMap.builder();
                while (it.hasNext()) {
                    Map.Entry e = it.next();
                    processValue(keyType, e.getKey());
                    processValue(valueType, e.getValue());
                }
            }
        }
    }

    void processCollection(IDataType elemType, Object val) throws AtlasException {

        if (elemType.getTypeCategory() == DataTypes.TypeCategory.PRIMITIVE) {
            return;
        }

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
                DataTypes.TypeCategory elemCategory = elemType.getTypeCategory();
                while (it.hasNext()) {
                    Object elem = it.next();
                    processValue(elemType, elem);
                }
            }
        }
    }

    void processStruct(Object val) throws AtlasException {

        if (val == null || !(val instanceof IStruct)) {
            return;
        }

        IStruct i = (IStruct) val;

        IConstructableType type = typeSystem.getDataType(IConstructableType.class, i.getTypeName());

        for (Map.Entry<String, AttributeInfo> e : type.fieldMapping().fields.entrySet()) {
            AttributeInfo aInfo = e.getValue();
            String attrName = e.getKey();
            if (aInfo.dataType().getTypeCategory() != DataTypes.TypeCategory.PRIMITIVE) {
                processValue(aInfo.dataType(), i.get(attrName));
            }
        }
    }

    void processReferenceableInstance(Object val) throws AtlasException {

        if (val == null || !(val instanceof IReferenceableInstance || val instanceof Id)) {
            return;
        }

        if (val instanceof Id) {
            Id id = (Id) val;
            if (id.isUnassigned()) {
                add(id, null);
            }
            return;
        }

        IReferenceableInstance ref = (IReferenceableInstance) val;
        Id id = ref.getId();
        if (id.isUnassigned()) {
            add(id, ref);
            if (!processedIds.contains(id)) {
                processedIds.add(id);
                processStruct(val);

                ImmutableList<String> traits = ref.getTraits();
                for (String trait : traits) {
                    processStruct(ref.getTrait(trait));
                }
            }
        }
    }

    void add(Id id, IReferenceableInstance ref) {
        queue.add(new InstanceTuple(id, ref));
    }


    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public InstanceTuple next() {
        try {
            InstanceTuple t = queue.poll();
            if(t != null) {
                processReferenceableInstance(t.instance);
            }
            return t;
        } catch (AtlasException me) {
            throw new RuntimeException(me);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public static class InstanceTuple {
        public final Id id;
        public final IReferenceableInstance instance;

        public InstanceTuple(Id id, IReferenceableInstance instance) {
            this.id = id;
            this.instance = instance;
        }
    }

}
