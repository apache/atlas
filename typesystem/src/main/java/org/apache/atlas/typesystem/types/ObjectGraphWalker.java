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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Given a IReferenceableInstance, a Walker will traverse the Object Graph
 * reachable form the instance. It will invoke the process call on the provided NodeProcessor
 * for each non-primitive attribute (Structs, Traits, References, Arrays of Non-Primitives, Maps
 * of Non-Primitives)
 */
public class ObjectGraphWalker {

    final Queue<IReferenceableInstance> queue;
    final TypeSystem typeSystem;
    final NodeProcessor nodeProcessor;
    Set<Id> processedIds;

    public ObjectGraphWalker(TypeSystem typeSystem, NodeProcessor nodeProcessor) throws AtlasException {
        this(typeSystem, nodeProcessor, (IReferenceableInstance) null);
    }

    public ObjectGraphWalker(TypeSystem typeSystem, NodeProcessor nodeProcessor, IReferenceableInstance start)
    throws AtlasException {
        this.typeSystem = typeSystem;
        this.nodeProcessor = nodeProcessor;
        queue = new LinkedList<>();
        processedIds = new HashSet<>();
        if (start != null) {
            visitReferenceableInstance(start);
        }
    }

    public ObjectGraphWalker(TypeSystem typeSystem, NodeProcessor nodeProcessor,
            List<? extends IReferenceableInstance> roots) throws AtlasException {
        this.typeSystem = typeSystem;
        this.nodeProcessor = nodeProcessor;
        queue = new LinkedList<IReferenceableInstance>();
        processedIds = new HashSet<Id>();
        for (IReferenceableInstance r : roots) {
            visitReferenceableInstance(r);
        }
    }

    public void walk() throws AtlasException {
        while (!queue.isEmpty()) {
            IReferenceableInstance r = queue.poll();
            processReferenceableInstance(r);
        }
    }

    public void addRoot(IReferenceableInstance root) {
        visitReferenceableInstance(root);
    }

    void traverseValue(IDataType dT, Object val) throws AtlasException {
        if (val != null) {
            if (dT.getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
                IDataType elemType = ((DataTypes.ArrayType) dT).getElemType();
                visitCollection(elemType, val);
            } else if (dT.getTypeCategory() == DataTypes.TypeCategory.MAP) {
                IDataType keyType = ((DataTypes.MapType) dT).getKeyType();
                IDataType valueType = ((DataTypes.MapType) dT).getValueType();
                visitMap(keyType, valueType, val);
            } else if (dT.getTypeCategory() == DataTypes.TypeCategory.STRUCT
                    || dT.getTypeCategory() == DataTypes.TypeCategory.TRAIT) {
                visitStruct(val);
            } else if (dT.getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                visitReferenceableInstance(val);
            }
        }
    }

    void visitMap(IDataType keyType, IDataType valueType, Object val) throws AtlasException {
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
                    traverseValue(keyType, e.getKey());
                    traverseValue(valueType, e.getValue());
                }
            }
        }
    }

    void visitCollection(IDataType elemType, Object val) throws AtlasException {

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
                    traverseValue(elemType, elem);
                }
            }
        }
    }

    void visitStruct(Object val) throws AtlasException {

        if (val == null || !(val instanceof IStruct)) {
            return;
        }

        IStruct i = (IStruct) val;

        IConstructableType type = typeSystem.getDataType(IConstructableType.class, i.getTypeName());

        for (Map.Entry<String, AttributeInfo> e : type.fieldMapping().fields.entrySet()) {
            AttributeInfo aInfo = e.getValue();
            String attrName = e.getKey();
            if (aInfo.dataType().getTypeCategory() != DataTypes.TypeCategory.PRIMITIVE) {
                Object aVal = i.get(attrName);
                nodeProcessor.processNode(new Node(i, attrName, aInfo, aVal));
                traverseValue(aInfo.dataType(), aVal);
            }
        }
    }

    void visitReferenceableInstance(Object val) {

        if (val == null || !(val instanceof IReferenceableInstance)) {
            return;
        }

        IReferenceableInstance ref = (IReferenceableInstance) val;

        if (!processedIds.contains(ref.getId())) {
            processedIds.add(ref.getId());
            if (!(ref instanceof Id)) {
                queue.add(ref);
            }
        }
    }

    void processReferenceableInstance(IReferenceableInstance ref) throws AtlasException {

        nodeProcessor.processNode(new Node(ref, null, null, null));
        visitStruct(ref);
        ImmutableList<String> traits = ref.getTraits();
        for (String trait : traits) {
            visitStruct(ref.getTrait(trait));
        }
    }

    public interface NodeProcessor {

        void processNode(Node nd) throws AtlasException;
    }

    /**
     * Represents a non-primitive value of an instance.
     */
    public static class Node {
        public final IStruct instance;
        public final String attributeName;
        public final AttributeInfo aInfo;
        public final Object value;

        public Node(IStruct instance, String attributeName, AttributeInfo aInfo, Object value) {
            this.instance = instance;
            this.attributeName = attributeName;
            this.aInfo = aInfo;
            this.value = value;
        }

        @Override
        public String toString(){
            StringBuilder string = new StringBuilder().append(instance).append(aInfo).append(value);
            return string.toString();
        }
    }
}
