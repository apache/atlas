package org.apache.hadoop.metadata.types;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.IStruct;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.storage.Id;
import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.IStruct;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.storage.Id;
import org.apache.hadoop.metadata.storage.RepositoryException;

import java.util.*;

/**
 * Given a IReferenceableInstance, a Walker will traverse the Object Graph
 * reachable form the instance. It will invoke the process call on the provided NodeProcessor
 * for each non-primitive attribute (Structs, Traits, References, Arrays of Non-Primitives, Maps of Non-Primitives)
 */
public class ObjectGraphWalker {

    final Queue<IReferenceableInstance> queue;
    final TypeSystem typeSystem;
    final NodeProcessor nodeProcessor;
    Set<Id> processedIds;

    public ObjectGraphWalker(TypeSystem typeSystem, NodeProcessor nodeProcessor, IReferenceableInstance start)
            throws MetadataException {
        this.typeSystem = typeSystem;
        this.nodeProcessor = nodeProcessor;
        queue = new LinkedList<IReferenceableInstance>();
        processedIds = new HashSet<Id>();
        visitReferenceableInstance(start);
    }

    public ObjectGraphWalker(TypeSystem typeSystem, NodeProcessor nodeProcessor,
                             List<? extends IReferenceableInstance> roots)
            throws MetadataException {
        this.typeSystem = typeSystem;
        this.nodeProcessor = nodeProcessor;
        queue = new LinkedList<IReferenceableInstance>();
        processedIds = new HashSet<Id>();
        for(IReferenceableInstance r : roots) {
            visitReferenceableInstance(r);
        }
    }

    public void walk() throws MetadataException {
        while (!queue.isEmpty()) {
            IReferenceableInstance r = queue.poll();
            processReferenceableInstance(r);
        }
    }

    void traverseValue(IDataType dT, Object val) throws MetadataException {
        if (val != null) {
            if (dT.getTypeCategory() == DataTypes.TypeCategory.ARRAY) {
                IDataType elemType = ((DataTypes.ArrayType) dT).getElemType();
                visitCollection(elemType, val);
            } else if (dT.getTypeCategory() == DataTypes.TypeCategory.MAP) {
                IDataType keyType = ((DataTypes.MapType) dT).getKeyType();
                IDataType valueType = ((DataTypes.MapType) dT).getKeyType();
                visitMap(keyType, valueType, val);
            } else if (dT.getTypeCategory() == DataTypes.TypeCategory.STRUCT ||
                    dT.getTypeCategory() == DataTypes.TypeCategory.TRAIT) {
                visitStruct(val);
            } else if (dT.getTypeCategory() == DataTypes.TypeCategory.CLASS) {
                visitReferenceableInstance(val);
            }
        }
    }

    void visitMap(IDataType keyType, IDataType valueType, Object val) throws MetadataException {
        if (keyType.getTypeCategory() == DataTypes.TypeCategory.PRIMITIVE &&
                valueType.getTypeCategory() == DataTypes.TypeCategory.PRIMITIVE) {
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

    void visitCollection(IDataType elemType, Object val) throws MetadataException {

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

    void visitStruct(Object val) throws MetadataException {

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

    void visitReferenceableInstance(Object val) throws MetadataException {

        if (val == null || !(val instanceof IReferenceableInstance)) {
            return;
        }

        IReferenceableInstance ref = (IReferenceableInstance) val;

        if (!processedIds.contains(ref.getId())) {
            processedIds.add(ref.getId());
            if ( !(ref instanceof  Id) ) {
                queue.add(ref);
            }
        }
    }

    void processReferenceableInstance(IReferenceableInstance ref) throws MetadataException {

        nodeProcessor.processNode(new Node(ref, null, null, null));
        visitStruct(ref);
        ImmutableList<String> traits = ref.getTraits();
        for (String trait : traits) {
            visitStruct(ref.getTrait(trait));
        }
    }

    public static interface NodeProcessor {

        void processNode(Node nd) throws MetadataException;
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
    }
}
