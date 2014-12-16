package org.apache.metadata.storage.memory;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import org.apache.metadata.ITypedInstance;
import org.apache.metadata.storage.RepositoryException;
import org.apache.metadata.storage.StructInstance;
import org.apache.metadata.types.AttributeInfo;
import org.apache.metadata.types.FieldMapping;
import org.apache.metadata.types.IConstructableType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AttributeStores {

    private static final Object NULL_VAL = new Object();

    static IAttributeStore createStore(AttributeInfo i) {
        return null;
    }

    static abstract class AbstractAttributeStore {
        AttributeInfo attrInfo;
        final BooleanArrayList nullList;
        final Map<Integer, Map<String, Object>> hiddenVals;

        AbstractAttributeStore(AttributeInfo attrInfo) {
            this.attrInfo = attrInfo;
            this.nullList = new BooleanArrayList();
            hiddenVals = new HashMap<Integer, Map<String, Object>>();
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
            if ( m == null ) {
                m = new HashMap<String, Object>();
                hiddenVals.put(pos, m);
            }
            for(int i=2; i < attrNames.size(); i++ ) {
                String attrName = attrNames.get(i);
                int nullPos = instance.fieldMapping().fieldNullPos.get(attrName);
                int colPos = instance.fieldMapping().fieldPos.get(attrName);
                if ( instance.nullFlags[nullPos] ) {
                    m.put(attrName, NULL_VAL);
                } else{
                    m.put(attrName, instance.bools[colPos]);
                }
            }
        }
    }

    static class BooleanAttributeStore extends AbstractAttributeStore implements IAttributeStore {

        final BooleanArrayList list;

        BooleanAttributeStore(AttributeInfo attrInfo) {
            super(attrInfo);
            this.list = new BooleanArrayList();
        }

        @Override
        public void store(int pos, IConstructableType type, StructInstance instance) throws RepositoryException {
            List<String> attrNames = type.getNames(attrInfo);
            String attrName = attrNames.get(0);
            int nullPos = instance.fieldMapping().fieldNullPos.get(attrName);
            int colPos = instance.fieldMapping().fieldPos.get(attrName);
            nullList.set(pos, instance.nullFlags[nullPos]);
            list.set(pos, instance.bools[colPos]);

            if ( attrNames.size() > 1 ) {
                storeHiddenVals(pos, type, instance);
            }
        }

        @Override
        public void load(int pos, IConstructableType type,  StructInstance instance) throws RepositoryException {

        }

        @Override
        public void ensureCapacity(int pos) throws RepositoryException {
            list.ensureCapacity(pos);
            nullList.ensureCapacity(pos);
        }
    }
}
