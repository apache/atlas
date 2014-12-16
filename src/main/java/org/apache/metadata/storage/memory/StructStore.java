package org.apache.metadata.storage.memory;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.metadata.storage.RepositoryException;
import org.apache.metadata.storage.StructInstance;
import org.apache.metadata.types.AttributeInfo;
import org.apache.metadata.types.HierarchicalType;
import org.apache.metadata.types.IConstructableType;
import org.apache.metadata.types.StructType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StructStore extends AttributeStores.AbstractAttributeStore implements IAttributeStore {

    final StructType structType;
    final ImmutableMap<AttributeInfo, IAttributeStore> attrStores;

    StructStore(AttributeInfo aInfo) throws RepositoryException {
        super(aInfo);
        this.structType = (StructType) aInfo.dataType();
        ImmutableMap.Builder<AttributeInfo, IAttributeStore> b = new ImmutableBiMap.Builder<AttributeInfo,
                IAttributeStore>();
        Collection<AttributeInfo> l =  structType.fieldMapping.fields.values();
        for(AttributeInfo i : l) {
            b.put(i, AttributeStores.createStore(i) );
        }
        attrStores = b.build();

    }

    @Override
    public void store(int pos, IConstructableType type, StructInstance instance) throws RepositoryException {
        List<String> attrNames = type.getNames(attrInfo);
        String attrName = attrNames.get(0);
        int nullPos = instance.fieldMapping().fieldNullPos.get(attrName);
        int colPos = instance.fieldMapping().fieldPos.get(attrName);
        nullList.set(pos, instance.nullFlags[nullPos]);
        if ( !instance.nullFlags[nullPos] ) {
            StructInstance s = instance.structs[colPos];
            for(Map.Entry<AttributeInfo, IAttributeStore> e : attrStores.entrySet()) {
                e.getValue().store(pos, structType, s);
            }
        }

        if ( attrNames.size() > 1 ) {
            storeHiddenVals(pos, type, instance);
        }
    }

    @Override
    public void load(int pos, IConstructableType type, StructInstance instance) throws RepositoryException {

    }

    @Override
    public void ensureCapacity(int pos) throws RepositoryException {

    }
}
