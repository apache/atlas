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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.persistence.StructInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.StructType;

import java.util.Collection;
import java.util.Map;

@Deprecated
public class StructStore extends AttributeStores.AbstractAttributeStore implements IAttributeStore {

    final StructType structType;
    final ImmutableMap<AttributeInfo, IAttributeStore> attrStores;

    StructStore(AttributeInfo aInfo) throws RepositoryException {
        super(aInfo);
        this.structType = (StructType) aInfo.dataType();
        ImmutableMap.Builder<AttributeInfo, IAttributeStore> b = new ImmutableBiMap.Builder<>();
        Collection<AttributeInfo> l = structType.fieldMapping.fields.values();
        for (AttributeInfo i : l) {
            b.put(i, AttributeStores.createStore(i));
        }
        attrStores = b.build();

    }

    @Override
    protected void store(StructInstance instance, int colPos, int pos) throws RepositoryException {
        StructInstance s = instance.structs[colPos];
        for (Map.Entry<AttributeInfo, IAttributeStore> e : attrStores.entrySet()) {
            IAttributeStore attributeStore = e.getValue();
            attributeStore.store(pos, structType, s);
        }
    }

    @Override
    protected void load(StructInstance instance, int colPos, int pos) throws RepositoryException {
        StructInstance s = (StructInstance) structType.createInstance();
        instance.structs[colPos] = s;
        for (Map.Entry<AttributeInfo, IAttributeStore> e : attrStores.entrySet()) {
            IAttributeStore attributeStore = e.getValue();
            attributeStore.load(pos, structType, s);
        }
    }

    @Override
    protected void store(StructInstance instance, int colPos, String attrName, Map<String, Object> m) {
        m.put(attrName, instance.structs[colPos]);
    }

    @Override
    protected void load(StructInstance instance, int colPos, Object val) {
        instance.structs[colPos] = (StructInstance) val;
    }

    @Override
    public void ensureCapacity(int pos) throws RepositoryException {
        for (Map.Entry<AttributeInfo, IAttributeStore> e : attrStores.entrySet()) {
            IAttributeStore attributeStore = e.getValue();
            attributeStore.ensureCapacity(pos);
        }
        nullList.size(pos + 1);
    }

}
