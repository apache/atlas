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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.persistence.StructInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IConstructableType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Deprecated
public abstract class HierarchicalTypeStore {

    final MemRepository repository;
    final IConstructableType hierarchicalType;
    final ArrayList<String> typeNameList;
    final ImmutableMap<AttributeInfo, IAttributeStore> attrStores;
    final ImmutableList<HierarchicalTypeStore> superTypeStores;


    /**
     * Map Id to position in storage lists.
     */
    Map<Id, Integer> idPosMap;

    List<Integer> freePositions;

    int nextPos;

    /**
     * Lock for each Class/Trait.
     */
    ReentrantReadWriteLock lock;

    HierarchicalTypeStore(MemRepository repository, HierarchicalType hierarchicalType) throws RepositoryException {
        this.hierarchicalType = (IConstructableType) hierarchicalType;
        this.repository = repository;
        ImmutableMap.Builder<AttributeInfo, IAttributeStore> b =
                new ImmutableBiMap.Builder<>();
        typeNameList = Lists.newArrayList((String) null);
        ImmutableList<AttributeInfo> l = hierarchicalType.immediateAttrs;
        for (AttributeInfo i : l) {
            b.put(i, AttributeStores.createStore(i));
        }
        attrStores = b.build();

        ImmutableList.Builder<HierarchicalTypeStore> b1 = new ImmutableList.Builder<>();
        Set<String> allSuperTypeNames = hierarchicalType.getAllSuperTypeNames();
        for (String s : allSuperTypeNames) {
            b1.add(repository.getStore(s));
        }
        superTypeStores = b1.build();

        nextPos = 0;
        idPosMap = new HashMap<>();
        freePositions = new ArrayList<>();

        lock = new ReentrantReadWriteLock();
    }

    /**
     * Assign a storage position to an Id.
     * - try to assign from freePositions
     * - ensure storage capacity.
     * - add entry in idPosMap.
     * @param id
     * @return
     * @throws RepositoryException
     */
    int assignPosition(Id id) throws RepositoryException {

        int pos = -1;
        if (!freePositions.isEmpty()) {
            pos = freePositions.remove(0);
        } else {
            pos = nextPos++;
            ensureCapacity(pos);
        }

        idPosMap.put(id, pos);

        for (HierarchicalTypeStore s : superTypeStores) {
            s.assignPosition(id);
        }

        return pos;
    }

    /**
     * - remove from idPosMap
     * - add to freePositions.
     * @throws RepositoryException
     */
    void releaseId(Id id) {

        Integer pos = idPosMap.get(id);
        if (pos != null) {
            idPosMap.remove(id);
            freePositions.add(pos);

            for (HierarchicalTypeStore s : superTypeStores) {
                s.releaseId(id);
            }
        }
    }

    void acquireReadLock() {
        lock.readLock().lock();
    }

    void acquireWriteLock() {
        lock.writeLock().lock();
    }

    void releaseReadLock() {
        lock.readLock().unlock();
    }

    void releaseWriteLock() {
        lock.writeLock().unlock();
    }

    protected void storeFields(int pos, StructInstance s) throws RepositoryException {
        for (Map.Entry<AttributeInfo, IAttributeStore> e : attrStores.entrySet()) {
            IAttributeStore attributeStore = e.getValue();
            attributeStore.store(pos, hierarchicalType, s);
        }
    }

    protected void loadFields(int pos, StructInstance s) throws RepositoryException {
        for (Map.Entry<AttributeInfo, IAttributeStore> e : attrStores.entrySet()) {
            IAttributeStore attributeStore = e.getValue();
            attributeStore.load(pos, hierarchicalType, s);
        }
    }

    /**
     * - store the typeName
     * - store the immediate attributes in the respective IAttributeStore
     * - call store on each SuperType.
     * @param i
     * @throws RepositoryException
     */
    void store(ReferenceableInstance i) throws RepositoryException {
        int pos = idPosMap.get(i.getId());
        typeNameList.set(pos, i.getTypeName());
        storeFields(pos, i);

        for (HierarchicalTypeStore s : superTypeStores) {
            s.store(i);
        }
    }

    /**
     * - copy over the immediate attribute values from the respective IAttributeStore
     * - call load on each SuperType.
     * @param i
     * @throws RepositoryException
     */
    void load(ReferenceableInstance i) throws RepositoryException {
        int pos = idPosMap.get(i.getId());
        loadFields(pos, i);

        for (HierarchicalTypeStore s : superTypeStores) {
            s.load(i);
        }
    }

    public void ensureCapacity(int pos) throws RepositoryException {
        while (typeNameList.size() < pos + 1) {
            typeNameList.add(null);
        }
        for (Map.Entry<AttributeInfo, IAttributeStore> e : attrStores.entrySet()) {
            IAttributeStore attributeStore = e.getValue();
            attributeStore.ensureCapacity(pos);
        }
    }
}
