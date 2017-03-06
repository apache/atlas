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

import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.persistence.StructInstance;
import org.apache.atlas.typesystem.types.TraitType;

import java.util.ArrayList;

@Deprecated
public class TraitStore extends HierarchicalTypeStore {

    final ArrayList<String> classNameStore;

    public TraitStore(MemRepository repository, TraitType hierarchicalType) throws RepositoryException {
        super(repository, hierarchicalType);
        classNameStore = new ArrayList<>();
    }

    void store(ReferenceableInstance i) throws RepositoryException {
        int pos = idPosMap.get(i.getId());
        StructInstance s = (StructInstance) i.getTrait(hierarchicalType.getName());
        super.storeFields(pos, s);
        classNameStore.set(pos, i.getTypeName());
    }

    void load(ReferenceableInstance i) throws RepositoryException {
        int pos = idPosMap.get(i.getId());
        StructInstance s = (StructInstance) i.getTrait(hierarchicalType.getName());
        super.loadFields(pos, s);
    }

    public void ensureCapacity(int pos) throws RepositoryException {
        super.ensureCapacity(pos);
        while (classNameStore.size() < pos + 1) {
            classNameStore.add(null);
        }
    }
}
