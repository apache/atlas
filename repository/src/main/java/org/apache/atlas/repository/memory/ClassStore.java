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
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.types.ClassType;

import java.util.ArrayList;
import java.util.Objects;

@Deprecated
public class ClassStore extends HierarchicalTypeStore {

    final ArrayList<ImmutableList<String>> traitNamesStore;
    final ClassType classType;

    public ClassStore(MemRepository repository, ClassType hierarchicalType) throws RepositoryException {
        super(repository, hierarchicalType);
        classType = hierarchicalType;
        traitNamesStore = new ArrayList<>();
    }

    void store(ReferenceableInstance i) throws RepositoryException {
        super.store(i);
        int pos = idPosMap.get(i.getId());
        traitNamesStore.set(pos, i.getTraits());
    }

    public void ensureCapacity(int pos) throws RepositoryException {
        super.ensureCapacity(pos);
        while (traitNamesStore.size() < pos + 1) {
            traitNamesStore.add(null);
        }
    }

    boolean validate(MemRepository repo, Id id) throws RepositoryException {
        if (id.isUnassigned()) {
            throw new RepositoryException(String.format("Invalid Id (unassigned) : %s", id));
        }
        Integer pos = idPosMap.get(id);
        if (pos == null) {
            throw new RepositoryException(String.format("Invalid Id (unknown) : %s", id));
        }

        String typeName = typeNameList.get(pos);
        if (!Objects.equals(typeName, hierarchicalType.getName())) {
            throw new RepositoryException(
                    String.format("Invalid Id (incorrect typeName, type is %s) : %s", typeName, id));
        }

        return true;
    }

    /*
     * - assumes id is already validated
     */
    ReferenceableInstance createInstance(MemRepository repo, Id id) throws RepositoryException {
        Integer pos = idPosMap.get(id);
        String typeName = typeNameList.get(pos);
        if (!Objects.equals(typeName, hierarchicalType.getName())) {
            return repo.getClassStore(typeName).createInstance(repo, id);
        }

        ImmutableList<String> traitNames = traitNamesStore.get(pos);
        String[] tNs = traitNames.toArray(new String[]{});

        try {
            return (ReferenceableInstance) classType.createInstance(id, tNs);
        } catch (AtlasException me) {
            throw new RepositoryException(me);
        }
    }
}
