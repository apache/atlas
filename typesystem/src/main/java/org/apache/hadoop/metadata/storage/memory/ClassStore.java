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

package org.apache.hadoop.metadata.storage.memory;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.metadata.storage.ReferenceableInstance;
import org.apache.hadoop.metadata.storage.RepositoryException;
import org.apache.hadoop.metadata.types.ClassType;
import org.apache.hadoop.metadata.types.HierarchicalType;

import java.util.ArrayList;

public class ClassStore extends HierarchicalTypeStore {

    final ArrayList<ImmutableList<String>> traitNamesStore;

    public ClassStore(MemRepository repository, ClassType hierarchicalType) throws RepositoryException {
        super(repository, hierarchicalType);
        traitNamesStore = new ArrayList<ImmutableList<String>>();
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
}
