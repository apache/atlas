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

package org.apache.atlas.repository;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.ObjectGraphWalker;

import java.util.HashMap;
import java.util.Map;

/**
 * Graph walker implementation for discovering instances.
 */
@Deprecated
public class DiscoverInstances implements ObjectGraphWalker.NodeProcessor {

    public final Map<Id, Id> idToNewIdMap;
    public final Map<Id, IReferenceableInstance> idToInstanceMap;
    final IRepository repository;

    public DiscoverInstances(IRepository repository) {
        this.repository = repository;
        idToNewIdMap = new HashMap<>();
        idToInstanceMap = new HashMap<>();
    }

    @Override
    public void processNode(ObjectGraphWalker.Node nd) throws AtlasException {

        IReferenceableInstance ref = null;
        Id id = null;

        if (nd.attributeName == null) {
            ref = (IReferenceableInstance) nd.instance;
            id = ref.getId();
        } else if (nd.aInfo.dataType().getTypeCategory() == DataTypes.TypeCategory.CLASS) {
            if (nd.value != null && (nd.value instanceof Id)) {
                id = (Id) nd.value;
            }
        }

        if (id != null) {
            if (id.isUnassigned()) {
                if (!idToNewIdMap.containsKey(id)) {
                    idToNewIdMap.put(id, repository.newId(id.typeName));
                }
                if (ref != null && idToInstanceMap.containsKey(ref)) {
                    // Oops
                    throw new RepositoryException(
                            String.format("Unexpected internal error: Id %s processed again", id));
                }
                if (ref != null) {
                    idToInstanceMap.put(id, ref);
                }
            }
        }
    }
}
