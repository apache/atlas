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

package org.apache.metadata.storage.memory;

import org.apache.metadata.*;
import org.apache.metadata.storage.IRepository;
import org.apache.metadata.storage.Id;
import org.apache.metadata.storage.RepositoryException;
import org.apache.metadata.types.ObjectGraphTraversal;
import org.apache.metadata.types.ObjectGraphWalker;
import org.apache.metadata.types.TypeSystem;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class MemRepository implements IRepository {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    final TypeSystem typeSystem;
    final AtomicInteger ID_SEQ = new AtomicInteger(0);

    public MemRepository(TypeSystem typeSystem) {
        this.typeSystem = typeSystem;
    }

    @Override
    public DateFormat getDateFormat() {
        return dateFormat;
    }

    @Override
    public DateFormat getTimestampFormat() {
        return timestampFormat;
    }

    @Override
    public boolean allowNullsInCollections() {
        return false;
    }

    Id newId(String typeName) {
        return new Id(ID_SEQ.incrementAndGet(), 0, typeName);
    }

    /**
     * 1. traverse the Object Graph from  i and create idToNewIdMap : Map[Id, Id],
     *    also create old Id to Instance Map: oldIdToInstance : Map[Id, IInstance]
     *   - traverse reference Attributes, List[ClassType], Maps where Key/value is ClassType
     *   - traverse Structs
     *   - traverse Traits.
     * 1b. Ensure that every newId has an associated Instance.
     * 2. Traverse oldIdToInstance map create newInstances : List[ITypedReferenceableInstance]
     *    - create a ITypedReferenceableInstance.
     *      replace any old References ( ids or object references) with new Ids.
     * 3. Traverse over newInstances
     *    - ask ClassStore to assign a position to the Id.
     *      - for Instances with Traits, assign a position for each Trait
     *    - invoke store on the nwInstance.
     *
     * Recovery:
     * - on each newInstance, invoke releaseId and delete on its ClassStore and Traits' Stores.
     *
     * @param i
     * @return
     * @throws RepositoryException
     */
    public ITypedReferenceableInstance create(IReferenceableInstance i) throws RepositoryException {

        DiscoverInstances discoverInstances = new DiscoverInstances(this);

        try {
            new ObjectGraphWalker(typeSystem, discoverInstances, i).walk();
        } catch (MetadataException me) {
            throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
        }


        throw new RepositoryException("not implemented");
    }

    public ITypedReferenceableInstance update(ITypedReferenceableInstance i) throws RepositoryException {
        throw new RepositoryException("not implemented");
    }

    public void delete(ITypedReferenceableInstance i) throws RepositoryException {
        throw new RepositoryException("not implemented");
    }

    public ITypedReferenceableInstance get(Id id) throws RepositoryException {
        throw new RepositoryException("not implemented");
    }

    HierarchicalTypeStore getStore(String typeName) {
        return null;
    }
}
