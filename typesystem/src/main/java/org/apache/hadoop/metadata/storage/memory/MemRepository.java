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

import org.apache.hadoop.metadata.IReferenceableInstance;
import org.apache.hadoop.metadata.ITypedReferenceableInstance;
import org.apache.hadoop.metadata.MetadataException;
import org.apache.hadoop.metadata.storage.*;
import org.apache.hadoop.metadata.types.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MemRepository implements IRepository {

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    final TypeSystem typeSystem;
    /*
     * A Store for each Class and Trait.
     */
    final Map<String, HierarchicalTypeStore> typeStores;
    final AtomicInteger ID_SEQ = new AtomicInteger(0);

    public MemRepository(TypeSystem typeSystem) {
        this.typeSystem = typeSystem;
        this.typeStores = new HashMap<String, HierarchicalTypeStore>();
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

    @Override
    public Id newId(String typeName) {
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
     * @throws org.apache.hadoop.metadata.storage.RepositoryException
     */
    public ITypedReferenceableInstance create(IReferenceableInstance i) throws RepositoryException {

        DiscoverInstances discoverInstances = new DiscoverInstances(this);

        /*
         * Step 1: traverse the Object Graph from  i and create idToNewIdMap : Map[Id, Id],
         *    also create old Id to Instance Map: oldIdToInstance : Map[Id, IInstance]
         *   - traverse reference Attributes, List[ClassType], Maps where Key/value is ClassType
         *   - traverse Structs
         *   - traverse Traits.
         */
        try {
            new ObjectGraphWalker(typeSystem, discoverInstances, i).walk();
        } catch (MetadataException me) {
            throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
        }

        /*
         * Step 1b: Ensure that every newId has an associated Instance.
         */
        for(Id oldId : discoverInstances.idToNewIdMap.keySet()) {
            if ( !discoverInstances.idToInstanceMap.containsKey(oldId)) {
                throw new RepositoryException(String.format("Invalid Object Graph: " +
                        "Encountered an unassignedId %s that is not associated with an Instance", oldId));
            }
        }

        /* Step 2: Traverse oldIdToInstance map create newInstances : List[ITypedReferenceableInstance]
         * - create a ITypedReferenceableInstance.
         *   replace any old References ( ids or object references) with new Ids.
        */
        List<ITypedReferenceableInstance> newInstances = new ArrayList<ITypedReferenceableInstance>();
        ITypedReferenceableInstance retInstance = null;
        Set<ClassType> classTypes = new TreeSet<ClassType>();
        Set<TraitType> traitTypes = new TreeSet<TraitType>();
        for(IReferenceableInstance transientInstance : discoverInstances.idToInstanceMap.values()) {
            try {
                ClassType cT = typeSystem.getDataType(ClassType.class, transientInstance.getTypeName());
                ITypedReferenceableInstance newInstance = cT.convert(transientInstance, Multiplicity.REQUIRED);
                newInstances.add(newInstance);

                classTypes.add(cT);
                for(String traitName : newInstance.getTraits()) {
                    TraitType tT = typeSystem.getDataType(TraitType.class, traitName);
                    traitTypes.add(tT);
                }

                if (newInstance.getId() == i.getId()) {
                    retInstance = newInstance;
                }

                /*
                 * Now replace old references with new Ids
                 */
                MapIds mapIds = new MapIds(this, discoverInstances.idToNewIdMap);
                new ObjectGraphWalker(typeSystem, mapIds, newInstances).walk();

            } catch (MetadataException me) {
                throw new RepositoryException(
                        String.format("Failed to create Instance(id = %s", transientInstance.getId()), me);
            }
        }

        /*
         * 3. Acquire Class and Trait Storage locks.
         * - acquire them in a stable order (super before subclass, classes before traits
         */
        for(ClassType cT : classTypes) {
            HierarchicalTypeStore st = typeStores.get(cT.getName());
            st.acquireWriteLock();
        }

        for(TraitType tT : traitTypes) {
            HierarchicalTypeStore st = typeStores.get(tT.getName());
            st.acquireWriteLock();
        }


        /*
         * 4. Traverse over newInstances
         *    - ask ClassStore to assign a position to the Id.
         *      - for Instances with Traits, assign a position for each Trait
         *    - invoke store on the nwInstance.
         */
        try {
            for (ITypedReferenceableInstance instance : newInstances) {
                HierarchicalTypeStore st = typeStores.get(instance.getTypeName());
                st.assignPosition(instance.getId());
            }

            for (ITypedReferenceableInstance instance : newInstances) {
                HierarchicalTypeStore st = typeStores.get(instance.getTypeName());
                st.store((ReferenceableInstance)instance);
            }
        } catch(RepositoryException re) {
            for (ITypedReferenceableInstance instance : newInstances) {
                HierarchicalTypeStore st = typeStores.get(instance.getTypeName());
                st.releaseId(instance.getId());
            }
            throw re;
        } finally {
            for(ClassType cT : classTypes) {
                HierarchicalTypeStore st = typeStores.get(cT.getName());
                st.releaseWriteLock();
            }

            for(TraitType tT : traitTypes) {
                HierarchicalTypeStore st = typeStores.get(tT.getName());
                st.releaseWriteLock();
            }
        }

        return retInstance;
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
        return typeStores.get(typeName);
    }

    public void defineClass(ClassType type) throws RepositoryException {
        HierarchicalTypeStore s = new HierarchicalTypeStore(this, type);
        typeStores.put(type.getName(), s);
    }

    public void defineTrait(TraitType type) throws RepositoryException {
        HierarchicalTypeStore s = new HierarchicalTypeStore(this, type);
        typeStores.put(type.getName(), s);
    }

    public void defineTypes(List<HierarchicalType> types) throws RepositoryException {
        List<TraitType> tTypes = new ArrayList<TraitType>();
        List<ClassType> cTypes = new ArrayList<ClassType>();

        for(HierarchicalType h : types) {
            if ( h.getTypeCategory() == DataTypes.TypeCategory.TRAIT ) {
                tTypes.add((TraitType) h);
            } else {
                cTypes.add((ClassType)h);
            }
        }

        Collections.sort(tTypes);
        Collections.sort(cTypes);

        for(TraitType tT : tTypes) {
            defineTrait(tT);
        }

        for(ClassType cT : cTypes) {
            defineClass(cT);
        }
    }
}
