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

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.DiscoverInstances;
import org.apache.atlas.repository.IRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.MapIds;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.HierarchicalTypeDependencySorter;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.ObjectGraphWalker;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

@Deprecated
public class MemRepository implements IRepository {

    final TypeSystem typeSystem;
    /*
     * A Store for each Class and Trait.
     */
    final Map<String, HierarchicalTypeStore> typeStores;
    final AtomicInteger ID_SEQ = new AtomicInteger(0);

    public MemRepository(TypeSystem typeSystem) {
        this.typeSystem = typeSystem;
        this.typeStores = new HashMap<>();
    }

    @Override
    public Id newId(String typeName) {
        return new Id("" + ID_SEQ.incrementAndGet(), 0, typeName);
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
     * @throws org.apache.atlas.repository.RepositoryException
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
        } catch (AtlasException me) {
            throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
        }

        /*
         * Step 1b: Ensure that every newId has an associated Instance.
         */
        for (Id oldId : discoverInstances.idToNewIdMap.keySet()) {
            if (!discoverInstances.idToInstanceMap.containsKey(oldId)) {
                throw new RepositoryException(String.format("Invalid Object Graph: "
                                + "Encountered an unassignedId %s that is not associated with an Instance", oldId));
            }
        }

        /* Step 2: Traverse oldIdToInstance map create newInstances :
        List[ITypedReferenceableInstance]
         * - create a ITypedReferenceableInstance.
         *   replace any old References ( ids or object references) with new Ids.
        */
        List<ITypedReferenceableInstance> newInstances = new ArrayList<>();
        ITypedReferenceableInstance retInstance = null;
        Set<ClassType> classTypes = new TreeSet<>();
        Set<TraitType> traitTypes = new TreeSet<>();
        for (IReferenceableInstance transientInstance : discoverInstances.idToInstanceMap.values()) {
            try {
                ClassType cT = typeSystem.getDataType(ClassType.class, transientInstance.getTypeName());
                ITypedReferenceableInstance newInstance = cT.convert(transientInstance, Multiplicity.REQUIRED);
                newInstances.add(newInstance);

                classTypes.add(cT);
                for (String traitName : newInstance.getTraits()) {
                    TraitType tT = typeSystem.getDataType(TraitType.class, traitName);
                    traitTypes.add(tT);
                }

                if (newInstance.getId() == i.getId()) {
                    retInstance = newInstance;
                }

                /*
                 * Now replace old references with new Ids
                 */
                MapIds mapIds = new MapIds(discoverInstances.idToNewIdMap);
                new ObjectGraphWalker(typeSystem, mapIds, newInstances).walk();

            } catch (AtlasException me) {
                throw new RepositoryException(
                        String.format("Failed to create Instance(id = %s", transientInstance.getId()), me);
            }
        }

        /*
         * 3. Acquire Class and Trait Storage locks.
         * - acquire them in a stable order (super before subclass, classes before traits
         */
        for (ClassType cT : classTypes) {
            HierarchicalTypeStore st = typeStores.get(cT.getName());
            st.acquireWriteLock();
        }

        for (TraitType tT : traitTypes) {
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
                for (String traitName : instance.getTraits()) {
                    HierarchicalTypeStore tt = typeStores.get(traitName);
                    tt.assignPosition(instance.getId());
                }
            }

            for (ITypedReferenceableInstance instance : newInstances) {
                HierarchicalTypeStore st = typeStores.get(instance.getTypeName());
                st.store((ReferenceableInstance) instance);
                for (String traitName : instance.getTraits()) {
                    HierarchicalTypeStore tt = typeStores.get(traitName);
                    tt.store((ReferenceableInstance) instance);
                }
            }
        } catch (RepositoryException re) {
            for (ITypedReferenceableInstance instance : newInstances) {
                HierarchicalTypeStore st = typeStores.get(instance.getTypeName());
                st.releaseId(instance.getId());
            }
            throw re;
        } finally {
            for (ClassType cT : classTypes) {
                HierarchicalTypeStore st = typeStores.get(cT.getName());
                st.releaseWriteLock();
            }

            for (TraitType tT : traitTypes) {
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

        try {
            ReplaceIdWithInstance replacer = new ReplaceIdWithInstance(this);
            ObjectGraphWalker walker = new ObjectGraphWalker(typeSystem, replacer);
            replacer.setWalker(walker);
            ITypedReferenceableInstance r = getDuringWalk(id, walker);
            walker.walk();
            return r;
        } catch (AtlasException me) {
            throw new RepositoryException("TypeSystem error when walking the ObjectGraph", me);
        }
    }

    /*
     * - Id must be valid; Class must be valid.
     * - Ask ClassStore to createInstance.
     * - Ask ClassStore to load instance.
     * - load instance traits
     * - add to GraphWalker
     */
    ITypedReferenceableInstance getDuringWalk(Id id, ObjectGraphWalker walker) throws RepositoryException {
        ClassStore cS = getClassStore(id.getTypeName());
        if (cS == null) {
            throw new RepositoryException(String.format("Unknown Class %s", id.getTypeName()));
        }
        cS.validate(this, id);
        ReferenceableInstance r = cS.createInstance(this, id);
        cS.load(r);
        for (String traitName : r.getTraits()) {
            HierarchicalTypeStore tt = typeStores.get(traitName);
            tt.load(r);
        }

        walker.addRoot(r);
        return r;
    }

    HierarchicalTypeStore getStore(String typeName) {
        return typeStores.get(typeName);
    }

    ClassStore getClassStore(String typeName) {
        return (ClassStore) getStore(typeName);
    }

    public void defineClass(ClassType type) throws RepositoryException {
        HierarchicalTypeStore s = new ClassStore(this, type);
        typeStores.put(type.getName(), s);
    }

    public void defineTrait(TraitType type) throws RepositoryException {
        HierarchicalTypeStore s = new TraitStore(this, type);
        typeStores.put(type.getName(), s);
    }

    public void defineTypes(List<HierarchicalType> types) throws RepositoryException {
        List<TraitType> tTypes = new ArrayList<>();
        List<ClassType> cTypes = new ArrayList<>();

        for (HierarchicalType h : types) {
            if (h.getTypeCategory() == DataTypes.TypeCategory.TRAIT) {
                tTypes.add((TraitType) h);
            } else {
                cTypes.add((ClassType) h);
            }
        }
        tTypes = HierarchicalTypeDependencySorter.sortTypes(tTypes);
        cTypes = HierarchicalTypeDependencySorter.sortTypes(cTypes);

        for (TraitType tT : tTypes) {
            defineTrait(tT);
        }

        for (ClassType cT : cTypes) {
            defineClass(cT);
        }
    }
}
