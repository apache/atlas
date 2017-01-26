/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;

/**
 * Helper class for TypedInstanceGraphMapper.  Determines which instances
 * should be loaded by GUID and which ones should be loaded by unique attribute.
 * In addition, it sorts the instances that should be loaded by unique
 * attribute by class.
 *
 */
public class VertexLookupContext {

    private final TypedInstanceToGraphMapper mapper;

    private static final TypeSystem typeSystem = TypeSystem.getInstance();

    private Map<ClassType,List<IReferenceableInstance>> instancesWithoutGuids = new HashMap<>();
    private Set<Id> guidsToLookup = new HashSet<>();


    /**
     * @param typedInstanceToGraphMapper
     */
    VertexLookupContext(TypedInstanceToGraphMapper typedInstanceToGraphMapper) {
        mapper = typedInstanceToGraphMapper;
    }

    /**
     * Adds an instance to be loaded.
     *
     */
    public void addInstance(IReferenceableInstance instance) throws AtlasException {

        ClassType classType = typeSystem.getDataType(ClassType.class, instance.getTypeName());
        ITypedReferenceableInstance newInstance = classType.convert(instance, Multiplicity.REQUIRED);
        findReferencedInstancesToPreLoad(newInstance);
        Id id = instance.getId();
        if(mapper.lookupVertex(id) == null) {
            if(id.isAssigned()) {
                guidsToLookup.add(id);
            }
            else {
                addToClassMap(classType, instance);
            }
        }
    }

    /**
     * Returns the instances that should be loaded by unique attribute, sorted by
     * class.
     *
     */
    public Map<ClassType,List<IReferenceableInstance>> getInstancesToLoadByUniqueAttribute() {
        return instancesWithoutGuids;
    }

    /**
     * Returns the Ids of the instance that should be loaded by GUID
     *
     * @return
     */
    public Set<Id> getInstancesToLoadByGuid() {
        return guidsToLookup;
    }

    private void addToClassMap(ClassType classType, IReferenceableInstance instance) throws AtlasException {

        List<IReferenceableInstance> toUpdate = instancesWithoutGuids.get(classType);
        if(toUpdate == null) {
            toUpdate = new ArrayList<>();
            instancesWithoutGuids.put(classType, toUpdate);
        }
        toUpdate.add(instance);
    }

    private void findReferencedInstancesToPreLoad(ITypedReferenceableInstance newInstance) throws AtlasException {
        //pre-load vertices for reference fields
        for(AttributeInfo info : newInstance.fieldMapping().fields.values()) {

            if(info.dataType().getTypeCategory() == TypeCategory.CLASS) {
                ITypedReferenceableInstance newAttributeValue = (ITypedReferenceableInstance)newInstance.get(info.name);
                addAdditionalInstance(newAttributeValue);
            }

            if(info.dataType().getTypeCategory() == TypeCategory.ARRAY) {
                IDataType elementType = ((DataTypes.ArrayType) info.dataType()).getElemType();
                if(elementType.getTypeCategory() == TypeCategory.CLASS) {
                    List<ITypedReferenceableInstance> newElements = (List) newInstance.get(info.name);
                    addAdditionalInstances(newElements);
                }
            }

            if(info.dataType().getTypeCategory() == TypeCategory.MAP) {
                IDataType elementType = ((DataTypes.MapType) info.dataType()).getValueType();
                if(elementType.getTypeCategory() == TypeCategory.CLASS) {
                    Map<Object, ITypedReferenceableInstance> newAttribute =
                            (Map<Object, ITypedReferenceableInstance>) newInstance.get(info.name);

                    if(newAttribute != null) {
                        addAdditionalInstances(newAttribute.values());
                    }
                }
            }
        }
    }

    private void addAdditionalInstance(ITypedReferenceableInstance instance) {

        if(instance == null) {
            return;
        }

        Id id = mapper.getExistingId(instance);
        if(! id.isAssigned()) {
            return;
        }
        guidsToLookup.add(id);
    }



    private void addAdditionalInstances(Collection<ITypedReferenceableInstance> newElements) {
        if(newElements != null) {
            for(ITypedReferenceableInstance instance: newElements) {
                addAdditionalInstance(instance);
            }
        }
    }
}