/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.notification.entity;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.IStruct;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Entity notification implementation.
 */
public class EntityNotificationImpl implements EntityNotification {

    private final Referenceable entity;
    private final OperationType operationType;
    private final List<IStruct> traits;


    // ----- Constructors ------------------------------------------------------

    /**
     * No-arg constructor for serialization.
     */
    @SuppressWarnings("unused")
    private EntityNotificationImpl() throws AtlasException {
        this(null, OperationType.ENTITY_CREATE, Collections.<IStruct>emptyList());
    }

    /**
     * Construct an EntityNotification.
     *
     * @param entity            the entity subject of the notification
     * @param operationType     the type of operation that caused the notification
     * @param traits            the traits for the given entity
     *
     * @throws AtlasException if the entity notification can not be created
     */
    public EntityNotificationImpl(Referenceable entity, OperationType operationType, List<IStruct> traits)
        throws AtlasException {
        this.entity = entity;
        this.operationType = operationType;
        this.traits = traits;
    }

    /**
     * Construct an EntityNotification.
     *
     * @param entity         the entity subject of the notification
     * @param operationType  the type of operation that caused the notification
     * @param typeSystem     the Atlas type system
     *
     * @throws AtlasException if the entity notification can not be created
     */
    public EntityNotificationImpl(Referenceable entity, OperationType operationType, TypeSystem typeSystem)
        throws AtlasException {
        this(entity, operationType, getAllTraits(entity, typeSystem));
    }


    // ----- EntityNotification ------------------------------------------------

    @Override
    public IReferenceableInstance getEntity() {
        return entity;
    }

    @Override
    public List<IStruct> getAllTraits() {
        return traits;
    }

    @Override
    public OperationType getOperationType() {
        return operationType;
    }


    // ----- Object overrides --------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntityNotificationImpl that = (EntityNotificationImpl) o;

        return !(entity != null ? !entity.equals(that.entity) : that.entity != null) &&
            operationType == that.operationType &&
            traits.equals(that.traits);
    }

    @Override
    public int hashCode() {
        int result = entity != null ? entity.hashCode() : 0;
        result = 31 * result + operationType.hashCode();
        result = 31 * result + traits.hashCode();
        return result;
    }


    // ----- helper methods ----------------------------------------------------

    private static List<IStruct> getAllTraits(IReferenceableInstance entityDefinition,
                                              TypeSystem typeSystem) throws AtlasException {
        List<IStruct> traitInfo = new LinkedList<>();
        for (String traitName : entityDefinition.getTraits()) {
            IStruct trait = entityDefinition.getTrait(traitName);
            String typeName = trait.getTypeName();
            Map<String, Object> valuesMap = trait.getValuesMap();
            traitInfo.add(new Struct(typeName, valuesMap));
            traitInfo.addAll(getSuperTraits(typeName, valuesMap, typeSystem));
        }
        return traitInfo;
    }

    private static List<IStruct> getSuperTraits(
        String typeName, Map<String, Object> values, TypeSystem typeSystem) throws AtlasException {

        List<IStruct> superTypes = new LinkedList<>();

        TraitType traitDef = typeSystem.getDataType(TraitType.class, typeName);
        Set<String> superTypeNames = traitDef.getAllSuperTypeNames();

        for (String superTypeName : superTypeNames) {
            TraitType superTraitDef = typeSystem.getDataType(TraitType.class, superTypeName);

            Map<String, Object> superTypeValues = new HashMap<>();

            FieldMapping fieldMapping = superTraitDef.fieldMapping();

            if (fieldMapping != null) {
                Set<String> superTypeAttributeNames = fieldMapping.fields.keySet();

                for (String superTypeAttributeName : superTypeAttributeNames) {
                    if (values.containsKey(superTypeAttributeName)) {
                        superTypeValues.put(superTypeAttributeName, values.get(superTypeAttributeName));
                    }
                }
            }
            IStruct superTrait = new Struct(superTypeName, superTypeValues);
            superTypes.add(superTrait);
            superTypes.addAll(getSuperTraits(superTypeName, values, typeSystem));
        }

        return superTypes;
    }
}