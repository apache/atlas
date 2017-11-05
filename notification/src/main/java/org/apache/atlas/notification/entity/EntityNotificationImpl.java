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
package org.apache.atlas.notification.entity;

import org.apache.atlas.AtlasException;
import org.apache.atlas.v1.model.instance.Referenceable;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.atlas.type.AtlasClassificationType;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Entity notification implementation.
 */
public class EntityNotificationImpl implements EntityNotification {

    private final Referenceable entity;
    private final OperationType operationType;
    private final List<Struct> traits;


    // ----- Constructors ------------------------------------------------------

    /**
     * No-arg constructor for serialization.
     */
    @SuppressWarnings("unused")
    private EntityNotificationImpl() throws AtlasException {
        this(null, OperationType.ENTITY_CREATE, Collections.<Struct>emptyList());
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
    public EntityNotificationImpl(Referenceable entity, OperationType operationType, List<Struct> traits)
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
     * @param typeRegistry     the Atlas type system
     *
     * @throws AtlasException if the entity notification can not be created
     */
    public EntityNotificationImpl(Referenceable entity, OperationType operationType, AtlasTypeRegistry typeRegistry)
        throws AtlasException {
        this(entity, operationType, getAllTraits(entity, typeRegistry));
    }


    // ----- EntityNotification ------------------------------------------------

    @Override
    public Referenceable getEntity() {
        return entity;
    }

    @Override
    public List<Struct> getAllTraits() {
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
        return Objects.equals(entity, that.entity) &&
                operationType == that.operationType &&
                Objects.equals(traits, that.traits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entity, operationType, traits);
    }


    // ----- helper methods ----------------------------------------------------

    private static List<Struct> getAllTraits(Referenceable entityDefinition, AtlasTypeRegistry typeRegistry) throws AtlasException {
        List<Struct> ret = new LinkedList<>();

        for (String traitName : entityDefinition.getTraitNames()) {
            Struct                  trait          = entityDefinition.getTrait(traitName);
            AtlasClassificationType traitType      = typeRegistry.getClassificationTypeByName(traitName);
            Set<String>             superTypeNames = traitType != null ? traitType.getAllSuperTypes() : null;

            ret.add(trait);

            if (CollectionUtils.isNotEmpty(superTypeNames)) {
                for (String superTypeName : superTypeNames) {
                    Struct superTypeTrait = new Struct(superTypeName);

                    if (MapUtils.isNotEmpty(trait.getValues())) {
                        AtlasClassificationType superType = typeRegistry.getClassificationTypeByName(superTypeName);

                        if (superType != null && MapUtils.isNotEmpty(superType.getAllAttributes())) {
                            Map<String, Object> attributes = new HashMap<>();

                            // TODO: add superTypeTrait attributess

                            superTypeTrait.setValues(attributes);
                        }
                    }

                    ret.add(superTypeTrait);
                }
            }
        }

        return ret;
    }}
