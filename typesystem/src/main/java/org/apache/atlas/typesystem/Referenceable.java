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

package org.apache.atlas.typesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.atlas.AtlasException;
import org.apache.atlas.classification.InterfaceAudience;
import org.apache.atlas.typesystem.persistence.Id;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a Class Instance that has not been associated with a FieldMapping.
 */
public class Referenceable extends Struct implements IReferenceableInstance {

    private Id id;
    private final ImmutableMap<String, IStruct> traits;
    private final ImmutableList<String> traitNames;

    public Referenceable(String typeName, String... traitNames) {
        super(typeName);
        id = new Id(typeName);
        this.traitNames = ImmutableList.copyOf(traitNames);
        ImmutableMap.Builder<String, IStruct> b = new ImmutableMap.Builder<>();
        for (String t : traitNames) {
            b.put(t, new Struct(t));
        }
        traits = b.build();
    }

    public Referenceable(String typeName, Map<String, Object> values) {
        super(typeName, values);
        id = new Id(typeName);
        traitNames = ImmutableList.of();
        traits = ImmutableMap.of();
    }

    public Referenceable(String guid, String typeName, Map<String, Object> values) {
        super(typeName, values);
        id = new Id(guid, 0, typeName);
        traitNames = ImmutableList.of();
        traits = ImmutableMap.of();
    }

    /**
     * Not public - only use during deserialization
     * @param guid      the unique id
     * @param typeName  the type name
     * @param values    the entity attribute values
     */
    @InterfaceAudience.Private
    public Referenceable(String guid, String typeName, Map<String, Object> values, List<String> _traitNames,
            Map<String, IStruct> _traits) {
        super(typeName, values);
        id = new Id(guid, 0, typeName);
        traitNames = ImmutableList.copyOf(_traitNames);
        traits = ImmutableMap.copyOf(_traits);
    }

    /**
     * Construct a Referenceable from the given IReferenceableInstance.
     *
     * @param instance  the referenceable instance to copy
     *
     * @throws AtlasException if the referenceable can not be created
     */
    public Referenceable(IReferenceableInstance instance) throws AtlasException {
        this(instance.getId()._getId(), instance.getTypeName(), instance.getValuesMap(), instance.getTraits(),
            getTraits(instance));
    }

    /**
     * No-arg constructor for serialization.
     */
    @SuppressWarnings("unused")
    private Referenceable() {
        super(null, null);
        id = null;
        traitNames = ImmutableList.of();
        traits = ImmutableMap.of();
    }

    @Override
    public ImmutableList<String> getTraits() {
        return traitNames;
    }

    @Override
    public Id getId() {
        return id;
    }

    @Override
    public IStruct getTrait(String typeName) {
        return traits.get(typeName);
    }

    /**
     * Matches traits, values associated with this Referenceable and skips the id match
     * @param o The Referenceable which needs to be matched with
     * @return
     */
    public boolean equalsContents(Object o) {
        if(this == o) {
            return true;
        }
        if(o == null) {
            return false;
        }
        if (o.getClass() != getClass()) {
            return false;
        }

        if(!super.equalsContents(o)) {
            return false;
        }

        Referenceable obj = (Referenceable)o;
        if (!traitNames.equals(obj.getTraits())) {
            return false;
        }

        return true;
    }

    public String toString() {
        return "{" +
            "Id='" + id + '\'' +
            ", traits=" + traitNames +
            ", values=" + getValuesMap() +
            '}';
    }

    public void replaceWithNewId(Id id) {
        this.id = id;
    }

    private static Map<String, IStruct> getTraits(IReferenceableInstance instance) throws AtlasException {
        Map<String, IStruct> traits = new HashMap<>();
        for (String traitName : instance.getTraits() ) {
            traits.put(traitName, new Struct(traitName, instance.getTrait(traitName).getValuesMap()));
        }
        return traits;
    }
}
