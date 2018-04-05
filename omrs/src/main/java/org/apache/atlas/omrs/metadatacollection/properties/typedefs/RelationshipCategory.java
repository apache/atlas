/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.omrs.metadatacollection.properties.typedefs;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * <p>
 *     The RelationshipCategory determines the style of relationship around containment and lifecycle.
 *     UML terminology is used for the values.  All relationships are navigable in both directions.
 * </p>
 * <p>
 *     Association is a simple relationship with no containment.
 *     Composition and Aggregation are containment relationships which means there is a notion of
 *     control or collective management of the contained entities by the containing entity.
 * </p>
 * <p>
 *     Entities in an aggregation relationship can be
 *     aggregated by many other entities and their lifecycle is not controlled by the containing entity.
 *     For example, contained entities are not deleted when the containing entity is deleted.
 *     The aggregating end of a relationship is End 1.
 * </p>
 * <p>
 *     Composition relationship is a "part of" relationship where the contained entities can only exist in the
 *     scope/context of the containing entity.  Often the fully qualified name of a contained entity
 *     in a composition relationship includes the name of its containing entity.
 *     The composing end of a relationship is end 1.
 * </p>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum RelationshipCategory implements Serializable
{
    UNKNOWN    (0, "<Unknown>",   "Uninitialized Relationship."),
    ASSOCIATION(1, "Association", "Simple relationship."),
    AGGREGATION(2, "Aggregation", "A grouping of entities that are managed together."),
    COMPOSITION(3, "Composition", "A grouping of entities that are part of a bigger concept.");

    private static final long serialVersionUID = 1L;

    private int            ordinal;
    private String         name;
    private String         description;


    /**
     * Constructor to set up a single instances of the enum.
     *
     * @param ordinal - code value
     * @param name - name
     * @param description - default description
     */
    RelationshipCategory(int   ordinal, String    name, String    description)
    {
        this.ordinal = ordinal;
        this.name = name;
        this.description = description;
    }


    /**
     * Return the numeric representation of the relationship category.
     *
     * @return int ordinal
     */
    public int getOrdinal() { return ordinal; }


    /**
     * Return the default name of the relationship category.
     *
     * @return String name
     */
    public String getName() { return name; }


    /**
     * Return the default description of the relationship category.
     *
     * @return String description
     */
    public String getDescription() { return description; }
}
