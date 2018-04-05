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
 * ClassificationPropagationRule is part of a relationship definition (RelationshipDef).
 * It indicates whether classifications from one entity should propagate across a relationship instance.
 * It allows classification for, say confidentiality to be propagated to related entities.
 * <p>
 *     The propagation rule defines the direction of propagation:
 * </p>
 * <ul>
 *     <li>NONE - no propagation of classifications across the relationship.</li>
 *     <li>ONE_TO_TWO - from entity at end 1 of the relationship to the entity at end 2 of the relationship.</li>
 *     <li>TWO_TO_ONE - from entity at end 2 of the relationship to the entity at end 1 of the relationship.</li>
 *     <li>BOTH - two way propagation.</li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum ClassificationPropagationRule implements Serializable
{
    NONE       (0, "NONE",       "No classification propagation"),
    ONE_TO_TWO (1, "ONE_TO_TWO", "Classification propagation direction is one way from entity one to entity two"),
    TWO_TO_ONE (2, "TWO_TO_ONE", "Classification propagation direction is one way from entity two to entity one"),
    BOTH       (3, "BOTH",       "Classification propagation in both directions");

    private static final long serialVersionUID = 1L;

    private int            ordinal;
    private String         name;
    private String         description;


    /**
     * Constructor to set up a single instances of the enum.
     *
     * @param ordinal - numerical representation of the propagation rule
     * @param name - default string name of the propagation rule
     * @param description - default string description of the propagation rule
     */
    ClassificationPropagationRule(int  ordinal, String name, String description)
    {
        this.ordinal = ordinal;
        this.name = name;
        this.description = description;
    }


    /**
     * Return the numeric representation of the propagation rule.
     *
     * @return int ordinal
     */
    public int getOrdinal() { return ordinal; }


    /**
     * Return the default name of the propagation rule.
     *
     * @return String name
     */
    public String getName() { return name; }


    /**
     * Return the default description of the propagation rule.
     *
     * @return String description
     */
    public String getDescription() { return description; }
}
