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
 * AttributeCardinality is used on a association from one TypeDef to another.  It defines how many instances the "linked to" TypeDef
 * are permitted to be connected to the "linked from" TypeDef:
 * <ul>
 *     <li>
 *         UNKNOWN - uninitialized cardinality
 *     </li>
 *     <li>
 *         AT_MOST_ONE - means there can be zero or one instances connected to an instance of this TypeDef.  This
 *                       relationship is often written as 0..1.</li>
 *     <li>
 *         ONE_ONLY - means there must be one instance, no more and no less.
 *     </li>
 *     <li>
 *         AT_LEAST_ONE_ORDERED - means there must be one or more instances connected to an instance of this TypeDef.
 *                                This relationship is often written as 1..*.  The linked instances are maintained
 *                                in an ordered list/array.
 *     </li>
 *     <li>
 *         AT_LEAST_ONE_UNORDERED - means there must be one or more instances connected to an instance of this TypeDef.
 *                                  This relationship is often written as 1..*.  The linked instances are maintained
 *                                  in an unordered set.
 *     </li>
 *     <li>
 *         ANY_NUMBER_ORDERED - means there can be none, one or many instances connected an instance of this TypeDef.
 *                              This relationship is often written as 0..*.  The linked instances are maintained
 *                              in an ordered list/array.
 *     </li>
 *     <li>
 *         ANY_NUMBER_UNORDERED - means there can be none, one or many instances connected an instance of this TypeDef.
 *                                This relationship is often written as 0..*.  The linked instances are maintained
 *                                in an unordered set.
 *     </li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum AttributeCardinality implements Serializable
{
    UNKNOWN                (0, "<Unknown>",                "Unknown or uninitialized cardinality"),
    AT_MOST_ONE            (1, "At Most One",              "0..1 - Zero or one instances. 0..1."),
    ONE_ONLY               (2, "One Only",                 "1 - One instance, no more and no less"),
    AT_LEAST_ONE_ORDERED   (3, "At Least One (Ordered)",   "1..* - One or more instances (stored in specific order)"),
    AT_LEAST_ONE_UNORDERED (4, "At Least One (Unordered)", "1..* - One or more instances (stored in any order)"),
    ANY_NUMBER_ORDERED     (5, "Any Number (Ordered)",     "0..* - Any number of instances (stored in a specific order)"),
    ANY_NUMBER_UNORDERED   (6, "Any Number (Unordered)",   "0..* - Any number of instances (stored in any order)");

    private static final long serialVersionUID = 1L;

    private int     ordinal;
    private String  name;
    private String  description;


    /**
     * Constructor to set up a single instances of the enum.
     *
     * @param ordinal - numerical representation of the cardinality
     * @param name - default string name of the cardinality
     * @param description - default string description of the cardinality
     */
    AttributeCardinality(int  ordinal, String name, String description)
    {
        this.ordinal = ordinal;
        this.name = name;
        this.description = description;
    }


    /**
     * Return the numeric representation of the cardinality.
     *
     * @return int ordinal
     */
    public int getOrdinal() { return ordinal; }


    /**
     * Return the default name of the cardinality.
     *
     * @return String name
     */
    public String getName() { return name; }


    /**
     * Return the default description of the cardinality.
     *
     * @return String description
     */
    public String getDescription() { return description; }
}
