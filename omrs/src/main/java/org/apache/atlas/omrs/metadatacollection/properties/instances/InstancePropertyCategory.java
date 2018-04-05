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
package org.apache.atlas.omrs.metadatacollection.properties.instances;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The InstancePropertyCategory defines the list of value types for the properties for open metadata.
 * It is used in the InstancePropertyValue class to distinguish its subclasses.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum InstancePropertyCategory implements Serializable
{
    UNKNOWN     (0, "<Unknown>", "Uninitialized InstancePropertyValue object."),
    PRIMITIVE   (1, "Primitive", "A primitive type."),
    ENUM        (2, "Enum",      "A pre-defined list of valid values."),
    STRUCT      (3, "Struct",    "A collection of related properties."),
    MAP         (4, "Map",       "A set of name->value pairs where all names are unique in the map."),
    ARRAY       (5, "Array",     "An ordered list of values, each with the same type.");

    private static final long serialVersionUID = 1L;

    private int            typeCode;
    private String         typeName;
    private String         typeDescription;


    /**
     * Default Constructor
     *
     * @param typeCode - ordinal for this enum
     * @param typeName - symbolic name for this enum
     * @param typeDescription - short description for this enum
     */
    InstancePropertyCategory(int     typeCode, String   typeName, String   typeDescription)
    {
        /*
         * Save the values supplied
         */
        this.typeCode = typeCode;
        this.typeName = typeName;
        this.typeDescription = typeDescription;
    }


    /**
     * Return the code for this enum instance
     *
     * @return int - type code
     */
    public int getTypeCode()
    {
        return typeCode;
    }


    /**
     * Return the default name for this enum instance.
     *
     * @return String - default name
     */
    public String getTypeName()
    {
        return typeName;
    }


    /**
     * Return the default description for the type for this enum instance.
     *
     * @return String - default description
     */
    public String getTypeDescription()
    {
        return typeDescription;
    }
}
