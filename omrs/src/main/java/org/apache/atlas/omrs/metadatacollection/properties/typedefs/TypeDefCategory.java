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
 * The TypeDefCategory defines the list of valid types of TypeDef for open metadata instances.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum TypeDefCategory implements Serializable
{
    UNKNOWN_DEF        (0, "<Unknown>",         "Uninitialized TypeDef object."),
    CLASSIFICATION_DEF (5, "ClassificationDef", "A description of a specific characteristic or grouping for entities."),
    ENTITY_DEF         (6, "EntityDef",         "An object or concept of interest."),
    RELATIONSHIP_DEF   (8, "RelationshipDef",   "A link between two entities.");

    private static final long serialVersionUID = 1L;

    private int            typeCode;
    private String         typeName;
    private String         typeDescription;


    /**
     * Constructor to set up a single instances of the enum.
     */
    TypeDefCategory(int     typeCode, String   typeName, String   typeDescription)
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
