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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * EnumElementDef describes a single valid value defined for an enum.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class EnumElementDef extends TypeDefElementHeader
{
    private int    ordinal         = 99;
    private String value           = null;
    private String description     = null;
    private String descriptionGUID = null;


    /**
     * Default constructor - sets up an empty EnumElementDef
     */
    public EnumElementDef()
    {
        super();
    }


    /**
     * Copy/clone constructor - sets up an EnumElementDef based on the values supplied in the template.
     *
     * @param template EnumElementDef
     */
    public EnumElementDef(EnumElementDef  template)
    {
        super(template);

        if (template != null)
        {
            ordinal = template.getOrdinal();
            value = template.getValue();
            description = template.getDescription();
            descriptionGUID = template.getDescriptionGUID();
        }
    }


    /**
     * Return the numeric value used for the enum value.
     *
     * @return int ordinal
     */
    public int getOrdinal() { return ordinal; }


    /**
     * Set up the numeric value for the enum value.
     *
     * @param ordinal int
     */
    public void setOrdinal(int ordinal) { this.ordinal = ordinal; }


    /**
     * Return the symbolic name for the enum value.
     *
     * @return String name
     */
    public String getValue() { return value; }


    /**
     * Set up the symbolic name for the enum value.
     *
     * @param value String name
     */
    public void setValue(String value) { this.value = value; }


    /**
     * Return the description for the enum value.
     *
     * @return String description
     */
    public String getDescription() { return description; }


    /**
     * Set up the description for the enum value.
     *
     * @param description String
     */
    public void setDescription(String description) { this.description = description; }


    /**
     * Return the unique identifier (guid) of the glossary term that describes this EnumElementDef.
     *
     * @return String guid
     */
    public String getDescriptionGUID()
    {
        return descriptionGUID;
    }


    /**
     * Set up the unique identifier (guid) of the glossary term that describes this EnumElementDef.
     *
     * @param descriptionGUID - String guid
     */
    public void setDescriptionGUID(String descriptionGUID)
    {
        this.descriptionGUID = descriptionGUID;
    }

    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "EnumElementDef{" +
                "ordinal=" + ordinal +
                ", value='" + value + '\'' +
                ", description='" + description + '\'' +
                ", descriptionGUID='" + descriptionGUID + '\'' +
                '}';
    }
}
