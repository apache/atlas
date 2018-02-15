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


import java.util.ArrayList;

/**
 * The EnumDef describes an open metadata enumeration.  This enumeration consists of a list of valid values
 * (stored in EnumElementDef objects) and a default value.
 */
public class EnumDef extends AttributeTypeDef
{
    private ArrayList<EnumElementDef> elementDefs    = null;
    private EnumElementDef            defaultValue   = null;


    /**
     * Default constructor sets up an empty EnumDef.
     */
    public EnumDef()
    {
        super(AttributeTypeDefCategory.ENUM_DEF);
    }


    /**
     * Copy/clone constructor sets the EnumDef based on the values from the supplied template.
     *
     * @param template EnumDef
     */
    public EnumDef(EnumDef   template)
    {
        super(template);

        if (template != null)
        {
            elementDefs = template.getElementDefs();
            defaultValue = template.getDefaultValue();
        }
    }


    /**
     * Return the list of defined Enum values for this EnumDef.
     *
     * @return EnumElementDefs list
     */
    public ArrayList<EnumElementDef> getElementDefs()
    {
        if (elementDefs == null)
        {
            return elementDefs;
        }
        else
        {
            return new ArrayList<>(elementDefs);
        }
    }


    /**
     * Set up the list of defined Enum values for this EnumDef.
     *
     * @param elementDefs - EnumElementDefs list
     */
    public void setElementDefs(ArrayList<EnumElementDef> elementDefs) { this.elementDefs = elementDefs; }


    /**
     * Return the default value for the EnumDef.
     *
     * @return EnumElementDef representing the default value
     */
    public EnumElementDef getDefaultValue() { return defaultValue; }


    /**
     * Set up the default value for the EnumDef.
     *
     * @param defaultValue - EnumElementDef representing the default value
     */
    public void setDefaultValue(EnumElementDef defaultValue) { this.defaultValue = defaultValue; }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "EnumDef{" +
                "elementDefs=" + elementDefs +
                ", defaultValue=" + defaultValue +
                ", category=" + category +
                ", guid='" + guid + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", descriptionGUID='" + descriptionGUID + '\'' +
                '}';
    }
}
