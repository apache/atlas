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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * An EnumPropertyValue stores the value for an enum property.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class EnumPropertyValue extends InstancePropertyValue
{
    private int     ordinal = 99;
    private String  symbolicName = null;
    private String  description = null;


    /**
     * Default constructor initializes an empty enum value
     */
    public EnumPropertyValue()
    {
        super(InstancePropertyCategory.ENUM);
    }


    /**
     * Copy/clone constructor initializes the enum with the values from the template.
     *
     * @param template - EnumPropertyValue to copy
     */
    public EnumPropertyValue(EnumPropertyValue template)
    {
        super(template);

        if (template != null)
        {
            this.ordinal = template.getOrdinal();
            this.symbolicName = template.getSymbolicName();
            this.description = template.getDescription();
        }
    }


    /**
     * Return the integer ordinal for this enum.
     *
     * @return int ordinal
     */
    public int getOrdinal() { return ordinal; }


    /**
     * Set the integer ordinal for this enum.
     *
     * @param ordinal - int
     */
    public void setOrdinal(int ordinal) { this.ordinal = ordinal; }


    /**
     * Return the symbolic name for this enum value.
     *
     * @return String symbolic name
     */
    public String getSymbolicName() { return symbolicName; }


    /**
     * Set up the symbolic name for this enum value.
     *
     * @param symbolicName - String symbolic name
     */
    public void setSymbolicName(String symbolicName) { this.symbolicName = symbolicName; }


    /**
     * Return the description for this enum.
     *
     * @return String description
     */
    public String getDescription() { return description; }


    /**
     * Set up the description for this enum.
     *
     * @param description - String description
     */
    public void setDescription(String description) { this.description = description; }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "EnumPropertyValue{" +
                "ordinal=" + ordinal +
                ", symbolicName='" + symbolicName + '\'' +
                ", description='" + description + '\'' +
                ", instancePropertyCategory=" + getInstancePropertyCategory() +
                ", typeGUID='" + getTypeGUID() + '\'' +
                ", typeName='" + getTypeName() + '\'' +
                '}';
    }
}

