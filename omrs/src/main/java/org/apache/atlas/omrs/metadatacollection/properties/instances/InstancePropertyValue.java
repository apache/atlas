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

import com.fasterxml.jackson.annotation.*;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * InstancePropertyValue provides a common class for holding an instance type and value.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ArrayPropertyValue.class, name = "ArrayPropertyValue"),
        @JsonSubTypes.Type(value = EnumPropertyValue.class, name = "EnumPropertyValue"),
        @JsonSubTypes.Type(value = MapPropertyValue.class, name = "MapPropertyValue"),
        @JsonSubTypes.Type(value = PrimitivePropertyValue.class, name = "PrimitivePropertyValue"),
        @JsonSubTypes.Type(value = StructPropertyValue.class, name = "StructPropertyValue")
})
public class InstancePropertyValue extends InstanceElementHeader
{
    /*
     * Common type information - this is augmented by the subclasses
     */
    private   InstancePropertyCategory   instancePropertyCategory = InstancePropertyCategory.UNKNOWN;
    private   String                     typeGUID  = null;
    private   String                     typeName  = null;


    /**
     * Default constructor for Jackson
     */
    public InstancePropertyValue()
    {
    }

    /**
     * Typical constructor initializes the instance property value to nulls.
     *
     * @param instancePropertyCategory - InstancePropertyCategory Enum
     */
    public InstancePropertyValue(InstancePropertyCategory   instancePropertyCategory)
    {
        super();
        this.instancePropertyCategory = instancePropertyCategory;
    }


    /**
     * Copy/clone constructor - initializes the instance property value from the supplied template.
     *
     * @param template InstancePropertyValue
     */
    public InstancePropertyValue(InstancePropertyValue  template)
    {
        super(template);

        if (template != null)
        {
            this.instancePropertyCategory = template.getInstancePropertyCategory();
            this.typeGUID = template.getTypeGUID();
            this.typeName = template.getTypeName();
        }
    }


    /**
     * Return the category of this instance property's type.
     *
     * @return TypeDefCategory enum value
     */
    public InstancePropertyCategory getInstancePropertyCategory() { return instancePropertyCategory; }


    /**
     * Return the unique GUID for the type.
     *
     * @return String unique identifier
     */
    public String getTypeGUID() { return typeGUID; }


    /**
     * Set up the unique GUID of the type.
     *
     * @param typeGUID - String unique identifier
     */
    public void setTypeGUID(String typeGUID) { this.typeGUID = typeGUID; }


    /**
     * Return the name of the type.
     *
     * @return String type name
     */
    public String getTypeName() { return typeName; }


    /**
     * Set up the name of the type.
     *
     * @param typeName - String type name
     */
    public void setTypeName(String typeName) { this.typeName = typeName; }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "InstancePropertyValue{" +
                "instancePropertyCategory=" + instancePropertyCategory +
                ", typeGUID='" + typeGUID + '\'' +
                ", typeName='" + typeName + '\'' +
                '}';
    }
}
