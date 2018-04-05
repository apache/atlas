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

import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * StructPropertyValue supports the value part of property that is defined as a complex structure.
 * It manages a list of properties that cover the fields in the structure.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class StructPropertyValue extends InstancePropertyValue
{
    private InstanceProperties  attributes = null;


    /**
     * Default constructor set StructProperyValue to null.
     */
    public StructPropertyValue()
    {
        super(InstancePropertyCategory.STRUCT);
    }


    /**
     * Copy/clone constructor sets up the values based on the template.
     *
     * @param template - StructPropertyValue to copy.
     */
    public StructPropertyValue(StructPropertyValue template)
    {
        super(template);

        if (template != null)
        {
            attributes = template.getAttributes();
        }
    }


    /**
     * Return the attributes that make up the fields of the struct.
     *
     * @return attributes - InstanceProperties iterator
     */
    public InstanceProperties getAttributes()
    {
        if (attributes == null)
        {
            return attributes;
        }
        else
        {
            return new InstanceProperties(attributes);
        }
    }

    /**
     * Set up the attributes that make up the fields of the struct.
     *
     * @param attributes - InstanceProperties iterator
     */
    public void setAttributes(InstanceProperties attributes) { this.attributes = attributes; }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "StructPropertyValue{" +
                "attributes=" + attributes +
                ", instancePropertyCategory=" + getInstancePropertyCategory() +
                ", typeGUID='" + getTypeGUID() + '\'' +
                ", typeName='" + getTypeName() + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        StructPropertyValue that = (StructPropertyValue) o;
        return Objects.equals(attributes, that.attributes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(attributes);
    }
}
