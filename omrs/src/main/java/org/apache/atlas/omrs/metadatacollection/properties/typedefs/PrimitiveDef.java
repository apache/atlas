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
 * PrimitiveDef supports the definition of a primitive type.  This information is managed in the
 * PrimitiveDefCategory.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class PrimitiveDef extends AttributeTypeDef
{
    private  PrimitiveDefCategory   primitiveDefCategory = null;


    /**
     * Default constructor initializes the PrimitiveDef based on the supplied category.
     *
     * @param primitiveDefCategory - PrimitiveDefCategory Enum
     */
    public PrimitiveDef(PrimitiveDefCategory  primitiveDefCategory)
    {
        super(AttributeTypeDefCategory.PRIMITIVE);

        this.primitiveDefCategory = primitiveDefCategory;
    }


    /**
     * Copy/clone constructor creates a copy of the supplied template.
     *
     * @param template PrimitiveDef to copy
     */
    public PrimitiveDef(PrimitiveDef template)
    {
        super(template);

        if (template != null)
        {
            this.primitiveDefCategory = template.getPrimitiveDefCategory();
        }
    }


    /**
     * Return the type category for this primitive type.
     *
     * @return PrimitiveDefCategory Enum
     */
    public PrimitiveDefCategory getPrimitiveDefCategory() { return primitiveDefCategory; }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "PrimitiveDef{" +
                "primitiveDefCategory=" + primitiveDefCategory +
                ", category=" + category +
                ", guid='" + guid + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", descriptionGUID='" + descriptionGUID + '\'' +
                '}';
    }
}
