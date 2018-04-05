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

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * CollectionDef supports the definition of a collection type.  This information about the generic
 * collection type is managed in the CollectionDefCategory.  It is instantiated with specific primitive
 * types when it is linked to a specific TypeDefAttribute.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class CollectionDef extends AttributeTypeDef
{
    private CollectionDefCategory           collectionDefCategory = null;
    private int                             argumentCount         = 0;
    private ArrayList<PrimitiveDefCategory> argumentTypes         = null;


    /**
     * Typical constructor initializes the CollectionDef based on the supplied category.
     *
     * @param collectionDefCategory - CollectionDefCategory Enum
     */
    public CollectionDef(CollectionDefCategory  collectionDefCategory)
    {
        super(AttributeTypeDefCategory.COLLECTION);

        this.collectionDefCategory = collectionDefCategory;
        this.argumentCount = collectionDefCategory.getArgumentCount();
        this.argumentTypes = new ArrayList<>();

        /*
         * Set up the type of the elements stored in the collection as "unknown" - like an initialized Java generic.
         */
        for (int i=0; i<argumentCount; i++)
        {
            argumentTypes.add(PrimitiveDefCategory.OM_PRIMITIVE_TYPE_UNKNOWN);
        }
    }


    /**
     * Copy/clone constructor creates a copy of the supplied template.
     *
     * @param template CollectionDef to copy
     */
    public CollectionDef(CollectionDef template)
    {
        super(template);

        if (template != null)
        {
            this.collectionDefCategory = template.getCollectionDefCategory();
            this.argumentCount = template.getArgumentCount();
            this.setArgumentTypes(template.getArgumentTypes());
        }
    }


    /**
     * Return the type category for this collection type.
     *
     * @return CollectionDefCategory Enum
     */
    public CollectionDefCategory getCollectionDefCategory() { return collectionDefCategory; }


    /**
     * Return the number of arguments needed to set up the collection type name.
     *
     * @return int count
     */
    public int getArgumentCount()
    {
        return argumentCount;
    }


    /**
     * Return the list of argument types set up for this collection.
     *
     * @return list of argument type
     */
    public List<PrimitiveDefCategory> getArgumentTypes()
    {
        if (argumentTypes == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(argumentTypes);
        }
    }


    /**
     * Set up the list of argument types.
     *
     * @param argumentTypes - list of argument types
     */
    public void setArgumentTypes(List<PrimitiveDefCategory> argumentTypes)
    {
        if (argumentTypes == null)
        {
            this.argumentTypes = null;
        }
        else
        {
            this.argumentTypes = new ArrayList<>(argumentTypes);
        }
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "CollectionDef{" +
                "collectionDefCategory=" + collectionDefCategory +
                ", argumentCount=" + argumentCount +
                ", argumentTypes=" + argumentTypes +
                ", category=" + category +
                ", guid='" + guid + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", descriptionGUID='" + descriptionGUID + '\'' +
                '}';
    }
}
