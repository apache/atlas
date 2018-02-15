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


import java.util.Objects;

/**
 * The AttributeTypeDef class is used to identify the type of an attribute.  These can be:
 * <ul>
 *     <li>PrimitiveDef</li>
 *     <li>CollectionDef</li>
 *     <li>EnumDef</li>
 * </ul>
 */
public abstract class AttributeTypeDef extends TypeDefElementHeader
{
    protected AttributeTypeDefCategory category        = AttributeTypeDefCategory.UNKNOWN_DEF;
    protected String                   guid            = null;
    protected String                   name            = null;
    protected String                   description     = null;
    protected String                   descriptionGUID = null;


    /**
     * Minimal constructor is passed the category of the attribute type
     *
     * @param category - category of this TypeDef
     */
    public AttributeTypeDef(AttributeTypeDefCategory   category)
    {
        this.category = category;
    }


    /**
     * Typical constructor is passed the values that describe the type.
     *
     * @param category - category of this TypeDef
     * @param guid - unique id for the TypeDef
     * @param name - unique name for the TypeDef
     */
    public AttributeTypeDef(AttributeTypeDefCategory   category,
                            String                     guid,
                            String                     name)
    {
        super();

        this.category = category;
        this.guid = guid;
        this.name = name;
    }


    /**
     * Copy/clone constructor copies the values from the supplied template.
     *
     * @param template AttributeTypeDef
     */
    public AttributeTypeDef(AttributeTypeDef template)
    {
        super(template);

        if (template != null)
        {
            this.category = template.getCategory();
            this.guid = template.getGUID();
            this.name = template.getName();
            this.description = template.getDescription();
            this.descriptionGUID = template.getDescriptionGUID();
        }
    }


    /**
     * Return the category of the TypeDef.
     *
     * @return AttributeTypeDefCategory enum
     */
    public AttributeTypeDefCategory getCategory() { return category; }


    /**
     * Set up the category of the TypeDef.
     *
     * @param category - AttributeTypeDefCategory enum
     */
    public void setCategory(AttributeTypeDefCategory category) { this.category = category; }


    /**
     * Return the unique identifier for this TypeDef.
     *
     * @return String guid
     */
    public String getGUID() { return guid; }


    /**
     * Set up the unique identifier for this TypeDef.
     *
     * @param guid - String guid
     */
    public void setGUID(String guid) { this.guid = guid; }


    /**
     * Return the type name for this TypeDef.  In simple environments, the type name is unique but where metadata
     * repositories from different vendors are in operation it is possible that 2 types may have a name clash.  The
     * GUID is the reliable unique identifier.
     *
     * @return String name
     */
    public String getName() { return name; }


    /**
     * Set up the type name for this TypeDef.  In simple environments, the type name is unique but where metadata
     * repositories from different vendors are in operation it is possible that 2 types may have a name clash.  The
     * GUID is the reliable unique identifier.
     *
     * @param name - String name
     */
    public void setName(String name) { this.name = name; }


    public String getDescription()
    {
        return description;
    }

    public void setDescription(String description)
    {
        this.description = description;
    }

    public String getDescriptionGUID()
    {
        return descriptionGUID;
    }

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
        return "AttributeTypeDef{" +
                "category=" + category +
                ", guid='" + guid + '\'' +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", descriptionGUID='" + descriptionGUID + '\'' +
                '}';
    }


    /**
     * Validated that the GUID, name and version number of a TypeDef are equal.
     *
     * @param object to test
     * @return boolean flag to say object is the same TypeDefSummary
     */
    @Override
    public boolean equals(Object object)
    {
        if (this == object)
        {
            return true;
        }
        if (object == null || getClass() != object.getClass())
        {
            return false;
        }
        AttributeTypeDef that = (AttributeTypeDef) object;
        return category == that.category &&
                Objects.equals(guid, that.guid) &&
                Objects.equals(name, that.name);
    }


    /**
     * Using the GUID as a hashcode - it should be unique if all connected metadata repositories are behaving properly.
     *
     * @return int hash code
     */
    @Override
    public int hashCode()
    {
        return guid != null ? guid.hashCode() : 0;
    }
}

