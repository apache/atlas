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


import com.fasterxml.jackson.annotation.*;

import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The AttributeTypeDef class is used to identify the type of an attribute.  These can be:
 * <ul>
 *     <li>PrimitiveDef</li>
 *     <li>CollectionDef</li>
 *     <li>EnumDef</li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PrimitiveDef.class, name = "PrimitiveDef"),
        @JsonSubTypes.Type(value = CollectionDef.class, name = "CollectionDef"),
        @JsonSubTypes.Type(value = EnumDef.class, name = "EnumDef"),
})
public class AttributeTypeDef extends TypeDefElementHeader
{
    protected long                     version         = 0L;
    protected String                   versionName     = null;
    protected AttributeTypeDefCategory category        = AttributeTypeDefCategory.UNKNOWN_DEF;
    protected String                   guid            = null;
    protected String                   name            = null;
    protected String                   description     = null;
    protected String                   descriptionGUID = null;


    /**
     * Default constructor
     */
    public AttributeTypeDef()
    {
    }


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
            this.version = template.getVersion();
            this.versionName = template.getVersionName();
            this.category = template.getCategory();
            this.guid = template.getGUID();
            this.name = template.getName();
            this.description = template.getDescription();
            this.descriptionGUID = template.getDescriptionGUID();
        }
    }


    /**
     * Return the version of the AttributeTypeDef.  Versions are created when an AttributeTypeDef's properties
     * are changed.  If a description is updated, then this does not create a new version.
     *
     * @return String version number
     */
    public long getVersion()
    {
        return version;
    }


    /**
     * Set up the version of the AttributeTypeDef.  Versions are created when an AttributeTypeDef's properties
     * are changed.  If a description is updated, then this does not create a new version.
     *
     * @param version - long version number
     */
    public void setVersion(long version)
    {
        this.version = version;
    }


    /**
     * Return the version name, which is a more of a human readable form of the version number.
     * It can be used to show whether the change is a minor or major update.
     *
     * @return String version name
     */
    public String getVersionName()
    {
        return versionName;
    }


    /**
     * Set up the version name, which is a more of a human readable form of the version number.
     * It can be used to show whether the change is a minor or major update.
     *
     * @param versionName - String version name
     */
    public void setVersionName(String versionName)
    {
        this.versionName = versionName;
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


    /**
     * Return the short description of this AttributeTypeDef.
     *
     * @return - String description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Set up the short description of this AttributeTypeDef.
     *
     * @param description - String description
     */
    public void setDescription(String description)
    {
        this.description = description;
    }


    /**
     * Return the unique identifier of the glossary term that describes this AttributeTypeDef.  Null means there
     * is no known glossary term.
     *
     * @return String guid
     */
    public String getDescriptionGUID()
    {
        return descriptionGUID;
    }


    /**
     * Set up the unique identifier of the glossary term that describes this AttributeTypeDef.  Null means there
     * is no known glossary term.
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
        return "AttributeTypeDef{" +
                "version=" + version +
                ", versionName='" + versionName + '\'' +
                ", category=" + category +
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
        return version == that.version &&
                Objects.equals(versionName, that.versionName) &&
                category == that.category &&
                Objects.equals(guid, that.guid) &&
                Objects.equals(name, that.name) &&
                Objects.equals(description, that.description) &&
                Objects.equals(descriptionGUID, that.descriptionGUID);
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

