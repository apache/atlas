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
import org.apache.atlas.omrs.metadatacollection.properties.instances.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The TypeDef is the base class for objects that store the properties of an open metadata type
 * definition (call ed a TypeDef).
 * <p>
 * The different categories of Typedefs are listed in TypeDefCategory.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = EntityDef.class, name = "EntityDef"),
        @JsonSubTypes.Type(value = RelationshipDef.class, name = "RelationshipDef"),
        @JsonSubTypes.Type(value = ClassificationDef.class, name = "ClassificationDef"),
})
public class TypeDef extends TypeDefSummary
{
    protected TypeDefLink                        superType                = null;
    protected String                             description              = null;
    protected String                             descriptionGUID          = null;
    protected String                             origin                   = null;
    protected String                             createdBy                = null;
    protected String                             updatedBy                = null;
    protected Date                               createTime               = null;
    protected Date                               updateTime               = null;
    protected Map<String, String>                options                  = null;
    protected ArrayList<ExternalStandardMapping> externalStandardMappings = null;
    protected ArrayList<InstanceStatus>          validInstanceStatusList  = null;
    protected InstanceStatus                     initialStatus            = null;
    protected ArrayList<TypeDefAttribute>        propertiesDefinition     = null;


    /**
     * Default constructor
     */
    public TypeDef()
    {
    }


    /**
     * Minimal constructor is passed the category of the typedef being constructed.
     * The rest of the properties are null.
     *
     * @param category - TypeDefCategory enum
     */
    public TypeDef(TypeDefCategory category)
    {
        super();
        this.category = category;
    }


    /**
     * Typical constructor is passed the properties of the typedef's super class being constructed.
     *
     * @param category    - category of this TypeDef
     * @param guid        - unique id for the TypeDef
     * @param name        - unique name for the TypeDef
     * @param version     - active version number for the TypeDef
     * @param versionName - name for the active version of the TypeDef
     */
    public TypeDef(TypeDefCategory category,
                   String          guid,
                   String          name,
                   long            version,
                   String          versionName)
    {
        super(category, guid, name, version, versionName);
    }


    /**
     * Copy/clone constructor copies the values from the supplied template.
     *
     * @param template TypeDef
     */
    public TypeDef(TypeDef template)
    {
        super(template);

        if (template != null)
        {
            this.superType = template.getSuperType();
            this.description = template.getDescription();
            this.descriptionGUID = template.getDescriptionGUID();
            this.origin = template.getOrigin();
            this.createdBy = template.getCreatedBy();
            this.updatedBy = template.getUpdatedBy();
            this.createTime = template.getCreateTime();
            this.updateTime = template.getUpdateTime();
            this.options = template.getOptions();
            this.setExternalStandardMappings(template.getExternalStandardMappings());
            this.setValidInstanceStatusList(template.getValidInstanceStatusList());
            this.setPropertiesDefinition(template.getPropertiesDefinition());
        }
    }


    /**
     * Return the super type for the TypeDef (or null if top-level)
     *
     * @return TypeDefLink for the super type
     */
    public TypeDefLink getSuperType()
    {
        return superType;
    }


    /**
     * Set up supertype for the TypeDef.  Only single inheritance is supported.  Use null if this type
     * is top-level.
     *
     * @param superType TypeDefLink for the super type
     */
    public void setSuperType(TypeDefLink superType) { this.superType = superType; }


    /**
     * Return the description of this TypeDef.
     *
     * @return String description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Set up the description of this TypeDef.
     *
     * @param description String
     */
    public void setDescription(String description)
    {
        this.description = description;
    }


    /**
     * Return the unique identifier (guid) of the glossary term that describes this TypeDef.
     *
     * @return String guid
     */
    public String getDescriptionGUID()
    {
        return descriptionGUID;
    }


    /**
     * Set up the unique identifier (guid) of the glossary term that describes this TypeDef.
     *
     * @param descriptionGUID - String guid
     */
    public void setDescriptionGUID(String descriptionGUID)
    {
        this.descriptionGUID = descriptionGUID;
    }


    /**
     * Return the unique identifier for metadata collection Id where this TypeDef came from.
     *
     * @return String guid
     */
    public String getOrigin()
    {
        return origin;
    }


    /**
     * Set up the unique identifier for metadata collection Id where this TypeDef came from.
     *
     * @param origin - String guid
     */
    public void setOrigin(String origin)
    {
        this.origin = origin;
    }


    /**
     * Return the user name of the person that created this TypeDef.
     *
     * @return String name
     */
    public String getCreatedBy()
    {
        return createdBy;
    }


    /**
     * Set up the user name of the person that created this TypeDef.
     *
     * @param createdBy String name
     */
    public void setCreatedBy(String createdBy)
    {
        this.createdBy = createdBy;
    }


    /**
     * Return the user name of the person that last updated this TypeDef.
     *
     * @return String name
     */
    public String getUpdatedBy()
    {
        return updatedBy;
    }


    /**
     * Set up the user name of the person that last updated this TypeDef.
     *
     * @param updatedBy String name
     */
    public void setUpdatedBy(String updatedBy)
    {
        this.updatedBy = updatedBy;
    }


    /**
     * Return the date/time that this TypeDef was created.
     *
     * @return Date
     */
    public Date getCreateTime()
    {
        return createTime;
    }


    /**
     * Set up the date/time that this TypeDef was created.
     *
     * @param createTime Date
     */
    public void setCreateTime(Date createTime)
    {
        this.createTime = createTime;
    }


    /**
     * Return the date/time that this TypeDef was last updated.
     *
     * @return Date
     */
    public Date getUpdateTime()
    {
        return updateTime;
    }


    /**
     * Set up the date/time that this TypeDef was last updated.
     *
     * @param updateTime Date
     */
    public void setUpdateTime(Date updateTime)
    {
        this.updateTime = updateTime;
    }


    /**
     * Return the options for this TypeDef. These are private properties used by the processors of this TypeDef
     * and ignored by the OMRS.
     *
     * @return Map from String to String
     */
    public Map<String, String> getOptions()
    {
        return options;
    }


    /**
     * Set up the options for this TypeDef.  These are private properties used by the processors of this TypeDef
     * and ignored by the OMRS.
     *
     * @param options - Map from String to String
     */
    public void setOptions(Map<String, String> options)
    {
        this.options = options;
    }


    /**
     * Return the list of mappings to external standards.
     *
     * @return ExternalStandardMappings list
     */
    public List<ExternalStandardMapping> getExternalStandardMappings()
    {
        if (externalStandardMappings == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(externalStandardMappings);
        }
    }


    /**
     * Set up the list of mappings to external standards.
     *
     * @param externalStandardMappings - ExternalStandardMappings list
     */
    public void setExternalStandardMappings(List<ExternalStandardMapping> externalStandardMappings)
    {
        if (externalStandardMappings == null)
        {
            this.externalStandardMappings = null;
        }
        else
        {
            this.externalStandardMappings = new ArrayList<>(externalStandardMappings);
        }
    }


    /**
     * Return the list of valid instance statuses supported by this TypeDef.
     *
     * @return InstanceStatus array of supported status values.
     */
    public List<InstanceStatus> getValidInstanceStatusList()
    {
        if (validInstanceStatusList == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(validInstanceStatusList);
        }
    }


    /**
     * Set up the list of valid instance statuses supported by this TypeDef.
     *
     * @param validInstanceStatusList - InstanceStatus Array
     */
    public void setValidInstanceStatusList(List<InstanceStatus> validInstanceStatusList)
    {
        if (validInstanceStatusList == null)
        {
            this.validInstanceStatusList = null;
        }
        else
        {
            this.validInstanceStatusList = new ArrayList<>(validInstanceStatusList);
        }
    }


    /**
     * Return the initial status setting for an instance of this type.
     *
     * @return InstanceStatus enum
     */
    public InstanceStatus getInitialStatus()
    {
        return initialStatus;
    }


    /**
     * Set up the initial status setting for an instance of this type.
     *
     * @param initialStatus - InstanceStatus enum
     */
    public void setInitialStatus(InstanceStatus initialStatus)
    {
        this.initialStatus = initialStatus;
    }


    /**
     * Return the list of AttributeDefs that define the valid properties for this type of classification.
     *
     * @return AttributeDefs list
     */
    public List<TypeDefAttribute> getPropertiesDefinition()
    {
        if (propertiesDefinition == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(propertiesDefinition);
        }
    }


    /**
     * Set up the list of AttributeDefs that define the valid properties for this type of classification.
     *
     * @param propertiesDefinition - AttributeDefs list
     */
    public void setPropertiesDefinition(List<TypeDefAttribute> propertiesDefinition)
    {
        if (propertiesDefinition == null)
        {
            this.propertiesDefinition = null;
        }
        else
        {
            this.propertiesDefinition = new ArrayList<>(propertiesDefinition);
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
        return "TypeDef{" +
                "superType=" + superType +
                ", description='" + description + '\'' +
                ", descriptionGUID='" + descriptionGUID + '\'' +
                ", origin='" + origin + '\'' +
                ", createdBy='" + createdBy + '\'' +
                ", updatedBy='" + updatedBy + '\'' +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                ", options=" + options +
                ", externalStandardMappings=" + externalStandardMappings +
                ", validInstanceStatusList=" + validInstanceStatusList +
                ", initialStatus=" + initialStatus +
                ", propertiesDefinition=" + propertiesDefinition +
                ", version=" + version +
                ", versionName='" + versionName + '\'' +
                ", category=" + category +
                ", guid='" + guid + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
