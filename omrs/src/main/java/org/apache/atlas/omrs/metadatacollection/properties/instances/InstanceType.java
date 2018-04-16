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
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefCategory;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefLink;

import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * InstanceType contains information from the instance's TypeDef that are useful for processing the instance.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class InstanceType extends InstanceElementHeader
{
    private TypeDefCategory           typeDefCategory         = TypeDefCategory.UNKNOWN_DEF;
    private String                    typeDefGUID             = null;
    private String                    typeDefName             = null;
    private long                      typeDefVersion          = 0L;
    private String                    typeDefDescription      = null;
    private String                    typeDefDescriptionGUID  = null;
    private List<TypeDefLink>         typeDefSuperTypes       = null;
    private List<InstanceStatus>      validStatusList         = null;
    private List<String>              validInstanceProperties = null;

    /**
     * Default constructor relies on initialization of variables in the declaration.
     */
    public InstanceType()
    {
    }


    /**
     * Typical constructor that set all of the properties at once.
     *
     * @param typeDefCategory - the category of the type
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @param typeDefVersion - version number of the type
     * @param typeDefDescription - short description of the type
     * @param typeDefDescriptionGUID - unique identifier of the glossary term describing this type.
     * @param typeDefSuperTypes - full list of super types for this type
     * @param validStatusList - list of statuses that this instance can have
     * @param validInstanceProperties - full list of valid property names that can be put in the instance (including
     *                                properties from the super types)
     */
    public InstanceType(TypeDefCategory           typeDefCategory,
                        String                    typeDefGUID,
                        String                    typeDefName,
                        long                      typeDefVersion,
                        String                    typeDefDescription,
                        String                    typeDefDescriptionGUID,
                        List<TypeDefLink>         typeDefSuperTypes,
                        List<InstanceStatus>      validStatusList,
                        List<String>              validInstanceProperties)
    {
        this.typeDefCategory = typeDefCategory;
        this.typeDefGUID = typeDefGUID;
        this.typeDefName = typeDefName;
        this.typeDefVersion = typeDefVersion;
        this.typeDefDescription = typeDefDescription;
        this.typeDefDescriptionGUID = typeDefDescriptionGUID;
        this.setTypeDefSuperTypes(typeDefSuperTypes);
        this.setValidStatusList(validStatusList);
        this.setValidInstanceProperties(validInstanceProperties);
    }


    /**
     * Copy/clone constructor
     *
     * @param template - instance type to copy
     */
    public InstanceType(InstanceType    template)
    {
        if (template != null)
        {
            typeDefCategory = template.getTypeDefCategory();
            typeDefGUID = template.getTypeDefGUID();
            typeDefName = template.getTypeDefName();
            typeDefVersion = template.getTypeDefVersion();
            typeDefDescription = template.getTypeDefDescription();
            typeDefDescriptionGUID = template.getTypeDefDescriptionGUID();
            setTypeDefSuperTypes(template.getTypeDefSuperTypes());
            setValidStatusList(template.getValidStatusList());
            setValidInstanceProperties(template.getValidInstanceProperties());
        }
    }


    /**
     * Return the category of this instance.  This defines the category of the TypeDef that determines its properties.
     *
     * @return TypeDefCategory enum
     */
    public TypeDefCategory getTypeDefCategory() { return typeDefCategory; }


    /**
     * Set up the category of this instance.  This defines the category of the TypeDef that determines its properties.
     *
     * @param typeDefCategory enum
     */
    public void setTypeDefCategory(TypeDefCategory typeDefCategory)
    {
        this.typeDefCategory = typeDefCategory;
    }

    /**
     * Return the unique identifier for the type of this instance.
     *
     * @return String unique identifier
     */
    public String getTypeDefGUID() { return typeDefGUID; }


    /**
     * Set up the unique identifier for the type of this instance.
     *
     * @param typeDefGUID - String unique identifier
     */
    public void setTypeDefGUID(String typeDefGUID) { this.typeDefGUID = typeDefGUID; }


    /**
     * Return the name of this instance's type.
     *
     * @return String type name
     */
    public String getTypeDefName() { return typeDefName; }


    /**
     * Set up the name of this instance's type.
     *
     * @param typeDefName - String type name
     */
    public void setTypeDefName(String typeDefName) { this.typeDefName = typeDefName; }


    /**
     * Return the version number of this instance's TypeDef.
     *
     * @return long version number
     */
    public long getTypeDefVersion()
    {
        return typeDefVersion;
    }


    /**
     * Set up the version for the TypeDef.
     *
     * @param typeDefVersion - long version number
     */
    public void setTypeDefVersion(long typeDefVersion)
    {
        this.typeDefVersion = typeDefVersion;
    }


    /**
     * Return the full list of defined super-types for this TypeDef working up the type hierarchy.
     *
     * @return list of types
     */
    public List<TypeDefLink> getTypeDefSuperTypes()
    {
        if (typeDefSuperTypes == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(typeDefSuperTypes);
        }
    }


    /**
     * Set up the full list of defined super-types for this TypeDef working up the type hierarchy.
     *
     * @param typeDefSuperTypes - list of type names
     */
    public void setTypeDefSuperTypes(List<TypeDefLink> typeDefSuperTypes)
    {
        if (typeDefSuperTypes == null)
        {
            this.typeDefSuperTypes = null;
        }
        else
        {
            this.typeDefSuperTypes = new ArrayList<>(typeDefSuperTypes);
        }
    }


    /**
     * Return the description for the TypeDef.
     *
     * @return - String description
     */
    public String getTypeDefDescription()
    {
        return typeDefDescription;
    }


    /**
     * Set up the description for the TypeDef.
     *
     * @param typeDefDescription - String description
     */
    public void setTypeDefDescription(String typeDefDescription)
    {
        this.typeDefDescription = typeDefDescription;
    }


    /**
     * Return the unique identifier of the glossary term that describes this TypeDef (null if no term defined).
     *
     * @return String unique identifier
     */
    public String getTypeDefDescriptionGUID()
    {
        return typeDefDescriptionGUID;
    }


    /**
     * Set up the unique identifier of the glossary term that describes this TypeDef (null if no term defined).
     *
     * @param typeDefDescriptionGUID - String unique identifier
     */
    public void setTypeDefDescriptionGUID(String typeDefDescriptionGUID)
    {
        this.typeDefDescriptionGUID = typeDefDescriptionGUID;
    }


    /**
     * Return the list of valid instance statuses supported by this instance.
     *
     * @return InstanceStatus array of supported status.
     */
    public List<InstanceStatus> getValidStatusList()
    {
        if ( validStatusList == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(validStatusList);
        }
    }


    /**
     * Set up the list of valid instance statuses supported by this instance.
     *
     * @param validStatusList - InstanceStatus Array
     */
    public void setValidStatusList(List<InstanceStatus> validStatusList)
    {
        if (validStatusList == null)
        {
            this.validStatusList = null;
        }
        else
        {
            this.validStatusList = new ArrayList<>(validStatusList);
        }
    }


    /**
     * Return the list of valid property names that can be stored in this instance.
     *
     * @return array of property names.
     */
    public List<String> getValidInstanceProperties()
    {
        if (validInstanceProperties == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(validInstanceProperties);
        }
    }


    /**
     * Set up the set of valid property names that can be stored in this instance.
     *
     * @param validInstanceProperties - array of property names.
     */
    public void setValidInstanceProperties(List<String> validInstanceProperties)
    {
        if (validInstanceProperties == null)
        {
            this.validInstanceProperties = null;
        }
        else
        {
            this.validInstanceProperties = new ArrayList<>(validInstanceProperties);
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
        return "InstanceType{" +
                "typeDefCategory=" + typeDefCategory +
                ", typeDefGUID='" + typeDefGUID + '\'' +
                ", typeDefName='" + typeDefName + '\'' +
                ", typeDefVersion=" + typeDefVersion +
                ", typeDefDescription='" + typeDefDescription + '\'' +
                ", typeDefDescriptionGUID='" + typeDefDescriptionGUID + '\'' +
                ", typeDefSuperTypes=" + typeDefSuperTypes +
                ", validStatusList=" + validStatusList +
                ", validInstanceProperties=" + validInstanceProperties +
                '}';
    }
}
