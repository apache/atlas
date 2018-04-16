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
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;


/**
 * TypeDefPatch describes a change (patch) to a typeDef's properties, options, external standards mappings or
 * list of valid instance statuses.
 * A patch can be applied to an EntityDef, RelationshipDef or ClassificationDef.
 * Changes to a TypeDef's category or superclasses requires a new type definition.
 * In addition it is not possible to delete an attribute through a patch.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class TypeDefPatch extends TypeDefElementHeader
{
    private TypeDefPatchAction                 action                   = null;
    private String                             typeDefGUID              = null;
    private String                             typeName                 = null;
    private long                               applyToVersion           = 0L;
    private long                               updateToVersion          = 0L;
    private String                             newVersionName           = null;
    private String                             description              = null;
    private String                             descriptionGUID          = null;
    private ArrayList<TypeDefAttribute>        typeDefAttributes        = null;
    private Map<String, String>                typeDefOptions           = null;
    private ArrayList<ExternalStandardMapping> externalStandardMappings = null;
    private ArrayList<InstanceStatus>          validInstanceStatusList  = null;

    private static final long serialVersionUID = 1L;


    /**
     * Default constructor relies on the initialization of variables in their declaration.
     */
    public TypeDefPatch()
    {
    }


    /**
     * Return the type of action that this patch requires.
     *
     * @return TypeDefPatchAction enum
     */
    public TypeDefPatchAction getAction() {
        return action;
    }


    /**
     * Set up the type of action that this patch requires.
     *
     * @param action - TypeDefPatchAction enum
     */
    public void setAction(TypeDefPatchAction action) {
        this.action = action;
    }


    /**
     * Return the unique identifier for the affected TypeDef.
     *
     * @return String guid
     */
    public String getTypeDefGUID()
    {
        return typeDefGUID;
    }


    /**
     * Set up the unique identifier for the affected TypeDef.
     *
     * @param typeDefGUID - String guid
     */
    public void setTypeDefGUID(String typeDefGUID)
    {
        this.typeDefGUID = typeDefGUID;
    }


    /**
     * Return the unique name for the affected TypeDef.
     *
     * @return String name
     */
    public String getTypeName() {
        return typeName;
    }


    /**
     * Set up the unique name for the affected TypeDef.
     *
     * @param typeName - String name
     */
    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }


    /**
     * Return the version number of the TypeDef that this patch applies to.
     *
     * @return long version number
     */
    public long getApplyToVersion() {
        return applyToVersion;
    }


    /**
     * Set up the version number of the TypeDef that this patch applies to.
     *
     * @param applyToVersion - long version number
     */
    public void setApplyToVersion(long applyToVersion) {
        this.applyToVersion = applyToVersion;
    }


    /**
     * Return the new version number of the TypeDef.
     *
     * @return long version number
     */
    public long getUpdateToVersion() {
        return updateToVersion;
    }


    /**
     * Set up the new version of the TypeDef.
     *
     * @param updateToVersion long version number
     */
    public void setUpdateToVersion(long updateToVersion) {
        this.updateToVersion = updateToVersion;
    }


    /**
     * Return the new version name ot use once the patch is applied.
     *
     * @return String version name
     */
    public String getNewVersionName()
    {
        return newVersionName;
    }


    /**
     * Set up the new version name ot use once the patch is applied.
     *
     * @param newVersionName - String version name
     */
    public void setNewVersionName(String newVersionName)
    {
        this.newVersionName = newVersionName;
    }


    /**
     * Return the new description for the TypeDef.
     *
     * @return String description
     */
    public String getDescription()
    {
        return description;
    }


    /**
     * Set up the new description for the TypeDef
     *
     * @param description - String description
     */
    public void setDescription(String description)
    {
        this.description = description;
    }


    /**
     * Return the unique identifier for the new glossary term that describes the TypeDef.
     *
     * @return String unique identifier
     */
    public String getDescriptionGUID()
    {
        return descriptionGUID;
    }


    /**
     * Set up the unique identifier for the new glossary term that describes the TypeDef.
     *
     * @param descriptionGUID - String unique identifier
     */
    public void setDescriptionGUID(String descriptionGUID)
    {
        this.descriptionGUID = descriptionGUID;
    }


    /**
     * Return the list of typeDefAttributes that are either new or changing.
     *
     * @return list of AttributeDefs
     */
    public List<TypeDefAttribute> getTypeDefAttributes()
    {
        if (typeDefAttributes == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(typeDefAttributes);
        }
    }


    /**
     * Set up the list of typeDefAttributes that are either new or changing.
     *
     * @param typeDefAttributes - list of AttributeDefs
     */
    public void setTypeDefAttributes(List<TypeDefAttribute> typeDefAttributes)
    {
        if (typeDefAttributes == null)
        {
            this.typeDefAttributes = null;
        }
        else
        {
            this.typeDefAttributes = new ArrayList<>(typeDefAttributes);
        }
    }


    /**
     * Return the TypeDef options for the patch.
     *
     * @return map of TypeDef Options that are new or changing.
     */
    public Map<String, String> getTypeDefOptions()
    {
        return typeDefOptions;
    }


    /**
     * Set up the TypeDef options for the patch.
     *
     * @param typeDefOptions - map of TypeDef Options that are new or changing.
     */
    public void setTypeDefOptions(Map<String, String> typeDefOptions)
    {
        this.typeDefOptions = typeDefOptions;
    }


    /**
     * Return the list of External Standards Mappings that are either new or changing.
     *
     * @return list of external standards mappings
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
     * Set up the list of External Standards Mappings that are either new or changing.
     *
     * @param externalStandardMappings list of external standards mappings
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
     * Return the list of valid statuses for an instance of this TypeDef.
     *
     * @return list of valid statuses
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
     * Set up the list of valid statuses for an instance of this TypeDef.
     *
     * @param validInstanceStatusList - list of valid statuses
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
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "TypeDefPatch{" +
                "action=" + action +
                ", typeDefGUID='" + typeDefGUID + '\'' +
                ", typeName='" + typeName + '\'' +
                ", applyToVersion=" + applyToVersion +
                ", updateToVersion=" + updateToVersion +
                ", newVersionName='" + newVersionName + '\'' +
                ", typeDefAttributes=" + typeDefAttributes +
                ", typeDefOptions=" + typeDefOptions +
                ", externalStandardMappings=" + externalStandardMappings +
                ", validInstanceStatusList=" + validInstanceStatusList +
                '}';
    }

    /**
     * Validated that the GUID, name and version of a TypeDef are equal.
     *
     * @param object to test
     * @return boolean flag to say object is the same TypeDefPatch
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

        TypeDefPatch that = (TypeDefPatch) object;

        if (applyToVersion != that.applyToVersion)
        {
            return false;
        }
        if (updateToVersion != that.updateToVersion)
        {
            return false;
        }
        if (action != that.action)
        {
            return false;
        }
        if (typeDefGUID != null ? !typeDefGUID.equals(that.typeDefGUID) : that.typeDefGUID != null)
        {
            return false;
        }
        if (typeName != null ? !typeName.equals(that.typeName) : that.typeName != null)
        {
            return false;
        }
        if (typeDefAttributes != null ? !typeDefAttributes.equals(that.typeDefAttributes) : that.typeDefAttributes != null)
        {
            return false;
        }
        if (typeDefOptions != null ? !typeDefOptions.equals(that.typeDefOptions) : that.typeDefOptions != null)
        {
            return false;
        }
        if (externalStandardMappings != null ? !externalStandardMappings.equals(that.externalStandardMappings) : that.externalStandardMappings != null)
        {
            return false;
        }
        return validInstanceStatusList != null ? validInstanceStatusList.equals(that.validInstanceStatusList) : that.validInstanceStatusList == null;
    }

    /**
     * Using the GUID as a hashcode - it should be unique if all connected metadata repositories are behaving properly.
     *
     * @return int hash code
     */
    @Override
    public int hashCode()
    {
        return typeDefGUID != null ? typeDefGUID.hashCode() : 0;
    }
}
