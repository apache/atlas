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


import java.util.ArrayList;

/**
 * ClassificationDef stores the properties for the definition of a type of classification.  Many of the properties
 * are inherited from TypeDef.  ClassificationDef adds a list of Entity Types that this Classification can be
 * connected to and a boolean to indicate if this classification is propagatable.
 */
public class ClassificationDef extends TypeDef
{
    private   ArrayList<TypeDefLink>     validEntityDefs = null;
    private   boolean                    propagatable = false;


    /**
     * Minimal constructor - sets up an empty ClassificationDef.
     */
    public ClassificationDef()
    {
        super(TypeDefCategory.CLASSIFICATION_DEF);
    }


    /**
     * Typical constructor is passed the properties of the typedef's super class being constructed.
     *
     * @param category    - category of this TypeDef
     * @param guid        - unique id for the TypeDef
     * @param name        - unique name for the TypeDef
     * @param version     - active version number for the TypeDef
     * @param versionName - unique name for the TypeDef
     */
    public ClassificationDef(TypeDefCategory category,
                             String          guid,
                             String          name,
                             long            version,
                             String          versionName)
    {
        super(category, guid, name, version, versionName);
    }


    /**
     * Copy/clone constructor copies values from the supplied template.
     *
     * @param template - template to copy
     */
    public ClassificationDef(ClassificationDef   template)
    {
        super(template);

        if (template != null)
        {
            validEntityDefs = template.getValidEntityDefs();
            propagatable = template.isPropagatable();
        }
    }


    /**
     * Return the list of identifiers for the types of entities that this type of Classification can be connected to.
     *
     * @return List of entity type identifiers
     */
    public ArrayList<TypeDefLink> getValidEntityDefs()
    {
        if (validEntityDefs == null)
        {
            return validEntityDefs;
        }
        else
        {
            return new ArrayList<>(validEntityDefs);
        }
    }


    /**
     * Set up the list of identifiers for the types of entities that this type of Classification can be connected to.
     *
     * @param validEntityDefs - List of entity type identifiers
     */
    public void setValidEntityDefs(ArrayList<TypeDefLink> validEntityDefs)
    {
        this.validEntityDefs = validEntityDefs;
    }


    /**
     * Return whether this classification should propagate to other entities if the relationship linking them
     * allows classification propagation.
     *
     * @return boolean flag
     */
    public boolean isPropagatable()
    {
        return propagatable;
    }


    /**
     * Sets up whether this classification should propagate to other entities if the relationship linking them
     * allows classification propagation.
     *
     * @param propagatable - boolean flag
     */
    public void setPropagatable(boolean propagatable)
    {
        this.propagatable = propagatable;
    }

    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "ClassificationDef{" +
                "validEntityDefs=" + validEntityDefs +
                ", propagatable=" + propagatable +
                ", superType=" + superType +
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
