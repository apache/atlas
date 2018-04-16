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
 * EntityDef describes a type of entity in the metadata collection.  It is the simplest TypeDef that adds
 * no other properties beyond TypeDef.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class EntityDef extends TypeDef
{
    /**
     * Minimal constructor - initializes the superclass as an Entity
     */
    public EntityDef()
    {
        super(TypeDefCategory.ENTITY_DEF);
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
    public EntityDef(TypeDefCategory category,
                     String          guid,
                     String          name,
                     long            version,
                     String          versionName)
    {
        super(category, guid, name, version, versionName);
    }


    /**
     * Copy/clone constructor - copies the values from the supplied template.
     *
     * @param template EntityDef
     */
    public EntityDef(EntityDef   template)
    {
        super(template);
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "EntityDef{" +
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
