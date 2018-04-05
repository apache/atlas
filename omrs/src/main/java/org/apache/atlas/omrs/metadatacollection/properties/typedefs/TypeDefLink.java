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

import java.util.Objects;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * The TypeDefHolds holds basic identifying information used to link one TypeDef to another.  It is used in
 * the definition of types - ie in the TypeDefs themselves.  Examples include linking a classification to an
 * entity, identifying super types and defining the entities at either end of a relationship.
 * <p>
 *     TypeDefs are identified using both the guid and the type name.  Both should be unique and most processing is
 *     with the type name because that is easiest for people to work with.  The guid provides a means to check the
 *     identity of the types since it is easy to introduce two types with the same name in the distributed model.
 * </p>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class TypeDefLink extends TypeDefElementHeader
{
    protected  String                   guid = null;
    protected  String                   name = null;


    /**
     * Default constructor
     */
    public TypeDefLink()
    {
        super();
    }


    /**
     * Typical constructor is passed the unique identifier and name of the typedef being constructed.
     *
     * @param guid - unique id for the TypeDef
     * @param name - unique name for the TypeDef
     */
    public TypeDefLink(String            guid,
                       String            name)
    {
        super();

        this.guid = guid;
        this.name = name;
    }


    /**
     * Copy/clone constructor copies the values from the supplied template.
     *
     * @param template TypeDefSummary
     */
    public TypeDefLink(TypeDefLink template)
    {
        super(template);

        if (template != null)
        {
            this.guid = template.getGUID();
            this.name = template.getName();
        }
    }


    /**
     * Return the unique identifier for this TypeDef.
     *
     * @return String guid
     */
    public String getGUID() {
        return guid;
    }


    /**
     * Set up the unique identifier for this TypeDef.
     *
     * @param guid - String guid
     */
    public void setGUID(String guid)
    {
        this.guid = guid;
    }


    /**
     * Return the type name for this TypeDef.  In simple environments, the type name is unique but where metadata
     * repositories from different vendors are in operation it is possible that 2 types may have a name clash.  The
     * GUID is the reliable unique identifier.
     *
     * @return String name
     */
    public String getName() {
        return name;
    }


    /**
     * Set up the type name for this TypeDef.  In simple environments, the type name is unique but where metadata
     * repositories from different vendors are in operation it is possible that 2 types may have a name clash.  The
     * GUID is the reliable unique identifier.
     *
     * @param name - String name
     */
    public void setName(String name)
    {
        this.name = name;
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "TypeDefSummary{" +
                ", guid='" + guid + '\'' +
                ", name='" + name + '\'' +
                '}';
    }


    /**
     * Validated that the GUID, name and version number of a TypeDef are equal.
     *
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
        TypeDefLink that = (TypeDefLink) object;
        return Objects.equals(guid, that.guid) &&
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
