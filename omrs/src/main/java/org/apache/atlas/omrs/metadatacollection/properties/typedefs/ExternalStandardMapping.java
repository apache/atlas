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
 * ExternalStandardMapping defines a mapping of TypeDefs and TypeDefAttributes to an external standard.  It includes the name
 * of the standard, the organization that owns the standard and the equivalent type in the external standard.
 * This mapping is done on a property type by property type basis.  The aim is to create clarity on the meaning
 * of the open metadata types and support importers and exporters between open metadata types and external standards.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class ExternalStandardMapping extends TypeDefElementHeader
{
    private   String standardName = null;
    private   String standardOrganization = null;
    private   String standardTypeName = null;


    /**
     * Default Constructor - initializes to null.
     */
    public ExternalStandardMapping()
    {
        /*
         * Initialize superclass.
         */
        super();
    }


    /**
     * Copy/clone constructor - copies values from supplied template.
     *
     * @param templateElement - template to copy.
     */
    public ExternalStandardMapping(ExternalStandardMapping  templateElement)
    {
        /*
         * Initialize superclass.
         */
        super(templateElement);

        /*
         * Copy the template values over.
         */
        this.standardName = templateElement.getStandardName();
        this.standardOrganization = templateElement.getStandardOrganization();
        this.standardTypeName = templateElement.getStandardTypeName();
    }


    /**
     * Return the name of the standard that this mapping relates to.
     *
     * @return String standard name
     */
    public String getStandardName() {
        return standardName;
    }


    /**
     * Set up the name of the standard that this mapping relates to.
     *
     * @param standardName - String standard name
     */
    public void setStandardName(String standardName) {
        this.standardName = standardName;
    }


    /**
     * Return the name of organization that owns the standard that this mapping refers to.
     *
     * @return String organization name
     */
    public String getStandardOrganization() {
        return standardOrganization;
    }


    /**
     * Set up the name of the organization that owns the standard that this mapping refers to.
     *
     * @param standardOrganization - String organization name
     */
    public void setStandardOrganization(String standardOrganization)
    {
        this.standardOrganization = standardOrganization;
    }


    /**
     * Return the name of the type from the standard that is equivalent to the linked open metadata type.
     *
     * @return String type name from standard
     */
    public String getStandardTypeName() {
        return standardTypeName;
    }


    /**
     * Set up the name of the type from the standard that is equivalent to the linked open metadata type.
     *
     * @param standardTypeName - String type name from standard
     */
    public void setStandardTypeName(String standardTypeName) {
        this.standardTypeName = standardTypeName;
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "ExternalStandardMapping{" +
                "standardName='" + standardName + '\'' +
                ", standardOrganization='" + standardOrganization + '\'' +
                ", standardTypeName='" + standardTypeName + '\'' +
                '}';
    }
}
