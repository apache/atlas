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
package org.apache.atlas.omrs.archivemanager.properties;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * OpenMetadataArchiveProperties defines the properties of an open metadata archive.  This includes the following
 * properties:
 * <ul>
 *     <li>
 *         Unique identifier (GUID) of the archive.
 *     </li>
 *     <li>
 *         Archive name.
 *     </li>
 *     <li>
 *         Archive description.
 *     </li>
 *     <li>
 *         Archive Type (CONTENT_PACK or METADATA_EXPORT).
 *     </li>
 *     <li>
 *         Originator name (organization or person).
 *     </li>
 *     <li>
 *         Creation date
 *     </li>
 *     <li>
 *         GUIDs for archives that this archive depends on.
 *     </li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class OpenMetadataArchiveProperties
{
    private String                  archiveGUID            = null;
    private String                  archiveName            = null;
    private String                  archiveDescription     = null;
    private OpenMetadataArchiveType archiveType            = null;
    private String                  originatorName         = null;
    private String                  originatorOrganization = null;
    private Date                    creationDate           = null;
    private ArrayList<String>       dependsOnArchives      = null;


    /**
     * Default constructor that relies on initialization of variables in their declaration.
     */
    public OpenMetadataArchiveProperties()
    {
    }


    /**
     * Return the unique identifier for this archive.
     *
     * @return String guid
     */
    public String getArchiveGUID()
    {
        return archiveGUID;
    }


    /**
     * Set up the unique identifier for this open metadata archive.
     *
     * @param archiveGUID - String guid
     */
    public void setArchiveGUID(String archiveGUID)
    {
        this.archiveGUID = archiveGUID;
    }


    /**
     * Return the descriptive name for this open metadata archive.
     *
     * @return String name
     */
    public String getArchiveName()
    {
        return archiveName;
    }


    /**
     * Set up the descriptive name for this open metadata archive.
     *
     * @param archiveName - String name
     */
    public void setArchiveName(String archiveName)
    {
        this.archiveName = archiveName;
    }


    /**
     * Return the description for this open metadata archive.
     *
     * @return String description
     */
    public String getArchiveDescription()
    {
        return archiveDescription;
    }


    /**
     * Set up the description for this open metadata archive.
     *
     * @param archiveDescription - String description
     */
    public void setArchiveDescription(String archiveDescription)
    {
        this.archiveDescription = archiveDescription;
    }


    /**
     * Return the type of this open metadata archive.
     *
     * @return OpenMetadataArchiveType enum
     */
    public OpenMetadataArchiveType getArchiveType()
    {
        return archiveType;
    }


    /**
     * Set up the type of this open metadata archive.
     *
     * @param archiveType - OpenMetadataArchiveType enum
     */
    public void setArchiveType(OpenMetadataArchiveType archiveType)
    {
        this.archiveType = archiveType;
    }


    /**
     * Return the name of the originator of this open metadata archive This will be used as the name of the
     * creator for each element in the archive.
     *
     * @return String name
     */
    public String getOriginatorName()
    {
        return originatorName;
    }


    /**
     * Set up the name of the originator of this open metadata archive.  This will be used as the name of the
     * creator for each element in the archive.
     *
     * @param originatorName - String name
     */
    public void setOriginatorName(String originatorName)
    {
        this.originatorName = originatorName;
    }


    /**
     * Return the name of the organization that provided this archive.
     *
     * @return String organization name
     */
    public String getOriginatorOrganization()
    {
        return originatorOrganization;
    }

    /**
     * Set up the name of the organization that provided this archive.
     *
     * @param originatorOrganization - String name
     */
    public void setOriginatorOrganization(String originatorOrganization)
    {
        this.originatorOrganization = originatorOrganization;
    }


    /**
     * Return the date that this open metadata archive was created.
     *
     * @return Date object
     */
    public Date getCreationDate()
    {
        return creationDate;
    }


    /**
     * Set up the date that this open metadata archive was created.
     *
     * @param creationDate - Date object
     */
    public void setCreationDate(Date creationDate)
    {
        this.creationDate = creationDate;
    }


    /**
     * Return the list of GUIDs for open metadata archives that need to be loaded before this one.
     *
     * @return list of guids
     */
    public List<String> getDependsOnArchives()
    {
        if (dependsOnArchives == null)
        {
            return null;
        }
        else
        {
            return new ArrayList<>(dependsOnArchives);
        }
    }


    /**
     * Set up the list of GUIDs for open metadata archives that need to be loaded before this one.
     *
     * @param dependsOnArchives - list of guids
     */
    public void setDependsOnArchives(List<String> dependsOnArchives)
    {
        if (dependsOnArchives == null)
        {
            this.dependsOnArchives = null;
        }
        else
        {
            this.dependsOnArchives = new ArrayList<>(dependsOnArchives);
        }
    }
}
