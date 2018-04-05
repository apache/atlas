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

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * OpenMetadataArchive defines the structure of the properties inside of an open metadata archive.
 * There are 3 sections:
 * <ul>
 *     <li>
 *         ArchiveProperties - provides details of the source and contents of the archive.
 *     </li>
 *     <li>
 *         TypeStore - a list of new TypeDefs and patches to existing TypeDefs.
 *     </li>
 *     <li>
 *         InstanceStore - a list of new metadata instances (Entities and Relationships).
 *     </li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class OpenMetadataArchive
{
    private OpenMetadataArchiveProperties    archiveProperties    = null;
    private OpenMetadataArchiveTypeStore     archiveTypeStore     = null;
    private OpenMetadataArchiveInstanceStore archiveInstanceStore = null;


    /**
     * Default constructor relies on the initialization of variables in their type declaration.
     */
    public OpenMetadataArchive()
    {
    }


    /**
     * Return details of the archive.
     *
     * @return OpenMetadataArchiveProperties object
     */
    public OpenMetadataArchiveProperties getArchiveProperties()
    {
        return archiveProperties;
    }


    /**
     * Set the archive properties for a new archive.
     *
     * @param archiveProperties - the descriptive properties of the archive
     */
    public void setArchiveProperties(OpenMetadataArchiveProperties archiveProperties)
    {
        this.archiveProperties = archiveProperties;
    }


    /**
     * Return the TypeStore for this archive.  The TypeStore contains TypeDefs and TypeDef patches.
     *
     * @return OpenMetadataArchiveTypeStore object
     */
    public OpenMetadataArchiveTypeStore getArchiveTypeStore()
    {
        return archiveTypeStore;
    }


    /**
     * Set up the TypeStore for this archive.  The TypeStore contains TypeDefs and TypeDef patches.
     *
     * @param archiveTypeStore - OpenMetadataArchiveTypeStore object
     */
    public void setArchiveTypeStore(OpenMetadataArchiveTypeStore archiveTypeStore)
    {
        this.archiveTypeStore = archiveTypeStore;
    }


    /**
     * Return the InstanceStore for this archive. The InstanceStore contains entity and relationship metadata
     * instances.
     *
     * @return OpenMetadataArchiveInstanceStore object
     */
    public OpenMetadataArchiveInstanceStore getArchiveInstanceStore()
    {
        return archiveInstanceStore;
    }


    /**
     * Set up the InstanceStore for this archive. The InstanceStore contains entity and relationship metadata
     * instances.
     *
     * @param archiveInstanceStore - OpenMetadataArchiveInstanceStore object
     */
    public void setArchiveInstanceStore(OpenMetadataArchiveInstanceStore archiveInstanceStore)
    {
        this.archiveInstanceStore = archiveInstanceStore;
    }
}
