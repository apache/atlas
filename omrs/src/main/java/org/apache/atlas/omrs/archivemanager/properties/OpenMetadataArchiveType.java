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
 * OpenMetadataArchiveType defines the origin of the open metadata archive.  Content pack means tha the archive contains
 * pre-defined types and instances for a particular use case.  Metadata export is a collection of types and instances
 * from a particular metadata server.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum OpenMetadataArchiveType
{
    CONTENT_PACK    (1, "ContentPack",
                        "A collection of metadata elements that define a standard or support a specific use case."),
    METADATA_EXPORT (2, "MetadataExport",
                        "A collection of metadata elements that have been extracted from a specific open metadata repository.");


    private int    archiveTypeCode;
    private String archiveTypeName;
    private String archiveTypeDescription;


    /**
     * Constructor fo an enum instance.
     *
     * @param archiveTypeCode - code number for the archive type
     * @param archiveTypeName - name for the archive type
     * @param archiveTypeDescription - default description ofr the archive type
     */
    OpenMetadataArchiveType(int archiveTypeCode, String archiveTypeName, String archiveTypeDescription)
    {
        this.archiveTypeCode = archiveTypeCode;
        this.archiveTypeName = archiveTypeName;
        this.archiveTypeDescription = archiveTypeDescription;
    }


    /**
     * Return the code number for the archive type.
     *
     * @return int code number
     */
    public int getArchiveTypeCode()
    {
        return archiveTypeCode;
    }


    /**
     * Return the printable name for the archive type.
     *
     * @return String archive type name
     */
    public String getArchiveTypeName()
    {
        return archiveTypeName;
    }


    /**
     * Return the default description of the archive type.
     *
     * @return String archive description
     */
    public String getArchiveTypeDescription()
    {
        return archiveTypeDescription;
    }
}
