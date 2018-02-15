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
