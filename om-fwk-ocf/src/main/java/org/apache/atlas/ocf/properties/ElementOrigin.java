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
package org.apache.atlas.ocf.properties;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * ElementOrigin defines where the metadata comes from and, hence if it can be updated.
 * <ul>
 *     <li>
 *         LOCAL_COHORT - the element is being maintained within the local cohort.
 *         The metadata collection id is for one of the repositories in the cohort.
 *         This metadata collection id identifies the home repository for this element.
 *     </li>
 *     <li>
 *         EXPORT_ARCHIVE - the element was created from an export archive.
 *         The metadata collection id for the element is the metadata collection id of the originating server.
 *         If the originating server later joins the cohort with the same metadata collection id then these
 *         elements will be refreshed from the originating server’s current repository.
 *     </li>
 *     <li>
 *         CONTENT_PACK - the element comes from an open metadata content pack.
 *         The metadata collection id of the elements is set to the GUID of the pack.
 *     </li>
 *     <li>
 *         DEREGISTERED_REPOSITORY - the element comes from a metadata repository that used to be a part
 *         of the repository cohort but has been deregistered. The metadata collection id remains the same.
 *         If the repository rejoins the cohort then these elements can be refreshed from the rejoining repository.
 *     </li>
 * </ul>
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum ElementOrigin implements Serializable
{
    LOCAL_COHORT(0, "Local to cohort",  "The element is being maintained within one of the local cohort members. " +
            "The metadata collection id is for one of the repositories in the cohort. " +
            "This metadata collection id identifies the home repository for this element. "),
    EXPORT_ARCHIVE(1, "Export Archive", "The element was created from an export archive. " +
            "The metadata collection id for the element is the metadata collection id of the originating server. " +
            "If the originating server later joins the cohort with the same metadata collection Id " +
            "then these elements will be refreshed from the originating server’s current repository."),
    CONTENT_PACK(2, "Content Pack", "The element comes from an open metadata content pack. " +
            "The metadata collection id of the elements is set to the GUID of the pack."),
    DEREGISTERED_REPOSITORY(3, "Deregistered Repository", "The element comes from a metadata repository that " +
            "used to be a member of the one of the local repository's cohorts but it has been deregistered. " +
            "The metadata collection id remains the same. If the repository rejoins the cohort " +
            "then these elements can be refreshed from the rejoining repository."),
    CONFIGURATION(4, "Configuration",
            "The element is part of a service's configuration.  The metadata collection id is null.");

    private static final long     serialVersionUID = 1L;

    private int    originCode;
    private String originName;
    private String originDescription;


    /**
     * Constructor for the enum.
     *
     * @param originCode - code number for origin
     * @param originName - name for origin
     * @param originDescription - description for origin
     */
    ElementOrigin(int originCode, String originName, String originDescription)
    {
        this.originCode = originCode;
        this.originName = originName;
        this.originDescription = originDescription;
    }


    /**
     * Return the code for metadata element.
     *
     * @return int code for the origin
     */
    public int getOriginCode()
    {
        return originCode;
    }


    /**
     * Return the name of the metadata element origin.
     *
     * @return String name
     */
    public String getOriginName()
    {
        return originName;
    }


    /**
     * Return the description of the metadata element origin.
     *
     * @return String description
     */
    public String getOriginDescription()
    {
        return originDescription;
    }


    /**
     * Standard toString method.
     *
     * @return print out of variables in a JSON-style
     */
    @Override
    public String toString()
    {
        return "ElementOrigin{" +
                "originCode=" + originCode +
                ", originName='" + originName + '\'' +
                ", originDescription='" + originDescription + '\'' +
                '}';
    }
}
