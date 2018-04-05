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
package org.apache.atlas.omrs.metadatacollection.properties.instances;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

/**
 * InstanceProvenanceType defines where the metadata comes from and, hence if it can be updated.
 * <ul>
 *     <li>
 *         UNKNOWN - uninitialized provenance value.
 *     </li>
 *     <li>
 *         LOCAL_COHORT - the element is being maintained within the local cohort.
 *         The metadata collection id is for one of the repositories in the cohort.
 *         This metadata collection id identifies the home repository for this element.
 *     </li>
 *     <li>
 *         EXPORT_ARCHIVE - the element was created from an export archive.
 *         The metadata collection id for the element is the metadata collection id of the originating server.
 *         If the originating server later joins the cohort with the same metadata collection Id then these
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
public enum InstanceProvenanceType implements Serializable
{
    UNKNOWN                 (0, "<Unknown>",               "Unknown provenance"),
    LOCAL_COHORT            (1, "Local Cohort",            "The instance is managed by one of the members of a cohort " +
                                                                      "that the local server belongs to"),
    EXPORT_ARCHIVE          (2, "Export Archive",          "The instance comes from an open metadata archive that was " +
                                                                      "created from a metadata export from an open " +
                                                                      "metadata repository"),
    CONTENT_PACK            (3, "Content Pack",            "The instance comes from an open metadata archive that was " +
                                                                      "created as a content pack"),
    DEREGISTERED_REPOSITORY (4, "Deregistered Repository", "The instance is a cached copy of a metadata instance " +
                                                                      "that is owned by a repository that is no longer " +
                                                                      "connected to one of the cohorts that the " +
                                                                      "local server belongs to; it may be out-of-date");

    private static final long serialVersionUID = 1L;

    private int            ordinal;
    private String         name;
    private String         description;


    /**
     * Default constructor for the instance provenance type.
     *
     * @param ordinal - numerical representation of the instance provenance type
     * @param name - default string name of the instance provenance type
     * @param description - default string description of the instance provenance type
     */
    InstanceProvenanceType(int  ordinal, String name, String description)
    {
        this.ordinal = ordinal;
        this.name = name;
        this.description = description;
    }


    /**
     * Return the numeric representation of the instance provenance type.
     *
     * @return int ordinal
     */
    public int getOrdinal() { return ordinal; }


    /**
     * Return the default name of the instance provenance type.
     *
     * @return String name
     */
    public String getName() { return name; }


    /**
     * Return the default description of the instance provenance type.
     *
     * @return String description
     */
    public String getDescription() { return description; }
}
