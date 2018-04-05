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

package org.apache.atlas.omag.admin;

import java.io.Serializable;

/**
 * LocalRepositoryMode defines the mode that the local repository will operate in.
 */
public enum LocalRepositoryMode implements Serializable
{
    NO_LOCAL_REPOSITORY    (0,    "No Local repository",      "There is no local repository so all of the metadata " +
                                                              "passed through the enterprise access " +
                                                              "layer to the open metadata access services comes from " +
                                                              "peer repositories from the cohort(s) that this repository " +
                                                              "is registered with."),
    IN_MEMORY_REPOSITORY   (1,    "In memory repository",     "The local repository is an in memory repository that does" +
                                                              "not save metadata between each run of the server."),
    LOCAL_GRAPH_REPOSITORY (2,    "Local graph repository",   "The built-in graph database is in use.  Metadata can be stored " +
                                                              "and retrieved from this graph database. " +
                                                              "This metadata can be combined with metadata from " +
                                                              "peer repositories from the cohort(s) that this repository " +
                                                              "is registered with."),
    REPOSITORY_PROXY       (3,    "Repository proxy",         "The local repository is implemented by a service that is " +
                                                              "external to the local server.  Metadata can be stored " +
                                                              "and retrieved from this repository. This metadata can be " +
                                                              "combined with metadata from peer repositories from the " +
                                                              "cohort(s) that this repository is registered with.");

    private static final long serialVersionUID = 1L;

    private int            typeCode;
    private String         typeName;
    private String         typeDescription;


    /**
     * Default Constructor
     *
     * @param typeCode - ordinal for this enum
     * @param typeName - symbolic name for this enum
     * @param typeDescription - short description for this enum
     */
    LocalRepositoryMode(int     typeCode, String   typeName, String   typeDescription)
    {
        /*
         * Save the values supplied
         */
        this.typeCode = typeCode;
        this.typeName = typeName;
        this.typeDescription = typeDescription;
    }


    /**
     * Return the code for this enum instance
     *
     * @return int - type code
     */
    public int getTypeCode()
    {
        return typeCode;
    }


    /**
     * Return the default name for this enum instance.
     *
     * @return String - default name
     */
    public String getTypeName()
    {
        return typeName;
    }


    /**
     * Return the default description for the type for this enum instance.
     *
     * @return String - default description
     */
    public String getTypeDescription()
    {
        return typeDescription;
    }
}
