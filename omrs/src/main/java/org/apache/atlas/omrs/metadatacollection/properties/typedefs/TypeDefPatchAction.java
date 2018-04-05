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
 * TypeDefPatchAction defines the types of actions that can be taken to update a TypeDef.  These changes are safe
 * to make while there are active instances using them.  If more extensive changes need to be made to a TypeDef
 * then a new TypeDef should be defined.
 */
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public enum TypeDefPatchAction
{
    ADD_ATTRIBUTES                (1, "AddAttributes",
                                      "Add one or more new attributes to a TypeDef"),
    ADD_OPTIONS                   (2, "AddOptions",
                                      "Extend the current list of options for a TypeDef. These options are used to " +
                                              "help process metadata instances. " +
                                              "They may be different in each TypeDef."),
    UPDATE_OPTIONS                (3, "UpdateOptions",
                                      "Replace the options from a TypeDef. These options are used to help " +
                                              "process metadata instances. " +
                                              "They may be different in each TypeDef."),
    DELETE_OPTIONS                (4,  "DeleteOptions",
                                       "Delete the options from a TypeDef. These options are used to help " +
                                               "process metadata instances. " +
                                               "They may be different in each TypeDef."),
    ADD_EXTERNAL_STANDARDS        (5,  "AddExternalStandardMapping",
                                       "Add a mapping to an external standard either for the TypeDef or the supplied attributes."),
    UPDATE_EXTERNAL_STANDARDS     (6,  "UpdateExternalStandardMapping",
                                       "Update a mapping to an external standard either for the TypeDef or the supplied attributes."),
    DELETE_EXTERNAL_STANDARDS     (7,  "DeleteExternalStandardMapping",
                                       "Remove a mapping to an external standard either for the TypeDef or the supplied attributes."),
    UPDATE_DESCRIPTIONS           (8,  "UpdateDescriptions",
                                       "Update the descriptions and descriptionGUIDs of the TypeDef and its attributes.");


    private int    patchActionCode;
    private String patchActionName;
    private String patchActionDescription;


    /**
     * Constructor to set up a single instances of the enum.
     *
     * @param patchActionCode - numeric code for the patch action
     * @param patchActionName - descriptive name for the patch action
     * @param patchActionDescription - description of the patch action
     */
    TypeDefPatchAction(int patchActionCode, String patchActionName, String patchActionDescription)
    {
        this.patchActionCode = patchActionCode;
        this.patchActionName = patchActionName;
        this.patchActionDescription = patchActionDescription;
    }


    /**
     * Return the code value for the patch action.
     *
     * @return int code
     */
    public int getPatchActionCode()
    {
        return patchActionCode;
    }


    /**
     * Return the descriptive name for the patch action.
     *
     * @return String name
     */
    public String getPatchActionName()
    {
        return patchActionName;
    }


    /**
     * Return the description of the patch action.
     *
     * @return String description
     */
    public String getPatchActionDescription()
    {
        return patchActionDescription;
    }
}
