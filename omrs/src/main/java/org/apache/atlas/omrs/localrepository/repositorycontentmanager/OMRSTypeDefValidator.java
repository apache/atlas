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
package org.apache.atlas.omrs.localrepository.repositorycontentmanager;

import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefCategory;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;

import java.util.ArrayList;

/**
 * OMRSTypeDefValidator describes a component that is able to manage TypeDefs for the local metadata repository.
 */
public interface OMRSTypeDefValidator
{
    /**
     * Return a summary list of the TypeDefs supported by the local metadata repository.  This is
     * broadcast to the other servers/repositories in the cohort during the membership registration exchanges
     * managed by the cohort registries.
     *
     * @return TypeDefSummary list
     */
    ArrayList<TypeDefSummary> getLocalTypeDefs();


    /**
     * Return a boolean flag indicating whether the list of TypeDefs passed are compatible with the
     * local metadata repository.  A true response means it is ok; false means conflicts have been found.
     *
     * A valid TypeDef is one that:
     * <ul>
     *     <li>
     *         Matches name, GUID and version to a TypeDef in the local repository, or
     *     </li>
     *     <li>
     *         Is not defined in the local repository.
     *     </li>
     * </ul>
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefSummaries - list of summary information about the TypeDefs.
     */
    void validateAgainstLocalTypeDefs(String                    sourceName,
                                      ArrayList<TypeDefSummary> typeDefSummaries);


    /**
     * Return a boolean flag indicating whether the list of TypeDefs passed are compatible with the
     * all known typedefs.
     *
     * A valid TypeDef is one that matches name, GUID and version to the full list of TypeDefs.
     * If a new TypeDef is present, it is added to the enterprise list.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefs - list of TypeDefs.
     * @return boolean flag
     */
    boolean   validateEnterpriseTypeDefs(String             sourceName,
                                         ArrayList<TypeDef> typeDefs);


    /**
     * Return boolean indicating whether the TypeDef is one of the standard open metadata types.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean result
     */
    boolean isOpenType(String  sourceName, String   typeDefGUID, String   typeDefName);


    /**
     * Return boolean indicating whether the TypeDef is known, either as an open type, or one defined
     * by one or more of the members of the cohort.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean result
     */
    boolean isKnownType(String  sourceName, String   typeDefGUID, String   typeDefName);


    /**
     * Return boolean indicating whether the TypeDef is in use in the local repository.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean result
     */
    boolean isActiveType(String  sourceName, String   typeDefGUID, String   typeDefName);


    /**
     * Return boolean indicating whether the TypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @return boolean result
     */
    boolean validTypeId(String          sourceName,
                        String          typeDefGUID,
                        String          typeDefName);


    /**
     * Return boolean indicating whether the TypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @param category - category for the TypeDef
     * @return boolean result
     */
    boolean validTypeId(String          sourceName,
                        String          typeDefGUID,
                        String          typeDefName,
                        TypeDefCategory category);


    /**
     * Return boolean indicating whether the TypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @param typeDefVersion - versionName of the type
     * @param category - category for the TypeDef
     * @return boolean result
     */
    boolean validTypeId(String          sourceName,
                        String          typeDefGUID,
                        String          typeDefName,
                        long            typeDefVersion,
                        TypeDefCategory category);


    /**
     * Return boolean indicating whether the supplied TypeDef is valid or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDef - TypeDef to test
     * @return boolean result
     */
    boolean validTypeDef(String         sourceName,
                         TypeDef        typeDef);


    /**
     * Return boolean indicating whether the supplied TypeDefSummary is valid or not.
     *
     * @param sourceName - source of the TypeDefSummary (used for logging)
     * @param typeDefSummary - TypeDefSummary to test.
     * @return boolean result.
     */
    boolean validTypeDefSummary(String                sourceName,
                                TypeDefSummary        typeDefSummary);
}
