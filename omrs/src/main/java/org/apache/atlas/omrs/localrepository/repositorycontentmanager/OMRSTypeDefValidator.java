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

import org.apache.atlas.omrs.ffdc.exception.RepositoryErrorException;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;

import java.util.List;

/**
 * OMRSTypeDefValidator describes a component that is able to manage TypeDefs for the local metadata repository.
 */
public interface OMRSTypeDefValidator
{
    /**
     * Return a boolean flag indicating whether the list of TypeDefs passed are compatible with the
     * all known typedefs.
     *
     * A valid TypeDef is one that matches name, GUID and version to the full list of TypeDefs.
     * If a new TypeDef is present, it is added to the enterprise list.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDefs - list of TypeDefs.
     * @param methodName - name of calling method
     * @throws RepositoryErrorException - a conflicting or invalid TypeDef has been returned
     */
    void   validateEnterpriseTypeDefs(String        sourceName,
                                      List<TypeDef> typeDefs,
                                      String        methodName) throws RepositoryErrorException;


    /**
     * Return a boolean flag indicating whether the list of TypeDefs passed are compatible with the
     * all known typedefs.
     *
     * A valid TypeDef is one that matches name, GUID and version to the full list of TypeDefs.
     * If a new TypeDef is present, it is added to the enterprise list.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param attributeTypeDefs - list of AttributeTypeDefs.
     * @param methodName - name of calling method
     * @throws RepositoryErrorException - a conflicting or invalid AttributeTypeDef has been returned
     */
    void   validateEnterpriseAttributeTypeDefs(String                 sourceName,
                                               List<AttributeTypeDef> attributeTypeDefs,
                                               String                 methodName) throws RepositoryErrorException;


    /**
     * Return boolean indicating whether the TypeDef is one of the standard open metadata types.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeGUID - unique identifier of the type
     * @param typeName - unique name of the type
     * @return boolean result
     */
    boolean isOpenType(String  sourceName, String   typeGUID, String   typeName);


    /**
     * Return boolean indicating whether the TypeDef is one of the standard open metadata types.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeGUID - unique identifier of the type
     * @return boolean result
     */
    boolean isOpenTypeId(String  sourceName, String   typeGUID);


    /**
     * Return boolean indicating whether the TypeDef/AttributeTypeDef is known, either as an open type, or one defined
     * by one or more of the members of the cohort.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeGUID - unique identifier of the type
     * @param typeName - unique name of the type
     * @return boolean result
     */
    boolean isKnownType(String  sourceName, String   typeGUID, String   typeName);


    /**
     * Return boolean indicating whether the TypeDef/AttributeTypeDef is known, either as an open type, or one defined
     * by one or more of the members of the cohort.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeGUID - unique identifier of the type
     * @return boolean result
     */
    boolean isKnownTypeId(String  sourceName, String   typeGUID);


    /**
     * Return boolean indicating whether the TypeDef/AttributeTypeDef is in use in the local repository.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeGUID - unique identifier of the type
     * @param typeName - unique name of the type
     * @return boolean result
     */
    boolean isActiveType(String  sourceName, String   typeGUID, String   typeName);


    /**
     * Return boolean indicating whether the TypeDef/AttributeTypeDef is in use in the local repository.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeGUID - unique identifier of the type
     * @return boolean result
     */
    boolean isActiveTypeId(String  sourceName, String   typeGUID);


    /**
     * Return boolean indicating whether the TypeDef/AttributeTypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeGUID - unique identifier of the TypeDef
     * @param typeName - unique name of the TypeDef
     * @return boolean result
     */
    boolean validTypeId(String          sourceName,
                        String          typeGUID,
                        String          typeName);


    /**
     * Return boolean indicating whether the TypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @param category - category for the TypeDef
     * @return boolean result
     */
    boolean validTypeDefId(String          sourceName,
                           String          typeDefGUID,
                           String          typeDefName,
                           TypeDefCategory category);


    /**
     * Return boolean indicating whether the AttributeTypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier of the AttributeTypeDef
     * @param attributeTypeDefName - unique name of the AttributeTypeDef
     * @param category - category for the AttributeTypeDef
     * @return boolean result
     */
    boolean validAttributeTypeDefId(String                   sourceName,
                                    String                   attributeTypeDefGUID,
                                    String                   attributeTypeDefName,
                                    AttributeTypeDefCategory category);


    /**
     * Return boolean indicating whether the TypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @param typeDefVersion - version of the type
     * @param category - category for the TypeDef
     * @return boolean result
     */
    boolean validTypeDefId(String          sourceName,
                           String          typeDefGUID,
                           String          typeDefName,
                           long            typeDefVersion,
                           TypeDefCategory category);


    /**
     * Return boolean indicating whether the TypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDefGUID - unique identifier of the TypeDef
     * @param attributeTypeDefName - unique name of the TypeDef
     * @param attributeTypeDefVersion - version of the type
     * @param category - category for the TypeDef
     * @return boolean result
     */
    boolean validAttributeTypeDefId(String                   sourceName,
                                    String                   attributeTypeDefGUID,
                                    String                   attributeTypeDefName,
                                    long                     attributeTypeDefVersion,
                                    AttributeTypeDefCategory category);


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
     * Return boolean indicating whether the supplied AttributeTypeDef is valid or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param attributeTypeDef - TypeDef to test
     * @return boolean result
     */
    boolean validAttributeTypeDef(String           sourceName,
                                  AttributeTypeDef attributeTypeDef);

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
