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

import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceType;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefCategory;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;

import java.util.ArrayList;

/**
 * OMRSRepositoryValidator provides methods to validate TypeDefs and Instances returned from
 * an open metadata repository.  An instance can be created by any OMRS component, or OMRS adapter and
 * it will connect to the local repository's content manager to access the local type definitions (TypeDefs)
 * and rules.
 */
public class OMRSRepositoryValidator implements OMRSTypeDefValidator, OMRSInstanceValidator
{
    private static OMRSRepositoryContentManager    repositoryContentManager = null;

    /**
     * Set up the local repository's content manager.  This maintains a cache of the local repository's type
     * definitions and rules to provide helpers and validators for TypeDefs and instances that are
     * exchanged amongst the open metadata repositories and open metadata access services (OMAS).
     *
     * @param repositoryContentManager - link to repository content manager.
     */
    public static synchronized void setRepositoryContentManager(OMRSRepositoryContentManager  repositoryContentManager)
    {
        OMRSRepositoryHelper.setRepositoryContentManager(repositoryContentManager);
    }


    /**
     * Return a summary list of the TypeDefs supported by the local metadata repository.  This is
     * broadcast to the other servers/repositories in the cluster during the membership registration exchanges
     * managed by the cluster registries.
     *
     * @return TypeDefSummary iterator
     */
    public ArrayList<TypeDefSummary> getLocalTypeDefs()
    {
        final String  methodName = "getLocalTypeDefs()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.getLocalTypeDefs();
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

    }


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
     * @param sourceName - name of the caller
     * @param typeDefSummaries - list of summary information about the TypeDefs.
     */
    public void validateAgainstLocalTypeDefs(String   sourceName,
                                             ArrayList<TypeDefSummary> typeDefSummaries)
    {
        final String  methodName = "validateAgainstLocalTypeDefs()";

        if (repositoryContentManager != null)
        {
            repositoryContentManager.validateAgainstLocalTypeDefs(sourceName, typeDefSummaries);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return a boolean flag indicating whether the list of TypeDefs passed are compatible with the
     * all known typedefs.
     *
     * A valid TypeDef is one that matches name, GUID and version to the full list of TypeDefs.
     * If a new TypeDef is present, it is added to the enterprise list.
     *
     * @param typeDefs - list of TypeDefs.
     * @return boolean flag
     */
    public boolean   validateEnterpriseTypeDefs(String             sourceName,
                                                ArrayList<TypeDef> typeDefs)
    {
        final String  methodName = "validateEnterpriseTypeDefs()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.validateEnterpriseTypeDefs(sourceName, typeDefs);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return boolean indicating whether the TypeDef is in use in the repository.
     *
     * @param sourceName - name of caller
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean flag
     */
    public boolean isActiveType(String   sourceName, String   typeDefGUID, String   typeDefName)
    {
        final String  methodName = "isActiveType()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.isActiveType(sourceName, typeDefGUID, typeDefName);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }

    /**
     * Return boolean indicating whether the TypeDef is one of the open metadata types.
     *
     * @param sourceName - name of caller
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean flag
     */
    public boolean isOpenType(String   sourceName, String   typeDefGUID, String   typeDefName)
    {
        final String  methodName = "isOpenType()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.isOpenType(sourceName, typeDefGUID, typeDefName);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return boolean indicating whether the TypeDef is in use in the repository.
     *
     * @param sourceName - name of caller
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean flag
     */
    public boolean isKnownType(String   sourceName, String   typeDefGUID, String   typeDefName)
    {
        final String  methodName = "isKnownType()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.isKnownType(sourceName, typeDefGUID, typeDefName);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return boolean indicating whether the TypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @return boolean result
     */
    public boolean validTypeId(String          sourceName,
                               String          typeDefGUID,
                               String          typeDefName)
    {
        final String  methodName = "validTypeId()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.validTypeId(sourceName,
                                                        typeDefGUID,
                                                        typeDefName);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return boolean indicating whether the TypeDef identifiers are from a single known type or not.
     *
     * @param sourceName - source of the request (used for logging)
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     * @param category - category for the TypeDef
     * @return boolean result
     */
    public boolean validTypeId(String          sourceName,
                               String          typeDefGUID,
                               String          typeDefName,
                               TypeDefCategory category)
    {
        final String  methodName = "validTypeId()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.validTypeId(sourceName,
                                                        typeDefGUID,
                                                        typeDefName,
                                                        category);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


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
    public boolean validTypeId(String          sourceName,
                               String          typeDefGUID,
                               String          typeDefName,
                               long            typeDefVersion,
                               TypeDefCategory category)
    {
        final String  methodName = "validTypeId()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.validTypeId(sourceName,
                                                        typeDefGUID,
                                                        typeDefName,
                                                        typeDefVersion,
                                                        category);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return boolean indicating whether the supplied TypeDef is valid or not.
     *
     * @param sourceName - source of the TypeDef (used for logging)
     * @param typeDef - TypeDef to test
     * @return boolean result
     */
    public boolean validTypeDef(String         sourceName,
                                TypeDef        typeDef)
    {
        final String  methodName = "validTypeDef()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.validTypeDef(sourceName, typeDef);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Return boolean indicating whether the supplied TypeDefSummary is valid or not.
     *
     * @param sourceName - source of the TypeDefSummary (used for logging)
     * @param typeDefSummary - TypeDefSummary to test.
     * @return boolean result.
     */
    public boolean validTypeDefSummary(String                sourceName,
                                       TypeDefSummary        typeDefSummary)
    {
        final String  methodName = "validTypeDefSummary()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.validTypeDefSummary(sourceName, typeDefSummary);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Test that the supplied entity is valid.
     *
     * @param sourceName - source of the entity (used for logging)
     * @param entity - entity to test
     * @return boolean result
     */
    public boolean validEntity(String       sourceName,
                               EntityDetail entity)
    {
        final String  methodName = "validEntity()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.validEntity(sourceName, entity);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Test that the supplied relationship is valid.
     *
     * @param sourceName - name of the caller (used for logging)
     * @param relationship - relationship to test
     * @return boolean result
     */
    public boolean validRelationship(String       sourceName,
                                     Relationship relationship)
    {
        final String  methodName = "validRelationship()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.validRelationship(sourceName, relationship);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }


    /**
     * Verify that the identifiers for an instance are correct.
     *
     * @param sourceName - source of the instance (used for logging)
     * @param typeDefGUID - unique identifier for the type.
     * @param typeDefName - unique name for the type.
     * @param category - expected category of the instance.
     * @param instanceGUID - unique identifier for the instance.
     * @return boolean indicating whether the identifiers are ok.
     */
    public boolean validInstanceId(String           sourceName,
                                   String           typeDefGUID,
                                   String           typeDefName,
                                   TypeDefCategory  category,
                                   String           instanceGUID)
    {
        final String  methodName = "validInstanceId()";

        if (repositoryContentManager != null)
        {
            return repositoryContentManager.validInstanceId(sourceName,
                                                            typeDefGUID,
                                                            typeDefName,
                                                            category,
                                                            instanceGUID);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }
}
