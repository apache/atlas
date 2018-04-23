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
package org.apache.atlas.omrs.eventmanagement;

import org.apache.atlas.omrs.admin.properties.OpenMetadataExchangeRule;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryContentManager;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceHeader;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceType;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDef;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;

import java.util.HashMap;
import java.util.List;

/**
 * OMRSRepositoryEventExchangeRule determines if particular types of events should be exchanged on the OMRS Topic.
 */
public class OMRSRepositoryEventExchangeRule
{
    private String                             sourceName;
    private OMRSRepositoryContentManager       repositoryContentManager;
    private OpenMetadataExchangeRule           exchangeRule;
    private HashMap<String, TypeDefSummary>    selectedTypesToProcess = new HashMap<>();


    /**
     * Constructor provides all of the objects used in the event exchange decision.
     *
     * @param sourceName - name of the caller
     * @param repositoryContentManager - local manager of the type definitions (TypeDefs) used by the local repository.
     * @param exchangeRule - enum detailing the types of events to process.
     * @param selectedTypesToProcess - supplementary list to support selective processing of events.
     */
    public OMRSRepositoryEventExchangeRule(String                       sourceName,
                                           OMRSRepositoryContentManager repositoryContentManager,
                                           OpenMetadataExchangeRule     exchangeRule,
                                           List<TypeDefSummary>         selectedTypesToProcess)
    {
        final String  methodName = "OMRSRepositoryEventExchangeRule constructor";

        /*
         * Validate the supplied parameters.
         */
        if (sourceName == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NULL_SOURCE_NAME;

            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(methodName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
        if (repositoryContentManager == null)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_CONTENT_MANAGER;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(methodName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
        if (exchangeRule == null)
        {
            OMRSErrorCode  errorCode = OMRSErrorCode.NULL_EXCHANGE_RULE;

            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(methodName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        this.sourceName = sourceName;
        this.repositoryContentManager = repositoryContentManager;
        this.exchangeRule = exchangeRule;

        /*
         * The selected types are loaded into a hash map for easy retrieval.
         */
        if (selectedTypesToProcess != null)
        {
            for (TypeDefSummary  typeDefSummary : selectedTypesToProcess)
            {
                if (typeDefSummary != null)
                {
                    String   typeDefSummaryGUID = typeDefSummary.getGUID();

                    if (typeDefSummaryGUID != null)
                    {
                        this.selectedTypesToProcess.put(typeDefSummaryGUID, typeDefSummary);
                    }
                }
            }
        }
    }


    /**
     * Determine if TypeDef events should be processed.  TypeDef events are processed unless the rule is set
     * to registration events only.
     *
     * @return boolean flag indicating if the event should be processed.
     */
    public boolean processTypeDefEvents()
    {
        if (exchangeRule != OpenMetadataExchangeRule.REGISTRATION_ONLY)
        {
            return true;
        }
        else
        {
            return false;
        }
    }


    /**
     * Determine from the type of the instance if an instance event should be processed.
     *
     * @param typeDefGUID - unique identifier of the type
     * @param typeDefName - unique name of the type
     * @return boolean flag
     */
    public boolean processInstanceEvent(String   typeDefGUID, String   typeDefName)
    {
        if (repositoryContentManager == null)
        {
            /*
             * A set up error
             */
            return false;
        }
        else if ((exchangeRule == OpenMetadataExchangeRule.REGISTRATION_ONLY) ||
                 (exchangeRule == OpenMetadataExchangeRule.JUST_TYPEDEFS))
        {
            /*
             * The rule says not to process instances
             */
            return false;
        }
        else if ((typeDefGUID == null) || (typeDefName == null))
        {
            /*
             * The instance is invalid - this will be logged elsewhere.
             */
            return false;
        }
        else
        {
            /*
             * Only active types should be processed.
             */
            if (repositoryContentManager.isActiveType(sourceName, typeDefGUID, typeDefName))
            {
                if (exchangeRule == OpenMetadataExchangeRule.ALL)
                {
                    /*
                     * All active types should be processed
                     */
                    return true;
                }
                else
                {
                    /*
                     * The exchange rule is either SELECTED_TYPES or LEARNED_TYPES - for either, the instance
                     * is processed if its type is in the selectedTypesToProcess list.
                     */
                    if (selectedTypesToProcess.get(typeDefGUID) != null)
                    {
                        return true;
                    }
                }
            }

            return false;
        }
    }


    /**
     * Determine from the type of the instance if an instance event should be processed.
     *
     * @param typeDefSummary - details of the type
     * @return boolean flag
     */
    public boolean processInstanceEvent(TypeDefSummary   typeDefSummary)
    {
        if (repositoryContentManager == null)
        {
            return false;
        }
        if (typeDefSummary == null)
        {
            return false;
        }
        else
        {
            return this.processInstanceEvent(typeDefSummary.getGUID(), typeDefSummary.getName());
        }
    }


    /**
     * Determine from the type of the instance if an instance event should be processed.
     *
     * @param instance - details of the instance to test
     * @return boolean flag
     */
    public boolean processInstanceEvent(InstanceHeader instance)
    {
        if (repositoryContentManager == null)
        {
            return false;
        }
        else if (instance == null)
        {
            return false;
        }
        else
        {
            InstanceType type = instance.getType();

            if (type == null)
            {
                return false;
            }
            else
            {
                return this.processInstanceEvent(type.getTypeDefGUID(), type.getTypeDefName());
            }
        }
    }


    /**
     * If the rule is in learning mode, determine if the type of the instance should be added to the list
     * of types being processed.  For this to happen, the instance header must include a valid type, the type
     * must be an active type for this server and not already included in the list.
     *
     * Any errors discovered in the types, of this rule's set up result in a false result.  No diagnostics are
     * created because this method is called very frequently and the errors will be trapped and logged elsewhere.
     *
     * @param instance - details of the instance to test
     * @return boolean flag - true if the instance should be saved as a learned instance.
     */
    public boolean learnInstanceEvent(InstanceHeader instance)
    {
        final String methodName = "learnInstanceEvent";
        final String parameterName = "instance";

        if (repositoryContentManager == null)
        {
            /*
             * This is a logic error the cause of which has probably already been logged.
             */
            return false;
        }
        else if (exchangeRule == OpenMetadataExchangeRule.LEARNED_TYPES)
        {
            InstanceType type = instance.getType();

            if (type != null)
            {
                if (repositoryContentManager.isActiveType(sourceName, type.getTypeDefGUID(), type.getTypeDefName()))
                {
                    /*
                     * All active types should be learned and added to the rule so save copies are updated by
                     * incoming events.
                     */
                    if (selectedTypesToProcess.get(type.getTypeDefGUID()) == null)
                    {
                        try
                        {
                            TypeDef typeDef = repositoryContentManager.getTypeDef(sourceName,
                                                                                  parameterName,
                                                                                  parameterName,
                                                                                  type.getTypeDefGUID(),
                                                                                  type.getTypeDefName(),
                                                                                  methodName);

                            if (typeDef != null)
                            {
                                selectedTypesToProcess.put(typeDef.getGUID(), typeDef);
                            }
                        }
                        catch (Throwable  error)
                        {
                            return false; /* Problem with type */
                        }
                    }

                    /*
                     * The instance should be saved if it is not already known.
                     */
                    return true;
                }
            }
        }

        return false; /* rule is not set to LEARNED_TYPES (or a problem with the instance) */
    }
}
