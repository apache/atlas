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

import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.auditlog.OMRSAuditingComponent;
import org.apache.atlas.omrs.eventmanagement.events.OMRSInstanceEventProcessor;
import org.apache.atlas.omrs.eventmanagement.events.OMRSTypeDefEventProcessor;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceProvenanceType;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * OMRSRepositoryEventManager is responsible for managing the distribution of TypeDef and instance events.
 * There is one OMRSRepositoryEventManager for each cohort that the local server is registered with and one for
 * the local repository.
 *
 * Since OMRSRepositoryEventManager sits at the crossroads of the flow of events between the cohorts,
 * the local repository and the enterprise access components, it performs detailed error checking of the
 * event contents to help assure the integrity of the open metadata ecosystem.
 */
public class OMRSRepositoryEventManager implements OMRSRepositoryEventProcessor
{
    private ArrayList<OMRSTypeDefEventProcessor>  typeDefEventConsumers  = new ArrayList<>();
    private ArrayList<OMRSInstanceEventProcessor> instanceEventConsumers = new ArrayList<>();
    private OMRSRepositoryValidator               repositoryValidator; /* set in constructor */
    private OMRSRepositoryEventExchangeRule       exchangeRule; /* set in constructor */

    /*
     * The audit log provides a verifiable record of the open metadata archives that have been loaded into
     * the open metadata repository.  The Logger is for standard debug.
     */
    private static final OMRSAuditLog auditLog = new OMRSAuditLog(OMRSAuditingComponent.REPOSITORY_EVENT_MANAGER);
    private static final Logger       log      = LoggerFactory.getLogger(OMRSRepositoryEventManager.class);

    /**
     * Constructor to initialize a repository event manager
     *
     * @param exchangeRule - this is the rule that determines which events are processed.
     * @param repositoryValidator - validator class for checking open metadata repository objects and parameters.
     */
    public OMRSRepositoryEventManager(OMRSRepositoryEventExchangeRule exchangeRule,
                                      OMRSRepositoryValidator         repositoryValidator)
    {
        final String   methodName = "OMRSRepositoryEventManager";
        /*
         * If the exchangeRule is null, throw exception
         */
        if (exchangeRule == null)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_EXCHANGE_RULE;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        this.exchangeRule = exchangeRule;

        /*
         * If the repository validator is null, throw an exception
         */
        if (repositoryValidator == null)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_REPOSITORY_VALIDATOR;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        this.repositoryValidator = repositoryValidator;

        if (log.isDebugEnabled())
        {
            log.debug("New Event Manager");
        }
    }


    /**
     * Adds a new consumer to the list of consumers that the OMRSRepositoryEventManager will notify of
     * any TypeDef events it receives.
     *
     * @param typeDefEventConsumer - the new consumer of TypeDef events from other members of the cohort
     */
    public void registerTypeDefProcessor(OMRSTypeDefEventProcessor typeDefEventConsumer)
    {
        typeDefEventConsumers.add(typeDefEventConsumer);
    }


    /**
     * Adds a new consumer to the list of consumers that the OMRSRepositoryEventManager will notify of
     * any instance events it receives.
     *
     * @param instanceEventConsumer - the new consumer of instance events from other members of the cohort
     */
    public void registerInstanceProcessor(OMRSInstanceEventProcessor instanceEventConsumer)
    {
        instanceEventConsumers.add(instanceEventConsumer);
    }


    /**
     * A new TypeDef has been defined in a metadata repository.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param typeDef - details of the new TypeDef.
     */
    public void processNewTypeDefEvent(String      sourceName,
                                       String      originatorMetadataCollectionId,
                                       String      originatorServerName,
                                       String      originatorServerType,
                                       String      originatorOrganizationName,
                                       TypeDef     typeDef)
    {
        if (exchangeRule.processTypeDefEvents())
        {
            for (OMRSTypeDefEventProcessor consumer: typeDefEventConsumers)
            {
                consumer.processNewTypeDefEvent(sourceName,
                                                originatorMetadataCollectionId,
                                                originatorServerName,
                                                originatorServerType,
                                                originatorOrganizationName,
                                                typeDef);
            }
        }
    }


    /**
     * A new AttributeTypeDef has been defined in an open metadata repository.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param attributeTypeDef - details of the new AttributeTypeDef.
     */
    public void processNewAttributeTypeDefEvent(String           sourceName,
                                                String           originatorMetadataCollectionId,
                                                String           originatorServerName,
                                                String           originatorServerType,
                                                String           originatorOrganizationName,
                                                AttributeTypeDef attributeTypeDef)
    {
        if (exchangeRule.processTypeDefEvents())
        {
            for (OMRSTypeDefEventProcessor consumer: typeDefEventConsumers)
            {
                consumer.processNewAttributeTypeDefEvent(sourceName,
                                                         originatorMetadataCollectionId,
                                                         originatorServerName,
                                                         originatorServerType,
                                                         originatorOrganizationName,
                                                         attributeTypeDef);
            }
        }
    }


    /**
     * An existing TypeDef has been updated in a remote metadata repository.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param typeDefPatch - details of the new version of the TypeDef
     */
    public void processUpdatedTypeDefEvent(String       sourceName,
                                           String       originatorMetadataCollectionId,
                                           String       originatorServerName,
                                           String       originatorServerType,
                                           String       originatorOrganizationName,
                                           TypeDefPatch typeDefPatch)
    {
        if (exchangeRule.processTypeDefEvents())
        {
            for (OMRSTypeDefEventProcessor consumer: typeDefEventConsumers)
            {
                consumer.processUpdatedTypeDefEvent(sourceName,
                                                    originatorMetadataCollectionId,
                                                    originatorServerName,
                                                    originatorServerType,
                                                    originatorOrganizationName,
                                                    typeDefPatch);
            }
        }
    }


    /**
     * An existing TypeDef has been deleted in a remote metadata repository.  Both the name and the
     * GUID are provided to ensure the right TypeDef is deleted in other cohort member repositories.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param typeDefGUID - unique identifier of the TypeDef
     * @param typeDefName - unique name of the TypeDef
     */
    public void processDeletedTypeDefEvent(String      sourceName,
                                           String      originatorMetadataCollectionId,
                                           String      originatorServerName,
                                           String      originatorServerType,
                                           String      originatorOrganizationName,
                                           String      typeDefGUID,
                                           String      typeDefName)
    {
        if (exchangeRule.processTypeDefEvents())
        {
            for (OMRSTypeDefEventProcessor consumer: typeDefEventConsumers)
            {
                consumer.processDeletedTypeDefEvent(sourceName,
                                                    originatorMetadataCollectionId,
                                                    originatorServerName,
                                                    originatorServerType,
                                                    originatorOrganizationName,
                                                    typeDefGUID,
                                                    typeDefName);
            }
        }
    }


    /**
     * An existing AttributeTypeDef has been deleted in an open metadata repository.  Both the name and the
     * GUID are provided to ensure the right AttributeTypeDef is deleted in other cohort member repositories.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param attributeTypeDefGUID - unique identifier of the AttributeTypeDef
     * @param attributeTypeDefName - unique name of the AttributeTypeDef
     */
    public void processDeletedAttributeTypeDefEvent(String      sourceName,
                                                    String      originatorMetadataCollectionId,
                                                    String      originatorServerName,
                                                    String      originatorServerType,
                                                    String      originatorOrganizationName,
                                                    String      attributeTypeDefGUID,
                                                    String      attributeTypeDefName)
    {
        if (exchangeRule.processTypeDefEvents())
        {
            for (OMRSTypeDefEventProcessor consumer: typeDefEventConsumers)
            {
                consumer.processDeletedAttributeTypeDefEvent(sourceName,
                                                             originatorMetadataCollectionId,
                                                             originatorServerName,
                                                             originatorServerType,
                                                             originatorOrganizationName,
                                                             attributeTypeDefGUID,
                                                             attributeTypeDefName);
            }
        }
    }


    /**
     * Process an event that changes either the name or guid of a TypeDef.  It is resolving a Conflicting TypeDef Error.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originalTypeDefSummary - details of the original TypeDef
     * @param typeDef - updated TypeDef with new identifiers inside.
     */
    public void processReIdentifiedTypeDefEvent(String         sourceName,
                                                String         originatorMetadataCollectionId,
                                                String         originatorServerName,
                                                String         originatorServerType,
                                                String         originatorOrganizationName,
                                                TypeDefSummary originalTypeDefSummary,
                                                TypeDef        typeDef)
    {
        if (exchangeRule.processTypeDefEvents())
        {
            for (OMRSTypeDefEventProcessor consumer: typeDefEventConsumers)
            {
                consumer.processReIdentifiedTypeDefEvent(sourceName,
                                                         originatorMetadataCollectionId,
                                                         originatorServerName,
                                                         originatorServerType,
                                                         originatorOrganizationName,
                                                         originalTypeDefSummary,
                                                         typeDef);
            }
        }
    }


    /**
     * Process an event that changes either the name or guid of an AttributeTypeDef.
     * It is resolving a Conflicting AttributeTypeDef Error.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originalAttributeTypeDef - description of original AttributeTypeDef
     * @param attributeTypeDef - updated AttributeTypeDef with new identifiers inside.
     */
    public void processReIdentifiedAttributeTypeDefEvent(String           sourceName,
                                                         String           originatorMetadataCollectionId,
                                                         String           originatorServerName,
                                                         String           originatorServerType,
                                                         String           originatorOrganizationName,
                                                         AttributeTypeDef originalAttributeTypeDef,
                                                         AttributeTypeDef attributeTypeDef)
    {
        if (exchangeRule.processTypeDefEvents())
        {
            for (OMRSTypeDefEventProcessor consumer: typeDefEventConsumers)
            {
                consumer.processReIdentifiedAttributeTypeDefEvent(sourceName,
                                                                  originatorMetadataCollectionId,
                                                                  originatorServerName,
                                                                  originatorServerType,
                                                                  originatorOrganizationName,
                                                                  originalAttributeTypeDef,
                                                                  attributeTypeDef);
            }
        }
    }


    /**
     * Process a detected conflict in type definitions (TypeDefs) used in the cohort.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originatorTypeDefSummary - details of the TypeDef in the event originator
     * @param otherMetadataCollectionId - the metadataCollection using the conflicting TypeDef
     * @param conflictingTypeDefSummary - the detaild of the TypeDef in the other metadata collection
     * @param errorMessage - description of error.
     */
    public void processTypeDefConflictEvent(String         sourceName,
                                            String         originatorMetadataCollectionId,
                                            String         originatorServerName,
                                            String         originatorServerType,
                                            String         originatorOrganizationName,
                                            TypeDefSummary originatorTypeDefSummary,
                                            String         otherMetadataCollectionId,
                                            TypeDefSummary conflictingTypeDefSummary,
                                            String         errorMessage)
    {
        if (exchangeRule.processTypeDefEvents())
        {
            for (OMRSTypeDefEventProcessor consumer: typeDefEventConsumers)
            {
                consumer.processTypeDefConflictEvent(sourceName,
                                                     originatorMetadataCollectionId,
                                                     originatorServerName,
                                                     originatorServerType,
                                                     originatorOrganizationName,
                                                     originatorTypeDefSummary,
                                                     otherMetadataCollectionId,
                                                     conflictingTypeDefSummary,
                                                     errorMessage);
            }
        }
    }


    /**
     * Process a detected conflict in the attribute type definitions (AttributeTypeDefs) used in the cohort.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originatorAttributeTypeDef- description of the AttributeTypeDef in the event originator.
     * @param otherMetadataCollectionId - the metadataCollection using the conflicting AttributeTypeDef.
     * @param conflictingAttributeTypeDef - description of the AttributeTypeDef in the other metadata collection.
     * @param errorMessage - details of the error that occurs when the connection is used.
     */
    public void processAttributeTypeDefConflictEvent(String           sourceName,
                                                     String           originatorMetadataCollectionId,
                                                     String           originatorServerName,
                                                     String           originatorServerType,
                                                     String           originatorOrganizationName,
                                                     AttributeTypeDef originatorAttributeTypeDef,
                                                     String           otherMetadataCollectionId,
                                                     AttributeTypeDef conflictingAttributeTypeDef,
                                                     String           errorMessage)
    {
        if (exchangeRule.processTypeDefEvents())
        {
            for (OMRSTypeDefEventProcessor consumer: typeDefEventConsumers)
            {
                consumer.processAttributeTypeDefConflictEvent(sourceName,
                                                              originatorMetadataCollectionId,
                                                              originatorServerName,
                                                              originatorServerType,
                                                              originatorOrganizationName,
                                                              originatorAttributeTypeDef,
                                                              otherMetadataCollectionId,
                                                              conflictingAttributeTypeDef,
                                                              errorMessage);
            }
        }
    }

    /**
     * A TypeDef from another member in the cohort is at a different version than the local repository.  This may
     * create some inconsistencies in the different copies of instances of this type in different members of the
     * cohort.  The recommended action is to update all TypeDefs to the latest version.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param targetMetadataCollectionId - identifier of the metadata collection that is reporting a TypeDef at a
     *                                   different level to the local repository.
     * @param targetTypeDefSummary - details of the target TypeDef
     * @param otherTypeDef - details of the TypeDef in the local repository.
     * @param errorMessage - description of error.
     */
    public void processTypeDefPatchMismatchEvent(String         sourceName,
                                                 String         originatorMetadataCollectionId,
                                                 String         originatorServerName,
                                                 String         originatorServerType,
                                                 String         originatorOrganizationName,
                                                 String         targetMetadataCollectionId,
                                                 TypeDefSummary targetTypeDefSummary,
                                                 TypeDef        otherTypeDef,
                                                 String         errorMessage)
    {
        if (exchangeRule.processTypeDefEvents())
        {
            for (OMRSTypeDefEventProcessor consumer: typeDefEventConsumers)
            {
                consumer.processTypeDefPatchMismatchEvent(sourceName,
                                                          originatorMetadataCollectionId,
                                                          originatorServerName,
                                                          originatorServerType,
                                                          originatorOrganizationName,
                                                          targetMetadataCollectionId,
                                                          targetTypeDefSummary,
                                                          otherTypeDef,
                                                          errorMessage);
            }
        }
    }


    /**
     * A new entity has been created.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param entity - details of the new entity
     */
    public void processNewEntityEvent(String       sourceName,
                                      String       originatorMetadataCollectionId,
                                      String       originatorServerName,
                                      String       originatorServerType,
                                      String       originatorOrganizationName,
                                      EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer : instanceEventConsumers)
                {
                    consumer.processNewEntityEvent(sourceName,
                                                   originatorMetadataCollectionId,
                                                   originatorServerName,
                                                   originatorServerType,
                                                   originatorOrganizationName,
                                                   entity);
                }
            }
        }
    }


    /**
     * An existing entity has been updated.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param entity - details of the new version of the entity.
     */
    public void processUpdatedEntityEvent(String       sourceName,
                                          String       originatorMetadataCollectionId,
                                          String       originatorServerName,
                                          String       originatorServerType,
                                          String       originatorOrganizationName,
                                          EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer : instanceEventConsumers)
                {
                    consumer.processUpdatedEntityEvent(sourceName,
                                                       originatorMetadataCollectionId,
                                                       originatorServerName,
                                                       originatorServerType,
                                                       originatorOrganizationName,
                                                       entity);
                }
            }
        }
    }


    /**
     * An update to an entity has been undone.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param entity - details of the version of the entity that has been restored.
     */
    public void processUndoneEntityEvent(String       sourceName,
                                         String       originatorMetadataCollectionId,
                                         String       originatorServerName,
                                         String       originatorServerType,
                                         String       originatorOrganizationName,
                                         EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer : instanceEventConsumers)
                {
                    consumer.processUndoneEntityEvent(sourceName,
                                                      originatorMetadataCollectionId,
                                                      originatorServerName,
                                                      originatorServerType,
                                                      originatorOrganizationName,
                                                      entity);
                }
            }
        }
    }


    /**
     * A new classification has been added to an entity.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param entity - details of the entity with the new classification added.
     */
    public void processClassifiedEntityEvent(String       sourceName,
                                             String       originatorMetadataCollectionId,
                                             String       originatorServerName,
                                             String       originatorServerType,
                                             String       originatorOrganizationName,
                                             EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer : instanceEventConsumers)
                {
                    consumer.processClassifiedEntityEvent(sourceName,
                                                          originatorMetadataCollectionId,
                                                          originatorServerName,
                                                          originatorServerType,
                                                          originatorOrganizationName,
                                                          entity);
                }
            }
        }
    }


    /**
     * A classification has been removed from an entity.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param entity - details of the entity after the classification has been removed.
     */
    public void processDeclassifiedEntityEvent(String       sourceName,
                                               String       originatorMetadataCollectionId,
                                               String       originatorServerName,
                                               String       originatorServerType,
                                               String       originatorOrganizationName,
                                               EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer : instanceEventConsumers)
                {
                    consumer.processDeclassifiedEntityEvent(sourceName,
                                                            originatorMetadataCollectionId,
                                                            originatorServerName,
                                                            originatorServerType,
                                                            originatorOrganizationName,
                                                            entity);
                }
            }
        }
    }


    /**
     * An existing classification has been changed on an entity.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param entity - details of the entity after the classification has been changed.
     */
    public void processReclassifiedEntityEvent(String       sourceName,
                                               String       originatorMetadataCollectionId,
                                               String       originatorServerName,
                                               String       originatorServerType,
                                               String       originatorOrganizationName,
                                               EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer : instanceEventConsumers)
                {
                    consumer.processReclassifiedEntityEvent(sourceName,
                                                            originatorMetadataCollectionId,
                                                            originatorServerName,
                                                            originatorServerType,
                                                            originatorOrganizationName,
                                                            entity);
                }
            }
        }
    }


    /**
     * An existing entity has been deleted.  This is a soft delete. This means it is still in the repository
     * but it is no longer returned on queries.
     *
     * All relationships to the entity are also soft-deleted and will no longer be usable.  These deleted relationships
     * will be notified through separate events.
     *
     * Details of the TypeDef are included with the entity's unique id (guid) to ensure the right entity is deleted in
     * the remote repositories.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param entity - deleted entity
     */
    public void processDeletedEntityEvent(String       sourceName,
                                          String       originatorMetadataCollectionId,
                                          String       originatorServerName,
                                          String       originatorServerType,
                                          String       originatorOrganizationName,
                                          EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer : instanceEventConsumers)
                {
                    consumer.processDeletedEntityEvent(sourceName,
                                                       originatorMetadataCollectionId,
                                                       originatorServerName,
                                                       originatorServerType,
                                                       originatorOrganizationName,
                                                       entity);
                }
            }
        }
    }


    /**
     * A deleted entity has been permanently removed from the repository.  This request can not be undone.
     *
     * Details of the TypeDef are included with the entity's unique id (guid) to ensure the right entity is purged in
     * the remote repositories.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param typeDefGUID - unique identifier for this entity's TypeDef
     * @param typeDefName - name of this entity's TypeDef
     * @param instanceGUID - unique identifier for the entity
     */
    public void processPurgedEntityEvent(String       sourceName,
                                         String       originatorMetadataCollectionId,
                                         String       originatorServerName,
                                         String       originatorServerType,
                                         String       originatorOrganizationName,
                                         String       typeDefGUID,
                                         String       typeDefName,
                                         String       instanceGUID)
    {
        if (repositoryValidator.validInstanceId(sourceName, typeDefGUID, typeDefName, TypeDefCategory.ENTITY_DEF, instanceGUID))
        {
            if (exchangeRule.processInstanceEvent(typeDefGUID, typeDefName))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processPurgedEntityEvent(sourceName,
                                                      originatorMetadataCollectionId,
                                                      originatorServerName,
                                                      originatorServerType,
                                                      originatorOrganizationName,
                                                      typeDefGUID,
                                                      typeDefName,
                                                      instanceGUID);
                }
            }
        }
    }


    /**
     * A deleted entity has been restored to the state it was before it was deleted.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param entity - details of the version of the entity that has been restored.
     */
    public void processRestoredEntityEvent(String       sourceName,
                                           String       originatorMetadataCollectionId,
                                           String       originatorServerName,
                                           String       originatorServerType,
                                           String       originatorOrganizationName,
                                           EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processRestoredEntityEvent(sourceName,
                                                        originatorMetadataCollectionId,
                                                        originatorServerName,
                                                        originatorServerType,
                                                        originatorOrganizationName,
                                                        entity);
                }
            }
        }
    }


    /**
     * An existing entity has had its type changed.  Typically this action is taken to move an entity's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originalTypeDefSummary - details of this entity's original TypeDef.
     * @param entity - new values for this entity, including the new type information.
     */
    public void processReTypedEntityEvent(String         sourceName,
                                          String         originatorMetadataCollectionId,
                                          String         originatorServerName,
                                          String         originatorServerType,
                                          String         originatorOrganizationName,
                                          TypeDefSummary originalTypeDefSummary,
                                          EntityDetail   entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processReTypedEntityEvent(sourceName,
                                                       originatorMetadataCollectionId,
                                                       originatorServerName,
                                                       originatorServerType,
                                                       originatorOrganizationName,
                                                       originalTypeDefSummary,
                                                       entity);
                }
            }
        }
    }


    /**
     * An existing entity has changed home repository.  This action is taken for example, if a repository
     * becomes permanently unavailable, or if the user community updating this entity move to working
     * from a different repository in the open metadata repository cluster.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originalHomeMetadataCollectionId - unique identifier for the original home metadata collection/repository.
     * @param entity - new values for this entity, including the new home information.
     */
    public void processReHomedEntityEvent(String       sourceName,
                                          String       originatorMetadataCollectionId,
                                          String       originatorServerName,
                                          String       originatorServerType,
                                          String       originatorOrganizationName,
                                          String       originalHomeMetadataCollectionId,
                                          EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processReHomedEntityEvent(sourceName,
                                                       originatorMetadataCollectionId,
                                                       originatorServerName,
                                                       originatorServerType,
                                                       originatorOrganizationName,
                                                       originalHomeMetadataCollectionId,
                                                       entity);
                }
            }
        }
    }


    /**
     * The guid of an existing entity has been changed to a new value.  This is used if two different
     * entities are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originalEntityGUID - the existing identifier for the entity.
     * @param entity - new values for this entity, including the new guid.
     */
    public void processReIdentifiedEntityEvent(String       sourceName,
                                               String       originatorMetadataCollectionId,
                                               String       originatorServerName,
                                               String       originatorServerType,
                                               String       originatorOrganizationName,
                                               String       originalEntityGUID,
                                               EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processReIdentifiedEntityEvent(sourceName,
                                                            originatorMetadataCollectionId,
                                                            originatorServerName,
                                                            originatorServerType,
                                                            originatorOrganizationName,
                                                            originalEntityGUID,
                                                            entity);
                }
            }
        }
    }


    /**
     * The local repository is requesting that an entity from another repository's metadata collection is
     * refreshed so the local repository can create a reference copy.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param typeDefGUID - - unique identifier for this entity's TypeDef
     * @param typeDefName - name of this entity's TypeDef
     * @param instanceGUID - unique identifier for the entity
     * @param homeMetadataCollectionId - metadata collection id for the home of this instance.
     */
    public void processRefreshEntityRequested(String       sourceName,
                                              String       originatorMetadataCollectionId,
                                              String       originatorServerName,
                                              String       originatorServerType,
                                              String       originatorOrganizationName,
                                              String       typeDefGUID,
                                              String       typeDefName,
                                              String       instanceGUID,
                                              String       homeMetadataCollectionId)
    {
        if (repositoryValidator.validInstanceId(sourceName, typeDefGUID, typeDefName, TypeDefCategory.ENTITY_DEF, instanceGUID))
        {
            if (exchangeRule.processInstanceEvent(typeDefGUID, typeDefName))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processRefreshEntityRequested(sourceName,
                                                           originatorMetadataCollectionId,
                                                           originatorServerName,
                                                           originatorServerType,
                                                           originatorOrganizationName,
                                                           typeDefGUID,
                                                           typeDefName,
                                                           instanceGUID,
                                                           homeMetadataCollectionId);
                }
            }
        }
    }


    /**
     * A remote repository in the cohort has sent entity details in response to a refresh request.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param entity - details of the requested entity
     */
    public void processRefreshEntityEvent(String       sourceName,
                                          String       originatorMetadataCollectionId,
                                          String       originatorServerName,
                                          String       originatorServerType,
                                          String       originatorOrganizationName,
                                          EntityDetail entity)
    {
        if (repositoryValidator.validEntity(sourceName, entity))
        {
            if (exchangeRule.processInstanceEvent(entity))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processRefreshEntityEvent(sourceName,
                                                       originatorMetadataCollectionId,
                                                       originatorServerName,
                                                       originatorServerType,
                                                       originatorOrganizationName,
                                                       entity);
                }
            }
        }
    }


    /**
     * A new relationship has been created.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param relationship - details of the new relationship
     */
    public void processNewRelationshipEvent(String       sourceName,
                                            String       originatorMetadataCollectionId,
                                            String       originatorServerName,
                                            String       originatorServerType,
                                            String       originatorOrganizationName,
                                            Relationship relationship)
    {
        if (repositoryValidator.validRelationship(sourceName, relationship))
        {
            if (exchangeRule.processInstanceEvent(relationship))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processNewRelationshipEvent(sourceName,
                                                         originatorMetadataCollectionId,
                                                         originatorServerName,
                                                         originatorServerType,
                                                         originatorOrganizationName,
                                                         relationship);
                }
            }
        }
    }


    /**
     * An existing relationship has been updated.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param relationship - details of the new version of the relationship.
     */
    public void processUpdatedRelationshipEvent(String       sourceName,
                                                String       originatorMetadataCollectionId,
                                                String       originatorServerName,
                                                String       originatorServerType,
                                                String       originatorOrganizationName,
                                                Relationship relationship)
    {
        if (repositoryValidator.validRelationship(sourceName, relationship))
        {
            if (exchangeRule.processInstanceEvent(relationship))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processUpdatedRelationshipEvent(sourceName,
                                                             originatorMetadataCollectionId,
                                                             originatorServerName,
                                                             originatorServerType,
                                                             originatorOrganizationName,
                                                             relationship);
                }
            }
        }
    }

    /**
     * An update to a relationship has been undone.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param relationship - details of the version of the relationship that has been restored.
     */
    public void processUndoneRelationshipEvent(String       sourceName,
                                               String       originatorMetadataCollectionId,
                                               String       originatorServerName,
                                               String       originatorServerType,
                                               String       originatorOrganizationName,
                                               Relationship relationship)
    {
        if (repositoryValidator.validRelationship(sourceName, relationship))
        {
            if (exchangeRule.processInstanceEvent(relationship))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processUndoneRelationshipEvent(sourceName,
                                                            originatorMetadataCollectionId,
                                                            originatorServerName,
                                                            originatorServerType,
                                                            originatorOrganizationName,
                                                            relationship);
                }
            }
        }
    }


    /**
     * An existing relationship has been deleted.  This is a soft delete. This means it is still in the repository
     * but it is no longer returned on queries.
     *
     * Details of the TypeDef are included with the relationship's unique id (guid) to ensure the right
     * relationship is deleted in the remote repositories.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param relationship - deleted relationship
     */
    public void processDeletedRelationshipEvent(String       sourceName,
                                                String       originatorMetadataCollectionId,
                                                String       originatorServerName,
                                                String       originatorServerType,
                                                String       originatorOrganizationName,
                                                Relationship relationship)
    {
        if (repositoryValidator.validRelationship(sourceName, relationship))
        {
            if (exchangeRule.processInstanceEvent(relationship))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processDeletedRelationshipEvent(sourceName,
                                                             originatorMetadataCollectionId,
                                                             originatorServerName,
                                                             originatorServerType,
                                                             originatorOrganizationName,
                                                             relationship);
                }
            }
        }
    }


    /**
     * A deleted relationship has been permanently removed from the repository.  This request can not be undone.
     *
     * Details of the TypeDef are included with the relationship's unique id (guid) to ensure the right
     * relationship is purged in the remote repositories.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param typeDefGUID - unique identifier for this relationship's TypeDef.
     * @param typeDefName - name of this relationship's TypeDef.
     * @param instanceGUID - unique identifier for the relationship.
     */
    public void processPurgedRelationshipEvent(String       sourceName,
                                               String       originatorMetadataCollectionId,
                                               String       originatorServerName,
                                               String       originatorServerType,
                                               String       originatorOrganizationName,
                                               String       typeDefGUID,
                                               String       typeDefName,
                                               String       instanceGUID)
    {
        if (repositoryValidator.validInstanceId(sourceName, typeDefGUID, typeDefName, TypeDefCategory.RELATIONSHIP_DEF, instanceGUID))
        {
            if (exchangeRule.processInstanceEvent(typeDefGUID, typeDefName))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processPurgedRelationshipEvent(sourceName,
                                                            originatorMetadataCollectionId,
                                                            originatorServerName,
                                                            originatorServerType,
                                                            originatorOrganizationName,
                                                            typeDefGUID,
                                                            typeDefName,
                                                            instanceGUID);
                }
            }
        }
    }


    /**
     * A deleted relationship has been restored to the state it was before it was deleted.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param relationship - details of the version of the relationship that has been restored.
     */
    public void processRestoredRelationshipEvent(String       sourceName,
                                                 String       originatorMetadataCollectionId,
                                                 String       originatorServerName,
                                                 String       originatorServerType,
                                                 String       originatorOrganizationName,
                                                 Relationship relationship)
    {
        if (repositoryValidator.validRelationship(sourceName, relationship))
        {
            if (exchangeRule.processInstanceEvent(relationship))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processRestoredRelationshipEvent(sourceName,
                                                              originatorMetadataCollectionId,
                                                              originatorServerName,
                                                              originatorServerType,
                                                              originatorOrganizationName,
                                                              relationship);
                }
            }
        }
    }


    /**
     * An existing relationship has had its type changed.  Typically this action is taken to move a relationship's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originalTypeDefSummary - original details for this relationship's TypeDef.
     * @param relationship - new values for this relationship, including the new type information.
     */
    public void processReTypedRelationshipEvent(String         sourceName,
                                                String         originatorMetadataCollectionId,
                                                String         originatorServerName,
                                                String         originatorServerType,
                                                String         originatorOrganizationName,
                                                TypeDefSummary originalTypeDefSummary,
                                                Relationship   relationship)
    {
        if (repositoryValidator.validRelationship(sourceName, relationship))
        {
            if (exchangeRule.processInstanceEvent(relationship))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processReTypedRelationshipEvent(sourceName,
                                                             originatorMetadataCollectionId,
                                                             originatorServerName,
                                                             originatorServerType,
                                                             originatorOrganizationName,
                                                             originalTypeDefSummary,
                                                             relationship);
                }
            }
        }
    }


    /**
     * An existing relationship has changed home repository.  This action is taken for example, if a repository
     * becomes permanently unavailable, or if the user community updating this relationship move to working
     * from a different repository in the open metadata repository cluster.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originalHomeMetadataCollection - unique identifier for the original home repository.
     * @param relationship - new values for this relationship, including the new home information.
     */
    public void processReHomedRelationshipEvent(String       sourceName,
                                                String       originatorMetadataCollectionId,
                                                String       originatorServerName,
                                                String       originatorServerType,
                                                String       originatorOrganizationName,
                                                String       originalHomeMetadataCollection,
                                                Relationship relationship)
    {
        if (repositoryValidator.validRelationship(sourceName, relationship))
        {
            if (exchangeRule.processInstanceEvent(relationship))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processReHomedRelationshipEvent(sourceName,
                                                             originatorMetadataCollectionId,
                                                             originatorServerName,
                                                             originatorServerType,
                                                             originatorOrganizationName,
                                                             originalHomeMetadataCollection,
                                                             relationship);
                }
            }
        }
    }


    /**
     * The guid of an existing relationship has changed.  This is used if two different
     * relationships are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param originalRelationshipGUID - the existing identifier for the relationship.
     * @param relationship - new values for this relationship, including the new guid.
     */
    public void processReIdentifiedRelationshipEvent(String       sourceName,
                                                     String       originatorMetadataCollectionId,
                                                     String       originatorServerName,
                                                     String       originatorServerType,
                                                     String       originatorOrganizationName,
                                                     String       originalRelationshipGUID,
                                                     Relationship relationship)
    {
        if (repositoryValidator.validRelationship(sourceName, relationship))
        {
            if (exchangeRule.processInstanceEvent(relationship))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processReIdentifiedRelationshipEvent(sourceName,
                                                                  originatorMetadataCollectionId,
                                                                  originatorServerName,
                                                                  originatorServerType,
                                                                  originatorOrganizationName,
                                                                  originalRelationshipGUID,
                                                                  relationship);
                }
            }
        }
    }


    /**
     * A repository has requested the home repository of a relationship send details of the relationship so
     * its local metadata collection can create a reference copy of the instance.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param typeDefGUID - unique identifier for this instance's TypeDef
     * @param typeDefName - name of this relationship's TypeDef
     * @param instanceGUID - unique identifier for the instance
     * @param homeMetadataCollectionId - metadata collection id for the home of this instance.
     */
    public void processRefreshRelationshipRequest(String       sourceName,
                                                  String       originatorMetadataCollectionId,
                                                  String       originatorServerName,
                                                  String       originatorServerType,
                                                  String       originatorOrganizationName,
                                                  String       typeDefGUID,
                                                  String       typeDefName,
                                                  String       instanceGUID,
                                                  String       homeMetadataCollectionId)
    {
        if (repositoryValidator.validInstanceId(sourceName, typeDefGUID, typeDefName, TypeDefCategory.RELATIONSHIP_DEF, instanceGUID))
        {
            if (exchangeRule.processInstanceEvent(typeDefGUID, typeDefName))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processRefreshRelationshipRequest(sourceName,
                                                               originatorMetadataCollectionId,
                                                               originatorServerName,
                                                               originatorServerType,
                                                               originatorOrganizationName,
                                                               typeDefGUID,
                                                               typeDefName,
                                                               instanceGUID,
                                                               homeMetadataCollectionId);
                }
            }
        }
    }


    /**
     * The local repository is refreshing the information about a relationship for the other
     * repositories in the cohort.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param relationship - relationship details
     */
    public void processRefreshRelationshipEvent(String       sourceName,
                                                String       originatorMetadataCollectionId,
                                                String       originatorServerName,
                                                String       originatorServerType,
                                                String       originatorOrganizationName,
                                                Relationship relationship)
    {
        if (repositoryValidator.validRelationship(sourceName, relationship))
        {
            if (exchangeRule.processInstanceEvent(relationship))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processRefreshRelationshipEvent(sourceName,
                                                             originatorMetadataCollectionId,
                                                             originatorServerName,
                                                             originatorServerType,
                                                             originatorOrganizationName,
                                                             relationship);
                }
            }
        }
    }

    /**
     * A remote repository has detected two metadata instances with the same identifier (guid).  One of these instances
     * has its home in the repository and the other is located in a metadata collection owned by another
     * repository in the cohort.  This is a serious error because it could lead to corruption of the metadata collection.
     * When this occurs, all repositories in the cohort delete their reference copies of the metadata instances and
     * at least one of the instances has its GUID changed in its respective home repository.  The updated instance(s)
     * are redistributed around the cohort.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - metadata collection id of the repository reporting the conflicting instance
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param targetMetadataCollectionId - metadata collection id of other repository with the conflicting instance
     * @param targetTypeDefSummary - details of the target instance's TypeDef
     * @param targetInstanceGUID - unique identifier for the source instance
     * @param otherOrigin - origin of the other (older) metadata instance
     * @param otherMetadataCollectionId - metadata collection of the other (older) metadata instance
     * @param otherTypeDefSummary - details of the other (older) instance's TypeDef
     * @param otherInstanceGUID - unique identifier for the other (older) instance
     * @param errorMessage - description of the error
     */
    public void processConflictingInstancesEvent(String                 sourceName,
                                                 String                 originatorMetadataCollectionId,
                                                 String                 originatorServerName,
                                                 String                 originatorServerType,
                                                 String                 originatorOrganizationName,
                                                 String                 targetMetadataCollectionId,
                                                 TypeDefSummary         targetTypeDefSummary,
                                                 String                 targetInstanceGUID,
                                                 String                 otherMetadataCollectionId,
                                                 InstanceProvenanceType otherOrigin,
                                                 TypeDefSummary         otherTypeDefSummary,
                                                 String                 otherInstanceGUID,
                                                 String                 errorMessage)
    {
        if ((repositoryValidator.validTypeDefSummary(sourceName, targetTypeDefSummary)) &&
            (repositoryValidator.validTypeDefSummary(sourceName, otherTypeDefSummary)))
        {
            if ((exchangeRule.processInstanceEvent(targetTypeDefSummary)) ||
                (exchangeRule.processInstanceEvent(otherTypeDefSummary)))
            {
                for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
                {
                    consumer.processConflictingInstancesEvent(sourceName,
                                                              originatorMetadataCollectionId,
                                                              originatorServerName,
                                                              originatorServerType,
                                                              originatorOrganizationName,
                                                              targetMetadataCollectionId,
                                                              targetTypeDefSummary,
                                                              targetInstanceGUID,
                                                              otherMetadataCollectionId,
                                                              otherOrigin,
                                                              otherTypeDefSummary,
                                                              otherInstanceGUID,
                                                              errorMessage);
                }
            }
        }
    }


    /**
     * An open metadata repository has detected an inconsistency in the version of the type used in an updated metadata
     * instance compared to its stored version.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - metadata collection id of the repository reporting the conflicting instance
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param targetMetadataCollectionId - metadata collection id of other repository with the conflicting instance
     * @param targetTypeDefSummary - details of the target instance's TypeDef
     * @param targetInstanceGUID - unique identifier for the source instance
     * @param otherTypeDefSummary - details of the other's TypeDef
     * @param errorMessage - description of the error.
     */
     public void processConflictingTypeEvent(String                 sourceName,
                                             String                 originatorMetadataCollectionId,
                                             String                 originatorServerName,
                                             String                 originatorServerType,
                                             String                 originatorOrganizationName,
                                             String                 targetMetadataCollectionId,
                                             TypeDefSummary         targetTypeDefSummary,
                                             String                 targetInstanceGUID,
                                             TypeDefSummary         otherTypeDefSummary,
                                             String                 errorMessage)
    {
        if (exchangeRule.processInstanceEvent(targetTypeDefSummary))
        {
            for (OMRSInstanceEventProcessor consumer: instanceEventConsumers)
            {
                consumer.processConflictingTypeEvent(sourceName,
                                                     originatorMetadataCollectionId,
                                                     originatorServerName,
                                                     originatorServerType,
                                                     originatorOrganizationName,
                                                     targetMetadataCollectionId,
                                                     targetTypeDefSummary,
                                                     targetInstanceGUID,
                                                     otherTypeDefSummary,
                                                     errorMessage);
            }
        }
    }
}
