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
package org.apache.atlas.omrs.localrepository.repositoryconnector;

import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omrs.auditlog.OMRSAuditCode;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.auditlog.OMRSAuditingComponent;
import org.apache.atlas.omrs.eventmanagement.*;
import org.apache.atlas.omrs.eventmanagement.events.OMRSInstanceEventProcessor;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.*;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.properties.instances.EntityDetail;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceProvenanceType;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceType;
import org.apache.atlas.omrs.metadatacollection.properties.instances.Relationship;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * LocalOMRSInstanceEventProcessor processes incoming metadata instance events that describe changes to the
 * entities and relationships in other repositories in the connected cohorts.
 * It uses the save exchange rule to decide which events to process and which to ignore.
 * Events that are to be processed are converted into reference copies of their respective entities and
 * relationships and stored in the local repository.
 */
public class LocalOMRSInstanceEventProcessor implements OMRSInstanceEventProcessor, OMRSInstanceRetrievalEventProcessor
{
    private String                          localMetadataCollectionId;
    private String                          localServerName;
    private OMRSRepositoryConnector         realLocalConnector;
    private OMRSRepositoryHelper            repositoryHelper;
    private OMRSRepositoryValidator         repositoryValidator;
    private OMRSRepositoryEventExchangeRule saveExchangeRule;
    private String                          realRepositoryName = "Local Repository";
    private OMRSMetadataCollection          realMetadataCollection = null;
    private OMRSRepositoryEventProcessor    outboundRepositoryEventProcessor = null;



    /*
     * The audit log provides a verifiable record of the open metadata archives that have been loaded into
     * the open metadata repository.  The Logger is for standard debug.
     */
    private static final OMRSAuditLog auditLog = new OMRSAuditLog(OMRSAuditingComponent.INSTANCE_EVENT_PROCESSOR);
    private static final Logger       log      = LoggerFactory.getLogger(LocalOMRSInstanceEventProcessor.class);


    /**
     * Constructor saves all of the information necessary to process incoming instance events.  It is intolerant
     * of nulls in any of its parameters and will throw a logic error exception is it finds any.
     *
     * @param localMetadataCollectionId - local metadata collection identifier
     * @param localServerName           - name of the local server - for logging
     * @param realLocalConnector        - connector to the real local repository
     * @param repositoryHelper          - helper class for building instances
     * @param repositoryValidator       - helper class for validating instances
     * @param saveExchangeRule          - rule that determines which events to process.
     */
    LocalOMRSInstanceEventProcessor(String                          localMetadataCollectionId,
                                    String                          localServerName,
                                    OMRSRepositoryConnector         realLocalConnector,
                                    OMRSRepositoryHelper            repositoryHelper,
                                    OMRSRepositoryValidator         repositoryValidator,
                                    OMRSRepositoryEventExchangeRule saveExchangeRule,
                                    OMRSRepositoryEventProcessor    outboundRepositoryEventProcessor)
    {
        final String methodName = "LocalOMRSInstanceEventProcessor constructor";

        this.localMetadataCollectionId = localMetadataCollectionId;
        this.localServerName = localServerName;
        this.realLocalConnector = realLocalConnector;
        this.repositoryHelper = repositoryHelper;
        this.repositoryValidator = repositoryValidator;
        this.saveExchangeRule = saveExchangeRule;

        if (realLocalConnector != null)
        {
            try
            {
                this.realMetadataCollection = realLocalConnector.getMetadataCollection();
            }
            catch (Throwable  error)
            {
                /*
                 * Nothing to do - will be logged in verifyEventProcessor
                 */
                this.realMetadataCollection = null;
            }
        }

        this.verifyEventProcessor(methodName);

        Connection connection = this.realLocalConnector.getConnection();
        if (connection != null)
        {
            this.realRepositoryName = connection.getConnectionName();
        }
    }


    /*
     * ====================================
     * OMRSInstanceEventProcessor
     */


    /**
     * A new entity has been created.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param entity                         - details of the new entity
     */
    public void processNewEntityEvent(String       sourceName,
                                      String       originatorMetadataCollectionId,
                                      String       originatorServerName,
                                      String       originatorServerType,
                                      String       originatorOrganizationName,
                                      EntityDetail entity)
    {
        final String methodName = "processNewEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * An existing entity has been updated.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param entity                         - details of the new version of the entity.
     */
    public void processUpdatedEntityEvent(String       sourceName,
                                          String       originatorMetadataCollectionId,
                                          String       originatorServerName,
                                          String       originatorServerType,
                                          String       originatorOrganizationName,
                                          EntityDetail entity)
    {
        final String methodName = "processUpdatedEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * An update to an entity has been undone.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param entity                         - details of the version of the entity that has been restored.
     */
    public void processUndoneEntityEvent(String       sourceName,
                                         String       originatorMetadataCollectionId,
                                         String       originatorServerName,
                                         String       originatorServerType,
                                         String       originatorOrganizationName,
                                         EntityDetail entity)
    {
        final String methodName = "processUndoneEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * A new classification has been added to an entity.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param entity                         - details of the entity with the new classification added.
     */
    public void processClassifiedEntityEvent(String sourceName,
                                             String originatorMetadataCollectionId,
                                             String originatorServerName,
                                             String originatorServerType,
                                             String originatorOrganizationName,
                                             EntityDetail entity)
    {
        final String methodName = "processClassifiedEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * A classification has been removed from an entity.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param entity                         - details of the entity after the classification has been removed.
     */
    public void processDeclassifiedEntityEvent(String sourceName,
                                               String originatorMetadataCollectionId,
                                               String originatorServerName,
                                               String originatorServerType,
                                               String originatorOrganizationName,
                                               EntityDetail entity)
    {
        final String methodName = "processDeclassifiedEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * An existing classification has been changed on an entity.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param entity                         - details of the entity after the classification has been changed.
     */
    public void processReclassifiedEntityEvent(String       sourceName,
                                               String       originatorMetadataCollectionId,
                                               String       originatorServerName,
                                               String       originatorServerType,
                                               String       originatorOrganizationName,
                                               EntityDetail entity)
    {
        final String methodName = "processReclassifiedEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * An existing entity has been deleted.  This is a soft delete. This means it is still in the repository
     * but it is no longer returned on queries.
     * <p>
     * All relationships to the entity are also soft-deleted and will no longer be usable.  These deleted relationships
     * will be notified through separate events.
     * <p>
     * Details of the TypeDef are included with the entity's unique id (guid) to ensure the right entity is deleted in
     * the remote repositories.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param entity                         - deleted entity
     */
    public void processDeletedEntityEvent(String       sourceName,
                                          String       originatorMetadataCollectionId,
                                          String       originatorServerName,
                                          String       originatorServerType,
                                          String       originatorOrganizationName,
                                          EntityDetail entity)
    {
        final String methodName = "processDeletedEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * A deleted entity has been permanently removed from the repository.  This request can not be undone.
     * <p>
     * Details of the TypeDef are included with the entity's unique id (guid) to ensure the right entity is purged in
     * the remote repositories.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param typeDefGUID                    - unique identifier for this entity's TypeDef
     * @param typeDefName                    - name of this entity's TypeDef
     * @param instanceGUID                   - unique identifier for the entity
     */
    public void processPurgedEntityEvent(String sourceName,
                                         String originatorMetadataCollectionId,
                                         String originatorServerName,
                                         String originatorServerType,
                                         String originatorOrganizationName,
                                         String typeDefGUID,
                                         String typeDefName,
                                         String instanceGUID)
    {
        final String methodName = "processPurgedEntityEvent";

        purgeReferenceInstance(sourceName,
                               methodName,
                               originatorMetadataCollectionId,
                               originatorServerName,
                               typeDefGUID,
                               typeDefName,
                               instanceGUID);
    }


    /**
     * A deleted entity has been restored to the state it was before it was deleted.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param entity                         - details of the version of the entity that has been restored.
     */
    public void processRestoredEntityEvent(String       sourceName,
                                           String       originatorMetadataCollectionId,
                                           String       originatorServerName,
                                           String       originatorServerType,
                                           String       originatorOrganizationName,
                                           EntityDetail entity)
    {
        final String methodName = "processRestoredEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * The guid of an existing entity has been changed to a new value.  This is used if two different
     * entities are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param originalEntityGUID             - the existing identifier for the entity.
     * @param entity                         - new values for this entity, including the new guid.
     */
    public void processReIdentifiedEntityEvent(String       sourceName,
                                               String       originatorMetadataCollectionId,
                                               String       originatorServerName,
                                               String       originatorServerType,
                                               String       originatorOrganizationName,
                                               String       originalEntityGUID,
                                               EntityDetail entity)
    {
        final String methodName = "processReIdentifiedEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * An existing entity has had its type changed.  Typically this action is taken to move an entity's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param originalTypeDefSummary         - original details of this entity's TypeDef.
     * @param entity                         - new values for this entity, including the new type information.
     */
    public void processReTypedEntityEvent(String         sourceName,
                                          String         originatorMetadataCollectionId,
                                          String         originatorServerName,
                                          String         originatorServerType,
                                          String         originatorOrganizationName,
                                          TypeDefSummary originalTypeDefSummary,
                                          EntityDetail   entity)
    {
        final String methodName = "processReTypedEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * An existing entity has changed home repository.  This action is taken for example, if a repository
     * becomes permanently unavailable, or if the user community updating this entity move to working
     * from a different repository in the open metadata repository cluster.
     *
     * @param sourceName                       - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                         local repository, or event mapper name.
     * @param originatorMetadataCollectionId   - unique identifier for the metadata collection hosted by the server that
     *                                         sent the event.
     * @param originatorServerName             - name of the server that the event came from.
     * @param originatorServerType             - type of server that the event came from.
     * @param originatorOrganizationName       - name of the organization that owns the server that sent the event.
     * @param originalHomeMetadataCollectionId - unique identifier for the original home repository.
     * @param entity                           - new values for this entity, including the new home information.
     */
    public void processReHomedEntityEvent(String       sourceName,
                                          String       originatorMetadataCollectionId,
                                          String       originatorServerName,
                                          String       originatorServerType,
                                          String       originatorOrganizationName,
                                          String       originalHomeMetadataCollectionId,
                                          EntityDetail entity)
    {
        final String methodName = "processReHomedEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * The local repository is requesting that an entity from another repository's metadata collection is
     * refreshed so the local repository can create a reference copy.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param typeDefGUID                    - - unique identifier for this entity's TypeDef
     * @param typeDefName                    - name of this entity's TypeDef
     * @param instanceGUID                   - unique identifier for the entity
     * @param homeMetadataCollectionId       - metadata collection id for the home of this instance.
     */
    public void processRefreshEntityRequested(String sourceName,
                                              String originatorMetadataCollectionId,
                                              String originatorServerName,
                                              String originatorServerType,
                                              String originatorOrganizationName,
                                              String typeDefGUID,
                                              String typeDefName,
                                              String instanceGUID,
                                              String homeMetadataCollectionId)
    {
        final String  methodName = "processRefreshEntityRequested";

        try
        {
            verifyEventProcessor(methodName);

            realMetadataCollection.refreshEntityReferenceCopy(sourceName,
                                                              instanceGUID,
                                                              typeDefGUID,
                                                              typeDefName,
                                                              originatorMetadataCollectionId);

        }
        catch (Throwable error)
        {
            OMRSAuditCode auditCode = OMRSAuditCode.UNEXPECTED_EXCEPTION_FROM_EVENT;
            auditLog.logRecord(methodName,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(methodName,
                                                                originatorServerName,
                                                                originatorMetadataCollectionId,
                                                                error.getMessage()),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());
        }
    }


    /**
     * A remote repository in the cohort has sent entity details in response to a refresh request.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param entity                         - details of the requested entity
     */
    public void processRefreshEntityEvent(String       sourceName,
                                          String       originatorMetadataCollectionId,
                                          String       originatorServerName,
                                          String       originatorServerType,
                                          String       originatorOrganizationName,
                                          EntityDetail entity)
    {
        final String methodName = "processReHomedEntityEvent";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              originatorMetadataCollectionId,
                              originatorServerName,
                              entity);
    }


    /**
     * A new relationship has been created.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param relationship                   - details of the new relationship
     */
    public void processNewRelationshipEvent(String       sourceName,
                                            String       originatorMetadataCollectionId,
                                            String       originatorServerName,
                                            String       originatorServerType,
                                            String       originatorOrganizationName,
                                            Relationship relationship)
    {
        final String methodName = "processNewRelationshipEvent";
        final String entityParameterName = "relationship";

        updateReferenceRelationship(sourceName,
                                    methodName,
                                    entityParameterName,
                                    originatorMetadataCollectionId,
                                    originatorServerName,
                                    relationship);
    }


    /**
     * An existing relationship has been updated.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param relationship                   - details of the new version of the relationship.
     */
    public void processUpdatedRelationshipEvent(String       sourceName,
                                                String       originatorMetadataCollectionId,
                                                String       originatorServerName,
                                                String       originatorServerType,
                                                String       originatorOrganizationName,
                                                Relationship relationship)
    {
        final String methodName = "processUpdatedRelationshipEvent";
        final String entityParameterName = "relationship";

        updateReferenceRelationship(sourceName,
                                    methodName,
                                    entityParameterName,
                                    originatorMetadataCollectionId,
                                    originatorServerName,
                                    relationship);
    }


    /**
     * An update to a relationship has been undone.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param relationship                   - details of the version of the relationship that has been restored.
     */
    public void processUndoneRelationshipEvent(String       sourceName,
                                               String       originatorMetadataCollectionId,
                                               String       originatorServerName,
                                               String       originatorServerType,
                                               String       originatorOrganizationName,
                                               Relationship relationship)
    {
        final String methodName = "processUndoneRelationshipEvent";
        final String entityParameterName = "relationship";

        updateReferenceRelationship(sourceName,
                                    methodName,
                                    entityParameterName,
                                    originatorMetadataCollectionId,
                                    originatorServerName,
                                    relationship);
    }


    /**
     * An existing relationship has been deleted.  This is a soft delete. This means it is still in the repository
     * but it is no longer returned on queries.
     * <p>
     * Details of the TypeDef are included with the relationship's unique id (guid) to ensure the right
     * relationship is deleted in the remote repositories.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param relationship                   - deleted relationship
     */
    public void processDeletedRelationshipEvent(String       sourceName,
                                                String       originatorMetadataCollectionId,
                                                String       originatorServerName,
                                                String       originatorServerType,
                                                String       originatorOrganizationName,
                                                Relationship relationship)
    {
        final String methodName = "processDeletedRelationshipEvent";
        final String entityParameterName = "relationship";

        updateReferenceRelationship(sourceName,
                                    methodName,
                                    entityParameterName,
                                    originatorMetadataCollectionId,
                                    originatorServerName,
                                    relationship);
    }


    /**
     * A deleted relationship has been permanently removed from the repository.  This request can not be undone.
     * <p>
     * Details of the TypeDef are included with the relationship's unique id (guid) to ensure the right
     * relationship is purged in the remote repositories.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param typeDefGUID                    - unique identifier for this relationship's TypeDef.
     * @param typeDefName                    - name of this relationship's TypeDef.
     * @param instanceGUID                   - unique identifier for the relationship.
     */
    public void processPurgedRelationshipEvent(String sourceName,
                                               String originatorMetadataCollectionId,
                                               String originatorServerName,
                                               String originatorServerType,
                                               String originatorOrganizationName,
                                               String typeDefGUID,
                                               String typeDefName,
                                               String instanceGUID)
    {
        final String methodName = "processPurgedRelationshipEvent";

        purgeReferenceInstance(sourceName,
                               methodName,
                               originatorMetadataCollectionId,
                               originatorServerName,
                               typeDefGUID,
                               typeDefName,
                               instanceGUID);
    }


    /**
     * A deleted relationship has been restored to the state it was before it was deleted.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param relationship                   - details of the version of the relationship that has been restored.
     */
    public void processRestoredRelationshipEvent(String       sourceName,
                                                 String       originatorMetadataCollectionId,
                                                 String       originatorServerName,
                                                 String       originatorServerType,
                                                 String       originatorOrganizationName,
                                                 Relationship relationship)
    {
        final String methodName = "processRestoredRelationshipEvent";
        final String entityParameterName = "relationship";

        updateReferenceRelationship(sourceName,
                                    methodName,
                                    entityParameterName,
                                    originatorMetadataCollectionId,
                                    originatorServerName,
                                    relationship);
    }


    /**
     * The guid of an existing relationship has changed.  This is used if two different
     * relationships are discovered to have the same guid.  This is extremely unlikely but not impossible so
     * the open metadata protocol has provision for this.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param originalRelationshipGUID       - the existing identifier for the relationship.
     * @param relationship                   - new values for this relationship, including the new guid.
     */
    public void processReIdentifiedRelationshipEvent(String       sourceName,
                                                     String       originatorMetadataCollectionId,
                                                     String       originatorServerName,
                                                     String       originatorServerType,
                                                     String       originatorOrganizationName,
                                                     String       originalRelationshipGUID,
                                                     Relationship relationship)
    {
        final String methodName = "processReIdentifiedRelationshipEvent";
        final String entityParameterName = "relationship";

        updateReferenceRelationship(sourceName,
                                    methodName,
                                    entityParameterName,
                                    originatorMetadataCollectionId,
                                    originatorServerName,
                                    relationship);
    }


    /**
     * An existing relationship has had its type changed.  Typically this action is taken to move a relationship's
     * type to either a super type (so the subtype can be deleted) or a new subtype (so additional properties can be
     * added.)  However, the type can be changed to any compatible type.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param originalTypeDefSummary         - original details of this relationship's TypeDef.
     * @param relationship                   - new values for this relationship, including the new type information.
     */
    public void processReTypedRelationshipEvent(String         sourceName,
                                                String         originatorMetadataCollectionId,
                                                String         originatorServerName,
                                                String         originatorServerType,
                                                String         originatorOrganizationName,
                                                TypeDefSummary originalTypeDefSummary,
                                                Relationship   relationship)
    {
        final String methodName = "processReTypedRelationshipEvent";
        final String entityParameterName = "relationship";

        updateReferenceRelationship(sourceName,
                                    methodName,
                                    entityParameterName,
                                    originatorMetadataCollectionId,
                                    originatorServerName,
                                    relationship);
    }


    /**
     * An existing relationship has changed home repository.  This action is taken for example, if a repository
     * becomes permanently unavailable, or if the user community updating this relationship move to working
     * from a different repository in the open metadata repository cluster.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param originalHomeMetadataCollection - unique identifier for the original home repository.
     * @param relationship                   - new values for this relationship, including the new home information.
     */
    public void processReHomedRelationshipEvent(String       sourceName,
                                                String       originatorMetadataCollectionId,
                                                String       originatorServerName,
                                                String       originatorServerType,
                                                String       originatorOrganizationName,
                                                String       originalHomeMetadataCollection,
                                                Relationship relationship)
    {
        final String methodName = "processReHomedRelationshipEvent";
        final String entityParameterName = "relationship";

        updateReferenceRelationship(sourceName,
                                    methodName,
                                    entityParameterName,
                                    originatorMetadataCollectionId,
                                    originatorServerName,
                                    relationship);
    }


    /**
     * A repository has requested the home repository of a relationship send details of the relationship so
     * the local repository can create a reference copy of the instance.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param typeDefGUID                    - unique identifier for this instance's TypeDef
     * @param typeDefName                    - name of this relationship's TypeDef
     * @param instanceGUID                   - unique identifier for the instance
     * @param homeMetadataCollectionId       - metadata collection id for the home of this instance.
     */
    public void processRefreshRelationshipRequest(String sourceName,
                                                  String originatorMetadataCollectionId,
                                                  String originatorServerName,
                                                  String originatorServerType,
                                                  String originatorOrganizationName,
                                                  String typeDefGUID,
                                                  String typeDefName,
                                                  String instanceGUID,
                                                  String homeMetadataCollectionId)
    {
        final String    methodName = "processRefreshRelationshipRequest";

        try
        {
            verifyEventProcessor(methodName);

            realMetadataCollection.refreshRelationshipReferenceCopy(sourceName,
                                                                    instanceGUID,
                                                                    typeDefGUID,
                                                                    typeDefName,
                                                                    originatorMetadataCollectionId);

        }
        catch (Throwable error)
        {
            OMRSAuditCode auditCode = OMRSAuditCode.UNEXPECTED_EXCEPTION_FROM_EVENT;
            auditLog.logRecord(methodName,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(methodName,
                                                                originatorServerName,
                                                                originatorMetadataCollectionId,
                                                                error.getMessage()),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());
        }
    }


    /**
     * The local repository is refreshing the information about a relationship for the other
     * repositories in the cohort.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param relationship                   - relationship details
     */
    public void processRefreshRelationshipEvent(String       sourceName,
                                                String       originatorMetadataCollectionId,
                                                String       originatorServerName,
                                                String       originatorServerType,
                                                String       originatorOrganizationName,
                                                Relationship relationship)
    {
        final String methodName = "processRefreshRelationshipEvent";
        final String entityParameterName = "relationship";

        updateReferenceRelationship(sourceName,
                                    methodName,
                                    entityParameterName,
                                    originatorMetadataCollectionId,
                                    originatorServerName,
                                    relationship);
    }


    /**
     * An open metadata repository has detected two metadata instances with the same identifier (guid).
     * This is a serious error because it could lead to corruption of the metadata collections within the cohort.
     * When this occurs, all repositories in the cohort delete their reference copies of the metadata instances and
     * at least one of the instances has its GUID changed in its respective home repository.  The updated instance(s)
     * are redistributed around the cohort.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - metadata collection id of the repository reporting the conflicting instance
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param targetMetadataCollectionId     - metadata collection id of other repository with the conflicting instance
     * @param targetTypeDefSummary           - details of the target instance's TypeDef
     * @param targetInstanceGUID             - unique identifier for the source instance
     * @param otherOrigin                    - origin of the other (older) metadata instance
     * @param otherMetadataCollectionId      - metadata collection of the other (older) metadata instance
     * @param otherTypeDefSummary            - details of the other (older) instance's TypeDef
     * @param otherInstanceGUID              - unique identifier for the other (older) instance
     * @param errorMessage                   - description of the error.
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

    }


    /**
     * An open metadata repository has detected an inconsistency in the version of the type used in an updated metadata
     * instance compared to its stored version.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param originatorMetadataCollectionId - metadata collection id of the repository reporting the conflicting instance
     * @param originatorServerName           - name of the server that the event came from.
     * @param originatorServerType           - type of server that the event came from.
     * @param originatorOrganizationName     - name of the organization that owns the server that sent the event.
     * @param targetMetadataCollectionId     - metadata collection id of other repository with the conflicting instance
     * @param targetTypeDefSummary           - details of the target instance's TypeDef
     * @param targetInstanceGUID             - unique identifier for the source instance
     * @param otherTypeDefSummary            - details of the local copy of the instance's TypeDef
     * @param errorMessage                   - description of the error.
     */
    public void processConflictingTypeEvent(String         sourceName,
                                            String         originatorMetadataCollectionId,
                                            String         originatorServerName,
                                            String         originatorServerType,
                                            String         originatorOrganizationName,
                                            String         targetMetadataCollectionId,
                                            TypeDefSummary targetTypeDefSummary,
                                            String         targetInstanceGUID,
                                            TypeDefSummary otherTypeDefSummary,
                                            String         errorMessage)
    {

    }


    /*
     * =======================
     * OMRSInstanceRetrievalEventProcessor
     */


    /**
     * Pass an entity that has been retrieved from a remote open metadata repository so it can be validated and
     * (if the rules permit) cached in the local repository.
     *
     * @param sourceName - name of the source of this event.
     * @param metadataCollectionId - unique identifier for the metadata from the remote repository
     * @param entity               - the retrieved entity.
     * @return Validated and processed entity.
     */
    public EntityDetail processRetrievedEntity(String       sourceName,
                                               String       metadataCollectionId,
                                               EntityDetail entity)
    {
        EntityDetail   processedEntity = new EntityDetail(entity);

        processedEntity.setMetadataCollectionId(metadataCollectionId);

        final String methodName = "processRetrievedEntity";
        final String entityParameterName = "entity";

        updateReferenceEntity(sourceName,
                              methodName,
                              entityParameterName,
                              metadataCollectionId,
                              localServerName,
                              entity);
        return entity;
    }


    /**
     * Pass a list of entities that have been retrieved from a remote open metadata repository so they can be
     * validated and (if the rules permit) cached in the local repository.
     *
     * @param sourceName - name of the source of this event.
     * @param metadataCollectionId - unique identifier for the metadata from the remote repository
     * @param entities             - the retrieved relationships
     * @return the validated and processed relationships
     */
    public List<EntityDetail> processRetrievedEntities(String                  sourceName,
                                                       String                  metadataCollectionId,
                                                       List<EntityDetail>      entities)
    {
        List<EntityDetail> processedEntities = new ArrayList<>();

        for (EntityDetail  entity : entities)
        {
            EntityDetail   processedEntity = this.processRetrievedEntity(sourceName, metadataCollectionId, entity);

            if (processedEntity != null)
            {
                processedEntities.add(processedEntity);
            }
        }

        return processedEntities;
    }


    /**
     * Pass a relationship that has been retrieved from a remote open metadata repository so it can be validated and
     * (if the rules permit) cached in the local repository.
     *
     * @param sourceName - name of the source of this event.
     * @param metadataCollectionId - unique identifier for the metadata from the remote repository
     * @param relationship         - the retrieved relationship
     * @return the validated and processed relationship
     */
    public Relationship processRetrievedRelationship(String       sourceName,
                                                     String       metadataCollectionId,
                                                     Relationship relationship)
    {
        Relationship   processedRelationship = new Relationship(relationship);

        /*
         * Ensure the metadata collection is set up correctly.
         */
        if (processedRelationship.getMetadataCollectionId() == null)
        {
            processedRelationship.setMetadataCollectionId(metadataCollectionId);
        }


        /*
         * Discover whether the instance should be learned.
         */
        if (saveExchangeRule.learnInstanceEvent(processedRelationship))
        {
            try
            {
                if (realMetadataCollection.isRelationshipKnown(sourceName, processedRelationship.getGUID()) == null)
                {
                    InstanceType type = processedRelationship.getType();

                    if (type != null)
                    {
                        /*
                         * It would be possible to save the relationship directly into the repository,
                         * but it is possible that some of the properties have been suppressed for the
                         * requesting user Id.  In which case saving it now would result in other users
                         * seeing a restricted view of the
                         */
                        realMetadataCollection.refreshRelationshipReferenceCopy(localServerName,
                                                                                processedRelationship.getGUID(),
                                                                                type.getTypeDefGUID(),
                                                                                type.getTypeDefName(),
                                                                                metadataCollectionId);
                    }
                }
            }
            catch (Throwable   error)
            {
                final String methodName = "processRetrievedRelationship";

                OMRSAuditCode auditCode = OMRSAuditCode.UNEXPECTED_EXCEPTION_FROM_EVENT;
                auditLog.logRecord(methodName,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   auditCode.getFormattedLogMessage(methodName,
                                                                    sourceName,
                                                                    metadataCollectionId,
                                                                    error.getMessage()),
                                   null,
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());
            }
        }

        return processedRelationship;
    }


    /**
     * Pass a list of relationships that have been retrieved from a remote open metadata repository so they can be
     * validated and (if the rules permit) cached in the local repository.
     *
     * @param sourceName - name of the source of this event.
     * @param metadataCollectionId - unique identifier for the metadata from the remote repository
     * @param relationships        - the list of retrieved relationships
     * @return the validated and processed relationships
     */
    public List<Relationship> processRetrievedRelationships(String             sourceName,
                                                            String             metadataCollectionId,
                                                            List<Relationship> relationships)
    {
        List<Relationship> processedRelationships = new ArrayList<>();

        for (Relationship  relationship : relationships)
        {
            Relationship processedRelationship = this.processRetrievedRelationship(sourceName,
                                                                                   metadataCollectionId,
                                                                                   relationship);

            if (processedRelationship != null)
            {
                processedRelationships.add(processedRelationship);
            }
        }

        return processedRelationships;
    }


    /*
     * ==============================
     * Private methods
     * ==============================
     */


    /**
     * Update the reference entity in the local repository.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param methodName                     - name of the event method
     * @param entityParameterName            - name of the parameter that passed the entity.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param entity                         - details of the new entity
     */
    private void updateReferenceEntity(String       sourceName,
                                       String       methodName,
                                       String       entityParameterName,
                                       String       originatorMetadataCollectionId,
                                       String       originatorServerName,
                                       EntityDetail entity)
    {
        try
        {
            verifyEventProcessor(methodName);
            repositoryValidator.validateReferenceInstanceHeader(realRepositoryName,
                                                                localMetadataCollectionId,
                                                                entityParameterName,
                                                                entity,
                                                                methodName);

            if (saveExchangeRule.processInstanceEvent(entity))
            {
                realMetadataCollection.saveEntityReferenceCopy(sourceName, entity);
            }
        }
        catch (Throwable error)
        {
            OMRSAuditCode auditCode = OMRSAuditCode.UNEXPECTED_EXCEPTION_FROM_EVENT;
            auditLog.logRecord(methodName,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(methodName,
                                                                originatorServerName,
                                                                originatorMetadataCollectionId,
                                                                error.getMessage()),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());
        }
    }


    /**
     * Update the reference relationship in the local repository.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param methodName                     - name of the event method
     * @param entityParameterName            - name of the parameter that passed the relationship.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param relationship                   - details of the relationship
     */
    private void updateReferenceRelationship(String       sourceName,
                                             String       methodName,
                                             String       entityParameterName,
                                             String       originatorMetadataCollectionId,
                                             String       originatorServerName,
                                             Relationship relationship)
    {
        try
        {
            verifyEventProcessor(methodName);
            repositoryValidator.validateReferenceInstanceHeader(realRepositoryName,
                                                                localMetadataCollectionId,
                                                                entityParameterName,
                                                                relationship,
                                                                methodName);

            if (saveExchangeRule.processInstanceEvent(relationship))
            {
                realMetadataCollection.saveRelationshipReferenceCopy(sourceName, relationship);
            }
        }
        catch (Throwable error)
        {
            OMRSAuditCode auditCode = OMRSAuditCode.UNEXPECTED_EXCEPTION_FROM_EVENT;
            auditLog.logRecord(methodName,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(methodName,
                                                                originatorServerName,
                                                                originatorMetadataCollectionId,
                                                                error.getMessage()),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());
        }
    }


    /**
     * Purge a reference copy of an instance from the repository.
     *
     * @param sourceName                     - name of the source of the event.  It may be the cohort name for incoming events or the
     *                                       local repository, or event mapper name.
     * @param methodName                     - name of the purge method
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection hosted by the server that
     *                                       sent the event.
     * @param originatorServerName           - name of the server that the event came from.
     * @param typeDefGUID                    - unique identifier for this entity's TypeDef
     * @param typeDefName                    - name of this entity's TypeDef
     * @param instanceGUID                   - unique identifier for the entity
     */
    private void purgeReferenceInstance(String sourceName,
                                        String methodName,
                                        String originatorMetadataCollectionId,
                                        String originatorServerName,
                                        String typeDefGUID,
                                        String typeDefName,
                                        String instanceGUID)
    {
        try
        {
            verifyEventProcessor(methodName);

            realMetadataCollection.purgeEntityReferenceCopy(sourceName,
                                                            instanceGUID,
                                                            typeDefGUID,
                                                            typeDefName,
                                                            originatorMetadataCollectionId);

        }
        catch (Throwable error)
        {
            OMRSAuditCode auditCode = OMRSAuditCode.UNEXPECTED_EXCEPTION_FROM_EVENT;
            auditLog.logRecord(methodName,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(methodName,
                                                                originatorServerName,
                                                                originatorMetadataCollectionId,
                                                                error.getMessage()),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());
        }
    }

    /**
     * Validate that this event processor is correctly initialized.
     *
     * @param methodName - name of the method being called
     */
    private void verifyEventProcessor(String    methodName)
    {
        if (localMetadataCollectionId == null)
        {
            OMRSErrorCode errorCode    = OMRSErrorCode.NULL_LOCAL_METADATA_COLLECTION;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        if (realLocalConnector == null)
        {
            OMRSErrorCode errorCode    = OMRSErrorCode.NO_LOCAL_REPOSITORY;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        if (realMetadataCollection == null)
        {
            OMRSErrorCode errorCode    = OMRSErrorCode.NULL_LOCAL_METADATA_COLLECTION;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        if (repositoryHelper ==null)
        {
            OMRSErrorCode errorCode    = OMRSErrorCode.NULL_REPOSITORY_HELPER;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        if (repositoryValidator == null)
        {
            OMRSErrorCode errorCode    = OMRSErrorCode.NULL_REPOSITORY_VALIDATOR;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        if (saveExchangeRule == null)
        {
            OMRSErrorCode errorCode    = OMRSErrorCode.NULL_EXCHANGE_RULE;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        if (realMetadataCollection == null)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_METADATA_COLLECTION;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(realRepositoryName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
    }
}
