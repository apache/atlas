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
package org.apache.atlas.omrs.metadatahighway;

import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.omrs.admin.properties.OpenMetadataEventProtocolVersion;
import org.apache.atlas.omrs.auditlog.OMRSAuditCode;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.auditlog.OMRSAuditingComponent;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSConnectorErrorException;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryContentManager;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.OMRSCohortRegistry;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.store.OMRSCohortRegistryStore;
import org.apache.atlas.omrs.eventmanagement.*;
import org.apache.atlas.omrs.enterprise.connectormanager.OMRSConnectionConsumer;
import org.apache.atlas.omrs.localrepository.OMRSLocalRepository;
import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventExchangeRule;
import org.apache.atlas.omrs.topicconnectors.OMRSTopicConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The OMRSCohortManager manages the components that connect to a single open metadata repository cohort.
 */
public class OMRSCohortManager
{
    private String                     cohortName                   = null;
    private OMRSTopicConnector         cohortTopicConnector         = null;
    private OMRSRepositoryEventManager cohortRepositoryEventManager = null;
    private OMRSCohortRegistry         cohortRegistry               = null;
    private OMRSEventListener          cohortEventListener          = null;
    private CohortConnectionStatus     cohortConnectionStatus       = CohortConnectionStatus.NOT_INITIALIZED;

    private OMRSRepositoryEventManager localRepositoryEventManager  = null;

    private static final OMRSAuditLog auditLog = new OMRSAuditLog(OMRSAuditingComponent.COHORT_MANAGER);

    private static final Logger log = LoggerFactory.getLogger(OMRSCohortManager.class);


    /**
     * Default Constructor that relies on the initialization of variables in their declaration.
     */
    public OMRSCohortManager()
    {
    }


    /**
     * The constructor defines the minimum information necessary to connect to a cohort.  If these values
     * are not correctly configured, the constructor will throw an exception.
     *
     * @param cohortName - name of the cohort.  This is a local name used for messages.
     * @param localMetadataCollectionId - configured value for the local metadata collection id
     * @param localServerName - the name of the local server. It is a descriptive name for informational purposes.
     * @param localServerType - the type of the local server.  It is a descriptive name for informational purposes.
     * @param localOrganizationName - the name of the organization that owns the local server/repository.
     *                              It is a descriptive name for informational purposes.
     * @param localRepository - link to the local repository - may be null.
     * @param localRepositoryContentManager - the content manager that stores information about the known types
     * @param connectionConsumer - The connection consumer is a component interested in maintaining details of the
     *                           connections to each of the members of the open metadata repository cohort.  If it is
     *                           null, the cohort registry does not publish connections for members of the open
     *                           metadata repository cohort.
     * @param cohortRegistryStore - the cohort registry store where details of members of the cohort are kept
     * @param cohortTopicConnector - Connector to the cohort's OMRS Topic.
     * @param enterpriseTopicConnector - Connector to the federated OMRS Topic.
     * @param eventProtocolVersion - Protocol to use for events to the cohort.
     * @param inboundEventExchangeRule - rule for processing inbound events.
     */
    public void initialize(String                           cohortName,
                           String                           localMetadataCollectionId,
                           String                           localServerName,
                           String                           localServerType,
                           String                           localOrganizationName,
                           OMRSLocalRepository              localRepository,
                           OMRSRepositoryContentManager     localRepositoryContentManager,
                           OMRSConnectionConsumer           connectionConsumer,
                           OMRSTopicConnector               enterpriseTopicConnector,
                           OMRSCohortRegistryStore          cohortRegistryStore,
                           OMRSTopicConnector               cohortTopicConnector,
                           OpenMetadataEventProtocolVersion eventProtocolVersion,
                           OMRSRepositoryEventExchangeRule  inboundEventExchangeRule)
    {
        final String   actionDescription = "Initialize Cohort Manager";

        if (log.isDebugEnabled())
        {
            log.debug(actionDescription);
        }

        try
        {
            this.cohortName = cohortName;

            OMRSAuditCode auditCode = OMRSAuditCode.COHORT_INITIALIZING;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(cohortName),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            /*
             * Set up the config status.  It is updated multiple times during this method to help detect whether
             * underlying component are hanging in their initialization.  Most of these intermediary states are
             * unlikely to be seen.
             */
            this.cohortConnectionStatus = CohortConnectionStatus.INITIALIZING;

            /*
             * Create the event manager for processing incoming events from the cohort's OMRS Topic.
             */
            this.cohortRepositoryEventManager = new OMRSRepositoryEventManager(inboundEventExchangeRule,
                                                                               new OMRSRepositoryValidator(localRepositoryContentManager));

            /*
             * Create an event publisher for the cohort registry to use to send registration requests.
             */
            OMRSEventPublisher outboundRegistryEventProcessor = new OMRSEventPublisher(cohortName,
                                                                                       eventProtocolVersion,
                                                                                       cohortTopicConnector);

            /*
             * Create the cohort registry.
             */
            this.cohortRegistry = new OMRSCohortRegistry();

            /*
             * The presence/absence of the local repository affects the behaviour of the cohort registry.
             */
            if (localRepository != null)
            {
                /*
                 * The local repository is present so set up the CohortRegistry to play a full role in the protocol.
                 */
                this.cohortRegistry.initialize(cohortName,
                                               localMetadataCollectionId,
                                               localRepository.getLocalRepositoryRemoteConnection(),
                                               localServerName,
                                               localServerType,
                                               localOrganizationName,
                                               outboundRegistryEventProcessor,
                                               cohortRegistryStore,
                                               connectionConsumer);

                localRepositoryEventManager = localRepository.getOutboundRepositoryEventManager();

                if (localRepositoryEventManager != null)
                {
                    /*
                     * Register an event publisher with the local repository for this cohort.  This will mean
                     * other members of the cohort can receive events from the local server's repository.
                     */
                    OMRSEventPublisher repositoryEventPublisher = new OMRSEventPublisher(cohortName,
                                                                                         eventProtocolVersion,
                                                                                         cohortTopicConnector);


                    localRepositoryEventManager.registerTypeDefProcessor(repositoryEventPublisher);
                    localRepositoryEventManager.registerInstanceProcessor(repositoryEventPublisher);
                }

                /*
                 * Register the local repository's processors with the cohort's event manager.  This will
                 * route incoming repository events to the local repository.
                 */
                if (localRepository.getIncomingTypeDefEventProcessor() != null)
                {
                    this.cohortRepositoryEventManager.registerTypeDefProcessor(
                            localRepository.getIncomingTypeDefEventProcessor());
                }
                if (localRepository.getIncomingInstanceEventProcessor() != null)
                {
                    this.cohortRepositoryEventManager.registerInstanceProcessor(
                            localRepository.getIncomingInstanceEventProcessor());
                }
            }
            else /* no local repository */
            {
                /*
                 * If there is no local repository, then the cohort registry is focusing on managing registrations
                 * from remote members of the cohort to configure the enterprise access capability.
                 */
                this.cohortRegistry.initialize(cohortName,
                                               null,
                                               null,
                                               localServerName,
                                               localServerType,
                                               localOrganizationName,
                                               outboundRegistryEventProcessor,
                                               null,
                                               connectionConsumer);
            }

            /*
             * If the enterprise omrs topic is active, then register an event publisher for it.
             * This topic is active if the Open Metadata Access Services (OMASs) are active.
             */
            if (enterpriseTopicConnector != null)
            {
                OMRSEventPublisher enterpriseEventPublisher = new OMRSEventPublisher("OMAS Enterprise Access",
                                                                                     eventProtocolVersion,
                                                                                     cohortTopicConnector);

                this.cohortRepositoryEventManager.registerInstanceProcessor(enterpriseEventPublisher);
            }

            this.cohortConnectionStatus = CohortConnectionStatus.NEW;

            /*
             * The cohort topic connector is used by the local cohort components to communicate with the other
             * members of the cohort.
             */
            if (cohortTopicConnector != null)
            {
                /*
                 * Finally create the event listener and register it with the cohort OMRS Topic.
                 */
                OMRSEventListener cohortEventListener = new OMRSEventListener(cohortName,
                                                                              localMetadataCollectionId,
                                                                              this.cohortRegistry,
                                                                              this.cohortRepositoryEventManager,
                                                                              this.cohortRepositoryEventManager);
                cohortTopicConnector.registerListener(cohortEventListener);
                cohortTopicConnector.start();
                this.cohortTopicConnector = cohortTopicConnector;
                this.cohortEventListener = cohortEventListener;

                /*
                 * Once the event infrastructure is set up it is ok to send out registration requests to the
                 * rest of the cohort.
                 */
                this.cohortRegistry.connectToCohort();

                this.cohortConnectionStatus = CohortConnectionStatus.CONNECTED;
            }
        }
        catch (Throwable   error)
        {
            this.cohortConnectionStatus = CohortConnectionStatus.CONFIGURATION_ERROR;

            OMRSAuditCode auditCode = OMRSAuditCode.COHORT_CONFIG_ERROR;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(cohortName, error.getMessage()),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());
        }

        if (log.isDebugEnabled())
        {
            log.debug(actionDescription + " COMPLETE");
        }
    }


    /**
     * Return the name of the cohort.
     *
     * @return String name
     */
    public String getCohortName()
    {
        return cohortName;
    }


    /**
     * Return the status of the connection with the metadata highway.
     *
     * @return CohortConnectionStatus
     */
    public CohortConnectionStatus getCohortConnectionStatus()
    {
        return cohortConnectionStatus;
    }


    /**
     * Disconnect from the cohort.
     *
     * @param permanent - flag indicating if the local repository should unregister from the cohort because it is
     *                  not going ot connect again.
     */
    public synchronized void  disconnect(boolean   permanent)
    {
        final String actionDescription = "Disconnect Cohort Manager";

        if (log.isDebugEnabled())
        {
            log.debug(actionDescription);
        }

        try
        {
            cohortConnectionStatus = CohortConnectionStatus.DISCONNECTING;

            if (cohortRegistry != null)
            {
                cohortRegistry.disconnectFromCohort(permanent);
            }

            if (cohortTopicConnector != null)
            {
                cohortTopicConnector.disconnect();
            }

            cohortConnectionStatus = CohortConnectionStatus.DISCONNECTED;
        }
        catch (ConnectorCheckedException   error)
        {
            if (log.isDebugEnabled())
            {
                log.debug(actionDescription + " FAILED with connector checked exception");
            }

            /*
             * Throw runtime exception to indicate that the cohort registry is not available.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.COHORT_DISCONNECT_FAILED;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(cohortName);

            throw new OMRSConnectorErrorException(errorCode.getHTTPErrorCode(),
                                                  this.getClass().getName(),
                                                  actionDescription,
                                                  errorMessage,
                                                  errorCode.getSystemAction(),
                                                  errorCode.getUserAction(),
                                                  error);

        }
        catch (Throwable  error)
        {
            if (log.isDebugEnabled())
            {
                log.debug(actionDescription + " FAILED with exception");
            }

            throw error;
        }

        if (log.isDebugEnabled())
        {
            log.debug(actionDescription + " COMPLETE");
        }
    }


    /**
     * Standard toString method.
     *
     * @return JSON style description of variables.
     */
    @Override
    public String toString()
    {
        return "OMRSCohortManager{" +
                "cohortName='" + cohortName + '\'' +
                ", cohortTopicConnector=" + cohortTopicConnector +
                ", cohortRepositoryEventManager=" + cohortRepositoryEventManager +
                ", cohortRegistry=" + cohortRegistry +
                ", cohortEventListener=" + cohortEventListener +
                ", cohortConnectionStatus=" + cohortConnectionStatus +
                ", localRepositoryEventManager=" + localRepositoryEventManager +
                '}';
    }
}
