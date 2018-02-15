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
package org.apache.atlas.omrs.admin;

import org.apache.atlas.ocf.Connector;
import org.apache.atlas.ocf.ConnectorBroker;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omrs.admin.properties.CohortConfig;
import org.apache.atlas.omrs.admin.properties.EnterpriseAccessConfig;
import org.apache.atlas.omrs.admin.properties.LocalRepositoryConfig;
import org.apache.atlas.omrs.admin.properties.RepositoryServicesConfig;
import org.apache.atlas.omrs.archivemanager.OMRSArchiveManager;
import org.apache.atlas.omrs.archivemanager.store.OpenMetadataArchiveStore;
import org.apache.atlas.omrs.auditlog.OMRSAuditCode;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.auditlog.OMRSAuditingComponent;
import org.apache.atlas.omrs.auditlog.store.OMRSAuditLogStore;
import org.apache.atlas.omrs.enterprise.connectormanager.OMRSConnectionConsumer;
import org.apache.atlas.omrs.enterprise.connectormanager.OMRSEnterpriseConnectorManager;
import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventExchangeRule;
import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventManager;
import org.apache.atlas.omrs.eventmanagement.repositoryeventmapper.OMRSRepositoryEventMapper;
import org.apache.atlas.omrs.ffdc.exception.OMRSConfigErrorException;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.ffdc.exception.OMRSRuntimeException;
import org.apache.atlas.omrs.enterprise.repositoryconnector.EnterpriseOMRSConnectorProvider;
import org.apache.atlas.omrs.localrepository.repositoryconnector.LocalOMRSConnectorProvider;
import org.apache.atlas.omrs.localrepository.repositoryconnector.LocalOMRSRepositoryConnector;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryContentManager;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSTypeDefValidator;
import org.apache.atlas.omrs.metadatahighway.OMRSMetadataHighwayManager;
import org.apache.atlas.omrs.rest.server.OMRSRepositoryRESTServices;
import org.apache.atlas.omrs.topicconnectors.OMRSTopicConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * OMRSOperationalServices provides the OMAG Server with access to the OMRS capabilities.
 * This includes managing the local metadata repository, connecting and disconnecting from the metadata
 * highway and supporting administrative
 * actions captured through the OMAG REST interface.
 *
 * Examples of the types of capabilities offered by the OMRS Manager include:
 * <ul>
 *     <li>Initialize and Shutdown the OMRS</li>
 *     <li>See the state of the cluster</li>
 *     <li>see the state of the connectors</li>
 *     <li>View the audit log</li>
 *     <li>Load new connector JARs</li>
 *     <li>Connect/disconnect from the metadata highway</li>
 * </ul>
 */
public class OMRSOperationalServices
{
    /*
     * The audit log provides a verifiable record of the membership of the open metadata repository cohort and the
     * metadata exchange activity they are involved in.  The Logger is for standard debug.
     */
    private static final Logger       log      = LoggerFactory.getLogger(OMRSOperationalServices.class);
    private static final OMRSAuditLog auditLog = new OMRSAuditLog(OMRSAuditingComponent.OPERATIONAL_SERVICES);


    private String                         localServerName               = null;
    private String                         localServerType               = null;
    private String                         localOrganizationName         = null;
    private String                         localMetadataCollectionId     = null;

    private OMRSRepositoryContentManager   localRepositoryContentManager = null;
    private OMRSRepositoryEventManager     localRepositoryEventManager   = null;
    private OMRSMetadataHighwayManager     metadataHighwayManager        = null;
    private OMRSEnterpriseConnectorManager enterpriseConnectorManager    = null;
    private OMRSTopicConnector             enterpriseOMRSTopicConnector  = null;
    private LocalOMRSRepositoryConnector   localRepositoryConnector      = null;
    private OMRSArchiveManager             archiveManager                = null;


    /**
     * Constructor used at server startup.
     *
     * @param localServerName - name of the local server
     * @param localServerType - type of the local server
     * @param organizationName - name of the organization that owns the local server
     */
    public OMRSOperationalServices(String                   localServerName,
                                   String                   localServerType,
                                   String                   organizationName)
    {
        /*
         * Save details about the local server
         */
        this.localServerName = localServerName;
        this.localServerType = localServerType;
        this.localOrganizationName = organizationName;
    }


    /**
     * Initialize the OMRS component for the Open Metadata Repository Services (OMRS).  The configuration
     * is taken as is.  Any configuration errors are reported as exceptions.
     *
     * @param repositoryServicesConfig - current configuration values
     */
    public void initialize(RepositoryServicesConfig repositoryServicesConfig)
    {
        final String   actionDescription = "Initialize Repository Operational Services";
        final String   methodName        = "initialize()";
        OMRSAuditCode  auditCode = null;


        if (repositoryServicesConfig == null)
        {
            /*
             * Throw exception as without configuration information the OMRS can not start.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_CONFIG;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage();

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        /*
         * Initialize the audit log
         */
        OMRSAuditLog.initialize(localServerName,
                                localServerType,
                                localOrganizationName,
                                getAuditLogStore(repositoryServicesConfig.getAuditLogConnection()));

        /*
         * Log that the OMRS is starting.  There is another Audit log message logged at the end of this method
         * to confirm that all of the pieces started successfully.
         */
        auditCode = OMRSAuditCode.OMRS_INITIALIZING;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());
        /*
         * There are 3 major groupings of components, each are optional and have linkages between one another.
         * These are the enterprise access services, local repository and the metadata highway (cohort services).
         * Each group as its own config.
         */
        EnterpriseAccessConfig  enterpriseAccessConfig = repositoryServicesConfig.getEnterpriseAccessConfig();
        LocalRepositoryConfig   localRepositoryConfig  = repositoryServicesConfig.getLocalRepositoryConfig();
        ArrayList<CohortConfig> cohortConfigList       = repositoryServicesConfig.getCohortConfigList();

        /*
         * Begin with the enterprise repository services.  They are always needed since the
         * Open Metadata Access Services (OMAS) is dependent on them.  There are 2 modes of operation: local only
         * and enterprise access.  Enterprise access provide an enterprise view of metadata
         * across all of the open metadata repository cohorts that this server connects to.
         * If EnterpriseAccessConfig is null, the enterprise repository services run in local only mode.
         * Otherwise the supplied configuration properties enable it to be configured for enterprise access.
         *
         * The connector manager manages the list of connectors to metadata repositories that the enterprise
         * repository services will use.  The OMRS Topic is used to publish events from these repositories to support the
         * OMASs' event notifications.
         */
        enterpriseConnectorManager = initializeEnterpriseConnectorManager(enterpriseAccessConfig);
        enterpriseOMRSTopicConnector = initializeEnterpriseOMRSTopicConnector(enterpriseAccessConfig);

        /*
         * The archive manager loads pre-defined types and instances that are stored in open metadata archives.
         */
        archiveManager = initializeOpenMetadataArchives(repositoryServicesConfig.getOpenMetadataArchiveConnectionList());

        /*
         * The local repository is optional.  However, the repository content manager is still
         * used to manage the validation of TypeDefs and the creation of metadata instances.
         * It is loaded with any TypeDefs from the archives to seed its in-memory TypeDef cache.
         */
        localRepositoryContentManager = new OMRSRepositoryContentManager();

        /*
         * The repository validator and helper are used by repository connectors to verify the types and instances
         * they receive from external parties and to build new types and instances.  Instances of these
         * classes are created in each of the repository connectors (and possibly the event mappers as well).
         * They are given a link to the repository content manager since it has the cache of TypeDefs.
         */
        OMRSRepositoryValidator.setRepositoryContentManager(localRepositoryContentManager);
        OMRSRepositoryHelper.setRepositoryContentManager(localRepositoryContentManager);

        /*
         * Start up the local repository if one is configured.
         */
        if (localRepositoryConfig != null)
        {
            localMetadataCollectionId = localRepositoryConfig.getMetadataCollectionId();

            auditCode = OMRSAuditCode.LOCAL_REPOSITORY_INITIALIZING;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(localMetadataCollectionId),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            localRepositoryEventManager =
                    new OMRSRepositoryEventManager(
                            new OMRSRepositoryEventExchangeRule("Local Repository Events to Send",
                                                                localRepositoryContentManager,
                                                                localRepositoryConfig.getEventsToSendRule(),
                                                                localRepositoryConfig.getSelectedTypesToSend()));

            /*
             * Pass the local metadata collectionId to the AuditLog
             */
            OMRSAuditLog.setLocalMetadataCollectionId(localMetadataCollectionId);

            localRepositoryConnector = initializeLocalRepository(localRepositoryConfig);

            /*
             * Set up the OMRS REST Services with the local repository
             */
            OMRSRepositoryRESTServices.setLocalRepository(localRepositoryConnector);
        }

        /*
         * This is the point at which the open metadata archives will be processed.  The archives are processed
         * using the same mechanisms as TypeDef/Instance events received from other members of the cohort.  This
         * is because the metadata in the archives is effectively reference metadata that is owned by the archive
         * and should not be updated in the local repository.
         *
         * Note that if the local repository is not configured then only TypeDefs are processed because there
         * is nowhere to store the instances.  The TypeDefs are used for validation of metadata that is passed to
         * the enterprise repository services.
         */
        if (localRepositoryConnector != null)
        {
            archiveManager.setLocalRepository(localRepositoryContentManager,
                                              localRepositoryConnector.getIncomingInstanceEventProcessor());
        }
        else
        {
            archiveManager.setLocalRepository(localRepositoryContentManager,
                                              null);
        }

        /*
         * Connect the local repository connector to the connector manager if they both exist.  This means
         * that enterprise repository requests will include metadata from the local repository.
         */
        if ((localRepositoryConnector != null) && (enterpriseConnectorManager != null))
        {
            enterpriseConnectorManager.setLocalConnector(localRepositoryConnector.getMetadataCollectionId(),
                                                         localRepositoryConnector);
        }

        /*
         * local operation is ready, now connect to the metadata highway.
         */
        if (cohortConfigList != null)
        {
            auditCode = OMRSAuditCode.METADATA_HIGHWAY_INITIALIZING;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            metadataHighwayManager = initializeCohorts(localServerName,
                                                       localServerType,
                                                       localOrganizationName,
                                                       localRepositoryConnector,
                                                       localRepositoryContentManager,
                                                       enterpriseConnectorManager,
                                                       enterpriseOMRSTopicConnector,
                                                       cohortConfigList);
        }

        /*
         * All done and no exceptions :)
         */
        auditCode = OMRSAuditCode.OMRS_INITIALIZED;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());
    }


    /**
     * Return the connector to the Enterprise OMRS Topic.  If null is returned it means the Enterprise OMRS Topic
     * is not needed.  A configuration error exception is thrown if there is a problem with the connection properties
     *
     * @param enterpriseAccessConfig - configuration from the OMAG server
     * @return connector to the Enterprise OMRS Topic or null
     */
    private OMRSTopicConnector  initializeEnterpriseOMRSTopicConnector(EnterpriseAccessConfig  enterpriseAccessConfig)
    {
        OMRSTopicConnector    enterpriseOMRSTopicConnector = null;

        if (enterpriseAccessConfig != null)
        {
            Connection        enterpriseOMRSTopicConnection = enterpriseAccessConfig.getEnterpriseOMRSTopicConnection();

            if (enterpriseOMRSTopicConnection != null)
            {
                enterpriseOMRSTopicConnector = getTopicConnector("Enterprise Access",
                                                                 enterpriseOMRSTopicConnection);
            }
        }

        return enterpriseOMRSTopicConnector;
    }


    /**
     * Initialize the OMRSEnterpriseConnectorManager and the EnterpriseOMRSConnector class.  If the
     * enterprise access configuration
     *
     * @param enterpriseAccessConfig - enterprise access configuration from the OMAG server
     * @return initialized OMRSEnterpriseConnectorManager object
     */
    private OMRSEnterpriseConnectorManager initializeEnterpriseConnectorManager(EnterpriseAccessConfig  enterpriseAccessConfig)
    {
        OMRSEnterpriseConnectorManager   enterpriseConnectorManager = null;

        if (enterpriseAccessConfig == null)
        {
            /*
             * Federation is not enabled in this server
             */
            enterpriseConnectorManager = new OMRSEnterpriseConnectorManager(false);

            /*
             * Pass the address of the enterprise connector manager to the OMRSEnterpriseConnectorProvider class as
             * the connector manager is needed by each instance of the EnterpriseOMRSConnector.
             */
            EnterpriseOMRSConnectorProvider.initialize(enterpriseConnectorManager,
                                                       null,
                                                       null);
        }
        else
        {
            /*
             * Enterprise access is enabled in this server
             */
            final String   actionDescription = "Initialize Repository Operational Services";

            OMRSAuditCode auditCode = OMRSAuditCode.ENTERPRISE_ACCESS_INITIALIZING;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            enterpriseConnectorManager = new OMRSEnterpriseConnectorManager(true);

            /*
             * Pass the address of the enterprise connector manager to the OMRSEnterpriseConnectorProvider class as
             * the connector manager is needed by each instance of the EnterpriseOMRSConnector.
             */
            EnterpriseOMRSConnectorProvider.initialize(enterpriseConnectorManager,
                                                       enterpriseAccessConfig.getEnterpriseMetadataCollectionId(),
                                                       enterpriseAccessConfig.getEnterpriseMetadataCollectionName());
        }

        return enterpriseConnectorManager;
    }


    /**
     * If the local repository is configured then set up the local repository connector.  The
     * information for the local repository's OMRS Repository Connector is configured as a OCF connection in
     * the local repository config.  In fact there are potentially 2 connections configured.  There is a connection
     * for remote access to the local repository and an optional connection for a locally optimized connector to use
     * within the local server.
     *
     * @param localRepositoryConfig - local repository config.
     * @return wrapped OMRS Repository Connector
     */
    private LocalOMRSRepositoryConnector  initializeLocalRepository(LocalRepositoryConfig  localRepositoryConfig)
    {
        LocalOMRSRepositoryConnector  localRepositoryConnector = null;

        /*
         * If the local repository is configured then create the connector to the local repository and
         * configure it.  It is valid to have a server with no local repository.
         */
        if (localRepositoryConfig != null)
        {
            /*
             * Create the local repository's Connector Provider.  This is a special connector provider that
             * creates an OMRS Repository Connector that wraps the real OMRS Repository Connector.  The
             * outer OMRS Repository Connector manages events, audit logging and error handling.
             */
            LocalOMRSConnectorProvider localConnectorProvider =
                    new LocalOMRSConnectorProvider(localMetadataCollectionId,
                                                   localRepositoryConfig.getLocalRepositoryRemoteConnection(),
                                                   localServerName,
                                                   localServerType,
                                                   localOrganizationName,
                                                   getLocalRepositoryEventMapper(localRepositoryConfig.getEventMapperConnection()),
                                                   localRepositoryEventManager,
                                                   localRepositoryContentManager,
                                                   new OMRSRepositoryEventExchangeRule("Local Repository Events To Save",
                                                                                       localRepositoryContentManager,
                                                                                       localRepositoryConfig.getEventsToSaveRule(),
                                                                                       localRepositoryConfig.getSelectedTypesToSave()));


            /*
             * Create the local repository's connector.  If there is no locally optimized connection, the
             * remote connection is used.
             */
            Connection                    localRepositoryConnection       = null;

            if (localRepositoryConfig.getLocalRepositoryLocalConnection() != null)
            {
                localRepositoryConnection = localRepositoryConfig.getLocalRepositoryLocalConnection();
            }
            else
            {
                localRepositoryConnection = localRepositoryConfig.getLocalRepositoryRemoteConnection();
            }
            localRepositoryConnector = this.getLocalOMRSConnector(localRepositoryConnection,
                                                                  localConnectorProvider);
        }

        return localRepositoryConnector;
    }


    /**
     * Return an OMRS archive manager configured with the list of Open Metadata Archive Stores to use.
     *
     * @param openMetadataArchiveConnections - connections to the open metadata archive stores
     * @return OMRS archive manager
     */
    private OMRSArchiveManager initializeOpenMetadataArchives(ArrayList<Connection>    openMetadataArchiveConnections)
    {
        ArrayList<OpenMetadataArchiveStore> openMetadataArchives = null;

        if (openMetadataArchiveConnections != null)
        {
            openMetadataArchives = new ArrayList<>();

            for (Connection archiveConnection : openMetadataArchiveConnections)
            {
                if (archiveConnection != null)
                {
                    /*
                     * Any problems creating the connectors will result in an exception.
                     */
                    openMetadataArchives.add(this.getOpenMetadataArchiveStore(archiveConnection));
                }
            }
        }

        return new OMRSArchiveManager(openMetadataArchives);
    }


    /**
     * A server can optionally connect to one or more open metadata repository cohorts.  There is one
     * CohortConfig for each cohort that the server is to connect to.  The communication between
     * members of a cohort is event-based.  The parameters provide supplied to the metadata highway manager
     * include values need to send compliant OMRS Events.
     *
     * @param localServerName - the name of the local server. This value flows in OMRS Events.
     * @param localServerType - the type of the local server. This value flows in OMRS Events.
     * @param localOrganizationName - the name of the organization that owns this server.
     *                              This value flows in OMRS Events.
     * @param localRepositoryConnector - the local repository connector is supplied if there is a local repository
     *                                 for this server.
     * @param localTypeDefValidator - the TypeDef validator validates the TypeDefs sent by the other members of
     *                              the cohort(s).  It is looking for conflicting TypeDefs that may cause
     *                              metadata integrity issues as the metadata is exchanged between the servers.
     * @param connectionConsumer - the connection consumer is from the enterprise repository services.  It
     *                           receives connection information about the other members of the cohort(s)
     *                           to enable enterprise access.
     * @param enterpriseTopicConnector - connector to the enterprise repository services Topic Connector.
     *                                 The cohorts replicate their events to the enterprise OMRS Topic so
     *                                 the Open Metadata Access Services (OMASs) can monitor changing metadata.
     * @param cohortConfigList - list of cohorts to connect to (and the configuration to do it)
     * @return newly created and initialized metadata highway manager.
     */
    private OMRSMetadataHighwayManager  initializeCohorts(String                          localServerName,
                                                          String                          localServerType,
                                                          String                          localOrganizationName,
                                                          LocalOMRSRepositoryConnector    localRepositoryConnector,
                                                          OMRSTypeDefValidator            localTypeDefValidator,
                                                          OMRSConnectionConsumer          connectionConsumer,
                                                          OMRSTopicConnector              enterpriseTopicConnector,
                                                          ArrayList<CohortConfig>         cohortConfigList)
    {
        /*
         * The metadata highway manager is constructed with the values that are the same for every cohort.
         */
        OMRSMetadataHighwayManager  metadataHighwayManager = new OMRSMetadataHighwayManager(localServerName,
                                                                                            localServerType,
                                                                                            localOrganizationName,
                                                                                            localRepositoryConnector,
                                                                                            localTypeDefValidator,
                                                                                            connectionConsumer,
                                                                                            enterpriseTopicConnector);

        /*
         * The metadata highway manager is initialize with the details specific to each cohort.
         */
        metadataHighwayManager.initialize(cohortConfigList);

        return metadataHighwayManager;
    }


    /**
     * Shutdown the Open Metadata Repository Services.
     *
     * @param permanent - boolean flag indicating whether this server permanently shutting down or not
     * @return boolean indicated whether the disconnect was successful.
     */
    public boolean disconnect(boolean   permanent)
    {
        /*
         * Log that the OMRS is disconnecting.  There is another Audit log message logged at the end of this method
         * to confirm that all of the pieces disconnected successfully.
         */
        final String   actionDescription = "Disconnect Repository Operational Services";
        OMRSAuditCode auditCode = OMRSAuditCode.OMRS_DISCONNECTING;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());


        if (metadataHighwayManager != null)
        {
            metadataHighwayManager.disconnect(permanent);
        }

        if (enterpriseOMRSTopicConnector != null)
        {
            try
            {
                enterpriseOMRSTopicConnector.disconnect();
            }
            catch (Throwable  error)
            {
                // TODO
            }
        }

        if (enterpriseConnectorManager != null)
        {
            enterpriseConnectorManager.disconnect();
        }

        if (localRepositoryConnector != null)
        {
            try
            {
                localRepositoryConnector.disconnect();
            }
            catch (Throwable  error)
            {

            }
        }

        if (archiveManager != null)
        {
            archiveManager.close();
        }

        auditCode = OMRSAuditCode.OMRS_DISCONNECTED;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());

        return true;
    }


    /**
     * Return the connector to the AuditLog store using the connection information supplied.  If there is a
     * problem with the connection information that means a connector can not be created, an exception is thrown.
     *
     * @param auditLogStoreConnection - properties for the audit log
     * @return audit log store connector
     */
    private OMRSAuditLogStore  getAuditLogStore(Connection   auditLogStoreConnection)
    {
        try
        {
            ConnectorBroker         connectorBroker = new ConnectorBroker();
            Connector               connector       = connectorBroker.getConnector(auditLogStoreConnection);

            return (OMRSAuditLogStore)connector;
        }
        catch (Throwable   error)
        {
            String methodName = "getAuditLogStore()";

            if (log.isDebugEnabled())
            {
                log.debug("Unable to create audit log store connector: " + error.toString());
            }

            /*
             * Throw runtime exception to indicate that the audit log is not available.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_AUDIT_LOG_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(localServerName);

            throw new OMRSConfigErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction(),
                                               error);
        }
    }



    /**
     * Creates a topic connector using information from the supplied topic connection.  This connector supported
     * the Open Connector Framework (OCF) so it is possible to configure different connector implementations for
     * different event/messaging infrastructure.   If there is a problem with the connection information
     * that means a connector can not be created, an exception is thrown.
     *
     * @param sourceName - name of the user of this topic
     * @param topicConnection - connection parameters
     * @return OMRSTopicConnector for managing communications with the event/messaging infrastructure.
     */
    private OMRSTopicConnector getTopicConnector(String     sourceName,
                                                 Connection topicConnection)
    {
        try
        {
            ConnectorBroker    connectorBroker = new ConnectorBroker();
            Connector          connector       = connectorBroker.getConnector(topicConnection);

            return (OMRSTopicConnector)connector;
        }
        catch (Throwable   error)
        {
            String methodName = "getTopicConnector()";

            if (log.isDebugEnabled())
            {
                log.debug("Unable to create topic connector: " + error.toString());
            }

            OMRSErrorCode errorCode = OMRSErrorCode.NULL_TOPIC_CONNECTOR;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(sourceName);

            throw new OMRSConfigErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction(),
                                               error);

        }
    }


    /**
     * Return the connector to an open metadata archive store.  Each connector instance can access a single
     * open metadata archive.  If there is a problem with the connection information
     * that means a connector can not be created, an exception is thrown.
     *
     * @param openMetadataArchiveStoreConnection - properties used to create the connection
     * @return open metadata archive connector
     */
    private OpenMetadataArchiveStore  getOpenMetadataArchiveStore(Connection   openMetadataArchiveStoreConnection)
    {
        try
        {
            ConnectorBroker          connectorBroker = new ConnectorBroker();
            Connector                connector       = connectorBroker.getConnector(openMetadataArchiveStoreConnection);

            return (OpenMetadataArchiveStore)connector;
        }
        catch (Throwable   error)
        {
            String methodName = "getOpenMetadataArchiveStore()";

            if (log.isDebugEnabled())
            {
                log.debug("Unable to create open metadata archive connector: " + error.toString());
            }

            /*
             * Throw runtime exception to indicate that the open metadata archive store is not available.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_ARCHIVE_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(localServerName);

            throw new OMRSRuntimeException(errorCode.getHTTPErrorCode(),
                                           this.getClass().getName(),
                                           methodName,
                                           errorMessage,
                                           errorCode.getSystemAction(),
                                           errorCode.getUserAction(),
                                           error);
        }
    }


    /**
     * The local repository may need an event mapper to convert its proprietary events to OMRS Events.
     * An event mapper is implemented as an OMRSRepositoryEventMapper Connector and it is initialized through the
     * OCF Connector Broker using an OCF connection.
     *
     * @param localRepositoryEventMapperConnection - connection to the local repository's event mapper.
     * @return local repository's event mapper
     */
    private OMRSRepositoryEventMapper getLocalRepositoryEventMapper(Connection   localRepositoryEventMapperConnection)
    {
        /*
         * If the event mapper is null it means the local repository does not need an event mapper.
         * This is not an error.
         */
        if (localRepositoryEventMapperConnection == null)
        {
            return null;
        }

        /*
         * The event mapper is a pluggable component that is implemented as an OCF connector.  Its configuration is
         * passed to it in a Connection object and the ConnectorBroker manages its creation and initialization.
         */
        try
        {
            ConnectorBroker           connectorBroker = new ConnectorBroker();
            Connector                 connector       = connectorBroker.getConnector(localRepositoryEventMapperConnection);

            return (OMRSRepositoryEventMapper)connector;
        }
        catch (Throwable   error)
        {
            String methodName = "getLocalRepositoryEventMapper()";

            if (log.isDebugEnabled())
            {
                log.debug("Unable to create local repository event mapper connector: " + error.toString());
            }

            /*
             * Throw runtime exception to indicate that the local repository's event mapper is not available.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_EVENT_MAPPER;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(localServerName);

            throw new OMRSConfigErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction(),
                                               error);
        }
    }



    /**
     * Private method to convert a Connection into a LocalOMRSRepositoryConnector using the LocalOMRSConnectorProvider.
     * The supplied connection is for the real local connector.  LocalOMRSRepositoryConnector will create the
     * real local connector and ensure all requests it receives are passed to it.
     *
     * @param connection - Connection properties for the real local connection
     * @param connectorProvider - connector provider to create the repository connector
     * @return LocalOMRSRepositoryConnector wrapping the real local connector
     */
    private LocalOMRSRepositoryConnector getLocalOMRSConnector(Connection                       connection,
                                                               LocalOMRSConnectorProvider       connectorProvider)
    {
        String     methodName = "getLocalOMRSConnector()";

        /*
         * Although the localOMRSConnector is an OMRSRepositoryConnector, its initialization is
         * managed directly with its connector provider (rather than using the connector broker) because it
         * needs access to a variety of OMRS components in order for it to support access to the local
         * repository by other OMRS components.  As such it needs more variables at initialization.
         */
        try
        {
            return (LocalOMRSRepositoryConnector)connectorProvider.getConnector(connection);
        }
        catch (Throwable  error)
        {
            /*
             * If there is a problem initializing the connector then the ConnectorBroker will have created a
             * detailed exception already.  The only error case that this method has introduced is the cast
             * of the Connector to OMRSRepositoryConnector.  This could occur if the connector configured is a valid
             * OCF Connector but not an OMRSRepositoryConnector.
             */
            String  connectionName = connection.getConnectionName();

            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_OMRS_CONNECTION;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(connectionName);

            throw new OMRSRuntimeException(errorCode.getHTTPErrorCode(),
                                           this.getClass().getName(),
                                           methodName,
                                           errorMessage,
                                           errorCode.getSystemAction(),
                                           errorCode.getUserAction(),
                                           error);
        }
    }
}
