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
package org.apache.atlas.omrs.metadatahighway.cohortregistry;

import org.apache.atlas.ocf.ffdc.OCFCheckedExceptionBase;
import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.omrs.auditlog.OMRSAuditCode;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.auditlog.OMRSAuditingComponent;
import org.apache.atlas.omrs.eventmanagement.events.OMRSRegistryEventProcessor;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.store.OMRSCohortRegistryStore;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.store.properties.MemberRegistration;
import org.apache.atlas.omrs.ffdc.exception.OMRSConfigErrorException;
import org.apache.atlas.omrs.ffdc.exception.OMRSLogicErrorException;
import org.apache.atlas.omrs.enterprise.connectormanager.OMRSConnectionConsumer;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;


/**
 * OMRSCohortRegistry manages the local server's registration into a cohort and receives registration
 * requests from other servers in the cohort.  This management involves:
 * <ul>
 *     <li>
 *         Sending and receiving registry events that contain registration information about the members
 *         of the cohort.
 *     </li>
 *     <li>
 *         Maintaining details of the local server's and other remote server's registration information
 *         in the cohort registry store to use for server restart.
 *     </li>
 *     <li>
 *         Configuring the federation services (OMRS Connection Manager and Enterprise OMRS Connector) with
 *         information about the other servers in the cohort as they register and unregister from the
 *         cohort.
 *     </li>
 * </ul>
 * Within a server, there is a single instance of the cohort registry for each cohort that the server joins.
 */
public class OMRSCohortRegistry implements OMRSRegistryEventProcessor
{
    /*
     * Local name of the cohort - used for messages rather than being part of the protocol.
     */
    private String     cohortName = null;

    /*
     * These variables describe the local server's properties.
     */
    private String     localMetadataCollectionId       = null;
    private Connection localRepositoryRemoteConnection = null;
    private String     localServerName                 = null;
    private String     localServerType                 = null;
    private String     localOrganizationName           = null;

    /*
     * The registry store is used to save information about the members of the open metadata repository cohort.
     */
    private OMRSCohortRegistryStore      registryStore = null;

    /*
     * The event publisher is used to send events to the rest of the open metadata repository cohort.
     */
    private OMRSRegistryEventProcessor   outboundRegistryEventProcessor = null;

    /*
     * The connection consumer supports components such as the EnterpriseOMRSRepositoryConnector that need to maintain a
     * list of remote partners that are part of the open metadata repository cohort.
     */
    private OMRSConnectionConsumer       connectionConsumer = null;

    /*
     * The audit log provides a verifiable record of the membership of the open metadata repository cohort and the
     * metadata exchange activity they are involved in.  The Logger is for standard debug.
     */
    private static final OMRSAuditLog    auditLog = new OMRSAuditLog(OMRSAuditingComponent.COHORT_REGISTRY);
    private static final Logger          log = LoggerFactory.getLogger(OMRSCohortRegistry.class);


    /**
     * Default constructor that relies on the initialization of variables in the declaration.
     */
    public OMRSCohortRegistry()
    {
    }


    /**
     * Validate that any metadata collection id previously used by the local server to register with the
     * open metadata repository cohort matches the local metadata collection id passed in the configuration
     * properties.
     *
     * @param configuredLocalMetadataCollectionId - configured value for the local metadata collection id - may be null
     *                                  if no local repository.
     */
    private  void   validateLocalMetadataCollectionId(String         configuredLocalMetadataCollectionId)
    {
        String methodName = "validateLocalMetadataCollectionId()";

        if (this.registryStore == null)
        {
            /*
             * Throw exception as the cohort registry store is not available.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_REGISTRY_STORE;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(cohortName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              methodName,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }

        MemberRegistration localRegistration = registryStore.retrieveLocalRegistration();

        if (localRegistration != null)
        {
            String storedLocalMetadataCollectionId = localRegistration.getMetadataCollectionId();

            if (storedLocalMetadataCollectionId != null)
            {
                /*
                 * There is a stored local metadata collection id which is going to be used.  There is a consistency check
                 * to ensure this stored local Id is the same as the configured local metadata collection id.
                 *
                 * If it is not the same, the administrator has changed the configured value after the server
                 * registered with the cohort.   The message on the audit log explains that the new value will be
                 * ignored until the local repository is un-registered with the old metadata collection id and then it
                 * can be registered with the new metadata collection Id.
                 */

                if (!storedLocalMetadataCollectionId.equals(configuredLocalMetadataCollectionId))
                {
                    if (configuredLocalMetadataCollectionId == null)
                    {
                        /*
                         * The change in the configuration is to remove the local repository.  This means
                         * the local server should simply unregister from the cohort.
                         */
                        this.unRegisterLocalRepositoryWithCohort(localRegistration);
                        registryStore.removeLocalRegistration();

                    }
                    else
                    {
                        /*
                         * The configured value is different from the value used to register with this cohort.
                         * This is a situation that could potentially damage the metadata integrity across the cohort.
                         * Hence the exception.
                         */
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_LOCAL_METADATA_COLLECTION_ID;
                        String errorMessage = errorCode.getErrorMessageId()
                                            + errorCode.getFormattedErrorMessage(cohortName,
                                                                                 localServerName,
                                                                                 storedLocalMetadataCollectionId,
                                                                                 configuredLocalMetadataCollectionId);

                        throw new OMRSConfigErrorException(errorCode.getHTTPErrorCode(),
                                                           this.getClass().getName(),
                                                           methodName,
                                                           errorMessage,
                                                           errorCode.getSystemAction(),
                                                           errorCode.getUserAction());
                    }
                }
            }
        }
    }

    /**
     * Initialize the cohort registry object.  The parameters passed control its behavior.
     *
     * @param cohortName - the name of the cohort that this cohort registry is communicating with.
     * @param localMetadataCollectionId - configured value for the local metadata collection id - may be null
     *                                  if no local repository.
     * @param localRepositoryRemoteConnection - the connection properties for a connector that can call this
     *                                        server from a remote server.
     * @param localServerName - the name of the local server. It is a descriptive name for informational purposes.
     * @param localServerType - the type of the local server.  It is a descriptive name for informational purposes.
     * @param localOrganizationName - the name of the organization that owns the local server/repository.
     *                              It is a descriptive name for informational purposes.
     * @param registryEventProcessor - used to send outbound registry events to the cohort.
     * @param cohortRegistryStore - the cohort registry store where details of members of the cohort are kept.
     * @param connectionConsumer - The connection consumer is a component interested in maintaining details of the
     *                           connections to each of the members of the open metadata repository cohort.  If it is
     *                           null, the cohort registry does not publish connections for members of the open
     *                           metadata repository cohort.
     */
    public void initialize(String                     cohortName,
                           String                     localMetadataCollectionId,
                           Connection                 localRepositoryRemoteConnection,
                           String                     localServerName,
                           String                     localServerType,
                           String                     localOrganizationName,
                           OMRSRegistryEventProcessor registryEventProcessor,
                           OMRSCohortRegistryStore    cohortRegistryStore,
                           OMRSConnectionConsumer     connectionConsumer)
    {
        String actionDescription = "Initialize cohort registry";

        if (cohortRegistryStore == null)
        {
            OMRSErrorCode errorCode = OMRSErrorCode.NULL_REGISTRY_STORE;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(cohortName);

            throw new OMRSLogicErrorException(errorCode.getHTTPErrorCode(),
                                              this.getClass().getName(),
                                              actionDescription,
                                              errorMessage,
                                              errorCode.getSystemAction(),
                                              errorCode.getUserAction());
        }
        this.registryStore = cohortRegistryStore;

        /*
         * Save information about the local server
         */
        this.localServerName = localServerName;
        this.localServerType = localServerType;
        this.localOrganizationName = localOrganizationName;

        /*
         * Save the cohort name for messages and the registry event processor for sending outbound events.
         */
        this.cohortName = cohortName;
        this.outboundRegistryEventProcessor = registryEventProcessor;

        /*
         * Verify that the configured local metadata collection Id matches the one stored in the registry store.
         * This will throw an exception if there are unresolvable differences.
         */
        this.validateLocalMetadataCollectionId(localMetadataCollectionId);
        this.localMetadataCollectionId = localMetadataCollectionId;

        /*
         * Save the connection consumer.  This component needs details of the current connections it should use
         * to contact various members of the cluster (including the local server). It needs an initial
         * upload of the members's connections and then ongoing notifications for any changes in the membership.
         */
        this.connectionConsumer = connectionConsumer;

        /*
         * Save the connections to the local repository.  The localRepositoryRemoteConnection is used
         * in the registration request that this repository sends out.
         */
        this.localRepositoryRemoteConnection = localRepositoryRemoteConnection;
    }


    /**
     * A new server needs to register the metadataCollectionId for its metadata repository with the other servers in the
     * open metadata repository.  It only needs to do this once and uses a timestamp to record that the registration
     * event has been sent.
     *
     * If the server has already registered in the past, it does not need to take any action.
     */
    public  void  connectToCohort()
    {
        if (registryStore == null)
        {
            /*
             * Throw exception as the cohort registry store is not available.
             */
            String methodName = "connectToCohort()";

            OMRSErrorCode errorCode = OMRSErrorCode.NULL_REGISTRY_STORE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSRuntimeException(errorCode.getHTTPErrorCode(),
                                           this.getClass().getName(),
                                           methodName,
                                           errorMessage,
                                           errorCode.getSystemAction(),
                                           errorCode.getUserAction());
        }


        /*
         * Extract member registration information from the cohort registry store.  If there is
         * no local registration, it means the local repository is not currently registered with the metadata
         * repository cohort.
         */
        MemberRegistration localRegistration = registryStore.retrieveLocalRegistration();

        if (localRegistration == null)
        {
            localRegistration = new MemberRegistration();
        }


        /*
         * Fill in the local registration with details from the caller.  Any value from the local repository
         * can change except the localMetadataCollectionId and this value has already been validated.
         */
        localRegistration.setMetadataCollectionId(localMetadataCollectionId);
        localRegistration.setServerName(localServerName);
        localRegistration.setServerType(localServerType);
        localRegistration.setOrganizationName(localOrganizationName);
        localRegistration.setRepositoryConnection(localRepositoryRemoteConnection);

        if (localMetadataCollectionId == null)
        {
            /*
             * If the local metadata collection Id is null it means there is no local repository.  No registration
             * is required but the cohort registry sends a registration refresh request to ensure it has a complete
             * list of the remote members for the connection consumer.
             */
            this.requestReRegistrationFromCohort(localRegistration);
        }
        else if (localRegistration.getRegistrationTime() == null)
        {
            /*
             * This repository has never registered with the open metadata repository cohort, so send registration
             * request.
             */
            localRegistration.setRegistrationTime(new Date());

            if (this.registerLocalRepositoryWithCohort(localRegistration))
            {
                /*
                 * Successfully registered so save the local registration to the registry store.
                 */
                registryStore.saveLocalRegistration(localRegistration);
            }
        }
        else
        {
            /*
             * Successfully registered already - save the local registration to the registry store.
             * in case some of the server details (name, type, organization name) have changed.
             */
            registryStore.saveLocalRegistration(localRegistration);

            /*
             * No registration is required but the cohort registry sends a registration refresh request to
             * ensure it has a complete list of the remote members for the connection consumer.
             * The connection consumer will be null if enterprise access is disabled.
             */
            if (connectionConsumer != null)
            {
                this.requestReRegistrationFromCohort(localRegistration);
            }
        }

        /*
         * Now read the remote registrations from the registry store and publish them to the connection consumer.
         */
        if (connectionConsumer != null)
        {
            /*
             * Extract remote member registrations from the cohort registry store and register each one with the
             * connection consumer.
             */
            List<MemberRegistration> remoteRegistrations = registryStore.retrieveRemoteRegistrations();

            if (remoteRegistrations != null)
            {
                for (MemberRegistration  remoteMember : remoteRegistrations)
                {
                    if (remoteMember != null)
                    {
                        this.registerRemoteConnectionWithConsumer(remoteMember.getMetadataCollectionId(),
                                                                  remoteMember.getServerName(),
                                                                  remoteMember.getServerType(),
                                                                  remoteMember.getOrganizationName(),
                                                                  remoteMember.getRepositoryConnection());
                    }
                }
            }
        }
    }


    /**
     * Close the connection to the registry store.
     *
     * @param permanent boolean flag indicating whether the disconnection is permanent or not.  If it is set
     *                  to true, the OMRS Cohort will remove all information about the cohort from the
     *                  cohort registry store.
     */
    public  void  disconnectFromCohort(boolean   permanent)
    {
        final String  actionDescription = "Disconnect from Cohort";

        if (registryStore != null)
        {
            if (permanent)
            {
                MemberRegistration  localRegistration = registryStore.retrieveLocalRegistration();

                OMRSAuditCode auditCode = OMRSAuditCode.COHORT_PERMANENTLY_DISCONNECTING;
                auditLog.logRecord(actionDescription,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   auditCode.getFormattedLogMessage(cohortName),
                                   null,
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());

                if (localRegistration != null)
                {
                    this.unRegisterLocalRepositoryWithCohort(localRegistration);
                }

                registryStore.clearAllRegistrations();

                if (connectionConsumer != null)
                {
                    connectionConsumer.removeCohort(cohortName);
                }
            }
            else
            {
                OMRSAuditCode auditCode = OMRSAuditCode.COHORT_DISCONNECTING;
                auditLog.logRecord(actionDescription,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   auditCode.getFormattedLogMessage(cohortName),
                                   null,
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());
            }

            registryStore.close();
        }
    }


    /**
     * Send a registration event to the open metadata repository cohort.  This means the
     * server has never registered with the cohort before.
     *
     * @param localRegistration - details of the local server that are needed to build the event
     * @return boolean indicating whether the repository registered successfully or not.
     */
    private boolean registerLocalRepositoryWithCohort(MemberRegistration   localRegistration)
    {
        final String    actionDescription = "Registering with cohort";

        OMRSAuditCode   auditCode = OMRSAuditCode.REGISTERED_WITH_COHORT;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(cohortName, localMetadataCollectionId),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());

        return outboundRegistryEventProcessor.processRegistrationEvent(cohortName,
                                                                       localRegistration.getMetadataCollectionId(),
                                                                       localRegistration.getServerName(),
                                                                       localRegistration.getServerType(),
                                                                       localRegistration.getOrganizationName(),
                                                                       localRegistration.getRegistrationTime(),
                                                                       localRegistration.getRepositoryConnection());
    }


    /**
     * Request that the remote members of the cohort send details of their registration to enable the local
     * server to ensure it has details of every member.  There are two use cases.  It may have missed a
     * registration event from a remote member because it was not online for some time.
     * Alternatively, it may not have a local repository and so can not trigger the reRegistration events
     * with its own registration events.
     *
     * @param localRegistration - information needed to sent the refresh request
     */
    private void requestReRegistrationFromCohort(MemberRegistration   localRegistration)
    {
        final String    actionDescription = "Re-registering with cohort";

        OMRSAuditCode   auditCode = OMRSAuditCode.RE_REGISTERED_WITH_COHORT;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(cohortName),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());

        outboundRegistryEventProcessor.processRegistrationRefreshRequest(cohortName,
                                                                         localRegistration.getServerName(),
                                                                         localRegistration.getServerType(),
                                                                         localRegistration.getOrganizationName());
    }


    /**
     * Unregister from the Cohort.
     *
     * @param localRegistration - details of the local registration
     */
    private void unRegisterLocalRepositoryWithCohort(MemberRegistration   localRegistration)
    {
        final String    actionDescription = "Unregistering from cohort";

        OMRSAuditCode   auditCode = OMRSAuditCode.UNREGISTERING_FROM_COHORT;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(cohortName, localMetadataCollectionId),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());

        outboundRegistryEventProcessor.processUnRegistrationEvent(cohortName,
                                                                  localRegistration.getMetadataCollectionId(),
                                                                  localRegistration.getServerName(),
                                                                  localRegistration.getServerType(),
                                                                  localRegistration.getOrganizationName());
    }


    /**
     * Register a new remote connection with the OMRSConnectionConsumer.  If there is a problem with the
     * remote connection, a bad connection registry event is sent to the remote repository.
     *
     * @param remoteMetadataCollectionId - id of the remote repository
     * @param remoteServerName - name of the remote server.
     * @param remoteServerType - type of the remote server.
     * @param owningOrganizationName - name of the organization the owns the remote server.
     * @param remoteRepositoryConnection - connection used to create a connector to call the remote repository.
     */
    private void registerRemoteConnectionWithConsumer(String      remoteMetadataCollectionId,
                                                      String      remoteServerName,
                                                      String      remoteServerType,
                                                      String      owningOrganizationName,
                                                      Connection  remoteRepositoryConnection)
    {
        final String    actionDescription = "Receiving registration request";
        OMRSAuditCode   auditCode = OMRSAuditCode.OUTGOING_BAD_CONNECTION;

        if (connectionConsumer != null)
        {
            /*
             * An exception is thrown if the remote connection is bad.
             */
            try
            {
                connectionConsumer.addRemoteConnection(cohortName,
                                                       remoteServerName,
                                                       remoteServerType,
                                                       owningOrganizationName,
                                                       remoteMetadataCollectionId,
                                                       remoteRepositoryConnection);
            }
            catch (OCFCheckedExceptionBase error)
            {
                auditLog.logRecord(actionDescription,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   auditCode.getFormattedLogMessage(cohortName,
                                                                    remoteRepositoryConnection.getConnectionName(),
                                                                    remoteServerName,
                                                                    remoteMetadataCollectionId),
                                   error.getErrorMessage(),
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());

                if (outboundRegistryEventProcessor != null)
                {
                    outboundRegistryEventProcessor.processBadConnectionEvent(cohortName,
                                                                             localMetadataCollectionId,
                                                                             localServerName,
                                                                             localServerType,
                                                                             localOrganizationName,
                                                                             remoteMetadataCollectionId,
                                                                             remoteRepositoryConnection,
                                                                             error.getErrorMessage());
                }
            }
            catch (Throwable  error)
            {
                String     formattedLogMessage = auditCode.getFormattedLogMessage(cohortName,
                                                                                  remoteRepositoryConnection.getConnectionName(),
                                                                                  remoteServerName,
                                                                                  remoteMetadataCollectionId);
                auditLog.logRecord(actionDescription,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   formattedLogMessage,
                                   error.toString(),
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());

                if (outboundRegistryEventProcessor != null)
                {
                    outboundRegistryEventProcessor.processBadConnectionEvent(cohortName,
                                                                             localMetadataCollectionId,
                                                                             localServerName,
                                                                             localServerType,
                                                                             localOrganizationName,
                                                                             remoteMetadataCollectionId,
                                                                             remoteRepositoryConnection,
                                                                             auditCode.getLogMessageId() + formattedLogMessage);
                }
            }
        }
    }

    /**
     * Unregister a remote connection from the OMRSConnectionConsumer.
     *
     * @param remoteMetadataCollectionId - id of the remote repository
     */
    private void unRegisterRemoteConnectionWithConsumer(String      remoteMetadataCollectionId)
    {
        if (connectionConsumer != null)
        {
            connectionConsumer.removeRemoteConnection(cohortName,
                                                      remoteMetadataCollectionId);
        }
    }


    /*
     * =============================
     * OMRSRegistryEventProcessor
     */

    /**
     * Introduces a new server/repository to the metadata repository cohort.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection that is registering with the cohort.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param registrationTimestamp - the time that the server/repository issued the registration request.
     * @param remoteConnection - the Connection properties for the connector used to call the registering server.
     */
    public boolean processRegistrationEvent(String                    sourceName,
                                            String                    originatorMetadataCollectionId,
                                            String                    originatorServerName,
                                            String                    originatorServerType,
                                            String                    originatorOrganizationName,
                                            Date                      registrationTimestamp,
                                            Connection                remoteConnection)
    {
        final String    actionDescription = "Receiving Registration event";

        if (registryStore != null)
        {
            /*
             * Store information about the remote repository in the cohort registry store.
             */
            MemberRegistration remoteRegistration = new MemberRegistration();

            remoteRegistration.setMetadataCollectionId(originatorMetadataCollectionId);
            remoteRegistration.setServerName(originatorServerName);
            remoteRegistration.setServerType(originatorServerType);
            remoteRegistration.setOrganizationName(originatorOrganizationName);
            remoteRegistration.setRegistrationTime(registrationTimestamp);
            remoteRegistration.setRepositoryConnection(remoteConnection);

            registryStore.saveRemoteRegistration(remoteRegistration);

            if (remoteConnection != null)
            {
                /*
                 * Pass the new remote connection to the connection consumer.
                 */
                this.registerRemoteConnectionWithConsumer(originatorMetadataCollectionId,
                                                          originatorServerName,
                                                          originatorServerType,
                                                          originatorOrganizationName,
                                                          remoteConnection);
            }

            OMRSAuditCode   auditCode = OMRSAuditCode.NEW_MEMBER_IN_COHORT;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(cohortName,
                                                                originatorServerName,
                                                                originatorMetadataCollectionId),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            return true;
        }
        else
        {
            return false;
        }
    }


    /**
     * Requests that the other servers in the cohort send re-registration events.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     */
    public boolean processRegistrationRefreshRequest(String                    sourceName,
                                                     String                    originatorServerName,
                                                     String                    originatorServerType,
                                                     String                    originatorOrganizationName)
    {
        final String    actionDescription = "Receiving Registration Refresh event";

        if (registryStore != null)
        {
            MemberRegistration localRegistration = registryStore.retrieveLocalRegistration();

            if (localRegistration != null)
            {
                OMRSAuditCode   auditCode = OMRSAuditCode.REFRESHING_REGISTRATION_WITH_COHORT;
                auditLog.logRecord(actionDescription,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   auditCode.getFormattedLogMessage(cohortName,
                                                                    localMetadataCollectionId,
                                                                    originatorServerName),
                                   null,
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());

                return outboundRegistryEventProcessor.processReRegistrationEvent(cohortName,
                                                                                 localRegistration.getMetadataCollectionId(),
                                                                                 localRegistration.getServerName(),
                                                                                 localRegistration.getServerType(),
                                                                                 localRegistration.getOrganizationName(),
                                                                                 localRegistration.getRegistrationTime(),
                                                                                 localRegistration.getRepositoryConnection());
            }
            else
            {
                return true;
            }
        }
        else
        {
            OMRSAuditCode   auditCode = OMRSAuditCode.MISSING_MEMBER_REGISTRATION;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(originatorServerName,
                                                                cohortName),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            return false;
        }
    }


    /**
     * Refreshes the other servers in the cohort with the originator server's registration.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - unique identifier for the metadata collection that is registering with the cohort.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param registrationTimestamp - the time that the server/repository first registered with the cohort.
     * @param remoteConnection - the Connection properties for the connector used to call the registering server.
     */
    public boolean processReRegistrationEvent(String                    sourceName,
                                              String                    originatorMetadataCollectionId,
                                              String                    originatorServerName,
                                              String                    originatorServerType,
                                              String                    originatorOrganizationName,
                                              Date                      registrationTimestamp,
                                              Connection                remoteConnection)
    {
        final String    actionDescription = "Receiving ReRegistration event";

        if (registryStore != null)
        {
            /*
             * Store information about the remote repository in the cohort registry store.  If the
             * repository is already stored in the registry store, its entry is refreshed.
             */
            MemberRegistration remoteRegistration = new MemberRegistration();

            remoteRegistration.setMetadataCollectionId(originatorMetadataCollectionId);
            remoteRegistration.setServerName(originatorServerName);
            remoteRegistration.setServerType(originatorServerType);
            remoteRegistration.setOrganizationName(originatorOrganizationName);
            remoteRegistration.setRegistrationTime(registrationTimestamp);
            remoteRegistration.setRepositoryConnection(remoteConnection);

            registryStore.saveRemoteRegistration(remoteRegistration);

            if (remoteConnection != null)
            {
                /*
                 * Pass the new remote connection to the connection consumer.  It may have been updated since
                 * the last registration request was received.
                 */
                this.registerRemoteConnectionWithConsumer(originatorMetadataCollectionId,
                                                          originatorServerName,
                                                          originatorServerType,
                                                          originatorOrganizationName,
                                                          remoteConnection);
            }

            OMRSAuditCode   auditCode = OMRSAuditCode.REFRESHED_MEMBER_IN_COHORT;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(cohortName,
                                                                originatorServerName,
                                                                originatorMetadataCollectionId),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            return true;
        }
        else
        {
            OMRSAuditCode   auditCode = OMRSAuditCode.MISSING_MEMBER_REGISTRATION;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(originatorServerName,
                                                                cohortName),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            return false;
        }
    }


    /**
     * A server/repository is being removed from the metadata repository cohort.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - metadata collectionId of originator.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     */
    public boolean processUnRegistrationEvent(String                    sourceName,
                                              String                    originatorMetadataCollectionId,
                                              String                    originatorServerName,
                                              String                    originatorServerType,
                                              String                    originatorOrganizationName)
    {
        final String    actionDescription = "Receiving Unregistration event";

        if (registryStore != null)
        {

            /*
             * Remove the remote member from the registry store.
             */
            registryStore.removeRemoteRegistration(originatorMetadataCollectionId);

            /*
             * Pass the new remote connection to the connection consumer.
             */
            this.unRegisterRemoteConnectionWithConsumer(originatorMetadataCollectionId);

            OMRSAuditCode   auditCode = OMRSAuditCode.MEMBER_LEFT_COHORT;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(originatorServerName,
                                                                originatorMetadataCollectionId,
                                                                cohortName),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            return true;
        }
        else
        {
            OMRSAuditCode   auditCode = OMRSAuditCode.MISSING_MEMBER_REGISTRATION;
            auditLog.logRecord(actionDescription,
                               auditCode.getLogMessageId(),
                               auditCode.getSeverity(),
                               auditCode.getFormattedLogMessage(originatorServerName,
                                                                cohortName),
                               null,
                               auditCode.getSystemAction(),
                               auditCode.getUserAction());

            return false;
        }
    }


    /**
     * There is more than one member of the open metadata repository cohort that is using the same metadata
     * collection Id.  This means that their metadata instances can be updated in more than one server and their
     * is a potential for data integrity issues.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - metadata collectionId of originator.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param conflictingMetadataCollectionId - unique identifier for the metadata collection that is registering with the cohort.
     * @param errorMessage - details of the conflict
     */
    public void    processConflictingCollectionIdEvent(String  sourceName,
                                                       String  originatorMetadataCollectionId,
                                                       String  originatorServerName,
                                                       String  originatorServerType,
                                                       String  originatorOrganizationName,
                                                       String  conflictingMetadataCollectionId,
                                                       String  errorMessage)
    {
        if (conflictingMetadataCollectionId != null)
        {
            final String    actionDescription = "Receiving Conflicting Metadata Collection Id event";

            if (conflictingMetadataCollectionId.equals(localMetadataCollectionId))
            {
                OMRSAuditCode   auditCode = OMRSAuditCode.INCOMING_CONFLICTING_LOCAL_METADATA_COLLECTION_ID;
                auditLog.logRecord(actionDescription,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   auditCode.getFormattedLogMessage(cohortName,
                                                                    originatorServerName,
                                                                    originatorMetadataCollectionId,
                                                                    conflictingMetadataCollectionId),
                                   null,
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());
            }
            else
            {
                OMRSAuditCode   auditCode = OMRSAuditCode.INCOMING_CONFLICTING_METADATA_COLLECTION_ID;
                auditLog.logRecord(actionDescription,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   auditCode.getFormattedLogMessage(cohortName,
                                                                    conflictingMetadataCollectionId),
                                   null,
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());
            }
        }
    }


    /**
     * A connection to one of the members of the open metadata repository cohort is not usable by one of the members.
     *
     * @param sourceName - name of the source of the event.  It may be the cohort name for incoming events or the
     *                   local repository, or event mapper name.
     * @param originatorMetadataCollectionId - metadata collectionId of originator.
     * @param originatorServerName - name of the server that the event came from.
     * @param originatorServerType - type of server that the event came from.
     * @param originatorOrganizationName - name of the organization that owns the server that sent the event.
     * @param targetMetadataCollectionId - Id for the repository with the bad remote connection.
     * @param remoteRepositoryConnection - the Connection properties for the connector used to call the registering server.
     * @param errorMessage - details of the error that occurs when the connection is used.
     */
    public void    processBadConnectionEvent(String     sourceName,
                                             String     originatorMetadataCollectionId,
                                             String     originatorServerName,
                                             String     originatorServerType,
                                             String     originatorOrganizationName,
                                             String     targetMetadataCollectionId,
                                             Connection remoteRepositoryConnection,
                                             String     errorMessage)
    {
        if (targetMetadataCollectionId != null)
        {
            if (targetMetadataCollectionId.equals(localMetadataCollectionId))
            {
                /*
                 * The event is directed to this server.
                 */
                final String    actionDescription = "Receiving Bad Connection event";
                OMRSAuditCode   auditCode = OMRSAuditCode.INCOMING_BAD_CONNECTION;
                auditLog.logRecord(actionDescription,
                                   auditCode.getLogMessageId(),
                                   auditCode.getSeverity(),
                                   auditCode.getFormattedLogMessage(cohortName,
                                                                    originatorServerName,
                                                                    originatorMetadataCollectionId,
                                                                    remoteRepositoryConnection.getConnectionName()),
                                   remoteRepositoryConnection.toString(),
                                   auditCode.getSystemAction(),
                                   auditCode.getUserAction());
            }
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
        return "OMRSCohortRegistry{" +
                "cohortName='" + cohortName + '\'' +
                ", localMetadataCollectionId='" + localMetadataCollectionId + '\'' +
                ", localRepositoryRemoteConnection=" + localRepositoryRemoteConnection +
                ", localServerName='" + localServerName + '\'' +
                ", localServerType='" + localServerType + '\'' +
                ", localOrganizationName='" + localOrganizationName + '\'' +
                ", registryStore=" + registryStore +
                ", outboundRegistryEventProcessor=" + outboundRegistryEventProcessor +
                ", connectionConsumer=" + connectionConsumer +
                '}';
    }
}
