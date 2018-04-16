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
package org.apache.atlas.omrs.enterprise.repositoryconnector;

import org.apache.atlas.ocf.Connector;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.omrs.auditlog.OMRSAuditCode;
import org.apache.atlas.omrs.auditlog.OMRSAuditLog;
import org.apache.atlas.omrs.auditlog.OMRSAuditingComponent;
import org.apache.atlas.omrs.enterprise.connectormanager.OMRSConnectorConsumer;
import org.apache.atlas.omrs.enterprise.connectormanager.OMRSConnectorManager;
import org.apache.atlas.omrs.ffdc.exception.RepositoryErrorException;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.properties.instances.InstanceHeader;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * EnterpriseOMRSRepositoryConnector supports federating calls to multiple metadata repositories.  As a result,
 * its OMRSMetadataCollection (EnterpriseOMRSMetadataCollection) returns metadata from all repositories in the
 * connected open metadata repository cohort(s).
 * <p>
 *     An instance of the EnterpriseOMRSRepositoryConnector is created by each Open Metadata Access Service (OMAS)
 *     using the OCF ConnectorBroker.  They use its metadata collection to retrieve and send the metadata they need.
 * </p>
 * <p>
 *     Each EnterpriseOMRSRepositoryConnector instance needs to maintain an up to date list of OMRS Connectors to all of the
 *     repositories in the connected open metadata repository cohort(s).  It does by registering as an OMRSConnectorConsumer
 *     with the OMRSConnectorManager to be notified when connectors to new open metadata repositories are available.
 * </p>
 */
public class EnterpriseOMRSRepositoryConnector extends OMRSRepositoryConnector implements OMRSConnectorConsumer
{
    private OMRSConnectorManager             connectorManager                 = null;
    private String                           connectorConsumerId              = null;

    private FederatedConnector               localCohortConnector             = null;
    private ArrayList<FederatedConnector>    remoteCohortConnectors           = new ArrayList<>();

    private String                           accessServiceName                = null;

    private static final Logger       log      = LoggerFactory.getLogger(EnterpriseOMRSRepositoryConnector.class);
    private static final OMRSAuditLog auditLog = new OMRSAuditLog(OMRSAuditingComponent.ENTERPRISE_REPOSITORY_CONNECTOR);


    /**
     * Constructor used by the EnterpriseOMRSConnectorProvider.
     *
     * @param connectorManager - provides notifications as repositories register and unregister with the
     *                         cohorts.
     */
    public EnterpriseOMRSRepositoryConnector(OMRSConnectorManager connectorManager)
    {
        super();

        String   methodName = "constructor";

        this.connectorManager = connectorManager;

        if (connectorManager != null)
        {
            this.connectorConsumerId = connectorManager.registerConnectorConsumer(this);
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_COHORT_CONFIG;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMRSRuntimeException(errorCode.getHTTPErrorCode(),
                                           this.getClass().getName(),
                                           methodName,
                                           errorMessage,
                                           errorCode.getSystemAction(),
                                           errorCode.getUserAction());
        }
    }


    /**
     * Set up the unique Id for this metadata collection.
     *
     * @param metadataCollectionId - String unique Id
     */
    public void setMetadataCollectionId(String     metadataCollectionId)
    {
        super.metadataCollectionId = metadataCollectionId;

        if (metadataCollectionId != null)
        {
            super.metadataCollection = new EnterpriseOMRSMetadataCollection(this,
                                                                            super.serverName,
                                                                            repositoryHelper,
                                                                            repositoryValidator,
                                                                            metadataCollectionId);

        }
    }


    /**
     * Return the name of the access service using this connector.
     *
     * @return access service name
     */
    public String getAccessServiceName()
    {
        return accessServiceName;
    }


    /**
     * Set up the name of the access service using this connector.
     *
     * @param accessServiceName - string name
     */
    public void setAccessServiceName(String accessServiceName)
    {
        this.accessServiceName = accessServiceName;
    }


    /**
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException - there is a problem within the connector.
     */
    public void start() throws ConnectorCheckedException
    {
        super.start();

        final String actionDescription = "start";

        OMRSAuditCode auditCode = OMRSAuditCode.STARTING_ENTERPRISE_CONNECTOR;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(accessServiceName),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());
    }


    /**
     * Free up any resources held since the connector is no longer needed.
     *
     * @throws ConnectorCheckedException - there is a problem disconnecting the connector.
     */
    public void disconnect() throws ConnectorCheckedException
    {
        super.disconnect();

        final String actionDescription = "disconnect";

        OMRSAuditCode auditCode = OMRSAuditCode.DISCONNECTING_ENTERPRISE_CONNECTOR;
        auditLog.logRecord(actionDescription,
                           auditCode.getLogMessageId(),
                           auditCode.getSeverity(),
                           auditCode.getFormattedLogMessage(accessServiceName),
                           null,
                           auditCode.getSystemAction(),
                           auditCode.getUserAction());

        if ((connectorManager != null) && (connectorConsumerId != null))
        {
            connectorManager.unregisterConnectorConsumer(connectorConsumerId);
        }

        localCohortConnector = null;
        remoteCohortConnectors = new ArrayList<>();
    }


    /**
     * Returns the connector to the repository where the supplied instance can be updated - ie its home repository.
     *
     * @param instance - instance to test
     * @param methodName - name of method making the request (used for logging)
     * @return repository connector
     * @throws RepositoryErrorException - home metadata collection is null
     */
    protected OMRSRepositoryConnector  getHomeConnector(InstanceHeader      instance,
                                                        String              methodName) throws RepositoryErrorException
    {
        this.validateRepositoryIsActive(methodName);

        repositoryValidator.validateHomeMetadataGUID(repositoryName, instance, methodName);

        String  instanceMetadataCollectionId = instance.getMetadataCollectionId();

        if (instanceMetadataCollectionId.equals(localCohortConnector.getMetadataCollectionId()))
        {
            return localCohortConnector.getConnector();
        }

        for (FederatedConnector   remoteCohortConnector : remoteCohortConnectors)
        {
            if (remoteCohortConnector != null)
            {
                if (instanceMetadataCollectionId.equals(remoteCohortConnector.getMetadataCollectionId()))
                {
                    return remoteCohortConnector.getConnector();
                }
            }
        }

        return null;
    }


    /**
     * Returns the list of repository connectors that the EnterpriseOMRSRepositoryConnector is federating queries across.
     *
     * This method is used by this connector's metadata collection object on each request it processes.  This
     * means it always has the most up to date list of connectors to work with.
     *
     * @param methodName - name of method making the request (used for logging)
     * @return OMRSRepositoryConnector List
     * @throws RepositoryErrorException - the enterprise services are not available
     */
    protected List<OMRSRepositoryConnector> getCohortConnectors(String     methodName) throws RepositoryErrorException
    {
        this.validateRepositoryIsActive(methodName);

        List<OMRSRepositoryConnector> cohortConnectors = new ArrayList<>();

        /*
         * Make sure the local connector is first.
         */
        if (localCohortConnector != null)
        {
            cohortConnectors.add(localCohortConnector.getConnector());
        }

        /*
         * Now add the remote connectors.
         */
        for (FederatedConnector federatedConnector : remoteCohortConnectors)
        {
            cohortConnectors.add(federatedConnector.getConnector());
        }

        if (! cohortConnectors.isEmpty())
        {
            return cohortConnectors;
        }
        else
        {
            OMRSErrorCode errorCode = OMRSErrorCode.NO_REPOSITORIES;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(accessServiceName);

            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                               this.getClass().getName(),
                                               methodName,
                                               errorMessage,
                                               errorCode.getSystemAction(),
                                               errorCode.getUserAction());
        }

    }


    /**
     * Save the connector to the local repository.  This is passed from the OMRSConnectorManager.
     *
     * @param metadataCollectionId - Unique identifier for the metadata collection.
     * @param localConnector - OMRSRepositoryConnector object for the local repository.
     */
    public void setLocalConnector(String                  metadataCollectionId,
                                  OMRSRepositoryConnector localConnector)
    {
        if (localConnector != null)
        {
            localCohortConnector = new FederatedConnector(metadataCollectionId, localConnector);
        }
        else
        {
            localCohortConnector = null;
        }
    }


    /**
     * Pass the connector to one of the remote repositories in the metadata repository cohort.
     *
     * @param metadataCollectionId - Unique identifier for the metadata collection.
     * @param remoteConnector - OMRSRepositoryConnector object providing access to the remote repository.
     */
    public void addRemoteConnector(String                  metadataCollectionId,
                                   OMRSRepositoryConnector remoteConnector)
    {
        if (remoteConnector != null)
        {
            remoteCohortConnectors.add(new FederatedConnector(metadataCollectionId, remoteConnector));
        }
    }


    /**
     * Pass the metadata collection id for a repository that has just left the metadata repository cohort.
     *
     * @param metadataCollectionId - identifier of the metadata collection that is no longer available.
     */
    public void removeRemoteConnector(String  metadataCollectionId)
    {
        Iterator<FederatedConnector> iterator = remoteCohortConnectors.iterator();

        while(iterator.hasNext())
        {
            FederatedConnector registeredConnector = iterator.next();

            if (registeredConnector.getMetadataCollectionId().equals(metadataCollectionId))
            {
                this.disconnectConnector(registeredConnector);
                iterator.remove();
            }
        }
    }


    /**
     * Call disconnect on all registered connectors and stop calling them.  The OMRS is about to shutdown.
     */
    public void disconnectAllConnectors()
    {
        try
        {
            super.disconnect();
        }
        catch (Throwable error)
        {
            /*
             * Nothing to do
             */
        }

        if (localCohortConnector != null)
        {
            this.disconnectConnector(localCohortConnector);
        }

        if (remoteCohortConnectors != null)
        {
            for (FederatedConnector remoteConnector : remoteCohortConnectors)
            {
                if (remoteConnector != null)
                {
                    this.disconnectConnector(remoteConnector);
                }
            }
        }
    }


    /**
     * Issue a disconnect call on the supplied connector.
     *
     * @param federatedConnector - connector to disconnect.
     */
    private void disconnectConnector(FederatedConnector  federatedConnector)
    {
        Connector    connector = null;

        if (federatedConnector != null)
        {
            connector = federatedConnector.getConnector();
        }

        if (connector != null)
        {
            try
            {
                connector.disconnect();
            }
            catch (Throwable  error)
            {
                log.error("Exception from disconnect of connector to metadata collection:" + federatedConnector.getMetadataCollectionId() + "  Error message was: " + error.getMessage());
            }
        }
    }

    /**
     * FederatedConnector is a private class for storing details of each of the connectors to the repositories
     * in the open metadata repository cohort.
     */
    private class FederatedConnector
    {
        private String                  metadataCollectionId = null;
        private OMRSRepositoryConnector connector            = null;


        /**
         * Constructor to set up the details of a federated connector.
         *
         * @param metadataCollectionId - unique identifier for the metadata collection accessed through the connector
         * @param connector - connector for the repository
         */
        public FederatedConnector(String metadataCollectionId, OMRSRepositoryConnector connector)
        {
            this.metadataCollectionId = metadataCollectionId;
            this.connector = connector;
        }


        /**
         * Return the identifier for the metadata collection accessed through the connector.
         *
         * @return String identifier
         */
        public String getMetadataCollectionId()
        {
            return metadataCollectionId;
        }


        /**
         * Return the connector for the repository.
         *
         * @return OMRSRepositoryConnector object
         */
        public OMRSRepositoryConnector getConnector()
        {
            return connector;
        }


        /**
         * Return the metadata collection associated with the connector.
         *
         * @return OMRSMetadataCollection object
         */
        public OMRSMetadataCollection getMetadataCollection()
        {
            if (connector != null)
            {
                try
                {
                    return connector.getMetadataCollection();
                }
                catch (Throwable   error)
                {
                    return null;
                }
            }

            return null;
        }
    }
}
