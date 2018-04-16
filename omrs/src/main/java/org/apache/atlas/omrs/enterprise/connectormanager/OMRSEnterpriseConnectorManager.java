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
package org.apache.atlas.omrs.enterprise.connectormanager;

import org.apache.atlas.ocf.Connector;
import org.apache.atlas.ocf.ConnectorBroker;
import org.apache.atlas.ocf.ffdc.ConnectionCheckedException;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.ffdc.exception.OMRSRuntimeException;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryContentManager;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatacollection.OMRSMetadataCollection;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

/**
 * OMRSEnterpriseConnectorManager provides the connectors for all of the repositories in the connected metadata
 * repository cohorts to each of the registered connector consumers.  It supports:
 * <ul>
 *     <li>
 *         A single local repository connector.
 *     </li>
 *     <li>
 *         A remote repository connector for each of the other repositories in the open metadata repository cohort.
 *     </li>
 * </ul>
 * <p>
 * Connector instances are then passed to each of the registered connector consumers.
 * </p>
 * <p>
 * The operation of the OMRSEnterpriseConnectorManager can be thought of in terms of its 3 contract interfaces:
 * </p>
 * <ul>
 *     <li>
 *         OMRSConnectionConsumer is the interface for passing connections to the OMRSEnterpriseConnectorManager.
 *         New connections are validated by creating a test connector and the combination of the metadata collection Id
 *         and connection are stored.  An instance of the connector is passed to each of the registered
 *         connector consumers.
 *     </li>
 *     <li>
 *         OMRSConnectorManager is the interface that enables OMRSConnectorConsumers to register with the federation
 *         manager.  When new connector consumers are stored their reference is stored and a uniqueId is
 *         returned to the connector consumer.  This id can be used to unregister from the connector manager.
 *     </li>
 *     <li>
 *         OMRSConnectorConsumer is the interface that the federation manager uses to pass connectors
 *         to each registered connector consumer.  The connector for the local repository is typically
 *         passed first (if it is available) followed by the remote connectors.
 *     </li>
 * </ul>
 * <p>
 * With these interfaces, the OMRSEnterpriseConnectorManager acts as a go between the OMRSCohortRegistry and
 * the EnterpriseOMRSRepositoryConnector instances.
 * </p>
 * <p>
 * Note: this class uses synchronized methods to ensure that no registration information is lost when the
 * server is operating multi-threaded.
 * </p>
 */
public class OMRSEnterpriseConnectorManager implements OMRSConnectionConsumer, OMRSConnectorManager
{
    private boolean                                enterpriseAccessEnabled;
    private int                                    maxPageSize;

    private String                                 localMetadataCollectionId    = null;
    private OMRSRepositoryConnector                localRepositoryConnector     = null;
    private OMRSRepositoryContentManager           repositoryContentManager     = null;
    private ArrayList<RegisteredConnector>         registeredRemoteConnectors   = new ArrayList<>();
    private ArrayList<RegisteredConnectorConsumer> registeredConnectorConsumers = new ArrayList<>();


    /**
     * Constructor for the enterprise connector manager.
     *
     * @param enterpriseAccessEnabled - boolean indicating whether the connector consumers should be
     *                                 informed of remote connectors.  If enterpriseAccessEnabled = true
     *                                 the connector consumers will be informed of remote connectors; otherwise
     *                                 they will not.
     * @param maxPageSize - the maximum number of elements that can be requested on a page.
     */
    public OMRSEnterpriseConnectorManager(boolean enterpriseAccessEnabled,
                                          int     maxPageSize)
    {
        this.enterpriseAccessEnabled = enterpriseAccessEnabled;
        this.maxPageSize = maxPageSize;
    }


    /**
     * The disconnect processing involved unregistering all repositories with each of the connector consumers.
     * Each connector consumer will pass the disconnect() request to each of their repository connector instances.
     */
    public void disconnect()
    {
        /*
         * Pass the disconnect request to each registered connector consumer.
         */
        for (RegisteredConnectorConsumer registeredConnectorConsumer : registeredConnectorConsumers)
        {
            registeredConnectorConsumer.getConnectorConsumer().disconnectAllConnectors();
        }
    }


    /**
     * Pass details of the connection for the local repository to the connection consumer.
     *
     * @param localMetadataCollectionId - Unique identifier for the metadata collection
     * @param localRepositoryConnector - connector to the local repository
     */
    public void setLocalConnector(String                     localMetadataCollectionId,
                                  OMRSRepositoryConnector    localRepositoryConnector)
    {

        /*
         * Connector is ok so save along with the metadata collection Id.
         */
        this.localRepositoryConnector = localRepositoryConnector;
        this.localMetadataCollectionId = localMetadataCollectionId;

        /*
         * Pass the local connector to each registered connector consumer.
         */
        for (RegisteredConnectorConsumer registeredConnectorConsumer : registeredConnectorConsumers)
        {
            registeredConnectorConsumer.getConnectorConsumer().setLocalConnector(localMetadataCollectionId,
                                                                                 localRepositoryConnector);
        }
    }


    /**
     * Pass details of the connection for one of the remote repositories registered in a connected
     * open metadata repository cohort.
     *
     * @param cohortName - name of the cohort adding the remote connection.
     * @param remoteServerName - name of the remote server for this connection.
     * @param remoteServerType - type of the remote server.
     * @param owningOrganizationName - name of the organization the owns the remote server.
     * @param metadataCollectionId - Unique identifier for the metadata collection
     * @param remoteConnection - Connection object providing properties necessary to create an
     *                         OMRSRepositoryConnector for the remote repository.
     * @throws ConnectionCheckedException - there are invalid properties in the Connection
     * @throws ConnectorCheckedException - there is a problem initializing the Connector
     */
     public synchronized void addRemoteConnection(String         cohortName,
                                                  String         remoteServerName,
                                                  String         remoteServerType,
                                                  String         owningOrganizationName,
                                                  String         metadataCollectionId,
                                                  Connection     remoteConnection) throws ConnectionCheckedException, ConnectorCheckedException
    {
        /*
         * First test that this connection represents an OMRSRepositoryConnector.  If it does not then an exception
         * is thrown by getOMRSRepositoryConnector() to tell the caller there is a problem.
         */
        OMRSRepositoryConnector remoteConnector = this.getOMRSRepositoryConnector(remoteConnection,
                                                                                  remoteServerName,
                                                                                  remoteServerType,
                                                                                  owningOrganizationName,
                                                                                  metadataCollectionId);


        if (remoteConnector != null)
        {
            OMRSMetadataCollection   metadataCollection = null;

            /*
             * Need to validate that this repository connector has a metadata collection.
             */
            try
            {
                metadataCollection = remoteConnector.getMetadataCollection();
            }
            catch (Throwable  error)
            {
                metadataCollection = null;
            }

            /*
             * Don't need to connector any more.
             */
            remoteConnector.disconnect();

            /*
             * Now test the metadata collection.
             */
            if (metadataCollection == null)
            {
                final String   methodName = "addRemoteConnection()";

                OMRSErrorCode errorCode = OMRSErrorCode.NULL_COHORT_METADATA_COLLECTION;
                String errorMessage = errorCode.getErrorMessageId()
                                    + errorCode.getFormattedErrorMessage(cohortName, metadataCollectionId);

                throw new ConnectorCheckedException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
            }
        }


        /*
         * Connector is ok so save the connection and metadata collection Id.
         */
        registeredRemoteConnectors.add(new RegisteredConnector(cohortName,
                                                               remoteServerName,
                                                               remoteServerType,
                                                               owningOrganizationName,
                                                               metadataCollectionId,
                                                               remoteConnection));

        /*
         * Pass the remote connector to each registered connector consumer if enterprise access is enabled.
         */
        if (enterpriseAccessEnabled)
        {
            for (RegisteredConnectorConsumer registeredConnectorConsumer : registeredConnectorConsumers)
            {
                registeredConnectorConsumer.getConnectorConsumer().addRemoteConnector(metadataCollectionId,
                                                                                      this.getOMRSRepositoryConnector(remoteConnection,
                                                                                                                      remoteServerName,
                                                                                                                      remoteServerType,
                                                                                                                      owningOrganizationName,
                                                                                                                      metadataCollectionId));
            }
        }
    }


    /**
     * Pass details that identify the connection for the repository that has left one of the open metadata repository cohorts.
     * Since any repository may be a member of multiple cohorts, we only remove it from the list if it is
     * the last connector for this repository to be removed.
     *
     * @param cohortName - name of the cohort removing the remote connection.
     * @param metadataCollectionId - Unique identifier for the metadata collection.
     */
    public synchronized void removeRemoteConnection(String         cohortName,
                                                    String         metadataCollectionId)
    {
        /*
         * Remove the connector from the registered list and work out if the repository is still registered
         * after it has been removed from the specified cohort.
         */
        Iterator<RegisteredConnector>  iterator = registeredRemoteConnectors.iterator();
        int                            repositoryRegistrationCount = 0;

        while (iterator.hasNext())
        {
            RegisteredConnector registeredRemoteConnector = iterator.next();

            if (registeredRemoteConnector.getMetadataCollectionId().equals(metadataCollectionId))
            {
                /*
                 * Found a match for this repository - if the cohort matches too, remove it.  If the
                 * cohort does not match then increment the count of registrations that still exist.
                 */
                if (registeredRemoteConnector.getSource().equals(cohortName))
                {
                    iterator.remove();
                }
                else
                {
                    repositoryRegistrationCount ++;
                }
            }
        }

        /*
         * Remove the connector from the registered connector consumers if federation is enabled
         * and the repository is no longer registered through any cohort.
         */
        if ((enterpriseAccessEnabled) && (repositoryRegistrationCount == 0))
        {
            for (RegisteredConnectorConsumer registeredConnectorConsumer : registeredConnectorConsumers)
            {
                registeredConnectorConsumer.getConnectorConsumer().removeRemoteConnector(metadataCollectionId);
            }
        }
    }


    /**
     * Remove all of the remote connections for the requested open metadata repository cohort.
     * Care must be taken to only remove the remote connectors from the registered connector consumers if the
     * remote connection is only registered with this cohort.
     *
     * @param cohortName - name of the cohort
     */
    public synchronized void removeCohort(String   cohortName)
    {
        /*
         * Step through the list of registered remote connections, building a list of metadata collection ids for
         * the cohort
         */
        ArrayList<String>    metadataCollectionIds = new ArrayList<>();

        for (RegisteredConnector  registeredRemoteConnector : registeredRemoteConnectors)
        {
            if (registeredRemoteConnector.getSource().equals(cohortName))
            {
                metadataCollectionIds.add(registeredRemoteConnector.getMetadataCollectionId());
            }
        }

        /*
         * Use the list of metadata collection Ids to call removeRemoteConnection().  This will manage the
         * removal of the remote connectors from the connector consumers if it is uniquely registered in this
         * cohort.
         */
        for (String  metadataCollectionId : metadataCollectionIds)
        {
            this.removeRemoteConnection(cohortName, metadataCollectionId);
        }
    }


    /**
     * Register the supplied connector consumer with the connector manager.  During the registration
     * request, the connector manager will pass the connector to the local repository and
     * the connectors to all currently registered remote repositories.  Once successfully registered
     * the connector manager will call the connector consumer each time the repositories in the
     * metadata repository cluster changes.
     *
     * @param connectorConsumer OMRSConnectorConsumer interested in details of the connectors to
     *                           all repositories registered in the metadata repository cluster.
     * @return String identifier for the connectorConsumer - used for the unregister call.
     */
    public synchronized String registerConnectorConsumer(OMRSConnectorConsumer    connectorConsumer)
    {
        /*
         * Store the new connector consumer.
         */
        RegisteredConnectorConsumer   registeredConnectorConsumer = new RegisteredConnectorConsumer(connectorConsumer);
        String                        connectorConsumerId = registeredConnectorConsumer.getConnectorConsumerId();

        registeredConnectorConsumers.add(registeredConnectorConsumer);


        /*
         * Pass the registered local connector to the new connector consumer (if available).
         */
        if (localRepositoryConnector != null)
        {
            connectorConsumer.setLocalConnector(this.localMetadataCollectionId,
                                                this.localRepositoryConnector);
        }

        /*
         * Pass each of the registered remote connectors (if any) to the new connector consumer
         * if federation is enabled.
         */
        if (enterpriseAccessEnabled)
        {
            for (RegisteredConnector registeredConnector : registeredRemoteConnectors)
            {
                connectorConsumer.addRemoteConnector(registeredConnector.getMetadataCollectionId(),
                                                     getOMRSRepositoryConnector(registeredConnector.getConnection(),
                                                                                registeredConnector.getServerName(),
                                                                                registeredConnector.getServerType(),
                                                                                registeredConnector.getOwningOrganizationName(),
                                                                                registeredConnector.getMetadataCollectionId()));
            }
        }

        return connectorConsumerId;
    }


    /**
     * Unregister a connector consumer from the connector manager so it is no longer informed of
     * changes to the metadata repository cluster.
     *
     * @param connectorConsumerId String identifier of the connector consumer returned on the
     *                             registerConnectorConsumer.
     */
    public synchronized void unregisterConnectorConsumer(String   connectorConsumerId)
    {
        /*
         * Remove the connector consumer from the registered list.
         */
        Iterator<RegisteredConnectorConsumer> iterator = registeredConnectorConsumers.iterator();

        while(iterator.hasNext())
        {
            RegisteredConnectorConsumer registeredConnectorConsumer = iterator.next();

            if (registeredConnectorConsumer.getConnectorConsumerId().equals(connectorConsumerId))
            {
                iterator.remove();
                break;
            }
        }
    }


    /**
     * Private method to convert a Connection into an OMRS repository connector using the OCF ConnectorBroker.
     * The OCF ConnectorBroker is needed because the implementation of the OMRS connector is unknown and
     * may have come from a third party.   Thus the official OCF protocol is followed to create the connector.
     * Any failure to create the connector is returned as an exception.
     *
     * @param connection - Connection properties
     * @param serverName - name of the server for this connection.
     * @param serverType - type of the remote server.
     * @param owningOrganizationName - name of the organization the owns the remote server.
     * @param metadataCollectionId - metadata collection Id for this repository
     * @return OMRSRepositoryConnector for the connection
     */
    private OMRSRepositoryConnector getOMRSRepositoryConnector(Connection connection,
                                                               String     serverName,
                                                               String     serverType,
                                                               String     owningOrganizationName,
                                                               String     metadataCollectionId)
    {
        String     methodName = "getOMRSRepositoryConnector()";

        try
        {
            ConnectorBroker         connectorBroker     = new ConnectorBroker();
            Connector               connector           = connectorBroker.getConnector(connection);

            OMRSRepositoryConnector repositoryConnector = (OMRSRepositoryConnector) connector;

            repositoryConnector.setServerName(serverName);
            repositoryConnector.setServerType(serverType);
            repositoryConnector.setOrganizationName(owningOrganizationName);
            repositoryConnector.setMaxPageSize(maxPageSize);
            repositoryConnector.setRepositoryValidator(new OMRSRepositoryValidator(repositoryContentManager));
            repositoryConnector.setRepositoryHelper((new OMRSRepositoryHelper(repositoryContentManager)));
            repositoryConnector.setMetadataCollectionId(metadataCollectionId);
            repositoryConnector.start();

            return repositoryConnector;
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
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(connectionName);

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
     * RegisteredConnector holds the information about connecting to a repository in the open metadata repository
     * cluster.
     */
    private class RegisteredConnector
    {
        private String     source;
        private String     serverName;
        private String     serverType;
        private String     owningOrganizationName;
        private String     metadataCollectionId;
        private Connection connection;


        /**
         * Constructor to set up registered connector.
         *
         * @param source - name of the source of the connector.
         * @param serverName - name of the server for this connection.
         * @param serverType - type of the remote server.
         * @param owningOrganizationName - name of the organization the owns the remote server.
         * @param metadataCollectionId - unique identifier for the metadata collection that this connector accesses.
         * @param connection - connection used to generate the connector
         */
        public RegisteredConnector(String     source,
                                   String     serverName,
                                   String     serverType,
                                   String     owningOrganizationName,
                                   String     metadataCollectionId,
                                   Connection connection)
        {
            this.source = source;
            this.serverName = serverName;
            this.serverType = serverType;
            this.owningOrganizationName = owningOrganizationName;
            this.metadataCollectionId = metadataCollectionId;
            this.connection = connection;
        }


        /**
         * Return the source name for this connector. (Typically the cohort)
         *
         * @return String name
         */
        public String getSource()
        {
            return source;
        }


        /**
         * Return the name of the server that this connection is used to access.
         *
         * @return String name
         */
        public String getServerName()
        {
            return serverName;
        }


        /**
         * Return the type of server that this connection is used to access.
         *
         * @return String type name
         */
        public String getServerType()
        {
            return serverType;
        }


        /**
         * Return the name of the organization that owns the server that this connection is used to access.
         *
         * @return String name
         */
        public String getOwningOrganizationName()
        {
            return owningOrganizationName;
        }

        /**
         * Return the unique identifier for the metadata collection that this connector accesses.
         *
         * @return String identifier
         */
        public String getMetadataCollectionId()
        {
            return metadataCollectionId;
        }


        /**
         * Return the connection used to generate the connector to the metadata repository.
         *
         * @return Connection properties
         */
        public Connection getConnection()
        {
            return connection;
        }
    }


    /**
     * RegisteredConnectorConsumer relates a connector consumer to an identifier.  It is used by
     * OMRSEnterpriseConnectorManager to manage the list of registered connector consumers.
     */
    private class RegisteredConnectorConsumer
    {
        private String                   connectorConsumerId  = null;
        private OMRSConnectorConsumer    connectorConsumer = null;


        /**
         * Constructor when the identifier of the connector consumer is known.
         *
         * @param connectorConsumerId - unique identifier of the connection consumer
         * @param connectorConsumer - connector consumer itself
         */
        public RegisteredConnectorConsumer(String connectorConsumerId, OMRSConnectorConsumer connectorConsumer)
        {
            this.connectorConsumerId = connectorConsumerId;
            this.connectorConsumer = connectorConsumer;
        }


        /**
         * Constructor when the identifier for the connector consumer needs to be allocated.
         *
         * @param connectorConsumer - connector consumer itself
         */
        public RegisteredConnectorConsumer(OMRSConnectorConsumer connectorConsumer)
        {
            this.connectorConsumer = connectorConsumer;
            this.connectorConsumerId = UUID.randomUUID().toString();
        }


        /**
         * Return the unique identifier of the connector consumer.
         *
         * @return String identifier
         */
        public String getConnectorConsumerId()
        {
            return connectorConsumerId;
        }


        /**
         * Return the registered connector consumer.
         *
         * @return - connector consumer object ref
         */
        public OMRSConnectorConsumer getConnectorConsumer()
        {
            return connectorConsumer;
        }
    }
}
