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
import org.apache.atlas.ocf.ffdc.ConnectionCheckedException;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omrs.enterprise.connectormanager.OMRSConnectorManager;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;

import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryContentManager;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnectorProviderBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;


/**
 * In the Open Connector Framework (OCF), a ConnectorProvider is a factory for a specific type of connector.
 * The EnterpriseOMRSConnectorProvider is the connector provider for the EnterpriseOMRSRepositoryConnector.
 *
 * It will return new instances of the EnterpriseOMRSRepositoryConnector as long as it is configured with the connector
 * manager.  This should happen at server startup, which means the exception due to a lack of connector
 * manager are unexpected.
 */
public class EnterpriseOMRSConnectorProvider extends OMRSRepositoryConnectorProviderBase
{
    private        final int hashCode = UUID.randomUUID().hashCode();

    private static final Logger log = LoggerFactory.getLogger(EnterpriseOMRSConnectorProvider.class);

    private static OMRSConnectorManager         connectorManager                 = null;
    private static OMRSRepositoryContentManager repositoryContentManager         = null;
    private static String                       localServerName                  = null;
    private static String                       localServerType                  = null;
    private static String                       owningOrganizationName           = null;
    private static String                       enterpriseMetadataCollectionId   = null;
    private static String                       enterpriseMetadataCollectionName = null;


    /**
     * Set up the connector manager.  This call is used to control whether the EnterpriseOMRSConnectorProvider
     * produces connectors or not.  An EnterpriseOMRSRepositoryConnector needs the connector manager to maintain the
     * list of connectors to the repositories in the cohort.
     *
     * @param connectorManager - manager of the list of connectors to remote repositories.
     * @param repositoryContentManager - manager of lists of active and known types with associated helper methods
     * @param localServerName - name of the local server for this connection.
     * @param localServerType - type of the local server.
     * @param owningOrganizationName - name of the organization the owns the remote server.
     * @param enterpriseMetadataCollectionId - unique identifier for the combined metadata collection covered by the
     *                                      connected open metadata repositories.
     * @param enterpriseMetadataCollectionName - name of the combined metadata collection covered by the connected open
     *                                        metadata repositories.  Used for messages.
     */
    public synchronized static void initialize(OMRSConnectorManager         connectorManager,
                                               OMRSRepositoryContentManager repositoryContentManager,
                                               String                       localServerName,
                                               String                       localServerType,
                                               String                       owningOrganizationName,
                                               String                       enterpriseMetadataCollectionId,
                                               String                       enterpriseMetadataCollectionName)
    {
        EnterpriseOMRSConnectorProvider.connectorManager = connectorManager;
        EnterpriseOMRSConnectorProvider.repositoryContentManager = repositoryContentManager;
        EnterpriseOMRSConnectorProvider.localServerName = localServerName;
        EnterpriseOMRSConnectorProvider.localServerType = localServerType;
        EnterpriseOMRSConnectorProvider.owningOrganizationName = owningOrganizationName;
        EnterpriseOMRSConnectorProvider.enterpriseMetadataCollectionId = enterpriseMetadataCollectionId;
        EnterpriseOMRSConnectorProvider.enterpriseMetadataCollectionName = enterpriseMetadataCollectionName;
    }


    /**
     * Typical constructor used with the connector broker.  It sets up the class to use for the repository connector
     * instance.
     */
    public EnterpriseOMRSConnectorProvider()
    {
        super();

        Class    connectorClass = EnterpriseOMRSRepositoryConnector.class;

        super.setConnectorClassName(connectorClass.getName());
    }


    /**
     * Creates a new instance of an EnterpriseOMRSRepositoryConnector based on the information in the supplied connection.
     *
     * @param connection - connection that should have all of the properties needed by the Connector Provider
     *                   to create a connector instance.
     * @return Connector - instance of the connector.
     * @throws ConnectionCheckedException - if there are missing or invalid properties in the connection
     * @throws ConnectorCheckedException - if there are issues instantiating or initializing the connector
     */
    public Connector getConnector(Connection connection) throws ConnectionCheckedException, ConnectorCheckedException
    {
        String   methodName = "getConnector()";

        if (log.isDebugEnabled())
        {
            log.debug(methodName + " called");
        }

        if (EnterpriseOMRSConnectorProvider.connectorManager == null)
        {
            /*
             * If the cohort is not connected then throw an exception to indicate that the repositories are offline.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.COHORT_NOT_CONNECTED;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new ConnectorCheckedException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }

        /*
         * Create and initialize a new connector.
         */
        EnterpriseOMRSRepositoryConnector connector = new EnterpriseOMRSRepositoryConnector(EnterpriseOMRSConnectorProvider.connectorManager);

        connector.initialize(this.getNewConnectorGUID(), connection);
        connector.setServerName(localServerName);
        connector.setServerType(localServerType);
        connector.setRepositoryName(enterpriseMetadataCollectionName);
        connector.setOrganizationName(owningOrganizationName);
        connector.setRepositoryHelper(new OMRSRepositoryHelper(repositoryContentManager));
        connector.setRepositoryValidator(new OMRSRepositoryValidator(repositoryContentManager));
        connector.setMetadataCollectionId(enterpriseMetadataCollectionId);
        connector.initializeConnectedAssetProperties(new EnterpriseOMRSConnectorProperties(connector,
                                                                                           EnterpriseOMRSConnectorProvider.connectorManager,
                                                                                           enterpriseMetadataCollectionId,
                                                                                           enterpriseMetadataCollectionName));

        if (log.isDebugEnabled())
        {
            log.debug(methodName + " returns: " + connector.getConnectorInstanceId() + ", " + connection.getConnectionName());
        }

        return connector;
    }


    /**
     * Simple hashCode implementation
     *
     * @return hashCode
     */
    public int hashCode()
    {
        return hashCode;
    }
}
