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

import org.apache.atlas.ocf.Connector;
import org.apache.atlas.ocf.ConnectorBroker;
import org.apache.atlas.ocf.ConnectorProvider;
import org.apache.atlas.ocf.ffdc.ConnectionCheckedException;
import org.apache.atlas.ocf.ffdc.ConnectorCheckedException;
import org.apache.atlas.ocf.properties.Connection;
import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventExchangeRule;
import org.apache.atlas.omrs.eventmanagement.OMRSRepositoryEventManager;
import org.apache.atlas.omrs.eventmanagement.repositoryeventmapper.OMRSRepositoryEventMapper;
import org.apache.atlas.omrs.eventmanagement.repositoryeventmapper.OMRSRepositoryEventMapperConnector;
import org.apache.atlas.omrs.ffdc.OMRSErrorCode;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryContentManager;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryHelper;
import org.apache.atlas.omrs.localrepository.repositorycontentmanager.OMRSRepositoryValidator;
import org.apache.atlas.omrs.metadatacollection.repositoryconnector.OMRSRepositoryConnector;

import java.util.UUID;


/**
 * LocalOMRSConnectorProvider implements the connector provider for LocalOMRSRepositoryConnector.   Since a server only
 * has one LocalOMRSRepositoryConnector, this connector provider returns the singleton connector for the local repository.
 */
public class LocalOMRSConnectorProvider extends ConnectorProvider
{
    private String                             localMetadataCollectionId       = null;
    private Connection                         localRepositoryRemoteConnection = null;
    private OMRSRepositoryEventMapperConnector realEventMapper                 = null;
    private OMRSRepositoryEventManager         outboundRepositoryEventManager  = null;
    private OMRSRepositoryContentManager       repositoryContentManager        = null;
    private OMRSRepositoryEventExchangeRule    saveExchangeRule                = null;
    private LocalOMRSRepositoryConnector       localRepositoryConnector        = null;



    /**
     * Constructor used by OMRSOperationalServices during server start-up - it
     * provides the configuration information about the local server that is used to set up the
     * local repository connector.
     *
     * @param localMetadataCollectionId - metadata collection Id for the local repository
     * @param localRepositoryRemoteConnection - connection object for creating a remote connector to this repository.
     * @param realEventMapper - optional event mapper for local repository
     * @param outboundRepositoryEventManager - event manager to call for outbound events.
     * @param repositoryContentManager - repositoryContentManager for supporting OMRS in managing TypeDefs.
     * @param saveExchangeRule - rule to determine what events to save to the local repository.
     */
    public LocalOMRSConnectorProvider(String                             localMetadataCollectionId,
                                      Connection                         localRepositoryRemoteConnection,
                                      OMRSRepositoryEventMapperConnector realEventMapper,
                                      OMRSRepositoryEventManager         outboundRepositoryEventManager,
                                      OMRSRepositoryContentManager       repositoryContentManager,
                                      OMRSRepositoryEventExchangeRule    saveExchangeRule)
    {
        this.localMetadataCollectionId = localMetadataCollectionId;
        this.localRepositoryRemoteConnection = localRepositoryRemoteConnection;
        this.realEventMapper = realEventMapper;
        this.outboundRepositoryEventManager = outboundRepositoryEventManager;
        this.repositoryContentManager = repositoryContentManager;
        this.saveExchangeRule = saveExchangeRule;
    }


    /**
     * Constructor used by the OCF ConnectorBroker.  This approach will result in an exception
     * when getConnector is called because there is no localMetadataCollectionId (amongst other things).
     */
    public LocalOMRSConnectorProvider()
    {
    }


    /**
     * Creates a new instance of a connector based on the information in the supplied connection.
     *
     * @param realLocalConnection - connection that should have all of the properties needed by the Connector Provider
     *                              to create a connector instance.
     * @return Connector - instance of the LocalOMRSRepositoryConnector wrapping the real local connector.
     * @throws ConnectionCheckedException - if there are missing or invalid properties in the connection
     * @throws ConnectorCheckedException - if there are issues instantiating or initializing the connector
     */
    public synchronized Connector getConnector(Connection realLocalConnection) throws ConnectionCheckedException,
                                                                                      ConnectorCheckedException
    {
        String methodName = "getConnector()";

        if (localMetadataCollectionId == null)
        {
            /*
             * Throw checked exception to indicate that the local repository is not available.  This
             * is likely to be a configuration error.
             */
            OMRSErrorCode errorCode = OMRSErrorCode.LOCAL_REPOSITORY_CONFIGURATION_ERROR;
            String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage();

            throw new ConnectorCheckedException(errorCode.getHTTPErrorCode(),
                                                this.getClass().getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
        }

        /*
         * Only create one instance of the connector
         */
        if (localRepositoryConnector == null)
        {
            OMRSRepositoryConnector realLocalConnector;

            /*
             * Any problems creating the connector to the local repository are passed to the caller as exceptions.
             */
            ConnectorBroker connectorBroker = new ConnectorBroker();
            Connector       connector       = connectorBroker.getConnector(realLocalConnection);

            /*
             * Try casting the returned connector to a repository connector.  This should work unless the connection
             * passed is for a different type of connector.
             */
            try
            {
                realLocalConnector = (OMRSRepositoryConnector) connector;
            }
            catch (Throwable error)
            {
                OMRSErrorCode errorCode = OMRSErrorCode.BAD_LOCAL_REPOSITORY_CONNECTION;
                String errorMessage = errorCode.getErrorMessageId()
                                     + errorCode.getFormattedErrorMessage();

                throw new ConnectionCheckedException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction(),
                                                    error);
            }

            /*
             * With the connection to the real local repository established it is possible to create the wrapper
             * for the local repository.  This wrapper is seen by most OMRS Components as the local repository
             * connector.  The exceptions are the inbound event processors that work with the real local connector.
             */
            localRepositoryConnector = new LocalOMRSRepositoryConnector(realLocalConnector,
                                                                        realEventMapper,
                                                                        outboundRepositoryEventManager,
                                                                        repositoryContentManager,
                                                                        saveExchangeRule);
            localRepositoryConnector.initialize(this.getNewConnectorGUID(), localRepositoryRemoteConnection);
        }

        return localRepositoryConnector;
    }


    /**
     * Each connector has a guid to make it easier to correlate log messages from the various components that
     * serve it.  It uses a type 4 (pseudo randomly generated) UUID.
     * The UUID is generated using a cryptographically strong pseudo random number generator.
     *
     * @return guid for a new connector instance
     */
    private String  getNewConnectorGUID()
    {
        UUID newUUID = UUID.randomUUID();

        return newUUID.toString();
    }
}
