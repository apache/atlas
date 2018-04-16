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

package org.apache.atlas.omag.admin.server;

import org.apache.atlas.ocf.Connector;
import org.apache.atlas.ocf.ConnectorBroker;
import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.ocf.properties.beans.ConnectorType;
import org.apache.atlas.ocf.properties.beans.Endpoint;
import org.apache.atlas.omag.admin.LocalRepositoryMode;
import org.apache.atlas.omag.admin.OMAGServerAdministration;
import org.apache.atlas.omag.admin.OMAGServiceMode;
import org.apache.atlas.omag.configuration.properties.AccessServiceConfig;
import org.apache.atlas.omag.configuration.properties.OMAGServerConfig;
import org.apache.atlas.omag.configuration.registration.AccessServiceAdmin;
import org.apache.atlas.omag.configuration.registration.AccessServiceOperationalStatus;
import org.apache.atlas.omag.configuration.registration.AccessServiceRegistration;
import org.apache.atlas.omag.configuration.store.OMAGServerConfigStore;
import org.apache.atlas.omag.configuration.store.file.FileBasedServerConfigStoreProvider;
import org.apache.atlas.omag.ffdc.OMAGErrorCode;
import org.apache.atlas.omag.ffdc.exception.OMAGConfigurationErrorException;
import org.apache.atlas.omag.ffdc.exception.OMAGInvalidParameterException;
import org.apache.atlas.omag.ffdc.exception.OMAGNotAuthorizedException;
import org.apache.atlas.omrs.admin.OMRSConfigurationFactory;
import org.apache.atlas.omrs.admin.OMRSOperationalServices;
import org.apache.atlas.omrs.admin.properties.CohortConfig;
import org.apache.atlas.omrs.admin.properties.EnterpriseAccessConfig;
import org.apache.atlas.omrs.admin.properties.LocalRepositoryConfig;
import org.apache.atlas.omrs.admin.properties.RepositoryServicesConfig;
import org.apache.atlas.omrs.topicconnectors.OMRSTopicConnector;
import org.springframework.web.bind.annotation.*;


import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * OMAGServerAdminResource provides the server-side implementation of the administrative interface for
 * an Open Metadata and Governance (OMAG) Server.  It provides all of the
 * configuration properties for the Open Metadata Access Services (OMASs) and delegates administration requests
 * to the Open Metadata Repository Services (OMRS).
 * <p>
 * There are four types of operations defined by OMAGServerAdministration interface:
 * </p>
 * <ul>
 * <li>
 * Basic configuration - these methods use the minimum of configuration information to run the
 * server using default properties.
 * </li>
 * <li>
 * Advanced Configuration - provides access to all configuration properties to provide
 * fine-grained control of the server.
 * </li>
 * <li>
 * Initialization and shutdown - these methods control the initialization and shutdown of the
 * open metadata and governance service instance based on the supplied configuration.
 * </li>
 * <li>
 * Operational status and control - these methods query the status of the open metadata and governance
 * services as well as the audit log.
 * </li>
 * </ul>
 */
@RestController
@RequestMapping("/omag/admin/{userId}/{serverName}")
public class OMAGServerAdminResource implements OMAGServerAdministration
{
    private OMAGServerConfigStore    serverConfigStore      = null;
    private OMRSOperationalServices  operationalServices    = null;
    private List<AccessServiceAdmin> accessServiceAdminList = new ArrayList<>();



    /*
     * =============================================================
     * Configure server - basic options using defaults
     */

    /**
     * Set up the root URL for this server that is used to construct full URL paths to calls for
     * this server's REST interfaces.  The default value is "localhost:8080".
     *
     * @param userId        - user that is issuing the request.
     * @param serverName    - local server name.
     * @param url           - String url.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or serverURLRoot parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/server-url-root")
    public void setServerURLRoot(@PathVariable String userId,
                                 @PathVariable String serverName,
                                 @RequestParam String url) throws OMAGNotAuthorizedException,
                                                                            OMAGInvalidParameterException
    {
        final String methodName = "setServerURLRoot()";
        final String omagName   = "/omag/";

        validateUserId(userId, serverName, methodName);
        validateServerName(serverName, methodName);

        OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

        serverConfig.setLocalServerURL(url + omagName + serverName);

        this.saveServerConfig(serverConfig);
    }


    /**
     * Set up the descriptive type of the server.  This value is added to distributed events to
     * make it easier to understand the source of events.  The default value is "Open Metadata and Governance Server".
     *
     * @param userId     - user that is issuing the request.
     * @param serverName - local server name.
     * @param typeName   - short description for the type of server.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or serverType parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/server-type")
    public void setServerType(@PathVariable String userId,
                              @PathVariable String serverName,
                              @RequestParam String typeName) throws OMAGNotAuthorizedException,
                                                                      OMAGInvalidParameterException
    {
        final String methodName = "setServerType()";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

        serverConfig.setLocalServerType(typeName);

        this.saveServerConfig(serverConfig);
    }


    /**
     * Set up the name of the organization that is running this server.  This value is added to distributed events to
     * make it easier to understand the source of events.  The default value is null.
     *
     * @param userId           - user that is issuing the request.
     * @param serverName       - local server name.
     * @param name             - String name of the organization.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or organizationName parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/organization-name")
    public void setOrganizationName(@PathVariable String userId,
                                    @PathVariable String serverName,
                                    @RequestParam String name) throws OMAGNotAuthorizedException,
                                                                                  OMAGInvalidParameterException
    {
        final String methodName = "setOrganizationName()";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

        serverConfig.setOrganizationName(name);

        this.saveServerConfig(serverConfig);
    }


    /**
     * Set an upper limit in the page size that can be requested on a REST call to the server.  The default
     * value is 1000.
     *
     * @param userId - user that is issuing the request.
     * @param serverName - local server name.
     * @param maxPageSize - max number of elements that can be returned on a request.
     * @throws OMAGNotAuthorizedException - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or maxPageSize parameter.
     */
    public void setMaxPageSize(String  userId,
                               String  serverName,
                               int     maxPageSize) throws OMAGNotAuthorizedException,
                                                           OMAGInvalidParameterException
    {
        final String methodName = "setMaxPageSize()";

        /*
         * Validate and set up the userName and server name.
         */
        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        if (maxPageSize > 0)
        {
            OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

            serverConfig.setMaxPageSize(maxPageSize);

            this.saveServerConfig(serverConfig);
        }
        else
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.BAD_MAX_PAGE_SIZE;
            String        errorMessage = errorCode.getErrorMessageId()
                                       + errorCode.getFormattedErrorMessage(serverName, Integer.toString(maxPageSize));

            throw new OMAGInvalidParameterException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
        }
    }


    /**
     * Set up whether the open metadata access services should be enabled or not.  This is controlled by the
     * serviceMode parameter.   The configuration properties for each access service can be changed from
     * their default using setAccessServicesConfig operation.
     * <p>
     * In addition to enabling the access services, this method also enables the OMRS Enterprise Repository Services
     * that supports the enterprise access layer used by the open metadata access services.
     *
     * @param userId      - user that is issuing the request.
     * @param serverName  - local server name.
     * @param serviceMode - OMAGServiceMode enum.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or serviceMode parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/access-services/mode")
    public void setAccessServicesMode(@PathVariable String          userId,
                                      @PathVariable String          serverName,
                                      @RequestParam OMAGServiceMode serviceMode) throws OMAGNotAuthorizedException,
                                                                                        OMAGInvalidParameterException
    {
        final String methodName = "setAccessServicesMode()";

        /*
         * Validate and set up the userName and server name.
         */
        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);
        validateServiceMode(serviceMode, serverName, methodName);

        OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

        ArrayList<AccessServiceConfig> accessServiceConfigList  = new ArrayList<>();
        EnterpriseAccessConfig         enterpriseAccessConfig   = null;

        if (serviceMode == OMAGServiceMode.ENABLED)
        {
            List<AccessServiceRegistration> accessServiceRegistrationList = OMAGAccessServiceRegistration.getAccessServiceRegistrationList();

            /*
             * Set up the available access services.
             */
            if (accessServiceRegistrationList != null)
            {
                for (AccessServiceRegistration  registration : accessServiceRegistrationList)
                {
                    if (registration != null)
                    {
                        if (registration.getAccessServiceOperationalStatus() == AccessServiceOperationalStatus.ENABLED)
                        {
                            AccessServiceConfig accessServiceConfig = new AccessServiceConfig(registration);
                            accessServiceConfigList.add(accessServiceConfig);
                        }
                    }
                }
            }


            /*
             * Now set up the enterprise repository services.
             */
            OMRSConfigurationFactory configurationFactory = new OMRSConfigurationFactory();
            enterpriseAccessConfig = configurationFactory.getDefaultEnterpriseAccessConfig(serverConfig.getLocalServerName());
        }

        if (accessServiceConfigList.isEmpty())
        {
            accessServiceConfigList = null;
        }

        this.setAccessServicesConfig(userId, serverName, accessServiceConfigList);
        this.setEnterpriseAccessConfig(userId, serverName, enterpriseAccessConfig);
    }


    /**
     * Set up the type of local repository.  There are three choices: No local Repository, Local Graph Repository
     * and Repository Proxy.  The default is No Local Repository.  If the local repository mode is set to
     * Repository Proxy then it is necessary to provide the connection to the local repository using the
     * setRepositoryProxyConnection operation.
     *
     * @param userId              - user that is issuing the request.
     * @param serverName          - local server name.
     * @param repositoryMode      - LocalRepositoryMode enum - NO_LOCAL_REPOSITORY, IN_MEMORY_REPOSITORY,
     *                            LOCAL_GRAPH_REPOSITORY or REPOSITORY_PROXY.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or localRepositoryMode parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/local-repository/mode")
    public void setLocalRepositoryMode(@PathVariable String userId,
                                       @PathVariable String serverName,
                                       @RequestParam LocalRepositoryMode repositoryMode) throws OMAGNotAuthorizedException,
                                                                                                     OMAGInvalidParameterException
    {
        final String methodName = "setLocalRepositoryMode()";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

        /*
         * The local repository mode should not be null.
         */
        if (repositoryMode == null)
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.NULL_LOCAL_REPOSITORY_MODE;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(serverName);

            throw new OMAGInvalidParameterException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
        }

        LocalRepositoryConfig    localRepositoryConfig    = null;
        OMRSConfigurationFactory configurationFactory     = new OMRSConfigurationFactory();

        switch (repositoryMode)
        {
            case NO_LOCAL_REPOSITORY:
                localRepositoryConfig = null;
                break;

            case IN_MEMORY_REPOSITORY:
                localRepositoryConfig = configurationFactory.getInMemoryLocalRepositoryConfig(serverConfig.getLocalServerName(),
                                                                                              serverConfig.getLocalServerURL());
                break;

            case LOCAL_GRAPH_REPOSITORY:
                localRepositoryConfig = configurationFactory.getLocalGraphLocalRepositoryConfig(serverConfig.getLocalServerName(),
                                                                                                serverConfig.getLocalServerURL());
                break;

            case REPOSITORY_PROXY:
                localRepositoryConfig = configurationFactory.getRepositoryProxyLocalRepositoryConfig(serverConfig.getLocalServerName(),
                                                                                                     serverConfig.getLocalServerURL());
                break;

        }

        this.setLocalRepositoryConfig(userId, serverName, localRepositoryConfig);
    }


    /**
     * Provide the connection to the local repository - used when the local repository mode is set to repository proxy.
     *
     * @param userId                    - user that is issuing the request.
     * @param serverName                - local server name.
     * @param connection                - connection to the OMRS repository connector.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or repositoryProxyConnection parameter
     * @throws OMAGConfigurationErrorException - the local repository mode has not been set
     */
    @RequestMapping(method = RequestMethod.POST, path = "/local-repository/proxy-connection")
    public void setRepositoryProxyConnection(@PathVariable String     userId,
                                             @PathVariable String     serverName,
                                             @RequestParam Connection connection) throws OMAGNotAuthorizedException,
                                                                                                        OMAGInvalidParameterException,
                                                                                                        OMAGConfigurationErrorException
    {
        final String methodName = "setRepositoryProxyConnection()";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

        RepositoryServicesConfig repositoryServicesConfig = serverConfig.getRepositoryServicesConfig();
        LocalRepositoryConfig    localRepositoryConfig    = null;

        /*
         * Extract any existing local repository configuration
         */
        if (repositoryServicesConfig != null)
        {
            localRepositoryConfig = repositoryServicesConfig.getLocalRepositoryConfig();
        }

        /*
         * If the local repository config is null then the local repository mode is not set up
         */
        if (localRepositoryConfig == null)
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.LOCAL_REPOSITORY_MODE_NOT_SET;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(serverName);

            throw new OMAGConfigurationErrorException(errorCode.getHTTPErrorCode(),
                                                      this.getClass().getName(),
                                                      methodName,
                                                      errorMessage,
                                                      errorCode.getSystemAction(),
                                                      errorCode.getUserAction());
        }

        /*
         * Set up the repository proxy connection in the local repository config
         */
        localRepositoryConfig.setLocalRepositoryLocalConnection(connection);

        this.setLocalRepositoryConfig(userId, serverName, localRepositoryConfig);
    }


    /**
     * Provide the connection to the local repository - used when the local repository mode is set to repository proxy.
     *
     * @param userId                    - user that is issuing the request.
     * @param serverName                - local server name.
     * @param connectorProvider         - connector provider class name to the OMRS repository connector.
     * @param url                       - URL of the repository's native REST API.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or repositoryProxyConnection parameter
     * @throws OMAGConfigurationErrorException - the local repository mode has not been set
     */
    @RequestMapping(method = RequestMethod.POST, path = "/local-repository/proxy-details")
    public void setRepositoryProxyConnection(@PathVariable String userId,
                                             @PathVariable String serverName,
                                             @RequestParam String connectorProvider,
                                             @RequestParam String url) throws OMAGNotAuthorizedException,
                                                                              OMAGInvalidParameterException,
                                                                              OMAGConfigurationErrorException
    {
        final String methodName               = "setRepositoryProxyConnection()";
        final String endpointGUID             = UUID.randomUUID().toString();
        final String connectorTypeGUID        = UUID.randomUUID().toString();
        final String connectionGUID           = UUID.randomUUID().toString();
        final String endpointDescription      = "Metadata repository native endpoint.";
        final String connectorTypeDescription = "Metadata repository native connector type.";
        final String connectionDescription    = "Metadata repository native connection.";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        OMRSConfigurationFactory configurationFactory = new OMRSConfigurationFactory();

        String endpointName    = "MetadataRepositoryNative.Endpoint." + serverName;

        Endpoint endpoint = new Endpoint();

        endpoint.setType(configurationFactory.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(endpointName);
        endpoint.setDisplayName(endpointName);
        endpoint.setDescription(endpointDescription);
        endpoint.setAddress(url);

        String connectorTypeName = "MetadataRepositoryNative.ConnectorType." + serverName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(configurationFactory.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorProvider);

        String connectionName = "MetadataRepositoryNative.Connection." + serverName;

        Connection connection = new Connection();

        connection.setType(configurationFactory.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setEndpoint(endpoint);
        connection.setConnectorType(connectorType);

        this.setRepositoryProxyConnection(userId, serverName, connection);
    }


    /**
     * Provide the connection to the local repository's event mapper if needed.  The default value is null which
     * means no event mapper.  An event mapper is needed if the local repository has additional APIs that can change
     * the metadata in the repository without going through the open metadata and governance services.
     *
     * @param userId                     - user that is issuing the request.
     * @param serverName                 - local server name.
     * @param connection - connection to the OMRS repository event mapper.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or localRepositoryEventMapper parameter.
     * @throws OMAGConfigurationErrorException - the local repository mode has not been set
     */
    @RequestMapping(method = RequestMethod.POST, path = "/local-repository/event-mapper-connection")
    public void setLocalRepositoryEventMapper(@PathVariable String     userId,
                                              @PathVariable String     serverName,
                                              @RequestParam Connection connection) throws OMAGNotAuthorizedException,
                                                                                                          OMAGInvalidParameterException,
                                                                                                          OMAGConfigurationErrorException
    {
        final String methodName = "setLocalRepositoryEventMapper()";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

        RepositoryServicesConfig repositoryServicesConfig = serverConfig.getRepositoryServicesConfig();
        LocalRepositoryConfig    localRepositoryConfig    = null;

        /*
         * Extract any existing local repository configuration
         */
        if (repositoryServicesConfig != null)
        {
            localRepositoryConfig = repositoryServicesConfig.getLocalRepositoryConfig();
        }

        /*
         * The local repository should be partially configured already by setLocalRepositoryMode()
         */
        if (localRepositoryConfig == null)
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.LOCAL_REPOSITORY_MODE_NOT_SET;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(serverName);

            throw new OMAGConfigurationErrorException(errorCode.getHTTPErrorCode(),
                                                      this.getClass().getName(),
                                                      methodName,
                                                      errorMessage,
                                                      errorCode.getSystemAction(),
                                                      errorCode.getUserAction());
        }

        /*
         * Set up the event mapper connection in the local repository config
         */
        localRepositoryConfig.setEventMapperConnection(connection);

        this.setLocalRepositoryConfig(userId, serverName, localRepositoryConfig);
    }


    /**
     * Provide the connection to the local repository's event mapper if needed.  The default value is null which
     * means no event mapper.  An event mapper is needed if the local repository has additional APIs that can change
     * the metadata in the repository without going through the open metadata and governance services.
     *
     * @param userId                     - user that is issuing the request.
     * @param serverName                 - local server name.
     * @param connectorProvider          - Java class name of the connector provider for the OMRS repository event mapper.
     * @param eventSource                - topic name or URL to the native event source.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or localRepositoryEventMapper parameter.
     * @throws OMAGConfigurationErrorException - the local repository mode has not been set
     */
    @RequestMapping(method = RequestMethod.POST, path = "/local-repository/event-mapper-details")
    public void setLocalRepositoryEventMapper(@PathVariable String     userId,
                                              @PathVariable String     serverName,
                                              @RequestParam String     connectorProvider,
                                              @RequestParam String     eventSource) throws OMAGNotAuthorizedException,
                                                                                           OMAGInvalidParameterException,
                                                                                           OMAGConfigurationErrorException
    {
        final String methodName = "setLocalRepositoryEventMapper()";
        final String endpointGUID             = UUID.randomUUID().toString();
        final String connectorTypeGUID        = UUID.randomUUID().toString();
        final String connectionGUID           = UUID.randomUUID().toString();
        final String endpointDescription      = "Event mapper endpoint.";
        final String connectorTypeDescription = "Event mapper connector type.";
        final String connectionDescription    = "Event mapper connection.";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        OMRSConfigurationFactory configurationFactory = new OMRSConfigurationFactory();

        String endpointName    = "EventMapper.Endpoint." + serverName;

        Endpoint endpoint = new Endpoint();

        endpoint.setType(configurationFactory.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(endpointName);
        endpoint.setDisplayName(endpointName);
        endpoint.setDescription(endpointDescription);
        endpoint.setAddress(eventSource);

        String connectorTypeName = "EventMapper.ConnectorType." + serverName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(configurationFactory.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorProvider);

        String connectionName = "EventMapper.Connection." + serverName;

        Connection connection = new Connection();

        connection.setType(configurationFactory.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setEndpoint(endpoint);
        connection.setConnectorType(connectorType);

        this.setLocalRepositoryEventMapper(userId, serverName, connection);
    }


    /**
     * Set up the mode for an open metadata repository cohort.  This is a group of open metadata repositories that
     * are sharing metadata.  An OMAG server can connect to zero, one or more cohorts.  Each cohort needs
     * a unique name.  The members of the cohort use a shared topic to exchange registration information and
     * events related to changes in their supported metadata types and instances.  They are also able to
     * query each other's metadata directly through REST calls.
     *
     * @param userId      - user that is issuing the request.
     * @param serverName  - local server name.
     * @param cohortName  - name of the cohort.
     * @param serviceMode - OMAGServiceMode enum - ENABLED or DISABLED.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName, cohortName or serviceMode parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/cohort/{cohortName}/mode")
    public void setCohortMode(@PathVariable String          userId,
                              @PathVariable String          serverName,
                              @PathVariable String          cohortName,
                              @RequestParam OMAGServiceMode serviceMode) throws OMAGNotAuthorizedException,
                                                                                OMAGInvalidParameterException
    {
        final String methodName = "setCohortMode()";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);
        validateCohortName(cohortName, serverName, methodName);
        validateServiceMode(serviceMode, serverName, methodName);

        OMAGServerConfig serverConfig    = this.getServerConfig(serverName, methodName);
        CohortConfig     newCohortConfig = null;

        /*
         * Build a new cohort configuration if requested.
         */
        if (serviceMode == OMAGServiceMode.ENABLED)
        {
            /*
             * Set up a new cohort
             */
            OMRSConfigurationFactory configurationFactory = new OMRSConfigurationFactory();

            newCohortConfig = configurationFactory.getDefaultCohortConfig(serverConfig.getLocalServerName(), cohortName);
        }

        this.setCohortConfig(userId, serverName, cohortName, newCohortConfig);
    }

    /*
     * =============================================================
     * Configure server - advanced options overriding defaults
     */


    /**
     * Set up the configuration for all of the open metadata access services (OMASs).  This overrides
     * the current values.
     *
     * @param userId               - user that is issuing the request.
     * @param serverName           - local server name.
     * @param accessServicesConfig - list of configuration properties for each access service.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or accessServicesConfig parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/access-services/configuration")
    public void setAccessServicesConfig(@PathVariable String                    userId,
                                        @PathVariable String                    serverName,
                                        @RequestParam List<AccessServiceConfig> accessServicesConfig) throws OMAGNotAuthorizedException,
                                                                                                             OMAGInvalidParameterException
    {
        final String methodName = "setAccessServicesConfig()";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

        serverConfig.setAccessServicesConfig(accessServicesConfig);

        this.saveServerConfig(serverConfig);
    }


    /**
     * Set up the configuration for the local repository.  This overrides the current values.
     *
     * @param userId                - user that is issuing the request.
     * @param serverName            - local server name.
     * @param localRepositoryConfig - configuration properties for the local repository.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or localRepositoryConfig parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/local-repository/configuration")
    public void setLocalRepositoryConfig(@PathVariable String                userId,
                                         @PathVariable String                serverName,
                                         @RequestParam LocalRepositoryConfig localRepositoryConfig) throws OMAGNotAuthorizedException,
                                                                                                           OMAGInvalidParameterException
    {
        final String methodName = "setLocalRepositoryConfig";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

        RepositoryServicesConfig repositoryServicesConfig = serverConfig.getRepositoryServicesConfig();

        /*
         * Set up the local repository config in the open metadata repository services config.
         */
        if (repositoryServicesConfig != null)
        {
            repositoryServicesConfig.setLocalRepositoryConfig(localRepositoryConfig);
        }
        else if (localRepositoryConfig != null)
        {
            OMRSConfigurationFactory configurationFactory     = new OMRSConfigurationFactory();

            repositoryServicesConfig = configurationFactory.getDefaultRepositoryServicesConfig(serverConfig.getLocalServerName());
            repositoryServicesConfig.setLocalRepositoryConfig(localRepositoryConfig);
        }

        /*
         * Save the open metadata repository services config in the server's config
         */
        serverConfig.setRepositoryServicesConfig(repositoryServicesConfig);
        this.saveServerConfig(serverConfig);
    }


    /**
     * Set up the configuration that controls the enterprise repository services.  These services are part
     * of the Open Metadata Repository Services (OMRS).  They provide federated queries and federated event
     * notifications that cover metadata from the local repository plus any repositories connected via
     * open metadata repository cohorts.
     *
     * @param userId                 - user that is issuing the request
     * @param serverName             - local server name
     * @param enterpriseAccessConfig - enterprise repository services configuration properties.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName or enterpriseAccessConfig parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/enterprise-access/configuration")
    public void setEnterpriseAccessConfig(@PathVariable String                 userId,
                                          @PathVariable String                 serverName,
                                          @RequestParam EnterpriseAccessConfig enterpriseAccessConfig) throws OMAGNotAuthorizedException,
                                                                                                              OMAGInvalidParameterException
    {
        final String methodName = "setEnterpriseAccessConfig";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        OMAGServerConfig serverConfig = this.getServerConfig(serverName, methodName);

        RepositoryServicesConfig repositoryServicesConfig = serverConfig.getRepositoryServicesConfig();

        if (repositoryServicesConfig != null)
        {
            repositoryServicesConfig.setEnterpriseAccessConfig(enterpriseAccessConfig);
        }
        else if (enterpriseAccessConfig != null)
        {
            OMRSConfigurationFactory configurationFactory     = new OMRSConfigurationFactory();

            repositoryServicesConfig = configurationFactory.getDefaultRepositoryServicesConfig(serverConfig.getLocalServerName());

            repositoryServicesConfig.setEnterpriseAccessConfig(enterpriseAccessConfig);
        }

        serverConfig.setRepositoryServicesConfig(repositoryServicesConfig);
        this.saveServerConfig(serverConfig);
    }


    /**
     * Set up the configuration properties for a cohort.  This may reconfigure an existing cohort or create a
     * cohort.  Use setCohortMode to delete a cohort.
     *
     * @param userId       - user that is issuing the request
     * @param serverName   - local server name
     * @param cohortName   - name of the cohort
     * @param cohortConfig - configuration for the cohort
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName, cohortName or cohortConfig parameter.
     */
    @RequestMapping(method = RequestMethod.POST, path = "/cohort/{cohortName}/configuration")
    public void setCohortConfig(@PathVariable String       userId,
                                @PathVariable String       serverName,
                                @PathVariable String       cohortName,
                                @RequestParam CohortConfig cohortConfig) throws OMAGNotAuthorizedException,
                                                                                OMAGInvalidParameterException
    {
        final String methodName = "setCohortConfig";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);
        validateCohortName(cohortName, serverName, methodName);

        OMAGServerConfig         serverConfig = this.getServerConfig(serverName, methodName);
        OMRSConfigurationFactory configurationFactory = new OMRSConfigurationFactory();
        RepositoryServicesConfig repositoryServicesConfig = serverConfig.getRepositoryServicesConfig();
        List<CohortConfig>       existingCohortConfigs = null;
        List<CohortConfig>       newCohortConfigs = new ArrayList<>();

        /*
         * Extract any existing local repository configuration
         */
        if (repositoryServicesConfig != null)
        {
            existingCohortConfigs = repositoryServicesConfig.getCohortConfigList();
        }

        /*
         * Transfer the cohort configurations of all other cohorts into the new cohort list
         */
        if (existingCohortConfigs != null)
        {
            /*
             * If there is already a cohort of the same name then effectively remove it.
             */
            for (CohortConfig existingCohort : existingCohortConfigs)
            {
                if (existingCohort != null)
                {
                    String existingCohortName = existingCohort.getCohortName();

                    if (! cohortName.equals(existingCohortName))
                    {
                        newCohortConfigs.add(existingCohort);
                    }
                }
            }
        }

        /*
         * Add the new cohort to the list of cohorts
         */
        if (cohortConfig != null)
        {
            newCohortConfigs.add(cohortConfig);
        }

        /*
         * If there are no cohorts to save then remove the array list.
         */
        if (newCohortConfigs.isEmpty())
        {
            newCohortConfigs = null;
        }

        /*
         * Add the cohort list to the open metadata repository services config
         */
        if (repositoryServicesConfig != null)
        {
            repositoryServicesConfig.setCohortConfigList(newCohortConfigs);
        }
        else if (newCohortConfigs != null)
        {
            repositoryServicesConfig = configurationFactory.getDefaultRepositoryServicesConfig(serverConfig.getLocalServerName());

            repositoryServicesConfig.setCohortConfigList(newCohortConfigs);
        }

        serverConfig.setRepositoryServicesConfig(repositoryServicesConfig);
        this.saveServerConfig(serverConfig);
    }


    /*
     * =============================================================
     * Query current configuration
     */


    /**
     * Return the complete set of configuration properties in use by the server.
     *
     * @param userId     - user that is issuing the request
     * @param serverName - local server name
     * @return OMAGServerConfig properties
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - invalid serverName parameter.
     */
    @RequestMapping(method = RequestMethod.GET, path = "/configuration")
    public OMAGServerConfig getCurrentConfiguration(@PathVariable String userId,
                                                    @PathVariable String serverName) throws OMAGNotAuthorizedException,
                                                                                            OMAGInvalidParameterException
    {
        final String methodName = "getCurrentConfiguration";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        return this.getServerConfig(serverName, methodName);
    }


    /*
     * =============================================================
     * Initialization and shutdown
     */

    /**
     * Initialize the open metadata and governance services using the stored configuration information.
     *
     * @param userId     - user that is issuing the request
     * @param serverName - local server name
     * @throws OMAGConfigurationErrorException - there is a problem using the supplied configuration
     */
    @RequestMapping(method = RequestMethod.POST, path = "/instance")
    public void initialize(@PathVariable String userId,
                           @PathVariable String serverName) throws OMAGNotAuthorizedException,
                                                                   OMAGInvalidParameterException,
                                                                   OMAGConfigurationErrorException
    {
        final String methodName = "initialize";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        this.initialize(userId, serverName, this.getServerConfig(serverName, methodName));
    }


    /**
     * Initialize the open metadata and governance services using the supplied information.
     *
     * @param userId        - user that is issuing the request
     * @param configuration - properties used to initialize the services
     * @param serverName    - local server name
     * @throws OMAGNotAuthorizedException      - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException   - the serverName is invalid.
     * @throws OMAGConfigurationErrorException - there is a problem using the supplied configuration
     */
    @RequestMapping(method = RequestMethod.POST, path = "/instance/configuration")
    public void initialize(@PathVariable String           userId,
                           @PathVariable String           serverName,
                           @RequestParam OMAGServerConfig configuration) throws OMAGNotAuthorizedException,
                                                                                OMAGInvalidParameterException,
                                                                                OMAGConfigurationErrorException
    {
        final String methodName = "initialize";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        if (configuration == null)
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.NULL_SERVER_CONFIG;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(serverName);

            throw new OMAGInvalidParameterException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
        }
        else
        {
            this.saveServerConfig(configuration);
        }

        /*
         * Initialize the open metadata repository services first
         */
        RepositoryServicesConfig  repositoryServicesConfig = configuration.getRepositoryServicesConfig();

        if (repositoryServicesConfig == null)
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.NULL_REPOSITORY_CONFIG;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(serverName);

            throw new OMAGConfigurationErrorException(errorCode.getHTTPErrorCode(),
                                                      this.getClass().getName(),
                                                      methodName,
                                                      errorMessage,
                                                      errorCode.getSystemAction(),
                                                      errorCode.getUserAction());
        }
        else if (operationalServices != null)
        {
            this.terminate(userId, serverName, false);
        }

        operationalServices = new OMRSOperationalServices(configuration.getLocalServerName(),
                                                          configuration.getLocalServerType(),
                                                          configuration.getOrganizationName(),
                                                          configuration.getLocalServerURL(),
                                                          configuration.getMaxPageSize());

        operationalServices.initialize(repositoryServicesConfig);

        /*
         * Now initialize the open metadata access services
         */
        List<AccessServiceConfig> accessServiceConfigList  = configuration.getAccessServicesConfig();
        OMRSTopicConnector        enterpriseTopicConnector = operationalServices.getEnterpriseOMRSTopicConnector();

        if (accessServiceConfigList != null)
        {
            for (AccessServiceConfig  accessServiceConfig : accessServiceConfigList)
            {
                if (accessServiceConfig != null)
                {
                    String    accessServiceAdminClassName = accessServiceConfig.getAccessServiceAdminClass();

                    if (accessServiceAdminClassName != null)
                    {
                        try
                        {
                            AccessServiceAdmin   accessServiceAdmin = (AccessServiceAdmin)Class.forName(accessServiceAdminClassName).newInstance();

                            accessServiceAdmin.initialize(accessServiceConfig,
                                                          enterpriseTopicConnector,
                                                          operationalServices.getEnterpriseOMRSRepositoryConnector(accessServiceConfig.getAccessServiceName()),
                                                          operationalServices.getAuditLog(accessServiceConfig.getAccessServiceId(),
                                                                                          accessServiceConfig.getAccessServiceName(),
                                                                                          accessServiceConfig.getAccessServiceDescription(),
                                                                                          accessServiceConfig.getAccessServiceWiki()),
                                                          "OMASUser");
                            accessServiceAdminList.add(accessServiceAdmin);
                        }
                        catch (Throwable  error)
                        {
                            OMAGErrorCode errorCode    = OMAGErrorCode.BAD_ACCESS_SERVICE_ADMIN_CLASS;
                            String        errorMessage = errorCode.getErrorMessageId()
                                                       + errorCode.getFormattedErrorMessage(serverName,
                                                                                            accessServiceAdminClassName,
                                                                                            accessServiceConfig.getAccessServiceName());

                            throw new OMAGConfigurationErrorException(errorCode.getHTTPErrorCode(),
                                                                      this.getClass().getName(),
                                                                      methodName,
                                                                      errorMessage,
                                                                      errorCode.getSystemAction(),
                                                                      errorCode.getUserAction());
                        }
                    }
                    else
                    {
                        OMAGErrorCode errorCode    = OMAGErrorCode.NULL_ACCESS_SERVICE_ADMIN_CLASS;
                        String        errorMessage = errorCode.getErrorMessageId()
                                                   + errorCode.getFormattedErrorMessage(serverName,
                                                                                        accessServiceConfig.getAccessServiceName());

                        throw new OMAGConfigurationErrorException(errorCode.getHTTPErrorCode(),
                                                                  this.getClass().getName(),
                                                                  methodName,
                                                                  errorMessage,
                                                                  errorCode.getSystemAction(),
                                                                  errorCode.getUserAction());
                    }
                }
            }
        }

        if (enterpriseTopicConnector != null)
        {
            try
            {
                enterpriseTopicConnector.start();
            }
            catch (Throwable  error)
            {
                OMAGErrorCode errorCode    = OMAGErrorCode.ENTERPRISE_TOPIC_START_FAILED;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(serverName, error.getMessage());

                throw new OMAGConfigurationErrorException(errorCode.getHTTPErrorCode(),
                                                          this.getClass().getName(),
                                                          methodName,
                                                          errorMessage,
                                                          errorCode.getSystemAction(),
                                                          errorCode.getUserAction());
            }
        }
    }


    /**
     * Terminate any open metadata and governance services.
     *
     * @param userId     - user that is issuing the request
     * @param serverName - local server name
     * @param permanent - Is the server being shutdown permanently - if yes, the local server will unregister from
     *                  its open metadata repository cohorts.
     * @throws OMAGNotAuthorizedException    - the supplied userId is not authorized to issue this command.
     * @throws OMAGInvalidParameterException - the serverName is invalid.
     */
    @RequestMapping(method = RequestMethod.DELETE, path = "/instance")
    public void terminate(@PathVariable String  userId,
                          @PathVariable String  serverName,
                          @RequestParam boolean permanent) throws OMAGNotAuthorizedException,
                                                                  OMAGInvalidParameterException
    {
        final String methodName = "terminate";

        validateServerName(serverName, methodName);
        validateUserId(userId, serverName, methodName);

        /*
         * Shutdown the access services
         */
        if (accessServiceAdminList != null)
        {
            for (AccessServiceAdmin  accessServiceAdmin : accessServiceAdminList)
            {
                if (accessServiceAdmin != null)
                {
                    accessServiceAdmin.shutdown();
                }
            }
        }

        /*
         * Terminate the OMRS
         */
        if (operationalServices != null)
        {
            operationalServices.disconnect(permanent);
            operationalServices = null;
        }
    }


    /*
     * =============================================================
     * Operational status and control
     */

    /* placeholder */


    /*
     * =============================================================
     * Private methods
     */

    /**
     * Validate that the user id is not null.
     *
     * @param userId - user name passed on the request
     * @param serverName - name of this server
     * @param methodName - method receiving the call
     * @throws OMAGNotAuthorizedException - no userId provided
     */
    private void validateUserId(String userId,
                                String serverName,
                                String methodName) throws OMAGNotAuthorizedException
    {
        if (userId == null)
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.NULL_USER_NAME;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(serverName);

            throw new OMAGNotAuthorizedException(errorCode.getHTTPErrorCode(),
                                                 this.getClass().getName(),
                                                 methodName,
                                                 errorMessage,
                                                 errorCode.getSystemAction(),
                                                 errorCode.getUserAction());
        }
    }


    /**
     * Validate that the server name is not null and save it in the config.
     *
     * @param serverName - serverName passed on a request
     * @param methodName - method being called
     * @throws OMAGInvalidParameterException - null server name
     */
    private void validateServerName(String serverName,
                                    String methodName) throws OMAGInvalidParameterException
    {
        /*
         * If the local server name is still null then save the server name in the configuration.
         */
        if (serverName == null)
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.NULL_LOCAL_SERVER_NAME;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMAGInvalidParameterException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
        }
    }


    /**
     * Validate that the service mode passed on a request is not null.
     *
     * @param serviceMode - indicates the mode a specific service should be set to
     * @param serverName - name of this server
     * @param methodName - name of the method called.
     * @throws OMAGInvalidParameterException - the service mode is null
     */
    private void validateServiceMode(OMAGServiceMode   serviceMode,
                                     String            serverName,
                                     String            methodName) throws OMAGInvalidParameterException
    {
        if (serviceMode == null)
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.NULL_SERVICE_MODE;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(serverName);

            throw new OMAGInvalidParameterException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
        }
    }


    /**
     * Validate that the cohort name is not null.
     *
     * @param cohortName - cohortName passed on the request
     * @param serverName - server name for this server
     * @param methodName - method called
     * @throws OMAGInvalidParameterException the cohort name is null
     */
    private void validateCohortName(String  cohortName,
                                    String  serverName,
                                    String  methodName) throws OMAGInvalidParameterException
    {
        if (cohortName == null)
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.NULL_COHORT_NAME;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(serverName);

            throw new OMAGInvalidParameterException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
        }
    }


    /**
     * Retrieve any saved configuration for this server.
     *
     * @param serverName - name of the server
     * @param methodName - method requesting the server details
     * @return = configuration properties
     * @throws OMAGInvalidParameterException - problem with the configuration file
     */
    private  OMAGServerConfig   getServerConfig(String   serverName,
                                                String   methodName) throws OMAGInvalidParameterException
    {
        if (serverConfigStore == null)
        {
            Endpoint   endpoint = new Endpoint();
            endpoint.setAddress("omag.server." + serverName + ".config");

            ConnectorType  connectorType = new ConnectorType();
            connectorType.setConnectorProviderClassName(FileBasedServerConfigStoreProvider.class.getName());

            Connection connection = new Connection();
            connection.setEndpoint(endpoint);
            connection.setConnectorType(connectorType);
            connection.setQualifiedName(endpoint.getAddress());

            try
            {

                ConnectorBroker connectorBroker = new ConnectorBroker();

                Connector connector = connectorBroker.getConnector(connection);

                serverConfigStore = (OMAGServerConfigStore) connector;
            }
            catch (Throwable   error)
            {
                OMAGErrorCode errorCode    = OMAGErrorCode.BAD_CONFIG_FILE;
                String        errorMessage = errorCode.getErrorMessageId()
                                           + errorCode.getFormattedErrorMessage(serverName, methodName, error.getMessage());

                throw new OMAGInvalidParameterException(errorCode.getHTTPErrorCode(),
                                                        this.getClass().getName(),
                                                        methodName,
                                                        errorMessage,
                                                        errorCode.getSystemAction(),
                                                        errorCode.getUserAction(),
                                                        error);
            }
        }

        OMAGServerConfig serverConfig = serverConfigStore.retrieveServerConfig();

        if (serverConfig == null)
        {
            serverConfig = new OMAGServerConfig();
        }

        serverConfig.setLocalServerName(serverName);

        return serverConfig;
    }


    /**
     * Save the Server config ...
     *
     * @param serverConfig - properties to save
     * @throws OMAGInvalidParameterException - problem with the config file
     */
    private  void saveServerConfig(OMAGServerConfig  serverConfig) throws OMAGInvalidParameterException
    {
        final String  methodName = "saveServerConfig";

        if (serverConfigStore != null)
        {
            serverConfigStore.saveServerConfig(serverConfig);
        }
        else
        {
            OMAGErrorCode errorCode    = OMAGErrorCode.NULL_CONFIG_FILE;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage();

            throw new OMAGInvalidParameterException(errorCode.getHTTPErrorCode(),
                                                    this.getClass().getName(),
                                                    methodName,
                                                    errorMessage,
                                                    errorCode.getSystemAction(),
                                                    errorCode.getUserAction());
        }
    }
}
