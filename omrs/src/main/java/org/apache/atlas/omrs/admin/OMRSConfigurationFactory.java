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

import org.apache.atlas.ocf.properties.ElementOrigin;
import org.apache.atlas.ocf.properties.beans.Connection;
import org.apache.atlas.ocf.properties.beans.ConnectorType;
import org.apache.atlas.ocf.properties.beans.Endpoint;
import org.apache.atlas.ocf.properties.beans.ElementType;


import org.apache.atlas.omrs.adapters.atlas.repositoryconnector.LocalAtlasOMRSRepositoryConnectorProvider;
import org.apache.atlas.omrs.adapters.inmemory.repositoryconnector.InMemoryOMRSRepositoryConnectorProvider;
import org.apache.atlas.omrs.admin.properties.*;
import org.apache.atlas.omrs.archivemanager.store.file.FileBasedOpenMetadataArchiveStoreProvider;
import org.apache.atlas.omrs.auditlog.store.file.FileBasedAuditLogStoreProvider;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.store.file.FileBasedRegistryStoreProvider;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;
import org.apache.atlas.omrs.rest.repositoryconnector.OMRSRESTRepositoryConnectorProvider;
import org.apache.atlas.omrs.topicconnectors.inmemory.InMemoryOMRSTopicProvider;
import org.apache.atlas.omrs.topicconnectors.kafka.KafkaOMRSTopicProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * OMRSConfigurationFactory sets up default configuration for the OMRS components.  It is used by the OMAG server
 * while it manages the changes made to the server configuration by the server administrator.  The aim is to
 * build up the RepositoryServicesConfig object that is used to initialize the OMRSOperationalServices.
 */
public class OMRSConfigurationFactory
{
    /*
     * Default property fillers
     */
    private static final String defaultEnterpriseMetadataCollectionName = " Enterprise Metadata Collection";

    private static final String defaultTopicRootName = "omag/omrs/";
    private static final String defaultTopicLeafName = "/OMRSTopic";

    private static final String defaultEnterpriseTopicConnectorRootName = "enterprise/";
    private static final String defaultCohortTopicConnectorRootName     = "cohort/";

    private static final String defaultCohortName = "defaultCohort";

    private static final String defaultOpenMetadataArchiveFileName = "OpenMetadataTypes.json";

    /**
     * Default constructor
     */
    public OMRSConfigurationFactory()
    {
    }


    /**
     * Return the connection for the default audit log.
     * By default, the Audit log is stored in a directory called localServerName.auditlog.
     *
     * @param localServerName - name of the local server
     * @return OCF Connection used to create the file-based audit logger
     */
    private Connection getDefaultAuditLogConnection(String localServerName)
    {
        final String endpointGUID      = "836efeae-ab34-4425-89f0-6adf2faa1f2e";
        final String connectorTypeGUID = "f8a24f09-9183-4d5c-8408-aa1c8852a7d6";
        final String connectionGUID    = "5390bf3e-6b38-4eda-b34a-de55ac4252a7";

        final String endpointDescription = "OMRS default audit log endpoint.";

        String endpointAddress = localServerName + ".auditlog";
        String endpointName    = "DefaultAuditLog.Endpoint." + endpointAddress;

        Endpoint endpoint = new Endpoint();

        endpoint.setType(this.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(endpointName);
        endpoint.setDisplayName(endpointName);
        endpoint.setDescription(endpointDescription);
        endpoint.setAddress(endpointAddress);

        final String connectorTypeDescription   = "OMRS default audit log connector type.";
        final String connectorTypeJavaClassName = FileBasedAuditLogStoreProvider.class.getName();

        String connectorTypeName = "DefaultAuditLog.ConnectorType." + localServerName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorTypeJavaClassName);

        final String connectionDescription = "OMRS default audit log connection.";

        String connectionName = "DefaultAuditLog.Connection." + localServerName;

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setEndpoint(endpoint);
        connection.setConnectorType(connectorType);

        return connection;
    }


    /**
     * Return the connection for the default audit log.
     * By default, the open metadata is stored in a file called localServerName.auditlog.
     *
     * @return OCF Connection used to create the file-based open metadata archive
     */
    public Connection getOpenMetadataTypesConnection()
    {
        final String endpointGUID      = "45877b9c-9192-44ba-a2b7-6817bc753969";
        final String connectorTypeGUID = "86f52a17-5d3c-47fd-9cac-0b5a45d150a9";
        final String connectionGUID    = "447bbb33-84f9-4a56-a712-addeebdcd764";

        final String endpointDescription = "Open metadata types archive endpoint.";

        String endpointAddress = defaultOpenMetadataArchiveFileName;
        String endpointName    = "OpenMetadataTypes.Endpoint" + endpointAddress;

        Endpoint endpoint = new Endpoint();

        endpoint.setType(this.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(endpointName);
        endpoint.setDisplayName(endpointName);
        endpoint.setDescription(endpointDescription);
        endpoint.setAddress(endpointAddress);

        final String connectorTypeDescription   = "Open metadata types archive connector type.";
        final String connectorTypeJavaClassName = FileBasedOpenMetadataArchiveStoreProvider.class.getName();

        String connectorTypeName = "OpenMetadataTypes.ConnectorType";

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorTypeJavaClassName);

        final String connectionDescription = "Open metadata types archive connection.";

        String connectionName = "OpenMetadataTypes.Connection";

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setEndpoint(endpoint);
        connection.setConnectorType(connectorType);

        return connection;
    }


    /**
     * Return the default local repository's local connection.  This is set to null which means use the remote
     * connection.
     *
     * @return null Connection object
     */
    private Connection getDefaultLocalRepositoryLocalConnection()
    {
        return null;
    }


    /**
     * Return the local graph repository's connection.  This is using the LocalAtlasOMRSRepositoryConnector.
     *
     * @param localServerName - name of the local server
     * @return Connection object
     */
    private Connection getLocalGraphRepositoryLocalConnection(String localServerName)
    {
        final String connectorTypeGUID = "18530415-44a2-4bd0-95bb-8efd333e53fb";
        final String connectionGUID    = "3f1fd4fc-90f9-436a-8e2c-2120d590f5e4";

        final String connectorTypeDescription   = "OMRS default graph local repository connector type.";
        final String connectorTypeJavaClassName = LocalAtlasOMRSRepositoryConnectorProvider.class.getName();

        String connectorTypeName = "DefaultLocalGraphRepository.ConnectorType." + localServerName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorTypeJavaClassName);


        final String connectionDescription = "OMRS default local graph repository connection.";

        String connectionName = "DefaultLocalGraphRepository.Connection." + localServerName;

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setConnectorType(connectorType);

        return connection;
    }


    /**
     * Return the in-memory local repository connection.  This is using the InMemoryOMRSRepositoryConnector.
     *
     * @param localServerName - name of the local server
     * @return Connection object
     */
    private Connection getInMemoryLocalRepositoryLocalConnection(String localServerName)
    {
        final String connectorTypeGUID = "21422eb9-c6c1-4071-b96b-0572c9680260";
        final String connectionGUID    = "6a3c07b0-0e04-42dc-bcc6-392609bf1d02";

        final String connectorTypeDescription   = "OMRS default in memory local repository connector type.";
        final String connectorTypeJavaClassName = InMemoryOMRSRepositoryConnectorProvider.class.getName();

        String connectorTypeName = "DefaultInMemoryRepository.ConnectorType." + localServerName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorTypeJavaClassName);


        final String connectionDescription = "OMRS default in memory local repository connection.";

        String connectionName = "DefaultInMemoryRepository.Connection." + localServerName;

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setConnectorType(connectorType);

        return connection;
    }


    /**
     * Return the Connection for this server's OMRS Repository REST API.  If the localServerURL is
     * something like localhost:8080/omag/localServerName and the REST API URL would be
     * localhost:8080/omag/localServerName/omrs/metadatacollection.
     *
     * @param localServerName - name of the local server
     * @param localServerURL - root of the local server's URL
     * @return Connection object
     */
    private  Connection getDefaultLocalRepositoryRemoteConnection(String localServerName,
                                                                 String localServerURL)
    {
        final String endpointGUID      = "cee85898-43aa-4af5-9bbd-2bed809d1acb";
        final String connectorTypeGUID = "64e67923-8190-45ea-8f96-39320d638c02";
        final String connectionGUID    = "858be98b-49d2-4ccf-9b23-01085a5f473f";

        final String endpointDescription = "OMRS default repository REST API endpoint.";

        String endpointAddress = localServerURL + "/omag/omrs/";
        String endpointName    = "DefaultRepositoryRESTAPI.Endpoint." + localServerName;

        Endpoint endpoint = new Endpoint();

        endpoint.setType(this.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(endpointName);
        endpoint.setDisplayName(endpointName);
        endpoint.setDescription(endpointDescription);
        endpoint.setAddress(endpointAddress);

        final String connectorTypeDescription   = "OMRS default repository REST API connector type.";
        final String connectorTypeJavaClassName = OMRSRESTRepositoryConnectorProvider.class.getName();

        String connectorTypeName = "DefaultRepositoryRESTAPI.ConnectorType." + localServerName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorTypeJavaClassName);


        final String connectionDescription = "OMRS default repository REST API connection.";

        String connectionName = "DefaultRepositoryRESTAPI.Connection." + localServerName;

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setEndpoint(endpoint);
        connection.setConnectorType(connectorType);

        return connection;
    }


    /**
     * Return the default local repository event mapper.  This is null since the use of, or need for, the event mapper
     * is determined by the type of local repository.
     *
     * @return null Connection object
     */
    private Connection getDefaultEventMapperConnection()
    {
        return null;
    }


    /**
     * Return the default connection for the enterprise OMRS topic.  This uses a Kafka topic called
     * omag/omrs/enterprise/localServerName/OMRSTopic.
     *
     * @param localServerName - name of local server
     * @return Connection object
     */
    private Connection getDefaultEnterpriseOMRSTopicConnection(String localServerName)
    {
        final String endpointGUID      = "e0d88035-8522-42bc-b57f-06df05f15825";
        final String connectorTypeGUID = "6536cb46-61f0-4f2d-abb4-2dadede30520";
        final String connectionGUID    = "2084ee90-717b-49a1-938e-8f9d49567b8e";

        final String endpointDescription = "OMRS default enterprise topic endpoint.";

        String endpointAddress = defaultTopicRootName + defaultEnterpriseTopicConnectorRootName + localServerName + defaultTopicLeafName;
        String endpointName    = "DefaultEnterpriseTopic.Endpoint." + endpointAddress;

        Endpoint endpoint = new Endpoint();

        endpoint.setType(this.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(endpointName);
        endpoint.setDisplayName(endpointName);
        endpoint.setDescription(endpointDescription);
        endpoint.setAddress(endpointAddress);


        final String connectorTypeDescription   = "OMRS default enterprise connector type.";
        final String connectorTypeJavaClassName = InMemoryOMRSTopicProvider.class.getName();

        String connectorTypeName = "DefaultEnterpriseTopic.ConnectorType." + localServerName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorTypeJavaClassName);


        final String connectionDescription = "OMRS default enterprise topic connection.";

        String connectionName = "DefaultEnterpriseTopic.Connection." + localServerName;

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setEndpoint(endpoint);
        connection.setConnectorType(connectorType);

        return connection;
    }


    /**
     * Return the connection for the OMRS topic for the named cohort.
     *
     * @param cohortName - name of the cohort
     * @return Connection object
     */
    private Connection getDefaultCohortOMRSTopicConnection(String cohortName)
    {
        final String endpointGUID      = "dca783a1-d5f9-44a8-b838-4de4d016303d";
        final String connectorTypeGUID = "32843dd8-2597-4296-831c-674af0d8b837";
        final String connectionGUID    = "023bb1f3-03dd-47ae-b3bc-dce62e9c11cb";

        final String endpointDescription = "OMRS default cohort topic endpoint.";

        String endpointAddress = defaultTopicRootName + defaultCohortTopicConnectorRootName + cohortName + defaultTopicLeafName;
        String endpointName    = "DefaultCohortTopic.Endpoint." + endpointAddress;

        Endpoint endpoint = new Endpoint();

        endpoint.setType(this.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(endpointName);
        endpoint.setDisplayName(endpointName);
        endpoint.setDescription(endpointDescription);
        endpoint.setAddress(endpointAddress);


        final String connectorTypeDescription   = "OMRS default cohort topic connector type.";
        final String connectorTypeJavaClassName = KafkaOMRSTopicProvider.class.getName();

        String connectorTypeName = "DefaultCohortTopic.ConnectorType." + cohortName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorTypeJavaClassName);


        final String connectionDescription = "OMRS default cohort topic connection.";

        String connectionName = "DefaultCohortTopic.Connection." + cohortName;

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setEndpoint(endpoint);
        connection.setConnectorType(connectorType);

        return connection;
    }


    /**
     * Return the connection to the default registry store called localServerName.cohortName.registrystore.
     *
     * @param localServerName - name of the local server
     * @param cohortName - name of the cohort
     * @return Connection object
     */
    private Connection getDefaultCohortRegistryConnection(String localServerName, String cohortName)
    {
        final String endpointGUID      = "8bf8f5fa-b5d8-40e1-a00e-e4a0c59fd6c0";
        final String connectorTypeGUID = "2e1556a3-908f-4303-812d-d81b48b19bab";
        final String connectionGUID    = "b9af734f-f005-4085-9975-bf46c67a099a";

        final String endpointDescription = "OMRS default cohort registry endpoint.";

        String endpointAddress = localServerName + "." + cohortName + ".registrystore";
        String endpointName    = "DefaultCohortRegistry.Endpoint." + endpointAddress;

        Endpoint endpoint = new Endpoint();

        endpoint.setType(this.getEndpointType());
        endpoint.setGUID(endpointGUID);
        endpoint.setQualifiedName(endpointName);
        endpoint.setDisplayName(endpointName);
        endpoint.setDescription(endpointDescription);
        endpoint.setAddress(endpointAddress);


        final String connectorTypeDescription   = "OMRS default cohort registry connector type.";
        final String connectorTypeJavaClassName = FileBasedRegistryStoreProvider.class.getName();

        String connectorTypeName = "DefaultCohortRegistry.ConnectorType." + localServerName + "." + cohortName;

        ConnectorType connectorType = new ConnectorType();

        connectorType.setType(this.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(connectorTypeJavaClassName);


        final String connectionDescription = "OMRS default cohort registry connection.";

        String connectionName = "DefaultCohortRegistry.Connection." + localServerName + "." + cohortName;

        Connection connection = new Connection();

        connection.setType(this.getConnectionType());
        connection.setGUID(connectionGUID);
        connection.setQualifiedName(connectionName);
        connection.setDisplayName(connectionName);
        connection.setDescription(connectionDescription);
        connection.setEndpoint(endpoint);
        connection.setConnectorType(connectorType);

        return connection;
    }


    /**
     * Return the protocol level to use for communications with local open metadata access services through the open metadata
     * enterprise repository services.
     *
     * @return protocol version
     */
    private OpenMetadataEventProtocolVersion getDefaultEnterpriseOMRSTopicProtocolVersion()
    {
        return OpenMetadataEventProtocolVersion.V1;
    }


    /**
     * Return the protocol level to use for communications with other members of the open metadata repository cohort.
     *
     * @return protocol version
     */
    private OpenMetadataEventProtocolVersion getDefaultCohortOMRSTopicProtocolVersion()
    {
        return OpenMetadataEventProtocolVersion.V1;
    }


    /**
     * Return the exchange rule set so that events for all local repository changes are sent.
     *
     * @return exchange rule
     */
    private OpenMetadataExchangeRule getDefaultEventsToSendRule()
    {
        return OpenMetadataExchangeRule.ALL;
    }


    /**
     * Return the default list of types to send as a null because the exchange rule above is set to ALL.
     *
     * @return null array list
     */
    private ArrayList<TypeDefSummary> getDefaultSelectedTypesToSend()
    {
        return null;
    }


    /**
     * Return the exchange rule set so that all received events are saved.
     *
     * @return exchange rule
     */
    private OpenMetadataExchangeRule getDefaultEventsToSaveRule()
    {
        return OpenMetadataExchangeRule.ALL;
    }


    /**
     * Return the default list of types to save as a null because the exchange rule above is set to ALL.
     *
     * @return null array list
     */
    private ArrayList<TypeDefSummary> getDefaultSelectedTypesToSave()
    {
        return null;
    }


    /**
     * Return the exchange rule set so that all incoming events are processed.
     *
     * @return exchange rule
     */
    private OpenMetadataExchangeRule getDefaultEventsToProcessRule()
    {
        return OpenMetadataExchangeRule.ALL;
    }


    /**
     * Return the default list of types to process as a null because the exchange rule above is set to ALL.
     *
     * @return null array list
     */
    private ArrayList<TypeDefSummary> getDefaultSelectedTypesToProcess()
    {
        return null;
    }


    /**
     * Returns the basic configuration for a local repository.
     *
     * @param localServerName - name of the local server
     * @param localServerURL - URL root of local server used for REST calls
     * @return LocalRepositoryConfig object
     */
    private LocalRepositoryConfig getDefaultLocalRepositoryConfig(String localServerName,
                                                                  String localServerURL)
    {
        LocalRepositoryConfig localRepositoryConfig = new LocalRepositoryConfig();

        localRepositoryConfig.setMetadataCollectionId(UUID.randomUUID().toString());
        localRepositoryConfig.setLocalRepositoryLocalConnection(this.getDefaultLocalRepositoryLocalConnection());
        localRepositoryConfig.setLocalRepositoryRemoteConnection(this.getDefaultLocalRepositoryRemoteConnection(
                localServerName,
                localServerURL));
        localRepositoryConfig.setEventsToSaveRule(this.getDefaultEventsToSaveRule());
        localRepositoryConfig.setSelectedTypesToSave(this.getDefaultSelectedTypesToSave());
        localRepositoryConfig.setEventsToSendRule(this.getDefaultEventsToSendRule());
        localRepositoryConfig.setSelectedTypesToSend(this.getDefaultSelectedTypesToSend());
        localRepositoryConfig.setEventMapperConnection(this.getDefaultEventMapperConnection());

        return localRepositoryConfig;
    }


    /**
     * Return the configuration for an in-memory local repository.
     *
     * @param localServerName - name of the local server
     * @param localServerURL  - URL root of local server used for REST calls
     * @return LocalRepositoryConfig object
     */
    public LocalRepositoryConfig getInMemoryLocalRepositoryConfig(String localServerName, String localServerURL)
    {
        LocalRepositoryConfig localRepositoryConfig = this.getDefaultLocalRepositoryConfig(localServerName,
                                                                                           localServerURL);

        localRepositoryConfig.setLocalRepositoryLocalConnection(this.getInMemoryLocalRepositoryLocalConnection(
                localServerName));

        return localRepositoryConfig;
    }


    /**
     * Return the configuration for a local repository that is using the built-in graph repository.
     *
     * @param localServerName - name of local server
     * @param localServerURL  - URL root of local server used for REST calls
     * @return LocalRepositoryConfig object
     */
    public LocalRepositoryConfig getLocalGraphLocalRepositoryConfig(String localServerName, String localServerURL)
    {
        LocalRepositoryConfig localRepositoryConfig = this.getDefaultLocalRepositoryConfig(localServerName,
                                                                                           localServerURL);

        localRepositoryConfig.setLocalRepositoryLocalConnection(this.getLocalGraphRepositoryLocalConnection(
                localServerName));

        return localRepositoryConfig;
    }


    /**
     * Return the local repository configuration for a repository proxy.
     *
     * @param localServerName - name of local server
     * @param localServerURL - url used to call local server
     * @return LocalRepositoryConfig object
     */
    public LocalRepositoryConfig getRepositoryProxyLocalRepositoryConfig(String localServerName, String localServerURL)
    {
        LocalRepositoryConfig localRepositoryConfig = this.getDefaultLocalRepositoryConfig(localServerName,
                                                                                           localServerURL);

        localRepositoryConfig.setLocalRepositoryLocalConnection(null);

        return localRepositoryConfig;
    }


    /**
     * Return the default settings for the enterprise repository services' configuration.
     *
     * @param localServerName - name of the local server
     * @return EnterpriseAccessConfig parameters
     */
    public EnterpriseAccessConfig getDefaultEnterpriseAccessConfig(String localServerName)
    {
        EnterpriseAccessConfig enterpriseAccessConfig = new EnterpriseAccessConfig();

        enterpriseAccessConfig.setEnterpriseMetadataCollectionId(UUID.randomUUID().toString());
        enterpriseAccessConfig.setEnterpriseMetadataCollectionName(localServerName + defaultEnterpriseMetadataCollectionName);
        enterpriseAccessConfig.setEnterpriseOMRSTopicConnection(this.getDefaultEnterpriseOMRSTopicConnection(
                localServerName));
        enterpriseAccessConfig.setEnterpriseOMRSTopicProtocolVersion(this.getDefaultEnterpriseOMRSTopicProtocolVersion());

        return enterpriseAccessConfig;
    }


    /**
     * Return a CohortConfig object that is pre-configured with default values.
     *
     * @param localServerName - name of the local server
     * @param cohortName      - name of the cohort
     * @return default values in a CohortConfig object
     */
    public CohortConfig getDefaultCohortConfig(String localServerName, String cohortName)
    {
        CohortConfig cohortConfig  = new CohortConfig();
        String       newCohortName = defaultCohortName;

        if (cohortName != null)
        {
            newCohortName = cohortName;
        }

        cohortConfig.setCohortName(newCohortName);
        cohortConfig.setCohortRegistryConnection(this.getDefaultCohortRegistryConnection(localServerName, newCohortName));
        cohortConfig.setCohortOMRSTopicConnection(this.getDefaultCohortOMRSTopicConnection(newCohortName));
        cohortConfig.setCohortOMRSTopicProtocolVersion(this.getDefaultCohortOMRSTopicProtocolVersion());
        cohortConfig.setEventsToProcessRule(this.getDefaultEventsToProcessRule());
        cohortConfig.setSelectedTypesToProcess(this.getDefaultSelectedTypesToProcess());

        return cohortConfig;
    }


    /**
     * Returns a repository services config with the audit log set up.
     *
     * @param localServerName - name of the local server
     * @return minimally configured repository services config
     */
    public RepositoryServicesConfig getDefaultRepositoryServicesConfig(String localServerName)
    {
        RepositoryServicesConfig repositoryServicesConfig = new RepositoryServicesConfig();

        List<Connection>   auditLogStoreConnections = new ArrayList<>();

        auditLogStoreConnections.add(this.getDefaultAuditLogConnection(localServerName));

        repositoryServicesConfig.setAuditLogConnections(auditLogStoreConnections);

        return repositoryServicesConfig;
    }


    /**
     * Return the standard type for an endpoint.
     *
     * @return ElementType object
     */
    public ElementType getEndpointType()
    {
        final String        elementTypeId                   = "dbc20663-d705-4ff0-8424-80c262c6b8e7";
        final String        elementTypeName                 = "Endpoint";
        final long          elementTypeVersion              = 1;
        final String        elementTypeDescription          = "Description of the network address and related information needed to call a software service.";
        final String        elementAccessServiceURL         = null;
        final ElementOrigin elementOrigin                   = ElementOrigin.LOCAL_COHORT;
        final String        elementHomeMetadataCollectionId = null;

        ElementType elementType = new ElementType();

        elementType.setElementTypeId(elementTypeId);
        elementType.setElementTypeName(elementTypeName);
        elementType.setElementTypeVersion(elementTypeVersion);
        elementType.setElementTypeDescription(elementTypeDescription);
        elementType.setElementAccessServiceURL(elementAccessServiceURL);
        elementType.setElementOrigin(elementOrigin);
        elementType.setElementHomeMetadataCollectionId(elementHomeMetadataCollectionId);

        return elementType;
    }


    /**
     * Return the standard type for a connector type.
     *
     * @return ElementType object
     */
    public ElementType getConnectorTypeType()
    {
        final String        elementTypeId                   = "954421eb-33a6-462d-a8ca-b5709a1bd0d4";
        final String        elementTypeName                 = "ConnectorType";
        final long          elementTypeVersion              = 1;
        final String        elementTypeDescription          = "A set of properties describing a type of connector.";
        final String        elementAccessServiceURL         = null;
        final ElementOrigin elementOrigin                   = ElementOrigin.LOCAL_COHORT;
        final String        elementHomeMetadataCollectionId = null;

        ElementType elementType = new ElementType();

        elementType.setElementTypeId(elementTypeId);
        elementType.setElementTypeName(elementTypeName);
        elementType.setElementTypeVersion(elementTypeVersion);
        elementType.setElementTypeDescription(elementTypeDescription);
        elementType.setElementAccessServiceURL(elementAccessServiceURL);
        elementType.setElementOrigin(elementOrigin);
        elementType.setElementHomeMetadataCollectionId(elementHomeMetadataCollectionId);

        return elementType;
    }


    /**
     * Return the standard type for a connection type.
     *
     * @return ElementType object
     */
    public ElementType getConnectionType()
    {
        final String        elementTypeId                   = "114e9f8f-5ff3-4c32-bd37-a7eb42712253";
        final String        elementTypeName                 = "Connection";
        final long          elementTypeVersion              = 1;
        final String        elementTypeDescription          = "A set of properties to identify and configure a connector instance.";
        final String        elementAccessServiceURL         = null;
        final ElementOrigin elementOrigin                   = ElementOrigin.LOCAL_COHORT;
        final String        elementHomeMetadataCollectionId = null;

        ElementType elementType = new ElementType();

        elementType.setElementTypeId(elementTypeId);
        elementType.setElementTypeName(elementTypeName);
        elementType.setElementTypeVersion(elementTypeVersion);
        elementType.setElementTypeDescription(elementTypeDescription);
        elementType.setElementAccessServiceURL(elementAccessServiceURL);
        elementType.setElementOrigin(elementOrigin);
        elementType.setElementHomeMetadataCollectionId(elementHomeMetadataCollectionId);

        return elementType;
    }
}
