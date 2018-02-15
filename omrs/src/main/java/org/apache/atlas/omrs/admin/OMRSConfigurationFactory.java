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

import org.apache.atlas.ocf.properties.*;
import org.apache.atlas.omrs.admin.properties.*;
import org.apache.atlas.omrs.metadatahighway.cohortregistry.store.file.FileBasedRegistryStoreProvider;
import org.apache.atlas.omrs.metadatacollection.properties.typedefs.TypeDefSummary;
import org.apache.atlas.omrs.topicconnectors.kafka.KafkaOMRSTopicProvider;

import java.util.ArrayList;
import java.util.UUID;


/**
 * OMRSConfigurationFactory sets up default configuration for the OMRS components and manages the changes made
 * by the server administrator.
 */
public class OMRSConfigurationFactory
{
    /*
     * Default property fillers
     */
    private static final String  defaultEnterpriseMetadataCollectionName = "Enterprise Metadata Collection";

    private static final String  defaultTopicRootName                    = "omag";
    private static final String  defaultTopicLeafName                    = "OMRSTopic";

    private static final String  defaultTopicConnectorLeafName           = "OMRS Topic Connector";
    private static final String  defaultEnterpriseTopicConnectorRootName = "Enterprise";

    private static final String  defaultTopicConnectorName               = "OMRS Kafka Topic Connector";
    private static final String  defaultTopicConnectorDescription        = "Kafka Topic used to exchange event between members of an open metadata repository cluster.";
    private static final String  defaultTopicConnectorProviderClassName  = KafkaOMRSTopicProvider.class.getName();

    private static final String  defaultCohortName                       = "defaultCohort";

    private static final String  defaultRegistryStoreFQName              = "OMRS Cohort Registry Store: cohort.registry";
    private static final String  defaultRegistryStoreDisplayName         = "Cohort Registry Store";
    private static final String  defaultRegistryStoreDescription         = "File-based Store use by the cohort registry to store information about the members of the open metadata repository cluster.";
    private static final String  defaultRegistryStoreAddress             = "cohort.registry";
    private static final String  defaultRegistryStoreProviderClassName   = FileBasedRegistryStoreProvider.class.getName();



    private String                     localServerName = null;
    private String                     localServerType = null;
    private String                     localServerURL = null;
    private String                     localOrganizationName = null;
    private RepositoryServicesConfig   repositoryServicesConfig = null;


    /**
     * Constructor used when the server is being configured.  Any parameter may be null.
     *
     * @param localServerName - name of the local server
     * @param localServerType - type of the local server
     * @param localServerURL - URL of the local server
     * @param localOrganizationName - name of the organization that owns the local server
     */
    public OMRSConfigurationFactory(String                   localServerName,
                                    String                   localServerType,
                                    String                   localServerURL,
                                    String                   localOrganizationName)
    {
        this.localServerName = localServerName;
        this.localServerType = localServerType;
        this.localServerURL = localServerURL;
        this.localOrganizationName = localOrganizationName;
    }

    public  Connection  getGenericConnection()
    {
        return null;
    }

    public  Connection  getDefaultAuditLogConnection()
    {
        return null;
    }

    public  Connection  getDefaultOpenMetadataTypesArchiveConnection()
    {
        return null;
    }

    private Connection  getGenericArchiveConnection()
    {
        return null;
    }

    private Connection  getGenericRepositoryConnection()
    {
        return null;
    }

    private Connection  getDefaultLocalRepositoryLocalConnection()
    {
        return null;
    }

    private Connection  getAtlasLocalRepositoryLocalConnection()
    {
        return null;
    }

    private Connection  getDefaultLocalRepositoryRemoteConnection(String    localServerURL)
    {
        return null;
    }

    private Connection  getDefaultEventMapperConnection()
    {
        return null;
    }

    public  Connection  getGenericEventMapperConnection()
    {
        return null;
    }

    private Connection  getDefaultEnterpriseOMRSTopicConnection()  { return null; }

    private Connection  getDefaultCohortOMRSTopicConnection () { return null; }

    private Connection  getDefaultCohortRegistryConnection ()
    {
        return null;
    }

    private OpenMetadataEventProtocolVersion getDefaultEnterpriseOMRSTopicProtocolVersion() { return OpenMetadataEventProtocolVersion.V1; }

    private OpenMetadataEventProtocolVersion getDefaultCohortOMRSTopicProtocolVersion() { return OpenMetadataEventProtocolVersion.V1; }

    private OpenMetadataExchangeRule  getDefaultEventsToSendRule()
    {
        return OpenMetadataExchangeRule.ALL;
    }

    private ArrayList<TypeDefSummary> getDefaultSelectedTypesToSend() { return null; }

    private OpenMetadataExchangeRule  getDefaultEventsToSaveRule()
    {
        return OpenMetadataExchangeRule.ALL;
    }

    private ArrayList<TypeDefSummary> getDefaultSelectedTypesToSave() { return null; }

    private OpenMetadataExchangeRule getDefaultEventsToProcessRule()
    {
        return OpenMetadataExchangeRule.ALL;
    }

    private ArrayList<TypeDefSummary> getDefaultSelectedTypesToProcess() { return null; }

    private ArrayList<Connection> getDefaultOpenMetadataArchiveList()
    {
        ArrayList<Connection> openMetadataArchiveList = new ArrayList<>();

        openMetadataArchiveList.add(this.getDefaultOpenMetadataTypesArchiveConnection());

        return openMetadataArchiveList;
    }

    public LocalRepositoryConfig getDefaultLocalRepositoryConfig(String    localServerURL)
    {
        LocalRepositoryConfig  localRepositoryConfig = new LocalRepositoryConfig();

        localRepositoryConfig.setMetadataCollectionId(UUID.randomUUID().toString());
        localRepositoryConfig.setLocalRepositoryLocalConnection(this.getDefaultLocalRepositoryLocalConnection());
        localRepositoryConfig.setLocalRepositoryRemoteConnection(this.getDefaultLocalRepositoryRemoteConnection(localServerURL));
        localRepositoryConfig.setEventsToSaveRule(this.getDefaultEventsToSaveRule());
        localRepositoryConfig.setSelectedTypesToSave(this.getDefaultSelectedTypesToSave());
        localRepositoryConfig.setEventsToSendRule(this.getDefaultEventsToSendRule());
        localRepositoryConfig.setSelectedTypesToSend(this.getDefaultSelectedTypesToSend());
        localRepositoryConfig.setEventMapperConnection(this.getDefaultEventMapperConnection());

        return localRepositoryConfig;
    }

    public EnterpriseAccessConfig getDefaultEnterpriseAccessConfig()
    {
        EnterpriseAccessConfig  enterpriseAccessConfig = new EnterpriseAccessConfig();

        enterpriseAccessConfig.setEnterpriseMetadataCollectionId(UUID.randomUUID().toString());
        enterpriseAccessConfig.setEnterpriseMetadataCollectionName(defaultEnterpriseMetadataCollectionName);
        enterpriseAccessConfig.setEnterpriseOMRSTopicConnection(this.getDefaultEnterpriseOMRSTopicConnection());
        enterpriseAccessConfig.setEnterpriseOMRSTopicProtocolVersion(this.getDefaultEnterpriseOMRSTopicProtocolVersion());

        return enterpriseAccessConfig;
    }


    /**
     * Return a CohortConfig object that is pre-configured with default values.
     *
     * @return default values in a CohortConfig object
     */
    public CohortConfig  getDefaultCohortConfig()
    {
        CohortConfig    cohortConfig = new CohortConfig();

        cohortConfig.setCohortName(defaultCohortName);
        cohortConfig.setCohortRegistryConnection(this.getDefaultCohortRegistryConnection());
        cohortConfig.setCohortOMRSTopicConnection(this.getDefaultCohortOMRSTopicConnection());
        cohortConfig.setCohortOMRSTopicProtocolVersion(this.getDefaultCohortOMRSTopicProtocolVersion());
        cohortConfig.setEventsToProcessRule(this.getDefaultEventsToProcessRule());
        cohortConfig.setSelectedTypesToProcess(this.getDefaultSelectedTypesToProcess());

        return cohortConfig;
    }

}
