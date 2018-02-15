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
package org.apache.atlas.omrs.admin.properties;

import org.apache.atlas.ocf.properties.Connection;

import java.util.ArrayList;

/**
 * RepositoryServicesConfig provides the configuration properties that are needed by the OMRS components
 * to manage access to the metadata repositories that are members of the open metadata repository clusters that
 * this server connects to.
 * <ul>
 *     <li>
 *         auditLogConnection is a connection describing the connector to the AuditLog that the OMRS
 *         component should use.
 *     </li>
 *     <li>
 *         openMetadataArchiveConnectionList is a list of Open Metadata Archive Connections.
 *         An open metadata archive connection provides properties needed to create a connector to manage
 *         an open metadata archive.  This contains pre-built TypeDefs and metadata instance.
 *         The archives are managed by the OMRSArchiveManager.
 *     </li>
 *     <li>
 *         localRepositoryConfig describes the properties used to manage the local metadata repository for this server.
 *     </li>
 *     <li>
 *         enterpriseAccessConfig describes the properties that control the cluster federation services that the
 *         OMRS provides to the Open Metadata AccessServices (OMASs).
 *     </li>
 *     <li>
 *         cohortConfigList provides details of each open metadata repository cluster that the local server is
 *         connected to.
 *     </li>
 * </ul>
 */
public class RepositoryServicesConfig
{
    private Connection              auditLogConnection                = null;
    private ArrayList<Connection>   openMetadataArchiveConnectionList = new ArrayList<>();
    private LocalRepositoryConfig   localRepositoryConfig             = null;
    private EnterpriseAccessConfig  enterpriseAccessConfig            = null;
    private ArrayList<CohortConfig> cohortConfigList                  = new ArrayList<>();


    /**
     * Default constructor does nothing
     */
    public RepositoryServicesConfig()
    {
    }


    /**
     * Constructor to set all properties.
     *
     * @param auditLogConnection - connection to the audit log.
     * @param openMetadataArchiveConnectionList - list of open metadata archive files to load.
     * @param localRepositoryConfig - properties to configure the behavior of the local repository.
     * @param enterpriseAccessConfig - properties to configure the behavior of the federation services provided
     *                                to the Open Metadata Access Services (OMASs).
     * @param cohortConfigList - properties about the open metadata repository clusters that this server connects to.
     */
    public RepositoryServicesConfig(Connection               auditLogConnection,
                                    ArrayList<Connection>    openMetadataArchiveConnectionList,
                                    LocalRepositoryConfig    localRepositoryConfig,
                                    EnterpriseAccessConfig   enterpriseAccessConfig,
                                    ArrayList<CohortConfig>  cohortConfigList)
    {
        this.auditLogConnection = auditLogConnection;
        this.openMetadataArchiveConnectionList = openMetadataArchiveConnectionList;
        this.localRepositoryConfig = localRepositoryConfig;
        this.enterpriseAccessConfig = enterpriseAccessConfig;
        this.cohortConfigList = cohortConfigList;
    }


    /**
     * Return the Connection properties used to create an OCF Connector to the AuditLog.
     *
     * @return Connection object
     */
    public Connection getAuditLogConnection()
    {
        return auditLogConnection;
    }


    /**
     * Set up the Connection properties used to create an OCF Connector to the AuditLog.
     *
     * @param auditLogConnection - Connection object
     */
    public void setAuditLogConnection(Connection auditLogConnection)
    {
        this.auditLogConnection = auditLogConnection;
    }


    /**
     * Return the list of Connection object, each of which is used to create the Connector to an Open Metadata
     * Archive.  Open Metadata Archive contains pre-built metadata types and instances.
     *
     * @return list of Connection objects
     */
    public ArrayList<Connection> getOpenMetadataArchiveConnectionList()
    {
        return openMetadataArchiveConnectionList;
    }


    /**
     * Set up the list of Connection object, each of which is used to create the Connector to an Open Metadata
     * Archive.  Open Metadata Archive contains pre-built metadata types and instances.
     *
     * @param openMetadataArchiveConnectionList - list of Connection objects
     */
    public void setOpenMetadataArchiveConnectionList(ArrayList<Connection> openMetadataArchiveConnectionList)
    {
        this.openMetadataArchiveConnectionList = openMetadataArchiveConnectionList;
    }


    /**
     * Return the configuration properties for the local repository.
     *
     * @return configuration properties
     */
    public LocalRepositoryConfig getLocalRepositoryConfig()
    {
        return localRepositoryConfig;
    }


    /**
     * Set up the configuration properties for the local repository.
     *
     * @param localRepositoryConfig - configuration properties
     */
    public void setLocalRepositoryConfig(LocalRepositoryConfig localRepositoryConfig)
    {
        this.localRepositoryConfig = localRepositoryConfig;
    }


    /**
     * Return the configuration for the federation services provided by OMRS to the Open Metadata Access
     * Services (OMASs).
     *
     * @return configuration properties
     */
    public EnterpriseAccessConfig getEnterpriseAccessConfig()
    {
        return enterpriseAccessConfig;
    }


    /**
     * Set up the configuration for the federation services provided by OMRS to the Open Metadata Access
     * Services (OMASs).
     *
     * @param enterpriseAccessConfig configuration properties
     */
    public void setEnterpriseAccessConfig(EnterpriseAccessConfig enterpriseAccessConfig)
    {
        this.enterpriseAccessConfig = enterpriseAccessConfig;
    }


    /**
     * Return the configuration properties for each open metadata repository cluster that this local server
     * connects to.
     *
     * @return list of cluster configuration properties
     */
    public ArrayList<CohortConfig> getCohortConfigList()
    {
        return cohortConfigList;
    }


    /**
     * Set up the configuration properties for each open metadata repository cluster that this local server
     * connects to.
     *
     * @param cohortConfigList - list of cluster configuration properties
     */
    public void setCohortConfigList(ArrayList<CohortConfig> cohortConfigList)
    {
        this.cohortConfigList = cohortConfigList;
    }
}
