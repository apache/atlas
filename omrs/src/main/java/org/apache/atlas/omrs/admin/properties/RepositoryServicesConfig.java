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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

import org.apache.atlas.ocf.properties.beans.Connection;

import java.util.ArrayList;
import java.util.List;


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
 *         openMetadataArchiveConnections is a list of Open Metadata Archive Connections.
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
@JsonAutoDetect(getterVisibility=PUBLIC_ONLY, setterVisibility=PUBLIC_ONLY, fieldVisibility=NONE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown=true)
public class RepositoryServicesConfig
{
    private ArrayList<Connection>   auditLogConnections            = new ArrayList<>();
    private ArrayList<Connection>   openMetadataArchiveConnections = new ArrayList<>();
    private LocalRepositoryConfig   localRepositoryConfig          = null;
    private EnterpriseAccessConfig  enterpriseAccessConfig         = null;
    private ArrayList<CohortConfig> cohortConfigList               = new ArrayList<>();


    /**
     * Default constructor does nothing
     */
    public RepositoryServicesConfig()
    {
    }


    /**
     * Constructor to set all properties.
     *
     * @param auditLogConnections - connections to copies of the audit log.
     * @param openMetadataArchiveConnections - list of open metadata archive files to load.
     * @param localRepositoryConfig - properties to configure the behavior of the local repository.
     * @param enterpriseAccessConfig - properties to configure the behavior of the federation services provided
     *                                to the Open Metadata Access Services (OMASs).
     * @param cohortConfigList - properties about the open metadata repository clusters that this server connects to.
     */
    public RepositoryServicesConfig(List<Connection>         auditLogConnections,
                                    List<Connection>         openMetadataArchiveConnections,
                                    LocalRepositoryConfig    localRepositoryConfig,
                                    EnterpriseAccessConfig   enterpriseAccessConfig,
                                    List<CohortConfig>       cohortConfigList)
    {
        this.setAuditLogConnections(auditLogConnections);
        this.setOpenMetadataArchiveConnections(openMetadataArchiveConnections);
        this.setLocalRepositoryConfig(localRepositoryConfig);
        this.setEnterpriseAccessConfig(enterpriseAccessConfig);
        this.setCohortConfigList(cohortConfigList);
    }


    /**
     * Return the Connection properties used to create an OCF Connector to the AuditLog.
     *
     * @return Connection object
     */
    public List<Connection> getAuditLogConnections()
    {
        if (auditLogConnections == null)
        {
            return null;
        }
        else
        {
            return auditLogConnections;
        }
    }


    /**
     * Set up the Connection properties used to create an OCF Connector to the AuditLog.
     *
     * @param auditLogConnections - list of Connection objects
     */
    public void setAuditLogConnections(List<Connection> auditLogConnections)
    {
        if (auditLogConnections == null)
        {
            this.auditLogConnections = null;
        }
        else
        {
            this.auditLogConnections = new ArrayList<>(auditLogConnections);
        }
    }


    /**
     * Return the list of Connection object, each of which is used to create the Connector to an Open Metadata
     * Archive.  Open Metadata Archive contains pre-built metadata types and instances.
     *
     * @return list of Connection objects
     */
    public List<Connection> getOpenMetadataArchiveConnections()
    {
        if (openMetadataArchiveConnections == null)
        {
            return null;
        }
        else
        {
            return openMetadataArchiveConnections;
        }
    }


    /**
     * Set up the list of Connection object, each of which is used to create the Connector to an Open Metadata
     * Archive.  Open Metadata Archive contains pre-built metadata types and instances.
     *
     * @param openMetadataArchiveConnections - list of Connection objects
     */
    public void setOpenMetadataArchiveConnections(List<Connection> openMetadataArchiveConnections)
    {
        if (openMetadataArchiveConnections == null)
        {
            this.openMetadataArchiveConnections = null;
        }
        else
        {
            this.openMetadataArchiveConnections = new ArrayList<>(openMetadataArchiveConnections);
        }
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
    public List<CohortConfig> getCohortConfigList()
    {
        if (cohortConfigList == null)
        {
            return null;
        }
        else
        {
            return cohortConfigList;
        }
    }


    /**
     * Set up the configuration properties for each open metadata repository cluster that this local server
     * connects to.
     *
     * @param cohortConfigList - list of cluster configuration properties
     */
    public void setCohortConfigList(List<CohortConfig> cohortConfigList)
    {
        if (cohortConfigList == null)
        {
            this.cohortConfigList = null;
        }
        else
        {
            this.cohortConfigList = new ArrayList<>(cohortConfigList);
        }
    }
}
